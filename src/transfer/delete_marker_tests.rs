//! `transfer::write_delete_marker` (P7 H1): the per-destination lease, the directory-object
//! capability, and a history replayed oldest to newest onto a versioned destination.

use std::error::Error;
use std::fs;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::engine::run_until_transferred;
use crate::model::{FailureClass, StoragePath};
use crate::storage::backends::local::test_destination_storage;
use crate::storage::backends::s3::tests::{
    MemoryS3, Versioning, content_md5, endpoint_of, native_context,
};
use crate::storage::backends::s3::{S3Protocol as _, S3VersionFacts, connect};
use crate::storage::{Storage, StorageRoleFailure};
use crate::transfer::{
    DeleteMarkerOutcome, PayloadShapingPolicy, SourceVersion, TransferFailure, TransferPhase,
    TransferRequest, TransferSide, destination_keeps_history, transfer, write_delete_marker,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn bucket(protocol: &Arc<MemoryS3>) -> TestResult<Storage> {
    connect(
        protocol.clone(),
        endpoint_of(protocol),
        Some(native_context()),
    )
}

fn class(failure: &TransferFailure) -> Option<FailureClass> {
    match Error::source(failure).and_then(|cause| cause.downcast_ref::<StorageRoleFailure>()) {
        Some(StorageRoleFailure::Entry(entry)) => Some(entry.class()),
        _ => None,
    }
}

async fn marker(storage: &Storage, key: &str) -> Result<DeleteMarkerOutcome, TransferFailure> {
    write_delete_marker(
        storage.clone(),
        StoragePath::new(key).unwrap_or_else(|error| panic!("{error}")),
        CancellationToken::new(),
    )
    .await
}

/// While a transfer of the key holds its lease, a delete marker on it is a transient `Conflict`;
/// once the transfer is gone it is written.
#[tokio::test]
async fn a_delete_marker_holds_the_destination_guard() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    protocol
        .objects
        .lock()
        .await
        .insert("src".into(), Bytes::from_static(b"payload"));
    let request = TransferRequest::new(
        bucket(&protocol)?,
        StoragePath::new("src")?,
        bucket(&protocol)?,
        CancellationToken::new(),
    )
    .with_final_path(StoragePath::new("dst/k")?)
    .with_payload_shaping(PayloadShapingPolicy::RequireClientShaped);
    let transferred = run_until_transferred(request).await?;
    let Err(busy) = marker(&bucket(&protocol)?, "dst/k").await else {
        return Err("a delete marker was written while a transfer held the key".into());
    };
    assert_eq!(
        (busy.phase(), busy.side(), class(&busy)),
        (
            TransferPhase::Prepare,
            TransferSide::Destination,
            Some(FailureClass::Conflict)
        )
    );
    transferred.discard().await?;
    assert!(matches!(
        marker(&bucket(&protocol)?, "dst/k").await?,
        DeleteMarkerOutcome::Written { .. }
    ));
    Ok(())
}

/// A filesystem keeps no delete markers: refused, nothing written. A directory object key on a
/// filesystem is refused earlier, at `Preflight`.
#[tokio::test]
async fn a_filesystem_keeps_no_delete_markers() -> TestResult {
    let root = tempfile::tempdir()?;
    let local = test_destination_storage(root.path(), "delete-marker-destination")?;
    let refused = marker(&local, "k").await.err().ok_or("written on Local")?;
    assert_eq!(
        (refused.phase(), class(&refused)),
        (TransferPhase::Publish, Some(FailureClass::Unsupported))
    );
    let refused = marker(&local, "d/").await.err().ok_or("written on Local")?;
    assert_eq!(
        (refused.phase(), class(&refused)),
        (TransferPhase::Preflight, Some(FailureClass::Unsupported))
    );
    assert_eq!(fs::read_dir(root.path())?.count(), 0);
    assert!(!destination_keeps_history(&local).await?);
    Ok(())
}

/// The destination's versioning is asked now: an enabled bucket keeps a history, a suspended one
/// does not.
#[tokio::test]
async fn destination_keeps_history_asks_the_bucket_now() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    let storage = bucket(&protocol)?;
    assert!(destination_keeps_history(&storage).await?);
    protocol.set_versioning(Versioning::Suspended);
    assert!(!destination_keeps_history(&storage).await?);
    Ok(())
}

/// D5 replay: the source history of `m/` (5 B, 0 B, marker, 3 B, marker) rebuilt oldest to
/// newest on a versioned destination has the same records, kinds and sizes, in order.
#[tokio::test]
async fn a_mixed_history_is_replayed_in_order() -> TestResult {
    let source = Arc::new(MemoryS3::default());
    source.set_versioning(Versioning::Enabled);
    for step in [Some(&b"fives"[..]), Some(b""), None, Some(b"333"), None] {
        match step {
            Some(body) => {
                let bytes = Bytes::copy_from_slice(body);
                let md5 = content_md5(&bytes);
                source
                    .put_object("m/", bytes.into(), &md5)
                    .await
                    .map_err(|e| format!("{e:?}"))?;
            }
            None => source
                .delete_object("m/")
                .await
                .map_err(|e| format!("{e:?}"))?,
        }
    }
    let mut records = source
        .list_versions("m/")
        .await
        .map_err(|e| format!("{e:?}"))?;
    records.reverse();
    let destination = Arc::new(MemoryS3::default());
    destination.set_versioning(Versioning::Enabled);
    for record in &records {
        if record.delete_marker {
            marker(&bucket(&destination)?, "m/").await?;
        } else {
            let request = TransferRequest::new(
                bucket(&source)?,
                StoragePath::new("m/")?,
                bucket(&destination)?,
                CancellationToken::new(),
            )
            .with_source_version(SourceVersion::Id(record.version_id.clone()))
            .with_payload_shaping(PayloadShapingPolicy::RequireClientShaped);
            transfer(request).await?;
        }
    }
    let shape = |records: &[S3VersionFacts]| -> Vec<(bool, u64)> {
        records
            .iter()
            .rev()
            .map(|record| (record.delete_marker, record.size))
            .collect()
    };
    let rebuilt = destination
        .list_versions("m/")
        .await
        .map_err(|e| format!("{e:?}"))?;
    assert_eq!(
        shape(&rebuilt),
        [(false, 5), (false, 0), (true, 0), (false, 3), (true, 0)]
    );
    assert_eq!(
        shape(&rebuilt),
        shape(
            &source
                .list_versions("m/")
                .await
                .map_err(|e| format!("{e:?}"))?
        )
    );
    Ok(())
}
