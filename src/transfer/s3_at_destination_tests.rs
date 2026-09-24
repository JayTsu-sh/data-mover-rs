//! Checkpointed transfers to S3 through the engine's at-destination route (ADR-0006 C15b), over the
//! in-memory S3 with the transition switch on (it stays off for real connections until C15c).

use std::error::Error;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::model::{FailureClass, StoragePath, Transience};
use crate::storage::PrepareFact;
use crate::storage::backends::s3::tests::{MemoryS3, identity};
use crate::storage::backends::s3::{S3Protocol as _, S3ProtocolFailure, connect_at_destination};
use crate::transfer::{
    EffectiveRecovery, InflightLimits, TransferPolicy, TransferRequest, transfer,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const MIB: usize = 1024 * 1024;
/// The automatic checkpoint interval these destinations declare (64 MiB for real connections).
const INTERVAL: usize = 16 * MIB;

/// Each test writes its own final key: the engine's per-file guard is process-wide.
fn checkpointed(protocol: &Arc<MemoryS3>, final_key: &str) -> TestResult<TransferRequest> {
    let storage = || connect_at_destination(protocol.clone(), identity(), INTERVAL as u64);
    Ok(TransferRequest::new(
        storage()?,
        StoragePath::new("source")?,
        storage()?,
        StoragePath::new(final_key)?,
        InflightLimits::new(4, 4 * MIB, 4)?,
        CancellationToken::new(),
    )
    .with_transfer_policy(TransferPolicy::Checkpointed))
}

async fn seeded(size: usize) -> (Arc<MemoryS3>, Bytes) {
    let protocol = Arc::new(MemoryS3::default());
    let payload = Bytes::from((0..=252_u8).cycle().take(size).collect::<Vec<_>>());
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), payload.clone());
    (protocol, payload)
}

/// Nothing but the source and the final object: no pointer, no upload.
async fn leaves_nothing_behind(protocol: &MemoryS3, final_key: &str) -> TestResult<bool> {
    let open = protocol
        .list_uploads(final_key)
        .await
        .map_err(|failure| format!("{failure:?}"))?;
    let artifacts = protocol
        .objects
        .lock()
        .await
        .keys()
        .any(|key| key.contains(".data-mover-"));
    Ok(open.is_empty() && !artifacts)
}

/// D3: a checkpointed object no larger than the interval (64 MiB for real connections) is a
/// multipart upload on the final key that never writes a pointer.
#[tokio::test]
async fn a_checkpointed_object_up_to_the_interval_writes_no_pointer() -> TestResult {
    const FINAL: &str = "dir/small-checkpointed";
    let (protocol, payload) = seeded(INTERVAL).await;
    let outcome = transfer(checkpointed(&protocol, FINAL)?).await?;
    assert_eq!(outcome.prepare, PrepareFact::Fresh);
    assert_eq!(
        outcome.recovery,
        EffectiveRecovery::SkippedBelowCheckpointThreshold
    );
    assert!(outcome.blake3.is_some());
    assert_eq!(*protocol.puts.lock().await, 0);
    assert_eq!(*protocol.multipart_begins.lock().await, 1);
    assert_eq!(protocol.objects.lock().await.get(FINAL), Some(&payload));
    assert!(leaves_nothing_behind(&protocol, FINAL).await?);
    Ok(())
}

/// Above the interval the engine's first deferred checkpoint makes the destination write its
/// pointer (one `PutObject`); publication completes the upload on the final key and removes it.
#[tokio::test]
async fn a_checkpointed_object_over_the_interval_writes_one_pointer() -> TestResult {
    const FINAL: &str = "dir/large-checkpointed";
    let (protocol, payload) = seeded(INTERVAL + 8 * MIB).await;
    let outcome = transfer(checkpointed(&protocol, FINAL)?).await?;
    assert_eq!(outcome.recovery, EffectiveRecovery::Checkpointed);
    assert!(outcome.blake3.is_some());
    assert_eq!(*protocol.puts.lock().await, 1);
    assert_eq!(*protocol.multipart_begins.lock().await, 1);
    assert_eq!(*protocol.completes.lock().await, 1);
    assert_eq!(protocol.objects.lock().await.get(FINAL), Some(&payload));
    assert!(leaves_nothing_behind(&protocol, FINAL).await?);
    Ok(())
}

/// A failed part before any checkpoint keeps an unrecoverable stage; discarding it aborts the
/// upload and never touches the final key.
#[tokio::test]
async fn a_failed_upload_is_discarded_without_touching_the_final_key() -> TestResult {
    const FINAL: &str = "dir/failed-checkpointed";
    let (protocol, _) = seeded(20 * MIB).await;
    protocol
        .objects
        .lock()
        .await
        .insert(FINAL.into(), Bytes::from_static(b"previous"));
    *protocol.part_failure.lock().await = Some((
        2,
        S3ProtocolFailure::entry(
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "denied",
        ),
    ));
    let failure = transfer(checkpointed(&protocol, FINAL)?)
        .await
        .err()
        .ok_or("a failed part must fail the transfer")?;
    assert!(!failure.final_destination_changed());
    assert!(failure.has_unpublished_stage() && !failure.has_recoverable_stage());
    failure.discard_stage().await?;
    assert!(leaves_nothing_behind(&protocol, FINAL).await?);
    assert_eq!(
        protocol.objects.lock().await.get(FINAL),
        Some(&Bytes::from_static(b"previous"))
    );
    Ok(())
}
