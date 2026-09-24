//! `Direct` transfers to S3 through the engine (ADR-0006 C14c), over the in-memory S3: the object
//! is written at its final key inside `write`, read back after publication, and never staged.

use std::error::Error;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::model::{FailureClass, StoragePath, Transience};
use crate::storage::PrepareFact;
use crate::storage::backends::s3::tests::{MemoryS3, identity};
use crate::storage::backends::s3::{S3Protocol as _, S3ProtocolFailure, connect};
use crate::transfer::{
    EffectiveRecovery, InflightLimits, TransferPolicy, TransferRequest, TransferRoute, transfer,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const MIB: usize = 1024 * 1024;

fn direct_request(protocol: &Arc<MemoryS3>) -> TestResult<TransferRequest> {
    let storage = || connect(protocol.clone(), identity(), None);
    Ok(TransferRequest::new(
        storage()?,
        StoragePath::new("source")?,
        storage()?,
        StoragePath::new("direct-final")?,
        InflightLimits::new(4, 4 * MIB, 4)?,
        CancellationToken::new(),
    )
    .with_transfer_policy(TransferPolicy::Direct))
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

/// No upload is open on the final key, and no object was ever staged under `.data-mover-`
/// (every upload the fake saw was begun on the final key: see `multipart_begins`).
async fn leaves_nothing_behind(protocol: &MemoryS3) -> TestResult<bool> {
    let open = protocol
        .list_uploads("direct-final")
        .await
        .map_err(|failure| format!("{failure:?}"))?;
    let staged = protocol
        .objects
        .lock()
        .await
        .keys()
        .any(|key| key.contains(".data-mover-"));
    Ok(open.is_empty() && !staged)
}

/// A small object is one `PutObject` to the final key, verified after publication.
#[tokio::test]
async fn a_small_direct_transfer_is_one_put() -> TestResult {
    let (protocol, payload) = seeded(1024).await;
    let outcome = transfer(direct_request(&protocol)?).await?;
    assert_eq!(outcome.route, TransferRoute::Streaming);
    assert_eq!(outcome.recovery, EffectiveRecovery::Disabled);
    assert_eq!(outcome.prepare, PrepareFact::Fresh);
    assert!(outcome.blake3.is_some());
    assert_eq!(*protocol.puts.lock().await, 1);
    assert_eq!(*protocol.multipart_begins.lock().await, 0);
    assert_eq!(
        protocol.objects.lock().await.get("direct-final"),
        Some(&payload)
    );
    assert!(leaves_nothing_behind(&protocol).await?);
    Ok(())
}

/// A large object is one multipart upload on the final key, completed inside `write`; on a
/// versioned bucket the outcome names the version it created.
#[tokio::test]
async fn a_large_direct_transfer_is_one_upload_on_the_final_key() -> TestResult {
    let (protocol, payload) = seeded(20 * MIB).await;
    protocol
        .put_version("direct-final", "v1", Bytes::from_static(b"old"))
        .await;
    let outcome = transfer(direct_request(&protocol)?).await?;
    assert!(outcome.blake3.is_some());
    let version = outcome
        .destination_version
        .ok_or("no destination version")?;
    assert_ne!(version, "v1");
    assert_eq!(*protocol.multipart_begins.lock().await, 1);
    assert_eq!(*protocol.completes.lock().await, 1);
    assert_eq!(*protocol.puts.lock().await, 0);
    assert_eq!(
        protocol.objects.lock().await.get("direct-final"),
        Some(&payload)
    );
    assert!(leaves_nothing_behind(&protocol).await?);
    Ok(())
}

/// A failed part fails the transfer with the final key possibly changed and no stage kept; the
/// upload was aborted inside `write`.
#[tokio::test]
async fn a_failed_direct_upload_leaves_no_upload() -> TestResult {
    let (protocol, _) = seeded(20 * MIB).await;
    *protocol.part_failure.lock().await = Some((
        2,
        S3ProtocolFailure::entry(
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "denied",
        ),
    ));
    let failure = transfer(direct_request(&protocol)?)
        .await
        .err()
        .ok_or("a failed part must fail the transfer")?;
    assert!(failure.final_destination_changed());
    assert!(!failure.has_unpublished_stage());
    assert_eq!(*protocol.aborts.lock().await, 1);
    assert!(!protocol.objects.lock().await.contains_key("direct-final"));
    assert!(leaves_nothing_behind(&protocol).await?);
    Ok(())
}
