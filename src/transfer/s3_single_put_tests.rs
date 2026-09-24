//! A small object copied to S3 through the engine is one `PutObject` to the final key, read back
//! after publication (ADR-0006 C14b), over the in-memory S3.

use std::error::Error;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::engine::run_until_transferred;
use crate::model::StoragePath;
use crate::storage::backends::s3::connect;
use crate::storage::backends::s3::tests::{MemoryS3, etag_of, identity, native_context};
use crate::transfer::{
    EffectiveRecovery, InflightLimits, PayloadShapingPolicy, TransferPolicy, TransferRequest,
    TransferRoute, transfer,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn request(protocol: &Arc<MemoryS3>, final_path: &str) -> TestResult<TransferRequest> {
    let source = connect(protocol.clone(), identity(), Some(native_context()))?;
    let destination = connect(protocol.clone(), identity(), Some(native_context()))?;
    Ok(TransferRequest::new(
        source,
        StoragePath::new("source")?,
        destination,
        StoragePath::new(final_path)?,
        InflightLimits::new(2, 64 * 1024, 2)?,
        CancellationToken::new(),
    )
    // The same fake on both ends would otherwise copy natively.
    .with_payload_shaping(PayloadShapingPolicy::RequireClientShaped))
}

/// On a versioned bucket the outcome names the version the PUT created, and the read-back came
/// after publication: every read was pinned to that version and our `ETag`.
#[tokio::test]
async fn a_small_object_is_one_put_verified_after_publication() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let payload = Bytes::from(vec![3; 200 * 1024]);
    protocol.put_version("source", "v1", payload.clone()).await;
    let outcome = transfer(request(&protocol, "single-final")?).await?;
    assert_eq!(outcome.route, TransferRoute::Streaming);
    let version = outcome
        .destination_version
        .clone()
        .ok_or("no destination version")?;
    assert_ne!(version, "v1");
    assert!(outcome.blake3.is_some());
    assert_eq!(*protocol.puts.lock().await, 1);
    assert_eq!(*protocol.multipart_begins.lock().await, 0);
    assert_eq!(
        protocol.objects.lock().await.get("single-final"),
        Some(&payload)
    );
    let pinned: Vec<_> = protocol
        .range_observations
        .lock()
        .await
        .iter()
        .filter(|read| read.version_id.as_deref() == Some(version.as_str()))
        .cloned()
        .collect();
    assert!(!pinned.is_empty(), "the read-back is pinned to our version");
    assert!(pinned.iter().all(|read| read.etag == etag_of(&payload)));
    assert!(
        protocol
            .objects
            .lock()
            .await
            .keys()
            .all(|key| !key.starts_with(".data-mover-"))
    );
    Ok(())
}

/// A checkpointed transfer of a small object keeps nothing in the local recovery store: the
/// destination declines recovery for a single PUT, and the outcome says it was skipped.
#[tokio::test]
async fn a_checkpointed_small_object_keeps_no_recovery_record() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let payload = Bytes::from(vec![4; 200 * 1024]);
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), payload.clone());
    let checkpointed = || {
        request(&protocol, "checkpointed-final")
            .map(|request| request.with_transfer_policy(TransferPolicy::Checkpointed))
    };
    let interrupted = run_until_transferred(checkpointed()?).await?;
    let binding = interrupted.recovery_binding();
    assert!(!interrupted.recovery_enabled());
    assert!(!super::recovery_store::has_entry(binding));
    interrupted.discard().await?;
    let outcome = transfer(checkpointed()?).await?;
    assert_eq!(
        outcome.recovery,
        EffectiveRecovery::SkippedBelowCheckpointThreshold
    );
    assert!(!super::recovery_store::has_entry(binding));
    assert_eq!(outcome.destination_version, None);
    assert_eq!(
        protocol.objects.lock().await.get("checkpointed-final"),
        Some(&payload)
    );
    assert_eq!(*protocol.multipart_begins.lock().await, 0);
    Ok(())
}
