//! `Direct` transfers to S3 through the engine (ADR-0006 C14c), over the in-memory S3: the object
//! is written at its final key inside `write`, read back after publication, and never staged.

use std::error::Error;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::model::{FailureClass, StoragePath, Transience};
use crate::storage::PrepareFact;
use crate::storage::backends::s3::tests::{MemoryS3, Versioning, endpoint_of, source_storage};
use crate::storage::backends::s3::{S3Protocol as _, S3ProtocolFailure, connect};
use crate::transfer::{
    EffectiveRecovery, FailedDataCleanup, MetadataTransferMode, TransferPhase, TransferPolicy,
    TransferRequest, TransferRoute, transfer,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const MIB: usize = 1024 * 1024;

fn direct_request(protocol: &Arc<MemoryS3>) -> TestResult<TransferRequest> {
    let storage = || connect(protocol.clone(), endpoint_of(protocol), None);
    Ok(TransferRequest::new(
        source_storage(protocol, "src")?,
        StoragePath::new("final")?,
        storage()?,
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
        .insert("src/final".into(), payload.clone());
    (protocol, payload)
}

/// No upload is open on the final key, and no object was ever staged under `.data-mover-`
/// (every upload the fake saw was begun on the final key: see `multipart_begins`).
async fn leaves_nothing_behind(protocol: &MemoryS3) -> TestResult<bool> {
    let open = protocol
        .list_uploads("final")
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
    assert_eq!(protocol.objects.lock().await.get("final"), Some(&payload));
    assert!(leaves_nothing_behind(&protocol).await?);
    Ok(())
}

/// 已确认发布后的数据校验失败也必须保留清理授权；只移除本次版本，旧版本和源保留。
#[tokio::test]
async fn opted_in_postpublication_verification_failure_removes_only_our_s3_output() -> TestResult {
    for versioned in [false, true] {
        for size in [1024, 20 * MIB] {
            let (protocol, payload) = seeded(size).await;
            if versioned {
                protocol.set_versioning(Versioning::Enabled);
                protocol
                    .put_version("final", "old", Bytes::from_static(b"valid old version"))
                    .await;
            }
            *protocol.head_failure.lock().await = Some((
                "final".into(),
                S3ProtocolFailure::entry(
                    FailureClass::PermissionDenied,
                    Transience::Permanent,
                    "read-back refused",
                ),
            ));
            let failure = transfer(
                direct_request(&protocol)?
                    .with_metadata_mode(MetadataTransferMode::CallerManaged)
                    .with_failed_data_cleanup(FailedDataCleanup::RemoveOwned),
            )
            .await
            .err()
            .ok_or("read-back failure must stop the transfer")?;
            assert_eq!(failure.phase(), TransferPhase::Verify);
            assert_eq!(protocol.objects.lock().await.get("final"), Some(&payload));
            assert!(failure.has_failed_data_cleanup(), "{failure}");
            *protocol.head_failure.lock().await = None;
            let failure = failure.cleanup_for_policy(TransferPolicy::Direct).await;
            assert!(failure.cleanup_failure().is_none(), "{failure}");
            assert_eq!(
                protocol.objects.lock().await.get("src/final"),
                Some(&payload)
            );
            if versioned {
                assert_eq!(
                    protocol.objects.lock().await.get("final"),
                    Some(&Bytes::from_static(b"valid old version"))
                );
                assert_eq!(protocol.version_deletes().len(), 1);
                assert_ne!(protocol.version_deletes()[0].1, "old");
            } else {
                assert!(!protocol.objects.lock().await.contains_key("final"));
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn default_direct_policy_keeps_published_output_on_failed_read_back() -> TestResult {
    let (protocol, payload) = seeded(1024).await;
    *protocol.head_failure.lock().await = Some((
        "final".into(),
        S3ProtocolFailure::entry(
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "read-back refused",
        ),
    ));
    let failure = transfer(
        direct_request(&protocol)?.with_metadata_mode(MetadataTransferMode::CallerManaged),
    )
    .await
    .err()
    .ok_or("read-back must fail")?;
    assert_eq!(failure.phase(), TransferPhase::Verify);
    assert!(!failure.has_failed_data_cleanup());
    let failure = failure.cleanup_for_policy(TransferPolicy::Direct).await;
    assert!(failure.cleanup_failure().is_none());
    assert_eq!(protocol.objects.lock().await.get("final"), Some(&payload));
    Ok(())
}

/// A large object is one multipart upload on the final key, completed inside `write`; on a
/// versioned bucket the outcome names the version it created.
#[tokio::test]
async fn a_large_direct_transfer_is_one_upload_on_the_final_key() -> TestResult {
    let (protocol, payload) = seeded(20 * MIB).await;
    protocol
        .put_version("final", "v1", Bytes::from_static(b"old"))
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
    assert_eq!(protocol.objects.lock().await.get("final"), Some(&payload));
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
    assert!(!protocol.objects.lock().await.contains_key("final"));
    assert!(leaves_nothing_behind(&protocol).await?);
    Ok(())
}

/// A completion that failed without a definite refusal may still complete on the server: like
/// every failed `Direct` write it reports the final key changed (ADR-0006 C15c applies the same
/// rule to the upload completed at publication), and the upload is aborted inside `write`.
#[tokio::test]
async fn an_ambiguous_direct_completion_failure_reports_the_final_key_changed() -> TestResult {
    let (protocol, _) = seeded(20 * MIB).await;
    *protocol.complete_failure.lock().await = Some(S3ProtocolFailure::session(
        FailureClass::Connectivity,
        Transience::Transient,
        "reset",
    ));
    let failure = transfer(direct_request(&protocol)?)
        .await
        .err()
        .ok_or("a failed completion must fail the transfer")?;
    assert!(failure.final_destination_changed());
    assert!(!failure.has_unpublished_stage());
    assert!(leaves_nothing_behind(&protocol).await?);
    Ok(())
}
