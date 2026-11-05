use std::error::Error;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use futures::{StreamExt, stream};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::ExpertDestinationSession;
use crate::model::{
    EntryKind, EntryOperationFailure, FailureClass, IdentityStrength, ObservedEntry, Operation,
    SourceIdentity, StoragePath, Transience,
};
use crate::storage::backends::local::test_destination_storage_with_role;
use crate::storage::{
    PositionedByteStream, PositionedChunk, SourceQosStats, Storage, StorageRoleFailure,
};
use crate::transfer::{
    ExpertDestinationRequest, ExpertSourceEvidence, TransferPhase, TransferPolicy,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const PAYLOAD: &[u8] = b"abcdefghijklmnop";

fn observation(target: &Storage) -> TestResult<ObservedEntry> {
    Ok(ObservedEntry::new(
        StoragePath::new("source")?,
        EntryKind::File,
        Some(PAYLOAD.len() as u64),
        None,
        SourceIdentity::new(
            target.identity().clone(),
            IdentityStrength::PathScoped,
            b"source",
        )?,
    )?)
}

async fn prepare(target: &Storage, policy: TransferPolicy) -> TestResult<ExpertDestinationSession> {
    Ok(ExpertDestinationSession::prepare(
        ExpertDestinationRequest::new(
            observation(target)?,
            4,
            target.clone(),
            StoragePath::new("file")?,
            CancellationToken::new(),
        )
        .with_transfer_policy(policy),
    )
    .await?)
}

fn payload() -> PositionedByteStream {
    Box::pin(stream::iter(PAYLOAD.chunks(4).enumerate().map(
        |(index, chunk)| {
            Ok(PositionedChunk {
                offset: (index * 4) as u64,
                data: Bytes::from_static(chunk),
            })
        },
    )))
}

fn evidence(target: &Storage) -> TestResult<ExpertSourceEvidence> {
    Ok(ExpertSourceEvidence {
        source_size: PAYLOAD.len() as u64,
        blake3: *blake3::hash(PAYLOAD).as_bytes(),
        identity_key: observation(target)?.identity_key(),
        source_qos: SourceQosStats::default(),
    })
}

#[tokio::test]
async fn positioned_input_is_complete_before_publication() -> TestResult {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"old")?;
    let (target, _) = test_destination_storage_with_role(root.path(), "positioned-input")?;
    let write = prepare(&target, TransferPolicy::AtomicReplace)
        .await?
        .write_positioned(payload())
        .await;
    assert_eq!(write.progress.bytes, PAYLOAD.len() as u64);
    assert_eq!(write.progress.blake3, *blake3::hash(PAYLOAD).as_bytes());
    assert!(write.progress.ended);
    assert_eq!(std::fs::read(root.path().join("file"))?, b"old");
    write.outcome?.complete(evidence(&target)?).await?;
    assert_eq!(std::fs::read(root.path().join("file"))?, PAYLOAD);
    Ok(())
}

#[tokio::test]
async fn a_gap_retains_input_facts_and_never_publishes() -> TestResult {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"old")?;
    let (target, _) = test_destination_storage_with_role(root.path(), "positioned-gap")?;
    let input = stream::iter([0, 5].map(|offset| {
        Ok(PositionedChunk {
            offset,
            data: Bytes::from_static(b"abcd"),
        })
    }));
    let write = prepare(&target, TransferPolicy::AtomicReplace)
        .await?
        .write_positioned(Box::pin(input))
        .await;
    assert_eq!(write.progress.bytes, 4);
    assert!(!write.progress.ended);
    let failure = write.outcome.err().ok_or("gap accepted")?;
    assert_eq!(failure.phase(), TransferPhase::Transfer);
    let failure = failure
        .cleanup_for_policy(TransferPolicy::AtomicReplace)
        .await;
    assert!(failure.cleanup_failure().is_none());
    assert_eq!(std::fs::read(root.path().join("file"))?, b"old");
    Ok(())
}

#[tokio::test]
async fn recovered_prefix_is_hashed_but_not_written_again() -> TestResult {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"old")?;
    let (target, role) = test_destination_storage_with_role(root.path(), "positioned-recovery")?;
    role.set_automatic_checkpoint_interval(4);
    let first = prepare(&target, TransferPolicy::Checkpointed).await?;
    let chunks = payload().take(2);
    let write = first.write_positioned(Box::pin(chunks)).await;
    assert_eq!(write.progress.bytes, 8);
    assert!(write.progress.ended);
    drop(write.outcome.err().ok_or("short input accepted")?);
    let resumed = prepare(&target, TransferPolicy::Checkpointed).await?;
    assert!(resumed.write_offset() > 0);
    let write = resumed.write_positioned(payload()).await;
    assert_eq!(write.progress.bytes, PAYLOAD.len() as u64);
    assert_eq!(write.progress.blake3, *blake3::hash(PAYLOAD).as_bytes());
    write.outcome?.complete(evidence(&target)?).await?;
    assert_eq!(std::fs::read(root.path().join("file"))?, PAYLOAD);
    Ok(())
}

#[tokio::test]
async fn pending_positioned_input_is_cancelled_and_joined() -> TestResult {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"old")?;
    let (target, _) = test_destination_storage_with_role(root.path(), "positioned-cancel")?;
    let cancel = CancellationToken::new();
    let session = ExpertDestinationSession::prepare(
        ExpertDestinationRequest::new(
            observation(&target)?,
            4,
            target,
            StoragePath::new("file")?,
            cancel.clone(),
        )
        .with_transfer_policy(TransferPolicy::AtomicReplace),
    )
    .await?;
    let started = Arc::new(Notify::new());
    let reader = Arc::clone(&started);
    let input = stream::poll_fn(move |_| {
        reader.notify_one();
        Poll::Pending
    });
    let writing = tokio::spawn(session.write_positioned(Box::pin(input)));
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .map_err(|_| "positioned input was never polled")?;
    cancel.cancel();
    let write = tokio::time::timeout(Duration::from_secs(2), writing)
        .await
        .map_err(|_| "pending positioned input did not cancel")??;
    assert_eq!(write.progress.bytes, 0);
    assert!(!write.progress.ended);
    let failure = write.outcome.err().ok_or("cancelled input accepted")?;
    let failure = failure
        .cleanup_for_policy(TransferPolicy::AtomicReplace)
        .await;
    assert!(failure.cleanup_failure().is_none());
    assert_eq!(std::fs::read(root.path().join("file"))?, b"old");
    assert_eq!(std::fs::read_dir(root.path())?.count(), 1);
    Ok(())
}

#[tokio::test]
async fn a_typed_input_error_survives_the_destination_write() -> TestResult {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"old")?;
    let (target, _) = test_destination_storage_with_role(root.path(), "positioned-error")?;
    let cause = StorageRoleFailure::Entry(EntryOperationFailure::new(
        StoragePath::new("file")?,
        Operation::Write,
        FailureClass::Protocol,
        Transience::Permanent,
        "input channel failed",
    )?);
    let input = stream::iter([Err(cause.clone())]);
    let write = prepare(&target, TransferPolicy::AtomicReplace)
        .await?
        .write_positioned(Box::pin(input))
        .await;
    assert_eq!(write.progress.bytes, 0);
    assert!(!write.progress.ended);
    let failure = write.outcome.err().ok_or("input error accepted")?;
    assert_eq!(
        failure
            .source()
            .and_then(|source| source.downcast_ref::<StorageRoleFailure>()),
        Some(&cause)
    );
    let failure = failure
        .cleanup_for_policy(TransferPolicy::AtomicReplace)
        .await;
    assert!(failure.cleanup_failure().is_none());
    assert_eq!(std::fs::read(root.path().join("file"))?, b"old");
    Ok(())
}
