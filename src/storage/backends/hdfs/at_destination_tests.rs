use futures::stream;

use super::*;
use crate::model::{BackendIdentity, BackendKind, IdentityStrength, SourceIdentity};
use crate::storage::artifacts::artifact_name;
use crate::storage::backends::hdfs::contract_tests::MemoryHdfs;
use crate::storage::{
    ByteStream, FinalDestination, PrepareFact, PrepareRequest, PublishRequest, RestartReason,
    SourceDescriptor, StagedDestination,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const IDENTITY: [u8; 32] = [4; 32];
const BINDING: [u8; 32] = [7; 32];
const PAYLOAD: &[u8] = b"0123456789";

fn backend() -> Result<BackendIdentity, Box<dyn std::error::Error>> {
    Ok(BackendIdentity::new(BackendKind::Hdfs, "test-hdfs")?)
}

fn request(
    binding: [u8; 32],
    resume: ResumeMode,
    recoverable: bool,
) -> Result<DestinationPrepareRequest, Box<dyn std::error::Error>> {
    let prepare = PrepareRequest {
        final_destination: FinalDestination::new(StoragePath::new("dir/final.bin")?),
        source: SourceDescriptor::new(
            StoragePath::new("source.bin")?,
            EntryKind::File,
            Some(PAYLOAD.len() as u64),
            SourceIdentity::new(backend()?, IdentityStrength::PathScoped, b"source")?,
        ),
        recovery_binding: binding,
    };
    Ok(DestinationPrepareRequest::new(prepare, IDENTITY)
        .with_resume(resume)
        .with_recoverable(recoverable))
}

fn name(kind: ArtifactKind) -> String {
    format!("dir/{}", artifact_name("final.bin", kind))
}

fn adapter(
    protocol: &Arc<MemoryHdfs>,
) -> Result<HdfsStagedDestination, Box<dyn std::error::Error>> {
    Ok(HdfsStagedDestination::new(Arc::clone(protocol), backend()?))
}

fn input(bytes: &[u8]) -> ByteStream {
    Box::pin(stream::iter([Ok(Bytes::copy_from_slice(bytes))]))
}

async fn publish(
    destination: &HdfsStagedDestination,
    stage: &PreparedStage,
) -> Result<(), crate::storage::PublicationFailure> {
    destination
        .publish(
            stage,
            PublishRequest {
                expected_size: PAYLOAD.len() as u64,
                expected_blake3: Some(*blake3::hash(PAYLOAD).as_bytes()),
                cancel: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await
        .map(|_| ())
}

async fn no_artifacts(protocol: &MemoryHdfs) -> bool {
    for kind in [ArtifactKind::Stage, ArtifactKind::Pointer] {
        if protocol.get(&name(kind)).await.is_some() {
            return false;
        }
    }
    true
}

/// An interrupted stage: its pointer, and `written` bytes of the payload in the stage.
async fn interrupted(protocol: &Arc<MemoryHdfs>, written: usize) -> TestResult {
    let first = adapter(protocol)?;
    let stage = first
        .prepare_at_destination(request(BINDING, ResumeMode::Discover, true)?)
        .await?;
    // The writer needs the whole payload; a short input fails after writing what it got.
    assert!(
        first
            .write(&stage, input(&PAYLOAD[..written]))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn a_fresh_stage_publishes_and_leaves_no_artifact() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let destination = adapter(&protocol)?;
    let stage = destination
        .prepare_at_destination(request(BINDING, ResumeMode::Discover, true)?)
        .await?;
    assert_eq!(stage.prepare_fact(), PrepareFact::Fresh);
    assert!(protocol.get(&name(ArtifactKind::Pointer)).await.is_some());
    destination.write(&stage, input(PAYLOAD)).await?;
    publish(&destination, &stage)
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(
        protocol.get("dir/final.bin").await.as_deref(),
        Some(PAYLOAD)
    );
    assert!(no_artifacts(&protocol).await);
    Ok(())
}

/// A new process resumes from the stage's length after lease recovery — here longer than what
/// the dead writer's pointer recorded, because recovery revealed an acknowledged tail.
#[tokio::test]
async fn a_new_process_resumes_from_the_recovered_length() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    interrupted(&protocol, 4).await?;
    protocol
        .reveal_tail_during_lease_recovery(Bytes::copy_from_slice(&PAYLOAD[..6]))
        .await;
    let second = adapter(&protocol)?;
    let stage = second
        .prepare_at_destination(request(BINDING, ResumeMode::Discover, true)?)
        .await?;
    assert_eq!(protocol.stabilize_calls(), 1);
    assert_eq!(stage.prepare_fact(), PrepareFact::Resumed { bytes: 6 });
    assert_eq!(stage.write_offset, 6);
    second.write(&stage, input(&PAYLOAD[6..])).await?;
    publish(&second, &stage)
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(
        protocol.get("dir/final.bin").await.as_deref(),
        Some(PAYLOAD)
    );
    assert!(no_artifacts(&protocol).await);
    Ok(())
}

/// Lease recovery takes the stage from whoever writes it, so it runs only for a stage this
/// prepare resumes — not before a clean-up.
#[tokio::test]
async fn only_a_resume_recovers_the_lease() -> TestResult {
    for (request, reason) in [
        (
            request(BINDING, ResumeMode::Restart, true)?,
            RestartReason::Requested,
        ),
        (
            request([8; 32], ResumeMode::Discover, true)?,
            RestartReason::BindingChanged,
        ),
    ] {
        let protocol = Arc::new(MemoryHdfs::default());
        interrupted(&protocol, 4).await?;
        let second = adapter(&protocol)?;
        let stage = second.prepare_at_destination(request).await?;
        assert_eq!(stage.prepare_fact(), PrepareFact::Restarted { reason });
        assert_eq!(protocol.stabilize_calls(), 0, "{reason:?}");
        second.discard(stage).await?;
        assert!(no_artifacts(&protocol).await, "{reason:?}");
    }
    Ok(())
}

/// A second process that resumes the stage takes it over: the first can no longer observe,
/// publish or clean it up, and the second publishes.
#[tokio::test]
async fn a_stage_taken_over_by_another_writer_is_left_to_it() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let first = adapter(&protocol)?;
    let held = first
        .prepare_at_destination(request(BINDING, ResumeMode::Discover, true)?)
        .await?;
    first.write(&held, input(PAYLOAD)).await?;
    let second = adapter(&protocol)?;
    let taken = second
        .prepare_at_destination(request(BINDING, ResumeMode::Discover, true)?)
        .await?;
    assert_eq!(
        taken.prepare_fact(),
        PrepareFact::Resumed {
            bytes: PAYLOAD.len() as u64
        }
    );
    assert!(first.observe_checkpoint(&held).await.is_err());
    let refused = publish(&first, &held)
        .await
        .err()
        .ok_or("a taken-over stage must not publish")?;
    assert!(matches!(
        refused.error,
        StorageRoleFailure::Entry(ref entry) if entry.class() == FailureClass::Conflict
    ));
    assert!(!refused.final_destination_changed);
    first.discard(held).await?;
    assert!(protocol.get(&name(ArtifactKind::Stage)).await.is_some());
    publish(&second, &taken)
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(
        protocol.get("dir/final.bin").await.as_deref(),
        Some(PAYLOAD)
    );
    assert!(no_artifacts(&protocol).await);
    Ok(())
}

/// A rename whose reply is lost after the `NameNode` committed it still publishes, and the pointer
/// goes.
#[tokio::test]
async fn a_lost_publication_reply_still_removes_the_pointer() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let destination = adapter(&protocol)?;
    let stage = destination
        .prepare_at_destination(request(BINDING, ResumeMode::Discover, true)?)
        .await?;
    destination.write(&stage, input(PAYLOAD)).await?;
    protocol.fail_rename_after_commit();
    publish(&destination, &stage)
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(
        protocol.get("dir/final.bin").await.as_deref(),
        Some(PAYLOAD)
    );
    assert!(no_artifacts(&protocol).await);
    Ok(())
}

#[tokio::test]
async fn final_paths_with_empty_dot_or_artifact_segments_are_refused() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let destination = adapter(&protocol)?;
    for path in [
        "dir/./final.bin",
        "dir/../final.bin",
        "dir/.data-mover-00.stage",
    ] {
        let mut refused = request(BINDING, ResumeMode::Discover, true)?;
        refused.prepare.final_destination = FinalDestination::new(StoragePath::new(path)?);
        assert!(
            destination.prepare_at_destination(refused).await.is_err(),
            "{path}"
        );
    }
    assert_eq!(protocol.len().await, 0);
    Ok(())
}

/// A lost publication reply counts as done only when the final file holds exactly the expected
/// content: a same-sized older file there is not ours.
#[tokio::test]
async fn a_lost_publication_reply_with_other_content_is_not_published() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let destination = adapter(&protocol)?;
    let stage = destination
        .prepare_at_destination(request(BINDING, ResumeMode::Discover, true)?)
        .await?;
    destination.write(&stage, input(PAYLOAD)).await?;
    // Another writer removed the stage; an older file of the same size is the final file.
    protocol
        .delete(
            &StoragePath::new(name(ArtifactKind::Stage))?,
            EntryKind::File,
        )
        .await?;
    protocol
        .insert("dir/final.bin", Bytes::from_static(b"XXXXXXXXXX"))
        .await;
    let failed = publish(&destination, &stage)
        .await
        .err()
        .ok_or("other content must not count as published")?;
    assert!(failed.final_destination_changed);
    assert!(protocol.get(&name(ArtifactKind::Pointer)).await.is_some());
    Ok(())
}

struct NeverRegistered;

#[async_trait::async_trait]
impl crate::storage::CheckpointRegistration for NeverRegistered {
    async fn register(
        &self,
        _stage: &PreparedStage,
        _identity: crate::storage::RecoveryIdentity,
    ) -> Result<(), StorageRoleFailure> {
        unreachable!("a stage kept at the destination never registers")
    }
}

/// A deferred checkpoint writes the stage's first pointer (after hsync) instead of registering,
/// and a later prepare that finds the stage shorter than that pointer's prefix starts over.
#[tokio::test]
async fn checkpoints_write_the_pointer_and_a_shorter_stage_restarts() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let destination = adapter(&protocol)?;
    let mut stage = destination
        .prepare_at_destination(request(BINDING, ResumeMode::Discover, false)?)
        .await?;
    assert!(protocol.get(&name(ArtifactKind::Pointer)).await.is_none());
    stage.deferred_checkpoint = Some(crate::storage::DeferredCheckpoint {
        interval_bytes: 4,
        source_size: PAYLOAD.len() as u64,
        registration: Arc::new(NeverRegistered),
    });
    destination.write(&stage, input(PAYLOAD)).await?;
    assert!(stage.recovery_enabled());
    let recorded = protocol
        .get(&name(ArtifactKind::Pointer))
        .await
        .ok_or("the checkpoint's pointer")?;
    let recorded = DestinationPointer::decode(&recorded).map_err(|_| "decode")?;
    assert_eq!(recorded.durable_prefix, Some(8));
    drop(stage);

    protocol
        .insert(
            &name(ArtifactKind::Stage),
            Bytes::copy_from_slice(&PAYLOAD[..2]),
        )
        .await;
    let second = adapter(&protocol)?;
    let stage = second
        .prepare_at_destination(request(BINDING, ResumeMode::Discover, true)?)
        .await?;
    assert_eq!(
        stage.prepare_fact(),
        PrepareFact::Restarted {
            reason: RestartReason::StageBehindPointer
        }
    );
    second.discard(stage).await?;
    assert!(no_artifacts(&protocol).await);
    Ok(())
}

/// HDFS keeps its recovery state at the destination: the engine routes it through
/// `prepare_at_destination` and never through the local recovery store (ADR-0006 C12c).
#[test]
fn hdfs_keeps_recovery_at_the_destination() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    assert!(adapter(&protocol)?.recovery_at_destination());
    Ok(())
}

fn is_unsupported<T>(result: &Result<T, StorageRoleFailure>) -> bool {
    matches!(
        result,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Unsupported
    )
}

/// The store-era entry points are gone: HDFS prepares every staged transfer at the destination
/// (ADR-0006 C12d), and none of them touches it.
#[tokio::test]
async fn store_era_entry_points_are_unsupported() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let destination = adapter(&protocol)?;
    let prepare = request(BINDING, ResumeMode::Discover, true)?.prepare;
    assert!(is_unsupported(&destination.prepare(prepare.clone()).await));
    assert!(is_unsupported(
        &destination.prepare_ephemeral(prepare.clone()).await
    ));
    assert_eq!(protocol.len().await, 0);
    let stage = destination
        .prepare_at_destination(request(BINDING, ResumeMode::Discover, true)?)
        .await?;
    assert!(is_unsupported(&destination.recovery_identity(&stage).await));
    assert!(is_unsupported(&destination.handoff_recovery(&stage).await));
    let recovered = destination
        .recover(crate::storage::RecoverRequest {
            identity: crate::storage::RecoveryIdentity::from_bytes(Bytes::from_static(b"old"))?,
            final_destination: prepare.final_destination,
            source: prepare.source,
            recovery_binding: BINDING,
            claim_token: [9; 32],
        })
        .await;
    assert!(is_unsupported(&recovered));
    destination.discard(stage).await?;
    assert_eq!(protocol.len().await, 0);
    Ok(())
}

/// Only a stage kept at the destination, or a direct one, is this adapter's: any other stage (as
/// the store era prepared them) is refused before anything is touched.
#[tokio::test]
async fn a_stage_neither_at_the_destination_nor_direct_is_refused() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let destination = adapter(&protocol)?;
    let final_path = StoragePath::new("dir/final.bin")?;
    let stage_path = artifact_path(&final_path, ArtifactKind::Stage, false)?;
    let stage = PreparedStage::new(
        backend()?,
        FinalDestination::new(final_path),
        stage_token(&stage_path, PAYLOAD.len() as u64),
        BINDING,
        0,
        None,
    );
    let refused = destination
        .write(&stage, input(PAYLOAD))
        .await
        .err()
        .ok_or("a stage not prepared at the destination must not be written")?;
    assert!(matches!(
        refused,
        StorageRoleFailure::Entry(ref entry) if entry.class() == FailureClass::Conflict
    ));
    assert!(destination.discard(stage).await.is_err());
    assert_eq!(protocol.len().await, 0);
    Ok(())
}

fn is_conflict<T>(result: &Result<T, StorageRoleFailure>) -> bool {
    matches!(
        result,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Conflict
    )
}

/// A stage whose token names another path than its kind allows — a direct stage aimed at another
/// file (a direct writer overwrites), or a kept stage aimed at another file's `.stage` — is refused
/// before anything is written, published or removed.
#[tokio::test]
async fn a_stage_token_naming_another_path_is_refused() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol
        .insert("dir/other.bin", Bytes::from_static(b"keep"))
        .await;
    let destination = adapter(&protocol)?;
    let other = StoragePath::new("dir/other.bin")?;
    let foreign = |path: &StoragePath| -> Result<PreparedStage, Box<dyn std::error::Error>> {
        Ok(PreparedStage::new(
            backend()?,
            FinalDestination::new(StoragePath::new("dir/final.bin")?),
            stage_token(path, PAYLOAD.len() as u64),
            BINDING,
            0,
            None,
        ))
    };
    let mut direct = foreign(&other)?;
    direct.direct = true;
    direct.mark_at_destination(PrepareFact::Fresh);
    let mut kept = foreign(&artifact_path(&other, ArtifactKind::Stage, false)?)?;
    kept.mark_at_destination(PrepareFact::Fresh);
    for stage in [&direct, &kept] {
        assert!(is_conflict(&destination.write(stage, input(PAYLOAD)).await));
        assert!(is_conflict(&destination.observe_checkpoint(stage).await));
        let refused = publish(&destination, stage)
            .await
            .err()
            .ok_or("a foreign stage must not publish")?;
        assert!(!refused.final_destination_changed);
    }
    assert!(is_conflict(&destination.discard(kept).await));
    assert_eq!(protocol.len().await, 1);
    assert_eq!(
        protocol.get("dir/other.bin").await.as_deref(),
        Some(b"keep".as_slice())
    );
    Ok(())
}

/// A direct stage writes the final path itself: its length is what it wrote, publication renames
/// nothing, and nothing is left beside it.
#[tokio::test]
async fn a_direct_stage_writes_and_publishes_the_final_path() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let destination = adapter(&protocol)?;
    let prepare = request(BINDING, ResumeMode::Discover, true)?.prepare;
    let mut stage = destination
        .prepare_direct(prepare, tokio_util::sync::CancellationToken::new())
        .await?;
    stage.mark_at_destination(PrepareFact::Fresh);
    destination.write(&stage, input(PAYLOAD)).await?;
    assert_eq!(
        destination.observe_checkpoint(&stage).await?.durable_prefix,
        PAYLOAD.len() as u64
    );
    publish(&destination, &stage)
        .await
        .map_err(|failure| failure.error)?;
    destination.discard(stage).await?;
    assert_eq!(
        protocol.get("dir/final.bin").await.as_deref(),
        Some(PAYLOAD)
    );
    assert_eq!(protocol.len().await, 1);
    Ok(())
}
