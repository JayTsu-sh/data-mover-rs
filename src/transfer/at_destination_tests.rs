//! The engine against a destination that keeps its recovery state beside the final file (ADR-0006
//! C7): an in-memory destination built on the real artifact names, pointer codec and discovery
//! driver. No backend has moved yet, so this is the only thing that drives the path.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

use crate::model::{
    BackendIdentity, BackendKind, EntryOperationFailure, FailureClass, Operation, StoragePath,
    Transience,
};
use crate::storage::artifacts::{ArtifactKind, sibling_artifact};
use crate::storage::backends::local::test_source_storage;
use crate::storage::discovery::{DestinationArtifacts, discover};
use crate::storage::pointer::DestinationPointer;
use crate::storage::{
    BackendCapabilities, ByteStream, CapabilityAvailability, CheckpointObservation,
    DestinationPrepareRequest, PrepareRequest, PreparedStage, PublicationDisposition,
    PublicationEvidence, PublicationFailure, PublishRequest, RecoverRequest, RecoveryIdentity,
    StagedDestination, Storage, StorageRoleFailure, UnsupportedReason, VerificationEvidence,
    VerifyRequest, WriteEvidence,
};
use crate::transfer::{
    InflightLimits, PrepareFact, RestartReason, TransferIdentity, TransferPhase, TransferPolicy,
    TransferRequest, transfer,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const SIZE: usize = 256 * 1024;
const CHECKPOINT: u64 = 64 * 1024;

fn failure(path: &StoragePath, operation: Operation, class: FailureClass) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            operation,
            class,
            Transience::Transient,
            "memory destination",
        )
        .unwrap_or_else(|_| unreachable!("the static diagnostic is valid")),
    )
}

/// Final files, stages and pointers by path, as the destination would hold them.
struct MemoryDestination {
    identity: BackendIdentity,
    files: Mutex<HashMap<StoragePath, Vec<u8>>>,
    /// The transfer identity each stage's pointer records.
    identities: Mutex<HashMap<StoragePath, [u8; 32]>>,
    /// Fail a write once the stage holds this many bytes.
    fail_at: Mutex<Option<u64>>,
    /// Bytes written by `write`, over all transfers.
    written: AtomicU64,
    /// Calls into the recovery-store prepare / recover path, which must never happen.
    legacy_calls: AtomicUsize,
}

impl MemoryDestination {
    fn new(name: &str) -> TestResult<Arc<Self>> {
        Ok(Arc::new(Self {
            identity: BackendIdentity::new("s3".parse::<BackendKind>()?, name)?,
            files: Mutex::default(),
            identities: Mutex::default(),
            fail_at: Mutex::default(),
            written: AtomicU64::new(0),
            legacy_calls: AtomicUsize::new(0),
        }))
    }

    fn files(&self) -> std::sync::MutexGuard<'_, HashMap<StoragePath, Vec<u8>>> {
        self.files.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn artifact(
        final_path: &StoragePath,
        kind: ArtifactKind,
    ) -> Result<StoragePath, StorageRoleFailure> {
        sibling_artifact(final_path, kind)
            .ok_or_else(|| failure(final_path, Operation::Prepare, FailureClass::InvalidInput))
    }

    fn write_pointer(&self, final_path: &StoragePath, binding: [u8; 32], prefix: u64) {
        let identity = self
            .identities
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(final_path)
            .copied()
            .unwrap_or_default();
        let pointer = DestinationPointer {
            binding,
            transfer_identity: identity,
            durable_prefix: Some(prefix),
            extension: Bytes::new(),
        };
        if let (Ok(path), Ok(bytes)) = (
            Self::artifact(final_path, ArtifactKind::Pointer),
            pointer.encode(),
        ) {
            self.files().insert(path, bytes);
        }
    }

    fn legacy(&self, path: &StoragePath) -> StorageRoleFailure {
        self.legacy_calls.fetch_add(1, Ordering::SeqCst);
        failure(path, Operation::Prepare, FailureClass::Unsupported)
    }

    fn storage(self: &Arc<Self>) -> TestResult<Storage> {
        let unavailable =
            CapabilityAvailability::Unsupported(UnsupportedReason::new("not supplied")?);
        Ok(Storage::connected(
            self.identity.clone(),
            BackendCapabilities::new(
                unavailable.clone(),
                CapabilityAvailability::Supported,
                unavailable.clone(),
                unavailable,
            ),
            None,
            Some(Arc::clone(self) as Arc<dyn StagedDestination>),
            None,
            None,
            None,
        )?)
    }
}

#[async_trait]
impl DestinationArtifacts for MemoryDestination {
    async fn read_pointer(
        &self,
        final_path: &StoragePath,
        limit: usize,
    ) -> Result<Option<Vec<u8>>, StorageRoleFailure> {
        let path = Self::artifact(final_path, ArtifactKind::Pointer)?;
        Ok(self.files().get(&path).map(|bytes| {
            let mut bytes = bytes.clone();
            bytes.truncate(limit);
            bytes
        }))
    }
    async fn observe_stage(
        &self,
        final_path: &StoragePath,
    ) -> Result<Option<u64>, StorageRoleFailure> {
        let path = Self::artifact(final_path, ArtifactKind::Stage)?;
        Ok(self.files().get(&path).map(|bytes| bytes.len() as u64))
    }
    async fn remove_pointer(&self, final_path: &StoragePath) -> Result<(), StorageRoleFailure> {
        let path = Self::artifact(final_path, ArtifactKind::Pointer)?;
        self.files().remove(&path);
        Ok(())
    }
    async fn remove_stage(&self, final_path: &StoragePath) -> Result<(), StorageRoleFailure> {
        let path = Self::artifact(final_path, ArtifactKind::Stage)?;
        self.files().remove(&path);
        Ok(())
    }
}

#[async_trait]
impl StagedDestination for MemoryDestination {
    fn recovery_at_destination(&self) -> bool {
        true
    }

    async fn prepare_at_destination(
        &self,
        request: DestinationPrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        let final_path = request.prepare.final_destination.path().clone();
        let found = discover(self, &request).await?;
        let stage_path = Self::artifact(&final_path, ArtifactKind::Stage)?;
        self.identities
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(final_path.clone(), request.transfer_identity);
        let offset = if let Some(point) = &found.resume {
            let mut files = self.files();
            if let Some(stage) = files.get_mut(&stage_path) {
                stage.truncate(usize::try_from(point.prefix).unwrap_or(usize::MAX));
            }
            point.prefix
        } else {
            self.files().insert(stage_path, Vec::new());
            if request.recoverable {
                self.write_pointer(&final_path, request.prepare.recovery_binding, 0);
            }
            0
        };
        let mut stage = PreparedStage::new(
            self.identity.clone(),
            request.prepare.final_destination.clone(),
            Bytes::new(),
            request.prepare.recovery_binding,
            offset,
            None,
        );
        if !request.recoverable && found.resume.is_none() {
            stage = stage.disable_recovery();
        }
        stage.mark_at_destination(found.fact);
        Ok(stage)
    }

    async fn prepare(&self, request: PrepareRequest) -> Result<PreparedStage, StorageRoleFailure> {
        Err(self.legacy(request.final_destination.path()))
    }
    async fn prepare_ephemeral(
        &self,
        request: PrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        Err(self.legacy(request.final_destination.path()))
    }
    async fn recovery_identity(
        &self,
        stage: &PreparedStage,
    ) -> Result<RecoveryIdentity, StorageRoleFailure> {
        Err(self.legacy(stage.final_destination.path()))
    }
    async fn recover(&self, request: RecoverRequest) -> Result<PreparedStage, StorageRoleFailure> {
        Err(self.legacy(request.final_destination.path()))
    }

    async fn write(
        &self,
        stage: &PreparedStage,
        mut input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        let final_path = stage.final_destination.path().clone();
        let stage_path = Self::artifact(&final_path, ArtifactKind::Stage)?;
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            let length = {
                let mut files = self.files();
                let bytes = files.entry(stage_path.clone()).or_default();
                bytes.extend_from_slice(&chunk);
                bytes.len() as u64
            };
            self.written.fetch_add(chunk.len() as u64, Ordering::SeqCst);
            if stage.recovery_enabled() {
                self.write_pointer(
                    &final_path,
                    stage.recovery_binding(),
                    length / CHECKPOINT * CHECKPOINT,
                );
            }
            let fail_at = *self.fail_at.lock().unwrap_or_else(PoisonError::into_inner);
            if fail_at.is_some_and(|limit| length >= limit) {
                return Err(failure(
                    &final_path,
                    Operation::Write,
                    FailureClass::Connectivity,
                ));
            }
        }
        let length = self.files().get(&stage_path).map_or(0, Vec::len) as u64;
        Ok(WriteEvidence {
            persisted_bytes: length,
        })
    }

    async fn observe_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<CheckpointObservation, StorageRoleFailure> {
        let stage_path = Self::artifact(stage.final_destination.path(), ArtifactKind::Stage)?;
        Ok(CheckpointObservation {
            durable_prefix: self.files().get(&stage_path).map_or(0, Vec::len) as u64,
        })
    }

    async fn verify(
        &self,
        stage: &PreparedStage,
        _request: VerifyRequest,
    ) -> Result<VerificationEvidence, StorageRoleFailure> {
        let stage_path = Self::artifact(stage.final_destination.path(), ArtifactKind::Stage)?;
        let bytes = self.files().get(&stage_path).cloned().unwrap_or_default();
        Ok(VerificationEvidence {
            verified_bytes: bytes.len() as u64,
            blake3: *blake3::hash(&bytes).as_bytes(),
        })
    }

    async fn publish(
        &self,
        stage: &PreparedStage,
        _request: PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        let final_path = stage.final_destination.path().clone();
        let stage_path = Self::artifact(&final_path, ArtifactKind::Stage).map_err(|error| {
            PublicationFailure {
                error,
                final_destination_changed: false,
            }
        })?;
        let mut files = self.files();
        let bytes = files.remove(&stage_path).unwrap_or_default();
        files.insert(final_path.clone(), bytes);
        if let Some(pointer) = sibling_artifact(&final_path, ArtifactKind::Pointer) {
            files.remove(&pointer);
        }
        Ok(PublicationEvidence {
            final_destination: final_path,
            disposition: PublicationDisposition::Published,
        })
    }

    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        self.remove_pointer(stage.final_destination.path()).await?;
        self.remove_stage(stage.final_destination.path()).await
    }
}

fn payload(seed: u8) -> Vec<u8> {
    (0..SIZE)
        .map(|index| u8::try_from((index + usize::from(seed)) % 251).unwrap_or(0))
        .collect()
}

fn request(
    source_root: &Path,
    destination: &Arc<MemoryDestination>,
) -> TestResult<TransferRequest> {
    let (source, _) = test_source_storage(source_root, "at-destination-source")?;
    Ok(TransferRequest::new(
        source,
        StoragePath::new("source.bin")?,
        destination.storage()?,
        StoragePath::new("out/final.bin")?,
        InflightLimits::new(2, 2 * 64 * 1024, 2)?,
        CancellationToken::new(),
    )
    .with_transfer_policy(TransferPolicy::Checkpointed))
}

fn source_root(seed: u8) -> TestResult<tempfile::TempDir> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("source.bin"), payload(seed))?;
    Ok(root)
}

fn final_bytes(destination: &MemoryDestination) -> TestResult<Vec<u8>> {
    Ok(destination
        .files()
        .get(&StoragePath::new("out/final.bin")?)
        .cloned()
        .unwrap_or_default())
}

/// Interrupts a transfer once the stage holds `bytes`; the failure is dropped, as a restarted
/// process would lose it.
async fn interrupt(source: &Path, destination: &Arc<MemoryDestination>, bytes: u64) -> TestResult {
    *destination
        .fail_at
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(bytes);
    let failed = transfer(request(source, destination)?).await;
    *destination
        .fail_at
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = None;
    let Err(failure) = failed else {
        return Err("the interrupted transfer succeeded".into());
    };
    assert_eq!(failure.phase(), TransferPhase::Transfer);
    Ok(())
}

#[tokio::test]
async fn a_fresh_transfer_prepares_at_the_destination_only() -> TestResult {
    let source = source_root(1)?;
    let destination = MemoryDestination::new("fresh")?;
    let outcome = transfer(request(source.path(), &destination)?).await?;
    assert_eq!(outcome.prepare, PrepareFact::Fresh);
    assert_eq!(outcome.reused_bytes, 0);
    assert_eq!(final_bytes(&destination)?, payload(1));
    assert_eq!(destination.legacy_calls.load(Ordering::SeqCst), 0);
    // Published: no artifact is left beside the final file.
    assert_eq!(destination.files().len(), 1);
    Ok(())
}

#[tokio::test]
async fn an_interrupted_transfer_resumes_from_the_pointer_left_at_the_destination() -> TestResult {
    let source = source_root(2)?;
    let destination = MemoryDestination::new("resume")?;
    interrupt(source.path(), &destination, 3 * CHECKPOINT).await?;
    let before = destination.written.load(Ordering::SeqCst);

    let outcome = transfer(request(source.path(), &destination)?).await?;

    let PrepareFact::Resumed { bytes } = outcome.prepare else {
        return Err(format!("expected a resume, got {:?}", outcome.prepare).into());
    };
    assert!(bytes >= CHECKPOINT, "resumed from {bytes}");
    assert_eq!(outcome.reused_bytes, bytes);
    assert_eq!(
        destination.written.load(Ordering::SeqCst) - before,
        SIZE as u64 - bytes
    );
    assert_eq!(final_bytes(&destination)?, payload(2));
    assert_eq!(destination.legacy_calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn a_changed_source_restarts_instead_of_resuming() -> TestResult {
    let source = source_root(3)?;
    let destination = MemoryDestination::new("changed")?;
    interrupt(source.path(), &destination, 2 * CHECKPOINT).await?;
    // Same size, other bytes: the binding (content version) changes.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    std::fs::write(source.path().join("source.bin"), payload(4))?;

    let outcome = transfer(request(source.path(), &destination)?).await?;

    assert_eq!(
        outcome.prepare,
        PrepareFact::Restarted {
            reason: RestartReason::BindingChanged
        }
    );
    assert_eq!(final_bytes(&destination)?, payload(4));
    Ok(())
}

#[tokio::test]
async fn another_transfers_stage_is_cleaned_not_resumed() -> TestResult {
    let source = source_root(5)?;
    let destination = MemoryDestination::new("other")?;
    interrupt(source.path(), &destination, 2 * CHECKPOINT).await?;

    let outcome = transfer(
        request(source.path(), &destination)?
            .with_identity_override(TransferIdentity::from_label("another job")?),
    )
    .await?;

    assert_eq!(
        outcome.prepare,
        PrepareFact::Restarted {
            reason: RestartReason::OtherTransfer
        }
    );
    assert_eq!(final_bytes(&destination)?, payload(5));
    Ok(())
}

#[tokio::test]
async fn an_atomic_replace_cleans_up_what_it_finds() -> TestResult {
    let source = source_root(6)?;
    let destination = MemoryDestination::new("atomic")?;
    interrupt(source.path(), &destination, 2 * CHECKPOINT).await?;

    let outcome = transfer(
        request(source.path(), &destination)?.with_transfer_policy(TransferPolicy::AtomicReplace),
    )
    .await?;

    assert_eq!(
        outcome.prepare,
        PrepareFact::Restarted {
            reason: RestartReason::Requested
        }
    );
    assert_eq!(final_bytes(&destination)?, payload(6));
    Ok(())
}

/// A resumed stage whose bytes no longer verify is cleaned up in place: its pointer would otherwise
/// resume it on every retry, across restarts.
#[tokio::test]
async fn a_resumed_stage_that_fails_verification_is_cleaned_up() -> TestResult {
    let source = source_root(7)?;
    let destination = MemoryDestination::new("stale")?;
    interrupt(source.path(), &destination, 2 * CHECKPOINT).await?;
    let stage = sibling_artifact(&StoragePath::new("out/final.bin")?, ArtifactKind::Stage)
        .ok_or("stage path")?;
    if let Some(bytes) = destination.files().get_mut(&stage) {
        bytes[0] ^= 0xff;
    }

    let Err(failure) = transfer(request(source.path(), &destination)?).await else {
        return Err("a corrupted stage was published".into());
    };
    assert_eq!(failure.phase(), TransferPhase::Verify);
    assert!(!failure.has_unpublished_stage());
    assert!(!destination.files().contains_key(&stage));

    let outcome = transfer(request(source.path(), &destination)?).await?;
    assert_eq!(outcome.prepare, PrepareFact::Fresh);
    assert_eq!(final_bytes(&destination)?, payload(7));
    Ok(())
}

/// One transfer at a time writes a final file: a second one fails at prepare with a transient
/// conflict for as long as the first holds its stage — including inside a failure — and goes
/// through once the stage is gone.
#[tokio::test]
async fn a_concurrent_transfer_of_the_same_file_waits_its_turn() -> TestResult {
    let source = source_root(8)?;
    let destination = MemoryDestination::new("guard")?;
    *destination
        .fail_at
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(2 * CHECKPOINT);
    let Err(held) = transfer(request(source.path(), &destination)?).await else {
        return Err("the interrupted transfer succeeded".into());
    };
    *destination
        .fail_at
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = None;
    assert!(held.has_unpublished_stage());

    let Err(refused) = transfer(request(source.path(), &destination)?).await else {
        return Err("a second writer was admitted".into());
    };
    assert_eq!(refused.phase(), TransferPhase::Prepare);
    let role = std::error::Error::source(&refused)
        .and_then(|cause| cause.downcast_ref::<StorageRoleFailure>());
    assert!(
        matches!(role, Some(StorageRoleFailure::Entry(entry))
            if entry.class() == FailureClass::Conflict && entry.transience() == Transience::Transient),
        "{refused:?}"
    );

    drop(held);
    let outcome = transfer(request(source.path(), &destination)?).await?;
    assert!(matches!(outcome.prepare, PrepareFact::Resumed { .. }));
    assert_eq!(final_bytes(&destination)?, payload(8));
    Ok(())
}

/// Runs the two expert halves over the in-memory destination, in one process.
async fn expert_run(
    source_root: &Path,
    destination: &Arc<MemoryDestination>,
) -> TestResult<Result<crate::transfer::TransferOutcome, crate::transfer::TransferFailure>> {
    use crate::model::ObservedEntry;
    use crate::storage::PreflightPolicy;
    use crate::transfer::{
        ExpertDestinationRequest, ExpertDestinationSession, ExpertSourceRequest,
        ExpertSourceSession,
    };
    let (source, _) = test_source_storage(source_root, "at-destination-source")?;
    let path = StoragePath::new("source.bin")?;
    let described = source
        .read_source(&PreflightPolicy::production())?
        .describe(&path)
        .await?;
    let observation = ObservedEntry::new(
        described.path,
        described.kind,
        described.size,
        None,
        described.source_identity,
    )?;
    let limits = InflightLimits::new(2, 2 * 64 * 1024, 2)?;
    let source_half = ExpertSourceSession::open(ExpertSourceRequest::new(
        source,
        observation.clone(),
        limits,
        CancellationToken::new(),
    ))
    .await?;
    let prepared = ExpertDestinationSession::prepare(
        ExpertDestinationRequest::new(
            observation,
            source_half.offer().maximum_chunk_bytes,
            destination.storage()?,
            StoragePath::new("out/final.bin")?,
            limits,
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Checkpointed),
    )
    .await;
    let session = match prepared {
        Ok(session) => session,
        Err(failure) => return Ok(Err(failure)),
    };
    let mut payload = source_half.stream_from(session.write_offset())?;
    let (sender, receiver) = futures::channel::mpsc::unbounded();
    let pump = async move {
        while let Some(chunk) = payload.next_chunk().await? {
            if sender.unbounded_send(Ok(chunk)).is_err() {
                break;
            }
        }
        drop(sender);
        payload.finish().await
    };
    let (written, evidence) = futures::join!(session.write(Box::pin(receiver)), pump);
    let transferred = match written {
        Ok(transferred) => transferred,
        Err(failure) => return Ok(Err(failure)),
    };
    Ok(transferred.complete(evidence?).await)
}

/// The expert destination half cleans a stale resumed stage in place too (the review's finding:
/// before, it kept the stage, whose pointer resumed it on every retry).
#[tokio::test]
async fn the_expert_half_also_cleans_up_a_stale_resumed_stage() -> TestResult {
    let source = source_root(9)?;
    let destination = MemoryDestination::new("expert-stale")?;
    *destination
        .fail_at
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(2 * CHECKPOINT);
    assert!(expert_run(source.path(), &destination).await?.is_err());
    *destination
        .fail_at
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = None;
    let stage = sibling_artifact(&StoragePath::new("out/final.bin")?, ArtifactKind::Stage)
        .ok_or("stage path")?;
    if let Some(bytes) = destination.files().get_mut(&stage) {
        bytes[0] ^= 0xff;
    }

    let Err(failure) = expert_run(source.path(), &destination).await? else {
        return Err("a corrupted stage was published".into());
    };
    assert_eq!(failure.phase(), TransferPhase::Verify);
    assert!(!failure.has_unpublished_stage());
    assert!(!destination.files().contains_key(&stage));

    let outcome = expert_run(source.path(), &destination).await??;
    assert_eq!(outcome.prepare, PrepareFact::Fresh);
    assert_eq!(final_bytes(&destination)?, payload(9));
    Ok(())
}
