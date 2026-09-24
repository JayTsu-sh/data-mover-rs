use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;

use super::staged::{CifsStageFile, CifsStagedDestination, CifsStagedProtocol};
use crate::model::{
    BackendIdentity, BackendKind, EntryKind, EntryOperationFailure, FailureClass, IdentityStrength,
    Operation, SourceIdentity, StoragePath, Transience,
};
use crate::storage::artifacts::{ArtifactKind, artifact_name};
use crate::storage::roles::{CheckpointRegistration, DeferredCheckpoint};
use crate::storage::{
    ByteStream, DestinationPrepareRequest, FinalDestination, PrepareFact, PrepareRequest,
    PreparedStage, PublishRequest, RecoverRequest, RecoveryIdentity, ResumeMode, SourceDescriptor,
    StagedDestination, StorageRoleFailure, VerifyRequest,
};

#[derive(Default)]
struct MemoryProtocol {
    files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    flushes: Arc<AtomicUsize>,
    fail_flush: Arc<AtomicBool>,
    activity: Arc<WriteActivity>,
    closes: Arc<AtomicUsize>,
    writes: Arc<Mutex<Vec<(u64, usize)>>>,
    fail_rename_after_commit: AtomicBool,
    fail_pointer_delete: AtomicBool,
    /// Every flushed path, in order — the pointer's temporary too. The counters above (`flushes`,
    /// `closes`, `writes`, `activity`) see the stage and final files only.
    flushed: Arc<Mutex<Vec<String>>>,
    /// Handles opened and closed on the pointer or its temporary: every one must be closed.
    pointer_opens: AtomicUsize,
    pointer_closes: Arc<AtomicUsize>,
}

struct MemoryFile {
    path: String,
    files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    flushes: Arc<AtomicUsize>,
    fail_flush: Arc<AtomicBool>,
    activity: Arc<WriteActivity>,
    closes: Arc<AtomicUsize>,
    writes: Arc<Mutex<Vec<(u64, usize)>>>,
    flushed: Arc<Mutex<Vec<String>>>,
    pointer_closes: Arc<AtomicUsize>,
}

#[derive(Default)]
struct WriteActivity {
    delayed: AtomicBool,
    active: AtomicUsize,
    peak: AtomicUsize,
    completed: tokio::sync::Notify,
}

impl MemoryFile {
    /// Whether this is the pointer or its temporary, which the data counters do not see.
    fn is_pointer(&self) -> bool {
        self.path.contains(".pointer")
    }
}

#[async_trait]
impl CifsStageFile for MemoryFile {
    fn maximum_read_chunk(&self) -> u32 {
        4
    }

    fn maximum_write_chunk(&self) -> u32 {
        4
    }

    async fn read_at(&self, offset: u64, count: u32) -> smb_domain::Result<Bytes> {
        let files = self
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let value = files
            .get(&self.path)
            .ok_or_else(|| smb_domain::Error::NotFound(self.path.clone()))?;
        let start = usize::try_from(offset)?;
        let end = start
            .checked_add(count as usize)
            .ok_or_else(|| smb_domain::Error::InvalidArgument("read range overflow".into()))?;
        Ok(Bytes::copy_from_slice(value.get(start..end).ok_or_else(
            || smb_domain::Error::InvalidMessage("short staged read".into()),
        )?))
    }

    async fn write_all_at(&self, offset: u64, bytes: Bytes) -> smb_domain::Result<()> {
        if !self.is_pointer() && self.activity.delayed.load(Ordering::SeqCst) {
            let active = self.activity.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.activity.peak.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            self.activity.active.fetch_sub(1, Ordering::SeqCst);
            self.activity.completed.notify_one();
        }
        if !self.is_pointer() {
            self.writes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((offset, bytes.len()));
        }
        let mut files = self
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let value = files
            .get_mut(&self.path)
            .ok_or_else(|| smb_domain::Error::NotFound(self.path.clone()))?;
        let start = usize::try_from(offset)?;
        let end = start
            .checked_add(bytes.len())
            .ok_or_else(|| smb_domain::Error::InvalidArgument("write range overflow".into()))?;
        value.resize(value.len().max(end), 0);
        value[start..end].copy_from_slice(&bytes);
        Ok(())
    }

    async fn flush(&self) -> smb_domain::Result<()> {
        assert_eq!(
            self.activity.active.load(Ordering::SeqCst),
            0,
            "FLUSH raced an outstanding write"
        );
        if !self.is_pointer() {
            self.flushes.fetch_add(1, Ordering::SeqCst);
        }
        self.flushed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(self.path.clone());
        if self.fail_flush.load(Ordering::SeqCst) {
            return Err(smb_domain::Error::InvalidState(
                "injected FLUSH failure".into(),
            ));
        }
        Ok(())
    }

    async fn close(self: Box<Self>) -> smb_domain::Result<()> {
        if self.is_pointer() {
            self.pointer_closes.fetch_add(1, Ordering::SeqCst);
        } else {
            self.closes.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[async_trait]
impl CifsStagedProtocol for MemoryProtocol {
    async fn create_empty(&self, path: &StoragePath) -> smb_domain::Result<()> {
        let mut files = self
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if files.insert(path.as_str().into(), Vec::new()).is_some() {
            return Err(smb_domain::Error::InvalidState(
                "stage already exists".into(),
            ));
        }
        Ok(())
    }

    async fn open(&self, path: &StoragePath) -> smb_domain::Result<Box<dyn CifsStageFile>> {
        if !self
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(path.as_str())
        {
            return Err(smb_domain::Error::NotFound(path.as_str().into()));
        }
        if path.as_str().contains(".pointer") {
            self.pointer_opens.fetch_add(1, Ordering::SeqCst);
        }
        Ok(Box::new(MemoryFile {
            path: path.as_str().into(),
            files: Arc::clone(&self.files),
            flushes: Arc::clone(&self.flushes),
            fail_flush: Arc::clone(&self.fail_flush),
            activity: self.activity.clone(),
            closes: Arc::clone(&self.closes),
            writes: Arc::clone(&self.writes),
            flushed: Arc::clone(&self.flushed),
            pointer_closes: Arc::clone(&self.pointer_closes),
        }))
    }

    async fn size(&self, path: &StoragePath) -> smb_domain::Result<u64> {
        self.files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(path.as_str())
            .map(|value| value.len() as u64)
            .ok_or_else(|| smb_domain::Error::NotFound(path.as_str().into()))
    }

    async fn rename(
        &self,
        from: &StoragePath,
        to: &StoragePath,
        replace: bool,
    ) -> smb_domain::Result<()> {
        let mut files = self
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !replace && files.contains_key(to.as_str()) {
            return Err(smb_domain::Error::InvalidState("destination exists".into()));
        }
        let value = files
            .remove(from.as_str())
            .ok_or_else(|| smb_domain::Error::NotFound(from.as_str().into()))?;
        files.insert(to.as_str().into(), value);
        if self.fail_rename_after_commit.load(Ordering::SeqCst) {
            return Err(smb_domain::Error::InvalidMessage(
                "scripted lost rename response".into(),
            ));
        }
        Ok(())
    }

    async fn delete(&self, path: &StoragePath) -> smb_domain::Result<()> {
        if path.as_str().ends_with(".pointer")
            && self.fail_pointer_delete.swap(false, Ordering::SeqCst)
        {
            return Err(smb_domain::Error::InvalidState(
                "injected cleanup failure".into(),
            ));
        }
        self.files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(path.as_str())
            .map(|_| ())
            .ok_or_else(|| smb_domain::Error::NotFound(path.as_str().into()))
    }
}

fn identity() -> Result<BackendIdentity, Box<dyn std::error::Error>> {
    Ok(BackendIdentity::new(BackendKind::Cifs, "test-share")?)
}

fn prepare_request(
    identity: &BackendIdentity,
) -> Result<PrepareRequest, Box<dyn std::error::Error>> {
    Ok(PrepareRequest {
        final_destination: FinalDestination::new(StoragePath::new("final.bin")?),
        source: SourceDescriptor::new(
            StoragePath::new("source.bin")?,
            EntryKind::File,
            Some(6),
            SourceIdentity::new(identity.clone(), IdentityStrength::PathScoped, b"source-v1")?,
        ),
        recovery_binding: [7; 32],
    })
}

fn at_destination(request: PrepareRequest, recoverable: bool) -> DestinationPrepareRequest {
    DestinationPrepareRequest::new(request, [3; 32])
        .with_resume(ResumeMode::Restart)
        .with_recoverable(recoverable)
}

/// A fresh stage that records prefix zero in its pointer before any byte is written.
async fn prepare_stage(
    destination: &CifsStagedDestination,
    request: PrepareRequest,
) -> Result<PreparedStage, StorageRoleFailure> {
    destination
        .prepare_at_destination(at_destination(request, true))
        .await
}

/// A fresh stage whose first pointer waits for its first checkpoint.
async fn prepare_ephemeral_stage(
    destination: &CifsStagedDestination,
    request: PrepareRequest,
) -> Result<PreparedStage, StorageRoleFailure> {
    destination
        .prepare_at_destination(at_destination(request, false))
        .await
}

/// Where the stage of `final_path` lives: its deterministic name, beside the final file.
fn stage_file(final_path: &str) -> String {
    let (parent, name) = final_path.rsplit_once('/').unwrap_or(("", final_path));
    let stage = artifact_name(name, ArtifactKind::Stage);
    if parent.is_empty() {
        stage
    } else {
        format!("{parent}/{stage}")
    }
}

/// Every handle opened on the pointer or its temporary was closed again (resource handles: S99).
fn assert_pointer_handles_closed(protocol: &MemoryProtocol) {
    let opened = protocol.pointer_opens.load(Ordering::SeqCst);
    assert!(opened > 0, "no pointer handle was opened");
    assert_eq!(opened, protocol.pointer_closes.load(Ordering::SeqCst));
}

/// How many pointers were written: each is flushed under the pointer's temporary name.
fn pointer_writes(protocol: &MemoryProtocol) -> usize {
    protocol
        .flushed
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .filter(|path| path.ends_with(".pointer.tmp"))
        .count()
}

/// A destination-kept stage never registers a checkpoint where data-mover runs.
struct NeverRegistered;

#[async_trait]
impl CheckpointRegistration for NeverRegistered {
    async fn register(
        &self,
        _stage: &PreparedStage,
        _identity: RecoveryIdentity,
    ) -> Result<(), StorageRoleFailure> {
        panic!("a CIFS stage registered a checkpoint outside the destination")
    }
}

fn deferred(interval_bytes: u64, source_size: u64) -> DeferredCheckpoint {
    DeferredCheckpoint {
        interval_bytes,
        source_size,
        registration: Arc::new(NeverRegistered),
    }
}

fn input(parts: &'static [&'static [u8]]) -> ByteStream {
    Box::pin(stream::iter(
        parts.iter().map(|part| Ok(Bytes::from_static(part))),
    ))
}

/// A final path the share could not hold under exactly that name is refused before anything is
/// created: a backslash adds a directory level on the server, a colon names an alternate data
/// stream, and empty, `.`, `..` or transfer-artifact segments are never a file's name.
#[tokio::test]
async fn final_paths_the_share_would_rename_are_refused() -> Result<(), Box<dyn std::error::Error>>
{
    let protocol = Arc::new(MemoryProtocol::default());
    let identity = identity()?;
    let destination = CifsStagedDestination::new(Arc::clone(&protocol), identity.clone());
    for path in [
        "dir\\final.bin",
        "final.bin:stream",
        "dir/./final.bin",
        "dir/../final.bin",
        "dir/.data-mover-0123.stage",
        ".data-mover-staging/final.bin",
    ] {
        let mut request = prepare_request(&identity)?;
        request.final_destination = FinalDestination::new(StoragePath::new(path)?);
        let refused = prepare_stage(&destination, request).await;
        assert!(
            matches!(
                refused,
                Err(StorageRoleFailure::Entry(ref error))
                    if error.class() == FailureClass::InvalidInput
            ),
            "{path}: {refused:?}"
        );
    }
    assert!(
        protocol
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn staged_lifecycle_flushes_verifies_and_atomically_publishes()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    let identity = identity()?;
    let destination = CifsStagedDestination::new(Arc::clone(&protocol), identity.clone());
    let stage = prepare_stage(&destination, prepare_request(&identity)?).await?;
    let written = destination.write(&stage, input(&[b"abcdef"])).await?;
    assert_eq!(written.persisted_bytes, 6);
    assert_eq!(
        *protocol
            .writes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![(0, 4), (4, 2)]
    );
    assert_eq!(protocol.flushes.load(Ordering::SeqCst), 1);
    // Prefix zero at prepare, then the flushed six bytes.
    assert_eq!(pointer_writes(&protocol), 2);
    assert_eq!(
        destination.observe_checkpoint(&stage).await?.durable_prefix,
        6
    );
    let hash = *blake3::hash(b"abcdef").as_bytes();
    assert_eq!(
        destination
            .verify(
                &stage,
                VerifyRequest {
                    expected_size: 6,
                    expected_blake3: hash,
                    cancel: tokio_util::sync::CancellationToken::new(),
                },
            )
            .await?
            .blake3,
        hash
    );
    destination
        .publish(
            &stage,
            PublishRequest {
                expected_size: 6,
                expected_blake3: Some(hash),
                cancel: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    let files = protocol
        .files
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(files.get("final.bin"), Some(&b"abcdef".to_vec()));
    assert_eq!(files.len(), 1, "no artifact is left");
    // The stage's handles: the write and the verification.
    assert_eq!(protocol.closes.load(Ordering::SeqCst), 2);
    assert_pointer_handles_closed(&protocol);
    Ok(())
}

#[tokio::test]
async fn lost_rename_response_reconciles_committed_final_content()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    let identity = identity()?;
    let destination = CifsStagedDestination::new(Arc::clone(&protocol), identity.clone());
    let stage = prepare_stage(&destination, prepare_request(&identity)?).await?;
    destination.write(&stage, input(&[b"abcdef"])).await?;
    protocol
        .fail_rename_after_commit
        .store(true, Ordering::SeqCst);
    let hash = *blake3::hash(b"abcdef").as_bytes();
    let published = destination
        .publish(
            &stage,
            PublishRequest {
                expected_size: 6,
                expected_blake3: Some(hash),
                cancel: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(
        published.disposition,
        crate::storage::PublicationDisposition::Published
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_publication_leaves_the_stage_and_final_unchanged()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    let identity = identity()?;
    let destination = CifsStagedDestination::new(Arc::clone(&protocol), identity.clone());
    let stage = prepare_stage(&destination, prepare_request(&identity)?).await?;
    destination.write(&stage, input(&[b"abcdef"])).await?;
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    let result = destination
        .publish(
            &stage,
            PublishRequest {
                expected_size: 6,
                expected_blake3: Some(*blake3::hash(b"abcdef").as_bytes()),
                cancel,
            },
        )
        .await;
    let Err(failure) = result else {
        return Err("cancelled publication unexpectedly succeeded".into());
    };
    assert!(!failure.final_destination_changed);
    assert!(
        !protocol
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key("final.bin")
    );
    assert_eq!(
        destination.observe_checkpoint(&stage).await?.durable_prefix,
        6
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_verification_fast_fails_and_preserves_the_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    let identity = identity()?;
    let destination = CifsStagedDestination::new(Arc::clone(&protocol), identity.clone());
    let stage = prepare_stage(&destination, prepare_request(&identity)?).await?;
    destination.write(&stage, input(&[b"abcdef"])).await?;
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    let result = destination
        .verify(
            &stage,
            VerifyRequest {
                expected_size: 6,
                expected_blake3: *blake3::hash(b"abcdef").as_bytes(),
                cancel,
            },
        )
        .await;
    assert!(result.is_err());
    assert_eq!(
        destination.observe_checkpoint(&stage).await?.durable_prefix,
        6
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_input_stops_inflight_write_without_flushing()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    let identity = identity()?;
    let destination = CifsStagedDestination::new(Arc::clone(&protocol), identity.clone());
    let stage = prepare_stage(&destination, prepare_request(&identity)?).await?;
    let cancelled = StorageRoleFailure::Entry(EntryOperationFailure::new(
        StoragePath::new("source.bin")?,
        Operation::Read,
        FailureClass::Cancelled,
        Transience::Transient,
        "source cancelled",
    )?);
    let input: ByteStream = Box::pin(stream::iter(vec![
        Ok(Bytes::from_static(b"abcd")),
        Err(cancelled),
    ]));
    assert!(destination.write(&stage, input).await.is_err());
    assert_eq!(protocol.flushes.load(Ordering::SeqCst), 0);
    assert_eq!(protocol.closes.load(Ordering::SeqCst), 1);
    Ok(())
}

/// A deferred checkpoint writes the stage's first pointer; a new connection resumes from the
/// prefix it records, not from the stage's length, which includes a hole.
#[tokio::test]
async fn periodic_checkpoint_resumes_recorded_prefix_not_sparse_file_length()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    let owner = CifsStagedDestination::new(protocol.clone(), identity()?);
    let mut request = prepare_request(&identity()?)?;
    request.source.size = Some(12);
    let mut stage = prepare_ephemeral_stage(&owner, request.clone()).await?;
    stage.deferred_checkpoint = Some(deferred(4, 12));
    let failure = EntryOperationFailure::new(
        StoragePath::new("source.bin")?,
        Operation::Read,
        FailureClass::Cancelled,
        Transience::Transient,
        "interrupted",
    )?;
    let stream = Box::pin(stream::iter(vec![
        Ok(Bytes::from_static(b"abcd")),
        Err(StorageRoleFailure::Entry(failure)),
    ]));
    assert!(owner.write(&stage, stream).await.is_err());
    assert_eq!(pointer_writes(&protocol), 1);
    // Simulate out-of-order writes after the durable checkpoint: EOF includes a hole.
    protocol
        .files
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get_mut(&stage_file("final.bin"))
        .ok_or("stage missing")?
        .extend_from_slice(b"\0\0\0\0ijkl");
    drop(stage);
    let fresh = CifsStagedDestination::new(protocol.clone(), identity()?);
    let recovered = fresh
        .prepare_at_destination(at_destination(request, true).with_resume(ResumeMode::Discover))
        .await?;
    assert_eq!(recovered.prepare_fact(), PrepareFact::Resumed { bytes: 4 });
    assert_eq!(recovered.write_offset, 4);
    fresh.write(&recovered, input(&[b"efghijkl"])).await?;
    fresh
        .publish(
            &recovered,
            PublishRequest {
                expected_size: 12,
                expected_blake3: Some(*blake3::hash(b"abcdefghijkl").as_bytes()),
                cancel: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await
        .map_err(|e| e.error)?;
    let files = protocol
        .files
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(
        files.get("final.bin").ok_or("final missing")?,
        b"abcdefghijkl"
    );
    assert_eq!(files.len(), 1);
    Ok(())
}

#[tokio::test]
async fn atomic_replace_omits_flush_and_checkpoint_and_colocates_stage()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    let owner = CifsStagedDestination::new(protocol.clone(), identity()?);
    let mut request = prepare_request(&identity()?)?;
    request.final_destination = FinalDestination::new(StoragePath::new("nested/final.bin")?);
    let mut stage = prepare_ephemeral_stage(&owner, request).await?;
    stage.durable_publication = false;
    assert_eq!(
        stage.token.as_ref(),
        stage_file("nested/final.bin").as_bytes()
    );
    owner.write(&stage, input(&[b"abcdef"])).await?;
    assert_eq!(protocol.flushes.load(Ordering::SeqCst), 0);
    assert_eq!(
        protocol
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        1
    );
    owner
        .publish(
            &stage,
            PublishRequest {
                expected_size: 6,
                expected_blake3: None,
                cancel: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await
        .map_err(|e| e.error)?;
    assert!(
        protocol
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key("nested/final.bin")
    );
    Ok(())
}

#[tokio::test]
async fn below_threshold_retains_final_flush_without_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    let owner = CifsStagedDestination::new(protocol.clone(), identity()?);
    assert_eq!(
        owner.automatic_checkpoint_interval_bytes(),
        Some(64 * 1024 * 1024)
    );
    let mut stage = prepare_ephemeral_stage(&owner, prepare_request(&identity()?)?).await?;
    stage.deferred_checkpoint = Some(deferred(6, 6));
    owner.write(&stage, input(&[b"abcdef"])).await?;
    assert_eq!(protocol.flushes.load(Ordering::SeqCst), 1);
    assert_eq!(pointer_writes(&protocol), 0);
    assert_eq!(
        protocol
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        1
    );
    owner.discard(stage).await?;
    assert!(
        protocol
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn failed_flush_does_not_write_a_pointer() -> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    let owner = CifsStagedDestination::new(protocol.clone(), identity()?);
    let mut stage = prepare_ephemeral_stage(&owner, prepare_request(&identity()?)?).await?;
    stage.deferred_checkpoint = Some(deferred(4, 6));
    protocol.fail_flush.store(true, Ordering::SeqCst);
    assert!(owner.write(&stage, input(&[b"abcdef"])).await.is_err());
    assert_eq!(protocol.closes.load(Ordering::SeqCst), 1);
    assert_eq!(pointer_writes(&protocol), 0);
    assert!(!stage.recovery_enabled());
    assert_eq!(
        protocol
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        1
    );
    owner.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn writes_overlap_and_drain_before_flush() -> Result<(), Box<dyn std::error::Error>> {
    for depth in [1, 8, 16, 24] {
        let protocol = Arc::new(MemoryProtocol::default());
        protocol.activity.delayed.store(true, Ordering::SeqCst);
        let owner = CifsStagedDestination::new(protocol.clone(), identity()?)
            .with_write_inflight(std::num::NonZeroUsize::new(depth).ok_or("zero depth")?);
        let mut request = prepare_request(&identity()?)?;
        request.source.size = Some(100);
        let stage = prepare_ephemeral_stage(&owner, request).await?;
        owner.write(&stage, input(&[&[42; 100]])).await?;
        assert_eq!(protocol.activity.peak.load(Ordering::SeqCst), depth);
        assert_eq!(protocol.activity.active.load(Ordering::SeqCst), 0);
        assert_eq!(protocol.flushes.load(Ordering::SeqCst), 1);
        assert_eq!(protocol.closes.load(Ordering::SeqCst), 1);
        owner.discard(stage).await?;
    }
    Ok(())
}

#[tokio::test]
async fn pending_input_does_not_prevent_issued_write_progress()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    protocol.activity.delayed.store(true, Ordering::SeqCst);
    let owner = CifsStagedDestination::new(protocol.clone(), identity()?);
    let stage = prepare_ephemeral_stage(&owner, prepare_request(&identity()?)?).await?;
    let activity = protocol.activity.clone();
    let input = stream::unfold((0, activity), |(index, activity)| async move {
        match index {
            0 => Some((Ok(Bytes::from_static(b"abcd")), (1, activity))),
            1 => {
                activity.completed.notified().await;
                Some((Ok(Bytes::from_static(b"ef")), (2, activity)))
            }
            _ => None,
        }
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        owner.write(&stage, Box::pin(input)),
    )
    .await??;
    assert_eq!(protocol.closes.load(Ordering::SeqCst), 1);
    owner.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn committed_cleanup_is_retryable_and_preserves_final_file()
-> Result<(), Box<dyn std::error::Error>> {
    for fail_cleanup in [true, false] {
        let protocol = Arc::new(MemoryProtocol::default());
        let owner = CifsStagedDestination::new(protocol.clone(), identity()?);
        let stage = prepare_stage(&owner, prepare_request(&identity()?)?).await?;
        owner.write(&stage, input(&[b"abcdef"])).await?;
        protocol
            .fail_pointer_delete
            .store(fail_cleanup, Ordering::SeqCst);
        let result = owner
            .publish(
                &stage,
                PublishRequest {
                    expected_size: 6,
                    expected_blake3: Some(*blake3::hash(b"abcdef").as_bytes()),
                    cancel: tokio_util::sync::CancellationToken::new(),
                },
            )
            .await;
        if fail_cleanup {
            assert!(matches!(result, Err(error) if error.final_destination_changed));
        } else {
            result.map_err(|error| error.error)?;
        }
        owner.discard(stage).await?;
        let files = protocol
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(files.len(), 1);
        assert_eq!(files.get("final.bin").ok_or("final missing")?, b"abcdef");
    }
    Ok(())
}

/// The store-era entry points are gone: CIFS prepares every stage at the destination.
#[tokio::test]
async fn store_era_entry_points_are_unsupported() -> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    let identity = identity()?;
    let destination = CifsStagedDestination::new(Arc::clone(&protocol), identity.clone());
    let unsupported = |result: Result<PreparedStage, StorageRoleFailure>| {
        matches!(
            result,
            Err(StorageRoleFailure::Entry(ref error)) if error.class() == FailureClass::Unsupported
        )
    };
    let request = prepare_request(&identity)?;
    assert!(unsupported(destination.prepare(request.clone()).await));
    assert!(unsupported(
        destination.prepare_ephemeral(request.clone()).await
    ));
    let stage = prepare_stage(&destination, request.clone()).await?;
    let identity_refused = destination.recovery_identity(&stage).await;
    assert!(matches!(
        identity_refused,
        Err(StorageRoleFailure::Entry(ref error)) if error.class() == FailureClass::Unsupported
    ));
    assert!(destination.handoff_recovery(&stage).await.is_err());
    let recovered = destination
        .recover(RecoverRequest {
            identity: RecoveryIdentity::from_bytes(Bytes::from_static(b"old"))?,
            final_destination: request.final_destination,
            source: request.source,
            recovery_binding: request.recovery_binding,
            claim_token: [9; 32],
        })
        .await;
    assert!(unsupported(recovered));
    destination.discard(stage).await?;
    assert!(
        protocol
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
    Ok(())
}

#[path = "metadata_stage_tests.rs"]
mod metadata_stage_tests;

#[path = "at_destination_tests.rs"]
mod at_destination_tests;

#[allow(clippy::unwrap_used)]
#[path = "positioned_tests.rs"]
mod positioned_tests;
