//! Destination-resident recovery for the HDFS stage (ADR-0006 C12). The stage and the pointer that
//! records its binding and durable prefix sit beside the final file under deterministic names.
//!
//! The pointer records the last hsync'd prefix, as a lower bound. A resume continues from the
//! stage's length after forced lease recovery instead (`continues_from_stage`): once recovery has
//! closed the file, its length is at least the hsync'd prefix, and the bytes past it were
//! acknowledged by the whole pipeline — proven, and in order. Lease recovery also fences the dead
//! writer: it cannot add another byte.
//!
//! The HDFS lease exists only while a writer has the file open, and belongs to the process rather
//! than the transfer, so it cannot stand in for a claim across verify and publication. As on
//! NFS and CIFS, every prepare draws a nonce and records it in the pointer (a resume rewrites the
//! pointer with its own); before it rewrites the pointer, publishes, or cleans up, a stage reads the
//! pointer back, and another nonce there means another writer took the stage over (`Conflict`). The
//! fence is a check, not a lock, and a stage prepared without a pointer has nothing to fence with
//! until its first checkpoint; both are two writers of one key, which the caller contract excludes.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use uuid::Uuid;

use super::protocol::{HdfsProtocol, entry_failure};
use super::staged::{
    HdfsStageState, HdfsStagedDestination, hash_file, publication_failure,
    publication_may_have_changed, stage_token,
};
use crate::model::{EntryKind, FailureClass, Operation, StoragePath, Transience};
use crate::storage::artifacts::{
    ArtifactKind, artifact_temporary_name, is_artifact_path, sibling_artifact,
};
use crate::storage::discovery::{DestinationArtifacts, discover};
use crate::storage::pointer::{DestinationPointer, MAX_POINTER_BYTES};
use crate::storage::{
    DestinationPrepareRequest, PreparedStage, PublicationDisposition, PublicationEvidence,
    PublicationFailure, PublishRequest, ResumeMode, StorageRoleFailure,
};

/// The tag every HDFS pointer's extension starts with; the fence nonce follows it.
const POINTER_TAG: &[u8; 8] = b"DMHSTG01";
const NONCE_BYTES: usize = 16;

/// What a destination-kept HDFS stage knows about its own pointer.
pub(super) struct Fence {
    nonce: [u8; NONCE_BYTES],
    transfer_identity: [u8; 32],
    /// Whether this stage has written a pointer; a pointer gone after that was removed by
    /// another writer.
    written: AtomicBool,
}

impl Fence {
    fn new(transfer_identity: [u8; 32]) -> Self {
        Self {
            nonce: *Uuid::new_v4().as_bytes(),
            transfer_identity,
            written: AtomicBool::new(false),
        }
    }

    fn extension(&self) -> Bytes {
        let mut extension = Vec::with_capacity(POINTER_TAG.len() + NONCE_BYTES);
        extension.extend_from_slice(POINTER_TAG);
        extension.extend_from_slice(&self.nonce);
        Bytes::from(extension)
    }
}

fn failure(path: &StoragePath, operation: Operation, class: FailureClass) -> StorageRoleFailure {
    entry_failure(path, operation, class, Transience::Permanent)
}

fn is_not_found(error: &StorageRoleFailure) -> bool {
    matches!(error, StorageRoleFailure::Entry(entry) if entry.class() == FailureClass::NotFound)
}

/// Whether `stage` keeps its state at the destination (`.stage` + `.pointer`). A direct stage
/// writes the final path itself; the engine marks it at-destination too, so it is excluded here.
pub(super) const fn resident(stage: &PreparedStage) -> bool {
    stage.at_destination && !stage.direct
}

/// A final path HDFS holds under exactly that name, with its artifacts beside it: no empty, `.` or
/// `..` segment and no transfer-artifact segment.
pub(super) fn validate_final(path: &StoragePath) -> Result<(), StorageRoleFailure> {
    let text = path.as_str();
    if text.is_empty()
        || text.split('/').any(|part| matches!(part, "" | "." | ".."))
        || is_artifact_path(text)
    {
        return Err(failure(
            path,
            Operation::Prepare,
            FailureClass::InvalidInput,
        ));
    }
    Ok(())
}

/// The path of one artifact of a final file, beside it.
pub(super) fn artifact_path(
    final_path: &StoragePath,
    kind: ArtifactKind,
    temporary: bool,
) -> Result<StoragePath, StorageRoleFailure> {
    let invalid = || failure(final_path, Operation::Prepare, FailureClass::InvalidInput);
    let plain = sibling_artifact(final_path, kind).ok_or_else(invalid)?;
    if !temporary {
        return Ok(plain);
    }
    let (parent, name) = final_path
        .as_str()
        .rsplit_once('/')
        .unwrap_or(("", final_path.as_str()));
    let temporary = artifact_temporary_name(name, kind);
    let path = if parent.is_empty() {
        temporary
    } else {
        format!("{parent}/{temporary}")
    };
    StoragePath::new(path).map_err(|_| invalid())
}

/// Whether a pointer is one an HDFS stage wrote: tagged, fenced, with a durable prefix.
fn accepts(pointer: &DestinationPointer) -> bool {
    pointer.durable_prefix.is_some()
        && pointer.extension.len() == POINTER_TAG.len() + NONCE_BYTES
        && pointer.extension.starts_with(POINTER_TAG)
}

fn fence(stage: &PreparedStage) -> Result<&Fence, StorageRoleFailure> {
    stage
        .backend_state
        .as_ref()
        .and_then(|state| state.downcast_ref::<HdfsStageState>())
        .map(|state| &state.fence)
        .ok_or_else(|| {
            failure(
                stage.final_destination.path(),
                Operation::Observe,
                FailureClass::Internal,
            )
        })
}

/// Reads a regular file whole, up to `limit` bytes; `None` if it is absent. Something that is not
/// a file at an artifact name is a conflict.
async fn read_regular(
    protocol: &dyn HdfsProtocol,
    path: &StoragePath,
    limit: usize,
) -> Result<Option<Vec<u8>>, StorageRoleFailure> {
    let facts = match protocol.stat(path).await {
        Ok(facts) => facts,
        Err(error) if is_not_found(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    if facts.kind != EntryKind::File {
        return Err(failure(path, Operation::Observe, FailureClass::Conflict));
    }
    let size = facts.size.unwrap_or(0).min(limit as u64);
    if size == 0 {
        return Ok(Some(Vec::new()));
    }
    match protocol.read_range(path, 0..size).await {
        Ok(bytes) => Ok(Some(bytes.to_vec())),
        Err(error) if is_not_found(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

async fn delete_if_present(
    protocol: &dyn HdfsProtocol,
    path: &StoragePath,
) -> Result<(), StorageRoleFailure> {
    match protocol.delete(path, EntryKind::File).await {
        Err(error) if !is_not_found(&error) => Err(error),
        _ => Ok(()),
    }
}

/// A final file's artifacts, as the discovery driver looks at them. Lease recovery — which takes
/// the stage from whoever writes it — runs only for a stage this prepare is going to resume.
struct HdfsArtifacts<'a> {
    protocol: &'a dyn HdfsProtocol,
    stage: StoragePath,
    pointer: StoragePath,
    pointer_temporary: StoragePath,
    request: &'a DestinationPrepareRequest,
    /// The pointer as `read_pointer` found it, for `observe_stage` to decide whether to recover.
    found_pointer: Mutex<Option<Vec<u8>>>,
}

impl HdfsArtifacts<'_> {
    fn resumable(&self) -> bool {
        if self.request.resume != ResumeMode::Discover {
            return false;
        }
        let found = self
            .found_pointer
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        found
            .as_deref()
            .and_then(|bytes| DestinationPointer::decode(bytes).ok())
            .is_some_and(|pointer| {
                accepts(&pointer)
                    && pointer.transfer_identity == self.request.transfer_identity
                    && pointer.binding == self.request.prepare.recovery_binding
            })
    }
}

#[async_trait]
impl DestinationArtifacts for HdfsArtifacts<'_> {
    async fn read_pointer(
        &self,
        _final_path: &StoragePath,
        limit: usize,
    ) -> Result<Option<Vec<u8>>, StorageRoleFailure> {
        let found = read_regular(self.protocol, &self.pointer, limit).await?;
        self.found_pointer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone_from(&found);
        Ok(found)
    }

    /// The stage's length; for a stage about to be resumed, after lease recovery closed it — then
    /// the length is proven (see the module documentation), and the resume continues from it.
    async fn observe_stage(
        &self,
        _final_path: &StoragePath,
    ) -> Result<Option<u64>, StorageRoleFailure> {
        let facts = match self.protocol.stat(&self.stage).await {
            Ok(facts) => facts,
            Err(error) if is_not_found(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        if facts.kind != EntryKind::File {
            return Err(failure(
                &self.stage,
                Operation::Prepare,
                FailureClass::Conflict,
            ));
        }
        if self.resumable() {
            return self
                .protocol
                .stabilize_recovered_stage(&self.stage)
                .await
                .map(Some);
        }
        Ok(facts.size)
    }

    async fn remove_pointer(&self, _final_path: &StoragePath) -> Result<(), StorageRoleFailure> {
        delete_if_present(self.protocol, &self.pointer).await?;
        delete_if_present(self.protocol, &self.pointer_temporary).await
    }

    async fn remove_stage(&self, _final_path: &StoragePath) -> Result<(), StorageRoleFailure> {
        delete_if_present(self.protocol, &self.stage).await
    }

    fn accepts_pointer(&self, pointer: &DestinationPointer) -> bool {
        accepts(pointer)
    }

    fn continues_from_stage(&self) -> bool {
        true
    }
}

/// Prepares a stage at the destination: looks at what is there and resumes or starts over as the
/// decision table says. A resume takes the stage over by rewriting its pointer with a new nonce.
pub(super) async fn prepare(
    adapter: &HdfsStagedDestination,
    request: DestinationPrepareRequest,
) -> Result<PreparedStage, StorageRoleFailure> {
    let final_path = request.prepare.final_destination.path().clone();
    validate_final(&final_path)?;
    let expected_size = request
        .prepare
        .source
        .size
        .ok_or_else(|| failure(&final_path, Operation::Prepare, FailureClass::Unsupported))?;
    let artifacts = HdfsArtifacts {
        protocol: &*adapter.protocol,
        stage: artifact_path(&final_path, ArtifactKind::Stage, false)?,
        pointer: artifact_path(&final_path, ArtifactKind::Pointer, false)?,
        pointer_temporary: artifact_path(&final_path, ArtifactKind::Pointer, true)?,
        request: &request,
        found_pointer: Mutex::new(None),
    };
    let found = discover(&artifacts, &request).await?;
    let mut stage = PreparedStage::new(
        adapter.identity().clone(),
        request.prepare.final_destination.clone(),
        stage_token(&artifacts.stage, expected_size)?,
        request.prepare.recovery_binding,
        0,
        None,
    );
    stage.backend_state = Some(Arc::new(HdfsStageState {
        fence: Fence::new(request.transfer_identity),
    }));
    stage.mark_at_destination(found.fact);
    let Some(point) = found.resume else {
        return start(adapter, stage, &artifacts.stage, request.recoverable).await;
    };
    restore_owner_write(&*adapter.protocol, &artifacts.stage).await?;
    write_pointer(adapter, &stage, point.prefix, true).await?;
    stage.write_offset = point.prefix;
    Ok(stage)
}

/// Metadata applied before a crash may have taken the owner's write permission; it is given back
/// (the metadata is applied again before publication).
async fn restore_owner_write(
    protocol: &dyn HdfsProtocol,
    path: &StoragePath,
) -> Result<(), StorageRoleFailure> {
    let mode = protocol.stat(path).await?.mode;
    if mode & 0o200 == 0 {
        protocol.set_mode(path, mode | 0o200).await?;
    }
    Ok(())
}

/// Creates a new stage. A recoverable one records prefix zero before any byte is written; any
/// other gets its first pointer at its first checkpoint.
async fn start(
    adapter: &HdfsStagedDestination,
    stage: PreparedStage,
    path: &StoragePath,
    recoverable: bool,
) -> Result<PreparedStage, StorageRoleFailure> {
    adapter
        .protocol
        .create_empty_stage_exclusive(path)
        .await
        .map_err(|error| transient_collision(stage.final_destination.path(), error))?;
    if !recoverable {
        return Ok(stage.disable_recovery());
    }
    // Nothing may be at the pointer's name yet: one that appeared is another writer's.
    if let Err(error) = write_pointer(adapter, &stage, 0, false).await {
        let _ = discard(adapter, &stage).await;
        return Err(error);
    }
    Ok(stage)
}

/// Discovery left nothing at the name, so something there now is another writer's: a retry
/// settles it.
fn transient_collision(final_path: &StoragePath, error: StorageRoleFailure) -> StorageRoleFailure {
    match error {
        StorageRoleFailure::Entry(entry) if entry.class() == FailureClass::Conflict => {
            entry_failure(
                final_path,
                Operation::Prepare,
                FailureClass::Conflict,
                Transience::Transient,
            )
        }
        other => other,
    }
}

/// Whether the pointer at the destination is still this stage's: present with its nonce, or —
/// before it wrote one — absent.
async fn still_ours(
    adapter: &HdfsStagedDestination,
    stage: &PreparedStage,
) -> Result<bool, StorageRoleFailure> {
    let fence = fence(stage)?;
    let pointer = artifact_path(stage.final_destination.path(), ArtifactKind::Pointer, false)?;
    Ok(
        match read_regular(&*adapter.protocol, &pointer, MAX_POINTER_BYTES + 1).await? {
            None => !fence.written.load(Ordering::Acquire),
            Some(bytes) => DestinationPointer::decode(&bytes)
                .is_ok_and(|found| found.extension == fence.extension()),
        },
    )
}

fn taken_over(final_path: &StoragePath, operation: Operation) -> StorageRoleFailure {
    // Another writer holds the stage now; retrying would take it back and forth.
    failure(final_path, operation, FailureClass::Conflict)
}

/// Records `prefix` in the pointer: through the fixed temporary (removed first, then created
/// exclusively, written and closed), renamed over the pointer. A lost rename reply is settled by
/// reading the pointer back. Unless `take_over`, the pointer must still be this stage's.
pub(super) async fn write_pointer(
    adapter: &HdfsStagedDestination,
    stage: &PreparedStage,
    prefix: u64,
    take_over: bool,
) -> Result<(), StorageRoleFailure> {
    let final_path = stage.final_destination.path();
    if !take_over && !still_ours(adapter, stage).await? {
        return Err(taken_over(final_path, Operation::Write));
    }
    let fence = fence(stage)?;
    let bytes = DestinationPointer {
        binding: stage.recovery_binding,
        transfer_identity: fence.transfer_identity,
        durable_prefix: Some(prefix),
        extension: fence.extension(),
    }
    .encode()
    .map_err(|_| failure(final_path, Operation::Write, FailureClass::Internal))?;
    let pointer = artifact_path(final_path, ArtifactKind::Pointer, false)?;
    let temporary = artifact_path(final_path, ArtifactKind::Pointer, true)?;
    let protocol = &*adapter.protocol;
    delete_if_present(protocol, &temporary).await?;
    if let Err(error) = write_temporary(protocol, &temporary, Bytes::from(bytes.clone())).await {
        let _ = protocol.delete(&temporary, EntryKind::File).await;
        return Err(error);
    }
    if let Err(rename_error) = protocol.rename(&temporary, &pointer, true).await {
        // A rename can fail after the NameNode committed it: read the pointer back before deciding.
        let committed = read_regular(protocol, &pointer, MAX_POINTER_BYTES + 1)
            .await
            .is_ok_and(|found| found.as_deref() == Some(bytes.as_slice()));
        let _ = protocol.delete(&temporary, EntryKind::File).await;
        if !committed {
            return Err(rename_error);
        }
    }
    fence.written.store(true, Ordering::Release);
    Ok(())
}

/// Creates `path` exclusively and writes `bytes` to it; closing completes the file.
async fn write_temporary(
    protocol: &dyn HdfsProtocol,
    path: &StoragePath,
    bytes: Bytes,
) -> Result<(), StorageRoleFailure> {
    protocol
        .create_empty_stage_exclusive(path)
        .await
        .map_err(|error| transient_collision(path, error))?;
    let mut writer = protocol.open_stage_writer(path, 0, false).await?;
    let length = bytes.len();
    let written = writer.write(bytes).await;
    let closed = writer.close().await;
    if written? != length {
        return Err(failure(path, Operation::Write, FailureClass::Corruption));
    }
    closed
}

/// The durable prefix of this stage: its length, once the pointer is confirmed still its own.
pub(super) async fn reobserve(
    adapter: &HdfsStagedDestination,
    stage: &PreparedStage,
    path: &StoragePath,
) -> Result<u64, StorageRoleFailure> {
    let final_path = stage.final_destination.path();
    if !still_ours(adapter, stage).await? {
        return Err(taken_over(final_path, Operation::Observe));
    }
    let facts = adapter.protocol.stat(path).await?;
    match (facts.kind, facts.size) {
        (EntryKind::File, Some(size)) => Ok(size),
        _ => Err(failure(
            final_path,
            Operation::Observe,
            FailureClass::Corruption,
        )),
    }
}

/// Publishes a stage kept at the destination: it must still be this stage's; a rename whose reply
/// is lost counts as done only when the stage is gone and the final file holds exactly the
/// expected content (its size, and its BLAKE3 when known); the pointer goes after the commit.
pub(super) async fn publish(
    adapter: &HdfsStagedDestination,
    stage: &PreparedStage,
    part: &StoragePath,
    request: &PublishRequest,
) -> Result<PublicationEvidence, PublicationFailure> {
    let final_path = stage.final_destination.path();
    before_publication(adapter, stage)
        .await
        .map_err(publication_failure)?;
    if let Err(error) = adapter.protocol.rename(part, final_path, true).await
        && !lost_reply_committed(adapter, part, final_path, request).await
    {
        return Err(publication_may_have_changed(error));
    }
    remove_pointer(adapter, stage)
        .await
        .map_err(|error| PublicationFailure {
            error,
            final_destination_changed: true,
        })?;
    Ok(PublicationEvidence {
        final_destination: final_path.clone(),
        disposition: PublicationDisposition::Published,
        version: None,
    })
}

async fn lost_reply_committed(
    adapter: &HdfsStagedDestination,
    part: &StoragePath,
    final_path: &StoragePath,
    request: &PublishRequest,
) -> bool {
    let protocol = &*adapter.protocol;
    let stage_gone = matches!(protocol.stat(part).await, Err(ref error) if is_not_found(error));
    let size_matches = protocol
        .stat(final_path)
        .await
        .is_ok_and(|facts| facts.size == Some(request.expected_size));
    if !(stage_gone && size_matches) {
        return false;
    }
    match request.expected_blake3 {
        Some(expected) => hash_file(
            protocol,
            final_path,
            final_path,
            request.expected_size,
            &request.cancel,
        )
        .await
        .is_ok_and(|digest| digest == expected),
        None => true,
    }
}

/// Before the stage becomes the final file: it must still be this stage's.
pub(super) async fn before_publication(
    adapter: &HdfsStagedDestination,
    stage: &PreparedStage,
) -> Result<(), StorageRoleFailure> {
    if still_ours(adapter, stage).await? {
        Ok(())
    } else {
        Err(taken_over(
            stage.final_destination.path(),
            Operation::Publish,
        ))
    }
}

/// Removes the pointer and its temporary once the stage is the final file (or gone). A stage that
/// never wrote a pointer has none to remove — and one there now is another writer's.
pub(super) async fn remove_pointer(
    adapter: &HdfsStagedDestination,
    stage: &PreparedStage,
) -> Result<(), StorageRoleFailure> {
    if !fence(stage)?.written.load(Ordering::Acquire) {
        return Ok(());
    }
    let final_path = stage.final_destination.path();
    for temporary in [false, true] {
        let path = artifact_path(final_path, ArtifactKind::Pointer, temporary)?;
        delete_if_present(&*adapter.protocol, &path).await?;
    }
    Ok(())
}

/// Removes the stage and its pointer, the pointer first. A stage another writer took over is left
/// alone: its names are that writer's now.
pub(super) async fn discard(
    adapter: &HdfsStagedDestination,
    stage: &PreparedStage,
) -> Result<(), StorageRoleFailure> {
    let path = artifact_path(stage.final_destination.path(), ArtifactKind::Stage, false)?;
    if still_ours(adapter, stage).await? {
        remove_pointer(adapter, stage).await?;
        delete_if_present(&*adapter.protocol, &path).await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "at_destination_tests.rs"]
mod tests;
