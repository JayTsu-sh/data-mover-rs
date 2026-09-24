//! Destination-resident recovery for the CIFS stage (ADR-0006 C11). The stage and the pointer that
//! records its binding and durable prefix sit beside the final file under deterministic names.
//!
//! As on NFS, no lock is honoured by every host here (the smb-rs facade offers no share-mode or
//! lease control), so exclusivity is the engine's in-process lease plus the caller contract, and a
//! nonce fence: every prepare draws a random nonce and records it in the pointer (a resume rewrites
//! the pointer with its own). Before it rewrites the pointer, publishes, or cleans up, a stage reads
//! the pointer back; another nonce there means another writer took the stage over, and this one
//! stops (`Conflict`). The fence is a check, not a lock; a stage prepared without a pointer (not
//! recoverable: its first pointer waits for its first checkpoint) has nothing to fence with until
//! then. Both are two writers of one key, which the caller contract excludes.
//!
//! SMB cannot shorten a file through the facade, so a resume does not truncate the stage: the writer
//! rewrites everything from the durable prefix to the source's size, and a stage that still ends up
//! longer fails verification as `Corruption`, which cleans it up.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use uuid::Uuid;

use super::protocol::is_collision;
use super::source::{classify, entry_failure, entry_failure_with_transience};
use super::staged::{CifsStageState, CifsStagedDestination, is_not_found, state};
use crate::model::{FailureClass, Operation, StoragePath, Transience};
use crate::storage::artifacts::{ArtifactKind, artifact_temporary_name, sibling_artifact};
use crate::storage::discovery::{DestinationArtifacts, discover};
use crate::storage::pointer::{DestinationPointer, MAX_POINTER_BYTES};
use crate::storage::{DestinationPrepareRequest, PreparedStage, StorageRoleFailure};

/// The tag every CIFS pointer's extension starts with; the fence nonce follows it.
const POINTER_TAG: &[u8; 8] = b"DMCSTG01";
const NONCE_BYTES: usize = 16;

/// What a destination-kept CIFS stage knows about its own pointer.
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

/// The path of one artifact of a final file, beside it. Final paths hold only `/` separators
/// (`validate_final`), so the artifact lands in the final file's own directory.
pub(super) fn artifact_path(
    final_path: &StoragePath,
    kind: ArtifactKind,
    temporary: bool,
) -> Result<StoragePath, StorageRoleFailure> {
    let invalid = || entry_failure(final_path, Operation::Prepare, FailureClass::InvalidInput);
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

/// The stage path a stage's token must name: the deterministic name beside the final file, and
/// only for a stage prepared at the destination.
pub(super) fn stage_path(stage: &PreparedStage) -> Result<StoragePath, StorageRoleFailure> {
    let final_path = stage.final_destination.path();
    let expected = artifact_path(final_path, ArtifactKind::Stage, false)?;
    if stage.at_destination && stage.token.as_ref() == expected.as_str().as_bytes() {
        Ok(expected)
    } else {
        Err(entry_failure(
            final_path,
            Operation::Observe,
            FailureClass::Conflict,
        ))
    }
}

/// Whether a pointer is one a CIFS stage wrote: tagged, fenced, with a durable prefix. A pointer
/// without a prefix would resume from the stage's size, which may include unflushed bytes.
fn accepts(pointer: &DestinationPointer) -> bool {
    pointer.durable_prefix.is_some()
        && pointer.extension.len() == POINTER_TAG.len() + NONCE_BYTES
        && pointer.extension.starts_with(POINTER_TAG)
}

fn fence(stage: &PreparedStage) -> Result<&Fence, StorageRoleFailure> {
    Ok(&state(stage)?.fence)
}

fn not_regular(path: &StoragePath, operation: Operation) -> StorageRoleFailure {
    entry_failure(path, operation, FailureClass::Conflict)
}

/// Discovery left nothing at the name, so something there now is another writer's (or a lost
/// CREATE reply): a retry settles it.
fn collision(path: &StoragePath, operation: Operation) -> StorageRoleFailure {
    entry_failure_with_transience(
        path,
        operation,
        FailureClass::Conflict,
        Transience::Transient,
        "a transfer artifact appeared at its name",
    )
}

/// Reads a regular file whole, up to `limit` bytes; `None` if it is absent. Something that is not
/// a file at an artifact name is a conflict. Reads exactly the size the server reports: SMB answers
/// a read at end of file with an error, not an empty read.
async fn read_regular(
    adapter: &CifsStagedDestination,
    path: &StoragePath,
    limit: usize,
    operation: Operation,
) -> Result<Option<Vec<u8>>, StorageRoleFailure> {
    let size = match adapter.protocol.stat(path).await {
        Ok((true, size)) => size,
        Ok((false, _)) => return Err(not_regular(path, operation)),
        Err(error) if is_not_found(&error) => return Ok(None),
        Err(error) => return Err(classify(path, operation, &error)),
    };
    let file = match adapter.protocol.open(path).await {
        Ok(file) => file,
        Err(error) if is_not_found(&error) => return Ok(None),
        Err(error) => return Err(classify(path, operation, &error)),
    };
    let wanted = usize::try_from(size).map_or(limit, |size| size.min(limit));
    let mut bytes = Vec::with_capacity(wanted);
    let read = async {
        let chunk = file.maximum_read_chunk().max(1);
        while bytes.len() < wanted {
            let count = u32::try_from(wanted - bytes.len())
                .unwrap_or(u32::MAX)
                .min(chunk);
            let part = file.read_at(bytes.len() as u64, count).await?;
            if part.is_empty() {
                break;
            }
            bytes.extend_from_slice(&part);
        }
        Ok(())
    }
    .await;
    let closed = file.close().await;
    read.and(closed)
        .map_err(|error| classify(path, operation, &error))?;
    Ok(Some(bytes))
}

async fn delete_if_present(
    adapter: &CifsStagedDestination,
    path: &StoragePath,
    operation: Operation,
) -> Result<(), StorageRoleFailure> {
    match adapter.protocol.delete(path).await {
        Err(error) if !is_not_found(&error) => Err(classify(path, operation, &error)),
        _ => Ok(()),
    }
}

/// A final file's artifacts, as the discovery driver looks at them.
struct CifsArtifacts<'a> {
    adapter: &'a CifsStagedDestination,
    stage: StoragePath,
    pointer: StoragePath,
    pointer_temporary: StoragePath,
}

#[async_trait]
impl DestinationArtifacts for CifsArtifacts<'_> {
    async fn read_pointer(
        &self,
        _final_path: &StoragePath,
        limit: usize,
    ) -> Result<Option<Vec<u8>>, StorageRoleFailure> {
        read_regular(self.adapter, &self.pointer, limit, Operation::Prepare).await
    }

    /// The stage's size confirms it exists and is not shorter than the pointer's prefix; it is
    /// never the resume offset, because [`Self::accepts_pointer`] refuses a pointer without one.
    async fn observe_stage(
        &self,
        _final_path: &StoragePath,
    ) -> Result<Option<u64>, StorageRoleFailure> {
        match self.adapter.protocol.stat(&self.stage).await {
            Ok((true, size)) => Ok(Some(size)),
            Ok((false, _)) => Err(not_regular(&self.stage, Operation::Prepare)),
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(classify(&self.stage, Operation::Prepare, &error)),
        }
    }

    async fn remove_pointer(&self, _final_path: &StoragePath) -> Result<(), StorageRoleFailure> {
        delete_if_present(self.adapter, &self.pointer, Operation::Prepare).await?;
        delete_if_present(self.adapter, &self.pointer_temporary, Operation::Prepare).await
    }

    async fn remove_stage(&self, _final_path: &StoragePath) -> Result<(), StorageRoleFailure> {
        delete_if_present(self.adapter, &self.stage, Operation::Prepare).await
    }

    fn accepts_pointer(&self, pointer: &DestinationPointer) -> bool {
        accepts(pointer)
    }
}

/// Prepares a stage at the destination: looks at what is there and resumes or starts over as the
/// decision table says. A resume takes the stage over by rewriting its pointer with a new nonce.
pub(super) async fn prepare(
    adapter: &CifsStagedDestination,
    request: DestinationPrepareRequest,
) -> Result<PreparedStage, StorageRoleFailure> {
    let final_path = request.prepare.final_destination.path().clone();
    super::staged::validate_final(&final_path)?;
    let artifacts = CifsArtifacts {
        adapter,
        stage: artifact_path(&final_path, ArtifactKind::Stage, false)?,
        pointer: artifact_path(&final_path, ArtifactKind::Pointer, false)?,
        pointer_temporary: artifact_path(&final_path, ArtifactKind::Pointer, true)?,
    };
    let found = discover(&artifacts, &request).await?;
    let token = Bytes::copy_from_slice(artifacts.stage.as_str().as_bytes());
    let mut stage = PreparedStage::new(
        adapter.identity().clone(),
        request.prepare.final_destination.clone(),
        token.clone(),
        request.prepare.recovery_binding,
        0,
        None,
    );
    stage.backend_state = Some(Arc::new(CifsStageState {
        published: AtomicBool::new(false),
        fence: Fence::new(request.transfer_identity),
    }));
    stage.mark_at_destination(found.fact);
    let Some(point) = found.resume else {
        return start(adapter, stage, &artifacts.stage, request.recoverable).await;
    };
    adapter.claim(token);
    if let Err(error) = write_pointer(adapter, &stage, point.prefix, true).await {
        // Whether or not the take-over landed, the stage stays for the next attempt to resume.
        adapter.release(&stage.token);
        return Err(error);
    }
    stage.write_offset = point.prefix;
    Ok(stage)
}

/// Creates a new stage. A recoverable one records prefix zero before any byte is written; any
/// other gets its first pointer at its first checkpoint.
async fn start(
    adapter: &CifsStagedDestination,
    stage: PreparedStage,
    path: &StoragePath,
    recoverable: bool,
) -> Result<PreparedStage, StorageRoleFailure> {
    let final_path = stage.final_destination.path();
    adapter.protocol.create_empty(path).await.map_err(|error| {
        if is_collision(&error) {
            collision(final_path, Operation::Prepare)
        } else {
            classify(final_path, Operation::Prepare, &error)
        }
    })?;
    adapter.claim(stage.token.clone());
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

/// Whether the pointer at the destination is still this stage's: present with its nonce, or —
/// before it wrote one — absent.
async fn still_ours(
    adapter: &CifsStagedDestination,
    stage: &PreparedStage,
    operation: Operation,
) -> Result<bool, StorageRoleFailure> {
    let fence = fence(stage)?;
    let pointer = artifact_path(stage.final_destination.path(), ArtifactKind::Pointer, false)?;
    Ok(
        match read_regular(adapter, &pointer, MAX_POINTER_BYTES + 1, operation).await? {
            None => !fence.written.load(Ordering::Acquire),
            Some(bytes) => DestinationPointer::decode(&bytes)
                .is_ok_and(|found| found.extension == fence.extension()),
        },
    )
}

fn taken_over(final_path: &StoragePath, operation: Operation) -> StorageRoleFailure {
    // Another writer holds the stage now; retrying would take it back and forth.
    entry_failure(final_path, operation, FailureClass::Conflict)
}

/// Records `prefix` in the pointer: through the fixed temporary (removed first, then created,
/// written and flushed), renamed over the pointer. A lost rename reply is settled by reading the
/// pointer back. Unless `take_over`, the pointer must still be this stage's.
pub(super) async fn write_pointer(
    adapter: &CifsStagedDestination,
    stage: &PreparedStage,
    prefix: u64,
    take_over: bool,
) -> Result<(), StorageRoleFailure> {
    let final_path = stage.final_destination.path();
    if !take_over && !still_ours(adapter, stage, Operation::Write).await? {
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
    .map_err(|_| entry_failure(final_path, Operation::Write, FailureClass::Internal))?;
    let pointer = artifact_path(final_path, ArtifactKind::Pointer, false)?;
    let temporary = artifact_path(final_path, ArtifactKind::Pointer, true)?;
    delete_if_present(adapter, &temporary, Operation::Write).await?;
    if let Err(error) = write_record(adapter, &temporary, Bytes::from(bytes.clone())).await {
        let _ = adapter.protocol.delete(&temporary).await;
        return Err(error);
    }
    if let Err(rename_error) = adapter.protocol.rename(&temporary, &pointer, true).await {
        // RENAME can fail after the server committed it: read the pointer back before deciding.
        let committed = read_regular(adapter, &pointer, MAX_POINTER_BYTES + 1, Operation::Write)
            .await
            .is_ok_and(|found| found.as_deref() == Some(bytes.as_slice()));
        let _ = adapter.protocol.delete(&temporary).await;
        if !committed {
            return Err(classify(&pointer, Operation::Write, &rename_error));
        }
    }
    fence.written.store(true, Ordering::Release);
    Ok(())
}

/// The durable prefix this stage's pointer proves, checked against the stage's size.
pub(super) async fn reobserve(
    adapter: &CifsStagedDestination,
    stage: &PreparedStage,
) -> Result<u64, StorageRoleFailure> {
    let final_path = stage.final_destination.path();
    let fence = fence(stage)?;
    let pointer = artifact_path(final_path, ArtifactKind::Pointer, false)?;
    let bytes = read_regular(adapter, &pointer, MAX_POINTER_BYTES + 1, Operation::Observe)
        .await?
        .ok_or_else(|| taken_over(final_path, Operation::Observe))?;
    let corrupt = || entry_failure(final_path, Operation::Observe, FailureClass::Corruption);
    let found = DestinationPointer::decode(&bytes).map_err(|_| corrupt())?;
    if found.extension != fence.extension() {
        return Err(taken_over(final_path, Operation::Observe));
    }
    let prefix = found
        .durable_prefix
        .filter(|_| {
            found.binding == stage.recovery_binding
                && found.transfer_identity == fence.transfer_identity
        })
        .ok_or_else(corrupt)?;
    let stage_file = adapter.stage_path(stage)?;
    let (regular, size) = adapter
        .protocol
        .stat(&stage_file)
        .await
        .map_err(|error| classify(final_path, Operation::Observe, &error))?;
    if !regular || size < prefix {
        return Err(corrupt());
    }
    Ok(prefix)
}

/// Creates `path` and writes `bytes` to it, flushed before it is closed.
async fn write_record(
    adapter: &CifsStagedDestination,
    path: &StoragePath,
    mut bytes: Bytes,
) -> Result<(), StorageRoleFailure> {
    let failed = |error: &smb_domain::Error| classify(path, Operation::Write, error);
    adapter
        .protocol
        .create_empty(path)
        .await
        .map_err(|error| failed(&error))?;
    let file = adapter
        .protocol
        .open(path)
        .await
        .map_err(|error| failed(&error))?;
    let result = async {
        let maximum = file.maximum_write_chunk() as usize;
        if maximum == 0 {
            return Err(entry_failure(
                path,
                Operation::Write,
                FailureClass::Protocol,
            ));
        }
        let mut offset = 0;
        while !bytes.is_empty() {
            let part = bytes.split_to(bytes.len().min(maximum));
            let length = part.len() as u64;
            file.write_all_at(offset, part)
                .await
                .map_err(|error| failed(&error))?;
            offset += length;
        }
        file.flush().await.map_err(|error| failed(&error))
    }
    .await;
    let close = file.close().await.map_err(|error| failed(&error));
    result.and(close)
}

/// Before the stage becomes the final file: it must still be this stage's.
pub(super) async fn before_publication(
    adapter: &CifsStagedDestination,
    stage: &PreparedStage,
) -> Result<(), StorageRoleFailure> {
    if still_ours(adapter, stage, Operation::Publish).await? {
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
    adapter: &CifsStagedDestination,
    stage: &PreparedStage,
) -> Result<(), StorageRoleFailure> {
    if !fence(stage)?.written.load(Ordering::Acquire) {
        return Ok(());
    }
    let final_path = stage.final_destination.path();
    for temporary in [false, true] {
        let path = artifact_path(final_path, ArtifactKind::Pointer, temporary)?;
        delete_if_present(adapter, &path, Operation::Namespace).await?;
    }
    Ok(())
}

/// Removes the stage and its pointer, the pointer first. A stage another writer took over is left
/// alone: its names are that writer's now.
pub(super) async fn discard(
    adapter: &CifsStagedDestination,
    stage: &PreparedStage,
) -> Result<(), StorageRoleFailure> {
    let path = stage_path(stage)?;
    let published = state(stage)?.published.load(Ordering::Acquire);
    if still_ours(adapter, stage, Operation::Namespace).await? {
        remove_pointer(adapter, stage).await?;
        if !published {
            delete_if_present(adapter, &path, Operation::Namespace).await?;
        }
    }
    adapter.release(&stage.token);
    Ok(())
}
