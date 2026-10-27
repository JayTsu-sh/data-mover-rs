//! Destination-resident recovery for the NFS stage (ADR-0006 C10). The stage and the pointer that
//! records its binding and durable prefix sit beside the final file under deterministic names.
//!
//! NFS has no lock every host honours, so exclusivity is the engine's in-process lease plus the
//! caller contract, and a fence: every prepare draws a random nonce and records it in the pointer
//! (a resume rewrites the pointer with its own before touching the stage). Before it rewrites the
//! pointer, publishes, or cleans up, a stage reads the pointer back; another nonce there means
//! another writer took the stage over, and this one stops (`Conflict`).
//!
//! The fence is a check, not a lock: a take-over that lands between the check and the rename or
//! removal that follows it is not seen. And a stage prepared without a pointer (not recoverable:
//! its first pointer waits for its first checkpoint) has nothing to fence with until then — writing
//! one at prepare would cost several round trips per small file. Both cases are two writers of one
//! key, which the caller contract excludes; read-back verification catches mixed bytes.

use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use uuid::Uuid;

use super::super::DEFAULT_FILE_MODE;
use super::source::{NfsProtocolFailure, entry_failure, role_failure};
use super::staged::{NfsStageFile, NfsStageState, NfsStagedDestinationAdapter, sibling_path};
use crate::model::{FailureClass, Operation, StoragePath, Transience};
use crate::storage::artifacts::{ArtifactKind, artifact_name, artifact_temporary_name};
use crate::storage::discovery::{DestinationArtifacts, discover};
use crate::storage::pointer::{DestinationPointer, MAX_POINTER_BYTES};
use crate::storage::{DestinationPrepareRequest, PreparedStage, StorageRoleFailure};

#[path = "hash_checkpoint.rs"]
mod hash_checkpoint;
pub(super) use hash_checkpoint::{checkpoint_barrier, finish_publication, release_hash_checkpoint};

/// The tag every NFS pointer's extension starts with; the fence nonce follows it.
const POINTER_TAG: &[u8; 8] = b"DMNSTG01";
const NONCE_BYTES: usize = 16;
// Keep legacy pointer I/O unchanged on unarmed role/expert paths.
const LEGACY_POINTER_BYTES: usize = 84 + 4096 + 32;

/// What a destination-kept NFS stage knows about its own pointer.
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

/// The path of one artifact of a final file, beside it.
pub(super) fn artifact_path(
    final_path: &StoragePath,
    kind: ArtifactKind,
    temporary: bool,
) -> Result<StoragePath, StorageRoleFailure> {
    let name = Path::new(final_path.as_str())
        .file_name()
        .and_then(OsStr::to_str)
        .ok_or_else(|| invalid(final_path, FailureClass::InvalidInput))?;
    let artifact = if temporary {
        artifact_temporary_name(name, kind)
    } else {
        artifact_name(name, kind)
    };
    sibling_path(final_path, &artifact)
}

/// The stage path a stage's token must name: the deterministic name beside the final file, and
/// only for a stage prepared at the destination.
pub(super) fn stage_path(stage: &PreparedStage) -> Result<StoragePath, StorageRoleFailure> {
    let final_path = stage.final_destination.path();
    let expected = artifact_path(final_path, ArtifactKind::Stage, false)?;
    if stage.at_destination && stage.token.as_ref() == expected.as_str().as_bytes() {
        Ok(expected)
    } else {
        Err(invalid(final_path, FailureClass::Conflict))
    }
}

/// Whether a pointer is one an NFS stage wrote: tagged, fenced, with a durable prefix. A pointer
/// without a prefix would resume from the stage's size, which may include unstable bytes.
fn accepts(pointer: &DestinationPointer) -> bool {
    pointer.durable_prefix.is_some()
        && pointer.extension.len() == POINTER_TAG.len() + NONCE_BYTES
        && pointer.extension.starts_with(POINTER_TAG)
}

fn fence(stage: &PreparedStage) -> Result<&Fence, StorageRoleFailure> {
    stage
        .backend_state
        .as_ref()
        .and_then(|state| state.downcast_ref::<NfsStageState>())
        .map(|state| &state.fence)
        .ok_or_else(|| invalid(stage.final_destination.path(), FailureClass::Corruption))
}

fn is_not_found(error: &NfsProtocolFailure) -> bool {
    error.class == FailureClass::NotFound
}

/// Reads up to `limit` bytes of a regular file; `None` if it is absent. Something that is not a
/// file at an artifact name is a conflict.
async fn read_regular(
    adapter: &NfsStagedDestinationAdapter,
    path: &StoragePath,
    limit: usize,
) -> Result<Option<Vec<u8>>, NfsProtocolFailure> {
    let size = match adapter.protocol.stat(path).await {
        Ok((true, size)) => size,
        Ok((false, _)) => return Err(not_regular()),
        Err(error) if is_not_found(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    let handle = match adapter.protocol.open_read(path).await {
        Ok(handle) => handle,
        Err(error) if is_not_found(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    let read = async {
        if size <= LEGACY_POINTER_BYTES as u64 {
            return handle
                .read_at(0, LEGACY_POINTER_BYTES + 1)
                .await
                .map(|bytes| bytes.to_vec());
        }
        let wanted = usize::try_from(size).unwrap_or(limit).min(limit);
        let maximum = adapter.protocol.maximum_read_chunk_bytes().max(1);
        let mut bytes = Vec::with_capacity(wanted);
        while bytes.len() < wanted {
            let count = (wanted - bytes.len()).min(maximum);
            let part = handle.read_at(bytes.len() as u64, count).await?;
            if part.is_empty() {
                break;
            }
            if part.len() > count {
                return Err(NfsProtocolFailure::protocol());
            }
            bytes.extend_from_slice(&part);
        }
        Ok(bytes)
    }
    .await;
    let closed = handle.close().await;
    let bytes = read?;
    closed?;
    Ok(Some(bytes))
}

fn not_regular() -> NfsProtocolFailure {
    NfsProtocolFailure::new(FailureClass::Conflict, Transience::Permanent)
}

async fn delete_if_present(
    adapter: &NfsStagedDestinationAdapter,
    path: &StoragePath,
) -> Result<(), NfsProtocolFailure> {
    match adapter.protocol.delete(path).await {
        Err(error) if !is_not_found(&error) => Err(error),
        _ => Ok(()),
    }
}

/// A final file's artifacts, as the discovery driver looks at them.
struct NfsArtifacts<'a> {
    adapter: &'a NfsStagedDestinationAdapter,
    stage: StoragePath,
    pointer: StoragePath,
    pointer_temporary: StoragePath,
}

#[async_trait]
impl DestinationArtifacts for NfsArtifacts<'_> {
    async fn observe_final(
        &self,
        final_path: &StoragePath,
    ) -> Result<Option<u64>, StorageRoleFailure> {
        match self.adapter.protocol.stat(final_path).await {
            Ok((true, size)) => Ok(Some(size)),
            Ok((false, _)) => Err(role_failure(final_path, Operation::Prepare, not_regular())),
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(role_failure(final_path, Operation::Prepare, error)),
        }
    }
    async fn read_pointer(
        &self,
        final_path: &StoragePath,
        limit: usize,
    ) -> Result<Option<Vec<u8>>, StorageRoleFailure> {
        read_regular(self.adapter, &self.pointer, limit)
            .await
            .map_err(|error| role_failure(final_path, Operation::Prepare, error))
    }

    /// The stage's size confirms it exists and is not shorter than the pointer's prefix; it is
    /// never the resume offset, because [`Self::accepts_pointer`] refuses a pointer without one.
    async fn observe_stage(
        &self,
        final_path: &StoragePath,
    ) -> Result<Option<u64>, StorageRoleFailure> {
        match self.adapter.protocol.stat(&self.stage).await {
            Ok((true, size)) => Ok(Some(size)),
            Ok((false, _)) => Err(role_failure(final_path, Operation::Prepare, not_regular())),
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(role_failure(final_path, Operation::Prepare, error)),
        }
    }

    async fn remove_pointer(&self, final_path: &StoragePath) -> Result<(), StorageRoleFailure> {
        delete_if_present(self.adapter, &self.pointer)
            .await
            .and(delete_if_present(self.adapter, &self.pointer_temporary).await)
            .map_err(|error| role_failure(final_path, Operation::Prepare, error))
    }

    async fn remove_stage(&self, final_path: &StoragePath) -> Result<(), StorageRoleFailure> {
        delete_if_present(self.adapter, &self.stage)
            .await
            .map_err(|error| role_failure(final_path, Operation::Prepare, error))
    }

    fn accepts_pointer(&self, pointer: &DestinationPointer) -> bool {
        accepts(pointer)
    }
}

/// Prepares a stage at the destination: looks at what is there and resumes or starts over as the
/// decision table says. A resume takes the stage over — rewrites its pointer with a new nonce —
/// before it truncates the stage to the durable prefix.
pub(super) async fn prepare(
    adapter: &NfsStagedDestinationAdapter,
    request: DestinationPrepareRequest,
) -> Result<PreparedStage, StorageRoleFailure> {
    let final_path = request.prepare.final_destination.path().clone();
    super::staged::checked_final(&final_path)?;
    let artifacts = NfsArtifacts {
        adapter,
        stage: artifact_path(&final_path, ArtifactKind::Stage, false)?,
        pointer: artifact_path(&final_path, ArtifactKind::Pointer, false)?,
        pointer_temporary: artifact_path(&final_path, ArtifactKind::Pointer, true)?,
    };
    let found = discover(&artifacts, &request).await?;
    let token = Bytes::copy_from_slice(artifacts.stage.as_str().as_bytes());
    let mut stage = PreparedStage::new(
        adapter.identity.clone(),
        request.prepare.final_destination.clone(),
        token.clone(),
        request.prepare.recovery_binding,
        0,
        None,
    );
    stage.backend_state = Some(Arc::new(NfsStageState {
        fence: Fence::new(request.transfer_identity),
    }));
    stage.mark_at_destination(found.fact);
    let Some(point) = found.resume else {
        let mode = if request.prepare.final_destination.private_creation() {
            0o600
        } else {
            DEFAULT_FILE_MODE
        };
        let handle = create(adapter, &artifacts.stage, &final_path, mode).await?;
        adapter.claim_authority(token.clone());
        adapter.cache_prepared_handle(token, handle);
        if !request.recoverable {
            return Ok(stage.disable_recovery());
        }
        // Nothing may be at the pointer's name yet: one that appeared is another writer's.
        if let Err(error) = write_pointer(adapter, &stage, 0, false).await {
            let _ = discard(adapter, &stage).await;
            return Err(error);
        }
        return Ok(stage);
    };
    stage
        .restore_checkpoint(point.pointer.checkpoint)
        .map_err(|_| invalid(&final_path, FailureClass::Corruption))?;
    if point.published {
        hash_checkpoint::recover_publication(adapter, &mut stage, point.prefix).await?;
        return Ok(stage);
    }
    if stage.restored_checkpoint().is_some() {
        let size = adapter
            .protocol
            .size(&artifacts.stage)
            .await
            .map_err(|error| role_failure(&final_path, Operation::Prepare, error))?;
        if stage.checkpoint_prefix() != point.prefix
            || stage
                .checkpoint_ranges()
                .iter()
                .any(|range| range.end > size)
        {
            return Err(invalid(&final_path, FailureClass::Corruption));
        }
    }
    let handle = reopen(adapter, &artifacts.stage, &final_path).await?;
    let took_over = match write_pointer(adapter, &stage, point.prefix, true).await {
        Ok(()) if stage.restored_checkpoint().is_none() => handle
            .set_len(point.prefix)
            .await
            .map_err(|error| role_failure(&final_path, Operation::Prepare, error)),
        other => other,
    };
    if let Err(error) = took_over {
        // Before the take-over the stage is still the previous writer's; after it, a retry
        // resumes it again. Either way it stays.
        let _ = handle.close().await;
        return Err(error);
    }
    adapter.claim_authority(token.clone());
    adapter.cache_prepared_handle(token, handle);
    stage.write_offset = point.prefix;
    Ok(stage)
}

async fn create(
    adapter: &NfsStagedDestinationAdapter,
    path: &StoragePath,
    final_path: &StoragePath,
    mode: u32,
) -> Result<Arc<dyn NfsStageFile>, StorageRoleFailure> {
    adapter
        .protocol
        .create_empty_open(path, mode)
        .await
        .map(Arc::from)
        .map_err(|error| role_failure(final_path, Operation::Prepare, transient_collision(error)))
}

/// Discovery left nothing at the name, so something there now is another writer's (or a lost
/// CREATE reply): a retry settles it. Only the exclusive-create collision is that (a `Conflict`
/// without a server status); a `Conflict` the server named — `NOTDIR`, a file where a parent
/// directory must be — stands, as does a failure at a missing parent level the CREATE had to
/// create first (reported at that level, [`NfsProtocolFailure::at_level`]).
fn transient_collision(error: NfsProtocolFailure) -> NfsProtocolFailure {
    let collision = error.class == FailureClass::Conflict && error.status.is_none();
    let at_the_name = error.level.is_none();
    if at_the_name && (collision || error.class == FailureClass::NotFound) {
        NfsProtocolFailure::new(FailureClass::Conflict, Transience::Transient)
    } else {
        error
    }
}

/// Opens the resumed stage for writing. Metadata applied before a crash may have taken the
/// owner's write permission; it is given back (the metadata is applied again before publication).
async fn reopen(
    adapter: &NfsStagedDestinationAdapter,
    path: &StoragePath,
    final_path: &StoragePath,
) -> Result<Arc<dyn NfsStageFile>, StorageRoleFailure> {
    let opened = match adapter.protocol.open_write(path).await {
        Err(error) if error.class == FailureClass::PermissionDenied => {
            adapter.restore_owner_write(path).await?;
            adapter.protocol.open_write(path).await
        }
        other => other,
    };
    opened
        .map(Arc::from)
        .map_err(|error| role_failure(final_path, Operation::Prepare, transient_collision(error)))
}

/// Whether the pointer at the destination is still this stage's: present with its nonce, or —
/// before it wrote one — absent.
async fn still_ours(
    adapter: &NfsStagedDestinationAdapter,
    stage: &PreparedStage,
) -> Result<bool, StorageRoleFailure> {
    let final_path = stage.final_destination.path();
    let fence = fence(stage)?;
    let pointer = artifact_path(final_path, ArtifactKind::Pointer, false)?;
    let bytes = read_regular(adapter, &pointer, MAX_POINTER_BYTES + 1)
        .await
        .map_err(|error| role_failure(final_path, Operation::Write, error))?;
    Ok(match bytes {
        None => !fence.written.load(Ordering::Acquire),
        Some(bytes) => DestinationPointer::decode(&bytes)
            .is_ok_and(|found| found.extension == fence.extension()),
    })
}

fn taken_over(final_path: &StoragePath) -> StorageRoleFailure {
    // Another writer holds the stage now; retrying would take it back and forth.
    entry_failure(
        final_path,
        Operation::Write,
        FailureClass::Conflict,
        Transience::Permanent,
    )
}

/// Records `prefix` in the pointer: through the fixed temporary (removed first, then created
/// exclusively), renamed over the pointer. A lost rename reply is settled by reading the pointer
/// back. Unless `take_over`, the pointer must still be this stage's.
pub(super) async fn write_pointer(
    adapter: &NfsStagedDestinationAdapter,
    stage: &PreparedStage,
    prefix: u64,
    take_over: bool,
) -> Result<(), StorageRoleFailure> {
    let final_path = stage.final_destination.path();
    let fence = fence(stage)?;
    let _guard = stage.checkpoint_guard().await;
    if !take_over && !still_ours(adapter, stage).await? {
        return Err(taken_over(final_path));
    }
    let prefix = if stage.checkpoint_enabled() {
        stage.checkpoint_prefix()
    } else {
        prefix
    };
    let bytes = DestinationPointer {
        binding: stage.recovery_binding,
        transfer_identity: fence.transfer_identity,
        durable_prefix: Some(prefix),
        extension: fence.extension(),
        checkpoint: stage.checkpoint_snapshot(),
    }
    .encode()
    .map_err(|_| invalid(final_path, FailureClass::Internal))?;
    let pointer = artifact_path(final_path, ArtifactKind::Pointer, false)?;
    let temporary = artifact_path(final_path, ArtifactKind::Pointer, true)?;
    write_temporary(adapter, &temporary, &bytes)
        .await
        .map_err(|error| role_failure(final_path, Operation::Write, error))?;
    if let Err(rename_error) = adapter.protocol.rename(&temporary, &pointer).await {
        // RENAME can fail after the server committed it: read the pointer back before deciding.
        let committed = read_regular(adapter, &pointer, MAX_POINTER_BYTES + 1)
            .await
            .is_ok_and(|found| found.as_deref() == Some(bytes.as_slice()));
        let _ = adapter.protocol.delete(&temporary).await;
        if !committed {
            return Err(role_failure(final_path, Operation::Write, rename_error));
        }
    }
    fence.written.store(true, Ordering::Release);
    stage.checkpoint_recorded(prefix);
    Ok(())
}

/// Writes `bytes` to the fixed temporary: an earlier one is removed, then it is created
/// exclusively. `write_at` commits before it returns, so the temporary is stable before the rename.
async fn write_temporary(
    adapter: &NfsStagedDestinationAdapter,
    temporary: &StoragePath,
    bytes: &[u8],
) -> Result<(), NfsProtocolFailure> {
    delete_if_present(adapter, temporary).await?;
    let handle = adapter
        .protocol
        // Checkpoint temporaries may carry raw source fragments; keep them private from CREATE.
        .create_empty_open(temporary, 0o600)
        .await
        .map_err(transient_collision)?;
    let written = async {
        if bytes.starts_with(b"DMDPTR01") {
            return handle.write_at(0, Bytes::copy_from_slice(bytes)).await;
        }
        let maximum = adapter.protocol.maximum_write_chunk_bytes().max(1);
        let mut offset = 0;
        while offset < bytes.len() {
            let end = (offset + maximum).min(bytes.len());
            let count = handle
                .write_at(offset as u64, Bytes::copy_from_slice(&bytes[offset..end]))
                .await?;
            if count != (end - offset) as u64 {
                return Err(NfsProtocolFailure::new(
                    FailureClass::Corruption,
                    Transience::Unknown,
                ));
            }
            offset = end;
        }
        Ok(offset as u64)
    }
    .await;
    let closed = handle.close().await;
    let outcome = match (written, closed) {
        (Ok(count), Ok(())) if count == bytes.len() as u64 => return Ok(()),
        (Ok(_), Ok(())) => NfsProtocolFailure::new(FailureClass::Corruption, Transience::Unknown),
        (Err(error), _) | (_, Err(error)) => error,
    };
    let _ = adapter.protocol.delete(temporary).await;
    Err(outcome)
}

/// The durable prefix this stage's pointer proves, checked against the stage's size.
pub(super) async fn reobserve(
    adapter: &NfsStagedDestinationAdapter,
    stage: &PreparedStage,
) -> Result<u64, StorageRoleFailure> {
    let final_path = stage.final_destination.path();
    let fence = fence(stage)?;
    let pointer = artifact_path(final_path, ArtifactKind::Pointer, false)?;
    let bytes = read_regular(adapter, &pointer, MAX_POINTER_BYTES + 1)
        .await
        .map_err(|error| role_failure(final_path, Operation::Verify, error))?
        .ok_or_else(|| taken_over(final_path))?;
    let found = DestinationPointer::decode(&bytes)
        .map_err(|_| invalid(final_path, FailureClass::Corruption))?;
    if found.extension != fence.extension() {
        return Err(taken_over(final_path));
    }
    let prefix = found
        .durable_prefix
        .filter(|_| {
            found.binding == stage.recovery_binding
                && found.transfer_identity == fence.transfer_identity
        })
        .ok_or_else(|| invalid(final_path, FailureClass::Corruption))?;
    let native = adapter.validate(stage)?;
    let observed = if stage.recovered_publication.is_some() {
        final_path
    } else {
        &native
    };
    let (regular, size) = adapter
        .protocol
        .stat(observed)
        .await
        .map_err(|error| role_failure(final_path, Operation::Verify, error))?;
    if !regular || size < prefix {
        return Err(invalid(final_path, FailureClass::Corruption));
    }
    if let Some(encoded) = &found.checkpoint {
        let data = crate::storage::checkpoint::CheckpointData::decode(encoded)
            .map_err(|_| invalid(final_path, FailureClass::Corruption))?;
        if data.prefix() != prefix || data.ranges.iter().any(|range| range.end > size) {
            return Err(invalid(final_path, FailureClass::Corruption));
        }
    }
    Ok(prefix)
}

/// Before the stage becomes the final file: it must still be this stage's.
pub(super) async fn before_publication(
    adapter: &NfsStagedDestinationAdapter,
    stage: &PreparedStage,
) -> Result<(), StorageRoleFailure> {
    if still_ours(adapter, stage).await? {
        Ok(())
    } else {
        Err(taken_over(stage.final_destination.path()))
    }
}

/// Removes the pointer and its temporary once the stage is the final file (or gone). A stage that
/// never wrote a pointer has none to remove — and one there now is another writer's — so it sends
/// nothing: two fewer round trips per small file.
pub(super) async fn remove_pointer(
    adapter: &NfsStagedDestinationAdapter,
    stage: &PreparedStage,
) -> Result<(), StorageRoleFailure> {
    if !fence(stage)?.written.load(Ordering::Acquire) {
        return Ok(());
    }
    let final_path = stage.final_destination.path();
    for path in [
        artifact_path(final_path, ArtifactKind::Pointer, false)?,
        artifact_path(final_path, ArtifactKind::Pointer, true)?,
    ] {
        delete_if_present(adapter, &path)
            .await
            .map_err(|error| role_failure(final_path, Operation::Write, error))?;
    }
    Ok(())
}

/// Removes the stage and its pointer, the pointer first. A stage another writer took over is left
/// alone: its names are that writer's now.
pub(super) async fn discard(
    adapter: &NfsStagedDestinationAdapter,
    stage: &PreparedStage,
) -> Result<(), StorageRoleFailure> {
    let final_path = stage.final_destination.path();
    let path = stage_path(stage)?;
    let closed = adapter.close_prepared_handle(stage).await;
    if still_ours(adapter, stage).await? {
        remove_pointer(adapter, stage).await?;
        delete_if_present(adapter, &path)
            .await
            .map_err(|error| role_failure(final_path, Operation::Write, error))?;
    }
    adapter.release_authority(stage);
    closed
}

fn invalid(path: &StoragePath, class: FailureClass) -> StorageRoleFailure {
    entry_failure(path, Operation::Write, class, Transience::Permanent)
}

#[cfg(test)]
#[path = "at_destination_tests.rs"]
mod tests;
