//! Destination-resident recovery for the Local stage (ADR-0006 C8). Every artifact of a final file
//! sits beside it under a deterministic name: the stage, the pointer that records its binding and
//! durable prefix, and the claim whose lock keeps other processes out while the stage lives.

use std::ffi::{OsStr, OsString};
#[cfg(unix)]
use std::fs::Permissions;
use std::io::{self, Read as _};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError};
#[cfg(test)]
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
#[cfg(unix)]
use cap_std::fs::MetadataExt as _;
use cap_std::fs::{Dir, OpenOptions};

use super::{LocalStagedDestination, failure, failure_with_transience, io_failure, publication};
use crate::model::{FailureClass, Operation, StoragePath, Transience};
use crate::storage::artifacts::{ArtifactKind, artifact_name, artifact_temporary_name};
use crate::storage::discovery::{DestinationArtifacts, Discovery, discover};
use crate::storage::durability::sync_directory;
use crate::storage::pointer::DestinationPointer;
use crate::storage::{
    DestinationPrepareRequest, PreparedStage, PublicationDisposition, PublicationEvidence,
    PublicationFailure, StorageRoleFailure,
};
#[cfg(unix)]
use crate::storage::{PrepareFact, PrepareRequest, ResumeMode};

/// The extension every Local pointer carries: the pointer is the stage's checkpoint record.
pub(super) const POINTER_TAG: &[u8] = b"DMLSTG03";

/// How often a claim is retried when the file it locked was unlinked or replaced meanwhile.
const CLAIM_ATTEMPTS: usize = 3;

/// The name of one artifact of a final file, as it sits in the final file's directory.
pub(super) fn artifact(
    final_path: &StoragePath,
    kind: ArtifactKind,
    temporary: bool,
) -> Result<OsString, StorageRoleFailure> {
    let name = final_name(final_path)?;
    Ok(if temporary {
        artifact_temporary_name(name, kind)
    } else {
        artifact_name(name, kind)
    }
    .into())
}

fn final_name(final_path: &StoragePath) -> Result<&str, StorageRoleFailure> {
    Path::new(final_path.as_str())
        .file_name()
        .and_then(OsStr::to_str)
        .ok_or_else(|| failure(final_path, Operation::Prepare, FailureClass::InvalidInput))
}

/// The pointer a Local stage writes at a checkpoint.
pub(super) fn pointer(
    stage: &PreparedStage,
    transfer_identity: [u8; 32],
    durable_prefix: u64,
) -> DestinationPointer {
    DestinationPointer {
        binding: stage.recovery_binding,
        transfer_identity,
        durable_prefix: Some(durable_prefix),
        extension: Bytes::from_static(POINTER_TAG),
    }
}

/// Whether a pointer is one a Local stage wrote: tagged, with a durable prefix. A pointer without
/// a prefix would resume from the stage's length, which may be sparse.
fn accepts(pointer: &DestinationPointer) -> bool {
    pointer.durable_prefix.is_some() && pointer.extension.as_ref() == POINTER_TAG
}

/// The durable prefix a read-back pointer proves for this stage, or `None` if the pointer is not
/// this stage's.
pub(super) fn proven_prefix(
    stage: &PreparedStage,
    transfer_identity: [u8; 32],
    record: &[u8],
) -> Option<u64> {
    let pointer = DestinationPointer::decode(record).ok()?;
    (accepts(&pointer)
        && pointer.binding == stage.recovery_binding
        && pointer.transfer_identity == transfer_identity)
        .then_some(pointer.durable_prefix)
        .flatten()
}

fn not_regular() -> io::Error {
    // `AlreadyExists` maps to a Conflict: something else holds an artifact's name.
    io::Error::new(
        io::ErrorKind::AlreadyExists,
        "a transfer artifact name is not a regular file",
    )
}

#[cfg(unix)]
fn same_file(seen: &cap_std::fs::Metadata, opened: &std::fs::Metadata) -> bool {
    seen.dev() == opened.dev() && seen.ino() == opened.ino()
}

#[cfg(not(unix))]
fn same_file(_seen: &cap_std::fs::Metadata, _opened: &std::fs::Metadata) -> bool {
    true
}

/// Opens `name` only if it is a regular file, never through a symlink: the name is looked at
/// without following, opened, and the opened file must be the one looked at. `None` if absent.
fn open_regular(
    directory: &Dir,
    name: &OsStr,
    options: &OpenOptions,
) -> io::Result<Option<std::fs::File>> {
    let seen = match directory.symlink_metadata(name) {
        Ok(seen) => seen,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if !seen.is_file() {
        return Err(not_regular());
    }
    let file = directory.open_with(name, options)?.into_std();
    if !same_file(&seen, &file.metadata()?) {
        return Err(not_regular());
    }
    Ok(Some(file))
}

/// Takes the claim: opens or creates it and locks it without waiting. A lock on a claim that was
/// unlinked or replaced meanwhile belongs to no one, so the name is checked again after locking.
/// `WouldBlock` while another process holds it.
fn acquire_claim(directory: &Dir, name: &OsStr) -> io::Result<std::fs::File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    for _ in 0..CLAIM_ATTEMPTS {
        match directory.symlink_metadata(name) {
            Ok(seen) if !seen.is_file() => return Err(not_regular()),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let file = directory.open_with(name, &options)?.into_std();
        file.try_lock()?;
        match directory.symlink_metadata(name) {
            Ok(seen) if same_file(&seen, &file.metadata()?) => return Ok(file),
            Ok(seen) if !seen.is_file() => return Err(not_regular()),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::from(io::ErrorKind::WouldBlock))
}

/// Removes the claim and then lets it go: the name goes while the lock is held, so no one can
/// lock the old file after the holder is done (std opens files with delete sharing on Windows).
fn remove_claim(held: Option<std::fs::File>, directory: &Dir, name: &OsStr) -> io::Result<()> {
    let removed = publication::remove_if_present(directory, name);
    drop(held);
    removed
}

fn take_claim(stage: &PreparedStage) -> Option<std::fs::File> {
    stage
        .claim
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
}

fn give_back_claim(stage: &PreparedStage, claim: std::fs::File) {
    *stage.claim.lock().unwrap_or_else(PoisonError::into_inner) = Some(claim);
}

fn holds_claim(stage: &PreparedStage) -> bool {
    stage
        .claim
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .is_some()
}

/// A final file's artifacts in its directory, as the discovery driver looks at them.
struct LocalArtifacts {
    directory: Arc<Dir>,
    stage: OsString,
    pointer: OsString,
    pointer_temporary: OsString,
}

impl LocalArtifacts {
    async fn blocking<T: Send + 'static>(
        &self,
        final_path: &StoragePath,
        work: impl FnOnce(&Dir) -> io::Result<T> + Send + 'static,
    ) -> Result<T, StorageRoleFailure> {
        let directory = Arc::clone(&self.directory);
        tokio::task::spawn_blocking(move || work(&directory))
            .await
            .map_err(|_| failure(final_path, Operation::Prepare, FailureClass::Internal))?
            .map_err(|error| io_failure(final_path, Operation::Prepare, &error))
    }
}

#[async_trait]
impl DestinationArtifacts for LocalArtifacts {
    async fn read_pointer(
        &self,
        final_path: &StoragePath,
        limit: usize,
    ) -> Result<Option<Vec<u8>>, StorageRoleFailure> {
        let name = self.pointer.clone();
        self.blocking(final_path, move |directory| {
            let mut options = OpenOptions::new();
            options.read(true);
            let Some(file) = open_regular(directory, &name, &options)? else {
                return Ok(None);
            };
            let mut bytes = Vec::new();
            file.take(limit as u64).read_to_end(&mut bytes)?;
            Ok(Some(bytes))
        })
        .await
    }

    /// The stage's length confirms it exists and is not shorter than the pointer's prefix; it is
    /// never the resume offset, because [`Self::accepts_pointer`] refuses a pointer without one.
    async fn observe_stage(
        &self,
        final_path: &StoragePath,
    ) -> Result<Option<u64>, StorageRoleFailure> {
        let name = self.stage.clone();
        self.blocking(final_path, move |directory| {
            match directory.symlink_metadata(&name) {
                Ok(seen) if seen.is_file() => Ok(Some(seen.len())),
                Ok(_) => Err(not_regular()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(error),
            }
        })
        .await
    }

    async fn remove_pointer(&self, final_path: &StoragePath) -> Result<(), StorageRoleFailure> {
        let (pointer, temporary) = (self.pointer.clone(), self.pointer_temporary.clone());
        self.blocking(final_path, move |directory| {
            publication::remove_if_present(directory, &pointer)?;
            publication::remove_if_present(directory, &temporary)
        })
        .await
    }

    async fn remove_stage(&self, final_path: &StoragePath) -> Result<(), StorageRoleFailure> {
        let stage = self.stage.clone();
        self.blocking(final_path, move |directory| {
            publication::remove_if_present(directory, &stage)
        })
        .await
    }

    fn accepts_pointer(&self, pointer: &DestinationPointer) -> bool {
        accepts(pointer)
    }
}

/// Prepares a stage at the destination: takes the claim, looks at what is there, and resumes
/// or starts over as the decision table says.
pub(super) async fn prepare(
    adapter: &LocalStagedDestination,
    request: DestinationPrepareRequest,
) -> Result<PreparedStage, StorageRoleFailure> {
    LocalStagedDestination::validate_prepare_request(&request.prepare)?;
    let final_path = request.prepare.final_destination.path().clone();
    let relative = LocalStagedDestination::checked_relative(&final_path, Operation::Prepare)?;
    let names = [
        artifact(&final_path, ArtifactKind::Stage, false)?,
        artifact(&final_path, ArtifactKind::Pointer, false)?,
        artifact(&final_path, ArtifactKind::Pointer, true)?,
        artifact(&final_path, ArtifactKind::Claim, false)?,
    ];
    let [stage_name, pointer, pointer_temporary, claim_name] = names;
    let (directory, claim) = claim_directory(adapter, &relative, claim_name, &final_path).await?;
    let artifacts = LocalArtifacts {
        directory: Arc::clone(&directory),
        stage: stage_name,
        pointer,
        pointer_temporary,
    };
    let mut stage = PreparedStage::new(
        adapter.identity.clone(),
        request.prepare.final_destination.clone(),
        Bytes::from(stage_token(&relative, &artifacts.stage)),
        request.prepare.recovery_binding,
        0,
        Some(claim),
    );
    let found = match look(&artifacts, &request, &final_path).await {
        Ok(found) => found,
        Err(error) => return Err(abandon(&stage, &directory, error).await),
    };
    stage.mark_at_destination(found.fact);
    let Some(point) = found.resume else {
        return start(adapter, stage, &artifacts, &request).await;
    };
    let reopened = reopen_stage(&artifacts, &final_path, point.prefix)
        .await
        .and_then(|file| {
            cache(
                &mut stage,
                &directory,
                file,
                request.transfer_identity,
                false,
            )
        });
    if let Err(error) = reopened {
        return Err(abandon(&stage, &directory, error).await);
    }
    stage.write_offset = point.prefix;
    Ok(stage)
}

/// Before a direct write of the final file: removes what a checkpointed run left for it, as a
/// restarting prepare would, and lets the claim go again. A live stage of another process holds
/// the claim, so the direct write is refused (`Conflict`, transient) rather than racing it. The
/// usual case — nothing there — costs three `lstat`s and takes no claim.
#[cfg(unix)]
pub(super) async fn clear_before_direct(
    adapter: &LocalStagedDestination,
    request: &PrepareRequest,
) -> Result<PrepareFact, StorageRoleFailure> {
    let final_path = request.final_destination.path().clone();
    let relative = LocalStagedDestination::checked_relative(&final_path, Operation::Prepare)?;
    let parent = relative.parent().map(Path::to_path_buf).unwrap_or_default();
    let names = [
        parent.join(artifact(&final_path, ArtifactKind::Stage, false)?),
        parent.join(artifact(&final_path, ArtifactKind::Pointer, false)?),
        parent.join(artifact(&final_path, ArtifactKind::Pointer, true)?),
    ];
    let root = Arc::clone(&adapter.root_dir);
    let found = tokio::task::spawn_blocking(move || {
        for name in &names {
            match root.symlink_metadata(name) {
                Ok(_) => return Ok(true),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                // A missing or non-directory parent holds no artifacts either.
                Err(error) if error.kind() == io::ErrorKind::NotADirectory => {}
                Err(error) => return Err(error),
            }
        }
        Ok(false)
    })
    .await
    .map_err(|_| failure(&final_path, Operation::Prepare, FailureClass::Internal))?
    .map_err(|error| io_failure(&final_path, Operation::Prepare, &error))?;
    if !found {
        return Ok(PrepareFact::Fresh);
    }
    let restart = DestinationPrepareRequest::new(request.clone(), [0; 32])
        .with_resume(ResumeMode::Restart)
        .with_recoverable(false);
    let stage = prepare(adapter, restart).await?;
    let fact = stage.prepare_fact;
    cleanup(adapter, &stage, Operation::Prepare).await?;
    Ok(fact)
}

/// Opens (or creates) the final file's directory and takes the claim in it.
async fn claim_directory(
    adapter: &LocalStagedDestination,
    relative: &Path,
    claim_name: OsString,
    final_path: &StoragePath,
) -> Result<(Arc<Dir>, std::fs::File), StorageRoleFailure> {
    let parent = relative
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_owned();
    let root = Arc::clone(&adapter.root_dir);
    tokio::task::spawn_blocking(move || {
        let directory = LocalStagedDestination::open_or_create_parent(&root, &parent)?;
        let claim = acquire_claim(&directory, &claim_name)?;
        Ok::<_, io::Error>((Arc::new(directory), claim))
    })
    .await
    .map_err(|_| failure(final_path, Operation::Prepare, FailureClass::Internal))?
    .map_err(|error| claim_failure(final_path, &error))
}

/// Discovery, then the sweep of every other leftover of the same final name, both under the claim.
async fn look(
    artifacts: &LocalArtifacts,
    request: &DestinationPrepareRequest,
    final_path: &StoragePath,
) -> Result<Discovery, StorageRoleFailure> {
    let found = discover(artifacts, request).await?;
    sweep(&artifacts.directory, final_path).await?;
    Ok(found)
}

/// Every name derived from the final file's name that is neither its live stage, its pointer nor
/// its claim: a pointer temporary a crash left beside nothing (which discovery never looks at),
/// and the kinds and temporaries Local does not write. Removed under the claim, by name — the
/// directory is never listed, so other files' artifacts are never touched.
fn leftover_names(final_path: &StoragePath) -> Result<Vec<OsString>, StorageRoleFailure> {
    let mut names = Vec::new();
    for kind in ArtifactKind::ALL {
        for temporary in [false, true] {
            let live = !temporary
                && matches!(
                    kind,
                    ArtifactKind::Stage | ArtifactKind::Pointer | ArtifactKind::Claim
                );
            if !live {
                names.push(artifact(final_path, kind, temporary)?);
            }
        }
    }
    Ok(names)
}

async fn sweep(directory: &Arc<Dir>, final_path: &StoragePath) -> Result<(), StorageRoleFailure> {
    let names = leftover_names(final_path)?;
    let directory = Arc::clone(directory);
    tokio::task::spawn_blocking(move || {
        for name in &names {
            match publication::remove_if_present(&directory, name) {
                // Something that is not a file holds a reserved name: a conflict, as at the
                // stage and pointer names.
                Err(error) if error.kind() == io::ErrorKind::IsADirectory => {
                    return Err(not_regular());
                }
                other => other?,
            }
        }
        Ok::<_, io::Error>(())
    })
    .await
    .map_err(|_| failure(final_path, Operation::Prepare, FailureClass::Internal))?
    .map_err(|error| io_failure(final_path, Operation::Prepare, &error))
}

fn claim_failure(final_path: &StoragePath, error: &io::Error) -> StorageRoleFailure {
    if error.kind() == io::ErrorKind::WouldBlock {
        failure_with_transience(
            final_path,
            Operation::Prepare,
            FailureClass::Conflict,
            Transience::Transient,
        )
    } else {
        io_failure(final_path, Operation::Prepare, error)
    }
}

fn stage_token(relative: &Path, stage_name: &OsStr) -> String {
    relative
        .parent()
        .map_or_else(PathBuf::new, Path::to_path_buf)
        .join(stage_name)
        .to_string_lossy()
        .into_owned()
}

fn cache(
    stage: &mut PreparedStage,
    directory: &Arc<Dir>,
    file: std::fs::File,
    transfer_identity: [u8; 32],
    fresh: bool,
) -> Result<(), StorageRoleFailure> {
    LocalStagedDestination::cache_stage(
        stage,
        Arc::clone(directory),
        Some(Arc::new(file)),
        fresh,
        Some(transfer_identity),
    )
}

/// Gives up a prepare before it created a stage: removes the claim and returns `error`. What
/// discovery found stays for the next prepare.
async fn abandon(
    stage: &PreparedStage,
    directory: &Arc<Dir>,
    error: StorageRoleFailure,
) -> StorageRoleFailure {
    let held = take_claim(stage);
    let Ok(claim) = artifact(stage.final_destination.path(), ArtifactKind::Claim, false) else {
        return error;
    };
    let directory = Arc::clone(directory);
    let _ = tokio::task::spawn_blocking(move || remove_claim(held, &directory, &claim)).await;
    error
}

/// Opens the resumed stage for writing and drops what lies past the durable prefix. A stage whose
/// metadata was applied before the crash may have lost its owner's write permission; that is
/// restored first (the metadata is applied again before publication).
async fn reopen_stage(
    artifacts: &LocalArtifacts,
    final_path: &StoragePath,
    prefix: u64,
) -> Result<std::fs::File, StorageRoleFailure> {
    let name = artifacts.stage.clone();
    artifacts
        .blocking(final_path, move |directory| {
            let mut writable = OpenOptions::new();
            writable.read(true).write(true);
            let file = match open_regular(directory, &name, &writable) {
                Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                    restore_owner_write(directory, &name)?;
                    open_regular(directory, &name, &writable)?
                }
                other => other?,
            }
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
            file.set_len(prefix)?;
            Ok(file)
        })
        .await
}

#[cfg(unix)]
fn restore_owner_write(directory: &Dir, name: &OsStr) -> io::Result<()> {
    let mut readable = OpenOptions::new();
    readable.read(true);
    let file = open_regular(directory, name, &readable)?
        .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
    let mode = file.metadata()?.permissions().mode();
    file.set_permissions(Permissions::from_mode(mode | 0o200))
}

#[cfg(not(unix))]
fn restore_owner_write(_directory: &Dir, _name: &OsStr) -> io::Result<()> {
    Err(io::Error::from(io::ErrorKind::PermissionDenied))
}

/// Creates a new stage. A recoverable one is made durable and gets its pointer at prefix zero
/// before any byte is written; any other gets its first pointer at the first checkpoint.
async fn start(
    adapter: &LocalStagedDestination,
    mut stage: PreparedStage,
    artifacts: &LocalArtifacts,
    request: &DestinationPrepareRequest,
) -> Result<PreparedStage, StorageRoleFailure> {
    let final_path = stage.final_destination.path().clone();
    let name = artifacts.stage.clone();
    let recoverable = request.recoverable;
    let created = artifacts
        .blocking(&final_path, move |directory| {
            let mut options = OpenOptions::new();
            options.create_new(true).read(true).write(true);
            let file = directory.open_with(&name, &options)?.into_std();
            let synced = if recoverable {
                file.sync_all().and_then(|()| sync_directory(directory))
            } else {
                Ok(())
            };
            match synced {
                Ok(()) => Ok(file),
                Err(error) => {
                    let _ = publication::remove_if_present(directory, &name);
                    Err(error)
                }
            }
        })
        .await;
    let file = match created {
        Ok(file) => file,
        Err(error) => return Err(abandon(&stage, &artifacts.directory, error).await),
    };
    let cached = cache(
        &mut stage,
        &artifacts.directory,
        file,
        request.transfer_identity,
        true,
    );
    if !recoverable && cached.is_ok() {
        return Ok(stage.disable_recovery());
    }
    let persisted = match cached {
        Ok(()) => adapter.persist_checkpoint(&stage, 0).await,
        Err(error) => Err(error),
    };
    if let Err(error) = persisted {
        return Err(match cleanup(adapter, &stage, Operation::Prepare).await {
            Ok(()) => error,
            Err(cleanup) => cleanup,
        });
    }
    Ok(stage)
}

/// Removes a stage and its artifacts: the pointer first, so a crash part-way never leaves a
/// pointer to resume from; the claim last, while still held. A stage that no longer holds its
/// claim (its publication finished and let it go) removes nothing: the names may be another
/// process's by now.
pub(super) async fn cleanup(
    adapter: &LocalStagedDestination,
    stage: &PreparedStage,
    operation: Operation,
) -> Result<(), StorageRoleFailure> {
    let path = stage.final_destination.path();
    // A stage that is not this adapter's, or whose token is not the stage beside its final file,
    // fails here, before anything is looked at.
    let names = [
        artifact(path, ArtifactKind::Pointer, false)?,
        artifact(path, ArtifactKind::Pointer, true)?,
        adapter.stage_name(stage, operation)?,
    ];
    if !holds_claim(stage) {
        return Ok(());
    }
    let claim = artifact(path, ArtifactKind::Claim, false)?;
    let directory = adapter.stage_directory(stage, operation).await?;
    let contents = Arc::clone(&directory);
    tokio::task::spawn_blocking(move || {
        for name in &names {
            publication::remove_if_present(&contents, name)?;
        }
        sync_directory(&contents)
    })
    .await
    .map_err(|_| failure(path, operation, FailureClass::Internal))?
    .map_err(|error| io_failure(path, operation, &error))?;

    #[cfg(test)]
    {
        adapter
            .write_probe
            .discard_contents_removed
            .store(true, Ordering::SeqCst);
        if adapter
            .write_probe
            .slow_discard_before_release
            .load(Ordering::SeqCst)
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    let held = take_claim(stage);
    tokio::task::spawn_blocking(move || remove_claim(held, &directory, &claim))
        .await
        .map_err(|_| failure(path, operation, FailureClass::Internal))?
        .map_err(|error| io_failure(path, operation, &error))
}

/// After the stage became the final file (and the pointer went with the commit): removes a
/// leftover pointer temporary, then the claim. The claim is let go only once nothing else can
/// fail; until then a failure keeps it, and the clean-up that follows still owns the names.
pub(super) async fn finish_publication(
    adapter: &LocalStagedDestination,
    stage: &PreparedStage,
    final_destination: StoragePath,
    disposition: PublicationDisposition,
) -> Result<PublicationEvidence, PublicationFailure> {
    let committed = |error| PublicationFailure {
        error,
        final_destination_changed: true,
    };
    let temporary = artifact(&final_destination, ArtifactKind::Pointer, true).map_err(committed)?;
    let claim = artifact(&final_destination, ArtifactKind::Claim, false).map_err(committed)?;
    let directory = adapter
        .stage_directory(stage, Operation::Publish)
        .await
        .map_err(committed)?;
    let held = take_claim(stage);
    let (removed, kept) =
        tokio::task::spawn_blocking(move || {
            match publication::remove_if_present(&directory, &temporary) {
                Ok(()) => (remove_claim(held, &directory, &claim), None),
                Err(error) => (Err(error), held),
            }
        })
        .await
        .map_err(|_| {
            committed(failure(
                &final_destination,
                Operation::Publish,
                FailureClass::Internal,
            ))
        })?;
    if let Some(claim) = kept {
        give_back_claim(stage, claim);
    }
    removed
        .map_err(|error| committed(io_failure(&final_destination, Operation::Publish, &error)))?;
    Ok(PublicationEvidence {
        final_destination,
        disposition,
        version: None,
    })
}

#[cfg(test)]
#[path = "at_destination_tests.rs"]
mod tests;
