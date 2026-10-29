//! Reconcile a retained source checkpoint with the final inode after a rename reply was lost.

use std::io;
use std::path::Path;
use std::sync::Arc;

use cap_std::fs::{Dir, OpenOptions};

use super::{LocalStagedDestination, at_destination, failure, io_failure};
use crate::model::{FailureClass, Operation, StoragePath};
use crate::storage::checkpoint::CheckpointPhase;
use crate::storage::durability::sync_directory;
use crate::storage::range_hash::RangeHash;
use crate::storage::{
    PreparedStage, PublicationDisposition, PublicationEvidence, StorageRoleFailure,
};

fn final_name(path: &StoragePath) -> Result<std::ffi::OsString, StorageRoleFailure> {
    Path::new(path.as_str())
        .file_name()
        .map(std::ffi::OsStr::to_owned)
        .ok_or_else(|| failure(path, Operation::Prepare, FailureClass::InvalidInput))
}

pub(super) async fn observe_final(
    directory: Arc<Dir>,
    path: &StoragePath,
) -> Result<Option<u64>, StorageRoleFailure> {
    // The existing non-Unix same_file fallback cannot pin a recovered final's identity.
    if !cfg!(unix) {
        return Err(failure(path, Operation::Prepare, FailureClass::Unsupported));
    }
    let name = final_name(path)?;
    tokio::task::spawn_blocking(move || {
        let mut options = OpenOptions::new();
        options.read(true);
        at_destination::open_regular(&directory, &name, &options)?
            .map(|file| file.metadata().map(|metadata| metadata.len()))
            .transpose()
    })
    .await
    .map_err(|_| failure(path, Operation::Prepare, FailureClass::Internal))?
    .map_err(|error| io_failure(path, Operation::Prepare, &error))
}

pub(super) async fn recover(
    stage: &mut PreparedStage,
    directory: Arc<Dir>,
    identity: [u8; 32],
) -> Result<(), StorageRoleFailure> {
    let path = stage.final_destination().path();
    let corrupt = || failure(path, Operation::Prepare, FailureClass::Corruption);
    let data = stage.restored_checkpoint().ok_or_else(corrupt)?;
    RangeHash::decode(&data.hash, data.size)
        .and_then(|hash| hash.finalize())
        .map_err(|_| corrupt())?;
    let name = final_name(path)?;
    let parent = Arc::clone(&directory);
    let file = tokio::task::spawn_blocking(move || {
        let mut options = OpenOptions::new();
        options.read(true);
        let file = at_destination::open_regular(&parent, &name, &options)?
            .filter(|file| {
                file.metadata()
                    .is_ok_and(|metadata| metadata.len() == data.size)
            })
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?;
        // A lost rename reply can precede the original parent-directory barrier.
        file.sync_data()?;
        sync_directory(&parent)?;
        Ok::<_, io::Error>(Arc::new(file))
    })
    .await
    .map_err(|_| corrupt())?
    .map_err(|error| io_failure(path, Operation::Prepare, &error))?;
    LocalStagedDestination::cache_stage(stage, directory, Some(file), false, Some(identity))?;
    stage.set_write_offset(data.size);
    stage.enable_checkpoint(data.size, data.hash);
    stage.checkpoint_set_phase(CheckpointPhase::Published);
    stage.set_recovered_publication(Some(PublicationEvidence::new(
        stage.final_destination().path().clone(),
        PublicationDisposition::Published,
        None,
    )));
    Ok(())
}

pub(super) fn file(stage: &PreparedStage) -> Result<Arc<std::fs::File>, StorageRoleFailure> {
    LocalStagedDestination::local_state(stage, Operation::Verify)?
        .and_then(|state| {
            state
                .file
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .map(Arc::clone)
        })
        .ok_or_else(|| {
            failure(
                stage.final_destination().path(),
                Operation::Verify,
                FailureClass::Corruption,
            )
        })
}

pub(super) async fn validate(
    adapter: &LocalStagedDestination,
    stage: &PreparedStage,
) -> Result<(), StorageRoleFailure> {
    let path = stage.final_destination().path();
    let name = final_name(path)?;
    stage
        .validate_owner(&adapter.identity)
        .map_err(|_| failure(path, Operation::Verify, FailureClass::Conflict))?;
    let relative = LocalStagedDestination::checked_relative(path, Operation::Verify)?;
    let parent = relative
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_owned();
    let root = Arc::clone(&adapter.root_dir);
    let file = file(stage)?;
    tokio::task::spawn_blocking(move || {
        // Re-resolve the business path: a renamed/replaced parent must not validate through
        // the stale staging capability that still holds the original pointer and claim.
        let directory = root.open_dir(parent)?;
        let seen = directory.symlink_metadata(name)?;
        if !seen.is_file() || !at_destination::same_file(&seen, &file.metadata()?) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "recovered final file was replaced",
            ));
        }
        Ok(())
    })
    .await
    .map_err(|_| failure(path, Operation::Verify, FailureClass::Internal))?
    .map_err(|error| io_failure(path, Operation::Verify, &error))
}
