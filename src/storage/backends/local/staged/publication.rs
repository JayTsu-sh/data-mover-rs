use std::io;
use std::path::Path;
#[cfg(test)]
use std::sync::atomic::Ordering;

use cap_std::fs::Dir;

use super::WriteProbe;
use crate::storage::durability::open_directory;
use crate::storage::{PublicationDisposition, PublishRequest};

#[derive(Clone, Copy)]
pub(super) struct Directories<'a> {
    pub(super) root: &'a Dir,
    pub(super) colocated: bool,
    pub(super) durable: bool,
    pub(super) staging: &'a Dir,
    pub(super) sync: &'a super::directory_sync::DirectorySync,
}

pub(super) struct LocalPublicationError {
    pub(super) error: io::Error,
    pub(super) committed: bool,
}

pub(super) fn publish_local(
    directories: Directories<'_>,
    stage_name: &std::ffi::OsStr,
    checkpoint_name: Option<&std::ffi::OsStr>,
    final_relative: &Path,
    request: &PublishRequest,
    probe: &WriteProbe,
) -> Result<PublicationDisposition, LocalPublicationError> {
    let Directories { root, staging, .. } = directories;
    let precommit = |error| LocalPublicationError {
        error,
        committed: false,
    };
    let parent_path = final_relative
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = super::LocalStagedDestination::open_or_create_parent(root, parent_path)
        .map_err(precommit)?;
    let final_relative = Path::new(
        final_relative
            .file_name()
            .ok_or_else(|| precommit(io::Error::from(io::ErrorKind::InvalidInput)))?,
    );
    let directories = Directories {
        root: &parent,
        ..directories
    };
    let root = directories.root;
    if request.cancel.is_cancelled() {
        return Err(precommit(io::Error::new(
            io::ErrorKind::Interrupted,
            "publication cancelled",
        )));
    }
    staging
        .rename(stage_name, root, final_relative)
        .map_err(precommit)?;
    finish_publication(
        directories,
        None,
        checkpoint_name,
        PublicationDisposition::Published,
        probe,
    )
}

fn finish_publication(
    directories: Directories<'_>,
    stage_name: Option<&std::ffi::OsStr>,
    checkpoint_name: Option<&std::ffi::OsStr>,
    disposition: PublicationDisposition,
    probe: &WriteProbe,
) -> Result<PublicationDisposition, LocalPublicationError> {
    let Directories { root, staging, .. } = directories;
    let committed = |error| LocalPublicationError {
        error,
        committed: true,
    };
    #[cfg(test)]
    if probe
        .fail_after_publication_commit
        .swap(false, Ordering::SeqCst)
    {
        return Err(committed(io::Error::other(
            "injected post-publication failure",
        )));
    }
    #[cfg(not(test))]
    let _ = probe;
    if let Some(stage_name) = stage_name {
        remove_if_present(staging, stage_name).map_err(committed)?;
    }
    if let Some(checkpoint_name) = checkpoint_name {
        remove_if_present(staging, checkpoint_name).map_err(committed)?;
    }
    if !directories.durable {
        return Ok(disposition);
    }
    #[cfg(test)]
    probe
        .final_directory_sync_calls
        .fetch_add(1, Ordering::SeqCst);
    let final_directory = open_directory(root).map_err(committed)?;
    #[cfg(unix)]
    {
        use cap_std::fs::MetadataExt as _;
        let final_metadata = final_directory.metadata().map_err(committed)?;
        // The staging capability already identifies the inode. Only reopen it for
        // fsync when it differs from the final parent (legacy or moved directory).
        let staging_metadata = staging.dir_metadata().map_err(committed)?;
        let same_directory = directories.colocated
            && final_metadata.dev() == staging_metadata.dev()
            && final_metadata.ino() == staging_metadata.ino();
        directories
            .sync
            .sync(final_directory, &final_metadata)
            .map_err(committed)?;
        if !same_directory {
            directories
                .sync
                .sync(
                    open_directory(staging).map_err(committed)?,
                    &staging_metadata,
                )
                .map_err(committed)?;
        }
    }
    #[cfg(not(unix))]
    {
        directories.sync.sync(final_directory).map_err(committed)?;
        directories
            .sync
            .sync(open_directory(staging).map_err(committed)?)
            .map_err(committed)?;
    }
    Ok(disposition)
}

pub(super) fn remove_if_present(directory: &Dir, name: &std::ffi::OsStr) -> io::Result<()> {
    match directory.remove_file(name) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
