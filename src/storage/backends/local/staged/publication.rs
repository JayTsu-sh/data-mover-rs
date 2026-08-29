use std::io::{self, Read as _};
use std::path::Path;
#[cfg(test)]
use std::sync::atomic::Ordering;

use cap_std::fs::Dir;

use super::WriteProbe;
use crate::storage::{ExistingDestinationPolicy, PublicationDisposition, PublishRequest};

pub(super) struct LocalPublicationError {
    pub(super) error: io::Error,
    pub(super) committed: bool,
}

struct ExistingGuard<'a> {
    staging: &'a Dir,
    name: std::ffi::OsString,
    armed: bool,
}

impl ExistingGuard<'_> {
    fn remove(&mut self) -> io::Result<()> {
        remove_if_present(self.staging, &self.name)?;
        self.armed = false;
        Ok(())
    }
}

impl Drop for ExistingGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = remove_if_present(self.staging, &self.name);
        }
    }
}

pub(super) fn publish_local(
    root: &Dir,
    staging: &Dir,
    stage_name: &std::ffi::OsStr,
    checkpoint_name: &std::ffi::OsStr,
    final_relative: &Path,
    request: &PublishRequest,
    probe: &WriteProbe,
) -> Result<PublicationDisposition, LocalPublicationError> {
    let precommit = |error| LocalPublicationError {
        error,
        committed: false,
    };
    if let Some(parent) = final_relative
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        root.create_dir_all(parent).map_err(precommit)?;
    }
    let existing = match root.open(final_relative) {
        Ok(file) => Some(file),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(precommit(error)),
    };
    if let Some(file) = existing {
        match request.policy {
            ExistingDestinationPolicy::FailIfExists => {
                return Err(precommit(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "final destination exists",
                )));
            }
            ExistingDestinationPolicy::VerifyOrSkip => {
                drop(file);
                return verify_and_rebind_existing(
                    root,
                    staging,
                    stage_name,
                    checkpoint_name,
                    final_relative,
                    request,
                    probe,
                );
            }
            ExistingDestinationPolicy::Overwrite => {}
        }
    }
    match request.policy {
        ExistingDestinationPolicy::FailIfExists | ExistingDestinationPolicy::VerifyOrSkip => {
            staging
                .hard_link(stage_name, root, final_relative)
                .map_err(precommit)?;
        }
        ExistingDestinationPolicy::Overwrite => {
            staging
                .rename(stage_name, root, final_relative)
                .map_err(precommit)?;
        }
    }
    finish_publication(
        root,
        staging,
        stage_name,
        checkpoint_name,
        final_relative,
        PublicationDisposition::Published,
        probe,
    )
}

fn verify_and_rebind_existing(
    root: &Dir,
    staging: &Dir,
    stage_name: &std::ffi::OsStr,
    checkpoint_name: &std::ffi::OsStr,
    final_relative: &Path,
    request: &PublishRequest,
    probe: &WriteProbe,
) -> Result<PublicationDisposition, LocalPublicationError> {
    let precommit = |error| LocalPublicationError {
        error,
        committed: false,
    };
    let mut guard_name = stage_name.to_os_string();
    guard_name.push(".existing");
    root.hard_link(final_relative, staging, &guard_name)
        .map_err(precommit)?;
    let mut guard = ExistingGuard {
        staging,
        name: guard_name,
        armed: true,
    };
    #[cfg(test)]
    if probe
        .replace_final_during_skip
        .swap(false, Ordering::SeqCst)
    {
        root.rename("replacement.bin", root, final_relative)
            .map_err(precommit)?;
    }
    let guard_file = staging.open(&guard.name).map_err(precommit)?.into_std();
    if let Err(error) = verify_existing(guard_file, request, probe) {
        return Err(precommit(error));
    }
    staging
        .rename(&guard.name, root, final_relative)
        .map_err(precommit)?;
    guard.remove().map_err(|error| LocalPublicationError {
        error,
        committed: true,
    })?;
    finish_publication(
        root,
        staging,
        stage_name,
        checkpoint_name,
        final_relative,
        PublicationDisposition::ExistingEquivalent,
        probe,
    )
}

fn finish_publication(
    root: &Dir,
    staging: &Dir,
    stage_name: &std::ffi::OsStr,
    checkpoint_name: &std::ffi::OsStr,
    final_relative: &Path,
    disposition: PublicationDisposition,
    probe: &WriteProbe,
) -> Result<PublicationDisposition, LocalPublicationError> {
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
    remove_if_present(staging, stage_name).map_err(committed)?;
    remove_if_present(staging, checkpoint_name).map_err(committed)?;
    let final_parent = final_relative
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    root.open(final_parent)
        .and_then(|directory| directory.sync_all())
        .map_err(committed)?;
    staging
        .open(".")
        .and_then(|directory| directory.sync_all())
        .map_err(committed)?;
    Ok(disposition)
}

fn verify_existing(
    mut file: std::fs::File,
    request: &PublishRequest,
    probe: &WriteProbe,
) -> io::Result<()> {
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut size = 0_u64;
    loop {
        #[cfg(test)]
        {
            probe.existing_verify_started.store(true, Ordering::SeqCst);
            if probe.slow_existing_verify.load(Ordering::SeqCst) {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        #[cfg(not(test))]
        let _ = probe;
        if request.cancel.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "publication cancelled",
            ));
        }
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        size = size
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "final size overflow"))?;
        if size > request.expected_size {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "final content differs",
            ));
        }
        hasher.update(&buffer[..read]);
    }
    if size != request.expected_size || *hasher.finalize().as_bytes() != request.expected_blake3 {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "final content differs",
        ));
    }
    Ok(())
}

pub(super) fn remove_if_present(directory: &Dir, name: &std::ffi::OsStr) -> io::Result<()> {
    match directory.remove_file(name) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
