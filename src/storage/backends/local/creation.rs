//! Creation defaults for data entries. Source metadata replaces private creation permissions
//! before publication; existing entries and transfer artifacts keep their own permissions.

use std::ffi::OsStr;
use std::io;
#[cfg(target_os = "linux")]
use std::{
    fs::{self, Permissions as StdPermissions},
    os::fd::AsRawFd,
    os::unix::fs::PermissionsExt as _,
    path::Path,
};

#[cfg(target_os = "linux")]
use cap_primitives::fs::open_dir_nofollow;
use cap_std::fs::{Dir, File, OpenOptions};
#[cfg(target_os = "linux")]
use cap_std::fs::{DirBuilder, DirBuilderExt as _, OpenOptionsExt as _, Permissions};

#[cfg(target_os = "linux")]
use super::super::{DEFAULT_DIRECTORY_MODE, DEFAULT_FILE_MODE};

/// Exclusively creates a data file, so defaults never chmod an existing direct destination.
pub(super) fn create_file(
    directory: &Dir,
    name: &OsStr,
    options: &OpenOptions,
    private: bool,
) -> io::Result<File> {
    let mut options = options.clone();
    options.create_new(true);
    #[cfg(target_os = "linux")]
    let mode = if private { 0o600 } else { DEFAULT_FILE_MODE };
    #[cfg(target_os = "linux")]
    options.mode(mode);
    #[cfg(not(target_os = "linux"))]
    let _ = private;
    let file = directory.open_with(name, &options)?;
    #[cfg(target_os = "linux")]
    {
        // open(2) still applies umask. Correct only the newly created inode before writing,
        // without changing the process-wide umask or permissions of existing files.
        let permissions = StdPermissions::from_mode(mode);
        if let Err(error) = file.set_permissions(Permissions::from_std(permissions)) {
            let _ = directory.remove_file(name);
            return Err(error);
        }
    }
    Ok(file)
}

/// Creates one directory with the data-directory default, without following a replacement link.
pub(super) fn create_directory(parent: &Dir, name: &OsStr) -> io::Result<()> {
    #[cfg(not(target_os = "linux"))]
    parent.create_dir(name)?;
    #[cfg(target_os = "linux")]
    {
        let mut options = DirBuilder::new();
        parent.create_dir_with(name, options.mode(DEFAULT_DIRECTORY_MODE))?;
        let parent_file = parent.try_clone()?.into_std_file();
        let directory = open_dir_nofollow(&parent_file, Path::new(name))?;
        set_directory_permissions(&directory)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn set_directory_permissions(directory: &fs::File) -> io::Result<()> {
    // cap-std directories use O_PATH, for which fchmod returns EBADF. Resolve the held
    // descriptor through procfs directly: resolving "." inside a new mode-000 directory
    // would require the execute permission that we are about to restore. No mutable
    // directory entry participates in this chmod; the descriptor remains live throughout.
    let descriptor = format!("/proc/self/fd/{}", directory.as_raw_fd());
    fs::set_permissions(
        descriptor,
        StdPermissions::from_mode(DEFAULT_DIRECTORY_MODE),
    )
}
