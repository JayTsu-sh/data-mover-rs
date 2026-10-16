//! Directory handles used for durable namespace changes.
use std::io;

use cap_std::fs::{Dir, File};
#[cfg(windows)]
use cap_std::fs::{OpenOptions, OpenOptionsExt as _};
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;

/// Keep resolution relative to the existing directory capability.
/// Windows needs both the directory flag and write access for `FlushFileBuffers`.
pub(crate) fn open_directory(directory: &Dir) -> io::Result<File> {
    #[cfg(windows)]
    {
        directory.open_with(
            ".",
            OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS),
        )
    }
    #[cfg(not(windows))]
    directory.open(".")
}

pub(crate) fn sync_directory(directory: &Dir) -> io::Result<()> {
    open_directory(directory)?.sync_all()
}
