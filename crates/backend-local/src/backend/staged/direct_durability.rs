//! Direct 只在最终发布阶段确认数据、元数据及目录项；不创建续传产物。
use std::path::{Component, Path};
#[cfg(any(test, feature = "test-support"))]
use std::sync::atomic::Ordering;
use std::{fmt, io};

use cap_std::fs::{Dir, MetadataExt as _};
use tokio_util::sync::CancellationToken;

use super::WriteProbe;
use super::directory_sync::DirectorySync;
use crate::storage::durability::open_directory;

#[derive(Debug)]
struct PersistenceIo {
    phase: &'static str,
    source: io::Error,
}

impl fmt::Display for PersistenceIo {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(output, "Direct {} failed: {}", self.phase, self.source)
    }
}

impl std::error::Error for PersistenceIo {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

fn context(phase: &'static str, source: io::Error) -> io::Error {
    io::Error::new(source.kind(), PersistenceIo { phase, source })
}

pub(super) fn sync(
    root: &Dir,
    file: &std::fs::File,
    opened_parent: &Dir,
    relative: &Path,
    sync: &DirectorySync,
    probe: &WriteProbe,
    cancel: &CancellationToken,
) -> io::Result<()> {
    let directories = resolve_parents(root, relative)?;
    let visible = directories
        .last()
        .ok_or(io::ErrorKind::InvalidInput)?
        .dir_metadata()?;
    let held = opened_parent.dir_metadata()?;
    if (visible.dev(), visible.ino()) != (held.dev(), held.ino()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Direct parent was replaced",
        ));
    }
    check_cancelled(cancel)?;
    sync_file(file, probe)?;
    for directory in directories.iter().rev() {
        sync_directory(directory, sync, probe, cancel)?;
    }
    Ok(())
}

fn sync_file(file: &std::fs::File, probe: &WriteProbe) -> io::Result<()> {
    #[cfg(any(test, feature = "test-support"))]
    {
        probe.final_data_sync_calls.fetch_add(1, Ordering::SeqCst);
        if probe.fail_direct_data_sync.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected Direct file persistence failure"));
        }
    }
    #[cfg(not(any(test, feature = "test-support")))]
    let _ = probe;
    file.sync_all()
        .map_err(|error| context("file persistence", error))
}

fn sync_directory(
    directory: &Dir,
    sync: &DirectorySync,
    probe: &WriteProbe,
    cancel: &CancellationToken,
) -> io::Result<()> {
    check_cancelled(cancel)?;
    #[cfg(any(test, feature = "test-support"))]
    {
        probe
            .final_directory_sync_calls
            .fetch_add(1, Ordering::SeqCst);
        if probe.fail_direct_directory_sync.load(Ordering::SeqCst) {
            return Err(io::Error::other(
                "injected Direct directory persistence failure",
            ));
        }
    }
    #[cfg(not(any(test, feature = "test-support")))]
    let _ = probe;
    let file = open_directory(directory)
        .map_err(|error| context("directory open for persistence", error))?;
    let metadata = file
        .metadata()
        .map_err(|error| context("directory identity for persistence", error))?;
    sync.sync(file, &metadata)
        .map_err(|error| context("directory persistence", error))
}

fn resolve_parents(root: &Dir, relative: &Path) -> io::Result<Vec<Dir>> {
    let mut directories = vec![root.try_clone()?];
    let parent = relative.parent().ok_or(io::ErrorKind::InvalidInput)?;
    for part in parent.components() {
        let Component::Normal(name) = part else {
            return Err(io::ErrorKind::InvalidInput.into());
        };
        let next = directories
            .last()
            .ok_or(io::ErrorKind::InvalidInput)?
            .open_dir(name)?;
        directories.push(next);
    }
    Ok(directories)
}

fn check_cancelled(cancel: &CancellationToken) -> io::Result<()> {
    if cancel.is_cancelled() {
        Err(io::ErrorKind::Interrupted.into())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_replaced_parent_is_refused_before_any_persistence_success()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("parent"))?;
        std::fs::write(root.path().join("parent/file"), b"held payload")?;
        let directory = Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
        let held = directory.open_dir("parent")?;
        let file = held.open("file")?.into_std();
        std::fs::rename(root.path().join("parent"), root.path().join("moved"))?;
        std::fs::create_dir(root.path().join("parent"))?;
        std::fs::write(root.path().join("parent/file"), b"unrelated replacement")?;
        let probe = WriteProbe::default();
        let error = sync(
            &directory,
            &file,
            &held,
            Path::new("parent/file"),
            &DirectorySync::default(),
            &probe,
            &CancellationToken::new(),
        )
        .err()
        .ok_or("replaced parent was accepted")?;
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("parent was replaced"));
        assert_eq!(probe.final_data_sync_calls.load(Ordering::SeqCst), 0);
        assert_eq!(probe.final_directory_sync_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            std::fs::read(root.path().join("parent/file"))?,
            b"unrelated replacement"
        );
        assert_eq!(
            std::fs::read(root.path().join("moved/file"))?,
            b"held payload"
        );
        Ok(())
    }
}
