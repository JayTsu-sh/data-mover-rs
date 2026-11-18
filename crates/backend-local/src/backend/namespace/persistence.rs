//! 已发布条目的持久化确认；不创建 stage、不读文件体、不修改数据或属性。
use std::ffi::OsStr;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::{fmt, io};

use cap_std::ambient_authority;
use cap_std::fs::{Dir, Metadata, MetadataExt as _, OpenOptions, OpenOptionsExt as _};
use tokio_util::sync::CancellationToken;

use super::{LocalNamespace, checked, entry_failure, io_failure, open_directory};
use crate::model::{
    BackendIdentity, EntryKind, EntryOperationFailure, FailureClass, Operation, StoragePath,
    Transience,
};
use crate::storage::SourceDescriptor;
use crate::storage::StorageRoleFailure;
use crate::storage::backends::local::observation::{content_version, entry_kind, source_identity};
use crate::storage::durability::open_directory as open_for_sync;

pub(super) struct RootAnchor {
    path: PathBuf,
    #[cfg(test)]
    directory_sync_calls: AtomicUsize,
    #[cfg(test)]
    fail_file_sync: AtomicBool,
    #[cfg(test)]
    fail_directory_sync: AtomicBool,
}

impl RootAnchor {
    pub(super) fn new(path: PathBuf) -> Self {
        Self {
            path,
            #[cfg(test)]
            directory_sync_calls: AtomicUsize::new(0),
            #[cfg(test)]
            fail_file_sync: AtomicBool::new(false),
            #[cfg(test)]
            fail_directory_sync: AtomicBool::new(false),
        }
    }

    fn open_chain(&self, cancel: &CancellationToken) -> io::Result<Vec<Dir>> {
        if !self.path.is_absolute() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        check_cancelled(cancel)?;
        let mut directories = vec![Dir::open_ambient_dir("/", ambient_authority())?];
        for part in self.path.components() {
            if part == Component::RootDir {
                continue;
            }
            let Component::Normal(name) = part else {
                return Err(io::ErrorKind::InvalidInput.into());
            };
            check_cancelled(cancel)?;
            let parent = directories.last().ok_or(io::ErrorKind::InvalidInput)?;
            directories.push(open_directory(parent, Path::new(name))?);
        }
        Ok(directories)
    }

    fn verified_chain(&self, root: &Dir, cancel: &CancellationToken) -> io::Result<Vec<Dir>> {
        // 每次重验根绑定；同 inode 移走再移回也不能复用旧目录项的持久性。
        let directories = self.open_chain(cancel)?;
        check_cancelled(cancel)?;
        let visible = directories
            .last()
            .ok_or(io::ErrorKind::InvalidInput)?
            .dir_metadata()?;
        check_cancelled(cancel)?;
        let held = root.dir_metadata()?;
        if (visible.dev(), visible.ino()) != (held.dev(), held.ino()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "configured endpoint root no longer matches the opened capability",
            ));
        }
        Ok(directories)
    }

    // 生产构建没有故障注入状态。
    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn sync_directory(&self, directory: &Dir, cancel: &CancellationToken) -> io::Result<()> {
        check_cancelled(cancel)?;
        #[cfg(test)]
        self.directory_sync_calls.fetch_add(1, Ordering::SeqCst);
        #[cfg(test)]
        if self.fail_directory_sync.load(Ordering::SeqCst) {
            return Err(io::Error::other(
                "injected published directory persistence failure",
            ));
        }
        let file = open_for_sync(directory)
            .map_err(|error| io_context("directory open for persistence", error))?;
        check_cancelled(cancel)?;
        file.sync_all()
            .map_err(|error| io_context("directory persistence", error))
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn sync_file(&self, file: &cap_std::fs::File) -> io::Result<()> {
        #[cfg(test)]
        if self.fail_file_sync.load(Ordering::SeqCst) {
            return Err(io::Error::other(
                "injected published file persistence failure",
            ));
        }
        file.sync_all()
            .map_err(|error| io_context("file persistence", error))
    }
}

impl LocalNamespace {
    pub(super) async fn persist_entry(
        &self,
        expected: &SourceDescriptor,
        cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        let relative = checked(&expected.path, Operation::Publish, true)?;
        if !matches!(expected.kind, EntryKind::File | EntryKind::Directory)
            || expected.content_version.is_none()
        {
            return Err(entry_failure(
                &expected.path,
                Operation::Publish,
                FailureClass::Unsupported,
            ));
        }
        let anchor = self.persistence.as_ref().ok_or_else(|| {
            entry_failure(
                &expected.path,
                Operation::Publish,
                FailureClass::Unsupported,
            )
        })?;
        let anchor = Arc::clone(anchor);
        let expected = expected.clone();
        let path = expected.path.clone();
        self.blocking(&path, Operation::Publish, move |root, identity| {
            persist_at_root(root, &relative, &expected, identity, &anchor, &cancel)
                .map_err(|error| persistence_failure(&expected.path, &error, &cancel))
        })
        .await
    }
}

fn persist_at_root(
    root: &Dir,
    relative: &Path,
    expected: &SourceDescriptor,
    identity: &BackendIdentity,
    anchor: &RootAnchor,
    cancel: &CancellationToken,
) -> io::Result<()> {
    check_cancelled(cancel)?;
    let ancestors = anchor.verified_chain(root, cancel)?;
    let parents = resolve_parents(root, relative, cancel)?;
    let parent = parents.last().ok_or(io::ErrorKind::InvalidInput)?;
    persist_leaf(
        parent,
        relative.file_name(),
        expected,
        identity,
        anchor,
        cancel,
    )?;
    for directory in parents.iter().rev() {
        anchor.sync_directory(directory, cancel)?;
    }
    // 根已包含在 parents 中；根外祖先逐项确认，不能因 inode 未变而跳过。
    for directory in ancestors.iter().rev().skip(1) {
        anchor.sync_directory(directory, cancel)?;
    }
    check_cancelled(cancel)
}

fn resolve_parents(
    root: &Dir,
    relative: &Path,
    cancel: &CancellationToken,
) -> io::Result<Vec<Dir>> {
    check_cancelled(cancel)?;
    let mut directories = vec![root.try_clone()?];
    for part in relative.parent().unwrap_or(Path::new("")).components() {
        let Component::Normal(name) = part else {
            return Err(io::ErrorKind::InvalidInput.into());
        };
        check_cancelled(cancel)?;
        let parent = directories.last().ok_or(io::ErrorKind::InvalidInput)?;
        directories.push(open_directory(parent, Path::new(name))?);
    }
    Ok(directories)
}

fn persist_leaf(
    parent: &Dir,
    leaf: Option<&OsStr>,
    expected: &SourceDescriptor,
    identity: &BackendIdentity,
    anchor: &RootAnchor,
    cancel: &CancellationToken,
) -> io::Result<()> {
    check_cancelled(cancel)?;
    let name = leaf.unwrap_or_else(|| OsStr::new("."));
    // O_NONBLOCK 避免被替换成 FIFO 后阻塞；O_NOFOLLOW 避免打开软链接目标。
    let file = parent.open_with(
        name,
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK),
    )?;
    check_cancelled(cancel)?;
    matches_expected(&file.metadata()?, expected, identity)?;
    check_cancelled(cancel)?;
    anchor.sync_file(&file)?;
    check_cancelled(cancel)?;
    matches_expected(&file.metadata()?, expected, identity)?;
    check_cancelled(cancel)?;
    matches_expected(&parent.symlink_metadata(name)?, expected, identity)
}

fn matches_expected(
    metadata: &Metadata,
    expected: &SourceDescriptor,
    identity: &BackendIdentity,
) -> io::Result<()> {
    let actual_identity = source_identity(identity, &expected.path, metadata)
        .map_err(|_| io::Error::other("cannot identify published entry"))?;
    if entry_kind(metadata) != Some(expected.kind)
        || actual_identity != expected.source_identity
        || (expected.kind == EntryKind::File && Some(metadata.len()) != expected.size)
        || expected.content_version.as_ref() != Some(&content_version(metadata))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "published identity, type, size or change evidence no longer matches",
        ));
    }
    Ok(())
}

#[derive(Debug)]
struct PersistenceIo {
    phase: &'static str,
    source: io::Error,
}

impl fmt::Display for PersistenceIo {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(output, "{}: {}", self.phase, self.source)
    }
}

impl std::error::Error for PersistenceIo {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

fn io_context(phase: &'static str, source: io::Error) -> io::Error {
    io::Error::new(source.kind(), PersistenceIo { phase, source })
}

fn persistence_failure(
    path: &StoragePath,
    error: &io::Error,
    cancel: &CancellationToken,
) -> StorageRoleFailure {
    let failure = io_failure(path, Operation::Publish, error);
    let StorageRoleFailure::Entry(entry) = failure else {
        return failure;
    };
    let (class, transience) = if error.kind() == io::ErrorKind::Interrupted && cancel.is_cancelled()
    {
        (FailureClass::Cancelled, Transience::Transient)
    } else if error.kind() == io::ErrorKind::InvalidData {
        (FailureClass::Conflict, Transience::Permanent)
    } else {
        (entry.class(), entry.transience())
    };
    let diagnostic = format!(
        "published entry persistence failed ({:?}): {error}",
        error.kind()
    );
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            Operation::Publish,
            class,
            transience,
            diagnostic,
        )
        .unwrap_or(entry),
    )
}

fn check_cancelled(cancel: &CancellationToken) -> io::Result<()> {
    if cancel.is_cancelled() {
        Err(io::ErrorKind::Interrupted.into())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
