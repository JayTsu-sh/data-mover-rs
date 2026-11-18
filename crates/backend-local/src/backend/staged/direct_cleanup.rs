//! 显式授权后的 Direct 数据失败清理；通过持有句柄的身份与初始变化证据限制删除范围。
use std::fs::{File, Metadata};
use std::io;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Component, Path};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use cap_std::ambient_authority;
use cap_std::fs::{Dir, Metadata as CapMetadata, MetadataExt as _};

use super::{LocalStageState, LocalStagedDestination, failure, io_failure};
use crate::model::{EntryOperationFailure, FailureClass, Operation, StoragePath};
use crate::storage::durability::sync_directory;
use crate::storage::{PreparedStage, StorageRoleFailure};

#[derive(Eq, PartialEq)]
struct FileEvidence {
    identity: (u64, u64),
    change: (u64, i64, i64, i64, i64),
}

impl FileEvidence {
    fn from_cap_metadata(metadata: &CapMetadata) -> Self {
        Self {
            identity: (metadata.dev(), metadata.ino()),
            change: (
                metadata.len(),
                metadata.mtime(),
                metadata.mtime_nsec(),
                metadata.ctime(),
                metadata.ctime_nsec(),
            ),
        }
    }

    fn from_metadata(metadata: &Metadata) -> Self {
        Self {
            identity: (metadata.dev(), metadata.ino()),
            change: (
                metadata.len(),
                metadata.mtime(),
                metadata.mtime_nsec(),
                metadata.ctime(),
                metadata.ctime_nsec(),
            ),
        }
    }
}

pub(super) struct MutationEvidence {
    created: bool,
    initial: FileEvidence,
    intended: AtomicBool,
    #[cfg(test)]
    fail_directory_sync: AtomicBool,
}

impl MutationEvidence {
    pub(super) fn new(created: bool, metadata: &CapMetadata) -> Self {
        Self {
            created,
            initial: FileEvidence::from_cap_metadata(metadata),
            intended: AtomicBool::new(false),
            #[cfg(test)]
            fail_directory_sync: AtomicBool::new(false),
        }
    }
}

pub(super) fn mark_mutation(stage: &PreparedStage) {
    if let Some(evidence) = stage
        .backend_state()
        .as_ref()
        .and_then(|owned| owned.downcast_ref::<LocalStageState>())
        .and_then(|owned| owned.direct_cleanup.as_ref())
    {
        evidence.intended.store(true, Ordering::Release);
    }
}

pub(super) async fn remove(
    adapter: &LocalStagedDestination,
    stage: PreparedStage,
) -> Result<(), StorageRoleFailure> {
    let path = stage.final_destination().path().clone();
    stage
        .validate_owner(&adapter.identity)
        .map_err(|_| failure(&path, Operation::Namespace, FailureClass::Conflict))?;
    if !stage.direct() || stage.recovered_publication().is_some() {
        return Err(failure(
            &path,
            Operation::Namespace,
            FailureClass::InvalidInput,
        ));
    }
    let root = Arc::clone(&adapter.root_dir);
    let root_path = Arc::clone(&adapter.root);
    let task_path = path.clone();
    tokio::task::spawn_blocking(move || {
        let owned = LocalStagedDestination::local_state(&stage, Operation::Namespace)?
            .ok_or_else(|| failure(&path, Operation::Namespace, FailureClass::InvalidInput))?;
        remove_at(&root, &root_path, owned).map_err(|error| cleanup_failure(&path, &error))
    })
    .await
    .map_err(|_| failure(&task_path, Operation::Namespace, FailureClass::Internal))?
}

fn remove_at(root: &Dir, root_path: &Path, state: &LocalStageState) -> io::Result<()> {
    let evidence = state
        .direct_cleanup
        .as_ref()
        .ok_or(io::ErrorKind::InvalidInput)?;
    let file = state
        .file
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
        .cloned()
        .ok_or(io::ErrorKind::InvalidInput)?;
    let parent = verified_parent(root, root_path, state)?;
    let visible = match parent.symlink_metadata(&state.name) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return sync_removed_parent(&parent, evidence);
        }
        result => result?,
    };
    if !should_remove(&file, &visible, evidence)? {
        return Ok(());
    }
    match parent.remove_file(&state.name) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        result => result?,
    }
    sync_removed_parent(&parent, evidence)
}

fn verified_parent(root: &Dir, root_path: &Path, state: &LocalStageState) -> io::Result<Dir> {
    let ambient = Dir::open_ambient_dir("/", ambient_authority())?;
    let visible_root = open_directory(&ambient, root_path)?;
    same_directory(&visible_root, root)?;
    let parent = open_directory(
        root,
        state.relative.parent().ok_or(io::ErrorKind::InvalidInput)?,
    )?;
    same_directory(&parent, &state.directory)?;
    Ok(parent)
}

fn should_remove(
    file: &File,
    visible: &CapMetadata,
    evidence: &MutationEvidence,
) -> io::Result<bool> {
    let current = FileEvidence::from_metadata(&file.metadata()?);
    if !visible.is_file()
        || (visible.dev(), visible.ino()) != current.identity
        || current.identity != evidence.initial.identity
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "failed Direct target was replaced",
        ));
    }
    Ok(evidence.created
        || (evidence.intended.load(Ordering::Acquire) && current.change != evidence.initial.change))
}

fn sync_removed_parent(parent: &Dir, evidence: &MutationEvidence) -> io::Result<()> {
    #[cfg(not(test))]
    let _ = evidence;
    #[cfg(test)]
    if evidence.fail_directory_sync.load(Ordering::Acquire) {
        return Err(io::Error::other(
            "injected failed Direct cleanup directory sync failure",
        ));
    }
    sync_directory(parent)
}

fn open_directory(root: &Dir, path: &Path) -> io::Result<Dir> {
    let mut current = root.try_clone()?;
    for component in path.components() {
        let name = match component {
            Component::RootDir | Component::CurDir => continue,
            Component::Normal(name) => name,
            _ => return Err(io::ErrorKind::InvalidInput.into()),
        };
        let parent = current.into_std_file();
        current = Dir::from_std_file(cap_primitives::fs::open_dir_nofollow(
            &parent,
            Path::new(name),
        )?);
    }
    Ok(current)
}

fn same_directory(visible: &Dir, held: &Dir) -> io::Result<()> {
    let visible = visible.dir_metadata()?;
    let held = held.dir_metadata()?;
    if (visible.dev(), visible.ino()) != (held.dev(), held.ino()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "failed Direct parent was replaced",
        ));
    }
    Ok(())
}

fn cleanup_failure(path: &StoragePath, error: &io::Error) -> StorageRoleFailure {
    let fallback = if error.kind() == io::ErrorKind::InvalidData {
        failure(path, Operation::Namespace, FailureClass::Conflict)
    } else {
        io_failure(path, Operation::Namespace, error)
    };
    let StorageRoleFailure::Entry(cause) = &fallback else {
        return fallback;
    };
    EntryOperationFailure::new(
        path.clone(),
        Operation::Namespace,
        cause.class(),
        cause.transience(),
        format!(
            "failed Direct output cleanup failed ({:?}): {error}",
            error.kind()
        ),
    )
    .map(StorageRoleFailure::Entry)
    .unwrap_or(fallback)
}

#[cfg(test)]
mod tests;
