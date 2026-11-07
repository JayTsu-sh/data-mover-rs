//! 调用方授权后的源端身份检查；默认只处理当前文件系统实体。

use tokio_util::sync::CancellationToken;

use super::Namespace;
use crate::model::{
    EntryKind, EntryOperationFailure, FailureClass, ObservedEntry, Operation, StoragePath,
    StorageTimestamp, Transience,
};
use crate::storage::StorageRoleFailure;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PurgeOutcome {
    Deleted {
        records: u64,
    },
    AlreadyAbsent,
    /// 虚拟目录没有独立实体，子项的清理由调用方逐项授权。
    VirtualDirectory,
}

pub(super) async fn current(
    namespace: &(impl Namespace + ?Sized),
    entry: &ObservedEntry,
    cancel: &CancellationToken,
) -> Result<PurgeOutcome, StorageRoleFailure> {
    let path = entry.path();
    check_cancel(path, cancel)?;
    if path.as_str().is_empty() {
        return Err(failure(
            path,
            FailureClass::InvalidInput,
            "cannot purge the storage root",
        ));
    }
    if entry.history().is_some() || entry.directory_marker_object().is_some() {
        return Err(failure(
            path,
            FailureClass::Unsupported,
            "this namespace cannot permanently purge object history",
        ));
    }
    let current = match namespace.stat(path).await {
        Ok(current) => current,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => {
            return Ok(PurgeOutcome::AlreadyAbsent);
        }
        Err(error) => return Err(error),
    };
    let modified = current
        .inline_timestamps
        .and_then(|times| times.modified)
        .map(StorageTimestamp::unix_nanos);
    if current.kind != entry.kind()
        || current.source_identity != *entry.source_identity()
        || (entry.kind() != EntryKind::Directory
            && (current.size != entry.size()
                || modified != entry.modified().map(StorageTimestamp::unix_nanos)))
    {
        return Err(failure(
            path,
            FailureClass::Conflict,
            "source changed after observation; retained",
        ));
    }
    if entry.kind() == EntryKind::Symlink
        && Some(&namespace.read_link(path).await?) != entry.symlink_target()
    {
        return Err(failure(
            path,
            FailureClass::Conflict,
            "source link target changed; retained",
        ));
    }
    check_cancel(path, cancel)?;
    namespace.delete(path).await?;
    Ok(PurgeOutcome::Deleted { records: 1 })
}

fn check_cancel(path: &StoragePath, cancel: &CancellationToken) -> Result<(), StorageRoleFailure> {
    if cancel.is_cancelled() {
        Err(failure(
            path,
            FailureClass::Cancelled,
            "source purge cancelled",
        ))
    } else {
        Ok(())
    }
}

fn failure(
    path: &StoragePath,
    class: FailureClass,
    diagnostic: &'static str,
) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            Operation::Namespace,
            class,
            if class == FailureClass::Cancelled {
                Transience::Transient
            } else {
                Transience::Permanent
            },
            diagnostic,
        )
        .unwrap_or_else(|_| unreachable!("static purge diagnostic is valid")),
    )
}
