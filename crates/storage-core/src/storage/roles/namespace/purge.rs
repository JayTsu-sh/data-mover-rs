//! 调用方授权后的源端身份检查；默认只处理当前文件系统实体。

use std::{error::Error, fmt};

use tokio_util::sync::CancellationToken;

use super::Namespace;
use crate::model::{
    EntryKind, EntryOperationFailure, FailureClass, ObservedEntry, Operation, SourceVersion,
    StoragePath, StorageTimestamp, Transience,
};
use crate::storage::StorageRoleFailure;

/// 永久清理的专用失败契约；首因必需，版本未知的列举／锚点故障不虚构版本。
/// 其余逐版本原因由实现按有界批次保存；部分删除或缺失回执不构成整体成功。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PurgeFailure {
    /// 首因对应的精确选择器；列举、锚点等整项故障为 None。
    pub version: Option<SourceVersion>,
    /// 首因保持标准 `Error::source` 链；`Box` 只在错误时分配，限制普通返回值大小。
    pub source: Box<StorageRoleFailure>,
    /// 同批其余版本的原始原因；实现必须按自己的有界批次限制数量。
    pub additional: Vec<(SourceVersion, StorageRoleFailure)>,
}

impl fmt::Display for PurgeFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "permanent purge failed (version={:?}): {}; {} additional version failures",
            self.version,
            self.source,
            self.additional.len()
        )
    }
}

impl Error for PurgeFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

impl From<StorageRoleFailure> for PurgeFailure {
    fn from(source: StorageRoleFailure) -> Self {
        Self {
            version: None,
            source: Box::new(source),
            additional: Vec::new(),
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedicated_purge_failure_preserves_original_error_chain_without_fabricating_a_version() {
        let role = failure(
            &StoragePath::root(),
            FailureClass::PermissionDenied,
            "permission denied",
        );
        let error = PurgeFailure::from(role.clone());
        assert!(error.version.is_none() && error.additional.is_empty());
        assert_eq!(
            Error::source(&error).and_then(|cause| cause.downcast_ref::<StorageRoleFailure>()),
            Some(&role)
        );
        assert!(
            Error::source(error.source.as_ref())
                .and_then(|cause| cause.downcast_ref::<EntryOperationFailure>())
                .is_some()
        );
    }
}
