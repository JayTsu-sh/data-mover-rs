//! 受保护的 Local 源清理；私有变化证据仅在 backend 内解释。
use std::io;

use cap_std::fs::Metadata;
use tokio_util::sync::CancellationToken;

use super::{LocalNamespace, checked, entry_failure, in_parent, io_failure, remove_link_or_file};
use crate::model::observation::PrivateBackendEntryFacts;
use crate::model::{
    BackendIdentity, EntryKind, EntryOperationFailure, FailureClass, ObservedEntry, Operation,
    SourceIdentity, StoragePath,
};
use crate::storage::backends::local::observation::{
    content_version, entry_kind, purge_content_version, source_identity,
};
use crate::storage::{PurgeOutcome, StorageRoleFailure};

/// 阻塞清理只捕获身份及固定大小的变化凭据，不复制 ACL、xattr、tags 等原始观察。
struct PurgeEvidence {
    path: StoragePath,
    kind: EntryKind,
    identity: SourceIdentity,
    size: Option<u64>,
    version: Option<[u8; 32]>,
}

impl PurgeEvidence {
    fn capture(entry: &ObservedEntry) -> Self {
        let version = match entry.backend_facts() {
            PrivateBackendEntryFacts::Local(facts) => {
                purge_content_version(facts).and_then(|value| value.try_into().ok())
            }
            _ => None,
        };
        Self {
            path: entry.path().clone(),
            kind: entry.kind(),
            identity: entry.source_identity().clone(),
            size: entry.size(),
            version,
        }
    }
}

impl LocalNamespace {
    pub(super) async fn purge_entry(
        &self,
        entry: &ObservedEntry,
        cancel: CancellationToken,
    ) -> Result<PurgeOutcome, StorageRoleFailure> {
        let native = checked(entry.path(), Operation::Namespace, false)?;
        if entry.history().is_some() || entry.directory_marker_object().is_some() {
            return Err(entry_failure(
                entry.path(),
                Operation::Namespace,
                FailureClass::Unsupported,
            ));
        }
        let expected = PurgeEvidence::capture(entry);
        self.blocking(entry.path(), Operation::Namespace, move |root, identity| {
            let result = check_cancel(&cancel).and_then(|()| {
                in_parent(root, &native, |parent, name| {
                    check_cancel(&cancel)?;
                    let metadata = parent.symlink_metadata(name)?;
                    verify(&expected, identity, &metadata)?;
                    check_cancel(&cancel)?;
                    // 受保护 purge 只获准删除这一条目，隐藏项也必须视为非空。
                    if expected.kind == EntryKind::Directory {
                        parent.remove_dir(name)
                    } else {
                        remove_link_or_file(parent, name)
                    }
                })
            });
            match result {
                Ok(()) => Ok(PurgeOutcome::Deleted { records: 1 }),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    Ok(PurgeOutcome::AlreadyAbsent)
                }
                Err(error) => Err(purge_failure(&expected.path, &error)),
            }
        })
        .await
    }
}

fn verify(
    expected: &PurgeEvidence,
    identity: &BackendIdentity,
    metadata: &Metadata,
) -> io::Result<()> {
    let actual = source_identity(identity, &expected.path, metadata).map_err(io::Error::other)?;
    if entry_kind(metadata) != Some(expected.kind)
        || actual != expected.identity
        || (expected.kind == EntryKind::File && Some(metadata.len()) != expected.size)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "source identity, kind or size changed after observation; retained",
        ));
    }
    if expected.kind == EntryKind::Directory {
        return Ok(());
    }
    let version = expected.version.as_ref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "missing or unsupported Local change evidence; source retained",
        )
    })?;
    if version.as_slice() != content_version(metadata).as_ref() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "source mtime/ctime changed after observation; retained",
        ));
    }
    Ok(())
}

fn check_cancel(cancel: &CancellationToken) -> io::Result<()> {
    if cancel.is_cancelled() {
        Err(io::ErrorKind::Interrupted.into())
    } else {
        Ok(())
    }
}

fn purge_failure(path: &StoragePath, error: &io::Error) -> StorageRoleFailure {
    let fallback = if error.kind() == io::ErrorKind::InvalidData {
        entry_failure(path, Operation::Namespace, FailureClass::Conflict)
    } else {
        io_failure(path, Operation::Namespace, error)
    };
    let StorageRoleFailure::Entry(cause) = &fallback else {
        return fallback;
    };
    let diagnostic = if error.kind() == io::ErrorKind::DirectoryNotEmpty {
        "source directory is not empty; all children retained".to_owned()
    } else {
        format!("Local protected source purge failed: {error}")
    };
    EntryOperationFailure::new(
        path.clone(),
        Operation::Namespace,
        cause.class(),
        cause.transience(),
        diagnostic,
    )
    .map(StorageRoleFailure::Entry)
    .unwrap_or(fallback)
}
