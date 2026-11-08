//! 受保护的 Local 源清理；私有变化证据仅在 backend 内解释。
use std::io;

use cap_std::fs::Metadata;
use tokio_util::sync::CancellationToken;

use super::{LocalNamespace, checked, delete_entry, entry_failure, in_parent, io_failure};
use crate::model::observation::PrivateBackendEntryFacts;
use crate::model::{BackendIdentity, EntryKind, FailureClass, ObservedEntry, Operation};
use crate::storage::backends::local::observation::{
    content_version, entry_kind, purge_content_version, source_identity,
};
use crate::storage::{PurgeOutcome, StorageRoleFailure};

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
        let expected = entry.clone();
        self.blocking(entry.path(), Operation::Namespace, move |root, identity| {
            let result = check_cancel(&cancel).and_then(|()| {
                in_parent(root, &native, |parent, name| {
                    check_cancel(&cancel)?;
                    let metadata = parent.symlink_metadata(name)?;
                    verify(&expected, identity, &metadata)?;
                    check_cancel(&cancel)?;
                    delete_entry(parent, name)
                })
            });
            match result {
                Ok(()) => Ok(PurgeOutcome::Deleted { records: 1 }),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    Ok(PurgeOutcome::AlreadyAbsent)
                }
                Err(error) => Err(purge_failure(&expected, &error)),
            }
        })
        .await
    }
}

fn verify(
    expected: &ObservedEntry,
    identity: &BackendIdentity,
    metadata: &Metadata,
) -> io::Result<()> {
    let actual = source_identity(identity, expected.path(), metadata).map_err(io::Error::other)?;
    if entry_kind(metadata) != Some(expected.kind())
        || actual != *expected.source_identity()
        || (expected.kind() == EntryKind::File && Some(metadata.len()) != expected.size())
    {
        return Err(io::ErrorKind::InvalidData.into());
    }
    if expected.kind() == EntryKind::Directory {
        return Ok(());
    }
    let PrivateBackendEntryFacts::Local(facts) = expected.backend_facts() else {
        return Err(io::ErrorKind::Unsupported.into());
    };
    let version = purge_content_version(facts).ok_or(io::ErrorKind::Unsupported)?;
    if version != content_version(metadata).as_ref() {
        return Err(io::ErrorKind::InvalidData.into());
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

fn purge_failure(expected: &ObservedEntry, error: &io::Error) -> StorageRoleFailure {
    if error.kind() == io::ErrorKind::InvalidData {
        entry_failure(
            expected.path(),
            Operation::Namespace,
            FailureClass::Conflict,
        )
    } else {
        io_failure(expected.path(), Operation::Namespace, error)
    }
}
