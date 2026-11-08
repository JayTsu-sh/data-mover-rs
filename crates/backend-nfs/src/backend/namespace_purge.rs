//! NFS 源项清理：只消费原观察，不递归、不重试不确定删除。
use tokio_util::sync::CancellationToken;

use super::{NfsNamespaceAdapter, NfsNamespaceObservation, checked};
use crate::model::{
    BackendKind, EntryKind, FailureClass, ObservedEntry, Operation, PrincipalIdentity, Transience,
};
use crate::storage::backends::nfs::source::{NfsProtocolFailure, role_failure};
use crate::storage::{InlineOwnership, PurgeOutcome, StorageRoleFailure};

impl NfsNamespaceAdapter {
    pub(super) async fn purge_entry(
        &self,
        entry: &ObservedEntry,
        cancel: CancellationToken,
    ) -> Result<PurgeOutcome, StorageRoleFailure> {
        checked(entry.path())?;
        let check = || {
            if entry.source_identity().backend() != &self.identity {
                return Err(refused(
                    FailureClass::Conflict,
                    "source belongs to another NFS endpoint",
                ));
            }
            validate(entry)
        };
        check().map_err(|error| role_failure(entry.path(), Operation::Namespace, error))?;
        if cancel.is_cancelled() {
            return Err(role_failure(
                entry.path(),
                Operation::Namespace,
                NfsProtocolFailure::new(FailureClass::Cancelled, Transience::Transient)
                    .with_status("NFS source purge cancelled before starting"),
            ));
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => Err(role_failure(entry.path(), Operation::Namespace,
                NfsProtocolFailure::new(FailureClass::Cancelled, Transience::Unknown)
                    .with_status("NFS source purge cancelled in flight; deletion is unconfirmed"))),
            result = self.protocol.delete(entry.path(), entry.kind(), Some(entry)) => match result {
                Ok(()) => Ok(PurgeOutcome::Deleted { records: 1 }),
                Err(error) if error.class == FailureClass::NotFound && error.transience == Transience::Permanent => Ok(PurgeOutcome::AlreadyAbsent),
                Err(error) => Err(role_failure(entry.path(), Operation::Namespace, error)),
            }
        }
    }
}

pub(in crate::backend) fn validate(entry: &ObservedEntry) -> Result<(), NfsProtocolFailure> {
    if entry.backend_kind() != BackendKind::Nfs
        || entry.history().is_some()
        || entry.directory_marker_object().is_some()
        || !matches!(
            entry.kind(),
            EntryKind::File | EntryKind::Directory | EntryKind::Symlink
        )
        || entry.source_identity().stable_bytes().is_none()
        || entry.metadata().failures().next().is_some()
        || (entry.kind() == EntryKind::File && entry.size().is_none())
    {
        return Err(refused(
            FailureClass::Unsupported,
            "NFS source purge requires a supported bound observation without metadata failures",
        ));
    }
    if entry.permissions().is_some_and(|facts| {
        matches!(facts.owner, Some(PrincipalIdentity::Named(_)))
            || matches!(facts.group, Some(PrincipalIdentity::Named(_)))
    }) {
        return Err(refused(
            FailureClass::Unsupported,
            "NFS source purge cannot verify named-only permission evidence",
        ));
    }
    if entry.kind() != EntryKind::Directory
        && (entry.modified().is_none()
            || entry
                .metadata()
                .timestamps()
                .value()
                .is_none_or(|times| times.created.is_none() || times.modified != entry.modified()))
    {
        return Err(refused(
            FailureClass::Unsupported,
            "NFS source purge requires original mtime and ctime; source retained",
        ));
    }
    Ok(())
}

pub(in crate::backend) fn verify_current(
    entry: &ObservedEntry,
    current: &NfsNamespaceObservation,
) -> Result<(), NfsProtocolFailure> {
    validate(entry)?;
    let before = entry.metadata().timestamps().value();
    let after = current.timestamps.as_ref();
    let changed_content = entry.kind() != EntryKind::Directory
        && (after.and_then(|times| times.modified) != entry.modified()
            || before.and_then(|times| times.created) != after.and_then(|times| times.created));
    let changed_owner = entry
        .metadata()
        .ownership_mode()
        .value()
        .is_some_and(|owner| current.ownership != Some(InlineOwnership::Known(*owner)));
    if current.path != *entry.path()
        || current.kind != entry.kind()
        || entry.source_identity().stable_bytes() != Some(current.file_handle.as_ref())
        || (entry.kind() == EntryKind::File && current.size != entry.size())
        || changed_content
        || changed_owner
        || permissions_changed(entry, current)
    {
        return Err(refused(
            FailureClass::Conflict,
            "NFS source identity, content evidence or ownership changed; source retained",
        ));
    }
    Ok(())
}

fn permissions_changed(entry: &ObservedEntry, current: &NfsNamespaceObservation) -> bool {
    entry.permissions().is_some_and(|facts| {
        facts
            .mode
            .is_some_and(|mode| current.mode.map(|value| value & 0o7777) != Some(mode))
            || facts.owner.as_ref().is_some_and(|owner| {
                current.uid.map(PrincipalIdentity::Numeric).as_ref() != Some(owner)
            })
            || facts.group.as_ref().is_some_and(|group| {
                current.gid.map(PrincipalIdentity::Numeric).as_ref() != Some(group)
            })
    })
}

pub(in crate::backend) fn refused(class: FailureClass, reason: &str) -> NfsProtocolFailure {
    NfsProtocolFailure::new(class, Transience::Permanent).with_status(reason)
}
