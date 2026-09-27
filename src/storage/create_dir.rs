//! Recursive directory creation over the [`Namespace`] role.
//!
//! Replaces the legacy per-backend `create_dir_all`: each missing component is created in
//! order through [`Namespace::create_directory`], so every backend that lends a namespace
//! role gets it without protocol-specific code. Symmetric with [`super::delete_tree`].
//!
//! Semantics worth knowing before relying on it:
//!
//! - **Already-existing levels are success.** Backends disagree on how they report one: CIFS
//!   answers `STATUS_OBJECT_NAME_COLLISION` (`Conflict`), while the NFS adapter's
//!   `create_directory` looks the name up after `EXIST` and answers success for a directory.
//!   Both are treated as "this level exists now", and so is a refusal once a `stat` shows a
//!   directory (below). Every backend's `create_directory` creates one level (NFS since it
//!   stopped forwarding to the recursive legacy `NFSStorage::create_dir_all`, which legacy
//!   callers keep), so this helper drives every level on every backend.
//! - **Only the leaf is verified on conflict.** A conflict costs one extra `stat` at the leaf
//!   to prove the existing entry is a directory rather than a file. Intermediate levels skip
//!   that check on CIFS and HDFS, where a file in the way makes the level below it fail, so
//!   the mistake cannot pass silently — only the leaf could.
//!
//!   **NFS is the exception, and it is not this helper's doing.** Its one-level
//!   `create_directory` (`src/nfs/role_namespace.rs`) removes a non-directory it finds at the
//!   level after `EXIST` and creates the directory in its place — the legacy helper's
//!   deliberate rsync-style migration semantics, kept and logged at `warn`. So on NFS a file at
//!   any level — leaf included — is replaced rather than reported, and the call succeeds
//!   without a conflict to verify. Verifying intermediate levels here would not change that:
//!   the delete happens inside the backend's call.
//! - **A refused level that already exists as a directory is success.** Servers may check
//!   access before the name: ONTAP answers `CREATE` of an existing directory on a share the
//!   account may only read with `ACCESS_DENIED`, not `COLLISION`. An entry-scoped
//!   `PermissionDenied` therefore costs one `stat` of that level — intermediate or leaf — and
//!   counts as "this level exists" only when the entry is a directory; a missing level, a
//!   file, or a `stat` that fails for that entry keeps the original `PermissionDenied`. So a
//!   read-only root above a writable directory no longer refuses the levels below it. A
//!   session-scoped refusal is never probed. NFS servers differ the same way (2026-09-27, an
//!   existing directory in a parent the caller may not write): DXN-2 answers `MKDIR` with
//!   `ACCES` on v3 and v4.0, ONTAP with `EXIST` on v3, v4.0 and v4.1; a read-only file system
//!   answers `ROFS`, also `PermissionDenied`.
//! - **A probe that fails beyond its entry reports itself.** When either `stat` above (the
//!   leaf conflict check or the refused-level check) ends `Cancelled`, with a session failure
//!   (a `Session` failure, or an entry failure of class `Connectivity` or `Authentication`), or
//!   `Throttled`, that failure is returned instead of the original one. Cancellation is an
//!   upstream signal, not a refusal (R10); a lost session or a busy server says nothing about
//!   the entry and must not read as a permanent per-entry `PermissionDenied` or `Conflict`.
//! - **No cross-call cache in this layer.** The legacy `DirExistsCache` lived in the backend
//!   because there was no role layer; which destination directories a session already created
//!   is caller state, so it belongs to the caller. Backends may still cache below the role:
//!   the NFS adapter keeps a process-wide directory-handle cache, and a level it holds a
//!   handle for answers success without an RPC.
//! - **The backend root always exists.** An empty path is a no-op, never a request: the HDFS
//!   namespace refuses a root-targeted `CreateDirectory` outright.

use std::fmt;

use super::{CapabilityUnavailable, Namespace, PreflightPolicy, Storage, StorageRoleFailure};
use crate::model::{
    EntryKind, EntryOperationFailure, FailureClass, Operation, StoragePath, Transience,
};

/// Why a recursive directory creation could not complete.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateDirectoryAllFailure {
    /// The storage lends no namespace role under the production preflight policy, or one whose
    /// mutating verbs are all unsupported (S3).
    Capability(CapabilityUnavailable),
    /// One level could not be created, naming the level that failed.
    Role(StorageRoleFailure),
}

impl fmt::Display for CreateDirectoryAllFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capability(error) => error.fmt(formatter),
            Self::Role(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for CreateDirectoryAllFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Capability(error) => Some(error),
            Self::Role(error) => Some(error),
        }
    }
}

impl From<CapabilityUnavailable> for CreateDirectoryAllFailure {
    fn from(error: CapabilityUnavailable) -> Self {
        Self::Capability(error)
    }
}

impl From<StorageRoleFailure> for CreateDirectoryAllFailure {
    fn from(error: StorageRoleFailure) -> Self {
        Self::Role(error)
    }
}

/// Creates every missing component of `path` through `storage`'s namespace role.
///
/// Succeeds when the directory already exists, including when the server refuses to create a
/// level that is already a directory (access checked before the name).
///
/// # Errors
/// Returns [`CreateDirectoryAllFailure::Capability`] when the storage lends no namespace role, or
/// one that cannot create anything (S3), before any backend I/O, and [`CreateDirectoryAllFailure::Role`] when a level cannot be
/// created or the leaf turns out to be a non-directory. When the `stat` that checks a level
/// fails with `Cancelled`, `Connectivity`, `Authentication` or `Throttled`, or at session scope,
/// that failure is returned (its operation is the `stat`'s) rather than the level's own.
pub async fn create_directory_all(
    storage: &Storage,
    path: &StoragePath,
) -> Result<(), CreateDirectoryAllFailure> {
    let namespace = storage.mutable_namespace(&PreflightPolicy::production())?;
    create_directory_all_with_namespace(namespace.as_ref(), path).await
}

pub(crate) async fn create_directory_all_with_namespace(
    namespace: &dyn Namespace,
    path: &StoragePath,
) -> Result<(), CreateDirectoryAllFailure> {
    let components: Vec<&str> = path
        .as_str()
        .split('/')
        .filter(|component| !component.is_empty())
        .collect();
    let mut prefix = String::new();
    for (index, component) in components.iter().enumerate() {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(component);
        let level = StoragePath::new(prefix.clone()).map_err(|_| invalid(path))?;
        let is_leaf = index + 1 == components.len();
        create_level(namespace, level, is_leaf).await?;
    }
    Ok(())
}

async fn create_level(
    namespace: &dyn Namespace,
    level: StoragePath,
    is_leaf: bool,
) -> Result<(), CreateDirectoryAllFailure> {
    let failure = match namespace.create_directory(&level).await {
        Ok(()) => return Ok(()),
        Err(failure) => failure,
    };
    let needs_proof = if class_of(&failure) == FailureClass::Conflict {
        // The leaf is the only level whose conflict could hide a file where the backend reports
        // one at all; see the NFS exception in the module docs.
        is_leaf
    } else if is_entry_permission_denied(&failure) {
        // The server may check the ACL before the name: only a directory already there turns
        // the refusal into "this level exists", at every level.
        true
    } else {
        return Err(failure.into());
    };
    if !needs_proof {
        return Ok(());
    }
    match namespace.stat(&level).await {
        Ok(entry) if entry.kind == EntryKind::Directory => Ok(()),
        // A cancelled, session-wide or throttled probe says nothing about the entry, and turning
        // it into the entry's refusal would make an upstream signal (R10), a lost session or a
        // busy server look like a permanent per-entry failure: report the probe's own failure.
        Err(probe) if outlives_the_entry(&probe) => Err(probe.into()),
        // Otherwise report the original failure rather than the probe's outcome: the caller
        // asked to create a directory, and what stopped it was the entry already there or the
        // refusal.
        _ => Err(failure.into()),
    }
}

/// Whether a failed probe is about something other than the probed entry: a cancellation, a
/// failure of the session (connectivity, authentication) whichever scope the backend reported
/// it in, or throttling (neither the entry's fault nor the session's, and always transient).
fn outlives_the_entry(probe: &StorageRoleFailure) -> bool {
    match probe {
        StorageRoleFailure::Session(_) => true,
        StorageRoleFailure::Entry(error) => matches!(
            error.class(),
            FailureClass::Cancelled
                | FailureClass::Connectivity
                | FailureClass::Authentication
                | FailureClass::Throttled
        ),
    }
}

fn is_entry_permission_denied(failure: &StorageRoleFailure) -> bool {
    match failure {
        StorageRoleFailure::Entry(error) => error.class() == FailureClass::PermissionDenied,
        StorageRoleFailure::Session(_) => false,
    }
}

fn class_of(failure: &StorageRoleFailure) -> FailureClass {
    match failure {
        StorageRoleFailure::Entry(error) => error.class(),
        StorageRoleFailure::Session(error) => error.class(),
    }
}

fn invalid(path: &StoragePath) -> CreateDirectoryAllFailure {
    CreateDirectoryAllFailure::Role(StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            Operation::Namespace,
            FailureClass::InvalidInput,
            Transience::Permanent,
            "path component is not a valid storage path",
        )
        .unwrap_or_else(|_| unreachable!("the static diagnostic is valid")),
    ))
}

#[cfg(test)]
mod tests;
