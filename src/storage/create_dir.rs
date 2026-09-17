//! Recursive directory creation over the [`Namespace`] role.
//!
//! Replaces the legacy per-backend `create_dir_all`: each missing component is created in
//! order through `NamespaceRequest::CreateDirectory`, so every backend that lends a namespace
//! role gets it without protocol-specific code. Symmetric with [`super::delete_tree`].
//!
//! Semantics worth knowing before relying on it:
//!
//! - **Already-existing levels are success.** Backends disagree on how they report one: CIFS
//!   answers `STATUS_OBJECT_NAME_COLLISION` (`Conflict`), while the NFS adapter's
//!   `create_directory` is itself recursive and idempotent and simply answers `Completed`.
//!   Both are treated as "this level exists now".
//! - **Only the leaf is verified on conflict.** A conflict costs one extra `Stat` at the leaf
//!   to prove the existing entry is a directory rather than a file. Intermediate levels skip
//!   that check on CIFS and HDFS, where a file in the way makes the level below it fail, so
//!   the mistake cannot pass silently — only the leaf could.
//!
//!   **NFS is the exception, and it is not this helper's doing.** Its `CreateDirectory`
//!   forwards to `nfs::create_dir_all`, which deletes a non-directory blocking a component and
//!   retries, by deliberate rsync-style migration semantics (`src/nfs.rs`, logged at `warn`).
//!   So on NFS a file at any level — leaf included — is removed rather than reported, and the
//!   leaf `Stat` then sees the directory that replaced it. Verifying intermediate levels here
//!   would not change that: the delete happens inside the one call this helper makes.
//! - **No cross-call cache in this layer.** The legacy `DirExistsCache` lived in the backend
//!   because there was no role layer; which destination directories a session already created
//!   is caller state, so it belongs to the caller. Backends may still cache below the role:
//!   the NFS adapter keeps a process-wide directory-handle cache.
//! - **The backend root always exists.** An empty path is a no-op, never a request: the HDFS
//!   namespace refuses a root-targeted `CreateDirectory` outright.

use std::fmt;

use super::{
    CapabilityUnavailable, Namespace, NamespaceRequest, NamespaceResult, PreflightPolicy, Storage,
    StorageRoleFailure,
};
use crate::model::{
    EntryKind, EntryOperationFailure, FailureClass, Operation, StoragePath, Transience,
};

/// Why a recursive directory creation could not complete.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateDirectoryAllFailure {
    /// The storage lends no namespace role under the production preflight policy.
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
/// Succeeds when the directory already exists.
///
/// # Errors
/// Returns [`CreateDirectoryAllFailure::Capability`] when the storage lends no namespace role,
/// before any backend I/O, and [`CreateDirectoryAllFailure::Role`] when a level cannot be
/// created or the leaf turns out to be a non-directory.
pub async fn create_directory_all(
    storage: &Storage,
    path: &StoragePath,
) -> Result<(), CreateDirectoryAllFailure> {
    let namespace = storage.namespace(&PreflightPolicy::production())?;
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
    let conflict = match namespace
        .execute(NamespaceRequest::CreateDirectory(level.clone()))
        .await
    {
        Ok(_) => return Ok(()),
        Err(failure) if class_of(&failure) == FailureClass::Conflict => failure,
        Err(failure) => return Err(failure.into()),
    };
    if !is_leaf {
        return Ok(());
    }
    // The leaf is the only level whose conflict could hide a file where the backend reports
    // one at all; see the NFS exception in the module docs.
    match namespace.execute(NamespaceRequest::Stat(level)).await {
        Ok(NamespaceResult::Entries(entries))
            if entries
                .first()
                .is_some_and(|entry| entry.kind == EntryKind::Directory) =>
        {
            Ok(())
        }
        // Report the original conflict rather than the probe's outcome: the caller asked to
        // create a directory, and what stopped it was the entry already there.
        _ => Err(conflict.into()),
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
