//! The NFS role's rename (`Namespace::rename`, `Staged` publish / pointer rename).
//!
//! Each RPC keeps the server error classification, including permission, cross-device and
//! non-empty-directory failures.
//!
//! - The missing levels of the destination's parent are created first
//!   ([`NFSStorage::role_create_all`], `src/nfs/role_create.rs`).
//! - Both parents are resolved through the handle cache with the role's classification
//!   ([`NFSStorage::role_lookup_fh`]).
//! - RENAME: `STALE` / `BADHANDLE`, or `NOENT` from a racing rename or delete → refresh the root
//!   handle, forget both paths' cached handles, resolve and send again (at most
//!   [`MAX_STALE_RETRIES`] retries, as the legacy rename). Any other answer, and the last one once
//!   the retries are spent, is classified from the nfs-rs error itself (`classify_error`),
//!   keeping the server's status: `ACCES` / `PERM` / `ROFS` → `PermissionDenied` / Permanent,
//!   `NOTEMPTY` / `EXIST` / `ISDIR` / `NOTDIR` → `Conflict` / Permanent, `XDEV` → `Unsupported` /
//!   Permanent, `NOENT` → `NotFound`.
//! - A rename that succeeded forgets the cached handles below both names (C10a).

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use bytes::Bytes;
use nfs_rs::NfsError;
use tracing::debug;

use super::{
    MAX_STALE_RETRIES, NFSStorage, classify_role_error, invalidate_path_cache,
    is_retryable_with_invalidation,
};
use crate::model::{FailureClass, StoragePath, Transience};
use crate::storage::backends::nfs::protocol::classify_error;
use crate::storage::backends::nfs::source::NfsProtocolFailure;

impl NFSStorage {
    /// Renames `from` to `to` after creating every missing level of `to`'s parent as the role
    /// does ([`Self::role_create_all`]). See the module docs.
    pub(super) async fn role_rename(
        &self,
        from: &StoragePath,
        to: &StoragePath,
    ) -> Result<(), NfsProtocolFailure> {
        let from = Path::new(from.as_str());
        let to = Path::new(to.as_str());
        let from_components = Self::collect_components(from).map_err(classify_role_error)?;
        let to_components = Self::collect_components(to).map_err(classify_role_error)?;
        let (Some((from_name, from_parents)), Some((to_name, to_parents))) =
            (from_components.split_last(), to_components.split_last())
        else {
            // The backend root is never renamed, nor renamed onto.
            return Err(NfsProtocolFailure::new(
                FailureClass::InvalidInput,
                Transience::Permanent,
            ));
        };
        self.role_create_all(to_parents).await?;
        rename_entry(&MountRename {
            storage: self,
            from,
            to,
            from_parent: from_parents.iter().collect(),
            to_parent: to_parents.iter().collect(),
            from_name,
            to_name,
            from_components: &from_components,
            to_components: &to_components,
        })
        .await
    }
}

/// The NFS calls one rename makes, so the decisions below are tested against scripted answers.
#[async_trait]
trait RenameCalls: Sync {
    /// The handles of the source's and the destination's parent, through the handle cache.
    async fn parents(&self) -> Result<(Bytes, Bytes), NfsProtocolFailure>;
    /// RENAME of the source's name under its parent to the destination's name under its parent.
    async fn rename(&self, from_parent: Bytes, to_parent: Bytes) -> Result<(), NfsError>;
    /// Refreshes the root handle and forgets the cached handles along both paths.
    async fn forget(&self) -> Result<(), NfsProtocolFailure>;
    /// Forgets the cached handles below both names once the rename took effect.
    fn renamed(&self);
}

async fn rename_entry(calls: &dyn RenameCalls) -> Result<(), NfsProtocolFailure> {
    let mut retries = 0;
    loop {
        let (from_parent, to_parent) = calls.parents().await?;
        match calls.rename(from_parent, to_parent).await {
            Ok(()) => {
                calls.renamed();
                return Ok(());
            }
            Err(error) if retries < MAX_STALE_RETRIES && is_retryable_with_invalidation(&error) => {
                debug!(%error, retries, "role rename: stale handle, forgetting cached handles");
                calls.forget().await?;
                retries += 1;
            }
            Err(error) => {
                debug!(%error, retries, "role rename failed");
                return Err(classify_error(error));
            }
        }
    }
}

/// [`RenameCalls`] against the mount.
struct MountRename<'a> {
    storage: &'a NFSStorage,
    from: &'a Path,
    to: &'a Path,
    from_parent: PathBuf,
    to_parent: PathBuf,
    from_name: &'a str,
    to_name: &'a str,
    from_components: &'a [String],
    to_components: &'a [String],
}

#[async_trait]
impl RenameCalls for MountRename<'_> {
    async fn parents(&self) -> Result<(Bytes, Bytes), NfsProtocolFailure> {
        let from = self.storage.role_lookup_fh(&self.from_parent).await?.fh;
        let to = self.storage.role_lookup_fh(&self.to_parent).await?.fh;
        Ok((from, to))
    }

    async fn rename(&self, from_parent: Bytes, to_parent: Bytes) -> Result<(), NfsError> {
        self.storage
            .mount
            .rename(from_parent, self.from_name, to_parent, self.to_name)
            .await
    }

    async fn forget(&self) -> Result<(), NfsProtocolFailure> {
        let generation = self.storage.refresh_generation.load(Ordering::Acquire);
        self.storage
            .maybe_refresh_root_fh(generation)
            .await
            .map_err(classify_role_error)?;
        let root = self.storage.get_root_fh();
        invalidate_path_cache(self.from_components, &root);
        invalidate_path_cache(self.to_components, &root);
        Ok(())
    }

    fn renamed(&self) {
        // A renamed directory's handles now point below `to`; whatever `to` was is gone.
        self.storage.forget_subtree(self.from);
        self.storage.forget_subtree(self.to);
    }
}

#[cfg(test)]
#[path = "role_rename_tests.rs"]
mod tests;
