//! One-level directory creation for the NFS Namespace role (`Namespace::create_directory`).
//!
//! The role verb creates exactly one level, as on CIFS and HDFS, so
//! `storage::create_directory_all` drives every level itself and its rule for a refused level
//! that already exists reaches NFS intermediate levels too.
//!
//! One level:
//! - a directory this process already created or looked up (the handle cache) is success
//!   without an RPC, as it was for the legacy helper's deepest cached level;
//! - otherwise MKDIR under the parent's handle. `EXIST` → LOOKUP the name: a directory is
//!   success; anything else is removed and MKDIR is sent again. That rsync-style replacement of
//!   a non-directory blocking a directory is the legacy helper's migration semantics, kept on
//!   purpose and logged at `warn`;
//! - a handle that no longer names what it did (`STALE` / `BADHANDLE`, or `NOENT` from a racing
//!   rename or delete) → refresh the root handle, forget the path's cached handles, retry;
//! - any other answer is classified from the nfs-rs error itself (`classify_error`), keeping
//!   the server's status: a refused MKDIR (`ACCES` / `PERM` / `ROFS`) is an entry
//!   `PermissionDenied` / Permanent, not the `Protocol` / Unknown the legacy string wrapping
//!   made of it.
//!
//! At most [`MAX_STALE_RETRIES`] retries in all; exhausting them reports the last cause, and a
//! non-directory is never removed without a retry left to create the directory.
//!
//! The parent is resolved with the role's classification (`role_lookup_fh`,
//! `src/storage/backends/nfs/client/role_create.rs`): a refused LOOKUP (`ACCES` / `PERM`) is `PermissionDenied` /
//! Permanent, `NOENT` is `NotFound`, a lost session is `Connectivity` — not the `Protocol` /
//! Unknown the legacy string wrapping made of them.
//!
//! A parent that is gone once its stale cached handle was forgotten was deleted after this
//! process cached it, so `create_directory_all` passed it without an RPC. It is rebuilt once
//! within the same call (every missing level of it, `role_create_all`) and the level tried
//! again, as the legacy helper rebuilt the whole path from the root; gone again after that, it
//! is `NotFound` / Transient.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use bytes::Bytes;
use nfs_rs::NfsError;
use tracing::warn;

use super::role_create::transient_if_gone;
use super::{
    FType3, GLOBAL_CACHE, MAX_STALE_RETRIES, NFSStorage, classify_role_error,
    invalidate_path_cache, is_retryable_with_invalidation,
};
use crate::model::{FailureClass, StoragePath, Transience};
use crate::storage::backends::nfs::protocol::classify_error;
use crate::storage::backends::nfs::source::NfsProtocolFailure;

/// Mode of a directory the role creates, as the legacy helper created them.
const DIRECTORY_MODE: u32 = 0o755;

impl NFSStorage {
    /// Creates the one directory `path` names; its parent must exist. See the module docs.
    pub(super) async fn role_create_directory(
        &self,
        path: &StoragePath,
    ) -> Result<(), NfsProtocolFailure> {
        let components =
            Self::collect_components(Path::new(path.as_str())).map_err(classify_role_error)?;
        self.role_create_level(&components).await
    }

    /// Creates the one directory `components` name; see [`Self::role_create_directory`].
    pub(super) async fn role_create_level(
        &self,
        components: &[String],
    ) -> Result<(), NfsProtocolFailure> {
        let Some((name, parents)) = components.split_last() else {
            // The backend root always exists.
            return Ok(());
        };
        let cache_path: PathBuf = components.iter().collect();
        if GLOBAL_CACHE
            .get(&(cache_path.clone(), self.get_root_fh()))
            .is_some()
        {
            return Ok(());
        }
        let level = MountLevel {
            storage: self,
            parent: parents.iter().collect(),
            name,
            cache_path,
            components,
        };
        create_level(&level).await
    }
}

/// The NFS calls creating one level makes, so the decisions below are tested against scripted
/// server answers.
#[async_trait]
trait LevelCalls: Sync {
    /// The level's path, for the log.
    fn path(&self) -> &Path;
    /// The parent directory's handle, through the handle cache.
    async fn parent(&self) -> Result<Bytes, NfsProtocolFailure>;
    /// MKDIR of the level's name; the new directory's handle.
    async fn mkdir(&self, parent: Bytes) -> Result<Bytes, NfsError>;
    /// LOOKUP of the level's name: its handle, and whether it is a directory.
    async fn lookup(&self, parent: Bytes) -> Result<(Bytes, bool), NfsError>;
    /// REMOVE of the level's name.
    async fn remove(&self, parent: Bytes) -> Result<(), NfsError>;
    /// Refreshes the root handle and forgets the cached handles along the level's path.
    async fn forget(&self) -> Result<(), NfsProtocolFailure>;
    /// Caches the level's directory handle.
    fn remember(&self, handle: &Bytes);
    /// Creates every missing level of the parent: it was deleted after this process cached it.
    async fn rebuild_parent(&self) -> Result<(), NfsProtocolFailure>;
}

/// What one attempt ended with.
enum Attempt {
    /// The level is a directory now.
    Done,
    /// Send MKDIR again; `cause` is reported if no retry is left.
    Again { cause: NfsProtocolFailure },
    /// Forget the cached handles, then send MKDIR again.
    AgainFresh { cause: NfsProtocolFailure },
}

async fn create_level(calls: &dyn LevelCalls) -> Result<(), NfsProtocolFailure> {
    let mut retries = 0;
    let mut forgotten = false;
    let mut rebuilt = false;
    loop {
        let parent = parent_of(calls, forgotten, &mut rebuilt).await?;
        let retries_left = retries < MAX_STALE_RETRIES;
        let attempt = match calls.mkdir(parent.clone()).await {
            Ok(handle) => {
                calls.remember(&handle);
                Attempt::Done
            }
            Err(error) if error.is_exist() => settle_existing(calls, parent, retries_left).await?,
            Err(error) => stale_or_failed(error)?,
        };
        let (Attempt::Again { cause } | Attempt::AgainFresh { cause }) = &attempt else {
            return Ok(());
        };
        if retries >= MAX_STALE_RETRIES {
            return Err(cause.clone());
        }
        retries += 1;
        if matches!(attempt, Attempt::AgainFresh { .. }) {
            calls.forget().await?;
            forgotten = true;
        }
    }
}

/// The parent's handle. Once a stale handle was forgotten (`forgotten`), a parent that is gone
/// was deleted after this process cached it, so `create_directory_all` passed it without an RPC:
/// it is rebuilt once (`rebuilt`) and resolved again. Gone after that, it is `NotFound` /
/// Transient — the cache is clean and a new attempt recreates it — not permanent.
async fn parent_of(
    calls: &dyn LevelCalls,
    forgotten: bool,
    rebuilt: &mut bool,
) -> Result<Bytes, NfsProtocolFailure> {
    let gone = |failure: &NfsProtocolFailure| forgotten && failure.class == FailureClass::NotFound;
    let failure = match calls.parent().await {
        Err(failure) if gone(&failure) && !*rebuilt => {
            *rebuilt = true;
            calls.rebuild_parent().await?;
            match calls.parent().await {
                Ok(parent) => return Ok(parent),
                Err(failure) => failure,
            }
        }
        other => return other.map_err(|failure| transient_if_gone(failure, forgotten)),
    };
    Err(transient_if_gone(failure, forgotten))
}

/// MKDIR answered `EXIST`: success when the name is a directory; otherwise the non-directory is
/// removed so the next MKDIR can create the directory (rsync-style, see the module docs) — but
/// only when a retry is left to send that MKDIR.
async fn settle_existing(
    calls: &dyn LevelCalls,
    parent: Bytes,
    retries_left: bool,
) -> Result<Attempt, NfsProtocolFailure> {
    let in_the_way = NfsProtocolFailure::new(FailureClass::Conflict, Transience::Unknown);
    match calls.lookup(parent.clone()).await {
        Ok((handle, true)) => {
            calls.remember(&handle);
            Ok(Attempt::Done)
        }
        Ok((_, false)) if !retries_left => Err(in_the_way),
        Ok((_, false)) => {
            warn!(
                "create_directory: {:?} exists but is not a directory, replacing with directory",
                calls.path()
            );
            match calls.remove(parent).await {
                // A concurrent task may have removed it first — and made the directory there
                // since, which REMOVE refuses as a directory: the next MKDIR finds it.
                Err(error) if !error.is_not_found() && !directory_now(&error) => {
                    Err(classify_error(error))
                }
                _ => Ok(Attempt::Again { cause: in_the_way }),
            }
        }
        // Gone again between MKDIR and LOOKUP, or the parent's handle went stale.
        Err(error) => stale_or_failed(error),
    }
}

/// Whether REMOVE was refused because a directory now stands at the name (`NFS3ERR_ISDIR`; a
/// non-empty one on `NFSv4`, whose REMOVE also removes empty directories).
fn directory_now(error: &NfsError) -> bool {
    matches!(
        error,
        NfsError::Nfs3(
            nfs_rs::Nfs3ErrorCode::NFS3ERR_ISDIR | nfs_rs::Nfs3ErrorCode::NFS3ERR_NOTEMPTY
        ) | NfsError::Nfs4(
            nfs_rs::Nfs4ErrorCode::NFS4ERR_ISDIR | nfs_rs::Nfs4ErrorCode::NFS4ERR_NOTEMPTY
        )
    )
}

fn stale_or_failed(error: NfsError) -> Result<Attempt, NfsProtocolFailure> {
    let retry = is_retryable_with_invalidation(&error);
    let cause = classify_error(error);
    if retry {
        Ok(Attempt::AgainFresh { cause })
    } else {
        Err(cause)
    }
}

/// [`LevelCalls`] against the mount.
struct MountLevel<'a> {
    storage: &'a NFSStorage,
    parent: PathBuf,
    name: &'a str,
    cache_path: PathBuf,
    components: &'a [String],
}

#[async_trait]
impl LevelCalls for MountLevel<'_> {
    fn path(&self) -> &Path {
        &self.cache_path
    }

    async fn parent(&self) -> Result<Bytes, NfsProtocolFailure> {
        self.storage
            .role_lookup_fh(&self.parent)
            .await
            .map(|object| object.fh)
    }

    async fn mkdir(&self, parent: Bytes) -> Result<Bytes, NfsError> {
        let object = self
            .storage
            .mount
            .mkdir(parent, self.name, DIRECTORY_MODE)
            .await?;
        Ok(object.fh)
    }

    async fn lookup(&self, parent: Bytes) -> Result<(Bytes, bool), NfsError> {
        let object = self.storage.mount.lookup(parent, self.name).await?;
        let file_type = match object.attr {
            Some(attrs) => attrs.type_,
            None => self.storage.mount.getattr(object.fh.clone()).await?.type_,
        };
        Ok((object.fh, file_type == FType3::NF3DIR as u32))
    }

    async fn remove(&self, parent: Bytes) -> Result<(), NfsError> {
        self.storage.mount.remove(parent, self.name).await
    }

    async fn forget(&self) -> Result<(), NfsProtocolFailure> {
        let generation = self.storage.refresh_generation.load(Ordering::Acquire);
        self.storage
            .maybe_refresh_root_fh(generation)
            .await
            .map_err(classify_role_error)?;
        invalidate_path_cache(self.components, &self.storage.get_root_fh());
        Ok(())
    }

    fn remember(&self, handle: &Bytes) {
        NFSStorage::cache_directory(
            (self.cache_path.clone(), self.storage.get_root_fh()),
            handle,
        );
    }

    async fn rebuild_parent(&self) -> Result<(), NfsProtocolFailure> {
        let parents = self.components.len().saturating_sub(1);
        self.storage
            .role_create_all(&self.components[..parents])
            .await
    }
}

#[cfg(test)]
#[path = "role_namespace_tests.rs"]
mod tests;
