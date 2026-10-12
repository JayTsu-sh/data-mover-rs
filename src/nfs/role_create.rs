//! What the NFS role's write paths create before they write: every missing level of a directory
//! path, and a new file in it.
//!
//! These role primitives preserve native error classification and the optimized handle cache.
//! A refused MKDIR or CREATE keeps its server status; existing directories are reused.
//!
//! - [`NFSStorage::role_create_all`] creates the levels one by one with the one-level role verb
//!   (`src/nfs/role_namespace.rs`), starting below the deepest level the handle cache holds, and
//!   applies `storage::create_directory_all`'s rule for a refused level: an entry
//!   `PermissionDenied` costs one probe of that level and counts as "this level exists" only when
//!   it is a directory; a probe that fails beyond the level (`Cancelled`, `Connectivity`,
//!   `Authentication`, `Throttled`) reports itself; otherwise the refusal stands.
//! - [`NFSStorage::role_create_file_exclusive`] creates those levels, then CREATEs the file
//!   exclusively, classifying the server's answer itself (`classify_error`, keeping its status). A
//!   parent that is gone once its stale cached handle was forgotten — deleted after this process
//!   cached it, so the level creation passed it without an RPC — is rebuilt once within the call.
//! - [`NFSStorage::role_rename`] (`src/nfs/role_rename.rs`) creates the missing levels of the
//!   destination's parent the same way before it renames.
//! - [`NFSStorage::role_lookup_fh`] resolves a path through the handle cache as the legacy
//!   `lookup_fh` does (same stale / busy retries) but classifies the LOOKUP that failed itself.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use bytes::Bytes;
use nfs_rs::NfsError;

use super::{
    FType3, LookupFailure, MAX_STALE_RETRIES, NFSFileHandle, NFSStorage, backoff_server_busy,
    classify_role_error, invalidate_path_cache, is_retryable_with_invalidation, is_server_busy,
};
use crate::model::{FailureClass, StoragePath, Transience};
use crate::storage::backends::nfs::protocol::classify_error;
use crate::storage::backends::nfs::source::NfsProtocolFailure;

impl NFSStorage {
    /// The object `path` names, through the directory handle cache, with the legacy lookup's
    /// retries; a failed LOOKUP is classified from its own error: `ACCES` / `PERM` →
    /// `PermissionDenied` / Permanent, `NOENT` (once the retries are spent) → `NotFound`, a lost
    /// session → `Connectivity`, keeping the server's status.
    pub(super) async fn role_lookup_fh(
        &self,
        path: &Path,
    ) -> Result<nfs_rs::ObjRes, NfsProtocolFailure> {
        self.lookup_fh_inner(path, 0).await.map_err(lookup_failure)
    }

    /// Creates every missing level of the directory `components` name (the backend root when
    /// empty), one level at a time; see the module docs.
    pub(super) async fn role_create_all(
        &self,
        components: &[String],
    ) -> Result<(), NfsProtocolFailure> {
        create_levels(&MountLevels {
            storage: self,
            components,
        })
        .await
    }

    /// Creates the new, empty file `path` (and every missing level of its parent) without a
    /// prior existence probe. `Ok(None)` is the exclusive-create collision: an existing file is
    /// never opened, so a successful CREATE already proves a follow-up truncate redundant. See
    /// the module docs.
    pub(super) async fn role_create_file_exclusive(
        &self,
        path: &Path,
        mode: Option<u32>,
    ) -> Result<Option<NFSFileHandle>, NfsProtocolFailure> {
        let components = Self::collect_components(path).map_err(classify_role_error)?;
        let Some((name, parents)) = components.split_last() else {
            return Err(NfsProtocolFailure::new(
                FailureClass::InvalidInput,
                Transience::Permanent,
            ));
        };
        let calls = MountFile {
            storage: self,
            path,
            parent: parents.iter().collect(),
            parents,
            components: &components,
            name,
            mode,
        };
        let created = create_exclusive(&calls).await?;
        Ok(created.map(|handle| NFSFileHandle::new(handle, path.to_path_buf())))
    }
}

/// A parent that is gone after its stale cached handle was forgotten (`forgotten`) was deleted
/// after this process cached it: `NotFound` / Transient, since the cache is clean now and a new
/// attempt recreates it — not the Permanent of a parent missing from the start.
pub(super) fn transient_if_gone(
    failure: NfsProtocolFailure,
    forgotten: bool,
) -> NfsProtocolFailure {
    if forgotten && failure.class == FailureClass::NotFound {
        NfsProtocolFailure {
            transience: Transience::Transient,
            ..failure
        }
    } else {
        failure
    }
}

/// A failed role lookup, classified from the LOOKUP's own error when a LOOKUP failed.
fn lookup_failure(failure: LookupFailure) -> NfsProtocolFailure {
    match failure {
        LookupFailure::Storage(error) => classify_role_error(error),
        LookupFailure::Component { error, .. } => classify_error(error),
    }
}

/// The calls creating every missing level of one directory path makes.
#[async_trait]
trait Levels: Sync {
    /// How many levels the path has.
    fn depth(&self) -> usize;
    /// How many leading levels the handle cache holds: those are not asked again.
    fn cached(&self) -> usize;
    /// Creates the level of the first `depth` components, as the one-level role verb does.
    async fn create(&self, depth: usize) -> Result<(), NfsProtocolFailure>;
    /// Whether the level of the first `depth` components is a directory.
    async fn is_directory(&self, depth: usize) -> Result<bool, NfsProtocolFailure>;
    /// The path of the level of the first `depth` components, where its failure is reported:
    /// the components joined with `/`, so for a path spelled with empty or `.` segments the
    /// level is named in the normalized spelling (`d//a/f`'s parent level is `d/a`).
    fn level(&self, depth: usize) -> Option<StoragePath>;
}

/// Creates the missing levels; a failure is reported at the level that failed
/// ([`NfsProtocolFailure::at_level`]), not at the entry whose write or rename needed them. A
/// failure that says nothing about the level ([`outlives_the_level`]: a cancellation, a lost
/// session, a busy server) is not tagged.
async fn create_levels(levels: &dyn Levels) -> Result<(), NfsProtocolFailure> {
    for depth in levels.cached() + 1..=levels.depth() {
        if let Err(failure) = create_one(levels, depth).await {
            return Err(match levels.level(depth) {
                Some(level) if !outlives_the_level(&failure) => failure.at_level(level),
                _ => failure,
            });
        }
    }
    Ok(())
}

async fn create_one(levels: &dyn Levels, depth: usize) -> Result<(), NfsProtocolFailure> {
    let Err(refused) = levels.create(depth).await else {
        return Ok(());
    };
    if refused.class != FailureClass::PermissionDenied {
        return Err(refused);
    }
    // The server may check access before the name: only a directory already there turns the
    // refusal into "this level exists".
    match levels.is_directory(depth).await {
        Ok(true) => Ok(()),
        // A cancelled probe, a lost session or a busy server says nothing about the level:
        // reporting the refusal would make it look permanent.
        Err(probe) if outlives_the_level(&probe) => Err(probe),
        _ => Err(refused),
    }
}

/// `storage::create_directory_all`'s rule for a probe that fails beyond its entry.
fn outlives_the_level(probe: &NfsProtocolFailure) -> bool {
    matches!(
        probe.class,
        FailureClass::Cancelled
            | FailureClass::Connectivity
            | FailureClass::Authentication
            | FailureClass::Throttled
    )
}

/// [`Levels`] against the mount.
struct MountLevels<'a> {
    storage: &'a NFSStorage,
    components: &'a [String],
}

#[async_trait]
impl Levels for MountLevels<'_> {
    fn depth(&self) -> usize {
        self.components.len()
    }

    fn cached(&self) -> usize {
        self.storage.deepest_cached_directory(self.components).1
    }

    async fn create(&self, depth: usize) -> Result<(), NfsProtocolFailure> {
        self.storage
            .role_create_level(&self.components[..depth])
            .await
    }

    async fn is_directory(&self, depth: usize) -> Result<bool, NfsProtocolFailure> {
        let level: PathBuf = self.components[..depth].iter().collect();
        let object = self.storage.role_lookup_fh(&level).await?;
        let file_type = match object.attr {
            Some(attrs) => attrs.type_,
            None => {
                self.storage
                    .mount
                    .getattr(object.fh)
                    .await
                    .map_err(classify_error)?
                    .type_
            }
        };
        Ok(file_type == FType3::NF3DIR as u32)
    }

    fn level(&self, depth: usize) -> Option<StoragePath> {
        StoragePath::new(self.components[..depth].join("/")).ok()
    }
}

/// The calls an exclusive file creation makes.
#[async_trait]
trait FileCalls: Sync {
    /// Creates every missing level of the file's parent.
    async fn create_parents(&self) -> Result<(), NfsProtocolFailure>;
    /// The parent directory's handle, through the handle cache.
    async fn parent(&self) -> Result<Bytes, NfsProtocolFailure>;
    /// Exclusive CREATE of the file's name; the new file's handle.
    async fn create(&self, parent: Bytes) -> Result<Bytes, NfsError>;
    /// Refreshes the root handle and forgets the cached handles along the file's path.
    async fn forget(&self) -> Result<(), NfsProtocolFailure>;
    /// Waits before asking a busy server again.
    async fn back_off(&self, attempt: u8);
}

async fn create_exclusive(calls: &dyn FileCalls) -> Result<Option<Bytes>, NfsProtocolFailure> {
    calls.create_parents().await?;
    let mut attempt = 0;
    let mut forgotten = false;
    let mut rebuilt = false;
    loop {
        let parent = match calls.parent().await {
            // The parent's cached handle went stale and the parent is gone: it was deleted after
            // this process cached it, so `create_parents` passed it without an RPC. Rebuild it
            // once, as the legacy helper would have on its next call.
            Err(failure) if forgotten && failure.class == FailureClass::NotFound && !rebuilt => {
                rebuilt = true;
                calls.create_parents().await?;
                calls
                    .parent()
                    .await
                    .map_err(|failure| transient_if_gone(failure, forgotten))?
            }
            other => other.map_err(|failure| transient_if_gone(failure, forgotten))?,
        };
        let retry_left = attempt < MAX_STALE_RETRIES;
        match calls.create(parent).await {
            Ok(handle) => return Ok(Some(handle)),
            Err(error) if error.is_exist() => return Ok(None),
            Err(error) if retry_left && is_retryable_with_invalidation(&error) => {
                calls.forget().await?;
                forgotten = true;
            }
            Err(error) if retry_left && is_server_busy(&error) => calls.back_off(attempt).await,
            Err(error) => return Err(classify_error(error)),
        }
        attempt += 1;
    }
}

/// [`FileCalls`] against the mount.
struct MountFile<'a> {
    storage: &'a NFSStorage,
    path: &'a Path,
    parent: PathBuf,
    parents: &'a [String],
    components: &'a [String],
    name: &'a str,
    mode: Option<u32>,
}

#[async_trait]
impl FileCalls for MountFile<'_> {
    async fn create_parents(&self) -> Result<(), NfsProtocolFailure> {
        self.storage.role_create_all(self.parents).await
    }

    async fn parent(&self) -> Result<Bytes, NfsProtocolFailure> {
        if self.parents.is_empty() {
            return Ok(self.storage.rpc_root_fh());
        }
        Ok(self.storage.role_lookup_fh(&self.parent).await?.fh)
    }

    async fn create(&self, parent: Bytes) -> Result<Bytes, NfsError> {
        let object = self
            .storage
            .mount
            .create(parent, self.name, self.mode)
            .await?;
        Ok(object.fh)
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

    async fn back_off(&self, attempt: u8) {
        backoff_server_busy("create_file_exclusive", &self.path, attempt).await;
    }
}

#[cfg(test)]
#[path = "role_create_tests.rs"]
mod tests;
