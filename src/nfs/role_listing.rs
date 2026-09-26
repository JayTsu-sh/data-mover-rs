//! The NFS role listing (P7 C7): READDIRPLUS read one reply at a time and handed to the listing's
//! pages as it arrives, each entry turned into what the Namespace role lists.
//!
//! Nothing here caches a handle. Before C7 the listing put every listed subdirectory's handle into
//! the process-wide handle cache so the descent could skip a LOOKUP; the descent now carries the
//! handle itself as the child's descent hint (`storage::backends::nfs::listing`), and the cache held
//! about 625 B per directory of a whole tree (P7 §12.7).

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use nfs_rs::{Nfs3ErrorCode, Nfs4ErrorCode, NfsError, RecoveryAction};

use super::{
    EntryEnum, NASEntry, NFSStorage, NfsEnrich, Result, invalidate_path_cache,
    is_retryable_with_invalidation, nfs_timestamp,
};
use crate::model::{EntryKind, StoragePath, TimestampMetadata};
use crate::storage::backends::nfs::namespace::{
    NfsListedChild, NfsListingPages, NfsNamespaceObservation, NfsReadEnd, NfsReadFailure,
    NfsRestart, NfsUndescribedChild, inline_ownership, lossily_decoded, undescribed_child,
    unnamed_child,
};
use crate::storage::backends::nfs::protocol::classify_error;

/// A READDIRPLUS entry the role listing could not turn into an [`EntryEnum`].
pub(crate) struct RoleOmittedEntry {
    relative_path: PathBuf,
    reason: &'static str,
}

/// One READDIRPLUS entry of a role listing, in the order the server sent it.
pub(crate) type RoleListedEntry = std::result::Result<EntryEnum, RoleOmittedEntry>;

/// One entry of a role listing as the NFS namespace protocol reports it.
pub(super) fn role_child(directory: &StoragePath, child: &RoleListedEntry) -> NfsListedChild {
    match child {
        Ok(entry) => role_observation(directory, entry),
        Err(omitted) => Err(undescribed_child(
            directory,
            &omitted.relative_path,
            omitted.reason,
        )),
    }
}

fn role_observation(
    directory: &StoragePath,
    entry: &EntryEnum,
) -> std::result::Result<NfsNamespaceObservation, NfsUndescribedChild> {
    let child = entry.get_relative_path();
    if let Some(name) = child.file_name().map(|name| name.to_string_lossy())
        && lossily_decoded(&name)
    {
        return Err(unnamed_child(directory, &name));
    }
    // An empty handle is what `post_op_fh3::FALSE` (v3) or a v4 entry whose attributes lack
    // `FATTR4_FILEHANDLE` leaves: no identity can be built from it.
    let file_handle = entry
        .get_file_handle()
        .filter(|handle| !handle.is_empty())
        .cloned()
        .ok_or_else(|| undescribed_child(directory, child, "the server returned no file handle"))?;
    let kind = if entry.get_is_symlink() {
        EntryKind::Symlink
    } else if entry.get_is_dir() {
        EntryKind::Directory
    } else {
        EntryKind::File
    };
    let path = StoragePath::new(child.to_string_lossy())
        .map_err(|_| undescribed_child(directory, child, "its name cannot be spelled"))?;
    let (timestamps, mode, ownership) = match entry {
        EntryEnum::NAS(nas) => (
            Some(TimestampMetadata {
                accessed: nfs_timestamp(nas.atime),
                modified: nfs_timestamp(nas.mtime),
                created: nfs_timestamp(nas.ctime),
            }),
            Some(nas.mode),
            // `nas.mode` has the set-id bits of an owner it could not map stripped
            // (`without_unowned_set_id`), but only then is it not the raw attribute mode, and
            // then the ownership is `Unmapped` and carries no mode at all.
            Some(inline_ownership(nas.uid, nas.gid, nas.mode)),
        ),
        _ => (None, None, None),
    };
    Ok(NfsNamespaceObservation {
        path,
        kind,
        size: (kind == EntryKind::File).then(|| entry.get_size()),
        file_handle,
        timestamps,
        mode,
        ownership,
    })
}

impl NFSStorage {
    /// The handle of the directory `path` names, through the directory handle cache.
    pub(super) async fn role_directory_handle(&self, path: &StoragePath) -> Result<Bytes> {
        Ok(self.lookup_fh(Path::new(path.as_str())).await?.fh)
    }

    /// Reads the directory `handle` with READDIRPLUS into `pages` (see
    /// `NfsNamespaceProtocol::read_directory`). nfs-rs asks for the next reply only once the
    /// entries of the last one have been taken, so the read runs no further ahead than `pages`.
    pub(super) async fn read_role_directory(
        &self,
        path: &StoragePath,
        handle: Bytes,
        pages: &mut dyn NfsListingPages,
    ) -> std::result::Result<NfsReadEnd, NfsReadFailure> {
        let mount = Arc::clone(&self.mount);
        let entries = mount.readdirplus(handle).await;
        drain_readdirplus(path, entries, pages, self.owners_are_strings()).await
    }

    /// Forgets the cached handles of `path` and its ancestors, refreshing the root handle first
    /// when it is the one that went stale.
    pub(super) async fn forget_role_directory(&self, path: &StoragePath) -> Result<()> {
        let stale_generation = self.refresh_generation.load(Ordering::Acquire);
        self.maybe_refresh_root_fh(stale_generation).await?;
        let components = Self::collect_components(Path::new(path.as_str()))?;
        invalidate_path_cache(&components, &self.get_root_fh());
        Ok(())
    }

    /// One READDIRPLUS entry of a role listing. READDIRPLUS may leave an entry's attributes
    /// out (`post_op_attr` is optional in v3): that entry cannot be described, its siblings still
    /// can.
    fn role_listed_entry(
        child: PathBuf,
        entry: nfs_rs::ReaddirplusEntry,
        owners_are_strings: bool,
    ) -> RoleListedEntry {
        let Some(attrs) = entry.attr else {
            return Err(RoleOmittedEntry {
                relative_path: child,
                reason: "the server omitted its attributes",
            });
        };
        let extension = child
            .extension()
            .and_then(|value| value.to_str())
            .map(str::to_owned);
        Ok(EntryEnum::NAS(NASEntry::from_nfs_attrs(
            entry.file_name,
            child,
            extension,
            &attrs,
            entry.handle,
            // The only consumer of this listing is `role_observation`, which reads the handle,
            // the kind, the path, the timestamps, the mode, the numeric uid / gid and the size —
            // never the ACL or the raw owner and group strings (the numeric ids are mapped from
            // them before this). Filling those would clone two `String`s and an `Acl` per entry
            // for nothing, which at fifty million entries is not nothing. Restore `from_attrs` here the moment
            // that observation starts reporting them.
            NfsEnrich::default(),
            owners_are_strings,
        )))
    }
}

/// Hands every READDIRPLUS entry of `entries` (the directory `directory`) to `pages` in the
/// order the server sent it, until the directory ends or `pages` says to stop. It asks
/// `pages` before taking every entry, so a stopped listing sends no further request.
pub(super) async fn drain_readdirplus<S>(
    directory: &StoragePath,
    mut entries: S,
    pages: &mut dyn NfsListingPages,
    owners_are_strings: bool,
) -> std::result::Result<NfsReadEnd, NfsReadFailure>
where
    S: Stream<Item = nfs_rs::Result<nfs_rs::ReaddirplusEntry>> + Unpin + Send,
{
    let native = PathBuf::from(directory.as_str());
    loop {
        if pages.stopping() {
            return Ok(NfsReadEnd::Stopped);
        }
        let Some(next) = entries.next().await else {
            return Ok(NfsReadEnd::Complete);
        };
        let entry = next.map_err(read_failure)?;
        let child = native.join(&entry.file_name);
        let listed = NFSStorage::role_listed_entry(child, entry, owners_are_strings);
        if pages.push(role_child(directory, &listed)) && !pages.hand_over().await {
            return Ok(NfsReadEnd::Stopped);
        }
    }
}

/// A READDIRPLUS failure, with what reading the directory anew could do about it.
fn read_failure(error: NfsError) -> NfsReadFailure {
    let restart = restart_for(&error);
    NfsReadFailure {
        failure: classify_error(error),
        restart,
    }
}

/// The server's own error, looking through the recovery wrapper nfs-rs puts around it.
fn server_error(error: &NfsError) -> &NfsError {
    match error {
        NfsError::OperationOutcome(outcome) => server_error(&outcome.source),
        other => other,
    }
}

/// What reading anew could do about `error`. Wrapped in nfs-rs's recovery outcome, only
/// `Retry` makes a transport failure worth resending (`Remount` means the session must be
/// replaced), and looking inside only names the server's status.
fn restart_for(error: &NfsError) -> NfsRestart {
    if let Some(outcome) = error.operation_outcome() {
        if matches!(outcome.recovery, RecoveryAction::Retry) {
            return NfsRestart::Resend;
        }
        return restart_for_status(server_error(error));
    }
    match error {
        NfsError::Io(io)
            if matches!(io.kind(), ErrorKind::ConnectionReset | ErrorKind::TimedOut) =>
        {
            NfsRestart::Resend
        }
        status => restart_for_status(status),
    }
}

/// What reading anew could do about the server's status.
fn restart_for_status(error: &NfsError) -> NfsRestart {
    match error {
        NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_FHEXPIRED) => NfsRestart::Relookup,
        NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_DELAY | Nfs4ErrorCode::NFS4ERR_GRACE)
        | NfsError::Nfs3(Nfs3ErrorCode::NFS3ERR_JUKEBOX) => NfsRestart::Backoff,
        error if is_retryable_with_invalidation(error) => NfsRestart::Relookup,
        _ => NfsRestart::Never,
    }
}

#[cfg(test)]
#[path = "role_listing_tests.rs"]
mod tests;
