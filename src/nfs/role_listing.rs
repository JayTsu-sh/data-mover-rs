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

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use nfs_rs::{Mount, NFSVersion, Nfs3ErrorCode, Nfs4ErrorCode, NfsError, RecoveryAction};
use tracing::info;

use super::{
    EntryEnum, FType3, NASEntry, NFSStorage, NfsEnrich, Result, invalidate_path_cache,
    is_nfs_noent, is_retryable_with_invalidation, nfs_timestamp, owners_are_strings,
};
use crate::model::{EntryKind, FailureClass, StoragePath, TimestampMetadata};
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
        let lookup = MountChildLookup {
            mount: mount.as_ref().as_ref(),
            directory: handle.clone(),
        };
        let entries = mount.readdirplus(handle).await;
        drain_readdirplus(path, entries, pages, self.version(), &lookup).await
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

/// The name ONTAP gives a volume's snapshot directory over NFS.
const SNAPSHOT_DIRECTORY: &str = ".snapshot";

/// LOOKUP and GETATTR of a child of the directory being read, for the one entry READDIRPLUS
/// lists without attributes that the listing looks at again (see [`snapshot_directory`]).
#[async_trait]
pub(super) trait ChildLookup: Sync {
    /// LOOKUP of the child `name` of the directory being read.
    async fn lookup(&self, name: &str) -> nfs_rs::Result<nfs_rs::ObjRes>;
    /// GETATTR of `handle`.
    async fn getattr(&self, handle: Bytes) -> nfs_rs::Result<nfs_rs::Attr>;
}

/// [`ChildLookup`] on a mount, in the directory `directory`.
struct MountChildLookup<'a> {
    mount: &'a dyn Mount,
    directory: Bytes,
}

#[async_trait]
impl ChildLookup for MountChildLookup<'_> {
    async fn lookup(&self, name: &str) -> nfs_rs::Result<nfs_rs::ObjRes> {
        self.mount.lookup(self.directory.clone(), name).await
    }

    async fn getattr(&self, handle: Bytes) -> nfs_rs::Result<nfs_rs::Attr> {
        self.mount.getattr(handle).await
    }
}

/// What the listing makes of a READDIRPLUS entry before describing it.
enum Examined {
    /// Describe it as it is (or as looked up).
    Child(nfs_rs::ReaddirplusEntry),
    /// ONTAP's snapshot directory: hidden, not a child.
    Hidden,
    /// Looking it up failed: a child failure with the lookup's class.
    Failed(NfsUndescribedChild),
    /// Looking it up failed in a way that ends the read (see [`lookup_failed`]).
    Abort(NfsReadFailure),
}

/// Looks again at an `NFSv3` entry named `.snapshot` that READDIRPLUS listed without attributes
/// (P7 C7b, user decision 2026-09-26). ONTAP 9.19.1 lists its snapshot directory so in a volume's
/// root (handle, no `post_op_attr`); its `NFSv4` READDIR leaves it out, and it reserves the name
/// (MKDIR `.snapshot` answers `EXIST` in any directory). One LOOKUP (and a GETATTR when that reply
/// carries no attributes) settles it:
/// - a directory → hidden like a transfer artifact (counted in `internal_hidden`, logged, not
///   descended into), so v3 lists what v4 lists and the traversal stays out of the snapshots;
/// - anything else → an ordinary child described by what the lookup returned;
/// - the lookup failed → a failure of that child with the lookup's own class (`NOENT` is
///   `NotFound`: it vanished), or the end of the read when the failure is the session's, a busy
///   server's or the directory handle's ([`lookup_failed`]).
///
/// Every other entry goes on untouched: one with attributes (a real `.snapshot` a server
/// describes, as DXN does), any other name, any entry over `NFSv4`. Legacy `walkdir` fills the
/// attributes in with a GETATTR and descends into it.
async fn snapshot_directory(
    directory: &StoragePath,
    entry: nfs_rs::ReaddirplusEntry,
    version: NFSVersion,
    lookup: &dyn ChildLookup,
) -> Examined {
    if !matches!(version, NFSVersion::NFSv3)
        || entry.file_name != SNAPSHOT_DIRECTORY
        || entry.attr.is_some()
    {
        return Examined::Child(entry);
    }
    let child = Path::new(directory.as_str()).join(SNAPSHOT_DIRECTORY);
    match looked_up(lookup).await {
        Ok((_, attr)) if attr.type_ == FType3::NF3DIR as u32 => {
            info!(
                path = %child.display(),
                "NFSv3 listed .snapshot without attributes and it is a directory (presumably ONTAP's snapshot directory); hidden, not traversed"
            );
            Examined::Hidden
        }
        Ok((handle, attr)) => Examined::Child(nfs_rs::ReaddirplusEntry {
            attr: Some(attr),
            // A copy: the LOOKUP reply is not kept alive by the listing.
            handle: Bytes::copy_from_slice(&handle),
            ..entry
        }),
        Err(failed) => lookup_failed(directory, &child, failed),
    }
}

/// A failed LOOKUP (`true`: sent with the directory's handle) or GETATTR (`false`) of a child.
type LookupFailure = (NfsError, bool);

/// The handle and attributes of the `.snapshot` of the directory being read: its LOOKUP, and a
/// GETATTR of the handle it returned when that reply carries no attributes.
async fn looked_up(
    lookup: &dyn ChildLookup,
) -> std::result::Result<(Bytes, nfs_rs::Attr), LookupFailure> {
    let found = lookup
        .lookup(SNAPSHOT_DIRECTORY)
        .await
        .map_err(|error| (error, true))?;
    match found.attr {
        Some(attr) => Ok((found.fh, attr)),
        None => match lookup.getattr(found.fh.clone()).await {
            Ok(attr) => Ok((found.fh, attr)),
            Err(error) => Err((error, false)),
        },
    }
}

/// What a failed look at the child `child` of `directory` makes of it. A failure that says more
/// about the read than about the child ends the read as a READDIRPLUS failure would, so the
/// listing retries, backs off or fails as it does for those: the session is gone
/// (`Connectivity`), the server is busy (`Throttled`), or the LOOKUP's handle — the directory's —
/// no longer names it. Anything else (`NOENT`: it vanished; `ACCES`; a stale child handle) is
/// that child's failure, with its own class and status.
fn lookup_failed(directory: &StoragePath, child: &Path, failed: LookupFailure) -> Examined {
    let (error, of_directory) = failed;
    // A LOOKUP's `NOENT` is the child's absence; only for READDIRPLUS does it mean the directory.
    let stale_directory = of_directory && !is_nfs_noent(server_error(&error));
    let failure = read_failure(error);
    if matches!(
        failure.failure.class,
        FailureClass::Connectivity | FailureClass::Throttled
    ) || (stale_directory && failure.restart == NfsRestart::Relookup)
    {
        return Examined::Abort(failure);
    }
    let mut undescribed = undescribed_child(
        directory,
        child,
        "the server omitted its attributes and looking it up failed",
    );
    undescribed.failure = failure.failure;
    Examined::Failed(undescribed)
}

/// Hands every READDIRPLUS entry of `entries` (the directory `directory`, read over `version`)
/// to `pages` in the order the server sent it, until the directory ends or `pages` says to stop.
/// It asks `pages` before taking every entry, so a stopped listing sends no further READDIRPLUS
/// request (an entry already taken may still cost the LOOKUP of [`snapshot_directory`], which
/// `lookup` sends in `directory`).
pub(super) async fn drain_readdirplus<S>(
    directory: &StoragePath,
    mut entries: S,
    pages: &mut dyn NfsListingPages,
    version: NFSVersion,
    lookup: &dyn ChildLookup,
) -> std::result::Result<NfsReadEnd, NfsReadFailure>
where
    S: Stream<Item = nfs_rs::Result<nfs_rs::ReaddirplusEntry>> + Unpin + Send,
{
    let native = PathBuf::from(directory.as_str());
    let owners_are_strings = owners_are_strings(version);
    loop {
        if pages.stopping() {
            return Ok(NfsReadEnd::Stopped);
        }
        let Some(next) = entries.next().await else {
            return Ok(NfsReadEnd::Complete);
        };
        let entry = next.map_err(read_failure)?;
        let listed = match snapshot_directory(directory, entry, version, lookup).await {
            Examined::Child(entry) => {
                let child = native.join(&entry.file_name);
                role_child(
                    directory,
                    &NFSStorage::role_listed_entry(child, entry, owners_are_strings),
                )
            }
            Examined::Hidden => {
                pages.hide();
                continue;
            }
            Examined::Failed(failed) => Err(failed),
            Examined::Abort(failure) => return Err(failure),
        };
        if pages.push(listed) && !pages.hand_over().await {
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
