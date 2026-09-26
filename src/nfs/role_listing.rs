//! The NFS role listing: READDIRPLUS entries turned into what the Namespace role lists.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use bytes::Bytes;
use futures::StreamExt;
use nfs_rs::NfsError;

use super::{
    EntryEnum, FType3, MAX_STALE_RETRIES, NASEntry, NFSStorage, NfsEnrich, Result, StorageError,
    backoff_server_busy, invalidate_path_cache, is_retryable_with_invalidation, is_server_busy,
    nfs_timestamp,
};
use crate::storage::backends::nfs::namespace::{
    NfsListedChild, NfsNamespaceObservation, NfsUndescribedChild, inline_ownership,
    lossily_decoded, undescribed_child, unnamed_child,
};

/// A READDIRPLUS entry the role listing could not turn into an [`EntryEnum`].
pub(crate) struct RoleOmittedEntry {
    relative_path: PathBuf,
    reason: &'static str,
}

/// One READDIRPLUS entry of a role listing, in the order the server sent it.
pub(crate) type RoleListedEntry = std::result::Result<EntryEnum, RoleOmittedEntry>;

/// One entry of a role listing as the NFS namespace protocol reports it.
pub(super) fn role_child(
    directory: &crate::model::StoragePath,
    child: &RoleListedEntry,
) -> NfsListedChild {
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
    directory: &crate::model::StoragePath,
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
        crate::model::EntryKind::Symlink
    } else if entry.get_is_dir() {
        crate::model::EntryKind::Directory
    } else {
        crate::model::EntryKind::File
    };
    let path = crate::model::StoragePath::new(child.to_string_lossy())
        .map_err(|_| undescribed_child(directory, child, "its name cannot be spelled"))?;
    let (timestamps, mode, ownership) = match entry {
        EntryEnum::NAS(nas) => (
            Some(crate::model::TimestampMetadata {
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
        size: (kind == crate::model::EntryKind::File).then(|| entry.get_size()),
        file_handle,
        timestamps,
        mode,
        ownership,
    })
}

impl NFSStorage {
    /// Whether a listing entry's handle is worth caching. Only directories are ever listed, and
    /// an empty handle means the server supplied none — `post_op_fh3::FALSE` on v3, or a v4
    /// `FATTR4_FILEHANDLE` that did not decode. Caching that would hand out a handle no RPC can
    /// use.
    fn caches_directory_handle(file_type: u32, handle: &Bytes) -> bool {
        file_type == FType3::NF3DIR as u32 && !handle.is_empty()
    }

    /// Lists exactly one directory for the storage Namespace role.
    ///
    /// A cached directory handle is handed back without being verified — `lookup_fh` sends no
    /// RPC when the whole path is cached — so a stale one first shows up here, at
    /// `readdirplus`. Invalidating the path and listing again is what makes that self-healing;
    /// every other NFS site already does this and this one did not, which left a directory
    /// failing for as long as the cache entry lived (two hours for a shallow path).
    ///
    /// Entries keep the order the server sent them in. The traversal contract deliberately does
    /// not constrain sibling order (`TraversalOrder::Blocks` keeps a block in listing order and
    /// sorts only the descent, in the cursor). Sorting here bought nothing: it cost an
    /// `O(n log n)` pass per directory, and it was the reason this backend looked ordered while
    /// the CIFS and Local namespaces, which do not sort, did not.
    pub(super) async fn list_role_entries(
        &self,
        relative_path: &Path,
    ) -> Result<Vec<RoleListedEntry>> {
        for attempt in 0..=MAX_STALE_RETRIES {
            match self.list_role_entries_once(relative_path).await? {
                Ok(entries) => return Ok(entries),
                Err(error)
                    if is_retryable_with_invalidation(&error) && attempt < MAX_STALE_RETRIES =>
                {
                    let stale_generation = self.refresh_generation.load(Ordering::Acquire);
                    self.maybe_refresh_root_fh(stale_generation).await?;
                    let root_fh = self.get_root_fh();
                    let components = Self::collect_components(relative_path)?;
                    invalidate_path_cache(&components, &root_fh);
                }
                Err(error) if is_server_busy(&error) && attempt < MAX_STALE_RETRIES => {
                    backoff_server_busy("list_role_entries", &relative_path, attempt).await;
                }
                Err(error) => {
                    return Err(StorageError::NfsError(format!(
                        "NFS directory listing failed: {error}"
                    )));
                }
            }
        }
        Err(StorageError::NfsError(format!(
            "NFS directory listing failed for {} after {MAX_STALE_RETRIES} retries",
            relative_path.display()
        )))
    }

    /// One listing attempt. The outer `Result` is terminal; the inner one hands the protocol
    /// error back so the caller can decide whether invalidating the cached handle is worth a
    /// retry.
    async fn list_role_entries_once(
        &self,
        relative_path: &Path,
    ) -> Result<std::result::Result<Vec<RoleListedEntry>, NfsError>> {
        let directory = self.lookup_fh(relative_path).await?;
        let mount = self.mount.clone();
        let mut stream = mount.readdirplus(directory.fh).await;
        let mut entries = Vec::new();
        while let Some(result) = stream.next().await {
            let entry = match result {
                Ok(entry) => entry,
                // Partial results are dropped on purpose: the caller starts the listing over,
                // and keeping them would duplicate every entry read before the failure.
                Err(error) => return Ok(Err(error)),
            };
            let child = relative_path.join(&entry.file_name);
            if let Some(attrs) = &entry.attr
                && Self::caches_directory_handle(attrs.type_, &entry.handle)
            {
                // The listing already carries this directory's handle, so caching it is what
                // spares the descent a LOOKUP (`cache_directory` copies it out of the page).
                Self::cache_directory(
                    (Self::cache_path(&child)?, self.get_root_fh()),
                    &entry.handle,
                );
            }
            entries.push(Self::role_listed_entry(
                child,
                entry,
                self.owners_are_strings(),
            ));
        }
        Ok(Ok(entries))
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

#[cfg(test)]
mod tests {
    use super::super::{Time, role_metadata};
    use super::*;

    /// Only directories are ever listed, and an empty handle is the server saying it has none.
    #[test]
    fn only_a_directory_with_a_handle_is_cached() {
        let handle = Bytes::from_static(b"fh");
        let empty = Bytes::new();
        assert!(NFSStorage::caches_directory_handle(
            FType3::NF3DIR as u32,
            &handle
        ));
        assert!(!NFSStorage::caches_directory_handle(
            FType3::NF3DIR as u32,
            &empty
        ));
        assert!(!NFSStorage::caches_directory_handle(
            FType3::NF3REG as u32,
            &handle
        ));
        assert!(!NFSStorage::caches_directory_handle(
            FType3::NF3LNK as u32,
            &handle
        ));
    }

    fn listed(name: &str, attr: Option<nfs_rs::Attr>, handle: &'static [u8]) -> RoleListedEntry {
        NFSStorage::role_listed_entry(
            PathBuf::from("dir").join(name),
            nfs_rs::ReaddirplusEntry {
                fileid: 1,
                file_name: name.to_owned(),
                attr,
                handle: Bytes::from_static(handle),
            },
            false,
        )
    }

    /// A child READDIRPLUS listed carries the ownership and timestamps the metadata role reads
    /// from a GETATTR of the same attributes — through `NASEntry::from_nfs_attrs` (set-id bits
    /// stripped for an owner it cannot map, owner strings mapped by `owner_id`) and
    /// `role_observation` — for files, directories and symlinks with set-id bits, and for the
    /// owner forms `NFSv3`, ONTAP and an LDAP-backed server send.
    #[test]
    fn a_listed_childs_ownership_and_times_are_what_a_getattr_reports() {
        let directory =
            crate::model::StoragePath::new("dir").unwrap_or_else(|error| panic!("{error}"));
        let owners = [
            (false, "", "", 1000),
            (true, "1000", "1000", 1000),
            (true, "root@localdomain", "0", 0),
            (true, "alice@example.com", "1000", 65534),
            (true, "", "", 0),
        ];
        let mut checked = 0;
        for (type_, mode) in [
            (FType3::NF3REG, 0o106_755),
            (FType3::NF3DIR, 0o042_775),
            (FType3::NF3LNK, 0o120_777),
        ] {
            for (owners_are_strings, owner, group, parsed) in owners {
                let attrs = nfs_rs::Attr {
                    type_: type_ as u32,
                    file_mode: mode,
                    uid: parsed,
                    gid: 1000,
                    atime: Time {
                        seconds: 1_700_000_000,
                        nseconds: 5,
                    },
                    mtime: Time {
                        seconds: 1_700_000_100,
                        nseconds: 6,
                    },
                    ctime: Time {
                        seconds: 1_700_000_200,
                        nseconds: 7,
                    },
                    owner: owner.to_owned(),
                    owner_group: group.to_owned(),
                    ..nfs_rs::Attr::default()
                };
                let child = NFSStorage::role_listed_entry(
                    PathBuf::from("dir/x"),
                    nfs_rs::ReaddirplusEntry {
                        fileid: 1,
                        file_name: "x".to_owned(),
                        attr: Some(attrs.clone()),
                        handle: Bytes::from_static(b"h"),
                    },
                    owners_are_strings,
                );
                let listed = role_child(&directory, &child)
                    .unwrap_or_else(|_| panic!("a child with attributes is described"));
                let getattr = role_metadata::metadata_inline(
                    Bytes::from_static(b"h"),
                    &attrs,
                    owners_are_strings,
                );
                let context = format!("{type_:?} {owner:?}/{group:?}");
                assert_eq!(
                    listed.ownership,
                    Some(inline_ownership(getattr.uid, getattr.gid, getattr.mode)),
                    "{context}"
                );
                assert_eq!(
                    listed.timestamps,
                    Some(crate::model::TimestampMetadata {
                        accessed: nfs_timestamp(getattr.atime),
                        modified: nfs_timestamp(getattr.mtime),
                        created: nfs_timestamp(getattr.ctime),
                    }),
                    "{context}"
                );
                assert_eq!(
                    listed.kind == crate::model::EntryKind::Symlink,
                    getattr.symlink,
                    "{context}"
                );
                checked += 1;
            }
        }
        assert_eq!(checked, 15);
    }

    fn regular() -> nfs_rs::Attr {
        nfs_rs::Attr {
            type_: FType3::NF3REG as u32,
            filesize: 3,
            ..nfs_rs::Attr::default()
        }
    }

    /// A READDIRPLUS entry without attributes, or without a file handle, cannot be described:
    /// it becomes an entry failure at its own path (`Protocol`, retrying may or may not help),
    /// while the entries beside it are still observed.
    #[test]
    fn an_entry_without_attributes_or_handle_fails_alone() {
        let directory =
            crate::model::StoragePath::new("dir").unwrap_or_else(|error| panic!("{error}"));
        let children: Vec<NfsListedChild> = [
            listed("before", Some(regular()), b"h1"),
            listed("no-attrs", None, b"h2"),
            listed("no-handle", Some(regular()), b""),
            listed("after", Some(regular()), b"h3"),
        ]
        .iter()
        .map(|child| role_child(&directory, child))
        .collect();
        let observed: Vec<&str> = children
            .iter()
            .filter_map(|child| child.as_ref().ok())
            .map(|entry| entry.path.as_str())
            .collect();
        assert_eq!(observed, ["dir/before", "dir/after"]);
        let failed: Vec<(&str, &str)> = children
            .iter()
            .filter_map(|child| child.as_ref().err())
            .map(|child| (child.path.as_str(), child.reason))
            .collect();
        assert_eq!(
            failed,
            [
                ("dir/no-attrs", "the server omitted its attributes"),
                ("dir/no-handle", "the server returned no file handle"),
            ]
        );
        for child in children.iter().filter_map(|child| child.as_ref().err()) {
            assert_eq!(child.failure.class, crate::model::FailureClass::Protocol);
            assert_eq!(child.failure.transience, crate::model::Transience::Unknown);
        }
    }
}
