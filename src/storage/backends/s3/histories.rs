//! The listing of every stored version of one S3 directory (ADR-0006 C22; P7 §5.3, Q9), gathered
//! whole before its children are built: on a store that does not keep keys in order a key's
//! versions can be on any page, so only a directory listed to its end has whole histories.

use std::collections::HashSet;

use super::history::{KeyHistory, oldest_first, warn_inferred};
use super::listing::{
    Block, Child, child_failure, directory_prefix, file_descriptor, merge, object_facts, place,
    refuse_absent,
};
use super::paging::{ListingLimits, PageRetry, Paging};
use super::source::{path_scoped_bytes, role_failure};
use super::{S3ListedVersion, S3Protocol, S3RejectedEntry, S3VersionMarker, is_real_version_id};
use crate::model::{
    BackendIdentity, EntryKind, EntryOperationFailure, EntryVersion, FailureClass, HistoryOrder,
    IdentityStrength, Operation, SourceIdentity, SourceVersion, StoragePath, StorageTimestamp,
    TimestampMetadata,
};
use crate::storage::{ListingFacts, ListingPage, SourceDescriptor, StorageRoleFailure};

/// Identity bytes of a delete marker that names no real version (a suspended bucket's `"null"`).
const NULL_MARKER_IDENTITY: &[u8] = b"\0s3-null-delete-marker";

/// Every page of a versions listing, gathered.
#[derive(Default)]
struct VersionPages {
    versions: Vec<S3ListedVersion>,
    markers: Vec<S3ListedVersion>,
    prefixes: Vec<String>,
    rejected: Vec<S3RejectedEntry>,
}

/// Pages through every version of `prefix`, backing off while throttled and sending a page that
/// failed transiently again with the same marker.
async fn page_versions<P: S3Protocol + ?Sized>(
    protocol: &P,
    directory: &StoragePath,
    prefix: &str,
    limits: ListingLimits,
) -> Result<VersionPages, StorageRoleFailure> {
    let mut paging = Paging::new(limits);
    let mut retry = PageRetry::new(limits);
    let mut marker: Option<S3VersionMarker> = None;
    let mut all = VersionPages::default();
    // Positions are per page; a later page's follow the earlier pages' (document order of the
    // whole listing).
    let mut listed_before = 0_u64;
    loop {
        let page = retry
            .fetch(|| protocol.list_versions_page(prefix, marker.as_ref()))
            .await
            .map_err(|e| role_failure(directory, Operation::Traverse, e))?;
        let entries = page.versions.len() + page.markers.len();
        let listed = entries + page.prefixes.len() + page.rejected.len();
        all.versions
            .extend(listing_wide(page.versions, listed_before));
        all.markers
            .extend(listing_wide(page.markers, listed_before));
        listed_before += entries as u64;
        all.prefixes.extend(page.prefixes);
        all.rejected.extend(page.rejected);
        marker = paging.advance(directory, listed, page.next)?;
        if marker.is_none() {
            return Ok(all);
        }
    }
}

/// Lists every stored version and delete marker of the objects of `directory`, and its
/// subdirectories.
pub(super) async fn list_all<P: S3Protocol + ?Sized>(
    protocol: &P,
    identity: &BackendIdentity,
    directory: &StoragePath,
    limits: ListingLimits,
) -> Result<ListingPage, StorageRoleFailure> {
    let prefix = directory_prefix(directory)?;
    let VersionPages {
        mut versions,
        mut markers,
        prefixes,
        rejected,
    } = page_versions(protocol, directory, &prefix, limits).await?;
    let absent =
        versions.is_empty() && markers.is_empty() && prefixes.is_empty() && rejected.is_empty();
    refuse_absent(directory, absent)?;
    // A key with an entry that could not be decoded has no whole history: it is that key's
    // failure, and none of its entries is listed.
    let broken: HashSet<String> = rejected.iter().filter_map(|r| r.key.clone()).collect();
    versions.retain(|entry: &S3ListedVersion| !broken.contains(&entry.key));
    markers.retain(|entry: &S3ListedVersion| !broken.contains(&entry.key));
    let placed_versions = place(&prefix, versions, |entry| &entry.key);
    let placed_markers = place(&prefix, markers, |entry| &entry.key);
    let mut block = Block::new(identity, directory, &prefix);
    block.outside(&placed_versions.outside);
    block.outside(&placed_markers.outside);
    // The directory's own key has one history, whatever it holds (P7 F5, D5).
    let own = oldest_first(placed_versions.own, placed_markers.own);
    warn_inferred(directory.as_str(), &own);
    for history in &own {
        block.own_history(history);
    }
    let histories = oldest_first(placed_versions.children, placed_markers.children);
    warn_inferred(directory.as_str(), &histories);
    let children = histories.into_iter().map(Child::History);
    let mut rolled = placed_versions.rolled;
    rolled.extend(placed_markers.rolled);
    block.rejected(rejected, &mut rolled, &mut HashSet::new());
    block.build(merge(children, prefixes, rolled));
    Ok(block.finish())
}

/// `entries` of one page with their positions moved after the `before` entries of earlier pages.
fn listing_wide(
    entries: Vec<S3ListedVersion>,
    before: u64,
) -> impl Iterator<Item = S3ListedVersion> {
    entries.into_iter().map(move |mut entry| {
        entry.position = entry.position.map(|position| before + position);
        entry
    })
}

impl Block<'_> {
    /// The whole history of the directory's own key `<path>/` (P7 F5, D5): every version, zero-byte
    /// ones included, and every delete marker, oldest first. At the storage root it has no path:
    /// ignored when no version holds content, a failure when one does.
    pub(super) fn own_history(&mut self, history: &KeyHistory<S3ListedVersion>) {
        if self.directory.as_str().is_empty() {
            let content = history.entries.iter().any(|entry| entry.facts.size > 0);
            self.root_object(content);
            return;
        }
        let described = self.path_of(self.prefix).and_then(|path| {
            history
                .entries
                .iter()
                .map(|entry| {
                    let listed = Listed {
                        entry,
                        order: history.order,
                    };
                    version_descriptor(self.identity, path.clone(), &listed)
                })
                .collect::<Result<Vec<_>, _>>()
        });
        // Whole or not at all, as any other key's history (`history_child`).
        match described {
            Ok(records) => self.own.extend(records),
            Err(failure) => self.failures.push(failure),
        }
    }

    pub(super) fn history_child(
        &mut self,
        history: &KeyHistory<S3ListedVersion>,
    ) -> Result<(), EntryOperationFailure> {
        let key = history.key.as_str();
        let name = key.get(self.prefix.len()..).unwrap_or_default();
        let Some(path) = self.child_path(key, name)? else {
            return Ok(());
        };
        for entry in &history.entries {
            let listed = Listed {
                entry,
                order: history.order,
            };
            let descriptor = version_descriptor(self.identity, path.clone(), &listed)?;
            self.entries.push(descriptor);
        }
        Ok(())
    }
}

/// One entry of a key's history, and where the history's order came from.
#[derive(Clone, Copy)]
struct Listed<'a> {
    entry: &'a S3ListedVersion,
    order: HistoryOrder,
}

fn version_descriptor(
    identity: &BackendIdentity,
    path: StoragePath,
    listed: &Listed<'_>,
) -> Result<SourceDescriptor, EntryOperationFailure> {
    let Listed { entry, order } = *listed;
    let facts = &entry.facts;
    let version = EntryVersion::from_listing(
        &facts.version_id,
        facts.is_latest,
        facts.delete_marker,
        order,
    )
    .map_err(|_| child_failure(&path, FailureClass::Protocol, "invalid S3 version id"))?;
    let mut descriptor = if facts.delete_marker {
        marker_descriptor(identity, path, &facts.version_id, entry.last_modified)?
    } else {
        let object = object_facts(
            facts.size,
            &facts.etag,
            Some(&facts.version_id),
            entry.last_modified,
        );
        file_descriptor(identity, path, &object)?
    };
    // A delete marker selects its own id, so a read that ever reached it would fail (405) rather
    // than read the current object; it is listing-only and never read.
    descriptor.version = version
        .source_version()
        .unwrap_or_else(|| SourceVersion::Id(facts.version_id.clone()));
    descriptor.listing = ListingFacts {
        listing_only: facts.delete_marker,
        version: Some(Box::new(version)),
    };
    Ok(descriptor)
}

/// A delete marker: a file-kind entry without size or content, identified by its version.
fn marker_descriptor(
    identity: &BackendIdentity,
    path: StoragePath,
    version_id: &str,
    last_modified: Option<StorageTimestamp>,
) -> Result<SourceDescriptor, EntryOperationFailure> {
    let (strength, bytes) = if is_real_version_id(version_id) {
        (
            IdentityStrength::VersionScoped,
            version_id.as_bytes().to_vec(),
        )
    } else {
        // Path-scoped identities include the path: every key's `"null"` marker is its own.
        (
            IdentityStrength::PathScoped,
            path_scoped_bytes(&path, NULL_MARKER_IDENTITY),
        )
    };
    let source_identity = SourceIdentity::new(identity.clone(), strength, bytes)
        .map_err(|_| child_failure(&path, FailureClass::Protocol, "invalid S3 marker identity"))?;
    let mut descriptor = SourceDescriptor::new(path, EntryKind::File, None, source_identity);
    if let Some(modified) = last_modified {
        descriptor = descriptor.with_inline_timestamps(TimestampMetadata {
            accessed: None,
            modified: Some(modified),
            created: None,
        });
    }
    Ok(descriptor)
}
