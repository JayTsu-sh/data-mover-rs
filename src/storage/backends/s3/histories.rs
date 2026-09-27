//! The listing of a versioned S3 directory (P7 §5.3, §10, Q7 / Q9): `ListObjectVersions` with
//! delimiter `/` under `<path>/`, every page gathered before any child is built — on a store that
//! does not keep keys in order a key's records can be on any page, so only a directory listed to
//! its end has whole histories (at most [`ListingLimits::max_entries`] records, 5 million in
//! production; past it the listing fails `Capacity`).
//!
//! **One child per key**, carrying its whole history ([`ObjectHistory`], oldest first, the order
//! the server listed it in unless that failed its checks, [`super::history`]): every version and
//! delete marker, also of a key deleted now. The entry-level facts are the current object's — the
//! same identity (`PathScoped` path and `ETag`), size, `ETag` and time `ListObjectsV2` gives it —
//! or, for a key whose latest record is a delete marker, no size, the marker's time and an identity
//! of its own. A key with a record that cannot be described (a malformed version id) is that key's
//! failure, none of its records listed. The directory's own key `<path>/` (P7 F5) is one child too.
//!
//! **Children are built a page at a time** (`page_hint` keys each) from the gathered records, so
//! what is held beyond the raw listing is one page of descriptors. Segmenting a long history
//! (Q8) is the traversal's, not the listing's: a key is one child whatever its length.

use std::collections::HashSet;

use super::history::{KeyHistory, oldest_first, warn_inferred};
use super::listing::{
    Block, Child, child_failure, directory_prefix, file_descriptor, merge, object_facts, place,
    refuse_absent,
};
use super::paging::{ListingLimits, PageRetry, Paging};
use super::source::{object_identity, path_scoped_bytes, role_failure};
use super::{
    S3ListedVersion, S3ObjectFacts, S3Protocol, S3RejectedEntry, S3VersionFacts, S3VersionMarker,
    is_real_version_id,
};
use crate::model::{
    BackendIdentity, EntryKind, EntryOperationFailure, FailureClass, IdentityStrength,
    ObjectHistory, Operation, RecordKind, SourceIdentity, StoragePath, StorageTimestamp,
    TimestampMetadata, VersionRecord,
};
use crate::storage::listing::{ListingClosed, cancelled};
use crate::storage::{ListRequest, PageSink, SourceDescriptor, StorageRoleFailure};

/// Identity bytes of a delete marker that names no real version (a suspended bucket's `"null"`).
const NULL_MARKER_IDENTITY: &[u8] = b"\0s3-null-delete-marker";
/// What follows the path in the identity of a key deleted now, before its latest marker's id.
const DELETED_KEY_IDENTITY: &[u8] = b"\0s3-deleted-key\0";

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

/// Produces the listing `request` asks for through `sink`: the directory's records gathered whole,
/// then its children built and handed over a page at a time. Cancellation, or the consumer
/// dropping the stream, abandons it at once: nothing stays open between S3 requests.
pub(super) async fn produce_histories<P: S3Protocol + ?Sized>(
    protocol: &P,
    identity: &BackendIdentity,
    limits: ListingLimits,
    request: &ListRequest,
    sink: &PageSink,
) {
    let directory = &request.path;
    let prefix = match directory_prefix(directory) {
        Ok(prefix) => prefix,
        Err(failure) => return sink.fail(failure).await,
    };
    let gathered = tokio::select! {
        biased;
        () = request.cancel.cancelled() => return sink.fail(cancelled(directory)).await,
        () = sink.closed() => return,
        gathered = gather(protocol, directory, &prefix, limits) => gathered,
    };
    match gathered {
        Ok(pages) => {
            let _ = deliver(identity, &prefix, pages, request, sink).await;
        }
        Err(failure) => sink.fail(failure).await,
    }
}

/// Every page of the directory, or its failure: `NotFound` when nothing at all lies under it.
async fn gather<P: S3Protocol + ?Sized>(
    protocol: &P,
    directory: &StoragePath,
    prefix: &str,
    limits: ListingLimits,
) -> Result<VersionPages, StorageRoleFailure> {
    let pages = page_versions(protocol, directory, prefix, limits).await?;
    let absent = pages.versions.is_empty()
        && pages.markers.is_empty()
        && pages.prefixes.is_empty()
        && pages.rejected.is_empty();
    refuse_absent(directory, absent)?;
    Ok(pages)
}

/// Builds the gathered directory's children, `page_hint` keys at a time, and hands each page over;
/// the first carries the directory's own object and the failures.
async fn deliver(
    identity: &BackendIdentity,
    prefix: &str,
    pages: VersionPages,
    request: &ListRequest,
    sink: &PageSink,
) -> Result<(), ListingClosed> {
    let directory = &request.path;
    let VersionPages {
        mut versions,
        mut markers,
        prefixes,
        rejected,
    } = pages;
    // A key with an entry that could not be decoded has no whole history: it is that key's
    // failure, and none of its entries is listed.
    let broken: HashSet<String> = rejected.iter().filter_map(|r| r.key.clone()).collect();
    versions.retain(|entry: &S3ListedVersion| !broken.contains(&entry.key));
    markers.retain(|entry: &S3ListedVersion| !broken.contains(&entry.key));
    let placed_versions = place(prefix, versions, |entry| &entry.key);
    let placed_markers = place(prefix, markers, |entry| &entry.key);
    let mut block = Block::new(identity, directory, prefix);
    block.outside(&placed_versions.outside);
    block.outside(&placed_markers.outside);
    // The directory's own key has one history, whatever it holds (P7 F5, D5).
    let own = oldest_first(placed_versions.own, placed_markers.own);
    warn_inferred(directory.as_str(), &own);
    for history in own {
        block.own_history(history);
    }
    let histories = oldest_first(placed_versions.children, placed_markers.children);
    warn_inferred(directory.as_str(), &histories);
    let mut rolled = placed_versions.rolled;
    rolled.extend(placed_markers.rolled);
    block.rejected(rejected, &mut rolled, &mut HashSet::new());
    let mut children =
        merge(histories.into_iter().map(Child::History), prefixes, rolled).into_iter();
    let hint = request.page_hint.get();
    loop {
        block.build(children.by_ref().take(hint).collect());
        sink.send_split(block.finish(), request).await?;
        if children.len() == 0 {
            return Ok(());
        }
        block = Block::new(identity, directory, prefix);
    }
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
    /// The directory's own key `<path>/` (P7 F5, D5): one child with its whole history — every
    /// version, zero-byte ones included, and every delete marker. At the storage root it has no
    /// path: ignored when no version holds content, a failure when one does.
    pub(super) fn own_history(&mut self, history: KeyHistory<S3ListedVersion>) {
        if self.directory.as_str().is_empty() {
            let content = history
                .entries
                .iter()
                .any(|entry| !entry.facts.delete_marker && entry.facts.size > 0);
            self.root_object(content);
            return;
        }
        let described = self
            .path_of(self.prefix)
            .and_then(|path| history_descriptor(self.identity, path, history));
        match described {
            Ok(descriptor) => self.own.push(descriptor),
            Err(failure) => self.failures.push(failure),
        }
    }

    /// One key with its whole history.
    pub(super) fn history_child(
        &mut self,
        history: KeyHistory<S3ListedVersion>,
    ) -> Result<(), EntryOperationFailure> {
        let key = history.key.as_str();
        let name = key.get(self.prefix.len()..).unwrap_or_default();
        let Some(path) = self.child_path(key, name)? else {
            return Ok(());
        };
        let descriptor = history_descriptor(self.identity, path, history)?;
        self.entries.push(descriptor);
        Ok(())
    }
}

/// The key at `path` with its history: the current object's facts, or those of a key deleted now.
/// Whole or not at all: a record that cannot be described fails the key.
fn history_descriptor(
    identity: &BackendIdentity,
    path: StoragePath,
    history: KeyHistory<S3ListedVersion>,
) -> Result<SourceDescriptor, EntryOperationFailure> {
    let records = history
        .entries
        .into_iter()
        .map(|entry| record(identity, &path, entry))
        .collect::<Result<Vec<_>, _>>()?;
    let whole = ObjectHistory::whole(records, history.order)
        .map_err(|_| child_failure(&path, FailureClass::Protocol, "an S3 key without records"))?;
    let mut descriptor = match whole.latest().map(VersionRecord::kind) {
        Some(RecordKind::Object {
            size,
            etag,
            modified,
        }) => file_descriptor(identity, path, &object_facts(*size, etag, None, *modified))?,
        Some(RecordKind::DeleteMarker { modified }) => {
            let marker = whole.latest().and_then(VersionRecord::version_id);
            deleted_descriptor(identity, path, marker, *modified)?
        }
        // `whole` has records, and a non-exhaustive kind added later is no current object.
        _ => {
            return Err(child_failure(
                &path,
                FailureClass::Protocol,
                "an S3 key without records",
            ));
        }
    };
    descriptor.listing.history = Some(Box::new(whole));
    Ok(descriptor)
}

/// One listed version or delete marker, with the identity that names exactly it: the one a
/// `describe_version` of it gives (`VersionScoped(id)`, `PathScoped(path, ETag)` for `"null"`).
fn record(
    backend: &BackendIdentity,
    path: &StoragePath,
    entry: S3ListedVersion,
) -> Result<VersionRecord, EntryOperationFailure> {
    let S3ListedVersion {
        facts,
        last_modified,
        ..
    } = entry;
    let latest = facts.is_latest;
    let (kind, identity, version_id) = if facts.delete_marker {
        let identity = marker_identity(backend, path, &facts.version_id)?;
        let kind = RecordKind::DeleteMarker {
            modified: last_modified,
        };
        (kind, identity, facts.version_id)
    } else {
        object_record(backend, path, facts, last_modified)?
    };
    VersionRecord::from_listing(version_id, latest, kind, identity)
        .map_err(|_| child_failure(path, FailureClass::Protocol, "invalid S3 version id"))
}

/// A listed version: what it is, its identity, and its id as listed.
fn object_record(
    backend: &BackendIdentity,
    path: &StoragePath,
    listed: S3VersionFacts,
    last_modified: Option<StorageTimestamp>,
) -> Result<(RecordKind, SourceIdentity, String), EntryOperationFailure> {
    let facts = S3ObjectFacts {
        size: listed.size,
        etag: listed.etag,
        version_id: Some(listed.version_id),
        last_modified,
    };
    let identity = object_identity(backend, path, &facts).map_err(|_| invalid(path))?;
    let S3ObjectFacts {
        size,
        etag,
        version_id,
        ..
    } = facts;
    let kind = RecordKind::Object {
        size,
        etag,
        modified: last_modified,
    };
    Ok((kind, identity, version_id.unwrap_or_default()))
}

fn invalid(path: &StoragePath) -> EntryOperationFailure {
    child_failure(path, FailureClass::Protocol, "invalid S3 object identity")
}

/// A delete marker, identified by its version; one that names no real version (a suspended
/// bucket's `"null"`) by its path.
fn marker_identity(
    identity: &BackendIdentity,
    path: &StoragePath,
    version_id: &str,
) -> Result<SourceIdentity, EntryOperationFailure> {
    let (strength, bytes) = if is_real_version_id(version_id) {
        (
            IdentityStrength::VersionScoped,
            version_id.as_bytes().to_vec(),
        )
    } else {
        // Path-scoped identities include the path: every key's `"null"` marker is its own.
        (
            IdentityStrength::PathScoped,
            path_scoped_bytes(path, NULL_MARKER_IDENTITY),
        )
    };
    SourceIdentity::new(identity.clone(), strength, bytes)
        .map_err(|_| child_failure(path, FailureClass::Protocol, "invalid S3 marker identity"))
}

/// A key whose latest record is the delete marker `marker` (`None`: `"null"`): no size, the
/// marker's time, and an identity of its own — the path, a tag no `ETag` can spell, and the
/// marker's id — so a key deleted now never shares an identity key with a live object. Nothing
/// behind it to observe (its records still are).
fn deleted_descriptor(
    identity: &BackendIdentity,
    path: StoragePath,
    marker: Option<&str>,
    modified: Option<StorageTimestamp>,
) -> Result<SourceDescriptor, EntryOperationFailure> {
    let mut tail = DELETED_KEY_IDENTITY.to_vec();
    tail.extend_from_slice(marker.unwrap_or("null").as_bytes());
    let source_identity = SourceIdentity::new(
        identity.clone(),
        IdentityStrength::PathScoped,
        path_scoped_bytes(&path, &tail),
    )
    .map_err(|_| invalid(&path))?;
    let mut descriptor = SourceDescriptor::new(path, EntryKind::File, None, source_identity);
    if let Some(modified) = modified {
        descriptor = descriptor.with_inline_timestamps(TimestampMetadata {
            accessed: None,
            modified: Some(modified),
            created: None,
        });
    }
    descriptor.listing.listing_only = true;
    Ok(descriptor)
}
