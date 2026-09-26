//! One S3 "directory" — the objects and common prefixes under `<path>/` — listed with delimiter
//! `/` for the namespace role (ADR-0006 C22), current objects only or every version.
//!
//! - Every page of the directory is collected before its children are built, so a key whose
//!   versions straddle a page boundary is whole, and a common prefix reported on two pages is one
//!   child. Continuation tokens and markers are opaque and passed back as the server sent them.
//! - Children come out in S3 key order, a prefix compared with its trailing `/`: the object `a`
//!   before the prefix `a/`.
//! - A key below a deeper `/` that a store reported anyway (one that ignores the delimiter) is
//!   rolled up into its first-level prefix.
//! - The directory's own marker object (`<path>/`, zero bytes) is not a child. A non-empty one,
//!   and a prefix with an empty segment (`a//`, a leading `/`), cannot be spelled as a path of
//!   this tree and are reported as per-child failures, never silently merged or dropped.
//! - Transfer artifacts (`.data-mover-*` in any segment) are hidden in both modes.

use std::collections::HashSet;

use bytes::Bytes;

use super::history::{KeyHistory, oldest_first, warn_inferred};
use super::paging::{ListingLimits, Paging};
use super::source::{object_identity, path_scoped_bytes, role_failure};
use super::{
    S3ListedObject, S3ListedVersion, S3ObjectFacts, S3Protocol, S3ProtocolFailure, S3RejectedEntry,
    S3VersionMarker, is_real_version_id,
};
use crate::model::{
    BackendIdentity, EntryKind, EntryOperationFailure, EntryVersion, FailureClass, HistoryOrder,
    IdentityStrength, Operation, SourceIdentity, SourceVersion, StoragePath, StorageTimestamp,
    TimestampMetadata, Transience,
};
use crate::storage::artifacts::is_artifact_path;
use crate::storage::throttle::retry_throttled;
use crate::storage::{
    ChildFailure, ListingFacts, ListingPage, SourceDescriptor, StorageRoleFailure,
};

/// Identity bytes of a delete marker that names no real version (a suspended bucket's `"null"`).
const NULL_MARKER_IDENTITY: &[u8] = b"\0s3-null-delete-marker";

/// One child of the directory before it becomes descriptors, keyed by its S3 key.
enum Child {
    Prefix(String),
    Object(S3ListedObject),
    History(KeyHistory<S3ListedVersion>),
}

impl Child {
    fn key(&self) -> &str {
        match self {
            Self::Prefix(key) => key,
            Self::History(history) => &history.key,
            Self::Object(object) => &object.key,
        }
    }
}

/// Lists the current objects and subdirectories of `directory`.
pub(super) async fn list_current<P: S3Protocol + ?Sized>(
    protocol: &P,
    identity: &BackendIdentity,
    directory: &StoragePath,
    limits: ListingLimits,
) -> Result<ListingPage, StorageRoleFailure> {
    let prefix = directory_prefix(directory)?;
    let mut paging = Paging::new(limits);
    let mut throttle = limits.throttle.start();
    let mut token: Option<String> = None;
    let (mut objects, mut prefixes, mut rejected) = (Vec::new(), Vec::new(), Vec::new());
    loop {
        let page = retry_throttled(&mut throttle, is_throttled, || {
            protocol.list_objects_page(&prefix, token.as_deref())
        })
        .await
        .map_err(|e| role_failure(directory, Operation::Traverse, past_budget(e)))?;
        let listed = page.objects.len() + page.prefixes.len() + page.rejected.len();
        objects.extend(page.objects);
        prefixes.extend(page.prefixes);
        rejected.extend(page.rejected);
        token = paging.advance(directory, listed, page.next)?;
        if token.is_none() {
            break;
        }
    }
    let absent = objects.is_empty() && prefixes.is_empty() && rejected.is_empty();
    refuse_absent(directory, absent)?;
    let mut keys = HashSet::new();
    // A server that repeats an object across a page boundary lists it once, as first listed.
    objects.retain(|object: &S3ListedObject| keys.insert(object.key.clone()));
    let placed = place(&prefix, objects, |object| &object.key);
    let mut block = Block::new(identity, directory, &prefix);
    block.outside(&placed.outside);
    block.own_marker(placed.own.iter().any(|object| object.size > 0));
    let mut rolled = placed.rolled;
    block.rejected(rejected, &mut rolled);
    let children = placed.children.into_iter().map(Child::Object);
    block.build(merge(children, prefixes, rolled));
    Ok(block.finish())
}

/// Every page of a versions listing, gathered.
#[derive(Default)]
struct VersionPages {
    versions: Vec<S3ListedVersion>,
    markers: Vec<S3ListedVersion>,
    prefixes: Vec<String>,
    rejected: Vec<S3RejectedEntry>,
}

/// Pages through every version of `prefix`, backing off while throttled.
async fn page_versions<P: S3Protocol + ?Sized>(
    protocol: &P,
    directory: &StoragePath,
    prefix: &str,
    limits: ListingLimits,
) -> Result<VersionPages, StorageRoleFailure> {
    let mut paging = Paging::new(limits);
    let mut throttle = limits.throttle.start();
    let mut marker: Option<S3VersionMarker> = None;
    let mut all = VersionPages::default();
    // Positions are per page; a later page's follow the earlier pages' (document order of the
    // whole listing).
    let mut listed_before = 0_u64;
    loop {
        let page = retry_throttled(&mut throttle, is_throttled, || {
            protocol.list_versions_page(prefix, marker.as_ref())
        })
        .await
        .map_err(|e| role_failure(directory, Operation::Traverse, past_budget(e)))?;
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
    block.own_marker(placed_versions.own.iter().any(|entry| entry.facts.size > 0));
    let histories = oldest_first(placed_versions.children, placed_markers.children);
    warn_inferred(directory.as_str(), &histories);
    let children = histories.into_iter().map(Child::History);
    let mut rolled = placed_versions.rolled;
    rolled.extend(placed_markers.rolled);
    block.rejected(rejected, &mut rolled);
    block.build(merge(children, prefixes, rolled));
    Ok(block.finish())
}

/// Whether the service asked this listing to slow down; it backs off rather than fails.
fn is_throttled(failure: &S3ProtocolFailure) -> bool {
    matches!(
        failure,
        S3ProtocolFailure::Entry {
            class: FailureClass::Throttled,
            ..
        } | S3ProtocolFailure::Session {
            class: FailureClass::Throttled,
            ..
        }
    )
}

/// A listing still throttled once its backoff budget is spent fails that listing alone: the
/// service is busy, the session is fine, and the traversal goes on (P7 §8, Q11).
fn past_budget(failure: S3ProtocolFailure) -> S3ProtocolFailure {
    match failure {
        S3ProtocolFailure::Session {
            class: FailureClass::Throttled,
            diagnostic,
            ..
        } => S3ProtocolFailure::entry(
            FailureClass::Throttled,
            Transience::Transient,
            format!("{diagnostic}; still throttled after the listing's backoff budget"),
        ),
        other => other,
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

/// S3 has no directories: a prefix under which nothing at all is stored does not exist. The
/// storage root is always there. Reporting an absent directory as an empty listing would let a
/// mistyped traversal root read as an exhaustively empty source.
fn refuse_absent(directory: &StoragePath, nothing_listed: bool) -> Result<(), StorageRoleFailure> {
    if nothing_listed && !directory.as_str().is_empty() {
        Err(StorageRoleFailure::Entry(child_failure(
            directory,
            FailureClass::NotFound,
            "nothing is stored under this S3 prefix",
        )))
    } else {
        Ok(())
    }
}

/// Whether a path segment cannot name a child of this tree: empty (`a//`, a leading `/`), or `.` /
/// `..`, which a destination's path handling would fold into another path (`a/./b` is `a/b`).
fn unspellable(segment: &str) -> bool {
    matches!(segment, "" | "." | "..")
}

/// The listing prefix of `directory`: empty for the storage root, otherwise the path and `/`. A
/// path that no listing of this tree produces (a trailing or leading `/`, an empty, `.` or `..`
/// segment) is refused: listing it would re-list a sibling or loop on `a//`.
pub(super) fn directory_prefix(directory: &StoragePath) -> Result<String, StorageRoleFailure> {
    let path = directory.as_str();
    if path.is_empty() {
        return Ok(String::new());
    }
    if path.split('/').any(unspellable) {
        return Err(StorageRoleFailure::Entry(child_failure(
            directory,
            FailureClass::InvalidInput,
            "an S3 directory path has an empty, '.' or '..' segment",
        )));
    }
    Ok(format!("{path}/"))
}

/// Listed entries sorted by where their key puts them relative to the directory.
struct Placed<T> {
    /// Directly in the directory.
    children: Vec<T>,
    /// The directory's own marker object, `<path>/`.
    own: Vec<T>,
    /// First-level prefixes of keys a store reported below a deeper `/`.
    rolled: Vec<String>,
    /// Keys that are not under the directory at all: the server ignored the prefix.
    outside: Vec<String>,
}

fn place<T>(prefix: &str, entries: Vec<T>, key: impl Fn(&T) -> &str) -> Placed<T> {
    let mut placed = Placed {
        children: Vec::with_capacity(entries.len()),
        own: Vec::new(),
        rolled: Vec::new(),
        outside: Vec::new(),
    };
    for entry in entries {
        let Some(rest) = key(&entry).strip_prefix(prefix) else {
            placed.outside.push(key(&entry).to_string());
            continue;
        };
        if rest.is_empty() {
            placed.own.push(entry);
        } else if let Some(end) = rest.find('/') {
            placed.rolled.push(format!("{prefix}{}", &rest[..=end]));
        } else {
            placed.children.push(entry);
        }
    }
    placed
}

/// Every child in S3 key order: entries and prefixes (the listing's and the rolled-up ones, each
/// once).
fn merge(
    entries: impl Iterator<Item = Child>,
    mut prefixes: Vec<String>,
    rolled: Vec<String>,
) -> Vec<Child> {
    prefixes.extend(rolled);
    prefixes.sort_unstable();
    prefixes.dedup();
    let mut children: Vec<Child> = entries
        .chain(prefixes.into_iter().map(Child::Prefix))
        .collect();
    // Keys are unique (one per object or history, prefixes deduplicated), so unstable is exact.
    children.sort_unstable_by(|left, right| left.key().cmp(right.key()));
    children
}

/// One directory's described children and the children it could not describe.
struct Block<'a> {
    identity: &'a BackendIdentity,
    /// The directory listed, which a failure without a path of its own is reported against.
    directory: &'a StoragePath,
    prefix: &'a str,
    entries: Vec<SourceDescriptor>,
    failures: Vec<EntryOperationFailure>,
    /// Children no path of this tree can name, with their raw keys.
    unnamed: Vec<ChildFailure>,
    /// Transfer artifacts hidden: one per key or prefix, whatever its versions.
    hidden: u32,
}

impl<'a> Block<'a> {
    const fn new(
        identity: &'a BackendIdentity,
        directory: &'a StoragePath,
        prefix: &'a str,
    ) -> Self {
        Self {
            identity,
            directory,
            prefix,
            entries: Vec::new(),
            failures: Vec::new(),
            unnamed: Vec::new(),
            hidden: 0,
        }
    }

    /// A child that no path of this tree can name: a failure at the directory, with its raw key.
    fn unnamed(&mut self, raw_key: &str, reason: &str) {
        self.unnamed.push(ChildFailure::unnamed(
            self.identity,
            self.directory,
            Bytes::copy_from_slice(raw_key.as_bytes()),
            FailureClass::Protocol,
            reason,
        ));
    }

    /// Keys the server listed although they are not under this directory.
    fn outside(&mut self, keys: &[String]) {
        for key in keys {
            self.unnamed(
                key,
                "the S3 listing returned a key outside the listed directory",
            );
        }
    }

    /// Entries the listing could not decode, each its own child's failure (once per key). One
    /// below a deeper `/` (a store ignoring the delimiter) only rolls up its first-level prefix:
    /// the listing of that subdirectory reports it.
    fn rejected(&mut self, rejected: Vec<S3RejectedEntry>, rolled: &mut Vec<String>) {
        let mut reported = HashSet::new();
        for rejection in rejected {
            let Some(key) = rejection.key.as_deref() else {
                self.unnamed(&rejection.raw_key, rejection.reason);
                continue;
            };
            let Some(rest) = key
                .strip_prefix(self.prefix)
                .filter(|rest| !rest.is_empty())
            else {
                self.unnamed(key, rejection.reason);
                continue;
            };
            if let Some(end) = rest.find('/') {
                rolled.push(format!("{}{}", self.prefix, &rest[..=end]));
            } else if reported.insert(key.to_string()) {
                match self.child_path(key, rest) {
                    Ok(Some(path)) => self.failures.push(child_failure(
                        &path,
                        FailureClass::Protocol,
                        rejection.reason,
                    )),
                    Ok(None) => {}
                    Err(failure) => self.failures.push(failure),
                }
            }
        }
    }

    /// A marker object `<path>/` with content cannot be a child of `<path>`: no path spells it.
    fn own_marker(&mut self, has_content: bool) {
        if has_content {
            self.failures.push(child_failure(
                self.directory,
                FailureClass::Unsupported,
                "an S3 object key ends in '/'; it is not listed",
            ));
        }
    }

    fn build(&mut self, children: Vec<Child>) {
        self.entries.reserve(children.len());
        for child in children {
            let result = match child {
                Child::Prefix(key) => self.prefix_child(&key),
                Child::Object(object) => self.object_child(&object),
                Child::History(history) => self.history_child(&history),
            };
            if let Err(failure) = result {
                self.failures.push(failure);
            }
        }
    }

    fn finish(self) -> ListingPage {
        let mut page =
            ListingPage::from_descriptors(self.directory, self.entries, self.failures, self.hidden);
        for failure in self.unnamed {
            page.push_failure(failure);
        }
        page
    }

    /// Whether the child `key` named `name` is listed: `Ok(None)` for a transfer artifact, a
    /// failure (reported at `key`) for a name no path of this tree can spell.
    fn child_path(
        &mut self,
        key: &str,
        name: &str,
    ) -> Result<Option<StoragePath>, EntryOperationFailure> {
        let path = self.path_of(key)?;
        if is_artifact_path(path.as_str()) {
            self.hidden = self.hidden.saturating_add(1);
            return Ok(None);
        }
        if unspellable(name) {
            return Err(child_failure(
                &path,
                FailureClass::Unsupported,
                "an S3 key has an empty, '.' or '..' path segment; it is not listed",
            ));
        }
        Ok(Some(path))
    }

    /// A common prefix `<dir>/<name>/`. A failure names the prefix as the server spelled it
    /// (`a//`, `a/../`), so it is never mistaken for another directory.
    fn prefix_child(&mut self, key: &str) -> Result<(), EntryOperationFailure> {
        let name = key
            .strip_prefix(self.prefix)
            .and_then(|rest| rest.strip_suffix('/'));
        let (Some(name), Some(spelled)) = (name, key.strip_suffix('/')) else {
            return Err(child_failure(
                &self.path_of(key)?,
                FailureClass::Protocol,
                "an S3 common prefix is not a subdirectory of the listed one",
            ));
        };
        if is_artifact_path(spelled) {
            self.hidden = self.hidden.saturating_add(1);
            return Ok(());
        }
        if name.contains('/') {
            // Well formed, but deeper than one level: the server did not group by the delimiter.
            return Err(child_failure(
                &self.path_of(key)?,
                FailureClass::Protocol,
                "the S3 listing reported a common prefix more than one level below the directory",
            ));
        }
        if unspellable(name) {
            return Err(child_failure(
                &self.path_of(key)?,
                FailureClass::Unsupported,
                "an S3 prefix has an empty, '.' or '..' path segment; its subtree is not listed",
            ));
        }
        let descriptor = directory_descriptor(self.identity, self.path_of(spelled)?)?;
        self.entries.push(descriptor);
        Ok(())
    }

    fn object_child(&mut self, object: &S3ListedObject) -> Result<(), EntryOperationFailure> {
        let name = object.key.get(self.prefix.len()..).unwrap_or_default();
        let Some(path) = self.child_path(&object.key, name)? else {
            return Ok(());
        };
        let facts = object_facts(object.size, &object.etag, None, object.last_modified);
        let descriptor = file_descriptor(self.identity, path, &facts)?;
        self.entries.push(descriptor);
        Ok(())
    }

    fn history_child(
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

    fn path_of(&self, key: &str) -> Result<StoragePath, EntryOperationFailure> {
        StoragePath::new(key).map_err(|_| {
            child_failure(
                self.directory,
                FailureClass::Unsupported,
                "an S3 key cannot be spelled as a storage path",
            )
        })
    }
}

fn child_failure(
    path: &StoragePath,
    class: FailureClass,
    diagnostic: &str,
) -> EntryOperationFailure {
    EntryOperationFailure::new(
        path.clone(),
        Operation::Traverse,
        class,
        Transience::Permanent,
        diagnostic,
    )
    .unwrap_or_else(|_| unreachable!("static S3 listing diagnostics are valid"))
}

pub(super) fn object_facts(
    size: u64,
    etag: &str,
    version_id: Option<&str>,
    last_modified: Option<StorageTimestamp>,
) -> S3ObjectFacts {
    S3ObjectFacts {
        size,
        etag: etag.to_string(),
        version_id: version_id.map(str::to_string),
        last_modified,
    }
}

/// A subdirectory: nothing behind it to observe, and no time of its own.
pub(super) fn directory_descriptor(
    identity: &BackendIdentity,
    path: StoragePath,
) -> Result<SourceDescriptor, EntryOperationFailure> {
    let bytes = if path.as_str().is_empty() {
        b"/".to_vec()
    } else {
        path.as_str().as_bytes().to_vec()
    };
    let source_identity =
        SourceIdentity::new(identity.clone(), IdentityStrength::PathScoped, bytes).map_err(
            |_| child_failure(&path, FailureClass::Protocol, "invalid S3 prefix identity"),
        )?;
    let mut descriptor = SourceDescriptor::new(path, EntryKind::Directory, None, source_identity);
    descriptor.listing.listing_only = true;
    Ok(descriptor)
}

/// A current object, or one stored version when `facts` names it. A stored version gets the
/// identity `ReadSource::describe_version` gives it (`VersionScoped(id)`, or `PathScoped(path,
/// ETag)` for `"null"`), so a bound observation recognises it. A current object from
/// `ListObjectsV2` has no version id to carry, so it is `PathScoped(path, ETag)` even in a
/// versioned bucket, where a describe pins `VersionScoped(id)`.
pub(super) fn file_descriptor(
    identity: &BackendIdentity,
    path: StoragePath,
    facts: &S3ObjectFacts,
) -> Result<SourceDescriptor, EntryOperationFailure> {
    let source_identity = object_identity(identity, &path, facts)
        .map_err(|_| child_failure(&path, FailureClass::Protocol, "invalid S3 object identity"))?;
    let mut descriptor =
        SourceDescriptor::new(path, EntryKind::File, Some(facts.size), source_identity);
    descriptor.content_version =
        (!facts.etag.is_empty()).then(|| Bytes::copy_from_slice(facts.etag.as_bytes()));
    if let Some(modified) = facts.last_modified {
        descriptor = descriptor.with_inline_timestamps(TimestampMetadata {
            accessed: None,
            modified: Some(modified),
            created: None,
        });
    }
    Ok(descriptor)
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
