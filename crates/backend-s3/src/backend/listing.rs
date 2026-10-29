//! One S3 "directory" — the objects and common prefixes under `<path>/` — as the namespace role
//! lists it (ADR-0006 C22), delimiter `/`: how a page of the listing becomes children. The listing
//! of every version, which is still read whole, is [`super::histories`].
//!
//! - Current objects stream (P7 C6, [`super::current`]): each server page is described as it
//!   arrives, by the same [`Block`] as here.
//! - Every version is gathered from every page of the directory before its children are built
//!   (P7 §5.3, Q9): on a store that does not keep keys in order a key's versions can be on any
//!   page, so only a directory listed to its end has whole histories. A common prefix reported on
//!   two pages is one child. Continuation markers are opaque and passed back as the server sent
//!   them.
//! - Within what is described together, children come out in S3 key order, a prefix compared with
//!   its trailing `/`: the object `a` before the prefix `a/`.
//! - A key below a deeper `/` that a store reported anyway (one that ignores the delimiter) is
//!   rolled up into its first-level prefix.
//! - **The directory's own object** (the key `<path>/` itself, P7 F5): every measured object
//!   store keeps content under such a key, and only the listing of `<path>` sees it (the parent's
//!   delimiter listing rolls it into the common prefix). It is handed over as a file child with
//!   the empty name, so its path is exactly `<path>/` — whatever its size: a zero-byte folder
//!   marker is a zero-byte file entry — and, listing every version, with its whole history,
//!   delete markers included. It is neither the directory `<path>` nor a file `<path>`: its
//!   identity is path-scoped to `<path>/` (or version-scoped). The storage root's own object (the
//!   URL prefix's key) has no path in this tree: a zero-byte one, or a history of zero-byte
//!   versions and markers, is a folder marker and ignored; one with content is a per-child
//!   `Unsupported` failure at the root, never dropped silently.
//! - A prefix with an empty segment (`a//`, a leading `/`) cannot be spelled as a path of this
//!   tree and is reported as a per-child failure, never silently merged or dropped.
//! - Transfer artifacts (`.data-mover-*` in any segment) are hidden in both modes.

use std::collections::HashSet;

use bytes::Bytes;

use super::history::KeyHistory;
use super::source::object_identity;
use super::{S3ListedObject, S3ListedVersion, S3ObjectFacts, S3RejectedEntry};
use crate::model::{
    BackendIdentity, EntryKind, EntryOperationFailure, FailureClass, IdentityStrength, Operation,
    SourceIdentity, StoragePath, StorageTimestamp, TimestampMetadata, Transience,
};
use crate::storage::artifacts::is_artifact_path;
use crate::storage::{ChildFailure, ListingPage, SourceDescriptor, StorageRoleFailure};

/// One child of the directory before it becomes descriptors, keyed by its S3 key.
pub(super) enum Child {
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

/// S3 has no directories: a prefix under which nothing at all is stored does not exist. The
/// storage root is always there. Reporting an absent directory as an empty listing would let a
/// mistyped traversal root read as an exhaustively empty source.
pub(super) fn refuse_absent(
    directory: &StoragePath,
    nothing_listed: bool,
) -> Result<(), StorageRoleFailure> {
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
pub(super) struct Placed<T> {
    /// Directly in the directory.
    pub(super) children: Vec<T>,
    /// The directory's own object, the key `<path>/` itself (P7 F5).
    pub(super) own: Vec<T>,
    /// First-level prefixes of keys a store reported below a deeper `/`.
    pub(super) rolled: Vec<String>,
    /// Keys that are not under the directory at all: the server ignored the prefix.
    pub(super) outside: Vec<String>,
}

pub(super) fn place<T>(prefix: &str, entries: Vec<T>, key: impl Fn(&T) -> &str) -> Placed<T> {
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
pub(super) fn merge(
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

/// Described children of one directory — the whole directory, or one page of it — and the
/// children that could not be described.
pub(super) struct Block<'a> {
    pub(super) identity: &'a BackendIdentity,
    /// The directory listed, which a failure without a path of its own is reported against.
    pub(super) directory: &'a StoragePath,
    pub(super) prefix: &'a str,
    pub(super) entries: Vec<SourceDescriptor>,
    /// The directory's own object `<path>/`: its current object, or every record of its history.
    pub(super) own: Vec<SourceDescriptor>,
    pub(super) failures: Vec<EntryOperationFailure>,
    /// Children no path of this tree can name, with their raw keys.
    unnamed: Vec<ChildFailure>,
    /// Transfer artifacts hidden: one per key or prefix, whatever its versions.
    hidden: u32,
}

impl<'a> Block<'a> {
    pub(super) const fn new(
        identity: &'a BackendIdentity,
        directory: &'a StoragePath,
        prefix: &'a str,
    ) -> Self {
        Self {
            identity,
            directory,
            prefix,
            entries: Vec::new(),
            own: Vec::new(),
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
    pub(super) fn outside(&mut self, keys: &[String]) {
        for key in keys {
            self.unnamed(
                key,
                "the S3 listing returned a key outside the listed directory",
            );
        }
    }

    /// Entries the listing could not decode, each its own child's failure (once per key: keys
    /// already in `reported` are not reported again, and each reported key is added). One below a
    /// deeper `/` (a store ignoring the delimiter) only rolls up its first-level prefix: the
    /// listing of that subdirectory reports it.
    pub(super) fn rejected(
        &mut self,
        rejected: Vec<S3RejectedEntry>,
        rolled: &mut Vec<String>,
        reported: &mut HashSet<String>,
    ) {
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

    /// The directory's current own object `<path>/` (P7 F5): a file child with the empty name,
    /// whatever its size. At the storage root it has no path: ignored when it is a zero-byte
    /// folder marker, a failure when it holds content.
    ///
    /// An own key listed with no `ETag` and no bytes is no object: `MinIO` (RELEASE.2023-03-20)
    /// keeps listing a directory object whose newest record is a delete marker that way (its HEAD
    /// answers 404, and its parent still lists the prefix). The current listing lists current
    /// objects only, so it is not handed over.
    pub(super) fn own_object(&mut self, object: &S3ListedObject) {
        if object.etag.is_empty() && object.size == 0 {
            return;
        }
        if self.directory.as_str().is_empty() {
            self.root_object(object.size > 0);
            return;
        }
        let facts = object_facts(object.size, &object.etag, None, object.last_modified);
        let described = self
            .path_of(self.prefix)
            .and_then(|path| file_descriptor(self.identity, path, &facts));
        match described {
            Ok(descriptor) => self.own.push(descriptor),
            Err(failure) => self.failures.push(failure),
        }
    }

    /// The storage root's own object (the URL prefix's key itself): no path of this tree spells
    /// it, so content or history is reported at the root rather than dropped; a current-only
    /// zero-byte folder marker stands for nothing the root does not already.
    pub(super) fn root_object(&mut self, content_or_history: bool) {
        if content_or_history {
            self.failures.push(child_failure(
                self.directory,
                FailureClass::Unsupported,
                "the S3 object at the storage root's own key (ending in '/') holds content or \
                 history; no path of this tree names it, so it is not listed; use its parent \
                 as the storage root to include it",
            ));
        }
    }

    pub(super) fn build(&mut self, children: Vec<Child>) {
        self.entries.reserve(children.len());
        for child in children {
            let result = match child {
                Child::Prefix(key) => self.prefix_child(&key),
                Child::Object(object) => self.object_child(&object),
                Child::History(history) => self.history_child(history),
            };
            if let Err(failure) = result {
                self.failures.push(failure);
            }
        }
    }

    /// The page: the directory's own object first — its key `<path>/` sorts before every key
    /// below it, so the page stays in S3 key order — then the other children.
    pub(super) fn finish(self) -> ListingPage {
        let mut page = ListingPage::default();
        for own in self.own {
            page.push_directory_object(self.directory, own);
        }
        let rest =
            ListingPage::from_descriptors(self.directory, self.entries, self.failures, self.hidden);
        page.children.extend(rest.children);
        page.failures.extend(rest.failures);
        page.internal_hidden = rest.internal_hidden;
        for failure in self.unnamed {
            page.push_failure(failure);
        }
        page
    }

    /// Whether the child `key` named `name` is listed: `Ok(None)` for a transfer artifact, a
    /// failure (reported at `key`) for a name no path of this tree can spell.
    pub(super) fn child_path(
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

    pub(super) fn path_of(&self, key: &str) -> Result<StoragePath, EntryOperationFailure> {
        StoragePath::new(key).map_err(|_| {
            child_failure(
                self.directory,
                FailureClass::Unsupported,
                "an S3 key cannot be spelled as a storage path",
            )
        })
    }
}

pub(super) fn child_failure(
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
