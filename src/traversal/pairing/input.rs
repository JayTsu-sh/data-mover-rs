//! Validate the boundary before interpreting absence.

use std::collections::{HashMap, HashSet};

use super::{AbsenceBarrier, ChildKey, ObservedChild, PairingInputError, PairingSide};
use crate::model::{EntryKind, ObservedEntry, StoragePath};
use crate::traversal::{BlockCounts, DirectoryListed, FailureScope, ScanFailure, TraversalItem};

pub(super) struct Block<'a> {
    pub listed: &'a DirectoryListed,
    pub children: HashMap<ChildKey, Vec<ObservedChild<'a>>>,
    pub failures: HashSet<ChildKey>,
    unnamed_failure: bool,
}

struct Reader<'a, 'd> {
    directory: &'d StoragePath,
    children: HashMap<ChildKey, Vec<ObservedChild<'a>>>,
    failures: HashSet<ChildKey>,
    unnamed_failure: bool,
    listing_failure: Option<&'a ScanFailure>,
    counts: BlockCounts,
    open: Option<&'a ObservedEntry>,
}

impl<'a> Block<'a> {
    pub fn read(
        items: &'a [TraversalItem],
        directory: &StoragePath,
        side: PairingSide,
    ) -> Result<Self, PairingInputError> {
        Reader::new(directory)
            .read(items)
            .map_err(|reason| PairingInputError { side, reason })
    }

    pub fn keys(&self) -> impl Iterator<Item = &ChildKey> {
        self.children.keys().chain(self.failures.iter())
    }

    pub fn barrier(&self) -> Option<AbsenceBarrier> {
        if !self.listed.outcome.is_complete() {
            Some(AbsenceBarrier::IncompleteListing)
        } else if self.unnamed_failure {
            Some(AbsenceBarrier::UnnamedChildFailure)
        } else if self.listed.counts.filtered != 0 {
            Some(AbsenceBarrier::FilteredChildren)
        } else {
            None
        }
    }
}

impl<'a, 'd> Reader<'a, 'd> {
    fn new(directory: &'d StoragePath) -> Self {
        Self {
            directory,
            children: HashMap::new(),
            failures: HashSet::new(),
            unnamed_failure: false,
            listing_failure: None,
            counts: BlockCounts::default(),
            open: None,
        }
    }

    fn read(mut self, items: &'a [TraversalItem]) -> Result<Block<'a>, &'static str> {
        let mut boundary = None;
        for item in items {
            if matches!(item, TraversalItem::Checkpoint(_)) {
                continue;
            }
            if boundary.is_some() {
                return Err("item after directory boundary");
            }
            match item {
                TraversalItem::Entry(entry) => self.entry(entry)?,
                TraversalItem::Failure(failure) => self.failure(failure)?,
                TraversalItem::DirectoryListed(listed) => {
                    self.close(listed)?;
                    boundary = Some(listed.as_ref());
                }
                TraversalItem::Checkpoint(_) | TraversalItem::ObservationFailure(_) => {}
            }
        }
        let listed = boundary.ok_or("missing directory boundary")?;
        Ok(Block {
            listed,
            children: self.children,
            failures: self.failures,
            unnamed_failure: self.unnamed_failure,
        })
    }

    fn entry(&mut self, entry: &'a ObservedEntry) -> Result<(), &'static str> {
        if self.listing_failure.is_some() {
            return Err("entry after listing failure");
        }
        let key = entry_key(self.directory, entry)?;
        let continuation = self.open.is_some();
        validate_segment(self.open, entry)?;
        let children = self.children.entry(key).or_default();
        if continuation {
            children
                .last_mut()
                .ok_or("history without its first segment")?
                .segments
                .push(entry);
        } else {
            children.push(ObservedChild {
                segments: vec![entry],
            });
            self.counts.count_emitted(entry);
        }
        if continuation {
            let mut segment_counts = BlockCounts::default();
            segment_counts.count_emitted(entry);
            self.counts.versions = self.counts.versions.saturating_add(segment_counts.versions);
            self.counts.delete_markers = self
                .counts
                .delete_markers
                .saturating_add(segment_counts.delete_markers);
            self.counts.all_versions_bytes = self
                .counts
                .all_versions_bytes
                .saturating_add(segment_counts.all_versions_bytes);
        }
        self.open = entry.history().filter(|h| !h.is_complete()).map(|_| entry);
        Ok(())
    }

    fn failure(&mut self, failure: &'a ScanFailure) -> Result<(), &'static str> {
        if self.open.is_some() || self.listing_failure.is_some() {
            return Err("failure interrupts history or follows listing failure");
        }
        match failure.scope {
            FailureScope::Listing => {
                if &failure.path != self.directory {
                    return Err("listing failure belongs to another directory");
                }
                self.listing_failure = Some(failure);
            }
            FailureScope::Child => {
                if failure.delivered.is_some() {
                    return Err("child failure has a listing count");
                }
                self.counts.failed_children = self.counts.failed_children.saturating_add(1);
                if &failure.path == self.directory {
                    self.unnamed_failure = true;
                } else {
                    self.failures
                        .insert(path_key(self.directory, &failure.path)?);
                }
            }
        }
        Ok(())
    }

    fn close(&self, listed: &DirectoryListed) -> Result<(), &'static str> {
        if &listed.path != self.directory {
            return Err("boundary belongs to another directory");
        }
        if self.open.is_some() {
            return Err("incomplete history at directory boundary");
        }
        if !listed.counts.is_balanced() || !same_counts(&self.counts, &listed.counts) {
            return Err("directory counts disagree with its items");
        }
        match self.listing_failure {
            Some(failure)
                if failure.listing_outcome() == Some(listed.outcome)
                    && failure.delivered == Some(listed.counts.listed) => {}
            None if listed.outcome.is_complete() => {}
            _ => return Err("listing failure and boundary disagree"),
        }
        if listed.vanished && (!listed.outcome.is_complete() || listed.counts.listed != 0) {
            return Err("vanished directory has a nonempty or failed listing");
        }
        Ok(())
    }
}

fn entry_key(directory: &StoragePath, entry: &ObservedEntry) -> Result<ChildKey, &'static str> {
    let key = path_key(directory, entry.path())?;
    if key == ChildKey::DirectoryObject
        && (entry.kind() != EntryKind::File || directory.as_str().is_empty())
    {
        return Err("only a non-root directory's file can be its own object");
    }
    Ok(key)
}

fn path_key(directory: &StoragePath, path: &StoragePath) -> Result<ChildKey, &'static str> {
    let parent = directory.as_str();
    let prefix = if parent.is_empty() {
        parent.to_owned()
    } else {
        format!("{parent}/")
    };
    let name = path
        .as_str()
        .strip_prefix(&prefix)
        .ok_or("entry is outside directory")?;
    if name.is_empty() && path != directory {
        return Ok(ChildKey::DirectoryObject);
    }
    if name.is_empty() || name == "." || name == ".." || name.contains('/') {
        return Err("entry is not a direct child");
    }
    Ok(ChildKey::Name(name.to_owned()))
}

fn validate_segment(
    previous: Option<&ObservedEntry>,
    entry: &ObservedEntry,
) -> Result<(), &'static str> {
    let Some(previous) = previous else {
        return if entry.history().is_some_and(|h| h.segment() != 0) {
            Err("history does not start at segment zero")
        } else {
            Ok(())
        };
    };
    let before = previous
        .history()
        .ok_or("history continuation lacks history")?;
    let next = entry
        .history()
        .ok_or("entry interrupts incomplete history")?;
    if previous.path() != entry.path()
        || previous.kind() != entry.kind()
        || previous.identity_key() != entry.identity_key()
        || previous.size() != entry.size()
        || before.segment().checked_add(1) != Some(next.segment())
        || before.order() != next.order()
        || before.largest_version() != next.largest_version()
    {
        return Err("history segments are not consecutive observations of one key");
    }
    Ok(())
}

fn same_counts(actual: &BlockCounts, stated: &BlockCounts) -> bool {
    actual.emitted == stated.emitted
        && actual.failed_children == stated.failed_children
        && actual.files == stated.files
        && actual.directories == stated.directories
        && actual.symlinks == stated.symlinks
        && actual.other == stated.other
        && actual.bytes == stated.bytes
        && actual.versions == stated.versions
        && actual.delete_markers == stated.delete_markers
        && actual.all_versions_bytes == stated.all_versions_bytes
}
