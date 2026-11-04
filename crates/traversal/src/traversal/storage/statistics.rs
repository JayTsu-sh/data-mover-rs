//! 两种调度器共用的目录统计：准备页只携带事实，输出完成后才交付快照。
use super::output::{ChildDescent, ChildItem};
use crate::model::{EntryKind, ObservedEntry, StoragePath, VersionRecord};
use crate::traversal::{DirectoryStatistics, EntryKindCounts, ListingOutcome, TraversalItem};

#[derive(Clone, Copy, Default)]
pub(super) struct PageStatistics {
    pub(super) filtered: EntryKindCounts,
    pub(super) vanished: EntryKindCounts,
    pub(super) pruned: u64,
    pub(super) truncated: u64,
}

impl PageStatistics {
    pub(super) fn hide(&mut self, kind: EntryKind, excluded: bool) {
        if !excluded {
            self.filtered.count(Some(kind));
        }
    }
}

pub(super) struct DirectoryAccumulator(DirectoryStatistics);

impl DirectoryAccumulator {
    pub(super) fn new(path: StoragePath) -> Self {
        Self(DirectoryStatistics {
            path,
            pass_start: true,
            revision: 0,
            scanned: EntryKindCounts::default(),
            filtered: EntryKindCounts::default(),
            retained: EntryKindCounts::default(),
            failed: EntryKindCounts::default(),
            vanished: EntryKindCounts::default(),
            pruned_dirs: 0,
            truncated_dirs: 0,
            regular_file_bytes: 0,
            unknown_file_sizes: 0,
            versions: 0,
            delete_markers: 0,
            all_versions_bytes: 0,
            outcome: None,
            vanished_listing: false,
        })
    }

    pub(super) fn page(&mut self, page: PageStatistics) {
        self.0.filtered = self.0.filtered.saturating_add(page.filtered);
        self.0.vanished = self.0.vanished.saturating_add(page.vanished);
        self.0.pruned_dirs = self.0.pruned_dirs.saturating_add(page.pruned);
        self.0.truncated_dirs = self.0.truncated_dirs.saturating_add(page.truncated);
    }

    pub(super) fn child(
        &mut self,
        item: &ChildItem,
        kind: Option<EntryKind>,
        descent: Option<ChildDescent>,
    ) {
        match descent {
            Some(ChildDescent::Pruned) => self.0.pruned_dirs = self.0.pruned_dirs.saturating_add(1),
            Some(ChildDescent::Truncated) => {
                self.0.truncated_dirs = self.0.truncated_dirs.saturating_add(1);
            }
            _ => {}
        }
        match item {
            ChildItem::Item(TraversalItem::Entry(entry)) => self.entry(entry),
            ChildItem::Item(TraversalItem::Failure(failure))
                if failure.listing_outcome().is_none() =>
            {
                self.0.failed.count(kind);
            }
            ChildItem::ObservedFailure { .. } => self.0.failed.count(kind),
            ChildItem::Vanished(_) => self.0.vanished.count(kind),
            ChildItem::Filtered(_) => self.0.filtered.count(kind),
            _ => {}
        }
    }

    fn entry(&mut self, entry: &ObservedEntry) {
        self.0.retained.count(Some(entry.kind()));
        if entry.kind() == EntryKind::File {
            if let Some(size) = entry.size() {
                self.0.regular_file_bytes = self.0.regular_file_bytes.saturating_add(size);
                if entry.history().is_none() {
                    self.0.all_versions_bytes = self.0.all_versions_bytes.saturating_add(size);
                }
            } else if !entry
                .history()
                .and_then(|history| history.latest())
                .is_some_and(VersionRecord::is_delete_marker)
            {
                self.0.unknown_file_sizes = self.0.unknown_file_sizes.saturating_add(1);
            }
        }
        if let Some(history) = entry.history() {
            for record in history.records() {
                if record.is_delete_marker() {
                    self.0.delete_markers = self.0.delete_markers.saturating_add(1);
                } else {
                    self.0.versions = self.0.versions.saturating_add(1);
                    self.0.all_versions_bytes = self
                        .0
                        .all_versions_bytes
                        .saturating_add(record.size().unwrap_or(0));
                }
            }
        }
    }

    pub(super) fn snapshot(
        &mut self,
        path: &StoragePath,
        outcome: Option<ListingOutcome>,
        vanished: bool,
    ) -> TraversalItem {
        if self.0.path != *path {
            self.0.path = path.clone();
        }
        self.0.scanned = self
            .0
            .retained
            .saturating_add(self.0.filtered)
            .saturating_add(self.0.failed)
            .saturating_add(self.0.vanished);
        self.0.revision = self.0.revision.saturating_add(1);
        self.0.pass_start = self.0.revision == 1;
        self.0.outcome = outcome;
        self.0.vanished_listing = vanished;
        TraversalItem::DirectoryStatistics(Box::new(self.0.clone()))
    }
}
