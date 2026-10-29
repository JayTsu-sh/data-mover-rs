//! Resumable traversal in the storage traversal (P7 §9): whether and when a traversal checkpoints,
//! what it resumes from, and how a checkpoint is made from either order's state.
//!
//! Only a request with checkpoints on, or one resuming, has a [`Resume`]; everything here is
//! reached through `Shared::resume`, so a traversal with neither reads no clock and tracks
//! nothing (Off costs nothing, §9.1).
//!
//! - **When.** One due time for the whole traversal. Whoever passes a check point after it is due
//!   (an Unordered worker after a page or a finished directory, the Blocks driver after a flush)
//!   claims it with one compare-and-swap, so a period yields one checkpoint however many workers
//!   see it due, and a traversal that ends sooner yields none.
//! - **What.** Unordered: the pool's frontier ([`frontier_entries`]). Blocks: the depth-first
//!   position recorded at the last directory whose block had begun before everything delivered so
//!   far ([`positions`]); a resumed Blocks traversal first lists the position's ancestors again,
//!   emitting nothing, to learn which of their subdirectories remain ([`relist`]).

use std::collections::HashSet;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

use super::DirectoryWork;
use crate::model::{BackendIdentity, StoragePath};
use crate::traversal::checkpoint::{self, FrontierEntry, MAX_ENTRIES};
use crate::traversal::{ResumeRefusal, TraversalItem, TraversalOrder, TraversalRequest};

pub(super) mod blocks;
pub(super) mod positions;
pub(super) mod relist;

/// What a traversal reads the time from. Only a traversal with checkpoints on has one.
pub(super) enum Clock {
    /// Milliseconds since the traversal started.
    System(Instant),
    /// A test's clock, which moves only when the test says so.
    #[cfg(test)]
    Manual(Arc<ManualClock>),
}

impl Clock {
    fn now_ms(&self) -> u64 {
        match self {
            Self::System(start) => u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
            #[cfg(test)]
            Self::Manual(clock) => {
                clock.reads.fetch_add(1, Ordering::SeqCst);
                clock.now_ms.load(Ordering::SeqCst)
            }
        }
    }
}

/// A clock a test moves by hand, counting how often the traversal read it.
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct ManualClock {
    now_ms: AtomicU64,
    reads: AtomicUsize,
}

#[cfg(test)]
impl ManualClock {
    pub(crate) fn advance(&self, by: Duration) {
        let by = u64::try_from(by.as_millis()).unwrap_or(u64::MAX);
        self.now_ms.fetch_add(by, Ordering::SeqCst);
    }

    pub(crate) fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

/// Periodic checkpoints: the interval, the clock, and when the next one is due.
struct Periodic {
    every_ms: u64,
    clock: Clock,
    due_ms: AtomicU64,
}

/// A traversal that checkpoints, resumes, or both.
pub(super) struct Resume {
    order: TraversalOrder,
    fingerprint: [u8; 32],
    periodic: Option<Periodic>,
    /// Where a resumed traversal starts: the checkpoint's entries, checked against the request.
    pub(super) start: Option<Vec<FrontierEntry>>,
}

impl Resume {
    /// What `request` asks for, checked before any I/O: `None` when it neither checkpoints nor
    /// resumes. `listing_resume_key`: the namespace's opaque
    /// listing semantics identifier, which the fingerprint binds. `clock` is called only when it
    /// checkpoints.
    ///
    /// # Errors
    /// Why the request cannot resume from its checkpoint, or cannot be resumable at all.
    pub(super) fn prepare(
        request: &TraversalRequest,
        identity: &BackendIdentity,
        listing_resume_key: &[u8],
        clock: impl FnOnce() -> Clock,
    ) -> Result<Option<Self>, ResumeRefusal> {
        let every = request.resumable.every();
        if every.is_none() && request.resume_from.is_none() {
            return Ok(None);
        }
        let fingerprint = checkpoint::fingerprint(identity, request, listing_resume_key)?;
        let start = request
            .resume_from
            .as_ref()
            .map(|saved| checkpoint::open(saved, &fingerprint, request.order, &request.root))
            .transpose()?;
        let periodic = every.map(|every| {
            let every_ms = u64::try_from(every.as_millis()).unwrap_or(u64::MAX);
            Periodic {
                every_ms,
                clock: clock(),
                due_ms: AtomicU64::new(every_ms),
            }
        });
        Ok(Some(Self {
            order: request.order,
            fingerprint,
            periodic,
            start,
        }))
    }

    /// Whether this traversal emits checkpoints (and drains on cancellation).
    pub(super) const fn emits(&self) -> bool {
        self.periodic.is_some()
    }

    /// Whether a checkpoint is due now, claiming it: `true` for exactly one caller per period.
    pub(super) fn claim_due(&self) -> bool {
        let Some(periodic) = &self.periodic else {
            return false;
        };
        let now = periodic.clock.now_ms();
        let due = periodic.due_ms.load(Ordering::Acquire);
        now >= due
            && periodic
                .due_ms
                .compare_exchange(
                    due,
                    now.saturating_add(periodic.every_ms),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
    }

    /// The checkpoint item of `entries`, or `None` (with a warning) past the size limits: an
    /// earlier checkpoint stays valid, so skipping one only means more is listed again.
    pub(super) fn checkpoint(&self, entries: &[FrontierEntry]) -> Option<TraversalItem> {
        let encoded = checkpoint::encode(self.order, &self.fingerprint, entries);
        if encoded.is_none() {
            tracing::warn!(
                entries = entries.len(),
                "traversal checkpoint skipped: its position is too large to record"
            );
        }
        encoded.map(|saved| TraversalItem::Checkpoint(Box::new(saved)))
    }
}

/// Past this many directories held, an Unordered snapshot is not taken at all.
pub(super) const SNAPSHOT_CAP: usize = MAX_ENTRIES;

/// A frontier snapshot as checkpoint entries: sorted, each once, and none whose ancestor is also
/// there — listing the ancestor again finds it again, with the same decisions, so keeping it
/// would only list its subtree twice.
pub(super) fn frontier_entries<'a>(
    works: impl IntoIterator<Item = &'a DirectoryWork>,
) -> Vec<FrontierEntry> {
    let mut entries: Vec<FrontierEntry> = works.into_iter().map(entry_of).collect();
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    entries.dedup_by(|later, earlier| later.path == earlier.path);
    let held: HashSet<&str> = entries.iter().map(|entry| entry.path.as_str()).collect();
    let keep: Vec<bool> = entries
        .iter()
        .map(|entry| !has_held_ancestor(&held, entry.path.as_str()))
        .collect();
    let mut keep = keep.into_iter();
    entries.retain(|_| keep.next().unwrap_or(true));
    entries
}

/// Whether some proper ancestor of `path`, walked component by component, is in `held`.
fn has_held_ancestor(held: &HashSet<&str>, path: &str) -> bool {
    if !path.is_empty() && held.contains("") {
        return true;
    }
    let mut rest = path;
    while let Some((parent, _)) = rest.rsplit_once('/') {
        if held.contains(parent) {
            return true;
        }
        rest = parent;
    }
    false
}

/// A directory to list, as a checkpoint records it: its descent hint is a live handle's, not
/// something to persist.
pub(super) fn entry_of(work: &DirectoryWork) -> FrontierEntry {
    FrontierEntry {
        path: work.path.clone(),
        child_depth: work.child_depth,
        filter_children: work.filter_children,
    }
}

/// The work a checkpoint entry stands for.
pub(super) fn work_of(entry: &FrontierEntry) -> DirectoryWork {
    DirectoryWork {
        path: entry.path.clone(),
        child_depth: entry.child_depth,
        filter_children: entry.filter_children,
        hint: None,
    }
}

/// The name a Blocks descent sorts by: the path's last non-empty segment.
pub(super) fn descent_name(path: &StoragePath) -> &str {
    super::entry_name(path)
}

#[cfg(test)]
mod tests;
