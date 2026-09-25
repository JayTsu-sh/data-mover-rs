//! What the cursor holds, and which of the tree's dimensions each part grows with.
//!
//! None of this is visible in the item stream, so these are the only assertions that can tell a
//! traversal which bounds what it retains from one that keeps every subdirectory of a
//! ten-million-entry tree. They assert the *shape* of each curve — grows with entries, grows
//! with subdirectories, stays within a budget — because that is what distinguishes a fix from a
//! coincidence, and because exact byte counts are not reproducible across allocators.

use super::*;
use crate::traversal::{TraversalCandidate, TraversalDecision, TraversalFilter};

/// One directory of `fan_out` children, all files or all (empty) directories, under `root`.
///
/// `root` is a parameter so a test can make the shared parent prefix long without making the
/// tree deep: prefix cost is per child, and every child of one directory repeats it.
struct WideNamespace {
    root: String,
    fan_out: usize,
    subdirs: bool,
}

#[async_trait]
impl Namespace for WideNamespace {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        let NamespaceRequest::List(directory) = request else {
            return Err(StorageRoleFailure::Entry(entry_failure(
                &StoragePath::root(),
                FailureClass::Unsupported,
            )));
        };
        if directory.as_str() != self.root {
            // Every child directory is empty, so descending into one costs a listing and holds
            // nothing: `descend_slots` stays the root's alone.
            return Ok(NamespaceResult::Entries(Vec::new()));
        }
        let kind = if self.subdirs {
            EntryKind::Directory
        } else {
            EntryKind::File
        };
        Ok(NamespaceResult::Entries(
            (0..self.fan_out)
                .map(|index| descriptor(&format!("{}/child-{index:06}", self.root), kind))
                .collect(),
        ))
    }
}

/// A chain `d0/d1/…`, each level holding `fan_out` files plus the next level.
struct DeepNamespace {
    depth: usize,
    fan_out: usize,
}

#[async_trait]
impl Namespace for DeepNamespace {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        let NamespaceRequest::List(directory) = request else {
            return Err(StorageRoleFailure::Entry(entry_failure(
                &StoragePath::root(),
                FailureClass::Unsupported,
            )));
        };
        let level =
            directory.as_str().matches('/').count() + usize::from(!directory.as_str().is_empty());
        if level >= self.depth {
            return Ok(NamespaceResult::Entries(Vec::new()));
        }
        let prefix = directory.as_str();
        let join = |name: &str| {
            if prefix.is_empty() {
                name.to_owned()
            } else {
                format!("{prefix}/{name}")
            }
        };
        let mut entries: Vec<_> = (0..self.fan_out)
            .map(|index| descriptor(&join(&format!("f{index}")), EntryKind::File))
            .collect();
        entries.push(descriptor(&join("next"), EntryKind::Directory));
        Ok(NamespaceResult::Entries(entries))
    }
}

/// `breadth` sibling directories, each holding `leaves` files, so prefetch can get ahead of the
/// cursor and hold several finished listings it has not been asked for yet.
struct PrefetchedNamespace {
    breadth: usize,
    leaves: usize,
}

#[async_trait]
impl Namespace for PrefetchedNamespace {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        let NamespaceRequest::List(directory) = request else {
            return Err(StorageRoleFailure::Entry(entry_failure(
                &StoragePath::root(),
                FailureClass::Unsupported,
            )));
        };
        Ok(NamespaceResult::Entries(if directory.as_str().is_empty() {
            (0..self.breadth)
                .map(|index| descriptor(&format!("d{index:04}"), EntryKind::Directory))
                .collect()
        } else {
            (0..self.leaves)
                .map(|index| {
                    descriptor(
                        &format!("{}/leaf{index:04}", directory.as_str()),
                        EntryKind::File,
                    )
                })
                .collect()
        }))
    }
}

/// Keeps everything; `needs_modified` decides whether the traversal may settle the decision from
/// the listing or has to wait for each child's observation.
#[derive(Debug)]
struct KeepAll {
    needs_modified: bool,
}

impl TraversalFilter for KeepAll {
    fn needs_modified(&self) -> bool {
        self.needs_modified
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        TraversalDecision {
            emit: true,
            descend: candidate.kind == EntryKind::Directory,
            filter_children: true,
        }
    }
}

fn probed_request(root: &str, window: usize) -> TraversalRequest {
    TraversalRequest {
        root: path(root),
        order: crate::traversal::TraversalOrder::Admission,
        max_inflight_operations: NonZeroUsize::new(window)
            .unwrap_or_else(|| unreachable!("callers pass a nonzero window")),
        max_buffered_items: NonZeroUsize::new(window)
            .unwrap_or_else(|| unreachable!("callers pass a nonzero window")),
        observation_plan: ObservationPlan::default(),
        cancel: tokio_util::sync::CancellationToken::new(),
        filter: None,
        max_depth: None,
        versions: TraversalVersions::Current,
    }
}

/// Runs to completion and returns the per-field high-water marks.
async fn peak_of(
    namespace: Arc<dyn Namespace>,
    request: TraversalRequest,
) -> super::super::residency::Residency {
    let (source, peak) =
        StorageTraversalSource::with_roles_probed(namespace, Arc::new(FakeMetadata));
    let mut session = source.traverse(request);
    while session.next_item().await.is_some() {}
    session
        .finish()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    peak.peak()
}

const FAN_OUT: usize = 2_000;

/// Files live in the block and never in the descend set, so bounding the block bounds them.
#[tokio::test]
async fn files_are_held_as_block_children_only() {
    let peak = peak_of(
        Arc::new(WideNamespace {
            root: "wide".to_owned(),
            fan_out: FAN_OUT,
            subdirs: false,
        }),
        probed_request("wide", 4),
    )
    .await;
    // Scales with the number of entries, not with the window: the whole listing is resident
    // bar the handful already admitted when the sample was taken.
    assert!(peak.block_children > FAN_OUT / 2, "{peak:?}");
    assert_eq!(peak.descend_slots, 0, "{peak:?}");
}

/// Subdirectories live in the descend set, which is the part no amount of paging can bound:
/// coming back to `FAN_OUT` subdirectories means remembering `FAN_OUT` places to come back to.
#[tokio::test]
async fn subdirectories_are_held_as_descend_slots() {
    let peak = peak_of(
        Arc::new(WideNamespace {
            root: "wide".to_owned(),
            fan_out: FAN_OUT,
            subdirs: true,
        }),
        probed_request("wide", 4),
    )
    .await;
    assert_eq!(peak.descend_slots, FAN_OUT, "{peak:?}");
}

/// The sample covers the whole stack, not its top: every frame on the path from the root keeps
/// its own remaining children, so a chain holds one block per level at once.
/// Depth is not a multiplier on what is pending: a block is admitted in full before the cursor
/// descends, so only the top frame ever has children waiting and only it has a slot left to
/// consume. Ancestor frames keep their queue's *room* though, which is why capacity is counted
/// apart — a chain of wide levels holds one widest-block allocation per level until it unwinds.
#[tokio::test]
async fn depth_multiplies_retained_room_but_not_pending_children() {
    let (depth, fan_out) = (32, 8);
    let peak = peak_of(
        Arc::new(DeepNamespace { depth, fan_out }),
        probed_request("", 4),
    )
    .await;
    assert!(
        peak.block_children <= fan_out + 1,
        "more than one level pending: {peak:?}"
    );
    // One per level at most: a frame's slot is consumed the moment the cursor descends through
    // it, so the stack carries the path's remaining turns, not each level's whole fan-out.
    assert!(
        peak.descend_slots <= depth,
        "slots grew faster than the depth: {peak:?}"
    );
    assert!(
        peak.block_capacity > 4 * (fan_out + 1),
        "ancestor frames released their room: {peak:?}"
    );
}

/// Prefetch keeps its two budgets: at most `P` listings in flight, and at most `2P` started but
/// not yet taken. Without this the probe could miss the arrived-but-untaken set entirely.
#[tokio::test]
async fn prefetched_listings_stay_within_their_budgets() {
    let window = 4;
    let peak = peak_of(
        Arc::new(WideNamespace {
            root: "wide".to_owned(),
            fan_out: 64,
            subdirs: true,
        }),
        probed_request("wide", window),
    )
    .await;
    assert!(peak.listings_inflight <= window, "{peak:?}");
    assert!(peak.listings_arrived <= 2 * window, "{peak:?}");
    assert!(
        peak.listings_inflight + peak.listings_arrived > 1,
        "prefetch never got going, so the budgets are untested: {peak:?}"
    );
}

/// The set that is easy to miss: a filter needing `modified` defers every decision, and a
/// deferred decision is held until its block closes — the same exit the descend slots use. So it
/// grows with the number of subdirectories just as far, and a change that shrinks the slots
/// while leaving this alone has not bounded anything on the mainline filter path.
#[tokio::test]
async fn a_deferred_filter_holds_one_decision_per_subdirectory() {
    let mut deferred = probed_request("wide", 4);
    deferred.filter = Some(Arc::new(KeepAll {
        needs_modified: true,
    }));
    let with_deferral = peak_of(
        Arc::new(WideNamespace {
            root: "wide".to_owned(),
            fan_out: FAN_OUT,
            subdirs: true,
        }),
        deferred,
    )
    .await;

    let mut immediate = probed_request("wide", 4);
    immediate.filter = Some(Arc::new(KeepAll {
        needs_modified: false,
    }));
    let without = peak_of(
        Arc::new(WideNamespace {
            root: "wide".to_owned(),
            fan_out: FAN_OUT,
            subdirs: true,
        }),
        immediate,
    )
    .await;

    assert_eq!(without.deferred_decisions, 0, "{without:?}");
    // Both sets, not one: the deferred path keeps a slot *and* a decision per subdirectory.
    assert!(
        with_deferral.deferred_decisions > FAN_OUT / 2,
        "{with_deferral:?}"
    );
    assert!(with_deferral.descend_slots >= FAN_OUT, "{with_deferral:?}");
}

/// Prefix cost is per child: every child of one directory repeats its parent's path in full.
/// Lengthening only the parent's name, with the same number of children carrying the same names,
/// has to move `path_bytes` by about the added prefix times the fan-out.
#[tokio::test]
async fn path_bytes_carries_the_parent_prefix_once_per_child() {
    let short = peak_of(
        Arc::new(WideNamespace {
            root: "w".to_owned(),
            fan_out: FAN_OUT,
            subdirs: false,
        }),
        probed_request("w", 4),
    )
    .await;
    let long_root = "w".repeat(200);
    let long = peak_of(
        Arc::new(WideNamespace {
            root: long_root.clone(),
            fan_out: FAN_OUT,
            subdirs: false,
        }),
        probed_request(&long_root, 4),
    )
    .await;
    let added = long.path_bytes - short.path_bytes;
    let expected = (long_root.len() - 1) * FAN_OUT;
    // Within a percent: the peaks are taken a few admissions apart, so a handful of children's
    // paths are outside one sample or the other.
    assert!(
        added * 100 >= expected * 99 && added * 99 <= expected * 100,
        "added {added}, expected about {expected}: {short:?} vs {long:?}"
    );
}

/// Listings that arrived before the cursor asked for them are resident too, and there can be
/// `2P` of them. Counting only the frames understates the peak by everything prefetch is
/// holding — here by an order of magnitude — which would make a paging change look effective
/// while the prefetch set still grew with the tree.
#[tokio::test]
async fn arrived_but_untaken_listings_count_towards_residency() {
    let (breadth, leaves) = (32, 50);
    let peak = peak_of(
        Arc::new(PrefetchedNamespace { breadth, leaves }),
        probed_request("", 4),
    )
    .await;
    // Several finished listings at once, each a full block: the frames alone never account for
    // this, and a probe that skipped them would report a fraction of the real peak.
    assert!(
        peak.prefetched_children > leaves,
        "at most one finished listing: {peak:?}"
    );
    assert!(
        peak.listings_arrived > 1,
        "prefetch never got ahead: {peak:?}"
    );
}
