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
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path;
        if directory.as_str() != self.root {
            // Every child directory is empty, so descending into one costs a listing and holds
            // nothing: `descend_slots` stays the root's alone.
            return entries(&directory, Vec::new());
        }
        let kind = if self.subdirs {
            EntryKind::Directory
        } else {
            EntryKind::File
        };
        entries(
            &directory,
            (0..self.fan_out)
                .map(|index| descriptor(&format!("{}/child-{index:06}", self.root), kind))
                .collect(),
        )
    }
}

/// A chain `d0/d1/…`, each level holding `fan_out` files plus the next level.
struct DeepNamespace {
    depth: usize,
    fan_out: usize,
}

#[async_trait]
impl Namespace for DeepNamespace {
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path;
        let level =
            directory.as_str().matches('/').count() + usize::from(!directory.as_str().is_empty());
        if level >= self.depth {
            return entries(&directory, Vec::new());
        }
        let prefix = directory.as_str();
        let join = |name: &str| {
            if prefix.is_empty() {
                name.to_owned()
            } else {
                format!("{prefix}/{name}")
            }
        };
        let mut children: Vec<_> = (0..self.fan_out)
            .map(|index| descriptor(&join(&format!("f{index}")), EntryKind::File))
            .collect();
        children.push(descriptor(&join("next"), EntryKind::Directory));
        entries(&directory, children)
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
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path;
        entries(
            &directory,
            if directory.as_str().is_empty() {
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
            },
        )
    }
}

/// Keeps everything; `needs_modified` decides whether the traversal may settle the decision from
/// the listing or has to wait for each child's observation.
#[derive(Debug)]
struct KeepAll {
    needs_modified: bool,
}

impl TraversalFilter for KeepAll {
    fn required_metadata(&self) -> MetadataRequirements {
        if self.needs_modified {
            MetadataRequirements::NONE.with(MetadataFamily::Timestamps)
        } else {
            MetadataRequirements::NONE
        }
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
    TraversalRequest::new(path(root))
        .order(crate::traversal::TraversalOrder::Blocks)
        .max_inflight_operations(
            NonZeroUsize::new(window)
                .unwrap_or_else(|| unreachable!("callers pass a nonzero window")),
        )
        .cancel(tokio_util::sync::CancellationToken::new())
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
/// This fixture hands its directory over as one page, so that page is resident whole; how many
/// pages of a paged listing are held at once is pinned in `streaming.rs`.
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
    // Scales with the page, not with the window: the one page is resident bar the handful
    // already admitted when the sample was taken.
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

/// The sample covers the whole stack, not its top. Depth is not a multiplier on what is pending:
/// a block is admitted in full, and its listing has ended, before the cursor descends, so only
/// the top frame ever has children waiting and only it has a slot left to consume. Nor on the
/// room held: a closed block releases its queue's allocation, so the room is what the top frame
/// and the prefetched listings hold (at most `4P` of them), whatever the depth. Before P7 C4 an
/// ancestor kept its widest block's room until it unwound.
#[tokio::test]
async fn depth_multiplies_neither_pending_children_nor_retained_room() {
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
    // The top frame's page and up to `4P` prefetched pages, `P` being the window of 4 — well
    // below one page per level of the 32.
    assert!(
        peak.block_capacity <= (4 * 4 + 1) * (fan_out + 1),
        "ancestor frames kept their room: {peak:?}"
    );
}

/// Prefetch keeps its request and completed-result budgets: at most `P` listings in flight, and
/// at most `4P` arrived but not yet taken. The unfinished `2P` bound is exercised by the parked
/// listing test. Without this probe could miss the arrived-but-untaken set entirely.
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
    assert!(peak.listings_arrived <= 4 * window, "{peak:?}");
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
    deferred.observation_plan =
        ObservationPlan::default().with_timestamps(ObservationMode::Required);
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
/// `4P` of them. Counting only the frames understates the peak by everything prefetch is
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
