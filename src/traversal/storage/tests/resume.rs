//! Resumable traversal (P7 C11) over a generated tree, in both orders: no checkpoint and no clock
//! when off; one per period on a manual clock; resuming from **every** checkpoint a traversal
//! emitted loses nothing (duplicates allowed, and counted); cancellation drains and ends with a
//! checkpoint; a terminal failure too; checkpoints of another request are refused before any I/O.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use super::filter::CountingMetadata;
use super::*;
use crate::model::BackendSessionFailure;
use crate::storage::PageSink;
use crate::storage::listing::child_path;
use crate::traversal::{
    Checkpoint, Resumable, ResumeRefusal, TraversalCandidate, TraversalDecision, TraversalFilter,
    TraversalOrder,
};

/// A generated tree, listed in pages of `page` children. A path it does not know is gone
/// (`NotFound`); listing a path in `denied` fails `PermissionDenied`; listing `broken` ends the
/// traversal with a session failure.
pub(super) struct Tree {
    dirs: Mutex<HashMap<String, Vec<(String, EntryKind)>>>,
    page: usize,
    denied: Mutex<HashSet<String>>,
    broken: Mutex<Option<String>>,
    lists: AtomicUsize,
    /// Reading a symlink's target waits for a permit of this, when set.
    links: Option<Arc<tokio::sync::Semaphore>>,
}

impl Tree {
    /// `fanout` subdirectories and `files` files per directory, `depth` levels of directories.
    pub(super) fn generate(fanout: usize, depth: usize, files: usize, page: usize) -> Self {
        let mut dirs = HashMap::new();
        let mut level = vec![String::new()];
        for depth_left in (0..=depth).rev() {
            let mut next = Vec::new();
            for dir in level {
                let prefix = if dir.is_empty() {
                    String::new()
                } else {
                    format!("{dir}/")
                };
                let mut children: Vec<(String, EntryKind)> = (0..files)
                    .map(|index| (format!("f{index}"), EntryKind::File))
                    .collect();
                if depth_left > 0 {
                    for index in (0..fanout).rev() {
                        children.push((format!("d{index}"), EntryKind::Directory));
                        next.push(format!("{prefix}d{index}"));
                    }
                }
                dirs.insert(dir, children);
            }
            level = next;
        }
        Self {
            dirs: Mutex::new(dirs),
            page,
            denied: Mutex::new(HashSet::new()),
            broken: Mutex::new(None),
            lists: AtomicUsize::new(0),
            links: None,
        }
    }

    /// The same tree with a symlink `link` first in the root, whose target is read only once
    /// the returned gate lets it.
    pub(super) fn with_gated_link(mut self) -> (Self, Arc<tokio::sync::Semaphore>) {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        self.links = Some(Arc::clone(&gate));
        self.dirs
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(String::new())
            .or_default()
            .insert(0, ("link".to_owned(), EntryKind::Symlink));
        (self, gate)
    }

    pub(super) fn lists(&self) -> usize {
        self.lists.load(Ordering::SeqCst)
    }

    pub(super) fn deny(&self, path: &str) {
        self.denied
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(path.to_owned());
    }

    pub(super) fn remove(&self, path: &str) {
        let mut dirs = self.dirs.lock().unwrap_or_else(PoisonError::into_inner);
        dirs.retain(|dir, _| dir != path && !dir.starts_with(&format!("{path}/")));
    }

    fn break_at(&self, path: Option<&str>) {
        *self.broken.lock().unwrap_or_else(PoisonError::into_inner) = path.map(str::to_owned);
    }

    /// Every entry of the tree, as a full traversal reports it.
    pub(super) fn all_entries(&self) -> BTreeSet<String> {
        let dirs = self.dirs.lock().unwrap_or_else(PoisonError::into_inner);
        dirs.iter()
            .flat_map(|(dir, children)| {
                children.iter().map(move |(name, _)| {
                    if dir.is_empty() {
                        name.clone()
                    } else {
                        format!("{dir}/{name}")
                    }
                })
            })
            .collect()
    }
}

fn session_failure() -> StorageRoleFailure {
    StorageRoleFailure::Session(
        BackendSessionFailure::new(
            Operation::Traverse,
            FailureClass::Connectivity,
            Transience::Transient,
            "the test session broke",
        )
        .unwrap_or_else(|error| panic!("{error}")),
    )
}

#[async_trait]
impl Namespace for Tree {
    fn list(&self, request: ListRequest) -> ListingStream {
        self.lists.fetch_add(1, Ordering::SeqCst);
        let directory = request.path;
        let key = directory.as_str().to_owned();
        let failure = if self
            .broken
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_deref()
            == Some(key.as_str())
        {
            Some(session_failure())
        } else if self
            .denied
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&key)
        {
            Some(StorageRoleFailure::Entry(entry_failure(
                &directory,
                FailureClass::PermissionDenied,
            )))
        } else {
            None
        };
        let children = self
            .dirs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
            .cloned();
        let page = self.page.max(1);
        ListingStream::from_producer(directory.clone(), move |sink: PageSink| async move {
            if let Some(failure) = failure {
                return sink.fail(failure).await;
            }
            let Some(children) = children else {
                return sink
                    .fail(StorageRoleFailure::Entry(entry_failure(
                        &directory,
                        FailureClass::NotFound,
                    )))
                    .await;
            };
            for chunk in children.chunks(page) {
                let described = chunk
                    .iter()
                    .map(|(name, kind)| {
                        let child =
                            child_path(&directory, name).unwrap_or_else(|error| panic!("{error}"));
                        descriptor(child.as_str(), *kind)
                    })
                    .collect();
                let page = ListingPage::from_descriptors(&directory, described, Vec::new(), 0);
                if sink.send(page).await.is_err() {
                    return;
                }
            }
        })
    }

    async fn read_link(&self, link: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        if let Some(gate) = &self.links
            && let Ok(permit) = gate.acquire().await
        {
            permit.forget();
        }
        SymlinkTarget::new(
            SymlinkTargetEncoding::UnixBytes,
            link.as_str().as_bytes().to_vec(),
        )
        .map_err(|_| StorageRoleFailure::Entry(entry_failure(link, FailureClass::Protocol)))
    }
}

/// A filter that needs `modified`, so every decision below a filtered directory waits for an
/// observation, and prunes directories named `d1`.
#[derive(Debug)]
struct PruneD1;

impl TraversalFilter for PruneD1 {
    fn required_metadata(&self) -> MetadataRequirements {
        MetadataRequirements::NONE.with(MetadataFamily::Timestamps)
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        let pruned = candidate.kind == EntryKind::Directory && candidate.name == "d1";
        TraversalDecision {
            emit: true,
            descend: !pruned,
            filter_children: true,
        }
    }

    fn resume_key(&self) -> Option<Vec<u8>> {
        Some(b"prune d1".to_vec())
    }
}

pub(super) struct Setup {
    pub(super) tree: Arc<Tree>,
    pub(super) clock: Arc<ManualClock>,
    pub(super) source: StorageTraversalSource,
}

pub(super) fn setup(tree: Tree, observed: bool) -> Setup {
    let tree = Arc::new(tree);
    let clock = Arc::new(ManualClock::default());
    let metadata: Arc<dyn Metadata> = if observed {
        Arc::new(CountingMetadata::new())
    } else {
        Arc::new(FakeMetadata)
    };
    let source =
        StorageTraversalSource::with_roles(Arc::clone(&tree) as Arc<dyn Namespace>, metadata)
            .clocked(&clock);
    Setup {
        tree,
        clock,
        source,
    }
}

pub(super) fn request(order: TraversalOrder, inflight: usize) -> TraversalRequest {
    TraversalRequest::new(StoragePath::root())
        .order(order)
        .max_inflight_operations(NonZeroUsize::new(inflight).unwrap_or(NonZeroUsize::MIN))
}

/// `request` over the subtree at `root`.
fn rooted(request: TraversalRequest, root: &str) -> TraversalRequest {
    TraversalRequest {
        root: path(root),
        ..request
    }
}

/// Checkpoints at every chance: the manual clock never moves, and every period is zero.
pub(super) fn every_chance() -> Resumable {
    Resumable::On {
        every: Duration::ZERO,
    }
}

pub(super) type Run = (
    Vec<TraversalItem>,
    Result<TraversalOutcome, TraversalTerminalFailure>,
);

/// Runs `request` to its end; with `cancel_after`, cancels once that many entries arrived and
/// drains what the traversal still delivers.
pub(super) async fn run(
    source: &StorageTraversalSource,
    request: TraversalRequest,
    cancel_after: Option<usize>,
) -> Run {
    let cancel = request.cancel.clone();
    let mut session = source.traverse(request);
    let drained = tokio::time::timeout(Duration::from_secs(30), async {
        let mut items = Vec::new();
        let mut entries = 0;
        while let Some(item) = session.next_item().await {
            entries += usize::from(matches!(item, TraversalItem::Entry(_)));
            items.push(item);
            if cancel_after.is_some_and(|after| entries == after) {
                cancel.cancel();
            }
        }
        (items, session.finish().await)
    })
    .await;
    drained.unwrap_or_else(|_| panic!("the traversal did not end"))
}

pub(super) fn entry_paths(items: &[TraversalItem]) -> Vec<String> {
    items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Entry(entry) => Some(entry.path().as_str().to_owned()),
            _ => None,
        })
        .collect()
}

pub(super) fn checkpoints(items: &[TraversalItem]) -> Vec<(usize, Checkpoint)> {
    items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| match item {
            TraversalItem::Checkpoint(checkpoint) => Some((index, (**checkpoint).clone())),
            _ => None,
        })
        .collect()
}

/// What the entries before a checkpoint and a resumed traversal delivered together, against the
/// full set: nothing missing and nothing new. Returns the entries delivered twice.
pub(super) fn assert_no_loss(
    before: &[String],
    resumed: &[String],
    full: &BTreeSet<String>,
    case: &str,
) -> usize {
    let union: BTreeSet<String> = before.iter().chain(resumed).cloned().collect();
    let missing: Vec<_> = full.difference(&union).collect();
    let extra: Vec<_> = union.difference(full).collect();
    assert!(missing.is_empty(), "{case}: lost {missing:?}");
    assert!(extra.is_empty(), "{case}: invented {extra:?}");
    before.len() + resumed.len() - full.len()
}

/// Resumes from every checkpoint of a full run, as if the consumer had crashed right after
/// committing it: what it had received before the checkpoint and what the resumed run delivers
/// cover the tree. Returns the duplicates of each resume.
async fn resume_from_each(
    setup: &Setup,
    make: impl Fn() -> TraversalRequest,
    case: &str,
) -> Vec<usize> {
    let full = setup.tree.all_entries();
    let (items, outcome) = run(&setup.source, make().resumable(every_chance()), None).await;
    assert!(
        matches!(outcome, Ok(TraversalOutcome::Completed(_))),
        "{case}: {outcome:?}"
    );
    let delivered = entry_paths(&items);
    assert_eq!(
        delivered.iter().cloned().collect::<BTreeSet<_>>(),
        full,
        "{case}: full run"
    );
    assert_eq!(
        delivered.len(),
        full.len(),
        "{case}: a full run delivers each entry once"
    );
    let saved = checkpoints(&items);
    assert!(saved.len() > 3, "{case}: only {} checkpoints", saved.len());
    let mut duplicates = Vec::new();
    for (at, checkpoint) in saved {
        let before = entry_paths(&items[..at]);
        let (resumed, outcome) = run(&setup.source, make().resume_from(checkpoint), None).await;
        assert!(
            matches!(outcome, Ok(TraversalOutcome::Completed(_))),
            "{case} @{at}: {outcome:?}"
        );
        duplicates.push(assert_no_loss(
            &before,
            &entry_paths(&resumed),
            &full,
            &format!("{case} @{at}"),
        ));
    }
    duplicates
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resuming_from_any_checkpoint_loses_nothing_in_either_order() {
    for order in [TraversalOrder::Unordered, TraversalOrder::Blocks] {
        for (inflight, buffered) in [(1, 1), (4, 1), (8, 64)] {
            let mut setup = setup(Tree::generate(3, 3, 4, 2), false);
            setup.source = setup
                .source
                .output_capacity(NonZeroUsize::new(buffered).unwrap_or(NonZeroUsize::MIN));
            let case = format!("{order:?} c={inflight} b={buffered}");
            let duplicates = resume_from_each(&setup, || request(order, inflight), &case).await;
            // Not exact: a resumed directory is listed again whole. But never the whole tree
            // again from a late checkpoint.
            let full = setup.tree.all_entries().len();
            assert!(
                duplicates.last().is_some_and(|last| *last < full),
                "{case}: {duplicates:?}"
            );
        }
    }
}

/// With a filter that needs `modified`, a resumed Blocks traversal decides its ancestors'
/// remaining subdirectories from an observation again, as the first run did; Unordered takes the
/// same decisions from its frontier. The filter prunes every `d1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_filter_decides_alike_after_resuming() {
    for order in [TraversalOrder::Unordered, TraversalOrder::Blocks] {
        let setup = setup(Tree::generate(3, 3, 2, 2), true);
        let make = || request(order, 4).filter(Arc::new(PruneD1));
        let (items, _) = run(&setup.source, make(), None).await;
        let full: BTreeSet<String> = entry_paths(&items).into_iter().collect();
        assert!(full.iter().all(|path| !path.contains("d1/")), "pruned");
        let (items, _) = run(&setup.source, make().resumable(every_chance()), None).await;
        for (at, checkpoint) in checkpoints(&items) {
            let (resumed, _) = run(&setup.source, make().resume_from(checkpoint), None).await;
            assert_no_loss(
                &entry_paths(&items[..at]),
                &entry_paths(&resumed),
                &full,
                &format!("{order:?} @{at}"),
            );
        }
    }
}

/// Off: no checkpoint, and the clock is never read. On but shorter than the interval: none
/// either. Each period that passes yields one.
#[tokio::test]
async fn checkpoints_come_once_per_period_and_never_when_off() {
    for order in [TraversalOrder::Unordered, TraversalOrder::Blocks] {
        let mut setup = setup(Tree::generate(3, 3, 2, 2), false);
        setup.source = setup.source.output_capacity(NonZeroUsize::MIN);
        let (items, _) = run(&setup.source, request(order, 2), None).await;
        assert!(checkpoints(&items).is_empty());
        assert_eq!(setup.clock.reads(), 0, "{order:?}: Off read the clock");
        let every = Duration::from_mins(1);
        let on = || request(order, 2).resumable(Resumable::On { every });
        let (items, _) = run(&setup.source, on(), None).await;
        assert!(
            checkpoints(&items).is_empty(),
            "{order:?}: shorter than a period"
        );
        assert!(setup.clock.reads() > 0);
        // Two periods pass while the consumer takes its 5th and its 40th entry.
        let mut session = setup.source.traverse(on());
        let mut items = Vec::new();
        while let Some(item) = session.next_item().await {
            let entries = entry_paths(&items).len();
            if matches!(item, TraversalItem::Entry(_)) && (entries == 4 || entries == 39) {
                setup.clock.advance(every);
            }
            items.push(item);
        }
        assert_eq!(checkpoints(&items).len(), 2, "{order:?}");
        assert!(matches!(
            session.finish().await,
            Ok(TraversalOutcome::Completed(_))
        ));
    }
}

/// Cancelled part way, a resumable traversal delivers what it had produced and then its final
/// checkpoint, last; resuming from it, and again after cancelling that, until one completes,
/// covers the tree.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_pauses_and_a_chain_of_resumes_completes_the_tree() {
    for order in [TraversalOrder::Unordered, TraversalOrder::Blocks] {
        // Each round delivers more than one directory's children (7 here) before it is cancelled:
        // a resumed traversal lists its directories again whole, so a round cancelled before it
        // finishes any would start over where the last one did (resuming is not exact, §9.2).
        for (inflight, buffered, cancel_after) in [(1, 1, 15), (4, 1, 20), (8, 16, 40)] {
            let mut setup = setup(Tree::generate(3, 3, 4, 3), false);
            setup.source = setup
                .source
                .output_capacity(NonZeroUsize::new(buffered).unwrap_or(NonZeroUsize::MIN));
            let full = setup.tree.all_entries();
            let case = format!("{order:?} c={inflight} b={buffered}");
            let make = || request(order, inflight).resumable(Resumable::on());
            let mut delivered = Vec::new();
            let mut from: Option<Checkpoint> = None;
            let mut rounds = 0;
            loop {
                rounds += 1;
                assert!(rounds < 200, "{case}: no progress");
                let request = from
                    .take()
                    .map_or_else(make, |checkpoint| make().resume_from(checkpoint));
                let (items, outcome) = run(&setup.source, request, Some(cancel_after)).await;
                delivered.extend(entry_paths(&items));
                if matches!(outcome, Ok(TraversalOutcome::Completed(_))) {
                    break;
                }
                assert_eq!(outcome, Ok(TraversalOutcome::Cancelled), "{case}");
                let Some(TraversalItem::Checkpoint(last)) = items.last() else {
                    panic!(
                        "{case}: the last item of a cancelled resumable traversal is no checkpoint"
                    )
                };
                assert_eq!(
                    checkpoints(&items).len(),
                    1,
                    "{case}: one checkpoint, the final one"
                );
                from = Some((**last).clone());
            }
            assert!(rounds > 2, "{case}: cancelled too late to test anything");
            assert_no_loss(&delivered, &[], &full, &case);
        }
    }
}

/// A traversal cancelled before it did anything hands back the checkpoint it resumed from.
#[tokio::test]
async fn cancelled_at_once_a_resumed_traversal_returns_its_checkpoint() {
    for order in [TraversalOrder::Unordered, TraversalOrder::Blocks] {
        let setup = setup(Tree::generate(3, 3, 2, 2), false);
        let (items, _) = run(
            &setup.source,
            request(order, 2).resumable(every_chance()),
            None,
        )
        .await;
        let saved = checkpoints(&items);
        let (_, middle) = saved
            .get(saved.len() / 2)
            .cloned()
            .unwrap_or_else(|| panic!("checkpoints"));
        let cancel = tokio_util::sync::CancellationToken::new();
        cancel.cancel();
        let resumed = request(order, 2)
            .resumable(Resumable::on())
            .resume_from(middle.clone())
            .cancel(cancel);
        let (items, outcome) = run(&setup.source, resumed, None).await;
        assert_eq!(outcome, Ok(TraversalOutcome::Cancelled));
        assert!(entry_paths(&items).is_empty(), "{order:?}");
        assert_eq!(
            checkpoints(&items)
                .into_iter()
                .map(|(_, c)| c)
                .collect::<Vec<_>>(),
            [middle],
            "{order:?}"
        );
    }
}

/// A session failure ends the traversal with its failure, after a final checkpoint; resumed once
/// the storage is back, nothing is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_terminal_failure_ends_with_a_checkpoint_to_resume_from() {
    for order in [TraversalOrder::Unordered, TraversalOrder::Blocks] {
        let setup = setup(Tree::generate(3, 3, 2, 2), false);
        let full = setup.tree.all_entries();
        setup.tree.break_at(Some("d1/d0"));
        let make = || request(order, 2).resumable(Resumable::on());
        let (items, outcome) = run(&setup.source, make(), None).await;
        assert!(
            matches!(outcome, Err(TraversalTerminalFailure::Session(_))),
            "{order:?}: {outcome:?}"
        );
        let Some(TraversalItem::Checkpoint(last)) = items.last() else {
            panic!("{order:?}: no final checkpoint after the failure")
        };
        setup.tree.break_at(None);
        let (resumed, outcome) =
            run(&setup.source, make().resume_from((**last).clone()), None).await;
        assert!(matches!(outcome, Ok(TraversalOutcome::Completed(_))));
        assert_no_loss(
            &entry_paths(&items),
            &entry_paths(&resumed),
            &full,
            &format!("{order:?}"),
        );
    }
}

/// A checkpoint of another traversal — another storage, root, order, filter or depth limit — or
/// no checkpoint at all, is refused before anything is listed; so is a filter without a key.
#[tokio::test]
async fn checkpoints_of_another_request_are_refused_before_any_io() {
    let setup = setup(Tree::generate(2, 2, 1, 2), false);
    let (items, _) = run(
        &setup.source,
        request(TraversalOrder::Unordered, 2).resumable(every_chance()),
        None,
    )
    .await;
    let (_, saved) = checkpoints(&items)
        .pop()
        .unwrap_or_else(|| panic!("a checkpoint"));
    let lists = setup.tree.lists();
    let refused = |outcome: &Result<TraversalOutcome, TraversalTerminalFailure>,
                   expected: ResumeRefusal,
                   case: &str| {
        assert_eq!(
            outcome,
            &Err(TraversalTerminalFailure::Resume(expected)),
            "{case}"
        );
        assert_eq!(setup.tree.lists(), lists, "{case}: listed before refusing");
    };
    let from = |request: TraversalRequest| request.resume_from(saved.clone());
    let cases = [
        (
            "root",
            from(rooted(request(TraversalOrder::Unordered, 2), "d0")),
        ),
        ("order", from(request(TraversalOrder::Blocks, 2))),
        (
            "depth",
            from(request(TraversalOrder::Unordered, 2).max_depth(NonZeroUsize::new(2))),
        ),
        (
            "filter",
            from(
                request(TraversalOrder::Unordered, 2)
                    .filter(Arc::new(PruneD1))
                    .observation_plan(
                        ObservationPlan::default().with_timestamps(ObservationMode::InlineOnly),
                    ),
            ),
        ),
    ];
    for (case, request) in cases {
        let (_, outcome) = run(&setup.source, request, None).await;
        refused(&outcome, ResumeRefusal::Mismatch, case);
    }
    let other = StorageTraversalSource::with_roles(
        Arc::clone(&setup.tree) as Arc<dyn Namespace>,
        Arc::new(FakeMetadata),
    )
    .identified(crate::storage::backends::local::test_identity("another"));
    let (_, outcome) = run(&other, from(request(TraversalOrder::Unordered, 2)), None).await;
    refused(&outcome, ResumeRefusal::Mismatch, "storage");
    let garbage = Checkpoint::from_bytes(bytes::Bytes::from_static(b"not a checkpoint"));
    let (_, outcome) = run(
        &setup.source,
        request(TraversalOrder::Unordered, 2).resume_from(garbage),
        None,
    )
    .await;
    refused(&outcome, ResumeRefusal::Malformed, "garbage");
    let keyless = request(TraversalOrder::Unordered, 2)
        .filter(Arc::new(super::filter::PathFilter {
            needs_modified: false,
        }))
        .resumable(Resumable::on());
    let (_, outcome) = run(&setup.source, keyless, None).await;
    refused(
        &outcome,
        ResumeRefusal::FilterNotResumable,
        "keyless filter",
    );
}
