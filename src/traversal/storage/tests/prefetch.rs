//! Concurrent listing prefetch: determinism, budgets and parallelism, on a virtual clock.

use super::filter::RecordingFilter;
use super::*;

/// A namespace over an explicit tree whose listings take a configurable (virtual) time, and
/// which records how many listings ran at once.
struct LatencyNamespace {
    tree: std::collections::HashMap<String, Vec<(String, EntryKind)>>,
    delay_ms: Box<dyn Fn(&str) -> u64 + Send + Sync>,
    inflight: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
    finished: std::sync::atomic::AtomicUsize,
    gate: Option<tokio::sync::Semaphore>,
}

impl LatencyNamespace {
    fn new(
        tree: Vec<(&str, Vec<(&str, EntryKind)>)>,
        delay_ms: impl Fn(&str) -> u64 + Send + Sync + 'static,
    ) -> Self {
        Self {
            tree: tree
                .into_iter()
                .map(|(parent, children)| {
                    let children = children
                        .into_iter()
                        .map(|(name, kind)| (name.to_owned(), kind))
                        .collect();
                    (parent.to_owned(), children)
                })
                .collect(),
            delay_ms: Box::new(delay_ms),
            inflight: std::sync::atomic::AtomicUsize::new(0),
            peak: std::sync::atomic::AtomicUsize::new(0),
            finished: std::sync::atomic::AtomicUsize::new(0),
            gate: None,
        }
    }

    fn peak(&self) -> usize {
        self.peak.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait]
impl Namespace for LatencyNamespace {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        use std::sync::atomic::Ordering::SeqCst;
        let NamespaceRequest::List(directory) = request else {
            unreachable!("only List is issued")
        };
        let now = self.inflight.fetch_add(1, SeqCst) + 1;
        self.peak.fetch_max(now, SeqCst);
        if let Some(gate) = &self.gate
            && !directory.as_str().is_empty()
        {
            let _permit = gate.acquire().await;
        }
        tokio::time::sleep(std::time::Duration::from_millis((self.delay_ms)(
            directory.as_str(),
        )))
        .await;
        self.inflight.fetch_sub(1, SeqCst);
        self.finished.fetch_add(1, SeqCst);
        let children = self
            .tree
            .get(directory.as_str())
            .map(|children| {
                children
                    .iter()
                    .map(|(name, kind)| descriptor(name, *kind))
                    .collect()
            })
            .unwrap_or_default();
        Ok(NamespaceResult::Entries(children))
    }
}

fn latency_request(inflight: usize) -> TraversalRequest {
    let mut request = request(tokio_util::sync::CancellationToken::new());
    request.observation_plan = ObservationPlan::default();
    request.max_inflight_operations =
        NonZeroUsize::new(inflight).unwrap_or_else(|| unreachable!("test limit is nonzero"));
    request.max_buffered_items =
        NonZeroUsize::new(4).unwrap_or_else(|| unreachable!("constant is nonzero"));
    request
}

/// `d0..d3`, each with `s0, s1` (directories) and a file `f`; each `s*` holds one file `leaf`.
fn two_level_tree() -> Vec<(String, Vec<(String, EntryKind)>)> {
    let mut tree = vec![(
        String::new(),
        (0..4)
            .map(|index| (format!("d{index}"), EntryKind::Directory))
            .collect(),
    )];
    for index in 0..4 {
        let parent = format!("d{index}");
        tree.push((
            parent.clone(),
            vec![
                (format!("{parent}/s0"), EntryKind::Directory),
                (format!("{parent}/s1"), EntryKind::Directory),
                (format!("{parent}/f"), EntryKind::File),
            ],
        ));
        for sub in 0..2 {
            let child = format!("{parent}/s{sub}");
            tree.push((
                child.clone(),
                vec![(format!("{child}/leaf"), EntryKind::File)],
            ));
        }
    }
    tree
}

fn latency_namespace(
    tree: &[(String, Vec<(String, EntryKind)>)],
    delay_ms: impl Fn(&str) -> u64 + Send + Sync + 'static,
) -> LatencyNamespace {
    LatencyNamespace::new(
        tree.iter()
            .map(|(parent, children)| {
                (
                    parent.as_str(),
                    children
                        .iter()
                        .map(|(name, kind)| (name.as_str(), *kind))
                        .collect(),
                )
            })
            .collect(),
        delay_ms,
    )
}

#[tokio::test(start_paused = true)]
async fn dfs_order_does_not_depend_on_the_order_listings_finish_in() {
    let tree = two_level_tree();
    let rank = |path: &str| -> u64 {
        path.bytes()
            .filter(u8::is_ascii_digit)
            .map(|digit| u64::from(digit - b'0'))
            .sum()
    };
    let mut expected: Vec<String> = (0..4).map(|index| format!("d{index}")).collect();
    for index in 0..4 {
        for name in ["s0", "s1", "f"] {
            expected.push(format!("d{index}/{name}"));
        }
        for sub in 0..2 {
            expected.push(format!("d{index}/s{sub}/leaf"));
        }
    }
    // Later siblings finish first in one run and last in the other.
    for reversed in [false, true] {
        let namespace = latency_namespace(&tree, move |path| {
            let value = rank(path) * 7 + u64::try_from(path.len()).unwrap_or(0);
            if reversed {
                100 - value % 100
            } else {
                value % 100
            }
        });
        let source =
            StorageTraversalSource::with_roles(Arc::new(namespace), Arc::new(FakeMetadata));
        let (paths, _) = collect(source.traverse(latency_request(8))).await;
        assert_eq!(paths, expected, "reversed = {reversed}");
    }
}

#[tokio::test(start_paused = true)]
async fn concurrent_listings_stay_within_the_listing_budget() {
    for (inflight, bound) in [(8, 9), (200, 65)] {
        let children: Vec<(String, EntryKind)> = (0..300)
            .map(|index| (format!("dir-{index:03}"), EntryKind::Directory))
            .collect();
        let tree = vec![(String::new(), children)];
        let namespace = Arc::new(latency_namespace(&tree, |_| 10));
        let source = StorageTraversalSource::with_roles(
            Arc::clone(&namespace) as Arc<dyn Namespace>,
            Arc::new(FakeMetadata),
        );
        let (paths, _) = collect(source.traverse(latency_request(inflight))).await;
        assert_eq!(paths.len(), 300);
        // The budget, plus the one listing the cursor needs that is always allowed to start.
        assert!(
            namespace.peak() <= bound,
            "peak {} > {bound}",
            namespace.peak()
        );
        assert!(namespace.peak() >= 2, "listings never overlapped");
    }
}

#[tokio::test(start_paused = true)]
async fn sibling_chains_are_listed_in_parallel_while_the_cursor_is_deep() {
    let mut tree = vec![(
        String::new(),
        (0..4)
            .map(|index| (format!("c{index}"), EntryKind::Directory))
            .collect::<Vec<_>>(),
    )];
    for chain in 0..4 {
        let mut parent = format!("c{chain}");
        for _ in 0..5 {
            let child = format!("{parent}/n");
            tree.push((parent.clone(), vec![(child.clone(), EntryKind::Directory)]));
            parent = child;
        }
    }
    let namespace = Arc::new(latency_namespace(&tree, |_| 10));
    let source = StorageTraversalSource::with_roles(
        Arc::clone(&namespace) as Arc<dyn Namespace>,
        Arc::new(FakeMetadata),
    );
    let (paths, _) = collect(source.traverse(latency_request(8))).await;
    assert_eq!(paths.len(), 4 + 4 * 5);
    // A single chain is inherently serial, but the pending siblings further down the stack are
    // prefetched while the cursor is deep in the first chain.
    assert!(namespace.peak() >= 2, "peak {}", namespace.peak());
}

#[tokio::test]
async fn cancellation_lets_inflight_listings_finish() {
    let children: Vec<(String, EntryKind)> = (0..8)
        .map(|index| (format!("dir-{index}"), EntryKind::Directory))
        .collect();
    let tree = vec![(String::new(), children)];
    let mut namespace = latency_namespace(&tree, |_| 0);
    namespace.gate = Some(tokio::sync::Semaphore::new(0));
    let namespace = Arc::new(namespace);
    let cancel = tokio_util::sync::CancellationToken::new();
    let mut request = latency_request(4);
    request.cancel = cancel.clone();
    let source = StorageTraversalSource::with_roles(
        Arc::clone(&namespace) as Arc<dyn Namespace>,
        Arc::new(FakeMetadata),
    );
    let mut session = source.traverse(request);
    let started = || namespace.inflight.load(std::sync::atomic::Ordering::SeqCst);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while started() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("subdirectory listings never started"));

    cancel.cancel();
    while session.next_item().await.is_some() {}
    assert_eq!(session.finish().await, Ok(TraversalOutcome::Cancelled));

    let blocked = started();
    let finished_before = namespace.finished.load(std::sync::atomic::Ordering::SeqCst);
    if let Some(gate) = &namespace.gate {
        gate.add_permits(64);
    }
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while namespace.finished.load(std::sync::atomic::Ordering::SeqCst)
            < finished_before + blocked
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("detached listings never finished"));
}

/// `top` directories at the root, each holding `sub` empty subdirectories.
fn fan_tree(top: usize, sub: usize) -> Vec<(String, Vec<(String, EntryKind)>)> {
    let mut tree = vec![(
        String::new(),
        (0..top)
            .map(|index| (format!("d{index:02}"), EntryKind::Directory))
            .collect(),
    )];
    for index in 0..top {
        let parent = format!("d{index:02}");
        let children = (0..sub)
            .map(|child| (format!("{parent}/s{child:02}"), EntryKind::Directory))
            .collect();
        tree.push((parent, children));
    }
    tree
}

/// Runs a traversal of `tree` with 10 ms listings and reports (paths, peak, virtual elapsed).
async fn timed(
    tree: &[(String, Vec<(String, EntryKind)>)],
    request: TraversalRequest,
) -> (Vec<String>, usize, std::time::Duration) {
    let namespace = Arc::new(latency_namespace(tree, |_| 10));
    let source = StorageTraversalSource::with_roles(
        Arc::clone(&namespace) as Arc<dyn Namespace>,
        Arc::new(FakeMetadata),
    );
    let started = tokio::time::Instant::now();
    let (paths, _) = collect(source.traverse(request)).await;
    (paths, namespace.peak(), started.elapsed())
}

/// Listings that already finished for shallow siblings must not keep the budget from the
/// deeper listings the cursor needs first: a two-level tree should take about as long as a
/// flat one with the same number of listings.
#[tokio::test(start_paused = true)]
async fn prefetched_shallow_results_do_not_starve_deeper_listings() {
    let (two_level, _, nested) = timed(&fan_tree(10, 20), latency_request(8)).await;
    let (flat, _, single) = timed(&fan_tree(210, 0), latency_request(8)).await;
    assert_eq!(two_level.len(), 210);
    assert_eq!(flat.len(), 210);
    assert!(
        nested.as_millis() * 10 <= single.as_millis() * 13,
        "two-level {nested:?} vs flat {single:?}"
    );
}

/// A deferred (`modified`) filter decides descent only after observing, but once decided the
/// subdirectories must be prefetched like any other, with the same output.
#[tokio::test(start_paused = true)]
async fn deferred_decisions_are_prefetched_once_they_settle() {
    let tree = fan_tree(64, 0);
    let mut deferred = latency_request(8);
    deferred.filter = Some(Arc::new(RecordingFilter::new(true)));
    let mut immediate = latency_request(8);
    immediate.filter = Some(Arc::new(RecordingFilter::new(false)));
    let (deferred_paths, peak, deferred_time) = timed(&tree, deferred).await;
    let (immediate_paths, _, immediate_time) = timed(&tree, immediate).await;
    assert_eq!(deferred_paths, immediate_paths);
    assert!(peak >= 4, "deferred listings never overlapped: peak {peak}");
    assert!(
        deferred_time.as_millis() * 10 <= immediate_time.as_millis() * 15,
        "deferred {deferred_time:?} vs immediate {immediate_time:?}"
    );
}

/// Many directories with a few subdirectories each: the subdirectories of a directory that was
/// prefetched but not yet reached must be prefetched too, or the second level lists one at a
/// time as the cursor walks across.
#[tokio::test(start_paused = true)]
async fn subdirectories_of_prefetched_directories_are_prefetched_too() {
    let nested_tree = fan_tree(64, 1);
    let (nested, peak, nested_time) = timed(&nested_tree, latency_request(8)).await;
    // The same 129 listings, all at one level.
    let (_, _, flat_time) = timed(&fan_tree(128, 0), latency_request(8)).await;
    // Depth-first blocks: the root block, then each `d` block (its one child) in turn.
    let mut blocks: Vec<String> = (0..64).map(|index| format!("d{index:02}")).collect();
    blocks.extend((0..64).map(|index| format!("d{index:02}/s00")));
    assert_eq!(nested, blocks);
    assert!(peak >= 4, "peak {peak}");
    assert!(
        nested_time.as_millis() * 10 <= flat_time.as_millis() * 15,
        "nested {nested_time:?} vs flat {flat_time:?}"
    );
}
