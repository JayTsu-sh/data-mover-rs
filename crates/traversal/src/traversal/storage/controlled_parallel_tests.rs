//! A held file receipt must not serialize unrelated ready directory subtrees.
use std::collections::HashSet;

use super::*;
use crate::storage::LocalBackendConfig;
use crate::storage::backends::local::connect_backend;

async fn four_files_with_all_file_feedback_held(consumer: bool) {
    let root = tempfile::tempdir().assert_value("parallel Local fixture");
    for index in 0..8 {
        let directory = root.path().join(format!("dir-{index}"));
        std::fs::create_dir(&directory).assert_value("parallel Local fixture");
        std::fs::write(directory.join("kept"), b"real Local body")
            .assert_value("parallel Local fixture");
    }
    let storage = connect_backend(LocalBackendConfig {
        root: root.path().to_path_buf(),
        read_concurrency: NonZeroUsize::MIN,
        write_concurrency: NonZeroUsize::MIN,
        read_chunk_bytes: None,
        write_chunk_bytes: None,
        rename_context: None,
    })
    .assert_value("parallel Local connection");
    let source = StorageTraversalSource::new(&storage)
        .assert_value("parallel Local source")
        .output_capacity(NonZeroUsize::new(4).assert_value("parallel Local capacity"));
    let mut request = configured();
    request.max_inflight_operations = NonZeroUsize::new(2).assert_value("parallel Local scan");
    request.consumer_max_inflight = NonZeroUsize::new(4).assert_value("parallel Local window");
    if !consumer {
        request.consumer_completion_budget = None;
    }
    let mut session = source.traverse(request);
    let mut feedback = Vec::new();
    let mut held = Vec::new();
    let mut files = HashSet::new();
    let run = async {
        while files.len() < 4 {
            let item = session
                .next_item(&mut feedback)
                .await
                .assert_value("parallel Local work");
            match item {
                TraversalItem::ConsumerEntry(work) => {
                    if work.1.kind() == EntryKind::Directory {
                        feedback.push((work.0, true));
                    } else {
                        assert_eq!(
                            std::fs::read(root.path().join(work.1.path().as_str()))
                                .assert_value("actual Local body"),
                            b"real Local body"
                        );
                        files.insert(work.1.path().as_str().to_owned());
                        held.push(work.0);
                    }
                }
                TraversalItem::Entry(entry) if !consumer => {
                    if entry.kind() == EntryKind::File {
                        assert_eq!(
                            std::fs::read(root.path().join(entry.path().as_str()))
                                .assert_value("actual Local body"),
                            b"real Local body"
                        );
                        files.insert(entry.path().as_str().to_owned());
                    }
                }
                TraversalItem::SubtreeReady(_) => {
                    panic!("no subtree can finalize before its held file receipt")
                }
                other => panic!("unexpected parallel work: {other:?}"),
            }
        }
    };
    let observed = tokio::time::timeout(Duration::from_secs(3), run).await;
    // A deadlock guard, not a throughput or sub-millisecond performance assertion.
    assert!(
        observed.is_ok(),
        "only {} directory file(s) issued with all file feedback held; independent subtrees must reach W=4",
        files.len()
    );
    assert_eq!(files.len(), 4);
    feedback.extend(held.into_iter().map(|id| (id, true)));
    while let Some(item) = session.next_item(&mut feedback).await {
        match item {
            TraversalItem::ConsumerEntry(work) | TraversalItem::SubtreeReady(work) => {
                feedback.push((work.0, true));
            }
            TraversalItem::Entry(_) if !consumer => {}
            other => panic!("unexpected parallel drain {other:?}"),
        }
    }
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Completed(_))
    ));
}

#[tokio::test]
async fn held_file_ack_does_not_block_four_independent_subtrees() {
    four_files_with_all_file_feedback_held(true).await;
}

#[tokio::test]
async fn ordinary_none_same_capacity_baseline_reaches_four_directory_files() {
    four_files_with_all_file_feedback_held(false).await;
}

#[tokio::test]
async fn one_phase_window_and_prepare_receipt_gate_descendant_work() {
    let root = tempfile::tempdir().assert_value("parallel gate fixture");
    std::fs::create_dir(root.path().join("dir")).assert_value("parallel gate fixture");
    std::fs::write(root.path().join("dir/kept"), b"kept").assert_value("parallel gate fixture");
    let (storage, namespace, metadata) =
        test_traversal_storage(root.path(), "parallel-gates").assert_value("parallel gate fixture");
    let namespace = Arc::new(ReadOnlyNamespace::new(namespace));
    let source = StorageTraversalSource::with_roles(namespace.clone(), metadata)
        .identified(storage.identity().clone())
        .output_capacity(NonZeroUsize::MIN);
    let mut request = configured();
    request.consumer_max_inflight = NonZeroUsize::MIN;
    let mut session = source.traverse(request);
    let mut feedback = Vec::new();
    let Some(TraversalItem::ConsumerEntry(prepare)) = session.next_item(&mut feedback).await else {
        panic!("missing prepare")
    };
    assert_eq!(prepare.1.kind(), EntryKind::Directory);
    assert!(session.next_item(&mut feedback).now_or_never().is_none());
    assert_eq!(
        &*namespace.calls.lock(),
        &[String::new()],
        "unacknowledged prepare cannot open its child listing"
    );
    feedback.push((prepare.0, true));
    let Some(TraversalItem::ConsumerEntry(file)) = session.next_item(&mut feedback).await else {
        panic!("missing file")
    };
    assert_eq!(file.1.kind(), EntryKind::File);
    assert!(
        session.next_item(&mut feedback).now_or_never().is_none(),
        "W1 must issue no second stage"
    );
    feedback.push((file.0, true));
    let Some(TraversalItem::SubtreeReady(finalize)) = session.next_item(&mut feedback).await else {
        panic!("missing finalize")
    };
    assert!(
        session.next_item(&mut feedback).now_or_never().is_none(),
        "EOF cannot complete before final ACK"
    );
    feedback.push((finalize.0, true));
    assert!(session.next_item(&mut feedback).await.is_none());
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Completed(_))
    ));
}

#[derive(Debug)]
struct HideHold;
impl TraversalFilter for HideHold {
    fn required_metadata(&self) -> MetadataRequirements {
        MetadataRequirements::default()
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        TraversalDecision {
            emit: candidate.name != "hold",
            descend: candidate.kind == EntryKind::Directory,
            filter_children: true,
        }
    }
}

#[tokio::test]
async fn parent_finalization_waits_for_held_file_and_child_finalize_including_hidden_topology() {
    for (hide, hold_finalize) in [(false, false), (false, true), (true, false)] {
        let root = tempfile::tempdir().assert_value("parallel parent fixture");
        for name in ["hold", "a", "b", "c"] {
            std::fs::create_dir_all(root.path().join("parent").join(name))
                .assert_value("parallel parent fixture");
            std::fs::write(root.path().join("parent").join(name).join("kept"), b"kept")
                .assert_value("parallel parent fixture");
        }
        let (storage, _, _) = test_traversal_storage(root.path(), "parallel-parent")
            .assert_value("parallel parent fixture");
        let source = StorageTraversalSource::new(&storage)
            .assert_value("parallel parent fixture")
            .output_capacity(NonZeroUsize::MIN);
        let mut request = configured();
        if hide {
            request = request.filter(Arc::new(HideHold));
        }
        let mut session = source.traverse(request);
        let mut feedback = Vec::new();
        let mut held = None;
        let mut others = HashSet::new();
        let run = async {
            while others.len() < 3 || held.is_none() {
                match session
                    .next_item(&mut feedback)
                    .await
                    .assert_value("parallel parent progress")
                {
                    TraversalItem::ConsumerEntry(work) => {
                        if !hold_finalize && work.1.path().as_str() == "parent/hold/kept" {
                            held = Some(work.0);
                        } else {
                            feedback.push((work.0, true));
                        }
                    }
                    TraversalItem::SubtreeReady(work) => {
                        let path = work.1.path().as_str();
                        assert_ne!(
                            path, "parent",
                            "ancestor finalized before selected descendant result"
                        );
                        if hold_finalize && path == "parent/hold" {
                            held = Some(work.0);
                        } else {
                            others.insert(path.to_owned());
                            feedback.push((work.0, true));
                        }
                    }
                    other => panic!("unexpected parent work {other:?}"),
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(3), run)
            .await
            .assert_value("other siblings progress while one result is held");
        assert!(
            session.next_item(&mut feedback).now_or_never().is_none(),
            "parent must remain dependent on the held descendant"
        );
        feedback.push((held.assert_value("held descendant"), true));
        let mut parent = false;
        let mut child = hold_finalize || hide;
        while let Some(item) = session.next_item(&mut feedback).await {
            match item {
                TraversalItem::ConsumerEntry(work) => feedback.push((work.0, true)),
                TraversalItem::SubtreeReady(work) => {
                    if work.1.path().as_str() == "parent/hold" {
                        child = true;
                    }
                    if work.1.path().as_str() == "parent" {
                        assert!(child);
                        parent = true;
                    }
                    feedback.push((work.0, true));
                }
                other => panic!("unexpected parent drain {other:?}"),
            }
        }
        assert!(parent);
        assert!(matches!(
            session.finish().await,
            Ok(TraversalOutcome::Completed(_))
        ));
    }
}

struct FailedFamily;
#[async_trait]
impl Metadata for FailedFamily {
    async fn observe(
        &self,
        _path: &StoragePath,
        _plan: crate::model::ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        tokio::task::yield_now().await;
        Ok(MetadataObservationParts {
            acl: MetadataObservation::Failed {
                class: FailureClass::PermissionDenied,
                transience: Transience::Permanent,
            },
            ..MetadataObservationParts::default()
        }
        .build()
        .assert_value("parallel metadata fact"))
    }
    async fn apply(
        &self,
        _path: &StoragePath,
        _mutation: MetadataMutation,
        _cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        panic!("read-only metadata fixture")
    }
}

#[tokio::test]
async fn sibling_cross_page_metadata_fact_and_entry_bundles_remain_adjacent_with_output_one() {
    let root = tempfile::tempdir().assert_value("parallel bundle fixture");
    for directory in 0..4 {
        let dir = root.path().join(format!("dir-{directory}"));
        std::fs::create_dir(&dir).assert_value("parallel bundle fixture");
        for file in 0..9 {
            std::fs::write(dir.join(format!("file-{file}")), b"kept")
                .assert_value("parallel bundle fixture");
        }
    }
    let (storage, namespace, _) = test_traversal_storage(root.path(), "parallel-bundles")
        .assert_value("parallel bundle fixture");
    let source = StorageTraversalSource::with_roles(namespace, Arc::new(FailedFamily))
        .identified(storage.identity().clone())
        .output_capacity(NonZeroUsize::MIN);
    let mut request = configured();
    request.max_inflight_operations = NonZeroUsize::new(2).assert_value("parallel scan slots");
    request.observation_plan = request
        .observation_plan
        .with_acl(ObservationMode::BestEffort);
    let mut session = source.traverse(request);
    let mut feedback = Vec::new();
    let mut prefix = None;
    let mut entries = HashSet::new();
    let mut facts = HashSet::new();
    let mut finalizes = 0;
    while let Some(item) = session.next_item(&mut feedback).await {
        match item {
            TraversalItem::ObservationFailure(failure) => {
                let path = failure.path.as_str().to_owned();
                assert!(
                    prefix.is_none(),
                    "another child fact arrived before its predecessor's entry"
                );
                assert!(facts.insert(path.clone()));
                prefix = Some(path);
            }
            TraversalItem::ConsumerEntry(work) => {
                assert_eq!(prefix.take().as_deref(), Some(work.1.path().as_str()));
                assert!(entries.insert(work.1.path().as_str().to_owned()));
                feedback.push((work.0, true));
            }
            TraversalItem::SubtreeReady(work) => {
                assert!(prefix.is_none());
                assert!(matches!(
                    work.1.metadata().acl(),
                    MetadataObservation::Failed { .. }
                ));
                finalizes += 1;
                feedback.push((work.0, true));
            }
            other => panic!("unexpected bundle output {other:?}"),
        }
    }
    assert_eq!(entries.len(), 40);
    assert_eq!(facts, entries);
    assert_eq!(finalizes, 4);
    assert!(prefix.is_none());
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Completed(_))
    ));
}

#[tokio::test]
async fn reverse_order_file_feedback_reuses_window_with_capacity_one() {
    let root = tempfile::tempdir().assert_value("parallel reverse fixture");
    for directory in 0..8 {
        let path = root.path().join(format!("dir-{directory}"));
        std::fs::create_dir(&path).assert_value("parallel reverse fixture");
        for file in 0..8 {
            std::fs::write(path.join(format!("file-{file}")), b"kept")
                .assert_value("parallel reverse fixture");
        }
    }
    let (storage, _, _) = test_traversal_storage(root.path(), "parallel-reverse")
        .assert_value("parallel reverse fixture");
    let source = StorageTraversalSource::new(&storage)
        .assert_value("parallel reverse fixture")
        .output_capacity(NonZeroUsize::MIN);
    let run = async {
        let mut session = source.traverse(configured());
        let mut feedback = Vec::new();
        let mut held = Vec::new();
        let mut files = HashSet::new();
        let mut finalizes = 0;
        while let Some(item) = session.next_item(&mut feedback).await {
            match item {
                TraversalItem::ConsumerEntry(work) => {
                    if work.1.kind() == EntryKind::File {
                        assert!(files.insert(work.1.path().as_str().to_owned()));
                        held.push(work.0);
                        if held.len() == 4 {
                            feedback.extend(held.drain(..).rev().map(|id| (id, true)));
                        }
                    } else {
                        // Only the still held file phases and this prepare occupy W.
                        assert!(held.len() < 4);
                        feedback.push((work.0, true));
                    }
                }
                TraversalItem::SubtreeReady(work) => {
                    finalizes += 1;
                    feedback.push((work.0, true));
                }
                other => panic!("unexpected reverse output {other:?}"),
            }
        }
        assert_eq!(files.len(), 64);
        assert_eq!(finalizes, 8);
        assert!(held.is_empty());
        assert!(matches!(
            session.finish().await,
            Ok(TraversalOutcome::Completed(_))
        ));
    };
    tokio::time::timeout(Duration::from_secs(3), run)
        .await
        .assert_value("reverse feedback cannot deadlock the bounded actor");
}

#[tokio::test]
async fn cancel_mid_fact_bundle_drains_known_facts_once_without_issuing_entries() {
    let root = tempfile::tempdir().assert_value("parallel cutoff fixture");
    for index in 0..8 {
        std::fs::write(root.path().join(format!("file-{index}")), b"kept")
            .assert_value("parallel cutoff fixture");
    }
    let (storage, namespace, _) = test_traversal_storage(root.path(), "parallel-cutoff")
        .assert_value("parallel cutoff fixture");
    let source = StorageTraversalSource::with_roles(namespace, Arc::new(FailedFamily))
        .identified(storage.identity().clone())
        .output_capacity(NonZeroUsize::MIN);
    let mut request = configured();
    request.observation_plan = request
        .observation_plan
        .with_acl(ObservationMode::BestEffort);
    let cancel = request.cancel.clone();
    let mut session = source.traverse(request);
    let mut feedback = Vec::new();
    let Some(TraversalItem::ObservationFailure(first)) = session.next_item(&mut feedback).await
    else {
        panic!("missing bundle fact")
    };
    // The first acquired page is registered before publishing any of its child bundles.
    cancel.cancel();
    let mut facts = HashSet::from([first.path.as_str().to_owned()]);
    while let Some(item) = session.next_item(&mut feedback).await {
        let TraversalItem::ObservationFailure(failure) = item else {
            panic!("cancel cannot issue the following child work: {item:?}")
        };
        assert!(facts.insert(failure.path.as_str().to_owned()));
    }
    assert_eq!(
        facts.len(),
        4,
        "only the admitted W-sized page was observed"
    );
    assert_eq!(session.finish().await, Ok(TraversalOutcome::Cancelled));
}

#[tokio::test]
async fn false_file_result_stops_parallel_siblings_without_ancestor_finalization() {
    let root = tempfile::tempdir().assert_value("parallel false fixture");
    for index in 0..8 {
        let path = root.path().join(format!("dir-{index}"));
        std::fs::create_dir(&path).assert_value("parallel false fixture");
        std::fs::write(path.join("kept"), b"kept").assert_value("parallel false fixture");
    }
    let (storage, _, _) = test_traversal_storage(root.path(), "parallel-false")
        .assert_value("parallel false fixture");
    let source = StorageTraversalSource::new(&storage)
        .assert_value("parallel false fixture")
        .output_capacity(NonZeroUsize::MIN);
    let mut session = source.traverse(configured());
    let mut feedback = Vec::new();
    let mut held = Vec::new();
    while held.len() < 4 {
        let Some(TraversalItem::ConsumerEntry(work)) = session.next_item(&mut feedback).await
        else {
            panic!("expected four concurrent child phases")
        };
        if work.1.kind() == EntryKind::Directory {
            feedback.push((work.0, true));
        } else {
            held.push(work.0);
        }
    }
    feedback.extend(held.into_iter().rev().map(|id| (id, false)));
    assert!(session.next_item(&mut feedback).await.is_none());
    assert_eq!(
        session.finish().await,
        Err(TraversalTerminalFailure::ConsumerFailed)
    );
}

#[tokio::test]
async fn reused_arena_slot_rejects_late_phase_and_page_and_deduplicates_ready_work() {
    let root = tempfile::tempdir().assert_value("parallel arena fixture");
    let (storage, _, _) = test_traversal_storage(root.path(), "parallel-arena")
        .assert_value("parallel arena fixture");
    let source = StorageTraversalSource::new(&storage).assert_value("parallel arena fixture");
    let (mut runner, _session) = resources::manual_runner(&source, configured());
    runner.root().await.assert_value("parallel arena root");
    let old = runner.nodes.next().assert_value("old arena key");
    let old_node = runner
        .nodes
        .remove(old)
        .assert_value("old arena slot")
        .node
        .assert_value("old arena node");
    let node = runner
        .node(root_work(&configured()), None, None)
        .assert_value("new arena node");
    let current = runner
        .nodes
        .insert(None, node, Stage::Children)
        .assert_value("new arena key");
    assert_ne!(old, current);
    runner
        .nodes
        .get_mut(current)
        .assert_value("current slot")
        .files = 1;
    for success in [true, false] {
        runner
            .acknowledged((old, Phase::File, Ok(success)))
            .assert_value("ignore stale result");
    }
    runner
        .read(super::super::io::Read {
            key: old,
            node: old_node,
            eof: true,
            result: Err(TraversalTerminalFailure::ConsumerFailed),
        })
        .assert_value("ignore stale page");
    assert_eq!(
        runner.nodes.get(current).assert_value("current slot").files,
        1
    );
    for _ in 0..100 {
        runner.nodes.queue(current, false);
    }
    assert_eq!(runner.nodes.ready_len(), 1);
    runner
        .acknowledged((current, Phase::File, Ok(true)))
        .assert_value("current result");
    assert_eq!(
        runner.nodes.get(current).assert_value("current slot").files,
        0
    );
    assert_eq!(runner.nodes.ready_len(), 1);
}

struct MeasuredListings {
    inner: Arc<dyn Namespace>,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}
struct ListingActive(Arc<AtomicUsize>);
impl Drop for ListingActive {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl Namespace for MeasuredListings {
    fn capabilities(&self) -> NamespaceCapabilities {
        self.inner.capabilities()
    }
    fn parked_listing_limit(&self) -> Option<NonZeroUsize> {
        NonZeroUsize::new(2)
    }
    async fn stat(
        &self,
        path: &StoragePath,
        fresh: bool,
    ) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.inner.stat(path, fresh).await
    }
    async fn read_link(&self, path: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        self.inner.read_link(path).await
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        let inner = self.inner.clone();
        let active = self.active.clone();
        let peak = self.peak.clone();
        ListingStream::from_producer(request.path.clone(), move |sink| async move {
            let count = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(count, Ordering::SeqCst);
            let _active = ListingActive(active);
            let mut stream = inner.list(request);
            while let Some(page) = stream.next().await {
                match page {
                    Ok(page) => {
                        if sink.send(page).await.is_err() {
                            return;
                        }
                    }
                    Err(failure) => {
                        sink.fail(failure).await;
                        return;
                    }
                }
            }
        })
    }
}

#[tokio::test]
async fn namespace_two_listing_limit_parks_admission_and_reuses_finished_producers() {
    let root = tempfile::tempdir().assert_value("parallel listing fixture");
    for index in 0..8 {
        let path = root.path().join(format!("dir-{index}"));
        std::fs::create_dir(&path).assert_value("parallel listing fixture");
        for file in 0..8 {
            std::fs::write(path.join(format!("file-{file}")), b"kept")
                .assert_value("parallel listing fixture");
        }
    }
    let (storage, namespace, metadata) = test_traversal_storage(root.path(), "parallel-listing")
        .assert_value("parallel listing fixture");
    let measured = Arc::new(MeasuredListings {
        inner: namespace,
        active: Arc::default(),
        peak: Arc::default(),
    });
    let source = StorageTraversalSource::with_roles(measured.clone(), metadata)
        .identified(storage.identity().clone())
        .output_capacity(NonZeroUsize::MIN);
    let (items, terminal) = tokio::time::timeout(
        Duration::from_secs(3),
        acknowledge(source.traverse(configured())),
    )
    .await
    .assert_value("bounded native listing admission");
    assert!(
        matches!(terminal, Ok(TraversalOutcome::Completed(_))),
        "{terminal:?}"
    );
    assert_eq!(
        items
            .iter()
            .filter(|item| matches!(item, TraversalItem::SubtreeReady(_)))
            .count(),
        8
    );
    assert_eq!(measured.active.load(Ordering::SeqCst), 0);
    let peak = measured.peak.load(Ordering::SeqCst);
    assert!(
        (1..=2).contains(&peak),
        "actual owned listing producers peak={peak}; this is not an all-RPC I/O measurement"
    );
}

#[tokio::test]
async fn ten_thousand_reused_subtrees_retain_concurrent_high_water_not_scan_history() {
    let root = tempfile::tempdir().assert_value("parallel reuse fixture");
    let (storage, _, _) = test_traversal_storage(root.path(), "parallel-reuse")
        .assert_value("parallel reuse fixture");
    let source = StorageTraversalSource::new(&storage).assert_value("parallel reuse fixture");
    let request = configured();
    let (mut runner, _session) = resources::manual_runner(&source, request.clone());
    runner.root().await.assert_value("reuse root");
    let parent = runner.nodes.next().assert_value("reuse parent");
    let parent_live = runner.nodes.get_mut(parent).assert_value("reuse parent");
    parent_live.eof = true;
    parent_live.stage = Stage::Children;
    let mut previous = None;
    let mut retained = None;
    let mut completed = 0;
    for round in 0..2500 {
        let mut children = Vec::new();
        for _ in 0..4 {
            let node = runner
                .node(root_work(&request), None, None)
                .assert_value("reused child");
            let key = runner
                .nodes
                .insert(Some(parent), node, Stage::Children)
                .assert_value("reused slot");
            runner.nodes.get_mut(key).assert_value("reused child").eof = true;
            for _ in 0..8 {
                runner.nodes.queue(key, false);
            }
            children.push(key);
        }
        assert_eq!(
            runner.nodes.len(),
            5,
            "one parent and four simultaneous children"
        );
        assert_eq!(runner.nodes.ready_len(), 4);
        if let Some(old) = previous {
            runner
                .acknowledged((old, Phase::File, Ok(false)))
                .assert_value("late previous-generation failure");
            assert!(runner.nodes.get(old).is_none());
        }
        for key in children {
            assert_eq!(runner.nodes.next(), Some(key));
            runner.complete(key).assert_value("subtree completion");
            completed += 1;
            previous = Some(key);
            // Parent completion is not part of this batch; remove its queued opportunity.
            assert_eq!(runner.nodes.next(), Some(parent));
        }
        assert_eq!(runner.nodes.len(), 1);
        assert_eq!(runner.nodes.ready_len(), 0);
        let capacity = runner.nodes.retained_capacity();
        if round == 0 {
            retained = Some(capacity);
        }
        assert_eq!(
            Some(capacity),
            retained,
            "slot/ready/free capacity must remain at concurrent high water"
        );
        assert_eq!(
            runner.resources.held(),
            Runner::node_bytes(&root_work(&request), None)
        );
    }
    assert_eq!(completed, 10_000);
}
