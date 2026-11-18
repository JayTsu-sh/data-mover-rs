use crate::AssertTestValue;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::{
    MetadataObservation, MetadataObservationParts, MetadataObservations, MetadataRequirements,
    ObservationMode, Operation, StoragePath, SymlinkTarget, Transience,
};
use crate::storage::backends::local::test_traversal_storage;
use crate::storage::{
    Metadata, MetadataMutation, Namespace, NamespaceCapabilities, SourceDescriptor,
};
use crate::traversal::{
    TraversalCandidate, TraversalDecision, TraversalFilter, TraversalOutcome, TraversalSource,
};

fn configured() -> TraversalRequest {
    let mut request = TraversalRequest::new(StoragePath::root());
    request.consumer_completion_budget = NonZeroUsize::new(1024 * 1024);
    request.consumer_max_inflight =
        NonZeroUsize::new(4).assert_value("controlled traversal fixture");
    request
}

async fn acknowledge(
    mut session: TraversalSession,
) -> (
    Vec<TraversalItem>,
    Result<TraversalOutcome, TraversalTerminalFailure>,
) {
    let mut feedback = Vec::new();
    let mut items = Vec::new();
    while let Some(item) = session.next_item(&mut feedback).await {
        if let TraversalItem::ConsumerEntry(work) | TraversalItem::SubtreeReady(work) = &item {
            feedback.push((work.0, true));
        }
        items.push(item);
    }
    (items, session.finish().await)
}

struct ReadOnlyNamespace {
    inner: Arc<dyn Namespace>,
    calls: Arc<Mutex<Vec<String>>>,
    stat_called: CancellationToken,
    parked: Option<NonZeroUsize>,
    objects: bool,
    block: bool,
    fail_root_after_page: bool,
    fail_root_release: CancellationToken,
    started: CancellationToken,
    closed: CancellationToken,
}

impl ReadOnlyNamespace {
    fn new(inner: Arc<dyn Namespace>) -> Self {
        Self {
            inner,
            calls: Arc::default(),
            stat_called: CancellationToken::new(),
            parked: None,
            objects: false,
            block: false,
            fail_root_after_page: false,
            fail_root_release: CancellationToken::new(),
            started: CancellationToken::new(),
            closed: CancellationToken::new(),
        }
    }
}

#[async_trait]
impl Namespace for ReadOnlyNamespace {
    fn capabilities(&self) -> NamespaceCapabilities {
        let mut capabilities = self.inner.capabilities();
        capabilities.create_directory = Err("read only");
        capabilities.directory_objects = self.objects;
        capabilities
    }
    fn parked_listing_limit(&self) -> Option<NonZeroUsize> {
        self.parked
    }
    async fn stat(
        &self,
        path: &StoragePath,
        fresh: bool,
    ) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.stat_called.cancel();
        self.inner.stat(path, fresh).await
    }
    async fn read_link(&self, path: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        self.inner.read_link(path).await
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        self.calls.lock().push(request.path.as_str().to_owned());
        if self.block {
            let (started, closed) = (self.started.clone(), self.closed.clone());
            return ListingStream::from_producer(request.path.clone(), move |_sink| async move {
                started.cancel();
                request.cancel.cancelled().await;
                closed.cancel();
            });
        }
        if self.fail_root_after_page && request.path.as_str().is_empty() {
            let path = request.path.clone();
            let release = self.fail_root_release.clone();
            let mut stream = self.inner.list(request);
            return ListingStream::from_producer(path.clone(), move |sink| async move {
                if let Some(Ok(page)) = stream.next().await
                    && sink.send(page).await.is_err()
                {
                    return;
                }
                release.cancelled().await;
                sink.fail(StorageRoleFailure::Entry(
                    crate::model::EntryOperationFailure::new(
                        path,
                        Operation::Traverse,
                        FailureClass::PermissionDenied,
                        Transience::Permanent,
                        "test listing source failure",
                    )
                    .assert_value("controlled traversal fixture"),
                ))
                .await;
            });
        }
        self.inner.list(request)
    }
}

#[tokio::test]
async fn directory_prepare_gates_listing_and_finalize_waits_for_descendants_using_first_raw() {
    let root = tempfile::tempdir().assert_value("controlled traversal fixture");
    std::fs::create_dir(root.path().join("dir")).assert_value("controlled traversal fixture");
    std::fs::write(root.path().join("dir/file"), b"data")
        .assert_value("controlled traversal fixture");
    let (storage, namespace, metadata) = test_traversal_storage(root.path(), "controlled-gates")
        .assert_value("controlled traversal fixture");
    let namespace = Arc::new(ReadOnlyNamespace::new(namespace));
    let source = StorageTraversalSource::with_roles(namespace.clone(), metadata)
        .identified(storage.identity().clone())
        .output_capacity(NonZeroUsize::MIN);
    let mut request = configured();
    request.observation_plan = request
        .observation_plan
        .with_ownership_mode(ObservationMode::Required)
        .with_timestamps(ObservationMode::InlineOnly);
    let mut session = source.traverse(request);
    let mut feedback = Vec::new();
    let TraversalItem::ConsumerEntry(prepare) = session
        .next_item(&mut feedback)
        .await
        .assert_value("controlled traversal fixture")
    else {
        panic!("missing prepare");
    };
    assert_eq!(prepare.1.path().as_str(), "dir");
    assert_eq!(&*namespace.calls.lock(), &[String::new()]);
    assert!(session.next_item(&mut feedback).now_or_never().is_none());
    filetime::set_file_mtime(
        root.path().join("dir"),
        filetime::FileTime::from_unix_time(12345, 0),
    )
    .assert_value("controlled traversal fixture");
    feedback.push((prepare.0, true));
    let TraversalItem::ConsumerEntry(file) = session
        .next_item(&mut feedback)
        .await
        .assert_value("controlled traversal fixture")
    else {
        panic!("missing descendant");
    };
    assert_eq!(file.1.path().as_str(), "dir/file");
    assert!(
        session.next_item(&mut feedback).now_or_never().is_none(),
        "no finalize before file result"
    );
    feedback.push((file.0, true));
    let TraversalItem::SubtreeReady(finalize) = session
        .next_item(&mut feedback)
        .await
        .assert_value("controlled traversal fixture")
    else {
        panic!("missing finalize");
    };
    assert_ne!(prepare.0, finalize.0);
    assert_eq!(prepare.1.encode_snapshot(), finalize.1.encode_snapshot());
    feedback.push((finalize.0, true));
    assert!(session.next_item(&mut feedback).await.is_none());
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Completed(_))
    ));
}

#[derive(Debug)]
struct SelectedRange {
    hide: bool,
    prune: bool,
}
impl TraversalFilter for SelectedRange {
    fn required_metadata(&self) -> MetadataRequirements {
        MetadataRequirements::NONE
    }
    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        if candidate.kind == EntryKind::Directory {
            TraversalDecision {
                emit: !self.hide,
                descend: !self.prune,
                filter_children: false,
            }
        } else {
            TraversalDecision::unfiltered(candidate.kind)
        }
    }
}

#[derive(Debug)]
struct HiddenTopology;
impl TraversalFilter for HiddenTopology {
    fn required_metadata(&self) -> MetadataRequirements {
        MetadataRequirements::NONE
    }
    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        TraversalDecision {
            emit: candidate.kind != EntryKind::Directory,
            descend: true,
            filter_children: true,
        }
    }
}

#[tokio::test]
async fn policy_boundaries_finalize_without_listing_and_hidden_directories_keep_topology() {
    let root = tempfile::tempdir().assert_value("controlled traversal fixture");
    std::fs::create_dir(root.path().join("dir")).assert_value("controlled traversal fixture");
    std::fs::write(root.path().join("dir/file"), b"data")
        .assert_value("controlled traversal fixture");
    for (hide, prune, depth) in [
        (false, true, None),
        (false, false, NonZeroUsize::new(1)),
        (true, false, None),
    ] {
        let (storage, namespace, metadata) =
            test_traversal_storage(root.path(), "controlled-policy")
                .assert_value("controlled traversal fixture");
        let namespace = Arc::new(ReadOnlyNamespace::new(namespace));
        let source = StorageTraversalSource::with_roles(namespace.clone(), metadata.clone())
            .identified(storage.identity().clone());
        let request = configured()
            .filter(Arc::new(SelectedRange { hide, prune }))
            .max_depth(depth);
        let (items, outcome) = acknowledge(source.traverse(request)).await;
        assert!(
            matches!(outcome, Ok(TraversalOutcome::Completed(_))),
            "{outcome:?}"
        );
        let entries: Vec<_> = items
            .iter()
            .filter_map(|item| {
                if let TraversalItem::ConsumerEntry(work) = item {
                    Some(work.1.path().as_str())
                } else {
                    None
                }
            })
            .collect();
        let ready = items
            .iter()
            .filter(|item| matches!(item, TraversalItem::SubtreeReady(_)))
            .count();
        if hide {
            assert_eq!(entries, ["dir/file"]);
            assert_eq!(ready, 0);
        } else {
            assert_eq!(entries, ["dir"]);
            assert_eq!(ready, 1);
            assert_eq!(&*namespace.calls.lock(), &[String::new()]);
        }
        assert!(
            metadata.probe_orders().0.is_empty(),
            "hidden topology must not fetch unrelated metadata"
        );
    }
}

#[tokio::test]
async fn snapshot_exhaustion_is_typed_and_single_stream_slot_is_reused_after_eof_without_relisting()
{
    let root = tempfile::tempdir().assert_value("controlled traversal fixture");
    std::fs::create_dir(root.path().join("dir")).assert_value("controlled traversal fixture");
    for raw_budget in [true, false] {
        let (storage, namespace, metadata) =
            test_traversal_storage(root.path(), "controlled-limits")
                .assert_value("controlled traversal fixture");
        let mut namespace = ReadOnlyNamespace::new(namespace);
        if !raw_budget {
            namespace.parked = Some(NonZeroUsize::MIN);
        }
        let namespace = Arc::new(namespace);
        let source = StorageTraversalSource::with_roles(namespace.clone(), metadata)
            .identified(storage.identity().clone());
        let mut request = configured();
        let budget = arena::storage_bytes() + 1;
        if raw_budget {
            request.consumer_completion_budget = NonZeroUsize::new(budget);
        }
        let (items, outcome) = tokio::time::timeout(
            Duration::from_secs(3),
            acknowledge(source.traverse(request)),
        )
        .await
        .assert_value("controlled traversal fixture");
        if raw_budget {
            assert!(items.is_empty());
            assert!(
                matches!(outcome, Err(TraversalTerminalFailure::ConsumerCompletionBudgetExceeded { limit, required }) if limit == budget && required > limit)
            );
        } else {
            assert!(
                matches!(outcome, Ok(TraversalOutcome::Completed(_))),
                "single handle slot must be reused after parent EOF: {outcome:?}"
            );
            assert_eq!(
                items
                    .iter()
                    .filter(|item| matches!(item, TraversalItem::SubtreeReady(_)))
                    .count(),
                1
            );
        }
        if raw_budget {
            assert_eq!(&*namespace.calls.lock(), &[String::new()]);
        } else {
            let calls = namespace.calls.lock();
            assert_eq!(calls.len(), 2);
            assert!(calls.contains(&String::new()) && calls.contains(&"dir".to_owned()));
        }
    }
}

#[tokio::test]
async fn source_failure_is_delivered_with_ack_outstanding_and_survives_derived_cancel() {
    let root = tempfile::tempdir().assert_value("controlled traversal fixture");
    std::fs::write(root.path().join("file"), b"data").assert_value("controlled traversal fixture");
    let (storage, namespace, metadata) =
        test_traversal_storage(root.path(), "controlled-source-fail")
            .assert_value("controlled traversal fixture");
    let mut namespace = ReadOnlyNamespace::new(namespace);
    namespace.fail_root_after_page = true;
    let release = namespace.fail_root_release.clone();
    let source = StorageTraversalSource::with_roles(Arc::new(namespace), metadata)
        .identified(storage.identity().clone())
        .output_capacity(NonZeroUsize::MIN);
    let request = configured();
    let cancel = request.cancel.clone();
    let mut session = source.traverse(request);
    let mut feedback = Vec::new();
    assert!(matches!(
        session.next_item(&mut feedback).await,
        Some(TraversalItem::ConsumerEntry(_))
    ));
    release.cancel(); // The source fails only after a real file phase is issued and held.
    let item = tokio::time::timeout(Duration::from_secs(3), session.next_item(&mut feedback))
        .await
        .assert_value("controlled traversal fixture")
        .assert_value("controlled traversal fixture");
    let TraversalItem::Failure(failure) = item else {
        panic!("source failure hidden behind ACK");
    };
    assert_eq!(failure.failure.class(), FailureClass::PermissionDenied);
    assert_eq!(failure.delivered, Some(1));
    cancel.cancel();
    assert!(session.next_item(&mut feedback).await.is_none());
    assert_eq!(
        session.finish().await,
        Err(TraversalTerminalFailure::ConsumerSource(Box::new(
            failure.failure
        )))
    );
}

#[tokio::test(start_paused = true)]
async fn already_cancelled_request_does_not_stat_root() {
    let root = tempfile::tempdir().assert_value("controlled traversal fixture");
    let (storage, namespace, metadata) =
        test_traversal_storage(root.path(), "controlled-pre-cancel")
            .assert_value("controlled traversal fixture");
    let namespace = Arc::new(ReadOnlyNamespace::new(namespace));
    let source = StorageTraversalSource::with_roles(namespace.clone(), metadata)
        .identified(storage.identity().clone());
    let request = configured();
    request.cancel.cancel();

    let (items, outcome) = acknowledge(source.traverse(request)).await;
    assert!(items.is_empty());
    assert_eq!(outcome, Ok(TraversalOutcome::Cancelled));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), namespace.stat_called.cancelled())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cancellation_closes_inflight_listing_without_waiting_for_ack_budget() {
    let root = tempfile::tempdir().assert_value("controlled traversal fixture");
    let (storage, namespace, metadata) = test_traversal_storage(root.path(), "controlled-cancel")
        .assert_value("controlled traversal fixture");
    let mut namespace = ReadOnlyNamespace::new(namespace);
    namespace.block = true;
    let namespace = Arc::new(namespace);
    let source = StorageTraversalSource::with_roles(namespace.clone(), metadata)
        .identified(storage.identity().clone());
    let request = configured();
    let cancel = request.cancel.clone();
    let mut session = source.traverse(request);
    let mut feedback = Vec::new();
    let mut next = Box::pin(session.next_item(&mut feedback));
    tokio::select! { item = &mut next => panic!("unexpected output {item:?}"), () = namespace.started.cancelled() => {} }
    cancel.cancel();
    assert!(next.await.is_none());
    assert_eq!(session.finish().await, Ok(TraversalOutcome::Cancelled));
    tokio::time::timeout(Duration::from_secs(3), namespace.closed.cancelled())
        .await
        .assert_value("controlled traversal fixture");
}

struct FailedAcl {
    inner: Arc<dyn Metadata>,
    calls: Arc<AtomicUsize>,
    recovered: Arc<AtomicBool>,
}
#[async_trait]
impl Metadata for FailedAcl {
    fn observation_capabilities(&self) -> crate::model::MetadataCapabilities {
        self.inner.observation_capabilities()
    }
    fn observe_listed(
        &self,
        descriptor: &SourceDescriptor,
        plan: crate::model::ObservationPlan,
    ) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
        self.inner.observe_listed(descriptor, plan)
    }
    async fn observe(
        &self,
        path: &StoragePath,
        plan: crate::model::ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        assert!(!path.as_str().is_empty(), "root ACL must not be fetched");
        if path.as_str() == "parent/child" && !self.recovered.load(Ordering::SeqCst) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            return Ok(MetadataObservationParts {
                acl: MetadataObservation::Failed {
                    class: FailureClass::PermissionDenied,
                    transience: Transience::Permanent,
                },
                ..MetadataObservationParts::default()
            }
            .build()
            .assert_value("controlled traversal fixture"));
        }
        self.inner.observe(path, plan).await
    }
    async fn apply(
        &self,
        _path: &StoragePath,
        _mutation: MetadataMutation,
        _cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        panic!("traversal must not mutate source metadata")
    }
}

#[tokio::test]
async fn metadata_failure_survives_prepare_allows_content_and_blocks_ancestor_on_finalize_false() {
    let root = tempfile::tempdir().assert_value("controlled traversal fixture");
    std::fs::create_dir_all(root.path().join("parent/child"))
        .assert_value("controlled traversal fixture");
    std::fs::write(root.path().join("parent/child/file"), b"kept content")
        .assert_value("controlled traversal fixture");
    let (storage, namespace, metadata) = test_traversal_storage(root.path(), "controlled-acl")
        .assert_value("controlled traversal fixture");
    let calls = Arc::new(AtomicUsize::new(0));
    let recovered = Arc::new(AtomicBool::new(false));
    let metadata = Arc::new(FailedAcl {
        inner: metadata,
        calls: calls.clone(),
        recovered: recovered.clone(),
    });
    let source = StorageTraversalSource::with_roles(namespace, metadata)
        .identified(storage.identity().clone());
    let mut request = configured();
    request.observation_plan = request
        .observation_plan
        .with_acl(ObservationMode::BestEffort);
    let mut session = source.traverse(request);
    let mut feedback = Vec::new();
    let mut original = None;
    let mut content = false;
    let mut failures = 0;
    let mut finalized = Vec::new();
    while let Some(item) = session.next_item(&mut feedback).await {
        match item {
            TraversalItem::ObservationFailure(failure) => {
                assert_eq!(failure.path.as_str(), "parent/child");
                failures += 1;
            }
            TraversalItem::ConsumerEntry(work) => {
                if work.1.path().as_str() == "parent/child" {
                    original = Some(work.1.encode_snapshot());
                    recovered.store(true, Ordering::SeqCst);
                }
                if work.1.kind() == EntryKind::File {
                    content = std::fs::read(root.path().join(work.1.path().as_str()))
                        .assert_value("controlled traversal fixture")
                        == b"kept content";
                }
                feedback.push((work.0, true));
            }
            TraversalItem::SubtreeReady(work) => {
                finalized.push(work.1.path().as_str().to_owned());
                assert_eq!(Some(work.1.encode_snapshot()), original);
                assert!(matches!(
                    work.1.metadata().acl(),
                    MetadataObservation::Failed { .. }
                ));
                feedback.push((work.0, false));
            }
            other => panic!("unexpected source error: {other:?}"),
        }
    }
    assert!(content);
    assert_eq!(failures, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(finalized, ["parent/child"]);
    assert_eq!(
        session.finish().await,
        Err(TraversalTerminalFailure::ConsumerFailed)
    );
}

#[tokio::test]
async fn real_local_consumer_phases_retain_original_directory_snapshot_without_root_work() {
    let root = tempfile::tempdir().assert_value("controlled traversal fixture");
    std::fs::create_dir(root.path().join("dir")).assert_value("controlled traversal fixture");
    std::fs::create_dir(root.path().join("empty")).assert_value("controlled traversal fixture");
    std::fs::write(root.path().join("dir/file"), b"body")
        .assert_value("controlled traversal fixture");
    let (storage, _, _) = test_traversal_storage(root.path(), "controlled-local")
        .assert_value("controlled traversal fixture");
    let source = StorageTraversalSource::new(&storage).assert_value("controlled traversal fixture");
    let mut request = TraversalRequest::new(StoragePath::root());
    request.consumer_completion_budget = NonZeroUsize::new(64 * 1024);
    request.consumer_max_inflight =
        NonZeroUsize::new(2).assert_value("controlled traversal fixture");
    let mut session = source.traverse(request);
    let mut feedback = Vec::new();
    let mut original = HashMap::new();
    let mut finalized = Vec::new();
    let mut entries = 0;
    while let Some(item) = session.next_item(&mut feedback).await {
        match item {
            TraversalItem::ConsumerEntry(work) => {
                assert!(
                    !work.1.path().as_str().is_empty(),
                    "root metadata belongs to the caller"
                );
                entries += 1;
                if work.1.kind() == EntryKind::Directory {
                    original.insert(work.1.path().clone(), work.1.encode_snapshot());
                }
                feedback.push((work.0, true));
            }
            TraversalItem::SubtreeReady(work) => {
                assert_eq!(Some(&work.1.encode_snapshot()), original.get(work.1.path()));
                finalized.push(work.1.path().as_str().to_owned());
                feedback.push((work.0, true));
            }
            other => panic!("unexpected output: {other:?}"),
        }
    }
    assert_eq!(entries, 3);
    finalized.sort();
    assert_eq!(finalized, ["dir", "empty"]);
    assert!(
        matches!(session.finish().await, Ok(TraversalOutcome::Completed(done)) if done.totals.emitted == 3)
    );
}

#[tokio::test]
async fn wide_tree_releases_directory_snapshots_before_admitting_more_siblings() {
    let root = tempfile::tempdir().assert_value("controlled traversal fixture");
    for index in 0..512 {
        std::fs::create_dir(root.path().join(format!("dir-{index}")))
            .assert_value("controlled traversal fixture");
    }
    let (storage, _, _) = test_traversal_storage(root.path(), "controlled-wide")
        .assert_value("controlled traversal fixture");
    let source = StorageTraversalSource::new(&storage).assert_value("controlled traversal fixture");
    let mut request = configured();
    request.consumer_completion_budget = NonZeroUsize::new(8 * std::mem::size_of::<Node>() + 4096);
    let (items, outcome) = tokio::time::timeout(
        Duration::from_secs(10),
        acknowledge(source.traverse(request)),
    )
    .await
    .assert_value("controlled traversal fixture");
    assert!(
        matches!(outcome, Ok(TraversalOutcome::Completed(_))),
        "bounded wide tree: {outcome:?}"
    );
    assert_eq!(
        items
            .iter()
            .filter(|item| matches!(item, TraversalItem::SubtreeReady(_)))
            .count(),
        512
    );
    assert!(matches!(outcome, Ok(TraversalOutcome::Completed(done)) if done.totals.emitted == 512));
}

#[tokio::test]
async fn hidden_topology_is_charged_even_without_raw_or_consumer_events() {
    let root = tempfile::tempdir().assert_value("controlled traversal fixture");
    std::fs::create_dir_all(root.path().join("a/b/c")).assert_value("controlled traversal fixture");
    std::fs::write(root.path().join("a/b/c/file"), b"retained")
        .assert_value("controlled traversal fixture");
    let (storage, namespace, metadata) =
        test_traversal_storage(root.path(), "controlled-hidden-budget")
            .assert_value("controlled traversal fixture");
    let namespace = Arc::new(ReadOnlyNamespace::new(namespace));
    let source = StorageTraversalSource::with_roles(namespace.clone(), metadata.clone())
        .identified(storage.identity().clone());
    let mut request = configured().filter(Arc::new(HiddenTopology));
    request.consumer_completion_budget = NonZeroUsize::new(2 * std::mem::size_of::<Node>() + 32);
    request.observation_plan = request
        .observation_plan
        .with_acl(ObservationMode::BestEffort);
    let (items, outcome) = acknowledge(source.traverse(request)).await;
    assert!(items.is_empty());
    assert!(matches!(
        outcome,
        Err(TraversalTerminalFailure::ConsumerCompletionBudgetExceeded { .. })
    ));
    assert!(
        namespace.calls.lock().len() <= 2,
        "page/work payloads may exhaust the same budget before the next hidden node"
    );
    assert!(metadata.probe_orders().0.is_empty());
    assert_eq!(
        std::fs::read(root.path().join("a/b/c/file")).assert_value("controlled traversal fixture"),
        b"retained"
    );
}

#[tokio::test]
async fn object_namespace_and_automatic_replay_are_refused_before_source_io() {
    let root = tempfile::tempdir().assert_value("controlled traversal fixture");
    let (storage, namespace, metadata) =
        test_traversal_storage(root.path(), "controlled-unsupported")
            .assert_value("controlled traversal fixture");
    let mut namespace = ReadOnlyNamespace::new(namespace);
    namespace.objects = true;
    let namespace = Arc::new(namespace);
    let source = StorageTraversalSource::with_roles(namespace.clone(), metadata)
        .identified(storage.identity().clone());
    let (_, outcome) = acknowledge(source.traverse(configured())).await;
    assert_eq!(
        outcome,
        Err(TraversalTerminalFailure::Configuration(
            TraversalConfigurationError::ConsumerCompletionUnsupported
        ))
    );
    assert!(namespace.calls.lock().is_empty());
    let policy = crate::traversal::TraversalRecoveryPolicy {
        resume: crate::traversal::TraversalRecoveryResume::RestartRoot,
        operation_timeout: None,
        recovery_window: Duration::from_secs(3),
    };
    let reconnect: crate::traversal::TraversalReconnect =
        Arc::new(|| Box::pin(async { panic!("unsupported replay must not reconnect") }));
    let result = crate::traversal::recoverable_traversal(&storage, configured(), policy, reconnect);
    assert!(matches!(
        result,
        Err(crate::traversal::ScanSetupError::Configuration(
            TraversalConfigurationError::ConsumerResumeUnsupported
        ))
    ));
}

#[tokio::test]
async fn consumer_file_root_is_explicitly_unsupported_and_never_an_empty_success() {
    let root = tempfile::tempdir().assert_value("controlled traversal fixture");
    std::fs::write(root.path().join("file"), b"single file retained")
        .assert_value("controlled traversal fixture");
    let (storage, _, metadata) = test_traversal_storage(root.path(), "controlled-file-root")
        .assert_value("controlled traversal fixture");
    let source = StorageTraversalSource::new(&storage).assert_value("controlled traversal fixture");
    let mut request = configured();
    request.root = StoragePath::new("file").assert_value("controlled traversal fixture");
    request.observation_plan = request
        .observation_plan
        .with_acl(ObservationMode::BestEffort);
    let (items, outcome) = acknowledge(source.traverse(request)).await;
    assert!(items.is_empty());
    assert!(
        matches!(outcome, Err(TraversalTerminalFailure::ConsumerSource(failure))
        if failure.path().as_str() == "file" && failure.class() == FailureClass::Unsupported)
    );
    assert!(metadata.probe_orders().0.is_empty());
    assert_eq!(
        std::fs::read(root.path().join("file")).assert_value("controlled traversal fixture"),
        b"single file retained"
    );
}

#[path = "controlled_source_failure_tests.rs"]
mod source_failure;

#[tokio::test]
async fn one_observation_slot_and_one_consumer_work_support_multilevel_tree() {
    let root = tempfile::tempdir().assert_value("controlled resource fixture");
    std::fs::create_dir_all(root.path().join("a/b/c")).assert_value("controlled resource fixture");
    std::fs::write(root.path().join("a/b/c/kept"), b"source stays present")
        .assert_value("controlled resource fixture");
    let (storage, _, _) = test_traversal_storage(root.path(), "controlled-one-slot")
        .assert_value("controlled resource fixture");
    let source = StorageTraversalSource::new(&storage).assert_value("controlled resource fixture");
    let mut request = configured();
    request.max_inflight_operations = NonZeroUsize::MIN;
    request.consumer_max_inflight = NonZeroUsize::MIN;
    let (items, outcome) = tokio::time::timeout(
        Duration::from_secs(3),
        acknowledge(source.traverse(request)),
    )
    .await
    .assert_value("controlled resource fixture");
    assert!(
        matches!(outcome, Ok(TraversalOutcome::Completed(_))),
        "single observation slot must not be a depth limit: {outcome:?}"
    );
    assert_eq!(
        items
            .iter()
            .filter(|item| matches!(item, TraversalItem::ConsumerEntry(_)))
            .count(),
        4
    );
    assert_eq!(
        items
            .iter()
            .filter(|item| matches!(item, TraversalItem::SubtreeReady(_)))
            .count(),
        3
    );
    assert_eq!(
        std::fs::read(root.path().join("a/b/c/kept")).assert_value("controlled resource fixture"),
        b"source stays present"
    );
}

#[path = "controlled_resource_tests.rs"]
mod resources;

#[path = "controlled_whole_role_tests.rs"]
mod whole_role;

#[path = "controlled_parallel_tests.rs"]
mod parallel;
