//! A returned source cause must survive cancellation while a sibling I/O is still running.

use tokio::sync::Barrier;

use super::*;
use crate::model::BackendSessionFailure;
use crate::storage::ListingPage;
use crate::storage::listing::ChildFailure;

struct ReturnedFailure {
    barrier: Barrier,
    failed: CancellationToken,
    blocked: CancellationToken,
    release: CancellationToken,
    closed: CancellationToken,
    failure: BackendSessionFailure,
}

#[async_trait]
impl Metadata for ReturnedFailure {
    async fn observe(
        &self,
        path: &StoragePath,
        _plan: crate::model::ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        self.barrier.wait().await;
        if path.as_str() == "bad" {
            self.failed.cancel();
            return Err(StorageRoleFailure::Session(self.failure.clone()));
        }
        self.blocked.cancel();
        self.release.cancelled().await;
        self.closed.cancel();
        Ok(MetadataObservations::default())
    }

    async fn apply(
        &self,
        _path: &StoragePath,
        _mutation: MetadataMutation,
        _cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        panic!("source traversal never applies metadata")
    }
}

#[tokio::test]
async fn returned_session_failure_survives_cancel_with_sibling_metadata_still_inflight() {
    let root = tempfile::tempdir().assert_value("source cause fixture");
    for name in ["bad", "slow"] {
        std::fs::write(root.path().join(name), b"retained").assert_value("source data fixture");
    }
    let (storage, namespace, _) =
        test_traversal_storage(root.path(), "returned-source-failure").assert_value("local roles");
    let failure = BackendSessionFailure::new(
        Operation::Observe,
        FailureClass::Connectivity,
        Transience::Transient,
        "returned source connectivity failure",
    )
    .assert_value("typed source failure");
    let metadata = Arc::new(ReturnedFailure {
        barrier: Barrier::new(2),
        failed: CancellationToken::new(),
        blocked: CancellationToken::new(),
        release: CancellationToken::new(),
        closed: CancellationToken::new(),
        failure: failure.clone(),
    });
    let source = StorageTraversalSource::with_roles(namespace, metadata.clone())
        .identified(storage.identity().clone());
    let mut request = configured();
    request.observation_plan = request
        .observation_plan
        .with_acl(ObservationMode::BestEffort);
    let cancel = request.cancel.clone();
    let mut session = source.traverse(request);
    let mut feedback = Vec::new();
    let mut next = Box::pin(session.next_item(&mut feedback));
    let ended = tokio::select! {
        item = &mut next => {assert!(item.is_none(), "source fatal cannot issue data work: {item:?}");true},
        () = async { metadata.failed.cancelled().await; metadata.blocked.cancelled().await; } => false,
    };
    cancel.cancel();
    if ended {
        drop(next);
    } else {
        assert!(next.await.is_none());
    }
    let terminal = session.finish().await;
    metadata.release.cancel();
    tokio::time::timeout(Duration::from_secs(3), metadata.closed.cancelled())
        .await
        .assert_value("owned metadata operation closes after release");
    assert_eq!(terminal, Err(TraversalTerminalFailure::Session(failure)));
}

struct DiagnosticPage {
    root: SourceDescriptor,
    page: ListingPage,
}

#[async_trait]
impl Namespace for DiagnosticPage {
    async fn stat(
        &self,
        path: &StoragePath,
        _fresh: bool,
    ) -> Result<SourceDescriptor, StorageRoleFailure> {
        assert!(
            path.as_str().is_empty(),
            "fixture needs no confirmation/reobservation"
        );
        Ok(self.root.clone())
    }

    fn list(&self, request: ListRequest) -> ListingStream {
        ListingStream::from_pages(request.path, [Ok(self.page.clone())])
    }
}

#[derive(Clone, Copy)]
enum DiagnosticResult {
    Failure,
    Blocked,
    Success,
}

struct InlineFailure {
    result: DiagnosticResult,
    inline: AtomicUsize,
    calls: AtomicUsize,
    started: CancellationToken,
    release: CancellationToken,
    closed: CancellationToken,
    failure: BackendSessionFailure,
    from_listing: AtomicBool,
}

#[async_trait]
impl Metadata for InlineFailure {
    fn observe_listed(
        &self,
        descriptor: &SourceDescriptor,
        plan: crate::model::ObservationPlan,
    ) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
        if descriptor.path.as_str() != "inline" || !self.from_listing.load(Ordering::SeqCst) {
            return None;
        }
        let acl = if plan.acl() == ObservationMode::Omit {
            MetadataObservation::NotRequested
        } else {
            self.inline.fetch_add(1, Ordering::SeqCst);
            MetadataObservation::Failed {
                class: FailureClass::PermissionDenied,
                transience: Transience::Permanent,
            }
        };
        Some(Ok(MetadataObservationParts {
            acl,
            ..MetadataObservationParts::default()
        }
        .build()
        .assert_value("known inline ACL failure")))
    }

    async fn observe(
        &self,
        path: &StoragePath,
        _plan: crate::model::ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if path.as_str() == "inline" {
            self.inline.fetch_add(1, Ordering::SeqCst);
            return Ok(failed_acl());
        }
        self.started.cancel();
        match self.result {
            DiagnosticResult::Failure => Err(StorageRoleFailure::Session(self.failure.clone())),
            DiagnosticResult::Blocked => {
                self.release.cancelled().await;
                self.closed.cancel();
                Ok(failed_acl()) // A late completed fact must not enter an already sealed drain.
            }
            DiagnosticResult::Success => Ok(MetadataObservationParts {
                acl: MetadataObservation::NotApplicable,
                ..MetadataObservationParts::default()
            }
            .build()
            .assert_value("successful sibling observation")),
        }
    }

    async fn apply(
        &self,
        _path: &StoragePath,
        _mutation: MetadataMutation,
        _cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        panic!("source is read only")
    }
}

fn failed_acl() -> MetadataObservations {
    MetadataObservationParts {
        acl: MetadataObservation::Failed {
            class: FailureClass::PermissionDenied,
            transience: Transience::Permanent,
        },
        ..MetadataObservationParts::default()
    }
    .build()
    .assert_value("known observed ACL failure")
}

async fn diagnostic_source(
    result: DiagnosticResult,
    names: &[&str],
    known_child_failure: bool,
) -> (
    tempfile::TempDir,
    StorageTraversalSource,
    Arc<InlineFailure>,
    Option<crate::model::EntryOperationFailure>,
) {
    let root = tempfile::tempdir().assert_value("diagnostic source fixture");
    for name in names {
        std::fs::write(root.path().join(name), b"retained").assert_value("source body");
    }
    let (storage, namespace, _) =
        test_traversal_storage(root.path(), "diagnostic-page").assert_value("local roles");
    let path = StoragePath::root();
    let descriptor = namespace
        .stat(&path, false)
        .await
        .assert_value("root shape");
    let mut page = ListingPage::default();
    for name in names {
        let child = namespace
            .stat(&StoragePath::new(*name).assert_value("child path"), false)
            .await
            .assert_value("bound descriptor");
        page.push_descriptor(&path, child, None);
    }
    let child_failure = known_child_failure.then(|| {
        crate::model::EntryOperationFailure::new(
            StoragePath::new("unreadable").assert_value("child failure path"),
            Operation::Traverse,
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "already known child failure",
        )
        .assert_value("typed child failure")
    });
    if let Some(failure) = &child_failure {
        page.push_failure(ChildFailure::from_failure(&path, failure.clone()));
    }
    let metadata = Arc::new(InlineFailure {
        result,
        inline: AtomicUsize::new(0),
        calls: AtomicUsize::new(0),
        started: CancellationToken::new(),
        release: CancellationToken::new(),
        closed: CancellationToken::new(),
        failure: BackendSessionFailure::new(
            Operation::Observe,
            FailureClass::Connectivity,
            Transience::Transient,
            "same page source failure",
        )
        .assert_value("typed session failure"),
        from_listing: AtomicBool::new(true),
    });
    let source = StorageTraversalSource::with_roles(
        Arc::new(DiagnosticPage {
            root: descriptor,
            page,
        }),
        metadata.clone(),
    )
    .identified(storage.identity().clone())
    .output_capacity(NonZeroUsize::MIN);
    (root, source, metadata, child_failure)
}

fn diagnostic_request() -> TraversalRequest {
    let mut request = configured();
    request.max_inflight_operations = NonZeroUsize::MIN;
    request.observation_plan = request
        .observation_plan
        .with_acl(ObservationMode::BestEffort);
    request
}

fn inline_diagnostics(items: &[TraversalItem]) -> usize {
    items
        .iter()
        .filter(|item| {
            matches!(item, TraversalItem::ObservationFailure(failure)
        if failure.path.as_str() == "inline" && failure.family == crate::model::MetadataFamily::Acl)
        })
        .count()
}

#[tokio::test]
async fn same_page_inline_failure_survives_terminal_result_and_skipped_queued_job() {
    for from_listing in [true, false] {
        let (_root, source, metadata, _) = diagnostic_source(
            DiagnosticResult::Failure,
            &["inline", "bad", "queued"],
            false,
        )
        .await;
        metadata.from_listing.store(from_listing, Ordering::SeqCst);
        let (items, terminal) = acknowledge(source.traverse(diagnostic_request())).await;
        assert_eq!(
            terminal,
            Err(TraversalTerminalFailure::Session(metadata.failure.clone()))
        );
        assert_eq!(
            metadata.calls.load(Ordering::SeqCst),
            if from_listing { 1 } else { 2 },
            "no new I/O after fatal source result"
        );
        assert_eq!(
            metadata.inline.load(Ordering::SeqCst),
            1,
            "inline source is not reobserved"
        );
        assert_eq!(
            inline_diagnostics(&items),
            1,
            "known inline metadata diagnostic must survive aggregation failure"
        );
        assert_eq!(items.len(), 1, "no new work after source failure");
    }
}

#[tokio::test]
async fn same_page_inline_failure_drains_on_cancel_without_waiting_for_unknown_io() {
    for from_listing in [true, false] {
        let (_root, source, metadata, _) =
            diagnostic_source(DiagnosticResult::Blocked, &["inline", "blocked"], false).await;
        metadata.from_listing.store(from_listing, Ordering::SeqCst);
        let request = diagnostic_request();
        let cancel = request.cancel.clone();
        let mut session = source.traverse(request);
        let mut feedback = Vec::new();
        let state = session
            .consumer
            .as_ref()
            .map(Arc::clone)
            .assert_value("actual controlled state");
        let mut next = Box::pin(session.next_item(&mut feedback));
        tokio::select! {
            item = &mut next => panic!("unexpected pre-cancel output {item:?}"),
            () = metadata.started.cancelled() => {}
        }
        let started = std::time::Instant::now();
        cancel.cancel();
        let first = tokio::time::timeout(Duration::from_secs(3), next)
            .await
            .assert_value("cancel returns before unknown I/O");
        let mut items: Vec<_> = first.into_iter().collect();
        while let Some(item) = session.next_item(&mut feedback).await {
            items.push(item);
        }
        let terminal = session.finish().await;
        assert!(
            !metadata.closed.is_cancelled(),
            "drain never waits for unreleased I/O"
        );
        metadata.release.cancel();
        tokio::time::timeout(Duration::from_secs(3), metadata.closed.cancelled())
            .await
            .assert_value("owned I/O eventually closes");
        assert!(
            state.next_diagnostic().is_none(),
            "late detached result cannot append after the cutoff"
        );
        assert!(state.failure().is_none());
        println!("controlled cancel/drain elapsed={:?}", started.elapsed());
        assert_eq!(terminal, Ok(TraversalOutcome::Cancelled));
        assert_eq!(metadata.inline.load(Ordering::SeqCst), 1);
        assert_eq!(
            inline_diagnostics(&items),
            1,
            "known inline failure must be drained after cancel"
        );
        assert_eq!(items.len(), 1, "cancel publishes diagnostics only");
    }
}

#[tokio::test]
async fn same_page_known_child_failure_preserves_all_inline_facts_without_starting_async_io() {
    let (_root, source, metadata, failure) =
        diagnostic_source(DiagnosticResult::Failure, &["inline", "bad"], true).await;
    let (items, terminal) = acknowledge(source.traverse(diagnostic_request())).await;
    assert_eq!(
        terminal,
        Err(TraversalTerminalFailure::ConsumerSource(Box::new(
            failure.assert_value("known child failure")
        )))
    );
    assert_eq!(
        metadata.calls.load(Ordering::SeqCst),
        0,
        "known page failure prevents new asynchronous I/O"
    );
    assert_eq!(inline_diagnostics(&items), 1);
    assert_eq!(
        items
            .iter()
            .filter(|item| matches!(item, TraversalItem::Failure(_)))
            .count(),
        1
    );
    assert_eq!(items.len(), 2);
}

#[tokio::test]
async fn same_page_success_delivers_inline_diagnostic_once_and_counts_entries_once() {
    let (_root, source, metadata, _) =
        diagnostic_source(DiagnosticResult::Success, &["inline", "good"], false).await;
    let (items, terminal) = acknowledge(source.traverse(diagnostic_request())).await;
    assert!(
        matches!(terminal, Ok(TraversalOutcome::Completed(done)) if done.totals.emitted == 2 && done.totals.listed == 2)
    );
    assert_eq!(metadata.inline.load(Ordering::SeqCst), 1);
    assert_eq!(metadata.calls.load(Ordering::SeqCst), 1);
    assert_eq!(inline_diagnostics(&items), 1);
    assert_eq!(items.len(), 3);
}
