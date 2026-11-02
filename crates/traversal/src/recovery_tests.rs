use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::FutureExt;
use futures::future::pending;
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::{
    BackendIdentity, BackendKind, EntryKind, HistoryOrder, IdentityStrength, MetadataObservations,
    ObjectHistory, ObservationMode, ObservationPlan, Operation, RecordKind, SourceIdentity,
    SourceVersion, StoragePath, SymlinkTarget, VersionRecord,
};
use crate::storage::{
    BackendCapabilities, CapabilityAvailability, CopiedMetadataObservation, ListRequest,
    ListingPage, ListingStream, Metadata, MetadataMutation, Namespace, NamespaceCapabilities,
    SourceDescriptor, StorageBuilder, UnsupportedReason,
};
use crate::traversal::{RemoteScanFailureKind, TraversalItem};

struct Fixture {
    fails: bool,
    first_page: bool,
    metadata: MetadataMode,
    children: Children,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum MetadataMode {
    Healthy,
    Fails,
    InlineThenFails,
    VersionFault,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Children {
    Both,
    OnlyB,
    Empty,
    Versions,
    Link,
}

fn identity() -> BackendIdentity {
    BackendIdentity::new(BackendKind::Cifs, "scan-recovery-fixture").unwrap()
}

fn descriptor(path: StoragePath, kind: EntryKind) -> SourceDescriptor {
    let source = SourceIdentity::new(
        identity(),
        IdentityStrength::PathScoped,
        format!("path:{}", path.as_str()),
    )
    .unwrap();
    SourceDescriptor::new(path, kind, Some(0), source)
}

fn network_failure() -> StorageRoleFailure {
    StorageRoleFailure::Session(
        BackendSessionFailure::new(
            Operation::Traverse,
            FailureClass::Connectivity,
            Transience::Transient,
            "connection reset",
        )
        .unwrap(),
    )
}

#[async_trait]
impl Namespace for Fixture {
    fn capabilities(&self) -> NamespaceCapabilities {
        NamespaceCapabilities {
            symlinks: self.children == Children::Link,
            ..NamespaceCapabilities::BASIC
        }
    }

    async fn stat(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        Ok(descriptor(path.clone(), EntryKind::Directory))
    }

    fn list(&self, request: ListRequest) -> ListingStream {
        if matches!(self.children, Children::Versions | Children::Link) {
            let child = match self.children {
                Children::Versions => versioned_child(),
                _ => descriptor(StoragePath::new("a").unwrap(), EntryKind::Symlink),
            };
            let page = ListingPage::from_descriptors(&request.path, vec![child], vec![], 0);
            return ListingStream::from_pages(request.path, [Ok(page)]);
        }
        let (fails, first_page, children) = (self.fails, self.first_page, self.children);
        ListingStream::from_producer(request.path.clone(), move |sink| async move {
            if (first_page || !fails) && children == Children::Both {
                let page = ListingPage::from_descriptors(
                    &request.path,
                    vec![descriptor(StoragePath::new("a").unwrap(), EntryKind::File)],
                    vec![],
                    0,
                );
                if sink.send(page).await.is_err() {
                    return;
                }
            }
            if fails {
                sink.fail(network_failure()).await;
            } else if children != Children::Empty {
                let page = ListingPage::from_descriptors(
                    &request.path,
                    vec![descriptor(StoragePath::new("b").unwrap(), EntryKind::File)],
                    vec![],
                    0,
                );
                let _ = sink.send(page).await;
            }
        })
    }

    async fn read_link(&self, _: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        sleep(Duration::from_millis(25)).await;
        Err(network_failure())
    }
}

fn versioned_child() -> SourceDescriptor {
    let mut child = descriptor(StoragePath::new("a").unwrap(), EntryKind::File);
    let records = [("healthy", false), ("bad", true)]
        .into_iter()
        .map(|(version, latest)| {
            VersionRecord::from_listing(
                version,
                latest,
                RecordKind::Object {
                    size: 0,
                    etag: version.into(),
                    modified: None,
                },
                child.source_identity.clone(),
            )
            .unwrap()
        })
        .collect();
    child.listing.history = Some(Box::new(
        ObjectHistory::whole(records, HistoryOrder::Server).unwrap(),
    ));
    child
}

#[async_trait]
impl Metadata for Fixture {
    fn observe_listed(
        &self,
        _: &SourceDescriptor,
        plan: ObservationPlan,
    ) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
        (self.metadata == MetadataMode::InlineThenFails && !plan.permits_io())
            .then(|| Ok(MetadataObservations::default()))
    }

    async fn observe(
        &self,
        _: &StoragePath,
        _: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        if self.metadata == MetadataMode::Healthy {
            return Ok(MetadataObservations::default());
        }
        if self.metadata == MetadataMode::InlineThenFails {
            sleep(Duration::from_millis(25)).await;
        }
        Err(network_failure())
    }
    async fn apply(
        &self,
        _: &StoragePath,
        _: MetadataMutation,
        _: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        unreachable!()
    }

    async fn observe_copy_bound_version(
        &self,
        _: &StoragePath,
        _: &SourceIdentity,
        version: &SourceVersion,
        _: ObservationPlan,
    ) -> Result<CopiedMetadataObservation, StorageRoleFailure> {
        sleep(Duration::from_millis(25)).await;
        if self.metadata == MetadataMode::VersionFault && version.version_id() == Some("bad") {
            return Err(network_failure());
        }
        Ok(CopiedMetadataObservation {
            observations: MetadataObservations::default(),
            mode_without_ownership: None,
            owner_names_unmapped: false,
        })
    }
}

fn storage(fails: bool, first_page: bool) -> Storage {
    fixture_storage(Fixture {
        fails,
        first_page,
        metadata: MetadataMode::Healthy,
        children: Children::Both,
    })
}

fn fixture_storage(fixture: impl Namespace + Metadata + 'static) -> Storage {
    let roles = Arc::new(fixture);
    let no = CapabilityAvailability::Unsupported(UnsupportedReason::new("scan fixture").unwrap());
    StorageBuilder::new(
        identity(),
        BackendCapabilities::new(
            no.clone(),
            no,
            CapabilityAvailability::Supported,
            CapabilityAvailability::Supported,
        ),
    )
    .namespace(roles.clone())
    .metadata(roles)
    .build()
    .unwrap()
}

fn policy() -> TraversalRecoveryPolicy {
    TraversalRecoveryPolicy {
        operation_timeout: Some(Duration::from_millis(100)),
        recovery_window: Duration::from_millis(80),
    }
}

fn reconnect(fails: bool, calls: Arc<AtomicUsize>) -> TraversalReconnect {
    Arc::new(move || {
        calls.fetch_add(1, Ordering::Relaxed);
        async move { Ok(storage(fails, false)) }.boxed()
    })
}

async fn drain(mut session: TraversalSession) -> (Vec<String>, Outcome) {
    let mut paths = vec![];
    while let Some(item) = session.next_item().await {
        if let TraversalItem::Entry(entry) = item {
            paths.push(entry.path().as_str().to_owned());
        }
    }
    (paths, session.finish().await)
}

#[tokio::test]
async fn reset_reconnects_without_skipping_paths_or_publishing_a_final_fault() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (session, status) = recoverable_traversal(
        &storage(true, true),
        TraversalRequest::new(StoragePath::root()),
        policy(),
        reconnect(false, calls.clone()),
    )
    .unwrap();
    let (paths, outcome) = timeout(Duration::from_secs(1), drain(session))
        .await
        .unwrap();
    assert!(paths.iter().any(|path| path == "a"));
    assert!(paths.iter().any(|path| path == "b"));
    assert!(matches!(outcome, Ok(TraversalOutcome::Completed(_))));
    assert!(calls.load(Ordering::Relaxed) >= 1);
    assert!(status.failure_signal().failure().is_none());
    assert!(matches!(
        *status.subscribe().borrow(),
        TraversalRecoveryState::Running { recoveries: 1 }
    ));
}

#[tokio::test]
async fn repeated_immediate_failures_share_one_recovery_budget() {
    let calls = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();
    let (session, status) = recoverable_traversal(
        &storage(true, false),
        TraversalRequest::new(StoragePath::root()),
        policy(),
        reconnect(true, calls.clone()),
    )
    .unwrap();
    let (_, outcome) = timeout(Duration::from_secs(1), drain(session))
        .await
        .unwrap();
    assert!(matches!(outcome, Err(TraversalTerminalFailure::Session(_))));
    assert!(start.elapsed() < Duration::from_millis(500));
    assert!(calls.load(Ordering::Relaxed) > 1);
    assert_eq!(
        status.failure_signal().failure().unwrap().kind,
        RemoteScanFailureKind::ConnectionLost
    );
    assert!(matches!(
        *status.subscribe().borrow(),
        TraversalRecoveryState::Failed { .. }
    ));
}

#[tokio::test(start_paused = true)]
async fn a_hanging_reconnect_uses_the_callers_window_and_keeps_the_original_reset_cause() {
    for window in [Duration::from_millis(40), Duration::from_millis(140)] {
        let connect: TraversalReconnect = Arc::new(|| pending().boxed());
        let selected = TraversalRecoveryPolicy {
            recovery_window: window,
            ..policy()
        };
        let start = Instant::now();
        let (session, status) = recoverable_traversal(
            &storage(true, false),
            TraversalRequest::new(StoragePath::root()),
            selected,
            connect,
        )
        .unwrap();
        let (_, outcome) = timeout(Duration::from_secs(1), drain(session))
            .await
            .unwrap();
        assert!(
            matches!(outcome, Err(TraversalTerminalFailure::Session(failure))
            if failure.class() == FailureClass::Connectivity)
        );
        assert!(start.elapsed() >= window);
        assert!(start.elapsed() < window + Duration::from_millis(5));
        let failure = status.failure_signal().failure().unwrap();
        assert_eq!(failure.kind, RemoteScanFailureKind::ConnectionLost);
        assert_eq!(failure.path, StoragePath::root());
    }
}

#[tokio::test]
async fn cancellation_during_recovery_is_cancelled_without_a_final_network_fault() {
    let cancel = CancellationToken::new();
    let connect: TraversalReconnect = Arc::new(|| pending().boxed());
    let (session, status) = recoverable_traversal(
        &storage(true, false),
        TraversalRequest::new(StoragePath::root()).cancel(cancel.clone()),
        policy(),
        connect,
    )
    .unwrap();
    let mut state = status.subscribe();
    let draining = tokio::spawn(drain(session));
    while !matches!(*state.borrow(), TraversalRecoveryState::Recovering { .. }) {
        state.changed().await.unwrap();
    }
    cancel.cancel();
    let (_, outcome) = timeout(Duration::from_secs(1), draining)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(outcome, Ok(TraversalOutcome::Cancelled)));
    assert!(status.failure_signal().failure().is_none());
}

#[tokio::test]
async fn native_faults_recover_even_without_an_operation_timeout() {
    let calls = Arc::new(AtomicUsize::new(0));
    let selected = TraversalRecoveryPolicy {
        operation_timeout: None,
        ..policy()
    };
    let (session, status) = recoverable_traversal(
        &storage(true, false),
        TraversalRequest::new(StoragePath::root()),
        selected,
        reconnect(false, calls),
    )
    .unwrap();
    let (paths, outcome) = timeout(Duration::from_secs(1), drain(session))
        .await
        .unwrap();
    assert!(matches!(outcome, Ok(TraversalOutcome::Completed(_))));
    assert!(paths.iter().any(|path| path == "b"));
    assert!(status.failure_signal().failure().is_none());
}

#[tokio::test]
async fn resumable_cancellation_drains_its_checkpoint_before_completion() {
    let cancel = CancellationToken::new();
    let connect: TraversalReconnect = Arc::new(|| pending().boxed());
    let request = TraversalRequest::new(StoragePath::root())
        .resumable(Resumable::on())
        .cancel(cancel.clone());
    let (mut session, status) =
        recoverable_traversal(&storage(true, false), request, policy(), connect).unwrap();
    let mut state = status.subscribe();
    let draining = tokio::spawn(async move {
        let mut checkpoint = false;
        while let Some(item) = session.next_item().await {
            checkpoint |= matches!(item, TraversalItem::Checkpoint(_));
        }
        (checkpoint, session.finish().await)
    });
    while !matches!(*state.borrow(), TraversalRecoveryState::Recovering { .. }) {
        state.changed().await.unwrap();
    }
    cancel.cancel();
    let (checkpoint, outcome) = timeout(Duration::from_secs(1), draining)
        .await
        .unwrap()
        .unwrap();
    assert!(checkpoint);
    assert!(matches!(outcome, Ok(TraversalOutcome::Cancelled)));
    assert!(status.failure_signal().failure().is_none());
}

#[tokio::test]
async fn successful_listing_cannot_refresh_a_persistent_metadata_failure_window() {
    let broken = || {
        fixture_storage(Fixture {
            fails: false,
            first_page: false,
            metadata: MetadataMode::Fails,
            children: Children::Both,
        })
    };
    let connect: TraversalReconnect = Arc::new(move || async move { Ok(broken()) }.boxed());
    let started = Instant::now();
    let (session, status) = recoverable_traversal(
        &broken(),
        TraversalRequest::new(StoragePath::root())
            .observation_plan(ObservationPlan::default().with_tags(ObservationMode::BestEffort)),
        policy(),
        connect,
    )
    .unwrap();
    let (_, outcome) = timeout(Duration::from_secs(1), drain(session))
        .await
        .unwrap();
    assert!(matches!(outcome, Err(TraversalTerminalFailure::Session(_))));
    assert!(started.elapsed() < Duration::from_millis(500));
    assert_eq!(
        status.failure_signal().failure().unwrap().operation,
        Operation::Observe
    );
}

fn waiting_recovery() -> (Recovery, channel::ItemReceiver, ScanFailureSignal) {
    use std::num::NonZeroUsize;
    let (items, output) = channel::channel(NonZeroUsize::MIN);
    let (state, _) = watch::channel(TraversalRecoveryState::Running { recoveries: 0 });
    let signal = ScanFailureSignal::new(BackendKind::Cifs, None);
    signal.publish_failure(RemoteScanFailure {
        backend: BackendKind::Cifs,
        operation: Operation::Traverse,
        path: StoragePath::root(),
        kind: RemoteScanFailureKind::ConnectionLost,
        operation_timeout: Duration::ZERO,
        source: None,
    });
    (
        Recovery {
            request: TraversalRequest::new(StoragePath::root()),
            policy: policy(),
            reconnect: reconnect(false, Arc::new(AtomicUsize::new(0))),
            producer: TraversalProducer {
                items,
                completion: None,
            },
            state,
            failure: signal.clone(),
            window: None,
            checkpoint: None,
            recoveries: 0,
        },
        output,
        signal,
    )
}

fn permanent_item() -> TraversalItem {
    use crate::model::EntryOperationFailure;
    use crate::traversal::ScanFailure;
    TraversalItem::Failure(Box::new(ScanFailure::child(
        EntryOperationFailure::new(
            StoragePath::new("denied").unwrap(),
            Operation::Observe,
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "denied",
        )
        .unwrap(),
    )))
}

#[tokio::test]
async fn healthy_replay_consumer_wait_does_not_exhaust_failure_budget() {
    let (mut recovery, mut output, _) = waiting_recovery();
    let signal = ScanFailureSignal::new(BackendKind::Cifs, None);
    recovery.failure = signal.clone();
    recovery
        .producer
        .items
        .send_one(permanent_item())
        .await
        .unwrap();
    let waiting = tokio::spawn(async move {
        let result = recovery.forward(permanent_item(), &signal).await;
        (recovery, result)
    });
    sleep(Duration::from_millis(160)).await;
    assert!(!waiting.is_finished());
    output.recv().await.unwrap();
    output.recv().await.unwrap();
    let (recovery, result) = waiting.await.unwrap();
    assert!(result.is_ok());
    assert!(recovery.window.is_none());
}

#[tokio::test]
async fn inline_metadata_cannot_refresh_a_persistent_additional_rpc_failure_window() {
    let broken = || {
        fixture_storage(Fixture {
            fails: false,
            first_page: false,
            metadata: MetadataMode::InlineThenFails,
            children: Children::Both,
        })
    };
    let connect: TraversalReconnect = Arc::new(move || async move { Ok(broken()) }.boxed());
    let (session, status) = recoverable_traversal(
        &broken(),
        TraversalRequest::new(StoragePath::root())
            .observation_plan(ObservationPlan::default().with_tags(ObservationMode::BestEffort)),
        policy(),
        connect,
    )
    .unwrap();
    let (_, outcome) = timeout(Duration::from_millis(500), drain(session))
        .await
        .unwrap();
    assert!(matches!(outcome, Err(TraversalTerminalFailure::Session(_))));
    let failure = status.failure_signal().failure().unwrap();
    assert_eq!(failure.kind, RemoteScanFailureKind::ConnectionLost);
    assert_eq!(failure.operation, Operation::Observe);
}

#[tokio::test]
async fn a_clean_completed_attempt_recovers_when_the_faulting_path_disappears() {
    for children in [Children::OnlyB, Children::Empty] {
        let initial = fixture_storage(Fixture {
            fails: false,
            first_page: false,
            metadata: MetadataMode::Fails,
            children: Children::Both,
        });
        let connect: TraversalReconnect = Arc::new(move || {
            async move {
                Ok(fixture_storage(Fixture {
                    fails: false,
                    first_page: false,
                    metadata: MetadataMode::Healthy,
                    children,
                }))
            }
            .boxed()
        });
        let (session, status) = recoverable_traversal(
            &initial,
            TraversalRequest::new(StoragePath::root()).observation_plan(
                ObservationPlan::default().with_tags(ObservationMode::BestEffort),
            ),
            policy(),
            connect,
        )
        .unwrap();
        let (paths, outcome) = timeout(Duration::from_millis(500), drain(session))
            .await
            .unwrap();
        assert!(matches!(outcome, Ok(TraversalOutcome::Completed(_))));
        if children == Children::OnlyB {
            assert!(paths.iter().any(|path| path == "b"));
        }
        assert!(status.failure_signal().failure().is_none());
        assert!(matches!(
            *status.subscribe().borrow(),
            TraversalRecoveryState::Running { recoveries: 1 }
        ));
    }
}

#[tokio::test]
async fn another_successful_read_cannot_renew_a_persistent_fault_at_the_same_path() {
    for (children, metadata) in [
        (Children::Link, MetadataMode::Healthy),
        (Children::Versions, MetadataMode::VersionFault),
    ] {
        let broken = move || {
            fixture_storage(Fixture {
                fails: false,
                first_page: false,
                metadata,
                children,
            })
        };
        let connect: TraversalReconnect = Arc::new(move || async move { Ok(broken()) }.boxed());
        let (session, status) = recoverable_traversal(
            &broken(),
            TraversalRequest::new(StoragePath::root()).observation_plan(
                ObservationPlan::default().with_tags(ObservationMode::BestEffort),
            ),
            policy(),
            connect,
        )
        .unwrap();
        let (_, outcome) = timeout(Duration::from_millis(500), drain(session))
            .await
            .unwrap();
        assert!(matches!(outcome, Err(TraversalTerminalFailure::Session(_))));
        assert_eq!(
            status.failure_signal().failure().unwrap().kind,
            RemoteScanFailureKind::ConnectionLost
        );
    }
}

#[path = "recovery_progress_tests.rs"]
mod recovery_progress_tests;

#[tokio::test]
async fn a_fault_during_consumer_wait_starts_its_deadline_at_detection() {
    let (mut recovery, _output, _) = waiting_recovery();
    let signal = ScanFailureSignal::new(BackendKind::Cifs, None);
    let failed = signal.clone();
    recovery
        .producer
        .items
        .send_one(permanent_item())
        .await
        .unwrap();
    let waiting = tokio::spawn(async move {
        let result = recovery.forward(permanent_item(), &signal).await;
        (recovery, result)
    });
    sleep(Duration::from_millis(160)).await;
    assert!(!waiting.is_finished());
    failed.publish_failure(RemoteScanFailure {
        backend: BackendKind::Cifs,
        operation: Operation::Traverse,
        path: StoragePath::root(),
        kind: RemoteScanFailureKind::ConnectionLost,
        operation_timeout: Duration::ZERO,
        source: None,
    });
    let (recovery, result) = timeout(Duration::from_millis(300), waiting)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err());
    assert!(recovery.window.as_ref().unwrap().budget.is_exhausted());
}

#[tokio::test]
async fn known_disconnect_deadline_is_not_paused_by_blocked_output() {
    let (mut recovery, _output, signal) = waiting_recovery();
    recovery
        .producer
        .items
        .send_one(permanent_item())
        .await
        .unwrap();
    let result = timeout(
        Duration::from_millis(300),
        recovery.forward(permanent_item(), &signal),
    )
    .await;
    assert!(
        result.is_ok(),
        "known disconnect exceeded its recovery window while output was blocked"
    );
    assert!(result.unwrap().is_err());
    assert!(recovery.window.as_ref().unwrap().budget.is_exhausted());
}
