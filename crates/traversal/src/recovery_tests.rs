use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::FutureExt;
use futures::future::pending;
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::{
    BackendIdentity, BackendKind, EntryKind, IdentityStrength, MetadataObservations,
    ObservationMode, ObservationPlan, Operation, SourceIdentity, StoragePath,
};
use crate::storage::{
    BackendCapabilities, CapabilityAvailability, ListRequest, ListingPage, ListingStream, Metadata,
    MetadataMutation, Namespace, SourceDescriptor, StorageBuilder, UnsupportedReason,
};
use crate::traversal::{RemoteScanFailureKind, TraversalItem};

struct Fixture {
    fails: bool,
    first_page: bool,
    metadata_fails: bool,
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
    async fn stat(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        Ok(descriptor(path.clone(), EntryKind::Directory))
    }

    fn list(&self, request: ListRequest) -> ListingStream {
        let (fails, first_page) = (self.fails, self.first_page);
        ListingStream::from_producer(request.path.clone(), move |sink| async move {
            if first_page || !fails {
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
            } else {
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
}

#[async_trait]
impl Metadata for Fixture {
    async fn observe(
        &self,
        _: &StoragePath,
        _: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        if self.metadata_fails {
            Err(network_failure())
        } else {
            Ok(MetadataObservations::default())
        }
    }
    async fn apply(
        &self,
        _: &StoragePath,
        _: MetadataMutation,
        _: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        unreachable!()
    }
}

fn storage(fails: bool, first_page: bool) -> Storage {
    fixture_storage(Fixture {
        fails,
        first_page,
        metadata_fails: false,
    })
}

fn fixture_storage(fixture: Fixture) -> Storage {
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

fn policy() -> ScanRecoveryPolicy {
    ScanRecoveryPolicy {
        operation_timeout: Some(Duration::from_millis(100)),
        recovery_window: Duration::from_millis(80),
    }
}

fn reconnect(fails: bool, calls: Arc<AtomicUsize>) -> ScanReconnect {
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
    let (session, status) = recoverable_scan(
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
        ScanRecoveryState::Running { recoveries: 1 }
    ));
}

#[tokio::test]
async fn repeated_immediate_failures_share_one_absolute_window() {
    let calls = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();
    let (session, status) = recoverable_scan(
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
        ScanRecoveryState::Failed { .. }
    ));
}

#[tokio::test]
async fn a_hanging_reconnect_expires_and_keeps_the_original_reset_cause() {
    let connect: ScanReconnect = Arc::new(|| pending().boxed());
    let (session, status) = recoverable_scan(
        &storage(true, false),
        TraversalRequest::new(StoragePath::root()),
        policy(),
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
    let failure = status.failure_signal().failure().unwrap();
    assert_eq!(failure.kind, RemoteScanFailureKind::ConnectionLost);
    assert_eq!(failure.path, StoragePath::root());
}

#[tokio::test]
async fn cancellation_during_recovery_is_cancelled_without_a_final_network_fault() {
    let cancel = CancellationToken::new();
    let connect: ScanReconnect = Arc::new(|| pending().boxed());
    let (session, status) = recoverable_scan(
        &storage(true, false),
        TraversalRequest::new(StoragePath::root()).cancel(cancel.clone()),
        policy(),
        connect,
    )
    .unwrap();
    let mut state = status.subscribe();
    let draining = tokio::spawn(drain(session));
    while !matches!(*state.borrow(), ScanRecoveryState::Recovering { .. }) {
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
    let selected = ScanRecoveryPolicy {
        operation_timeout: None,
        ..policy()
    };
    let (session, status) = recoverable_scan(
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
    let connect: ScanReconnect = Arc::new(|| pending().boxed());
    let request = TraversalRequest::new(StoragePath::root())
        .resumable(Resumable::on())
        .cancel(cancel.clone());
    let (mut session, status) =
        recoverable_scan(&storage(true, false), request, policy(), connect).unwrap();
    let mut state = status.subscribe();
    let draining = tokio::spawn(async move {
        let mut checkpoint = false;
        while let Some(item) = session.next_item().await {
            checkpoint |= matches!(item, TraversalItem::Checkpoint(_));
        }
        (checkpoint, session.finish().await)
    });
    while !matches!(*state.borrow(), ScanRecoveryState::Recovering { .. }) {
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
            metadata_fails: true,
        })
    };
    let connect: ScanReconnect = Arc::new(move || async move { Ok(broken()) }.boxed());
    let started = Instant::now();
    let (session, status) = recoverable_scan(
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
    let (state, _) = watch::channel(ScanRecoveryState::Running { recoveries: 0 });
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
            restoring: false,
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
async fn consumer_wait_longer_than_recovery_budget_does_not_exhaust_it() {
    let (mut recovery, mut output, signal) = waiting_recovery();
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
    tokio::time::sleep(Duration::from_millis(160)).await;
    assert!(!waiting.is_finished());
    output.recv().await.unwrap();
    output.recv().await.unwrap();
    let (recovery, result) = waiting.await.unwrap();
    assert!(result.is_ok());
    assert!(recovery.window.as_ref().unwrap().deadline > Instant::now());
}
