use tokio::time::sleep;

use super::*;
use crate::traversal::TraversalSource;

struct ReplayTree {
    failure: ReplayFailure,
    prefix_delay: Duration,
}

#[derive(Clone, Copy)]
enum ReplayFailure {
    Metadata,
    SecondPage,
    FirstPage,
    Empty,
    None,
}

#[async_trait]
impl Namespace for ReplayTree {
    fn list(&self, request: ListRequest) -> ListingStream {
        let failure = self.failure;
        let delay = self.prefix_delay;
        ListingStream::from_producer(request.path.clone(), move |sink| async move {
            if matches!(failure, ReplayFailure::Empty) {
                return;
            }
            if matches!(failure, ReplayFailure::FirstPage) {
                sleep(delay).await;
                return sink.fail(network_failure()).await;
            }
            if !matches!(failure, ReplayFailure::Metadata) {
                sleep(delay).await;
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
            if matches!(failure, ReplayFailure::SecondPage) {
                sleep(Duration::from_millis(25)).await;
                return sink.fail(network_failure()).await;
            }
            let page = ListingPage::from_descriptors(
                &request.path,
                vec![descriptor(StoragePath::new("z").unwrap(), EntryKind::File)],
                vec![],
                0,
            );
            let _ = sink.send(page).await;
        })
    }
}

#[async_trait]
impl Metadata for ReplayTree {
    async fn observe(
        &self,
        _: &StoragePath,
        _: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        if matches!(self.failure, ReplayFailure::Metadata) {
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

#[tokio::test]
async fn healthy_replay_slower_than_recovery_window_reaches_the_failed_target() {
    let initial = fixture_storage(ReplayTree {
        failure: ReplayFailure::Metadata,
        prefix_delay: Duration::ZERO,
    });
    let connect: TraversalReconnect = Arc::new(|| {
        async {
            Ok(fixture_storage(ReplayTree {
                failure: ReplayFailure::None,
                prefix_delay: Duration::from_millis(160),
            }))
        }
        .boxed()
    });
    let (session, status) = recoverable_traversal(
        &initial,
        TraversalRequest::new(StoragePath::root())
            .observation_plan(ObservationPlan::default().with_tags(ObservationMode::BestEffort)),
        TraversalRecoveryPolicy {
            resume: TraversalRecoveryResume::Checkpoint,
            operation_timeout: Some(Duration::from_secs(1)),
            ..policy()
        },
        connect,
    )
    .unwrap();
    let (paths, outcome) = timeout(Duration::from_secs(1), drain(session))
        .await
        .unwrap();
    assert!(matches!(outcome, Ok(TraversalOutcome::Completed(_))));
    assert!(paths.iter().any(|path| path == "a"));
    assert!(paths.iter().any(|path| path == "z"));
    assert!(status.failure_signal().failure().is_none());
}

#[tokio::test]
async fn a_successful_first_page_cannot_renew_a_persistent_second_page_fault() {
    let broken = || {
        fixture_storage(ReplayTree {
            failure: ReplayFailure::SecondPage,
            prefix_delay: Duration::ZERO,
        })
    };
    let connect: TraversalReconnect = Arc::new(move || async move { Ok(broken()) }.boxed());
    let (session, status) = recoverable_traversal(
        &broken(),
        TraversalRequest::new(StoragePath::root()),
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

#[tokio::test]
async fn a_listing_target_unverifiable_within_the_window_fails_safely() {
    let initial = fixture_storage(ReplayTree {
        failure: ReplayFailure::SecondPage,
        prefix_delay: Duration::ZERO,
    });
    let connect: TraversalReconnect = Arc::new(|| {
        async {
            Ok(fixture_storage(ReplayTree {
                failure: ReplayFailure::None,
                prefix_delay: Duration::from_millis(160),
            }))
        }
        .boxed()
    });
    let (session, status) = recoverable_traversal(
        &initial,
        TraversalRequest::new(StoragePath::root()),
        TraversalRecoveryPolicy {
            resume: TraversalRecoveryResume::Checkpoint,
            operation_timeout: Some(Duration::from_secs(1)),
            ..policy()
        },
        connect,
    )
    .unwrap();
    let (_, outcome) = timeout(Duration::from_secs(1), drain(session))
        .await
        .unwrap();
    assert!(matches!(outcome, Err(TraversalTerminalFailure::Session(_))));
    assert_eq!(
        status.failure_signal().failure().unwrap().kind,
        RemoteScanFailureKind::ConnectionLost
    );
}

#[tokio::test]
async fn a_new_failure_after_verified_recovery_has_its_own_window() {
    let initial = fixture_storage(ReplayTree {
        failure: ReplayFailure::Metadata,
        prefix_delay: Duration::ZERO,
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let connects = calls.clone();
    let connect: TraversalReconnect = Arc::new(move || {
        connects.fetch_add(1, Ordering::Relaxed);
        async {
            Ok(fixture_storage(ReplayTree {
                failure: ReplayFailure::FirstPage,
                prefix_delay: Duration::from_millis(160),
            }))
        }
        .boxed()
    });
    let (session, status) = recoverable_traversal(
        &initial,
        TraversalRequest::new(StoragePath::root())
            .observation_plan(ObservationPlan::default().with_tags(ObservationMode::BestEffort)),
        TraversalRecoveryPolicy {
            resume: TraversalRecoveryResume::Checkpoint,
            operation_timeout: Some(Duration::from_secs(1)),
            ..policy()
        },
        connect,
    )
    .unwrap();
    let (_, outcome) = timeout(Duration::from_secs(1), drain(session))
        .await
        .unwrap();
    assert!(matches!(outcome, Err(TraversalTerminalFailure::Session(_))));
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    let failure = status.failure_signal().failure().unwrap();
    assert_eq!(failure.operation, Operation::Traverse);
    assert_eq!(failure.kind, RemoteScanFailureKind::ConnectionLost);
}

#[tokio::test]
async fn an_eof_before_the_failed_listing_page_recovers_the_current_tree() {
    let initial = fixture_storage(ReplayTree {
        failure: ReplayFailure::SecondPage,
        prefix_delay: Duration::ZERO,
    });
    let connect: TraversalReconnect = Arc::new(|| {
        async {
            Ok(fixture_storage(ReplayTree {
                failure: ReplayFailure::Empty,
                prefix_delay: Duration::ZERO,
            }))
        }
        .boxed()
    });
    let (session, status) = recoverable_traversal(
        &initial,
        TraversalRequest::new(StoragePath::root()),
        policy(),
        connect,
    )
    .unwrap();
    let (_, outcome) = timeout(Duration::from_secs(1), drain(session))
        .await
        .unwrap();
    assert!(matches!(outcome, Ok(TraversalOutcome::Completed(_))));
    assert!(matches!(
        *status.subscribe().borrow(),
        TraversalRecoveryState::Running { recoveries: 1 }
    ));
    assert!(status.failure_signal().failure().is_none());
}

#[tokio::test]
async fn verified_target_ends_recovering_before_slow_checkpoint_rebuild() {
    let initial = fixture_storage(ReplayTree {
        failure: ReplayFailure::Metadata,
        prefix_delay: Duration::ZERO,
    });
    let connect: TraversalReconnect = Arc::new(|| {
        async {
            Ok(fixture_storage(ReplayTree {
                failure: ReplayFailure::None,
                prefix_delay: Duration::from_millis(160),
            }))
        }
        .boxed()
    });
    let (session, status) = recoverable_traversal(
        &initial,
        TraversalRequest::new(StoragePath::root())
            .observation_plan(ObservationPlan::default().with_tags(ObservationMode::BestEffort)),
        TraversalRecoveryPolicy {
            resume: TraversalRecoveryResume::Checkpoint,
            operation_timeout: Some(Duration::from_secs(1)),
            ..policy()
        },
        connect,
    )
    .unwrap();
    let mut updates = status.subscribe();
    let recovered = async {
        loop {
            if matches!(
                *updates.borrow_and_update(),
                TraversalRecoveryState::Running { recoveries: 1 }
            ) {
                break;
            }
            updates.changed().await.unwrap();
        }
    };
    let (verified, outcome) = tokio::join!(
        timeout(policy().recovery_window, recovered),
        timeout(Duration::from_secs(1), drain(session)),
    );
    assert!(
        verified.is_ok(),
        "fault target must be verified before slow healthy checkpoint rebuilding"
    );
    assert!(matches!(
        outcome.unwrap().1,
        Ok(TraversalOutcome::Completed(_))
    ));
}

async fn recorded_metadata_fault() -> ScanFailureSignal {
    let initial = fixture_storage(ReplayTree {
        failure: ReplayFailure::Metadata,
        prefix_delay: Duration::ZERO,
    });
    let (source, signal) =
        ScanFailureSignal::source(&initial, Some(Duration::from_secs(1))).unwrap();
    let session = source.traverse(
        TraversalRequest::new(StoragePath::root())
            .observation_plan(ObservationPlan::default().with_tags(ObservationMode::BestEffort)),
    );
    assert!(drain(session).await.1.is_err());
    signal
}

#[tokio::test]
async fn target_verification_does_not_wait_for_the_old_output_consumer() {
    let signal = recorded_metadata_fault().await;
    let (mut recovery, mut output, _) = waiting_recovery();
    recovery.failure = ScanFailureSignal::new(BackendKind::Cifs, None);
    recovery.reconnect = Arc::new(|| {
        async {
            Ok(fixture_storage(ReplayTree {
                failure: ReplayFailure::None,
                prefix_delay: Duration::from_millis(160),
            }))
        }
        .boxed()
    });
    let counters = recovery.counters.clone();
    let mut updates = recovery.state.subscribe();
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
    let verified = timeout(policy().recovery_window, async {
        loop {
            if matches!(
                *updates.borrow_and_update(),
                TraversalRecoveryState::Running { recoveries: 1 }
            ) {
                break;
            }
            updates.changed().await.unwrap();
        }
    })
    .await;
    assert!(
        verified.is_ok(),
        "original fault must recover while the old output queue is blocked"
    );
    assert_eq!(counters.snapshot().successful_recoveries, 1);
    sleep(Duration::from_millis(160)).await;
    assert!(!waiting.is_finished());
    output.recv().await.unwrap();
    output.recv().await.unwrap();
    let (recovery, result) = waiting.await.unwrap();
    assert!(result.is_ok());
    assert!(recovery.failure.failure().is_none());
    assert_eq!(counters.snapshot().successful_recoveries, 1);
}
