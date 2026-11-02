use super::*;
use tokio::time::sleep;

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
    let connect: ScanReconnect = Arc::new(|| {
        async {
            Ok(fixture_storage(ReplayTree {
                failure: ReplayFailure::None,
                prefix_delay: Duration::from_millis(160),
            }))
        }
        .boxed()
    });
    let (session, status) = recoverable_scan(
        &initial,
        TraversalRequest::new(StoragePath::root())
            .observation_plan(ObservationPlan::default().with_tags(ObservationMode::BestEffort)),
        ScanRecoveryPolicy {
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
    let connect: ScanReconnect = Arc::new(move || async move { Ok(broken()) }.boxed());
    let (session, status) = recoverable_scan(
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
async fn healthy_listing_prefix_can_outlast_the_budget_before_the_faulting_page() {
    let initial = fixture_storage(ReplayTree {
        failure: ReplayFailure::SecondPage,
        prefix_delay: Duration::ZERO,
    });
    let connect: ScanReconnect = Arc::new(|| {
        async {
            Ok(fixture_storage(ReplayTree {
                failure: ReplayFailure::None,
                prefix_delay: Duration::from_millis(160),
            }))
        }
        .boxed()
    });
    let (session, status) = recoverable_scan(
        &initial,
        TraversalRequest::new(StoragePath::root()),
        ScanRecoveryPolicy {
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
    assert!(paths.iter().any(|path| path == "z"));
    assert!(status.failure_signal().failure().is_none());
}

#[tokio::test]
async fn a_failed_slow_replay_charges_the_original_failure_budget() {
    let initial = fixture_storage(ReplayTree {
        failure: ReplayFailure::Metadata,
        prefix_delay: Duration::ZERO,
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let connects = calls.clone();
    let connect: ScanReconnect = Arc::new(move || {
        connects.fetch_add(1, Ordering::Relaxed);
        async {
            Ok(fixture_storage(ReplayTree {
                failure: ReplayFailure::FirstPage,
                prefix_delay: Duration::from_millis(160),
            }))
        }
        .boxed()
    });
    let (session, status) = recoverable_scan(
        &initial,
        TraversalRequest::new(StoragePath::root())
            .observation_plan(ObservationPlan::default().with_tags(ObservationMode::BestEffort)),
        ScanRecoveryPolicy {
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
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    let failure = status.failure_signal().failure().unwrap();
    assert_eq!(failure.operation, Operation::Observe);
    assert_eq!(failure.kind, RemoteScanFailureKind::ConnectionLost);
}

#[tokio::test]
async fn an_eof_before_the_failed_listing_page_recovers_the_current_tree() {
    let initial = fixture_storage(ReplayTree {
        failure: ReplayFailure::SecondPage,
        prefix_delay: Duration::ZERO,
    });
    let connect: ScanReconnect = Arc::new(|| {
        async {
            Ok(fixture_storage(ReplayTree {
                failure: ReplayFailure::Empty,
                prefix_delay: Duration::ZERO,
            }))
        }
        .boxed()
    });
    let (session, status) = recoverable_scan(
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
        ScanRecoveryState::Running { recoveries: 1 }
    ));
    assert!(status.failure_signal().failure().is_none());
}
