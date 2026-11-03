use std::future::pending;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::{sync::Notify, time::sleep};

use super::*;
use crate::model::{BackendIdentity, EntryKind, EntryOperationFailure, IdentityStrength};
use crate::traversal::{
    TraversalItem, TraversalRequest, TraversalSource, TraversalTerminalFailure,
};
use MetadataMutation;

struct CleanupEvidence {
    closed: Arc<AtomicBool>,
    abandoned: Arc<AtomicBool>,
}
impl Drop for CleanupEvidence {
    fn drop(&mut self) {
        if !self.closed.load(Ordering::SeqCst) {
            self.abandoned.store(true, Ordering::SeqCst);
        }
    }
}

#[tokio::test]
async fn timed_out_operation_keeps_ownership_until_backend_cleanup_finishes() {
    let ready = Arc::new(Notify::new());
    let finished = Arc::new(Notify::new());
    let closed = Arc::new(AtomicBool::new(false));
    let abandoned = Arc::new(AtomicBool::new(false));
    let evidence = CleanupEvidence {
        closed: closed.clone(),
        abandoned: abandoned.clone(),
    };
    let release = ready.clone();
    let done = finished.clone();
    let signal = ScanFailureSignal::new(BackendKind::Cifs, Some(Duration::from_millis(10)));
    let result = signal
        .operation(&ScanRead::Stat, &StoragePath::root(), async move {
            let resource = evidence;
            release.notified().await;
            resource.closed.store(true, Ordering::SeqCst);
            done.notify_one();
            Ok(())
        })
        .await;
    assert!(result.is_err());
    assert!(
        !abandoned.load(Ordering::SeqCst),
        "deadline must retain the resource-owning future"
    );
    assert!(!closed.load(Ordering::SeqCst));
    ready.notify_one();
    timeout(Duration::from_millis(250), finished.notified())
        .await
        .unwrap();
    assert!(closed.load(Ordering::SeqCst));
    assert!(!abandoned.load(Ordering::SeqCst));
}

struct Fixture {
    backend: BackendKind,
    denied: bool,
    conflict: bool,
}
#[async_trait]
impl Namespace for Fixture {
    async fn stat(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        Ok(SourceDescriptor::new(
            path.clone(),
            EntryKind::Directory,
            None,
            SourceIdentity::new(
                BackendIdentity::new(self.backend, "guard-fixture").unwrap(),
                IdentityStrength::PathScoped,
                "root",
            )
            .unwrap(),
        ))
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        if self.denied {
            ListingStream::failed(
                request.path.clone(),
                StorageRoleFailure::Entry(
                    EntryOperationFailure::new(
                        request.path,
                        Operation::Traverse,
                        FailureClass::PermissionDenied,
                        Transience::Permanent,
                        "denied",
                    )
                    .unwrap(),
                ),
            )
        } else {
            ListingStream::from_producer(request.path, |_sink| async { pending::<()>().await })
        }
    }
}
#[async_trait]
impl Metadata for Fixture {
    async fn observe(
        &self,
        path: &StoragePath,
        _: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        if self.conflict {
            return Err(StorageRoleFailure::Entry(
                EntryOperationFailure::new(
                    path.clone(),
                    Operation::Metadata,
                    FailureClass::Conflict,
                    Transience::Transient,
                    "source identity changed",
                )
                .unwrap(),
            ));
        }
        Ok(MetadataObservations::default())
    }
    async fn observe_copy_bound_version(
        &self,
        path: &StoragePath,
        _: &SourceIdentity,
        _: &SourceVersion,
        plan: ObservationPlan,
    ) -> Result<CopiedMetadataObservation, StorageRoleFailure> {
        self.observe(path, plan)
            .await
            .map(|observations| CopiedMetadataObservation {
                observations,
                mode_without_ownership: None,
                owner_names_unmapped: false,
            })
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
fn roles(backend: BackendKind, denied: bool) -> Storage {
    fixture_roles(backend, denied, false)
}
fn fixture_roles(backend: BackendKind, denied: bool, conflict: bool) -> Storage {
    let role = Arc::new(Fixture {
        backend,
        denied,
        conflict,
    });
    let unsupported =
        CapabilityAvailability::Unsupported(UnsupportedReason::new("test only").unwrap());
    StorageBuilder::new(
        BackendIdentity::new(backend, "guard-fixture").unwrap(),
        BackendCapabilities::new(
            unsupported.clone(),
            unsupported,
            CapabilityAvailability::Supported,
            CapabilityAvailability::Supported,
        ),
    )
    .namespace(role.clone())
    .metadata(role)
    .build()
    .unwrap()
}
#[tokio::test]
async fn all_remote_backends_report_operation_timeout_without_waiting_for_native_retries() {
    for backend in [
        BackendKind::Nfs,
        BackendKind::S3,
        BackendKind::Cifs,
        BackendKind::Hdfs,
    ] {
        let (source, signal) =
            ScanFailureSignal::source(&roles(backend, false), Some(Duration::from_millis(20)))
                .unwrap();
        let mut session = source.traverse(TraversalRequest::new(StoragePath::root()));
        let result = timeout(Duration::from_millis(250), async {
            while session.next_item().await.is_some() {}
            session.finish().await
        })
        .await
        .unwrap();
        assert!(
            matches!(result,Err(TraversalTerminalFailure::Session(failure)) if failure.class()==FailureClass::Timeout)
        );
        let error = signal.failure().unwrap();
        assert_eq!(error.backend, backend);
        assert_eq!(error.code(), "storage_operation_timeout");
    }
}
#[tokio::test]
async fn permission_denied_never_becomes_a_network_failure() {
    let (source, signal) = ScanFailureSignal::source(
        &roles(BackendKind::Cifs, true),
        Some(Duration::from_millis(20)),
    )
    .unwrap();
    let mut session = source.traverse(TraversalRequest::new(StoragePath::root()));
    let mut denied = false;
    while let Some(item) = session.next_item().await {
        if let TraversalItem::Failure(failure) = item {
            denied |= failure.failure.class() == FailureClass::PermissionDenied;
        }
    }
    session.finish().await.unwrap();
    assert!(denied);
    assert!(signal.failure().is_none());
}
#[tokio::test]
async fn already_failed_notification_preserves_the_first_failure() {
    let signal = ScanFailureSignal::new(BackendKind::Nfs, Some(Duration::from_secs(3)));
    signal.record(
        &ScanRead::listing(
            &ListRequest::new(StoragePath::root(), CancellationToken::new()),
            None,
        ),
        &StoragePath::root(),
        RemoteScanFailureKind::ConnectionLost,
        None,
        None,
    );
    signal.record(
        &ScanRead::Stat,
        &StoragePath::root(),
        RemoteScanFailureKind::OperationTimedOut,
        None,
        None,
    );
    timeout(Duration::from_millis(50), signal.failed())
        .await
        .unwrap();
    assert_eq!(signal.failure().unwrap().code(), "storage_connection_lost");
}

#[tokio::test]
async fn native_entry_network_errors_stop_the_session_without_operation_timeout() {
    let signal = ScanFailureSignal::new(BackendKind::Nfs, None);
    let result: Result<(), _> = signal
        .operation(
            &ScanRead::listing(
                &ListRequest::new(StoragePath::root(), CancellationToken::new()),
                None,
            ),
            &StoragePath::root(),
            async {
                Err(StorageRoleFailure::Entry(
                    EntryOperationFailure::new(
                        StoragePath::root(),
                        Operation::Traverse,
                        FailureClass::Connectivity,
                        Transience::Transient,
                        "native reset",
                    )
                    .unwrap(),
                ))
            },
        )
        .await;
    assert!(
        matches!(result, Err(StorageRoleFailure::Session(error)) if error.class() == FailureClass::Connectivity)
    );
    let saved = signal.failure().unwrap();
    assert_eq!(saved.code(), "storage_connection_lost");
    assert!(matches!(saved.source, Some(StorageRoleFailure::Entry(_))));
}

#[test]
fn child_network_errors_in_successful_pages_cannot_complete_the_directory() {
    use crate::storage::ChildFailure;
    let signal = ScanFailureSignal::new(BackendKind::Cifs, None);
    let failure = EntryOperationFailure::new(
        StoragePath::new("/child").unwrap(),
        Operation::Traverse,
        FailureClass::Timeout,
        Transience::Transient,
        "native child timeout",
    )
    .unwrap();
    let page = ListingPage::new(
        Vec::new(),
        vec![ChildFailure::from_failure(&StoragePath::root(), failure)],
        0,
    );
    assert!(
        matches!(signal.checked_page(&ScanRead::listing(&ListRequest::new(StoragePath::root(), CancellationToken::new()), None), &StoragePath::root(), Some(page)),
        Err(StorageRoleFailure::Session(error)) if error.class() == FailureClass::Timeout)
    );
    assert_eq!(
        signal.failure().unwrap().code(),
        "storage_operation_timeout"
    );
    assert!(signal.successful_listing_at().is_none());
}

#[test]
fn retained_metadata_network_errors_cannot_be_silently_skipped() {
    use crate::model::{MetadataFamily, MetadataObservationFailure};
    let signal = ScanFailureSignal::new(BackendKind::Cifs, None);
    let mut metadata = MetadataObservations::default();
    metadata.retain_failure_summary(MetadataObservationFailure {
        family: MetadataFamily::Acl,
        class: FailureClass::Connectivity,
        transience: Transience::Transient,
    });
    assert!(
        matches!(signal.checked_metadata(&ScanRead::Metadata(ObservationPlan::default()), &StoragePath::root(), metadata, None),
        Err(StorageRoleFailure::Session(error)) if error.class() == FailureClass::Connectivity)
    );
    assert!(signal.failure().is_some());
}

#[tokio::test]
async fn a_guarded_target_read_obeys_the_recovery_deadline() {
    let signal = ScanFailureSignal::new(BackendKind::Cifs, None);
    let budget = RecoveryBudget::new(Instant::now() + Duration::from_millis(80));
    signal.watch_recovery(
        RecoveryTarget {
            read: ScanRead::Stat,
            path: StoragePath::root(),
            inline_descriptor: None,
        },
        budget.clone(),
    );
    let ready = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let started = ready.clone();
    let done = release.clone();
    let guarded = signal.clone();
    let call = tokio::spawn(async move {
        guarded
            .operation(&ScanRead::Stat, &StoragePath::root(), async move {
                started.notify_one();
                done.notified().await;
                Ok(())
            })
            .await
    });
    ready.notified().await;
    sleep(Duration::from_millis(160)).await;
    assert!(call.is_finished());
    assert_eq!(
        signal.failure().unwrap().kind,
        RemoteScanFailureKind::OperationTimedOut
    );
    release.notify_one();
    assert!(
        matches!(timeout(Duration::from_millis(250), call).await.unwrap().unwrap(),
        Err(StorageRoleFailure::Session(error)) if error.class() == FailureClass::Timeout)
    );
}

#[tokio::test]
async fn changed_inline_descriptor_recovers_without_reusing_stale_facts() {
    let storage = roles(BackendKind::Cifs, false);
    let (_, signal) = ScanFailureSignal::source(&storage, None).unwrap();
    let path = StoragePath::root();
    let mut original = storage.namespace().unwrap().stat(&path).await.unwrap();
    original.kind = EntryKind::File;
    let target = RecoveryTarget {
        read: ScanRead::InlineMetadata(ObservationPlan::default()),
        path,
        inline_descriptor: Some(Arc::new(original)),
    };
    signal.watch_recovery(
        target.clone(),
        RecoveryBudget::new(Instant::now() + Duration::from_secs(1)),
    );
    assert!(
        target
            .probe(&storage, &signal, CancellationToken::new())
            .await
    );
    assert!(signal.failure().is_none());
}

#[tokio::test]
async fn replaced_bound_identity_recovers_with_a_fresh_checkpoint_replay() {
    let storage = fixture_roles(BackendKind::Cifs, false, true);
    let (_, signal) = ScanFailureSignal::source(&storage, None).unwrap();
    let path = StoragePath::root();
    let identity = SourceIdentity::new(
        BackendIdentity::new(BackendKind::Cifs, "guard-fixture").unwrap(),
        IdentityStrength::PathScoped,
        "replaced-old-identity",
    )
    .unwrap();
    let target = RecoveryTarget {
        read: ScanRead::BoundMetadata(ObservationPlan::default(), identity),
        path,
        inline_descriptor: None,
    };
    signal.watch_recovery(
        target.clone(),
        RecoveryBudget::new(Instant::now() + Duration::from_secs(1)),
    );
    assert!(
        target
            .probe(&storage, &signal, CancellationToken::new())
            .await
    );
    assert!(signal.failure().is_none());
}

#[tokio::test]
async fn an_unchanged_identity_conflict_does_not_prove_network_recovery() {
    let storage = fixture_roles(BackendKind::Cifs, false, true);
    let (_, signal) = ScanFailureSignal::source(&storage, None).unwrap();
    let path = StoragePath::root();
    let identity = storage
        .namespace()
        .unwrap()
        .stat(&path)
        .await
        .unwrap()
        .source_identity;
    let target = RecoveryTarget {
        read: ScanRead::BoundMetadata(ObservationPlan::default(), identity),
        path,
        inline_descriptor: None,
    };
    signal.watch_recovery(
        target.clone(),
        RecoveryBudget::new(Instant::now() + Duration::from_secs(1)),
    );
    assert!(
        !target
            .probe(&storage, &signal, CancellationToken::new())
            .await
    );
    assert!(signal.recovered_at().is_none());
}

#[tokio::test]
async fn current_descriptor_does_not_prove_a_historical_version_changed() {
    let storage = fixture_roles(BackendKind::Cifs, false, true);
    let (_, signal) = ScanFailureSignal::source(&storage, None).unwrap();
    let path = StoragePath::root();
    let identity = storage
        .namespace()
        .unwrap()
        .stat(&path)
        .await
        .unwrap()
        .source_identity;
    let target = RecoveryTarget {
        read: ScanRead::VersionMetadata(
            ObservationPlan::default(),
            SourceVersion::Id("v1".into()),
            identity,
        ),
        path,
        inline_descriptor: None,
    };
    signal.watch_recovery(
        target.clone(),
        RecoveryBudget::new(Instant::now() + Duration::from_secs(1)),
    );
    assert!(
        !target
            .probe(&storage, &signal, CancellationToken::new())
            .await
    );
    assert!(signal.recovered_at().is_none());
}
