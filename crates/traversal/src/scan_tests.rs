use std::future::pending;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

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
        .operation(Operation::Observe, &StoragePath::root(), async move {
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
        _: &StoragePath,
        _: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        Ok(MetadataObservations::default())
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
    let role = Arc::new(Fixture { backend, denied });
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
        Operation::Traverse,
        &StoragePath::root(),
        RemoteScanFailureKind::ConnectionLost,
        None,
    );
    signal.record(
        Operation::Observe,
        &StoragePath::root(),
        RemoteScanFailureKind::OperationTimedOut,
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
        .operation(Operation::Traverse, &StoragePath::root(), async {
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
        })
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
        matches!(signal.checked_page(&StoragePath::root(), Some(page)),
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
        matches!(signal.checked_metadata(&StoragePath::root(), metadata),
        Err(StorageRoleFailure::Session(error)) if error.class() == FailureClass::Connectivity)
    );
    assert!(signal.failure().is_some());
}
