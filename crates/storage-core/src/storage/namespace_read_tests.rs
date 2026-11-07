//! 确定性验证截止时间及分类；真实协议故障由独立 S3 runner 验证。
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

use super::{NamespaceReadCause, NamespaceReadFailure, NamespaceReadPolicy, read_namespace_entry};
use crate::model::{
    BackendIdentity, BackendKind, EntryOperationFailure, FailureClass, Operation, StoragePath,
    Transience,
};
use crate::storage::{
    BackendCapabilities, CapabilityAvailability, ListRequest, ListingStream, Namespace,
    SourceDescriptor, Storage, StorageBuilder, StorageRoleFailure, UnsupportedReason,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct Fault {
    calls: AtomicU32,
    class: FailureClass,
    recover: bool,
    stall: bool,
}

#[async_trait]
impl Namespace for Fault {
    async fn stat(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        let calls = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.stall {
            sleep(Duration::from_secs(100)).await;
        }
        let class = if self.recover && calls > 0 {
            FailureClass::NotFound
        } else {
            self.class
        };
        Err(StorageRoleFailure::Entry(
            EntryOperationFailure::new(
                path.clone(),
                Operation::Observe,
                class,
                Transience::Transient,
                "injected typed stat refusal",
            )
            .unwrap_or_else(|error| panic!("{error}")),
        ))
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        ListingStream::from_pages(request.path, [])
    }
}

fn fixture(class: FailureClass, recover: bool, stall: bool) -> TestResult<(Storage, Arc<Fault>)> {
    let fault = Arc::new(Fault {
        calls: AtomicU32::new(0),
        class,
        recover,
        stall,
    });
    let unavailable = CapabilityAvailability::Unsupported(UnsupportedReason::new("unused")?);
    let storage = StorageBuilder::new(
        BackendIdentity::new(BackendKind::Local, "namespace-read-test")?,
        BackendCapabilities::new(
            unavailable.clone(),
            unavailable.clone(),
            CapabilityAvailability::Supported,
            unavailable,
        ),
    )
    .namespace(fault.clone())
    .build()?;
    Ok((storage, fault))
}

fn policy() -> NamespaceReadPolicy {
    NamespaceReadPolicy {
        operation_timeout: Duration::from_secs(1),
        recovery_window: Duration::from_secs(3),
        retry_interval: Duration::from_millis(100),
    }
}

#[tokio::test(start_paused = true)]
async fn only_typed_not_found_proves_path_absence_and_a_recovered_fault_is_not_an_error()
-> TestResult {
    let path = StoragePath::new("file")?;
    let (storage, fault) = fixture(FailureClass::Connectivity, true, false)?;
    let outcome =
        read_namespace_entry(&storage, &path, policy(), &CancellationToken::new()).await?;
    assert!(outcome.descriptor.is_none());
    assert_eq!(outcome.attempts, 2);
    assert_eq!(fault.calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn permission_and_authentication_are_final_even_when_adapter_marks_them_transient()
-> TestResult {
    for class in [
        FailureClass::PermissionDenied,
        FailureClass::Authentication,
        FailureClass::Unsupported,
    ] {
        let (storage, fault) = fixture(class, false, false)?;
        let error = read_namespace_entry(
            &storage,
            &StoragePath::new("file")?,
            policy(),
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("expected refusal")?;
        assert!(
            matches!(error, NamespaceReadFailure::Operation { last, attempts: 1, .. } if matches!(*last, NamespaceReadCause::Role(StorageRoleFailure::Entry(ref e)) if e.class()==class))
        );
        assert_eq!(fault.calls.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn stalled_requests_cannot_reset_the_window_and_cancel_interrupts_inflight_read() -> TestResult
{
    let (storage, _) = fixture(FailureClass::Connectivity, false, true)?;
    let start = Instant::now();
    assert!(matches!(
        read_namespace_entry(
            &storage,
            &StoragePath::new("file")?,
            policy(),
            &CancellationToken::new()
        )
        .await,
        Err(NamespaceReadFailure::Operation { .. })
    ));
    assert_eq!(start.elapsed(), Duration::from_secs(4));
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(250)).await;
        signal.cancel();
    });
    let start = Instant::now();
    assert!(matches!(
        read_namespace_entry(&storage, &StoragePath::new("file")?, policy(), &cancel).await,
        Err(NamespaceReadFailure::Cancelled { .. })
    ));
    assert_eq!(start.elapsed(), Duration::from_millis(250));
    Ok(())
}
