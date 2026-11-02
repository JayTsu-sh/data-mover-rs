//! Shared scan operation budgets and first-failure notification for every remote backend.
//! 远程扫描的操作时限与旁路失败信号；不对消费者等待或整轮扫描计时。

use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{Arc, OnceLock},
    time::Duration,
};

use async_trait::async_trait;
use futures::StreamExt;
use tokio::{
    sync::Mutex,
    time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;

use crate::{
    model::{
        BackendKind, BackendSessionFailure, FailureClass, MetadataCapabilities,
        MetadataObservations, ObservationPlan, Operation, RenameContext, SourceIdentity,
        SourceVersion, StoragePath, SymlinkTarget, Transience,
    },
    storage::{
        BackendCapabilities, CapabilityAvailability, CapabilityUnavailable,
        CopiedMetadataObservation, ListRequest, ListingPage, ListingStream, Metadata,
        MetadataMutation, Namespace, NamespaceCapabilities, SourceDescriptor, Storage,
        StorageBuildError, StorageBuilder, StorageRoleFailure, UnsupportedReason,
    },
    traversal::StorageTraversalSource,
};

/// The evidence that stopped remote scanning; a timeout does not prove disconnection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoteScanFailureKind {
    ConnectionLost,
    OperationTimedOut,
}

/// 稳定类别供上层匹配；显示文本不含 URL、凭据或后端原始诊断。
#[derive(Clone, Debug, thiserror::Error)]
#[error(
    "Storage {backend:?} scan {operation:?} failed at {path}: {kind:?} (operation limit {operation_timeout:?})"
)]
pub struct RemoteScanFailure {
    pub backend: BackendKind,
    pub operation: Operation,
    pub path: StoragePath,
    pub kind: RemoteScanFailureKind,
    pub operation_timeout: Duration,
    #[source]
    pub source: Option<StorageRoleFailure>,
}
impl RemoteScanFailure {
    /// Stable error code for applications and persisted task diagnostics.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self.kind {
            RemoteScanFailureKind::ConnectionLost => "storage_connection_lost",
            RemoteScanFailureKind::OperationTimedOut => "storage_operation_timeout",
        }
    }
}

/// First remote failure notification, independent of buffered traversal entries.
#[derive(Clone)]
pub struct ScanFailureSignal {
    backend: BackendKind,
    limit: Option<Duration>,
    failed: CancellationToken,
    failure: Arc<OnceLock<RemoteScanFailure>>,
    successful_listing: Arc<OnceLock<Instant>>,
    failure_at: Arc<OnceLock<Instant>>,
    recovery_target: Arc<OnceLock<(Operation, StoragePath)>>,
    recovered_at: Arc<OnceLock<Instant>>,
}
impl ScanFailureSignal {
    pub(crate) fn new(backend: BackendKind, limit: Option<Duration>) -> Self {
        Self {
            backend,
            limit,
            failed: CancellationToken::new(),
            failure: Arc::default(),
            successful_listing: Arc::default(),
            failure_at: Arc::default(),
            recovery_target: Arc::default(),
            recovered_at: Arc::default(),
        }
    }
    /// Builds traversal over the connected roles with bounded remote operations.
    ///
    /// # Errors
    /// Rejects a zero deadline or storage without the namespace and metadata roles.
    pub fn source(
        storage: &Storage,
        limit: Option<Duration>,
    ) -> std::result::Result<(StorageTraversalSource, Self), ScanSetupError> {
        if limit.is_some_and(|value| value.is_zero()) {
            return Err(ScanSetupError::InvalidDeadline);
        }
        let guard = Self::new(storage.kind(), limit);
        // 只借用遍历需要的角色；不改变原始 Storage 的拷贝、删除或发布行为。
        let unsupported = CapabilityAvailability::Unsupported(
            UnsupportedReason::new("scan-only storage role")
                .unwrap_or_else(|_| unreachable!("static role diagnostic")),
        );
        let roles = StorageBuilder::new(
            storage.identity().clone(),
            BackendCapabilities::new(
                unsupported.clone(),
                unsupported,
                CapabilityAvailability::Supported,
                CapabilityAvailability::Supported,
            ),
        )
        .namespace(Arc::new(GuardedNamespace {
            inner: storage.namespace()?,
            guard: guard.clone(),
        }))
        .metadata(Arc::new(GuardedMetadata {
            inner: storage.metadata()?,
            guard: guard.clone(),
        }))
        .build()?;
        Ok((StorageTraversalSource::new(&roles)?, guard))
    }
    /// Returns the first failure, preserving its original classification.
    #[must_use]
    pub fn failure(&self) -> Option<RemoteScanFailure> {
        self.failure.get().cloned()
    }
    /// Completes as soon as a remote operation has failed or exceeded its budget.
    pub async fn failed(&self) {
        self.failed.cancelled().await;
    }
    pub(super) fn publish_failure(&self, failure: RemoteScanFailure) {
        self.failure.get_or_init(|| failure);
        self.failed.cancel();
    }
    #[cfg(test)]
    pub(super) fn successful_listing_at(&self) -> Option<Instant> {
        self.successful_listing.get().copied()
    }
    pub(super) fn failure_detected_at(&self) -> Option<Instant> {
        self.failure_at.get().copied()
    }
    pub(super) fn watch_recovery(&self, operation: Operation, path: StoragePath) {
        let _ = self.recovery_target.set((operation, path));
    }
    pub(super) fn recovered_at(&self) -> Option<Instant> {
        self.recovered_at.get().copied()
    }
    fn record_success(&self, operation: Operation, path: &StoragePath) {
        if self.failure.get().is_none()
            && self
                .recovery_target
                .get()
                .is_some_and(|(expected, target)| *expected == operation && target == path)
        {
            self.recovered_at.get_or_init(Instant::now);
        }
    }
    fn record(
        &self,
        operation: Operation,
        path: &StoragePath,
        kind: RemoteScanFailureKind,
        source: Option<StorageRoleFailure>,
    ) {
        self.failure_at.get_or_init(Instant::now);
        self.failure.get_or_init(|| RemoteScanFailure {
            backend: self.backend,
            operation,
            path: path.clone(),
            kind,
            operation_timeout: self.limit.unwrap_or_default(),
            source,
        });
        self.failed.cancel();
    }
    fn normalize_error(
        &self,
        operation: Operation,
        path: &StoragePath,
        error: StorageRoleFailure,
    ) -> StorageRoleFailure {
        let class = match &error {
            StorageRoleFailure::Session(failure) => failure.class(),
            StorageRoleFailure::Entry(failure) => failure.class(),
        };
        let kind = match class {
            FailureClass::Connectivity => RemoteScanFailureKind::ConnectionLost,
            FailureClass::Timeout => RemoteScanFailureKind::OperationTimedOut,
            _ => return error,
        };
        self.record(operation, path, kind, Some(error));
        stopped_failure(operation, kind)
    }

    fn checked_page(
        &self,
        path: &StoragePath,
        page: Option<ListingPage>,
    ) -> Result<Option<ListingPage>, StorageRoleFailure> {
        if let Some(failure) = page.as_ref().and_then(|page| {
            page.failures.iter().find(|error| {
                matches!(
                    error.failure.class(),
                    FailureClass::Connectivity | FailureClass::Timeout
                )
            })
        }) {
            return Err(self.normalize_error(
                Operation::Traverse,
                path,
                StorageRoleFailure::Entry(failure.failure.clone()),
            ));
        }
        self.successful_listing.get_or_init(Instant::now);
        self.record_success(Operation::Traverse, path);
        Ok(page)
    }

    fn checked_metadata(
        &self,
        path: &StoragePath,
        observations: MetadataObservations,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        if let Some(failure) = observations.failures().find(|failure| {
            matches!(
                failure.class,
                FailureClass::Connectivity | FailureClass::Timeout
            )
        }) {
            let source = BackendSessionFailure::new(
                Operation::Observe,
                failure.class,
                failure.transience,
                "backend returned a failed scan metadata observation",
            )
            .unwrap_or_else(|_| unreachable!("static observation diagnostic"));
            return Err(self.normalize_error(
                Operation::Observe,
                path,
                StorageRoleFailure::Session(source),
            ));
        }
        self.record_success(Operation::Observe, path);
        Ok(observations)
    }

    async fn operation<T: Send + 'static>(
        &self,
        operation: Operation,
        path: &StoragePath,
        work: impl Future<Output = Result<T, StorageRoleFailure>> + Send + 'static,
    ) -> Result<T, StorageRoleFailure> {
        if let Some(failure) = self.failure.get() {
            return Err(stopped_failure(operation, failure.kind));
        }
        let mut work = RetainedOperation::new(work);
        let result = match self.limit {
            Some(limit) => timeout(limit, work.wait()).await,
            None => Ok(work.wait().await),
        };
        if let Ok(result) = result {
            result.map_err(|error| self.normalize_error(operation, path, error))
        } else {
            self.record(
                operation,
                path,
                RemoteScanFailureKind::OperationTimedOut,
                None,
            );
            Err(stopped_failure(
                operation,
                RemoteScanFailureKind::OperationTimedOut,
            ))
        }
    }
}

/// Poll normally in the caller. If its wait ends early, retain the native call
/// until its own deadline/cleanup completes; the first-failure gate bounds admission.
pub(super) struct RetainedOperation<F: Future + Send + 'static> {
    pending: Option<Pin<Box<F>>>,
}
impl<F: Future + Send + 'static> RetainedOperation<F> {
    pub(super) fn new(work: F) -> Self {
        Self {
            pending: Some(Box::pin(work)),
        }
    }
    pub(super) async fn wait(&mut self) -> F::Output {
        let pending = self
            .pending
            .as_mut()
            .unwrap_or_else(|| unreachable!("owned operation is awaited once"));
        let result = pending.await;
        self.pending.take();
        result
    }
}
impl<F: Future + Send + 'static> Drop for RetainedOperation<F> {
    fn drop(&mut self) {
        if let Some(pending) = self.pending.take() {
            drop(tokio::spawn(async move {
                let _ = pending.await;
            }));
        }
    }
}

fn stopped_failure(operation: Operation, kind: RemoteScanFailureKind) -> StorageRoleFailure {
    let class = match kind {
        RemoteScanFailureKind::ConnectionLost => FailureClass::Connectivity,
        RemoteScanFailureKind::OperationTimedOut => FailureClass::Timeout,
    };
    StorageRoleFailure::Session(
        BackendSessionFailure::new(
            operation,
            class,
            Transience::Transient,
            "remote scan stopped after its first failure",
        )
        .unwrap_or_else(|_| unreachable!("static remote scan diagnostic")),
    )
}

struct GuardedNamespace {
    inner: Arc<dyn Namespace>,
    guard: ScanFailureSignal,
}
impl GuardedNamespace {
    fn listing(&self, request: ListRequest, root: Option<bool>) -> ListingStream {
        let path = request.path.clone();
        let inner = self.inner.clone();
        let guard = self.guard.clone();
        ListingStream::from_producer(path.clone(), move |sink| async move {
            let pages = Arc::new(Mutex::new(match root {
                Some(root) => inner.list_for_traversal(request, root),
                None => inner.list(request),
            }));
            loop {
                // 计时仅覆盖取下一页；send 等待消费者时不计时。
                let next_page = pages.clone();
                let next = guard
                    .operation(Operation::Traverse, &path, async move {
                        next_page.lock().await.next().await.transpose()
                    })
                    .await;
                match next {
                    Ok(page) => match guard.checked_page(&path, page) {
                        Ok(Some(page)) => {
                            if sink.send(page).await.is_err() {
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(error) => {
                            sink.fail(error).await;
                            break;
                        }
                    },
                    Err(error) => {
                        sink.fail(error).await;
                        break;
                    }
                }
            }
        })
    }
}
#[async_trait]
impl Namespace for GuardedNamespace {
    async fn stat(
        &self,
        path: &StoragePath,
    ) -> std::result::Result<SourceDescriptor, StorageRoleFailure> {
        let inner = self.inner.clone();
        let owned_path = path.clone();
        let value = self
            .guard
            .operation(Operation::Observe, path, async move {
                inner.stat(&owned_path).await
            })
            .await?;
        self.guard.record_success(Operation::Observe, path);
        Ok(value)
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        self.listing(request, None)
    }
    fn list_for_traversal(&self, request: ListRequest, root: bool) -> ListingStream {
        self.listing(request, Some(root))
    }
    fn listing_resume_key(&self) -> &[u8] {
        self.inner.listing_resume_key()
    }
    fn rename_context(&self) -> Option<&RenameContext> {
        self.inner.rename_context()
    }
    fn capabilities(&self) -> NamespaceCapabilities {
        self.inner.capabilities()
    }
    fn parked_listing_limit(&self) -> Option<NonZeroUsize> {
        self.inner.parked_listing_limit()
    }
    async fn read_link(
        &self,
        path: &StoragePath,
    ) -> std::result::Result<SymlinkTarget, StorageRoleFailure> {
        let inner = self.inner.clone();
        let owned_path = path.clone();
        let value = self
            .guard
            .operation(Operation::Observe, path, async move {
                inner.read_link(&owned_path).await
            })
            .await?;
        self.guard.record_success(Operation::Observe, path);
        Ok(value)
    }
}
struct GuardedMetadata {
    inner: Arc<dyn Metadata>,
    guard: ScanFailureSignal,
}
#[async_trait]
impl Metadata for GuardedMetadata {
    async fn apply(
        &self,
        path: &StoragePath,
        mutation: MetadataMutation,
        cancel: CancellationToken,
    ) -> std::result::Result<(), StorageRoleFailure> {
        self.inner.apply(path, mutation, cancel).await
    }
    fn observation_capabilities(&self) -> MetadataCapabilities {
        self.inner.observation_capabilities()
    }
    fn observe_listed(
        &self,
        descriptor: &SourceDescriptor,
        plan: ObservationPlan,
    ) -> Option<std::result::Result<MetadataObservations, StorageRoleFailure>> {
        self.inner.observe_listed(descriptor, plan).map(|result| {
            result
                .map_err(|error| {
                    self.guard
                        .normalize_error(Operation::Observe, &descriptor.path, error)
                })
                .and_then(|value| self.guard.checked_metadata(&descriptor.path, value))
        })
    }
    async fn observe(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
    ) -> std::result::Result<MetadataObservations, StorageRoleFailure> {
        let inner = self.inner.clone();
        let owned_path = path.clone();
        let value = self
            .guard
            .operation(Operation::Observe, path, async move {
                inner.observe(&owned_path, plan).await
            })
            .await?;
        self.guard.checked_metadata(path, value)
    }
    async fn observe_bound(
        &self,
        path: &StoragePath,
        expected: &SourceIdentity,
        plan: ObservationPlan,
    ) -> std::result::Result<MetadataObservations, StorageRoleFailure> {
        let inner = self.inner.clone();
        let owned_path = path.clone();
        let expected = expected.clone();
        let value = self
            .guard
            .operation(Operation::Observe, path, async move {
                inner.observe_bound(&owned_path, &expected, plan).await
            })
            .await?;
        self.guard.checked_metadata(path, value)
    }
    fn copied_metadata_observation_plan(&self) -> Option<ObservationPlan> {
        self.inner.copied_metadata_observation_plan()
    }
    async fn observe_copy_bound(
        &self,
        path: &StoragePath,
        expected: &SourceIdentity,
        plan: ObservationPlan,
    ) -> Result<CopiedMetadataObservation, StorageRoleFailure> {
        let inner = self.inner.clone();
        let owned_path = path.clone();
        let expected = expected.clone();
        let value = self
            .guard
            .operation(Operation::Observe, path, async move {
                inner.observe_copy_bound(&owned_path, &expected, plan).await
            })
            .await?;
        let mut value = value;
        value.observations = self.guard.checked_metadata(path, value.observations)?;
        Ok(value)
    }
    async fn observe_copy_bound_version(
        &self,
        path: &StoragePath,
        expected: &SourceIdentity,
        version: &SourceVersion,
        plan: ObservationPlan,
    ) -> Result<CopiedMetadataObservation, StorageRoleFailure> {
        let inner = self.inner.clone();
        let owned_path = path.clone();
        let expected = expected.clone();
        let version = version.clone();
        let value = self
            .guard
            .operation(Operation::Observe, path, async move {
                inner
                    .observe_copy_bound_version(&owned_path, &expected, &version, plan)
                    .await
            })
            .await?;
        let mut value = value;
        value.observations = self.guard.checked_metadata(path, value.observations)?;
        Ok(value)
    }
}

/// Setup failures are distinct from a remote operation failing after traversal started.
#[derive(Debug, thiserror::Error)]
pub enum ScanSetupError {
    #[error("scan operation timeout must be positive")]
    InvalidDeadline,
    #[error(transparent)]
    Capability(#[from] CapabilityUnavailable),
    #[error(transparent)]
    Roles(#[from] StorageBuildError),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[path = "../scan_tests.rs"]
mod tests;
