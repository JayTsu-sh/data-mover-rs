//! Shared scan operation budgets and first-failure notification for every remote backend.
//! 远程扫描的操作时限与旁路失败信号；不对消费者等待或整轮扫描计时。

use crate::{
    model::{
        BackendKind, BackendSessionFailure, FailureClass, MetadataCapabilities,
        MetadataObservations, ObservationPlan, Operation, RenameContext, SourceIdentity,
        StoragePath, SymlinkTarget, Transience,
    },
    storage::{
        BackendCapabilities, CapabilityAvailability, ListRequest, ListingStream, Metadata,
        Namespace, NamespaceCapabilities, SourceDescriptor, Storage, StorageBuilder,
        StorageRoleFailure, UnsupportedReason,
    },
    traversal::StorageTraversalSource,
};
use async_trait::async_trait;
use futures::StreamExt;

use std::{
    future::Future,
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

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
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self.kind {
            RemoteScanFailureKind::ConnectionLost => "storage_connection_lost",
            RemoteScanFailureKind::OperationTimedOut => "storage_operation_timeout",
        }
    }
}

#[derive(Clone)]
pub struct ScanFailureSignal {
    backend: BackendKind,
    limit: Option<Duration>,
    failed: CancellationToken,
    failure: Arc<OnceLock<RemoteScanFailure>>,
}
impl ScanFailureSignal {
    pub(crate) fn new(backend: BackendKind, limit: Option<Duration>) -> Self {
        Self {
            backend,
            limit,
            failed: CancellationToken::new(),
            failure: Arc::default(),
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
        if storage.kind() == BackendKind::Local || limit.is_none() {
            return Ok((StorageTraversalSource::new(storage)?, guard));
        }
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
    #[must_use]
    pub fn failure(&self) -> Option<RemoteScanFailure> {
        self.failure.get().cloned()
    }
    pub async fn failed(&self) {
        self.failed.cancelled().await;
    }
    fn record(
        &self,
        operation: Operation,
        path: &StoragePath,
        kind: RemoteScanFailureKind,
        source: Option<StorageRoleFailure>,
    ) {
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
    async fn operation<T>(
        &self,
        operation: Operation,
        path: &StoragePath,
        work: impl Future<Output = std::result::Result<T, StorageRoleFailure>>,
    ) -> std::result::Result<T, StorageRoleFailure> {
        let Some(limit) = self.limit else {
            return work.await;
        };
        match timeout(limit, work).await {
            Ok(result) => {
                if let Err(error) = &result {
                    let class = match error {
                        StorageRoleFailure::Session(failure) => failure.class(),
                        StorageRoleFailure::Entry(failure) => failure.class(),
                    };
                    if class == FailureClass::Connectivity {
                        self.record(
                            operation,
                            path,
                            RemoteScanFailureKind::ConnectionLost,
                            Some(error.clone()),
                        );
                    } else if class == FailureClass::Timeout {
                        self.record(
                            operation,
                            path,
                            RemoteScanFailureKind::OperationTimedOut,
                            Some(error.clone()),
                        );
                    }
                }
                result
            }
            Err(_) => {
                self.record(
                    operation,
                    path,
                    RemoteScanFailureKind::OperationTimedOut,
                    None,
                );
                let failure = BackendSessionFailure::new(
                    operation,
                    FailureClass::Timeout,
                    Transience::Transient,
                    "remote scan operation deadline exceeded",
                )
                .unwrap_or_else(|_| unreachable!("static remote scan diagnostic"));
                Err(StorageRoleFailure::Session(failure))
            }
        }
    }
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
            let mut pages = match root {
                Some(root) => inner.list_for_traversal(request, root),
                None => inner.list(request),
            };
            loop {
                // 计时仅覆盖取下一页；send 等待消费者时不计时。
                let next = guard
                    .operation(Operation::Traverse, &path, async {
                        pages.next().await.transpose()
                    })
                    .await;
                match next {
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
        self.guard
            .operation(Operation::Observe, path, self.inner.stat(path))
            .await
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
        self.guard
            .operation(Operation::Observe, path, self.inner.read_link(path))
            .await
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
        mutation: crate::storage::MetadataMutation,
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
        self.inner.observe_listed(descriptor, plan)
    }
    async fn observe(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
    ) -> std::result::Result<MetadataObservations, StorageRoleFailure> {
        self.guard
            .operation(Operation::Observe, path, self.inner.observe(path, plan))
            .await
    }
    async fn observe_bound(
        &self,
        path: &StoragePath,
        expected: &SourceIdentity,
        plan: ObservationPlan,
    ) -> std::result::Result<MetadataObservations, StorageRoleFailure> {
        self.guard
            .operation(
                Operation::Observe,
                path,
                self.inner.observe_bound(path, expected, plan),
            )
            .await
    }
    fn copied_metadata_observation_plan(&self) -> Option<ObservationPlan> {
        self.inner.copied_metadata_observation_plan()
    }
    async fn observe_copy_bound(
        &self,
        path: &StoragePath,
        expected: &SourceIdentity,
        plan: ObservationPlan,
    ) -> Result<crate::storage::CopiedMetadataObservation, StorageRoleFailure> {
        self.guard
            .operation(
                Operation::Observe,
                path,
                self.inner.observe_copy_bound(path, expected, plan),
            )
            .await
    }
    async fn observe_copy_bound_version(
        &self,
        path: &StoragePath,
        expected: &SourceIdentity,
        version: &crate::model::SourceVersion,
        plan: ObservationPlan,
    ) -> Result<crate::storage::CopiedMetadataObservation, StorageRoleFailure> {
        self.guard
            .operation(
                Operation::Observe,
                path,
                self.inner
                    .observe_copy_bound_version(path, expected, version, plan),
            )
            .await
    }
}

/// Setup failures are distinct from a remote operation failing after traversal started.
#[derive(Debug, thiserror::Error)]
pub enum ScanSetupError {
    #[error("scan operation timeout must be positive")]
    InvalidDeadline,
    #[error(transparent)]
    Capability(#[from] crate::storage::CapabilityUnavailable),
    #[error(transparent)]
    Roles(#[from] crate::storage::StorageBuildError),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::model::{BackendIdentity, EntryKind, EntryOperationFailure, IdentityStrength};
    use crate::storage::MetadataMutation;
    use crate::traversal::{
        TraversalItem, TraversalRequest, TraversalSource, TraversalTerminalFailure,
    };

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
                ListingStream::from_producer(request.path, |_sink| async {
                    std::future::pending::<()>().await
                })
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
}
