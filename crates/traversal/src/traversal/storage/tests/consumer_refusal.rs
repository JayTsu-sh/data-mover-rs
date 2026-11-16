use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::traversal::{TraversalConfigurationError, TraversalTerminalFailure};

struct CountingNamespace(Arc<AtomicUsize>);

#[async_trait]
impl Namespace for CountingNamespace {
    async fn stat(&self, _path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(not_found())
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        self.0.fetch_add(1, Ordering::Relaxed);
        entries(&request.path, Vec::new())
    }

    async fn read_link(&self, _path: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(not_found())
    }
}

#[tokio::test]
async fn consumer_blocks_mode_is_refused_before_any_storage_io() {
    let mut traversal_completed = Vec::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let source = StorageTraversalSource::with_roles(
        Arc::new(CountingNamespace(calls.clone())),
        Arc::new(FakeMetadata),
    );
    let mut request = TraversalRequest::new(StoragePath::root());
    request.consumer_completion_budget = Some(NonZeroUsize::MIN);
    request.order = TraversalOrder::Blocks;
    let mut session = source.traverse(request);
    assert!(session.next_item(&mut traversal_completed).await.is_none());
    assert_eq!(
        calls.load(Ordering::Relaxed),
        0,
        "unsupported mode must not list"
    );
    assert_eq!(
        session.finish().await,
        Err(TraversalTerminalFailure::Configuration(
            TraversalConfigurationError::ConsumerOrderUnsupported,
        ))
    );
}

#[tokio::test]
async fn consumer_checkpoint_modes_are_refused_before_any_storage_io() {
    for resume in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let source = StorageTraversalSource::with_roles(
            Arc::new(CountingNamespace(calls.clone())),
            Arc::new(FakeMetadata),
        );
        let mut request = TraversalRequest::new(StoragePath::root());
        request.consumer_completion_budget = Some(NonZeroUsize::MIN);
        if resume {
            request.resume_from = Some(crate::traversal::Checkpoint::from_bytes(
                bytes::Bytes::from_static(b"invalid checkpoint"),
            ));
        } else {
            request.resumable = crate::traversal::Resumable::on();
        }
        let mut session = source.traverse(request);
        assert!(session.next_item(&mut Vec::new()).await.is_none());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            session.finish().await,
            Err(TraversalTerminalFailure::Configuration(
                TraversalConfigurationError::ConsumerResumeUnsupported
            ))
        );
    }
}
