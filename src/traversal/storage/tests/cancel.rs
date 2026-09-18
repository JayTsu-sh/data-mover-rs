//! Cancellation never abandons a backend operation that holds a handle.

use super::*;

/// Models a backend observation that holds a handle: it "opens", blocks until released, then
/// "closes". An observation cut off between the two leaks the handle.
struct HandleMetadata {
    opened: std::sync::atomic::AtomicUsize,
    closed: std::sync::atomic::AtomicUsize,
    release: tokio::sync::Semaphore,
}

impl Default for HandleMetadata {
    fn default() -> Self {
        Self {
            opened: std::sync::atomic::AtomicUsize::new(0),
            closed: std::sync::atomic::AtomicUsize::new(0),
            release: tokio::sync::Semaphore::new(0),
        }
    }
}

#[async_trait]
impl Metadata for HandleMetadata {
    async fn observe(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        self.opened
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _permit = self.release.acquire().await;
        self.closed
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        FakeMetadata.observe(path, plan).await
    }

    async fn apply(
        &self,
        _path: &StoragePath,
        _mutation: MetadataMutation,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        unreachable!("traversal never applies metadata")
    }
}

#[tokio::test]
async fn cancellation_lets_inflight_observations_finish_and_close_their_handles() {
    let metadata = Arc::new(HandleMetadata::default());
    let source = StorageTraversalSource::with_roles(
        Arc::new(FakeNamespace),
        Arc::clone(&metadata) as Arc<dyn Metadata>,
    );
    let cancel = tokio_util::sync::CancellationToken::new();
    let mut session = source.traverse(request(cancel.clone()));
    let opened = || metadata.opened.load(std::sync::atomic::Ordering::SeqCst);
    let closed = || metadata.closed.load(std::sync::atomic::Ordering::SeqCst);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while opened() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no observation started"));

    cancel.cancel();
    while session.next_item().await.is_some() {}
    assert_eq!(session.finish().await, Ok(TraversalOutcome::Cancelled));

    // Cancellation is reported promptly; the observations already holding a handle still run
    // to completion and close it.
    metadata.release.add_permits(64);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while closed() < opened() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "{} of {} observations never closed",
            opened() - closed(),
            opened()
        )
    });
}
