use super::*;

use crate::model::{AclEncoding, AclMetadata, MetadataProvenance};
use crate::storage::SourceDescriptor;

fn manual_runner(
    source: &StorageTraversalSource,
    request: TraversalRequest,
) -> (Runner, TraversalSession) {
    let requirements = crate::traversal::configuration::validate(
        &request,
        source.metadata.observation_capabilities(),
    )
    .assert_value("controlled resource fixture");
    let (output, receiver) = channel::channel(NonZeroUsize::MIN);
    let (_completion, result) = oneshot::channel();
    let (session, consumer) = TraversalSession::controlled(receiver, result, &request, 1);
    let shared = Arc::new(Shared::new(
        source.namespace.clone(),
        source.metadata.clone(),
        request,
        requirements,
        None,
        source.probe.clone(),
    ));
    (
        Runner::new(shared, consumer, output, source.identity.clone()),
        session,
    )
}

#[test]
fn page_charge_follows_partial_transfer_and_detached_owner_until_actual_drop() {
    let resources = Resources::new(100);
    let page = PageCharge::new(
        resources
            .reserve(80)
            .assert_value("controlled resource fixture"),
    );
    let observer = page.clone();
    let item = page
        .take(b"owned payload", 50)
        .assert_value("controlled resource fixture");
    assert_eq!(resources.held(), 80);
    drop(page);
    assert_eq!(
        resources.held(),
        80,
        "detached owner keeps the rest of this page charged"
    );
    drop(observer);
    assert_eq!(
        resources.held(),
        50,
        "transferred item is its sole remaining owner"
    );
    assert!(matches!(
        resources.reserve(51),
        Err(TraversalTerminalFailure::ConsumerCompletionBudgetExceeded {
            limit: 100,
            required: 101
        })
    ));
    assert_eq!(
        resources.held(),
        50,
        "failed reservation cannot consume credit"
    );
    drop(item);
    assert_eq!(resources.held(), 0);
}

#[tokio::test]
async fn acquired_page_releases_each_transferred_payload_and_all_state_on_drop() {
    let root = tempfile::tempdir().assert_value("controlled resource fixture");
    for index in 0..4 {
        std::fs::write(root.path().join(format!("file-{index}")), b"kept")
            .assert_value("controlled resource fixture");
    }
    let (storage, _, _) = test_traversal_storage(root.path(), "controlled-page-charge")
        .assert_value("controlled resource fixture");
    let source = StorageTraversalSource::new(&storage).assert_value("controlled resource fixture");
    let request = configured();
    let limit = request
        .consumer_completion_budget
        .assert_value("controlled resource fixture")
        .get();
    let (mut runner, _session) = manual_runner(&source, request);
    let resources = runner.resources.clone();
    assert_eq!(runner.listing_limit, limit / std::mem::size_of::<Node>());
    runner
        .root()
        .await
        .assert_value("controlled resource fixture");
    let node_only = resources.held();
    assert_eq!(runner.nodes.len(), 1, "internal root is charged");
    assert!(node_only >= std::mem::size_of::<Node>());
    runner
        .read_or_finish(0)
        .await
        .assert_value("controlled resource fixture");
    assert_eq!(runner.nodes[0].page.items.len(), 4);
    let full = resources.held();
    let item = runner.nodes[0]
        .page
        .items
        .pop_front()
        .assert_value("controlled resource fixture");
    assert_eq!(
        resources.held(),
        full,
        "moving an item cannot return credit while it is owned"
    );
    drop(item);
    assert!(
        resources.held() < full,
        "partially consumed page releases payload, not the other entries"
    );
    assert!(resources.held() > node_only);
    drop(runner);
    assert_eq!(
        resources.held(),
        0,
        "node, page buffers and remaining entries all release"
    );
}

struct MeasuredMetadata {
    inner: Arc<dyn Metadata>,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    payload: Option<usize>,
    inline: bool,
}

impl MeasuredMetadata {
    fn large(&self) -> MetadataObservations {
        MetadataObservationParts {
            acl: MetadataObservation::Value {
                value: AclMetadata::new(
                    AclEncoding::NfsV4,
                    vec![0; self.payload.assert_value("controlled resource fixture")],
                )
                .assert_value("controlled resource fixture"),
                provenance: MetadataProvenance::AdditionalCall,
            },
            ..MetadataObservationParts::default()
        }
        .build()
        .assert_value("controlled resource fixture")
    }
}

struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Metadata for MeasuredMetadata {
    fn observation_capabilities(&self) -> crate::model::MetadataCapabilities {
        self.inner.observation_capabilities()
    }
    fn observe_listed(
        &self,
        _descriptor: &SourceDescriptor,
        _plan: crate::model::ObservationPlan,
    ) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
        self.inline.then(|| Ok(self.large()))
    }
    async fn observe(
        &self,
        path: &StoragePath,
        plan: crate::model::ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        let _active = Active(self.active.clone());
        tokio::task::yield_now().await;
        if self.payload.is_some() {
            Ok(self.large())
        } else {
            self.inner.observe(path, plan).await
        }
    }
    async fn apply(
        &self,
        _path: &StoragePath,
        _mutation: MetadataMutation,
        _cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        panic!("controlled source remains read only")
    }
}

#[tokio::test]
async fn metadata_observation_concurrency_one_is_independent_of_page_width() {
    let root = tempfile::tempdir().assert_value("controlled resource fixture");
    for index in 0..16 {
        std::fs::write(root.path().join(format!("file-{index}")), b"kept")
            .assert_value("controlled resource fixture");
    }
    let (storage, namespace, metadata) =
        test_traversal_storage(root.path(), "controlled-observation-slot")
            .assert_value("controlled resource fixture");
    let measured = Arc::new(MeasuredMetadata {
        inner: metadata,
        active: Arc::default(),
        peak: Arc::default(),
        calls: Arc::default(),
        payload: None,
        inline: false,
    });
    let source = StorageTraversalSource::with_roles(namespace, measured.clone())
        .identified(storage.identity().clone());
    let mut request = configured();
    request.consumer_max_inflight =
        NonZeroUsize::new(8).assert_value("controlled resource fixture");
    request.max_inflight_operations = NonZeroUsize::MIN;
    request.observation_plan = request
        .observation_plan
        .with_acl(ObservationMode::BestEffort);
    let (_, terminal) = acknowledge(source.traverse(request)).await;
    assert!(
        matches!(terminal, Ok(TraversalOutcome::Completed(_))),
        "{terminal:?}"
    );
    assert_eq!(measured.calls.load(Ordering::SeqCst), 16);
    assert_eq!(
        measured.peak.load(Ordering::SeqCst),
        1,
        "metadata-role I/O only, not backend producer I/O"
    );
    assert_eq!(measured.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn oversized_inline_and_async_observations_fail_typed_before_consumer_work() {
    for inline in [true, false] {
        let root = tempfile::tempdir().assert_value("controlled resource fixture");
        std::fs::write(root.path().join("kept"), b"source retained")
            .assert_value("controlled resource fixture");
        let (storage, namespace, metadata) =
            test_traversal_storage(root.path(), "controlled-large-observation")
                .assert_value("controlled resource fixture");
        let measured = Arc::new(MeasuredMetadata {
            inner: metadata,
            active: Arc::default(),
            peak: Arc::default(),
            calls: Arc::default(),
            payload: Some(32 * 1024),
            inline,
        });
        let source = StorageTraversalSource::with_roles(namespace, measured.clone())
            .identified(storage.identity().clone());
        let mut request = configured();
        request.consumer_completion_budget = NonZeroUsize::new(8 * 1024);
        request.consumer_max_inflight = NonZeroUsize::MIN;
        request.observation_plan = request
            .observation_plan
            .with_acl(ObservationMode::BestEffort);
        let (items, terminal) = acknowledge(source.traverse(request)).await;
        assert!(items.is_empty());
        assert!(
            matches!(terminal, Err(TraversalTerminalFailure::ConsumerCompletionBudgetExceeded { limit: 8192, required }) if required > 8192),
            "{terminal:?}"
        );
        assert_eq!(measured.calls.load(Ordering::SeqCst), usize::from(!inline));
        assert_eq!(measured.active.load(Ordering::SeqCst), 0);
        assert_eq!(
            std::fs::read(root.path().join("kept")).assert_value("controlled resource fixture"),
            b"source retained"
        );
    }
}

#[tokio::test]
async fn deep_single_slot_tree_cancellation_drains_without_new_work_or_lost_source() {
    let root = tempfile::tempdir().assert_value("controlled resource fixture");
    let deep = (0..48).map(|_| "d").collect::<Vec<_>>().join("/");
    std::fs::create_dir_all(root.path().join(&deep)).assert_value("controlled resource fixture");
    std::fs::write(root.path().join(&deep).join("kept"), b"source retained")
        .assert_value("controlled resource fixture");
    let (storage, _, _) = test_traversal_storage(root.path(), "controlled-deep-cancel")
        .assert_value("controlled resource fixture");
    let source = StorageTraversalSource::new(&storage).assert_value("controlled resource fixture");
    let mut request = configured();
    request.consumer_max_inflight = NonZeroUsize::MIN;
    request.max_inflight_operations = NonZeroUsize::MIN;
    let cancel = request.cancel.clone();
    let run = async {
        let mut session = source.traverse(request);
        let mut feedback = Vec::new();
        for _ in 0..8 {
            let Some(TraversalItem::ConsumerEntry(work)) = session.next_item(&mut feedback).await
            else {
                panic!("deep tree failed before cancellation")
            };
            feedback.push((work.0, true));
        }
        cancel.cancel();
        while let Some(item) = session.next_item(&mut feedback).await {
            assert!(matches!(
                item,
                TraversalItem::Failure(_) | TraversalItem::ObservationFailure(_)
            ));
        }
        assert_eq!(session.finish().await, Ok(TraversalOutcome::Cancelled));
    };
    tokio::time::timeout(Duration::from_secs(3), run)
        .await
        .assert_value("controlled resource fixture");
    assert_eq!(
        std::fs::read(root.path().join(&deep).join("kept"))
            .assert_value("controlled resource fixture"),
        b"source retained"
    );
}
