use std::num::NonZeroUsize;

use async_trait::async_trait;

use super::*;
use crate::model::{
    BackendIdentity, BackendKind as Kind, IdentityStrength, MetadataObservation,
    MetadataProvenance, ObservationPlan, SourceIdentity, SymlinkTarget, SymlinkTargetEncoding,
    TimestampMetadata,
};
use crate::storage::MetadataMutation;

struct FakeNamespace;
struct FakeMetadata;

#[derive(Clone, Copy)]
enum MetadataFailureMode {
    Entry,
    Session,
}

struct FailingMetadata(MetadataFailureMode);
struct BlockingMetadata;

fn path(value: &str) -> StoragePath {
    StoragePath::new(value).unwrap_or_else(|error| panic!("{error}"))
}

fn descriptor(value: &str, kind: EntryKind) -> SourceDescriptor {
    SourceDescriptor {
        path: path(value),
        kind,
        size: (kind == EntryKind::File).then_some(3),
        source_identity: SourceIdentity::new(
            BackendIdentity::new(Kind::Nfs, "traversal-test")
                .unwrap_or_else(|error| panic!("{error}")),
            IdentityStrength::StableWithinBackend,
            value.as_bytes(),
        )
        .unwrap_or_else(|error| panic!("{error}")),
        backend_fact: None,
        content_version: None,
        inline_timestamps: None,
    }
}

#[async_trait]
impl Namespace for FakeNamespace {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        match request {
            NamespaceRequest::List(root) if root == StoragePath::root() => {
                Ok(NamespaceResult::Entries(vec![
                    descriptor("dir", EntryKind::Directory),
                    descriptor("file", EntryKind::File),
                    descriptor("link", EntryKind::Symlink),
                ]))
            }
            NamespaceRequest::List(root) if root == path("dir") => Ok(NamespaceResult::Entries(
                vec![descriptor("dir/child", EntryKind::File)],
            )),
            NamespaceRequest::ReadLink(link) if link == path("link") => {
                Ok(NamespaceResult::LinkTarget(
                    SymlinkTarget::new(SymlinkTargetEncoding::UnixBytes, b"file".to_vec())
                        .unwrap_or_else(|error| panic!("{error}")),
                ))
            }
            _ => Err(StorageRoleFailure::Entry(entry_failure(
                &StoragePath::root(),
                FailureClass::NotFound,
            ))),
        }
    }
}

#[async_trait]
impl Metadata for FakeMetadata {
    async fn observe(
        &self,
        _path: &StoragePath,
        _plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        MetadataObservations::new(
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            MetadataObservation::NotApplicable,
            MetadataObservation::NotRequested,
            MetadataObservation::Value {
                value: TimestampMetadata {
                    accessed: None,
                    modified: None,
                    created: None,
                },
                provenance: MetadataProvenance::Inline,
            },
        )
        .map_err(|_| {
            StorageRoleFailure::Entry(entry_failure(&StoragePath::root(), FailureClass::Protocol))
        })
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

#[async_trait]
impl Metadata for FailingMetadata {
    async fn observe(
        &self,
        observed_path: &StoragePath,
        _plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        match self.0 {
            MetadataFailureMode::Entry => Err(StorageRoleFailure::Entry(entry_failure(
                observed_path,
                FailureClass::PermissionDenied,
            ))),
            MetadataFailureMode::Session => Err(StorageRoleFailure::Session(
                crate::model::BackendSessionFailure::new(
                    Operation::Observe,
                    FailureClass::Connectivity,
                    Transience::Transient,
                    "test storage session failed",
                )
                .unwrap_or_else(|error| panic!("{error}")),
            )),
        }
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

#[async_trait]
impl Metadata for BlockingMetadata {
    async fn observe(
        &self,
        _path: &StoragePath,
        _plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        std::future::pending().await
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
async fn recursively_traverses_roles_with_stable_order_and_symlink_target() {
    let source =
        StorageTraversalSource::with_roles(Arc::new(FakeNamespace), Arc::new(FakeMetadata));
    let mut session = source.traverse(TraversalRequest {
        root: StoragePath::root(),
        order: crate::traversal::TraversalOrder::Admission,
        max_inflight_operations: NonZeroUsize::new(2)
            .unwrap_or_else(|| unreachable!("constant is nonzero")),
        max_buffered_items: NonZeroUsize::new(1)
            .unwrap_or_else(|| unreachable!("constant is nonzero")),
        observation_plan: ObservationPlan::default(),
        cancel: tokio_util::sync::CancellationToken::new(),
        filter: None,
        max_depth: None,
    });
    let mut observed = Vec::new();
    while let Some(item) = session.next_item().await {
        let TraversalItem::Entry(entry) = item else {
            panic!("unexpected entry failure")
        };
        observed.push((entry.path().clone(), entry.symlink_target().cloned()));
    }
    assert_eq!(
        observed
            .iter()
            .map(|(path, _)| path.as_str())
            .collect::<Vec<_>>(),
        ["dir", "file", "link", "dir/child"]
    );
    assert_eq!(
        observed[2].1.as_ref().map(SymlinkTarget::as_bytes),
        Some(&b"file"[..])
    );
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Completed(TraversalCompletion {
            observed_entries: 4,
            entry_failures: 0
        }))
    ));
}

/// Requests timestamps explicitly so the Metadata role is exercised; under the default
/// (all-`Omit`) plan the traversal builds observations locally and never calls it.
fn request(cancel: tokio_util::sync::CancellationToken) -> TraversalRequest {
    TraversalRequest {
        root: StoragePath::root(),
        order: crate::traversal::TraversalOrder::Admission,
        max_inflight_operations: NonZeroUsize::new(2)
            .unwrap_or_else(|| unreachable!("constant is nonzero")),
        max_buffered_items: NonZeroUsize::new(1)
            .unwrap_or_else(|| unreachable!("constant is nonzero")),
        observation_plan: ObservationPlan::default()
            .with_timestamps(crate::model::ObservationMode::Required),
        cancel,
        filter: None,
        max_depth: None,
    }
}

#[tokio::test]
async fn entry_failures_are_items_while_session_failures_are_terminal() {
    let source = StorageTraversalSource::with_roles(
        Arc::new(FakeNamespace),
        Arc::new(FailingMetadata(MetadataFailureMode::Entry)),
    );
    let mut session = source.traverse(request(tokio_util::sync::CancellationToken::new()));
    let mut failures = 0;
    while let Some(item) = session.next_item().await {
        assert!(matches!(item, TraversalItem::EntryFailure(_)));
        failures += 1;
    }
    assert_eq!(failures, 4);
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Completed(TraversalCompletion {
            observed_entries: 0,
            entry_failures: 4
        }))
    ));

    let source = StorageTraversalSource::with_roles(
        Arc::new(FakeNamespace),
        Arc::new(FailingMetadata(MetadataFailureMode::Session)),
    );
    let mut session = source.traverse(request(tokio_util::sync::CancellationToken::new()));
    while session.next_item().await.is_some() {}
    assert!(matches!(
        session.finish().await,
        Err(TraversalTerminalFailure::Session(error))
            if error.class() == FailureClass::Connectivity
    ));
}

#[tokio::test]
async fn precancelled_traversal_has_a_distinct_cancelled_outcome() {
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    let source =
        StorageTraversalSource::with_roles(Arc::new(FakeNamespace), Arc::new(FakeMetadata));
    let mut session = source.traverse(request(cancel));
    assert!(session.next_item().await.is_none());
    assert_eq!(session.finish().await, Ok(TraversalOutcome::Cancelled));
}

#[tokio::test]
async fn cancellation_while_inflight_is_full_terminates_without_spinning() {
    let cancel = tokio_util::sync::CancellationToken::new();
    let source =
        StorageTraversalSource::with_roles(Arc::new(FakeNamespace), Arc::new(BlockingMetadata));
    let mut traversal_request = request(cancel.clone());
    traversal_request.max_inflight_operations =
        NonZeroUsize::new(1).unwrap_or_else(|| unreachable!("constant is nonzero"));
    let mut session = source.traverse(traversal_request);
    tokio::task::yield_now().await;
    cancel.cancel();
    assert!(session.next_item().await.is_none());
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), session.finish())
            .await
            .unwrap_or_else(|_| panic!("cancelled traversal did not terminate")),
        Ok(TraversalOutcome::Cancelled)
    );
}

/// Namespace over a fixed tree: `keep/{a,b/c}`, `drop/{x,y/z}`, `top`.
struct TreeNamespace {
    lists: std::sync::Mutex<Vec<String>>,
}

impl TreeNamespace {
    fn new() -> Self {
        Self {
            lists: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn listed(&self) -> Vec<String> {
        self.lists
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl Namespace for TreeNamespace {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        let NamespaceRequest::List(directory) = request else {
            return Err(StorageRoleFailure::Entry(entry_failure(
                &StoragePath::root(),
                FailureClass::Unsupported,
            )));
        };
        self.lists
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(directory.as_str().to_owned());
        let entries = match directory.as_str() {
            "" => vec![
                descriptor("keep", EntryKind::Directory),
                descriptor("drop", EntryKind::Directory),
                descriptor("top", EntryKind::File),
            ],
            "keep" => vec![
                descriptor("keep/a", EntryKind::File),
                descriptor("keep/b", EntryKind::Directory),
            ],
            "keep/b" => vec![descriptor("keep/b/c", EntryKind::File)],
            "drop" => vec![
                descriptor("drop/x", EntryKind::File),
                descriptor("drop/y", EntryKind::Directory),
            ],
            "drop/y" => vec![descriptor("drop/y/z", EntryKind::File)],
            _ => Vec::new(),
        };
        Ok(NamespaceResult::Entries(entries))
    }
}

/// Metadata role that counts calls and reports a fixed `modified`.
struct CountingMetadata {
    calls: std::sync::atomic::AtomicUsize,
}

impl CountingMetadata {
    fn new() -> Self {
        Self {
            calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait]
impl Metadata for CountingMetadata {
    async fn observe(
        &self,
        _path: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let timestamps = if plan.timestamps() == crate::model::ObservationMode::Omit {
            MetadataObservation::NotRequested
        } else {
            MetadataObservation::Value {
                value: TimestampMetadata {
                    accessed: None,
                    modified: Some(
                        crate::model::StorageTimestamp::new(
                            7_000_000_000,
                            crate::model::TimePrecision::Seconds,
                        )
                        .unwrap_or_else(|error| panic!("{error}")),
                    ),
                    created: None,
                },
                provenance: MetadataProvenance::AdditionalCall,
            }
        };
        MetadataObservations::new(
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            timestamps,
        )
        .map_err(|_| {
            StorageRoleFailure::Entry(entry_failure(&StoragePath::root(), FailureClass::Protocol))
        })
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

/// Filter that prunes everything under `drop`, hides `keep` itself but descends into it,
/// and stops filtering below `keep/b`.
#[derive(Debug)]
struct PathFilter {
    needs_modified: bool,
}

impl crate::traversal::TraversalFilter for PathFilter {
    fn needs_modified(&self) -> bool {
        self.needs_modified
    }

    fn decide(
        &self,
        candidate: &crate::traversal::TraversalCandidate<'_>,
    ) -> crate::traversal::TraversalDecision {
        if self.needs_modified {
            assert!(
                candidate.modified.is_some(),
                "deferred filter must never see an absent modified timestamp"
            );
        }
        let path = candidate.path;
        if path.starts_with("drop") {
            return crate::traversal::TraversalDecision {
                emit: false,
                descend: false,
                filter_children: false,
            };
        }
        if path == "keep" {
            return crate::traversal::TraversalDecision {
                emit: false,
                descend: true,
                filter_children: true,
            };
        }
        if path == "keep/b" {
            return crate::traversal::TraversalDecision {
                emit: true,
                descend: true,
                filter_children: false,
            };
        }
        crate::traversal::TraversalDecision::unfiltered(candidate.kind)
    }
}

fn filtered_request(filter: PathFilter, max_depth: Option<usize>) -> TraversalRequest {
    let mut request = request(tokio_util::sync::CancellationToken::new());
    request.observation_plan = ObservationPlan::default();
    request.filter = Some(Arc::new(filter));
    request.max_depth = max_depth.and_then(NonZeroUsize::new);
    request
}

async fn collect(mut session: TraversalSession) -> (Vec<String>, TraversalOutcome) {
    let mut paths = Vec::new();
    while let Some(item) = session.next_item().await {
        let TraversalItem::Entry(entry) = item else {
            panic!("unexpected entry failure")
        };
        paths.push(entry.path().as_str().to_owned());
    }
    let outcome = session
        .finish()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    (paths, outcome)
}

#[tokio::test]
async fn immediate_filter_prunes_without_observing_and_hides_directories() {
    let namespace = Arc::new(TreeNamespace::new());
    let metadata = Arc::new(CountingMetadata::new());
    let source = StorageTraversalSource::with_roles(
        Arc::clone(&namespace) as Arc<dyn Namespace>,
        Arc::clone(&metadata) as Arc<dyn Metadata>,
    );
    let request = filtered_request(
        PathFilter {
            needs_modified: false,
        },
        None,
    );
    let (paths, outcome) = collect(source.traverse(request)).await;
    assert_eq!(paths, ["top", "keep/a", "keep/b", "keep/b/c"]);
    assert_eq!(
        outcome,
        TraversalOutcome::Completed(TraversalCompletion {
            observed_entries: 4,
            entry_failures: 0
        })
    );
    assert_eq!(
        namespace.listed(),
        ["", "keep", "keep/b"],
        "drop is never listed"
    );
    assert_eq!(
        metadata.calls(),
        0,
        "default plan needs no metadata round trip at all"
    );
}

#[tokio::test]
async fn deferred_filter_observes_first_and_still_lists_trailing_directories() {
    let namespace = Arc::new(TreeNamespace::new());
    let metadata = Arc::new(CountingMetadata::new());
    let source = StorageTraversalSource::with_roles(
        Arc::clone(&namespace) as Arc<dyn Namespace>,
        Arc::clone(&metadata) as Arc<dyn Metadata>,
    );
    let mut request = filtered_request(
        PathFilter {
            needs_modified: true,
        },
        None,
    );
    request.max_inflight_operations =
        NonZeroUsize::new(1).unwrap_or_else(|| unreachable!("constant is nonzero"));
    let (paths, _) = collect(source.traverse(request)).await;
    assert_eq!(paths, ["top", "keep/a", "keep/b", "keep/b/c"]);
    assert_eq!(namespace.listed(), ["", "keep", "keep/b"]);
    // root children (3) + keep children (2) are observed before deciding; `keep/b/c`
    // sits below an unfiltered directory and is observed for delivery.
    assert_eq!(metadata.calls(), 6);
    assert!(
        paths.iter().all(|path| !path.starts_with("drop")),
        "deferred decisions still prune"
    );
}

#[tokio::test]
async fn max_depth_lists_only_admitted_levels() {
    let namespace = Arc::new(TreeNamespace::new());
    let source = StorageTraversalSource::with_roles(
        Arc::clone(&namespace) as Arc<dyn Namespace>,
        Arc::new(CountingMetadata::new()),
    );
    let mut shallow = request(tokio_util::sync::CancellationToken::new());
    shallow.observation_plan = ObservationPlan::default();
    shallow.max_depth = NonZeroUsize::new(1);
    let (paths, _) = collect(source.traverse(shallow)).await;
    assert_eq!(paths, ["keep", "drop", "top"]);
    assert_eq!(namespace.listed(), [""]);

    let namespace = Arc::new(TreeNamespace::new());
    let source = StorageTraversalSource::with_roles(
        Arc::clone(&namespace) as Arc<dyn Namespace>,
        Arc::new(CountingMetadata::new()),
    );
    let mut two_levels = request(tokio_util::sync::CancellationToken::new());
    two_levels.observation_plan = ObservationPlan::default();
    two_levels.max_depth = NonZeroUsize::new(2);
    let (paths, _) = collect(source.traverse(two_levels)).await;
    assert_eq!(
        paths,
        [
            "keep", "drop", "top", "keep/a", "keep/b", "drop/x", "drop/y"
        ]
    );
    assert_eq!(namespace.listed(), ["", "keep", "drop"]);
}

#[tokio::test]
async fn inline_timestamps_replace_the_metadata_round_trip() {
    struct InlineNamespace;
    #[async_trait]
    impl Namespace for InlineNamespace {
        async fn execute(
            &self,
            request: NamespaceRequest,
        ) -> Result<NamespaceResult, StorageRoleFailure> {
            let NamespaceRequest::List(directory) = request else {
                unreachable!("only List is issued")
            };
            if directory != StoragePath::root() {
                return Ok(NamespaceResult::Entries(Vec::new()));
            }
            let stamp = crate::model::StorageTimestamp::new(
                42_000_000_000,
                crate::model::TimePrecision::Seconds,
            )
            .unwrap_or_else(|error| panic!("{error}"));
            Ok(NamespaceResult::Entries(vec![
                descriptor("with", EntryKind::File).with_inline_timestamps(TimestampMetadata {
                    accessed: None,
                    modified: Some(stamp),
                    created: None,
                }),
                descriptor("without", EntryKind::File),
            ]))
        }
    }
    let metadata = Arc::new(CountingMetadata::new());
    let source = StorageTraversalSource::with_roles(
        Arc::new(InlineNamespace),
        Arc::clone(&metadata) as Arc<dyn Metadata>,
    );
    let mut session = source.traverse(request(tokio_util::sync::CancellationToken::new()));
    let mut modified = Vec::new();
    while let Some(item) = session.next_item().await {
        let TraversalItem::Entry(entry) = item else {
            panic!("unexpected entry failure")
        };
        modified.push(
            entry
                .modified()
                .map(crate::model::StorageTimestamp::unix_nanos),
        );
    }
    assert_eq!(modified, [Some(42_000_000_000), Some(7_000_000_000)]);
    assert_eq!(
        metadata.calls(),
        1,
        "only the entry without inline timestamps consults the role"
    );
}

/// Metadata that fails for one named path and reports a fixed timestamp for every other.
struct FailingAt {
    path: &'static str,
    inner: CountingMetadata,
}

#[async_trait]
impl Metadata for FailingAt {
    async fn observe(
        &self,
        observed: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        if observed.as_str() == self.path {
            return Err(StorageRoleFailure::Entry(entry_failure(
                observed,
                FailureClass::PermissionDenied,
            )));
        }
        self.inner.observe(observed, plan).await
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

/// Records every candidate path the filter is shown, and admits everything.
#[derive(Debug)]
struct RecordingFilter {
    needs_modified: bool,
    seen: std::sync::Mutex<Vec<String>>,
}

impl RecordingFilter {
    fn new(needs_modified: bool) -> Self {
        Self {
            needs_modified,
            seen: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn seen(&self) -> Vec<String> {
        self.seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl crate::traversal::TraversalFilter for RecordingFilter {
    fn needs_modified(&self) -> bool {
        self.needs_modified
    }

    fn decide(
        &self,
        candidate: &crate::traversal::TraversalCandidate<'_>,
    ) -> crate::traversal::TraversalDecision {
        self.seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(candidate.path.to_owned());
        // Keep filtering below directories so every descendant is shown to the filter.
        crate::traversal::TraversalDecision {
            emit: true,
            descend: candidate.kind == EntryKind::Directory,
            filter_children: true,
        }
    }
}

#[tokio::test]
async fn a_deferred_observation_failure_still_lists_the_directory() {
    // Without this, a transient stat failure on one directory would silently drop every
    // descendant while the session still reported Completed.
    let namespace = Arc::new(TreeNamespace::new());
    let source = StorageTraversalSource::with_roles(
        Arc::clone(&namespace) as Arc<dyn Namespace>,
        Arc::new(FailingAt {
            path: "keep",
            inner: CountingMetadata::new(),
        }),
    );
    let request = filtered_request(
        PathFilter {
            needs_modified: true,
        },
        None,
    );
    let mut session = source.traverse(request);
    let mut emitted = Vec::new();
    let mut failed = Vec::new();
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => emitted.push(entry.path().as_str().to_owned()),
            TraversalItem::EntryFailure(error) => failed.push(error.path().as_str().to_owned()),
        }
    }
    assert_eq!(failed, ["keep"]);
    assert!(
        emitted.contains(&"keep/a".to_owned()) && emitted.contains(&"keep/b/c".to_owned()),
        "the subtree below the failed directory is still enumerated: {emitted:?}"
    );
    assert!(
        session.finish().await.is_ok(),
        "an entry failure is not terminal"
    );
}

#[tokio::test]
async fn candidate_paths_are_relative_to_the_traversal_root() {
    for needs_modified in [false, true] {
        let filter = Arc::new(RecordingFilter::new(needs_modified));
        let source = StorageTraversalSource::with_roles(
            Arc::new(TreeNamespace::new()),
            Arc::new(CountingMetadata::new()),
        );
        let mut request = request(tokio_util::sync::CancellationToken::new());
        request.root = path("keep");
        request.filter = Some(Arc::clone(&filter) as Arc<dyn crate::traversal::TraversalFilter>);
        let (paths, _) = collect(source.traverse(request)).await;
        // Emitted entries keep their backend-relative path ...
        assert_eq!(paths, ["keep/a", "keep/b", "keep/b/c"]);
        // ... while the filter sees them relative to the traversal root, like the legacy walkers.
        let mut seen = filter.seen();
        seen.sort();
        assert_eq!(seen, ["a", "b", "b/c"], "needs_modified={needs_modified}");
    }
}
