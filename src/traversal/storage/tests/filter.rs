//! Filters, depth limits and the depth-first block order over a fixed tree.

use super::*;

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
    // The root is listed first; its two subdirectories may be listed in either order.
    let mut listed = namespace.listed();
    listed[1..].sort();
    assert_eq!(listed, ["", "drop", "keep"]);
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
        if crate::traversal::is_completion(&item) {
            continue;
        }
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
pub(super) struct RecordingFilter {
    needs_modified: bool,
    seen: std::sync::Mutex<Vec<String>>,
}

impl RecordingFilter {
    pub(super) fn new(needs_modified: bool) -> Self {
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
            TraversalItem::DirectoryListed(_) | TraversalItem::SubtreeComplete(_) => {}
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

/// Output is depth-first by blocks: one directory's children together, in listing order, then
/// each descended subdirectory's block in turn.
#[tokio::test]
async fn output_is_depth_first_in_listing_order_blocks() {
    let namespace = Arc::new(TreeNamespace::new());
    let source = StorageTraversalSource::with_roles(
        Arc::clone(&namespace) as Arc<dyn Namespace>,
        Arc::new(CountingMetadata::new()),
    );
    let mut request = request(tokio_util::sync::CancellationToken::new());
    request.observation_plan = ObservationPlan::default();
    let (paths, _) = collect(source.traverse(request)).await;
    assert_eq!(
        paths,
        [
            "keep", "drop", "top", "keep/a", "keep/b", "keep/b/c", "drop/x", "drop/y", "drop/y/z"
        ]
    );
}
