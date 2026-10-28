//! Metadata errors remain visible independently of entry filtering and usable fallback values.

use super::*;

struct InlineFailure {
    timestamps: bool,
}

#[async_trait]
impl Metadata for InlineFailure {
    fn observe_listed(
        &self,
        _descriptor: &SourceDescriptor,
        plan: ObservationPlan,
    ) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
        let mut metadata = MetadataObservations::default();
        if self.timestamps && plan.timestamps() != M::Omit {
            metadata.timestamps = MetadataObservation::Failed {
                class: FailureClass::PermissionDenied,
                transience: Transience::Permanent,
            };
            metadata.retain_failure(F::Timestamps);
            metadata.timestamps = MetadataObservation::Value {
                value: times(),
                provenance: MetadataProvenance::Inline,
            };
        }
        if plan.tags() != M::Omit {
            metadata.tags = MetadataObservation::Failed {
                class: FailureClass::PermissionDenied,
                transience: Transience::Permanent,
            };
        }
        Some(Ok(metadata))
    }

    async fn observe(
        &self,
        _path: &StoragePath,
        _plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        unreachable!("inline metadata must not trigger I/O")
    }

    async fn apply(
        &self,
        _path: &StoragePath,
        _mutation: MetadataMutation,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        unreachable!("traversal does not apply metadata")
    }
}

fn assert_observation_case(
    items: &[TraversalItem],
    outcome: &Result<TraversalOutcome, TraversalTerminalFailure>,
    emit: bool,
) {
    let failures = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::ObservationFailure(failure) => Some(failure.as_ref()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(failures.len(), 1, "{items:?}");
    let failure = failures[0];
    assert_eq!(
        (&failure.path, failure.family, &failure.version),
        (&path("file"), F::Tags, &SourceVersion::Current)
    );
    assert_eq!(
        (failure.class, failure.transience),
        (FailureClass::PermissionDenied, Transience::Permanent)
    );
    let Ok(TraversalOutcome::Completed(completed)) = outcome else {
        panic!("unexpected outcome: {outcome:?}");
    };
    assert_eq!(
        (
            completed.totals.listed,
            completed.totals.emitted,
            completed.totals.filtered,
            completed.totals.failed_children
        ),
        (1, u64::from(emit), u64::from(!emit), 0)
    );
    for item in items {
        if let TraversalItem::DirectoryListed(listed) = item {
            assert!(listed.counts.is_balanced());
        }
    }
}

async fn observation_case(order: TraversalOrder, inline: bool, emit: bool) {
    let mut fixture = fixture(false, S::Supported, Reply::Failed, None);
    if inline {
        fixture.source.metadata = Arc::new(InlineFailure { timestamps: false });
    }
    let filter = Arc::new(depends(F::Tags, emit));
    let (items, outcome) = run(
        &fixture.source,
        TraversalRequest::new(StoragePath::root())
            .order(order)
            .filter(filter.clone())
            .observation_plan(ObservationPlan::default().with_tags(M::BestEffort)),
    )
    .await;
    assert_observation_case(&items, &outcome, emit);
    assert_eq!(filter.calls.load(Ordering::SeqCst), 1);
    if inline {
        assert!(
            fixture
                .observations
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
        );
    }
}

#[tokio::test]
async fn observation_failure_events_survive_inline_and_deferred_filters_without_extra_counts() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        for inline in [false, true] {
            for emit in [false, true] {
                observation_case(order, inline, emit).await;
            }
        }
    }
}

fn reported_versions(items: &[TraversalItem]) -> Vec<SourceVersion> {
    items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::ObservationFailure(failure) => {
                assert_eq!(failure.family, F::Tags);
                Some(failure.version.clone())
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn version_observation_failures_are_reported_once_per_record_even_when_filtered() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        for emit in [false, true] {
            let fixture = fixture(false, S::Supported, Reply::Failed, None);
            let source = StorageTraversalSource::with_roles(
                Arc::new(HistoryListing),
                fixture.source.metadata.clone(),
            );
            let (items, outcome) = run(
                &source,
                TraversalRequest::new(StoragePath::root())
                    .order(order)
                    .filter(Arc::new(depends(F::Tags, emit)))
                    .observation_plan(ObservationPlan::default().with_tags(M::BestEffort)),
            )
            .await;
            let versions = reported_versions(&items);
            assert_eq!(
                versions,
                [
                    SourceVersion::Id("old".into()),
                    SourceVersion::Id("current".into())
                ]
            );
            let Ok(TraversalOutcome::Completed(completed)) = outcome else {
                panic!("unexpected outcome: {outcome:?}");
            };
            assert_eq!(
                (completed.totals.listed, completed.totals.failed_children),
                (1, 0)
            );
        }
    }
}

#[tokio::test]
async fn a_tags_failure_preserves_inline_timestamps_without_reporting_false_timestamp_errors() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        for emit in [false, true] {
            let fixture = fixture(false, S::Supported, Reply::CallFailed, None);
            let source = StorageTraversalSource::with_roles(
                Arc::new(HistoryListing),
                fixture.source.metadata.clone(),
            );
            let (items, outcome) = run(
                &source,
                TraversalRequest::new(StoragePath::root())
                    .order(order)
                    .filter(Arc::new(depends(F::Tags, emit)))
                    .observation_plan(
                        ObservationPlan::default()
                            .with_tags(M::BestEffort)
                            .with_timestamps(M::BestEffort),
                    ),
            )
            .await;
            let failures = items
                .iter()
                .filter_map(|item| match item {
                    TraversalItem::ObservationFailure(failure) => Some(failure.as_ref()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(failures.len(), 3, "{items:?}");
            assert_eq!(
                failures
                    .iter()
                    .filter(|failure| failure.family == F::Timestamps)
                    .count(),
                1
            );
            for item in &items {
                if let TraversalItem::Entry(entry) = item {
                    assert_eq!(entry.metadata().timestamps().value(), Some(&times()));
                    let current = entry
                        .history()
                        .and_then(ObjectHistory::current)
                        .and_then(VersionRecord::metadata)
                        .unwrap_or_else(|| panic!("expected observation"));
                    assert!(current.timestamps().value().is_some());
                    assert!(
                        !current
                            .failures()
                            .any(|failure| failure.family == F::Timestamps)
                    );
                }
            }
            let Ok(TraversalOutcome::Completed(completed)) = outcome else {
                panic!("{outcome:?}")
            };
            assert_eq!(
                (completed.totals.listed, completed.totals.failed_children),
                (1, 0)
            );
        }
    }
}

#[tokio::test]
async fn a_timestamp_only_fallback_reports_failure_while_the_filter_receives_its_value() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        for emit in [false, true] {
            let mut fixture = fixture(false, S::Supported, Reply::Failed, None);
            fixture.source.metadata = Arc::new(InlineFailure { timestamps: true });
            let filter = Arc::new(depends(F::Timestamps, emit));
            let (items, outcome) = run(
                &fixture.source,
                TraversalRequest::new(StoragePath::root())
                    .order(order)
                    .filter(filter.clone())
                    .observation_plan(ObservationPlan::default().with_timestamps(M::BestEffort)),
            )
            .await;
            let failures = items
                .iter()
                .filter_map(|item| match item {
                    TraversalItem::ObservationFailure(failure) => Some(failure.as_ref()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(failures.len(), 1);
            assert_eq!(
                (failures[0].family, &failures[0].version),
                (F::Timestamps, &SourceVersion::Current)
            );
            assert!(
                filter
                    .observed_state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .iter()
                    .all(|state| state.starts_with("Value"))
            );
            let Ok(TraversalOutcome::Completed(completed)) = outcome else {
                panic!("{outcome:?}")
            };
            assert_eq!(
                (
                    completed.totals.emitted,
                    completed.totals.filtered,
                    completed.totals.failed_children
                ),
                (u64::from(emit), u64::from(!emit), 0)
            );
        }
    }
}

#[tokio::test]
async fn a_required_history_family_failure_preserves_other_successful_families() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let fixture = fixture(false, S::Supported, Reply::Failed, None);
        let source = StorageTraversalSource::with_roles(
            Arc::new(HistoryListing),
            fixture.source.metadata.clone(),
        );
        let (items, outcome) = run(
            &source,
            TraversalRequest::new(StoragePath::root())
                .order(order)
                .observation_plan(
                    ObservationPlan::default()
                        .with_tags(M::Required)
                        .with_timestamps(M::BestEffort),
                ),
        )
        .await;
        let failures = items
            .iter()
            .filter_map(|item| match item {
                TraversalItem::ObservationFailure(failure) => Some(failure),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(failures.len(), 2);
        assert!(failures.iter().all(|failure| failure.family == F::Tags));
        let entry = items
            .iter()
            .find_map(|item| match item {
                TraversalItem::Entry(entry) => Some(entry),
                _ => None,
            })
            .unwrap_or_else(|| panic!("expected observation"));
        for record in entry
            .history()
            .unwrap_or_else(|| panic!("expected observation"))
            .records()
        {
            assert_eq!(
                record
                    .metadata()
                    .unwrap_or_else(|| panic!("expected observation"))
                    .timestamps()
                    .value(),
                Some(&times())
            );
        }
        assert!(matches!(outcome, Ok(TraversalOutcome::Completed(_))));
    }
}

struct ProgressiveMetadata;

#[async_trait]
impl Metadata for ProgressiveMetadata {
    async fn observe(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        FailingMetadata(MetadataFailureMode::Session)
            .observe(path, plan)
            .await
    }

    async fn observe_copy_bound_version(
        &self,
        path: &StoragePath,
        _identity: &SourceIdentity,
        version: &SourceVersion,
        plan: ObservationPlan,
    ) -> Result<CopiedMetadataObservation, StorageRoleFailure> {
        let observations = if version == &SourceVersion::Id("old".into()) {
            MetadataObservations {
                tags: MetadataObservation::Failed {
                    class: FailureClass::PermissionDenied,
                    transience: Transience::Permanent,
                },
                ..MetadataObservations::default()
            }
        } else {
            self.observe(path, plan).await?
        };
        Ok(CopiedMetadataObservation {
            observations,
            mode_without_ownership: None,
            owner_names_unmapped: false,
        })
    }

    async fn apply(
        &self,
        _path: &StoragePath,
        _mutation: MetadataMutation,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        unreachable!("traversal does not apply metadata")
    }
}

#[tokio::test]
async fn an_earlier_version_error_is_visible_before_a_later_version_ends_the_session() {
    use crate::traversal::Resumable;
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let source = StorageTraversalSource::with_roles(
            Arc::new(HistoryListing),
            Arc::new(ProgressiveMetadata),
        );
        let (items, outcome) = run(
            &source,
            TraversalRequest::new(StoragePath::root())
                .order(order)
                .resumable(Resumable::On {
                    every: std::time::Duration::from_hours(1),
                })
                .observation_plan(ObservationPlan::default().with_tags(M::BestEffort)),
        )
        .await;
        let failures = items
            .iter()
            .filter_map(|item| match item {
                TraversalItem::ObservationFailure(failure) => Some(failure),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(failures.len(), 1, "{items:?}");
        assert_eq!(
            (&failures[0].path, &failures[0].version),
            (&path("file"), &SourceVersion::Id("old".into()))
        );
        assert!(!items.iter().any(|item| matches!(
            item,
            TraversalItem::Entry(_) | TraversalItem::DirectoryListed(_)
        )));
        assert!(matches!(items.last(), Some(TraversalItem::Checkpoint(_))));
        assert!(matches!(outcome, Err(TraversalTerminalFailure::Session(_))));
    }
}

struct OrderedListing;

#[async_trait]
impl Namespace for OrderedListing {
    fn list(&self, request: ListRequest) -> ListingStream {
        entries(
            &request.path,
            vec![
                descriptor("a", EntryKind::File),
                descriptor("b", EntryKind::File),
            ],
        )
    }
}

struct OrderedMetadata;

#[async_trait]
impl Metadata for OrderedMetadata {
    fn observe_listed(
        &self,
        descriptor: &SourceDescriptor,
        plan: ObservationPlan,
    ) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
        let mut metadata = MetadataObservations::default();
        if plan.tags() != M::Omit {
            metadata.tags = if descriptor.path == path("a") {
                MetadataObservation::Value {
                    value: Vec::new(),
                    provenance: MetadataProvenance::Inline,
                }
            } else {
                MetadataObservation::Failed {
                    class: FailureClass::PermissionDenied,
                    transience: Transience::Permanent,
                }
            };
        }
        Some(Ok(metadata))
    }
    async fn observe(
        &self,
        _path: &StoragePath,
        _plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        unreachable!("all observations are inline")
    }
    async fn apply(
        &self,
        _path: &StoragePath,
        _mutation: MetadataMutation,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        unreachable!("traversal does not apply metadata")
    }
}

#[derive(Debug)]
struct HideFailedTags;

impl TraversalFilter for HideFailedTags {
    fn required_metadata(&self) -> MetadataRequirements {
        MetadataRequirements::NONE.with(F::Tags)
    }
    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        TraversalDecision {
            emit: candidate.metadata.tags().value().is_some(),
            descend: false,
            filter_children: true,
        }
    }
}

#[tokio::test]
async fn an_immediately_filtered_error_keeps_its_childs_listing_position() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let source =
            StorageTraversalSource::with_roles(Arc::new(OrderedListing), Arc::new(OrderedMetadata));
        let (items, outcome) = run(
            &source,
            TraversalRequest::new(StoragePath::root())
                .order(order)
                .filter(Arc::new(HideFailedTags))
                .observation_plan(ObservationPlan::default().with_tags(M::BestEffort)),
        )
        .await;
        let rows = items
            .iter()
            .filter_map(|item| match item {
                TraversalItem::Entry(entry) => Some(format!("entry {}", entry.path())),
                TraversalItem::ObservationFailure(failure) => {
                    Some(format!("error {}", failure.path))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(rows, ["entry a", "error b"]);
        let Ok(TraversalOutcome::Completed(completed)) = outcome else {
            panic!("{outcome:?}")
        };
        assert_eq!(
            (
                completed.totals.listed,
                completed.totals.emitted,
                completed.totals.filtered,
                completed.totals.failed_children
            ),
            (2, 1, 1, 0)
        );
    }
}

fn later_failure(observed: &StoragePath, terminal: bool) -> StorageRoleFailure {
    if terminal {
        StorageRoleFailure::Session(
            crate::model::BackendSessionFailure::new(
                Operation::Observe,
                FailureClass::Connectivity,
                Transience::Transient,
                "a later operation ended the session",
            )
            .unwrap_or_else(|error| panic!("invalid session failure: {error}")),
        )
    } else {
        StorageRoleFailure::Entry(entry_failure(observed, FailureClass::PermissionDenied))
    }
}

struct LaterFailureListing {
    marker: bool,
    terminal: bool,
}

#[async_trait]
impl Namespace for LaterFailureListing {
    fn list(&self, request: ListRequest) -> ListingStream {
        if !request.path.as_str().is_empty() {
            return entries(&request.path, Vec::new());
        }
        let child = if self.marker {
            let mut directory = descriptor("dir", EntryKind::Directory);
            let mut marker = descriptor("dir/", EntryKind::File);
            marker.size = Some(0);
            directory.listing.directory_marker = Some(Box::new(marker));
            directory
        } else {
            descriptor("link", EntryKind::Symlink)
        };
        entries(&request.path, vec![child])
    }
    async fn read_link(&self, observed: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        Err(later_failure(observed, self.terminal))
    }
}

struct LaterFailureMetadata {
    inline: bool,
    terminal: bool,
}

impl LaterFailureMetadata {
    fn metadata(plan: ObservationPlan) -> MetadataObservations {
        let mut metadata = MetadataObservations::default();
        if plan.tags() != M::Omit {
            metadata.tags = MetadataObservation::Failed {
                class: FailureClass::PermissionDenied,
                transience: Transience::Permanent,
            };
        }
        metadata
    }
}

#[async_trait]
impl Metadata for LaterFailureMetadata {
    fn observe_listed(
        &self,
        descriptor: &SourceDescriptor,
        plan: ObservationPlan,
    ) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
        if descriptor.path == path("dir") {
            return Some(Err(later_failure(&descriptor.path, self.terminal)));
        }
        self.inline.then(|| Ok(Self::metadata(plan)))
    }
    async fn observe(
        &self,
        _path: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        Ok(Self::metadata(plan))
    }
    async fn apply(
        &self,
        _path: &StoragePath,
        _mutation: MetadataMutation,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        unreachable!("traversal does not apply metadata")
    }
}

async fn later_failure_case(order: TraversalOrder, marker: bool, inline: bool, terminal: bool) {
    let source = StorageTraversalSource::with_roles(
        Arc::new(LaterFailureListing { marker, terminal }),
        Arc::new(LaterFailureMetadata { inline, terminal }),
    );
    let (items, outcome) = run(
        &source,
        TraversalRequest::new(StoragePath::root())
            .order(order)
            .observation_plan(ObservationPlan::default().with_tags(M::BestEffort)),
    )
    .await;
    let failures = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::ObservationFailure(failure) => Some(failure),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        failures.len(),
        1,
        "marker={marker} inline={inline} terminal={terminal}: {items:?}"
    );
    assert_eq!(failures[0].path, path(if marker { "dir/" } else { "link" }));
    assert!(
        !items
            .iter()
            .any(|item| matches!(item, TraversalItem::Entry(_)))
    );
    if terminal {
        assert!(matches!(outcome, Err(TraversalTerminalFailure::Session(_))));
        assert!(
            !items
                .iter()
                .any(|item| matches!(item, TraversalItem::DirectoryListed(_)))
        );
    } else {
        let Ok(TraversalOutcome::Completed(completed)) = outcome else {
            panic!("{outcome:?}")
        };
        assert_eq!(completed.totals.failed_children, 1);
        assert_eq!(completed.totals.vanished, 0);
    }
}

#[tokio::test]
async fn symlink_and_marker_metadata_errors_survive_a_later_operation_failure() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        for marker in [false, true] {
            for inline in [false, true] {
                for terminal in [false, true] {
                    later_failure_case(order, marker, inline, terminal).await;
                }
            }
        }
    }
}
