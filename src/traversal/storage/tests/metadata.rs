//! Observation authority, capability preflight and filter timing through the traversal seam.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};

use super::filter::TreeNamespace;
use super::*;
use crate::model::{
    HistoryOrder, MetadataCapabilities, MetadataFamily as F, MetadataRequirements, ObjectHistory,
    ObservationMode as M, ObservationSupport as S, RecordKind, StorageTimestamp, TimePrecision,
    VersionRecord,
};
use crate::storage::CopiedMetadataObservation;
use crate::traversal::{TraversalConfigurationError, TraversalOrder};

struct Listed {
    calls: Arc<AtomicUsize>,
    inline_time: bool,
}

fn times() -> TimestampMetadata {
    TimestampMetadata {
        accessed: None,
        modified: Some(
            StorageTimestamp::new(42, TimePrecision::Nanoseconds)
                .unwrap_or_else(|error| panic!("{error}")),
        ),
        created: None,
    }
}

#[async_trait]
impl Namespace for Listed {
    fn list(&self, request: ListRequest) -> ListingStream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut child = descriptor("file", EntryKind::File);
        if self.inline_time {
            child.inline_timestamps = Some(times());
        }
        entries(&request.path, vec![child])
    }
}

#[derive(Clone, Copy)]
enum Reply {
    Value,
    Unsupported,
    Failed,
    CallFailed,
}

struct Observed {
    calls: Arc<Mutex<Vec<ObservationPlan>>>,
    support: S,
    reply: Reply,
    /// When present, the inline-dependent decision must have happened before this call.
    decided: Option<Arc<AtomicUsize>>,
}

#[async_trait]
impl Metadata for Observed {
    fn observation_capabilities(&self) -> MetadataCapabilities {
        MetadataCapabilities::default().with(F::Tags, self.support)
    }

    async fn observe(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        if matches!(self.reply, Reply::CallFailed) {
            return Err(StorageRoleFailure::Entry(entry_failure(
                path,
                FailureClass::PermissionDenied,
            )));
        }
        if let Some(decided) = &self.decided {
            assert_eq!(decided.load(Ordering::SeqCst), 1);
        }
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(plan);
        let mut metadata = MetadataObservations::default();
        if plan.tags() != M::Omit {
            metadata.tags = match self.reply {
                Reply::Value => MetadataObservation::Value {
                    value: Vec::new(),
                    provenance: MetadataProvenance::AdditionalCall,
                },
                Reply::Unsupported => MetadataObservation::Unsupported,
                Reply::CallFailed => unreachable!("failed before observing individual families"),
                Reply::Failed => MetadataObservation::Failed {
                    class: FailureClass::PermissionDenied,
                    transience: Transience::Permanent,
                },
            };
        }
        if plan.timestamps() != M::Omit {
            metadata.timestamps = MetadataObservation::Value {
                value: times(),
                provenance: MetadataProvenance::AdditionalCall,
            };
        }
        Ok(metadata)
    }

    async fn apply(
        &self,
        _path: &StoragePath,
        _mutation: MetadataMutation,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        unreachable!("traversal does not apply metadata")
    }

    async fn observe_copy_bound_version(
        &self,
        path: &StoragePath,
        _expected: &SourceIdentity,
        _version: &SourceVersion,
        plan: ObservationPlan,
    ) -> Result<CopiedMetadataObservation, StorageRoleFailure> {
        Ok(CopiedMetadataObservation {
            observations: self.observe(path, plan).await?,
            mode_without_ownership: None,
            owner_names_unmapped: false,
        })
    }
}

struct HistoryListing;

#[async_trait]
impl Namespace for HistoryListing {
    fn list(&self, request: ListRequest) -> ListingStream {
        let mut child = descriptor("file", EntryKind::File);
        child.inline_timestamps = Some(times());
        let records = [("old", false, None), ("current", true, times().modified)]
            .into_iter()
            .map(|(version, latest, modified)| {
                VersionRecord::new(
                    Some(version.to_owned()),
                    latest,
                    RecordKind::Object {
                        size: 3,
                        etag: version.to_owned(),
                        modified,
                    },
                    child.source_identity.clone(),
                    None,
                )
                .unwrap_or_else(|error| panic!("{error}"))
            })
            .collect();
        child.listing.history = Some(Box::new(
            ObjectHistory::whole(records, HistoryOrder::Inferred)
                .unwrap_or_else(|error| panic!("{error}")),
        ));
        entries(&request.path, vec![child])
    }
}

#[tokio::test]
async fn an_inline_key_filter_does_not_wait_for_an_older_versions_missing_metadata() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let fixture = fixture(true, S::Unknown, Reply::Value, None);
        let source = StorageTraversalSource::with_roles(
            Arc::new(HistoryListing),
            fixture.source.metadata.clone(),
        );
        let filter = Arc::new(depends(F::Timestamps, false));
        let (_, outcome) = run(
            &source,
            TraversalRequest::new(StoragePath::root())
                .order(order)
                .filter(filter.clone())
                .observation_plan(ObservationPlan::default().with_timestamps(M::Required)),
        )
        .await;
        assert!(matches!(outcome, Ok(TraversalOutcome::Completed(_))));
        assert_eq!(filter.calls.load(Ordering::SeqCst), 1);
        assert!(
            fixture
                .observations
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
        );
    }
}

#[derive(Debug)]
struct Depends {
    family: F,
    calls: Arc<AtomicUsize>,
    emit: bool,
    observed_state: Arc<Mutex<Vec<String>>>,
}

impl TraversalFilter for Depends {
    fn required_metadata(&self) -> MetadataRequirements {
        MetadataRequirements::NONE.with(self.family)
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let state = match self.family {
            F::Tags => format!("{:?}", candidate.metadata.tags()),
            F::Timestamps => {
                assert_eq!(
                    candidate.modified,
                    candidate
                        .metadata
                        .timestamps()
                        .value()
                        .and_then(|times| times.modified)
                );
                format!("{:?}", candidate.metadata.timestamps())
            }
            _ => unreachable!(),
        };
        self.observed_state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(state);
        TraversalDecision {
            emit: self.emit,
            descend: false,
            filter_children: false,
        }
    }

    fn resume_key(&self) -> Option<Vec<u8>> {
        Some(vec![self.family as u8])
    }
}

struct Fixture {
    source: StorageTraversalSource,
    listings: Arc<AtomicUsize>,
    observations: Arc<Mutex<Vec<ObservationPlan>>>,
}

fn fixture(
    inline_time: bool,
    support: S,
    reply: Reply,
    decided: Option<Arc<AtomicUsize>>,
) -> Fixture {
    let listings = Arc::new(AtomicUsize::new(0));
    let observations = Arc::new(Mutex::new(Vec::new()));
    Fixture {
        source: StorageTraversalSource::with_roles(
            Arc::new(Listed {
                calls: listings.clone(),
                inline_time,
            }),
            Arc::new(Observed {
                calls: observations.clone(),
                support,
                reply,
                decided,
            }),
        ),
        listings,
        observations,
    }
}

async fn run(
    source: &StorageTraversalSource,
    request: TraversalRequest,
) -> (
    Vec<TraversalItem>,
    Result<TraversalOutcome, TraversalTerminalFailure>,
) {
    let mut session = source.traverse(request);
    let mut items = Vec::new();
    while let Some(item) = session.next_item().await {
        items.push(item);
    }
    (items, session.finish().await)
}

fn depends(family: F, emit: bool) -> Depends {
    Depends {
        family,
        emit,
        calls: Arc::new(AtomicUsize::new(0)),
        observed_state: Arc::new(Mutex::new(Vec::new())),
    }
}

#[tokio::test]
async fn invalid_requests_are_refused_before_any_listing_observation_or_decision() {
    for unsupported in [false, true] {
        let fixture = fixture(false, S::Unsupported, Reply::Unsupported, None);
        let filter = Arc::new(depends(F::Tags, true));
        let mut request = TraversalRequest::new(StoragePath::root()).filter(filter.clone());
        let expected = if unsupported {
            request.observation_plan = ObservationPlan::default().with_tags(M::Required);
            TraversalConfigurationError::UnsupportedMetadata(F::Tags)
        } else {
            TraversalConfigurationError::OmittedFilterMetadata(F::Tags)
        };
        let (items, outcome) = run(&fixture.source, request).await;
        assert!(items.is_empty());
        assert_eq!(
            outcome,
            Err(TraversalTerminalFailure::Configuration(expected))
        );
        assert_eq!(fixture.listings.load(Ordering::SeqCst), 0);
        assert!(
            fixture
                .observations
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
        );
        assert_eq!(filter.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn inline_only_missing_metadata_is_final_without_an_extra_call() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let fixture = fixture(false, S::Unknown, Reply::Value, None);
        let filter = Arc::new(depends(F::Timestamps, true));
        let (items, outcome) = run(
            &fixture.source,
            TraversalRequest::new(StoragePath::root())
                .order(order)
                .filter(filter.clone())
                .observation_plan(ObservationPlan::default().with_timestamps(M::InlineOnly)),
        )
        .await;
        assert!(matches!(outcome, Ok(TraversalOutcome::Completed(_))));
        assert!(
            items
                .iter()
                .any(|item| matches!(item, TraversalItem::Entry(_)))
        );
        assert_eq!(filter.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *filter
                .observed_state
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
            ["Unsupported"]
        );
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
async fn an_inline_dependency_filters_before_unrelated_observations() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        for emit in [false, true] {
            let filter = Arc::new(depends(F::Timestamps, emit));
            let fixture = fixture(true, S::Supported, Reply::Value, Some(filter.calls.clone()));
            let (items, outcome) = run(
                &fixture.source,
                TraversalRequest::new(StoragePath::root())
                    .order(order)
                    .filter(filter.clone())
                    .observation_plan(
                        ObservationPlan::default()
                            .with_timestamps(M::InlineOnly)
                            .with_tags(M::Required),
                    ),
            )
            .await;
            assert!(matches!(outcome, Ok(TraversalOutcome::Completed(_))));
            assert_eq!(filter.calls.load(Ordering::SeqCst), 1);
            let calls = fixture
                .observations
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            assert_eq!(calls.len(), usize::from(emit));
            if emit {
                assert_eq!(calls[0].timestamps(), M::Omit);
                assert_eq!(calls[0].tags(), M::Required);
                let entry = items
                    .iter()
                    .find_map(|item| match item {
                        TraversalItem::Entry(entry) => Some(entry),
                        _ => None,
                    })
                    .unwrap_or_else(|| panic!("an entry was expected"));
                assert_eq!(entry.modified(), times().modified);
            }
        }
    }
}

#[tokio::test]
async fn additional_dependencies_are_observed_once_and_their_states_reach_the_filter() {
    for reply in [Reply::Value, Reply::Failed, Reply::Unsupported] {
        for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
            let filter = Arc::new(depends(F::Tags, true));
            let fixture = fixture(false, S::Unknown, reply, None);
            let (_, outcome) = run(
                &fixture.source,
                TraversalRequest::new(StoragePath::root())
                    .order(order)
                    .filter(filter.clone())
                    .observation_plan(ObservationPlan::default().with_tags(M::BestEffort)),
            )
            .await;
            assert!(matches!(outcome, Ok(TraversalOutcome::Completed(_))));
            assert_eq!(
                fixture
                    .observations
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .len(),
                1
            );
            assert_eq!(filter.calls.load(Ordering::SeqCst), 1);
            let expected = match reply {
                Reply::Value => "Value",
                Reply::Failed => "Failed",
                Reply::Unsupported => "Unsupported",
                Reply::CallFailed => unreachable!("this matrix uses family replies"),
            };
            assert!(
                filter
                    .observed_state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)[0]
                    .starts_with(expected)
            );
        }
    }
}

#[tokio::test]
async fn known_unsupported_and_not_applicable_families_need_no_observation() {
    for (support, mode) in [
        (S::Unsupported, M::BestEffort),
        (S::NotApplicable, M::Required),
    ] {
        let fixture = fixture(false, support, Reply::Value, None);
        let (items, outcome) = run(
            &fixture.source,
            TraversalRequest::new(StoragePath::root())
                .observation_plan(ObservationPlan::default().with_tags(mode)),
        )
        .await;
        assert!(matches!(outcome, Ok(TraversalOutcome::Completed(_))));
        let entry = items
            .iter()
            .find_map(|item| match item {
                TraversalItem::Entry(entry) => Some(entry),
                _ => None,
            })
            .unwrap_or_else(|| panic!("an entry was expected"));
        assert_eq!(
            entry.metadata().tags(),
            &if support == S::Unsupported {
                MetadataObservation::Unsupported
            } else {
                MetadataObservation::NotApplicable
            }
        );
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
async fn unknown_required_support_is_checked_after_observation_without_evaluating_the_filter() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        for reply in [Reply::Unsupported, Reply::Failed] {
            let filter = Arc::new(depends(F::Tags, true));
            let fixture = fixture(false, S::Unknown, reply, None);
            let (items, outcome) = run(
                &fixture.source,
                TraversalRequest::new(StoragePath::root())
                    .order(order)
                    .filter(filter.clone())
                    .observation_plan(ObservationPlan::default().with_tags(M::Required)),
            )
            .await;
            assert!(matches!(outcome, Ok(TraversalOutcome::Completed(_))));
            assert!(
                items
                    .iter()
                    .any(|item| matches!(item, TraversalItem::Failure(_)))
            );
            assert!(
                !items
                    .iter()
                    .any(|item| matches!(item, TraversalItem::Entry(_)))
            );
            assert_eq!(filter.calls.load(Ordering::SeqCst), 0);
            assert_eq!(
                fixture
                    .observations
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .len(),
                1
            );
        }
    }
}

#[tokio::test]
async fn a_required_dependency_failure_on_a_directory_still_visits_its_subtree() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let fixture = fixture(false, S::Unknown, Reply::Unsupported, None);
        let namespace = Arc::new(TreeNamespace::new());
        let source =
            StorageTraversalSource::with_roles(namespace.clone(), fixture.source.metadata.clone());
        let filter = Arc::new(depends(F::Tags, true));
        let (items, outcome) = run(
            &source,
            TraversalRequest::new(StoragePath::root())
                .order(order)
                .filter(filter.clone())
                .observation_plan(ObservationPlan::default().with_tags(M::Required)),
        )
        .await;
        assert!(matches!(outcome, Ok(TraversalOutcome::Completed(_))));
        let failures: Vec<_> = items
            .iter()
            .filter_map(|item| match item {
                TraversalItem::Failure(failure) => Some(failure.path.as_str()),
                _ => None,
            })
            .collect();
        for path in ["keep", "keep/a", "keep/b", "keep/b/c"] {
            assert!(failures.contains(&path), "missing child failure: {path}");
        }
        let listed = namespace.listed();
        assert!(listed.contains(&"keep".to_owned()));
        assert!(listed.contains(&"keep/b".to_owned()));
        assert_eq!(filter.calls.load(Ordering::SeqCst), 0);
    }
}

mod observation_failures;
