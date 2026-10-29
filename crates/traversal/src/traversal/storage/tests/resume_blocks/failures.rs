//! New errors encountered while reconstructing an ancestor are observable through the session.

use crate::model::MetadataObservationParts;
use std::sync::atomic::{AtomicU8, Ordering};

use futures::StreamExt as _;

use super::*;
use crate::model::{BackendSessionFailure, MetadataFamily, MetadataRequirements, ObservationMode};
use crate::storage::{ChildFailure, PageSink};
use crate::traversal::{TraversalCandidate, TraversalDecision, TraversalFilter};

struct Faults(AtomicU8);

fn broken_session() -> StorageRoleFailure {
    StorageRoleFailure::Session(
        BackendSessionFailure::new(
            Operation::Observe,
            FailureClass::Connectivity,
            Transience::Transient,
            "ancestor observation session broke",
        )
        .unwrap_or_else(|error| panic!("{error}")),
    )
}

impl Faults {
    fn failed_tags() -> MetadataObservation<Vec<crate::model::ObjectTag>> {
        MetadataObservation::Failed {
            class: FailureClass::PermissionDenied,
            transience: Transience::Permanent,
        }
    }

    fn failure(&self, observed: &StoragePath) -> Option<StorageRoleFailure> {
        if observed != &path("d2") {
            return None;
        }
        match self.0.load(Ordering::SeqCst) {
            2 => Some(StorageRoleFailure::Entry(entry_failure(
                observed,
                FailureClass::PermissionDenied,
            ))),
            3..=5 => Some(broken_session()),
            _ => None,
        }
    }
}

struct ListingFault {
    tree: Arc<Tree>,
    faults: Arc<Faults>,
}

#[async_trait]
impl Namespace for ListingFault {
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path.clone();
        let mut stream = self.tree.list(request);
        let inject = directory == StoragePath::root()
            && matches!(self.faults.0.load(Ordering::SeqCst), 1 | 5 | 6);
        let broken_ancestor = directory == path("d0") && self.faults.0.load(Ordering::SeqCst) == 6;
        ListingStream::from_producer(directory.clone(), move |sink: PageSink| async move {
            if broken_ancestor {
                return sink.fail(broken_session()).await;
            }
            let mut inject = inject;
            while let Some(item) = stream.next().await {
                let mut page = match item {
                    Ok(page) => page,
                    Err(failure) => return sink.fail(failure).await,
                };
                if std::mem::take(&mut inject) {
                    page.push_failure(ChildFailure::from_failure(
                        &directory,
                        entry_failure(&path("unreadable"), FailureClass::PermissionDenied),
                    ));
                }
                if sink.send(page).await.is_err() {
                    return;
                }
            }
        })
    }
}

#[async_trait]
impl Metadata for Faults {
    async fn observe(
        &self,
        observed: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        if let Some(failure) = self.failure(observed) {
            return Err(failure);
        }
        let mut metadata =
            MetadataObservationParts::from(FakeMetadata.observe(observed, plan).await?);
        if self.0.load(Ordering::SeqCst) == 7 && observed == &path("d2") {
            metadata.tags = Self::failed_tags();
        }
        Ok(metadata.build().unwrap_or_else(|error| panic!("{error}")))
    }

    fn observe_listed(
        &self,
        descriptor: &SourceDescriptor,
        _plan: ObservationPlan,
    ) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
        if self.0.load(Ordering::SeqCst) == 8 && descriptor.path == path("d2") {
            return Some(Ok(crate::model::MetadataObservationParts {
                tags: Self::failed_tags(),
                timestamps: MetadataObservation::Value {
                    value: TimestampMetadata {
                        accessed: None,
                        modified: None,
                        created: None,
                    },
                    provenance: MetadataProvenance::Inline,
                },
                ..crate::model::MetadataObservationParts::default()
            }
            .build()
            .unwrap_or_else(|error| panic!("{error}"))));
        }
        (self.0.load(Ordering::SeqCst) == 4)
            .then(|| self.failure(&descriptor.path))
            .flatten()
            .map(Err)
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
struct NeedsTime;

impl TraversalFilter for NeedsTime {
    fn required_metadata(&self) -> MetadataRequirements {
        MetadataRequirements::NONE.with(MetadataFamily::Timestamps)
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        TraversalDecision::unfiltered(candidate.kind)
    }

    fn resume_key(&self) -> Option<Vec<u8>> {
        Some(b"resume error visibility".to_vec())
    }
}

fn observing_request() -> TraversalRequest {
    request(TraversalOrder::Blocks, 2)
        .filter(Arc::new(NeedsTime))
        .observation_plan(
            ObservationPlan::default()
                .with_timestamps(ObservationMode::Required)
                .with_tags(ObservationMode::BestEffort),
        )
}

async fn run_fault(mode: u8) -> super::super::resume::Run {
    let tree = Arc::new(Tree::generate(3, 3, 2, 2));
    let faults = Arc::new(Faults(AtomicU8::new(0)));
    let source = StorageTraversalSource::with_roles(
        Arc::new(ListingFault {
            tree,
            faults: Arc::clone(&faults),
        }),
        Arc::clone(&faults) as Arc<dyn Metadata>,
    );
    let (items, outcome) = run(&source, observing_request().resumable(every_chance()), None).await;
    assert!(matches!(outcome, Ok(TraversalOutcome::Completed(_))));
    let checkpoint = checkpoints(&items)
        .into_iter()
        .map(|(_, checkpoint)| checkpoint)
        .find(|checkpoint| {
            let chain = position(&observing_request(), checkpoint);
            chain.len() >= 3 && chain[1].path == path("d0")
        })
        .unwrap_or_else(|| panic!("no checkpoint within d0"));
    faults.0.store(mode, Ordering::SeqCst);
    run(
        &source,
        observing_request()
            .resumable(every_chance())
            .resume_from(checkpoint),
        None,
    )
    .await
}

fn assert_child_failure(
    items: &[TraversalItem],
    outcome: &Result<TraversalOutcome, TraversalTerminalFailure>,
    failed: &str,
) {
    let failures: Vec<_> = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Failure(failure) => Some(failure),
            _ => None,
        })
        .collect();
    assert_eq!(failures.len(), 1, "{items:?}");
    assert_eq!(
        (failures[0].scope, failures[0].path.as_str()),
        (FailureScope::Child, failed)
    );
    assert_eq!(failures[0].failure.class(), FailureClass::PermissionDenied);
    let Ok(TraversalOutcome::Completed(completion)) = outcome else {
        panic!("{outcome:?}")
    };
    assert_eq!(completion.totals.failed_children, 1);
    assert!(completion.totals.is_balanced());
    assert!(!items.iter().any(|item| matches!(item, TraversalItem::DirectoryListed(listed) if !listed.outcome.is_complete())));
}

#[tokio::test]
async fn relisted_page_child_failures_reach_the_public_stream() {
    let (items, outcome) = run_fault(1).await;
    assert_child_failure(&items, &outcome, "unreadable");
}

#[tokio::test]
async fn relisted_deferred_observation_failure_is_reported_and_still_descends() {
    let (items, outcome) = run_fault(2).await;
    assert_child_failure(&items, &outcome, "d2");
    assert!(
        entry_paths(&items)
            .iter()
            .any(|entry| entry.starts_with("d2/"))
    );
}

#[tokio::test]
async fn relisted_session_failure_ends_the_public_session() {
    for mode in [3, 4] {
        let (_, outcome) = run_fault(mode).await;
        assert!(
            matches!(outcome, Err(TraversalTerminalFailure::Session(ref failure)) if failure.class() == FailureClass::Connectivity),
            "mode {mode}: {outcome:?}"
        );
    }
}

#[tokio::test]
async fn relisted_child_failure_precedes_a_later_session_failure() {
    for mode in [5, 6] {
        let (items, outcome) = run_fault(mode).await;
        let Some(TraversalItem::Failure(failure)) = items.first() else {
            panic!("mode {mode}: earlier failure was lost: {items:?}")
        };
        assert_eq!(
            (failure.scope, failure.path.as_str()),
            (FailureScope::Child, "unreadable")
        );
        assert!(
            matches!(outcome, Err(TraversalTerminalFailure::Session(_))),
            "mode {mode}: {outcome:?}"
        );
        assert!(matches!(items.last(), Some(TraversalItem::Checkpoint(_))));
    }
}

#[tokio::test]
async fn relisted_metadata_errors_are_visible_without_replaying_entries() {
    for mode in [7, 8] {
        let (items, outcome) = run_fault(mode).await;
        let failures: Vec<_> = items
            .iter()
            .filter_map(|item| match item {
                TraversalItem::ObservationFailure(failure) => Some(failure),
                _ => None,
            })
            .collect();
        assert_eq!(failures.len(), 1, "mode {mode}: {items:?}");
        assert_eq!(
            (&failures[0].path, failures[0].family),
            (&path("d2"), MetadataFamily::Tags)
        );
        assert_eq!(failures[0].class, FailureClass::PermissionDenied);
        assert!(!entry_paths(&items).iter().any(|entry| entry == "d2"));
        let Ok(TraversalOutcome::Completed(completion)) = outcome else {
            panic!("{outcome:?}")
        };
        assert_eq!(completion.totals.failed_children, 0);
        assert!(completion.totals.is_balanced());
    }
}
