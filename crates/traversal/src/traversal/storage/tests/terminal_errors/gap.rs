//! Terminal details cross a pending observation without advancing its resume position.

use std::sync::atomic::{AtomicBool, Ordering};

use super::super::resume::entry_paths;
use super::*;
use crate::model::{HistoryOrder, ObjectHistory, RecordKind, VersionRecord};
use crate::storage::CopiedMetadataObservation;
use crate::traversal::Checkpoint;
use crate::traversal::checkpoint::{fingerprint, open};

struct GapNamespace;

#[async_trait]
impl Namespace for GapNamespace {
    fn list(&self, request: ListRequest) -> ListingStream {
        let mut history = descriptor("history", EntryKind::File);
        let records = ["v1", "v2"]
            .into_iter()
            .map(|version| {
                VersionRecord::from_listing(
                    version,
                    version == "v2",
                    RecordKind::Object {
                        size: 3,
                        etag: "etag".into(),
                        modified: None,
                    },
                    descriptor(version, EntryKind::File).source_identity,
                )
                .unwrap_or_else(|error| panic!("{error}"))
            })
            .collect();
        history.listing.history = Some(Box::new(
            ObjectHistory::whole(records, HistoryOrder::Server)
                .unwrap_or_else(|error| panic!("{error}")),
        ));
        answer(
            &request.path,
            Ok((
                vec![
                    descriptor("gap", EntryKind::File),
                    descriptor("ready", EntryKind::File),
                    history,
                ],
                vec![entry_failure(
                    &path("unreadable"),
                    FailureClass::PermissionDenied,
                )],
            )),
        )
    }
}

struct GapMetadata {
    gate: Arc<tokio::sync::Semaphore>,
    failing: AtomicBool,
}

impl GapMetadata {
    fn tags(failed: bool) -> MetadataObservations {
        crate::model::MetadataObservationParts {
            tags: if failed {
                MetadataObservation::Failed {
                    class: FailureClass::PermissionDenied,
                    transience: Transience::Permanent,
                }
            } else {
                MetadataObservation::Value {
                    value: Vec::new(),
                    provenance: MetadataProvenance::AdditionalCall,
                }
            },
            ..crate::model::MetadataObservationParts::default()
        }
        .build()
        .unwrap_or_else(|error| panic!("{error}"))
    }
}

#[async_trait]
impl Metadata for GapMetadata {
    fn observe_listed(
        &self,
        descriptor: &SourceDescriptor,
        _plan: ObservationPlan,
    ) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
        (descriptor.path == path("ready")).then(|| Ok(Self::tags(true)))
    }

    async fn observe(
        &self,
        observed: &StoragePath,
        _plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        assert_eq!(observed, &path("gap"));
        let permit = self
            .gate
            .acquire()
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        permit.forget();
        Ok(Self::tags(false))
    }

    async fn observe_copy_bound_version(
        &self,
        _path: &StoragePath,
        _expected: &SourceIdentity,
        version: &SourceVersion,
        _plan: ObservationPlan,
    ) -> Result<CopiedMetadataObservation, StorageRoleFailure> {
        let failing = self.failing.load(Ordering::SeqCst);
        if failing && version == &SourceVersion::Id("v2".into()) {
            return MixedMetadata::result(&path("broken")).map(|_| unreachable!());
        }
        Ok(CopiedMetadataObservation {
            observations: Self::tags(failing),
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

fn request() -> TraversalRequest {
    TraversalRequest::new(StoragePath::root())
        .order(TraversalOrder::Blocks)
        .max_inflight_operations(NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN))
        .resumable(Resumable::on())
        .observation_plan(ObservationPlan::default().with_tags(ObservationMode::BestEffort))
}

fn assert_terminal_details(items: &[TraversalItem]) -> Checkpoint {
    assert!(!items.iter().any(|item| matches!(
        item,
        TraversalItem::Entry(_) | TraversalItem::DirectoryListed(_)
    )));
    let failures: Vec<_> = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Failure(failure) => Some(failure.path.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(failures, ["unreadable"]);
    let mut metadata: Vec<_> = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::ObservationFailure(failure) => {
                Some((failure.path.as_str(), failure.version.clone()))
            }
            _ => None,
        })
        .collect();
    metadata.sort_by(|left, right| left.0.cmp(right.0));
    assert_eq!(
        metadata,
        [
            ("history", SourceVersion::Id("v1".into())),
            ("ready", SourceVersion::Current)
        ]
    );
    let Some(TraversalItem::Checkpoint(checkpoint)) = items.last() else {
        panic!("{items:?}")
    };
    (**checkpoint).clone()
}

fn assert_root_position(checkpoint: &Checkpoint) {
    let identity = crate::storage::backends::local::test_identity("traversal-test");
    let print = fingerprint(&identity, &request(), &[0]).unwrap_or_else(|error| panic!("{error}"));
    let chain = open(
        checkpoint,
        &print,
        TraversalOrder::Blocks,
        &StoragePath::root(),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(chain.len(), 1);
    assert_eq!(chain[0].path, StoragePath::root());
}

#[tokio::test]
async fn terminal_details_cross_a_gap_without_completing_or_skipping_it() {
    let metadata = Arc::new(GapMetadata {
        gate: Arc::new(tokio::sync::Semaphore::new(0)),
        failing: AtomicBool::new(true),
    });
    let source = StorageTraversalSource::with_roles(
        Arc::new(GapNamespace),
        Arc::clone(&metadata) as Arc<dyn Metadata>,
    );
    let (items, outcome) = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        drain(source.traverse(request())),
    )
    .await
    .unwrap_or_else(|_| panic!("terminal details waited for the incomplete gap"));
    assert!(
        matches!(outcome, Err(TraversalTerminalFailure::Session(_))),
        "{outcome:?}"
    );
    let checkpoint = assert_terminal_details(&items);
    assert_root_position(&checkpoint);
    metadata.failing.store(false, Ordering::SeqCst);
    metadata.gate.add_permits(2);
    let (resumed, outcome) = drain(source.traverse(request().resume_from(checkpoint))).await;
    assert!(
        matches!(outcome, Ok(TraversalOutcome::Completed(_))),
        "{outcome:?}"
    );
    let entries = entry_paths(&resumed);
    assert!(entries.iter().any(|entry| entry == "gap"));
    assert!(entries.iter().any(|entry| entry == "history"));
}
