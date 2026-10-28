//! A later session failure preserves the ordered output already discovered in its page.

use super::*;
use crate::model::{BackendSessionFailure, MetadataFamily, ObservationMode};
use crate::traversal::{FailureScope, Resumable, TraversalOrder};

mod gap;

struct MixedFailurePage;

#[async_trait]
impl Namespace for MixedFailurePage {
    fn list(&self, request: ListRequest) -> ListingStream {
        let page = ListingPage::from_descriptors(
            &request.path,
            vec![
                descriptor("warn", EntryKind::File),
                descriptor("broken", EntryKind::File),
                descriptor("tail", EntryKind::File),
                descriptor("denied", EntryKind::File),
            ],
            vec![entry_failure(
                &path("unreadable"),
                FailureClass::PermissionDenied,
            )],
            0,
        );
        ListingStream::from_pages(request.path, [Ok(page)])
    }
}

struct MixedMetadata {
    inline: bool,
}

impl MixedMetadata {
    fn result(observed: &StoragePath) -> Result<MetadataObservations, StorageRoleFailure> {
        if observed == &path("denied") {
            return Err(StorageRoleFailure::Entry(entry_failure(
                observed,
                FailureClass::PermissionDenied,
            )));
        }
        if observed == &path("broken") {
            return Err(StorageRoleFailure::Session(
                BackendSessionFailure::new(
                    Operation::Observe,
                    FailureClass::Connectivity,
                    Transience::Transient,
                    "the session failed after earlier page errors",
                )
                .unwrap_or_else(|error| panic!("{error}")),
            ));
        }
        Ok(MetadataObservations {
            tags: MetadataObservation::Failed {
                class: FailureClass::PermissionDenied,
                transience: Transience::Permanent,
            },
            ..MetadataObservations::default()
        })
    }
}

#[async_trait]
impl Metadata for MixedMetadata {
    fn observe_listed(
        &self,
        descriptor: &SourceDescriptor,
        _plan: ObservationPlan,
    ) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
        self.inline.then(|| Self::result(&descriptor.path))
    }

    async fn observe(
        &self,
        observed: &StoragePath,
        _plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        Self::result(observed)
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

fn session(order: TraversalOrder, inline: bool, resumable: bool) -> TraversalSession {
    StorageTraversalSource::with_roles(
        Arc::new(MixedFailurePage),
        Arc::new(MixedMetadata { inline }),
    )
    .output_capacity(NonZeroUsize::MIN)
    .traverse(
        TraversalRequest::new(StoragePath::root())
            .order(order)
            .max_inflight_operations(NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN))
            .resumable(if resumable {
                Resumable::on()
            } else {
                Resumable::Off
            })
            .observation_plan(ObservationPlan::default().with_tags(ObservationMode::BestEffort)),
    )
}

async fn drain(mut session: TraversalSession) -> super::resume::Run {
    let mut items = Vec::new();
    while let Some(item) = session.next_item().await {
        items.push(item);
    }
    (items, session.finish().await)
}

fn assert_page_prefix(items: &[TraversalItem], resumable: bool) {
    let Some(TraversalItem::Failure(child)) = items.first() else {
        panic!("{items:?}")
    };
    assert_eq!(
        (child.scope, child.path.as_str()),
        (FailureScope::Child, "unreadable")
    );
    let Some(TraversalItem::ObservationFailure(metadata)) = items.get(1) else {
        panic!("{items:?}")
    };
    assert_eq!(
        (metadata.path.as_str(), metadata.family),
        ("warn", MetadataFamily::Tags)
    );
    assert_eq!(metadata.class, FailureClass::PermissionDenied);
    assert!(
        matches!(items.get(2), Some(TraversalItem::Entry(entry)) if entry.path() == &path("warn"))
    );
    assert!(
        !items
            .iter()
            .any(|item| matches!(item, TraversalItem::DirectoryListed(_)))
    );
    assert_eq!(
        matches!(items.last(), Some(TraversalItem::Checkpoint(_))),
        resumable
    );
}

#[tokio::test]
async fn page_errors_are_delivered_before_a_later_session_failure() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        for inline in [true, false] {
            for resumable in [false, true] {
                let (items, outcome) = drain(session(order, inline, resumable)).await;
                assert_page_prefix(&items, resumable);
                if inline {
                    assert_known_tail_errors(&items);
                }
                assert!(
                    matches!(outcome, Err(TraversalTerminalFailure::Session(ref failure))
                    if failure.class() == FailureClass::Connectivity),
                    "{order:?}/{inline}: {outcome:?}"
                );
            }
        }
    }
}

fn assert_known_tail_errors(items: &[TraversalItem]) {
    assert_eq!(
        items
            .iter()
            .filter(|item| matches!(item,
                TraversalItem::ObservationFailure(failure) if failure.path == path("tail")
            ))
            .count(),
        1,
        "{items:?}"
    );
    assert_eq!(
        items
            .iter()
            .filter(|item| matches!(item,
                TraversalItem::Failure(failure) if failure.path == path("denied")
                    && failure.scope == FailureScope::Child
            ))
            .count(),
        1,
        "{items:?}"
    );
    assert!(!items.iter().any(|item| matches!(item,
        TraversalItem::Entry(entry) if entry.path() == &path("tail") || entry.path() == &path("denied")
    )));
}
