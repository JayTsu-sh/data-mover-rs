use std::num::NonZeroUsize;

use async_trait::async_trait;

use super::*;
use crate::model::{
    BackendIdentity, BackendKind as Kind, IdentityStrength, MetadataObservation,
    MetadataObservations, MetadataProvenance, ObservationPlan, SourceIdentity, SymlinkTarget,
    SymlinkTargetEncoding, TimestampMetadata,
};
use crate::storage::{MetadataMutation, NamespaceRequest};

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
        inline_mode: None,
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

/// Lists `dir` with one describable child and one child the backend could not describe.
struct PartialListingNamespace;

#[async_trait]
impl Namespace for PartialListingNamespace {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        match request {
            NamespaceRequest::List(root) if root == StoragePath::root() => Ok(
                NamespaceResult::Entries(vec![descriptor("dir", EntryKind::Directory)]),
            ),
            NamespaceRequest::List(root) if root == path("dir") => Ok(NamespaceResult::Listing {
                entries: vec![descriptor("dir/ok", EntryKind::File)],
                failures: vec![entry_failure(&path("dir/bad"), FailureClass::Unsupported)],
            }),
            _ => Err(StorageRoleFailure::Entry(entry_failure(
                &StoragePath::root(),
                FailureClass::NotFound,
            ))),
        }
    }
}

#[tokio::test]
async fn an_undescribable_child_is_a_failure_item_without_hiding_its_siblings() {
    let source = StorageTraversalSource::with_roles(
        Arc::new(PartialListingNamespace),
        Arc::new(FakeMetadata),
    );
    let mut request = request(tokio_util::sync::CancellationToken::new());
    request.observation_plan = ObservationPlan::default();
    let mut session = source.traverse(request);
    let mut entries = Vec::new();
    let mut failures = Vec::new();
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => entries.push(entry.path().as_str().to_owned()),
            TraversalItem::EntryFailure(error) => failures.push(error.path().as_str().to_owned()),
            TraversalItem::DirectoryListed(_) | TraversalItem::SubtreeComplete(_) => {}
        }
    }
    assert_eq!(entries, ["dir", "dir/ok"]);
    assert_eq!(failures, ["dir/bad"]);
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Completed(TraversalCompletion {
            observed_entries: 2,
            entry_failures: 1
        }))
    ));
}

/// Lists `file-00` … `file-11` at the root.
struct WideNamespace;

#[async_trait]
impl Namespace for WideNamespace {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        match request {
            NamespaceRequest::List(root) if root == StoragePath::root() => {
                Ok(NamespaceResult::Entries(
                    (0..12)
                        .map(|index| descriptor(&format!("file-{index:02}"), EntryKind::File))
                        .collect(),
                ))
            }
            _ => Ok(NamespaceResult::Entries(Vec::new())),
        }
    }
}

/// Counts started observations and holds the first one back, so everything after it settles
/// out of order and waits in the reorder buffer.
#[derive(Default)]
struct SlowFirstMetadata {
    started: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl Metadata for SlowFirstMetadata {
    async fn observe(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        self.started
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if path.as_str() == "file-00" {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
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
async fn the_admission_window_bounds_entries_waiting_to_be_emitted_not_only_inflight_ones() {
    let metadata = Arc::new(SlowFirstMetadata::default());
    let source = StorageTraversalSource::with_roles(
        Arc::new(WideNamespace),
        Arc::clone(&metadata) as Arc<dyn Metadata>,
    );
    let mut request = request(tokio_util::sync::CancellationToken::new());
    request.max_inflight_operations =
        NonZeroUsize::new(3).unwrap_or_else(|| unreachable!("constant is nonzero"));
    let mut session = source.traverse(request);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    // `file-00` is still held back, so nothing can be emitted: the entries behind it that
    // already settled must still count against the window.
    assert!(metadata.started.load(std::sync::atomic::Ordering::SeqCst) <= 3);
    let mut emitted = Vec::new();
    while let Some(item) = session.next_item().await {
        let TraversalItem::Entry(entry) = item else {
            panic!("unexpected failure item")
        };
        emitted.push(entry.path().as_str().to_owned());
    }
    let expected = (0..12)
        .map(|index| format!("file-{index:02}"))
        .collect::<Vec<_>>();
    assert_eq!(emitted, expected);
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Completed(_))
    ));
}

mod cancel;
mod filter;
mod prefetch;
