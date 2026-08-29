use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use super::{
    TraversalCompletion, TraversalItem, TraversalOutcome, TraversalRequest, TraversalSession,
    TraversalSource, TraversalTerminalFailure,
};
use crate::model::{
    EntryKind, EntryOperationFailure, FailureClass, MetadataObservations, Operation, StoragePath,
    Transience,
};
use crate::storage::{
    CapabilityUnavailable, Metadata, Namespace, NamespaceRequest, NamespaceResult, PreflightPolicy,
    SourceDescriptor, Storage, StorageRoleFailure,
};

/// Protocol-neutral traversal assembled from one connected storage's namespace and metadata roles.
pub struct StorageTraversalSource {
    namespace: Arc<dyn Namespace>,
    metadata: Arc<dyn Metadata>,
}

impl StorageTraversalSource {
    /// Lends the roles required for traversal using production capability policy.
    ///
    /// # Errors
    /// Returns the first unavailable role before starting backend I/O.
    pub fn new(storage: &Storage) -> Result<Self, CapabilityUnavailable> {
        let policy = PreflightPolicy::production();
        Ok(Self {
            namespace: storage.namespace(&policy)?,
            metadata: storage.metadata(&policy)?,
        })
    }

    #[cfg(test)]
    fn with_roles(namespace: Arc<dyn Namespace>, metadata: Arc<dyn Metadata>) -> Self {
        Self {
            namespace,
            metadata,
        }
    }
}

impl TraversalSource for StorageTraversalSource {
    fn traverse(&self, request: TraversalRequest) -> TraversalSession {
        let (item_tx, item_rx) = mpsc::channel(request.max_buffered_items.get());
        let (completion_tx, completion_rx) = oneshot::channel();
        let cancel = request.cancel.clone();
        tokio::spawn(run(
            Arc::clone(&self.namespace),
            Arc::clone(&self.metadata),
            request,
            item_tx,
            completion_tx,
        ));
        TraversalSession::new(item_rx, completion_rx, cancel)
    }
}

type ObservationTask = JoinSet<(u64, Result<crate::model::ObservedEntry, StorageRoleFailure>)>;

struct State {
    next_sequence: u64,
    next_output: u64,
    observed: u64,
    failed: u64,
    pending: BTreeMap<u64, TraversalItem>,
}

impl State {
    const fn new() -> Self {
        Self {
            next_sequence: 0,
            next_output: 0,
            observed: 0,
            failed: 0,
            pending: BTreeMap::new(),
        }
    }
}

async fn run(
    namespace: Arc<dyn Namespace>,
    metadata: Arc<dyn Metadata>,
    request: TraversalRequest,
    items: mpsc::Sender<TraversalItem>,
    completion: oneshot::Sender<Result<TraversalOutcome, TraversalTerminalFailure>>,
) {
    let mut state = State::new();
    let mut directories = VecDeque::from([request.root.clone()]);
    let mut tasks = JoinSet::new();
    let result = enumerate_and_observe(
        &namespace,
        &metadata,
        &request,
        &items,
        &mut directories,
        &mut tasks,
        &mut state,
    )
    .await;
    tasks.abort_all();
    drop(items);
    let terminal = if request.cancel.is_cancelled() {
        Ok(TraversalOutcome::Cancelled)
    } else {
        result.map(|()| {
            TraversalOutcome::Completed(TraversalCompletion {
                observed_entries: state.observed,
                entry_failures: state.failed,
            })
        })
    };
    let _ = completion.send(terminal);
}

async fn enumerate_and_observe(
    namespace: &Arc<dyn Namespace>,
    metadata: &Arc<dyn Metadata>,
    request: &TraversalRequest,
    items: &mpsc::Sender<TraversalItem>,
    directories: &mut VecDeque<StoragePath>,
    tasks: &mut ObservationTask,
    state: &mut State,
) -> Result<(), TraversalTerminalFailure> {
    while let Some(directory) = directories.pop_front() {
        if request.cancel.is_cancelled() {
            return Ok(());
        }
        let listed = namespace
            .execute(NamespaceRequest::List(directory.clone()))
            .await;
        let descriptors = match listed {
            Ok(NamespaceResult::Entries(values)) => values,
            Ok(_) => {
                queue_failure(state, entry_failure(&directory, FailureClass::Protocol))?;
                flush(items, state, &request.cancel).await?;
                continue;
            }
            Err(StorageRoleFailure::Entry(error)) => {
                queue_failure(state, error)?;
                flush(items, state, &request.cancel).await?;
                continue;
            }
            Err(StorageRoleFailure::Session(error)) => {
                return Err(TraversalTerminalFailure::Session(error));
            }
        };
        for descriptor in descriptors {
            if descriptor.kind == EntryKind::Directory {
                directories.push_back(descriptor.path.clone());
            }
            while tasks.len() >= request.max_inflight_operations.get() {
                if request.cancel.is_cancelled() {
                    return Ok(());
                }
                settle(tasks, state, &request.cancel).await?;
                flush(items, state, &request.cancel).await?;
            }
            let sequence = state.next_sequence;
            state.next_sequence = state
                .next_sequence
                .checked_add(1)
                .ok_or(TraversalTerminalFailure::Internal)?;
            let namespace = Arc::clone(namespace);
            let metadata = Arc::clone(metadata);
            let plan = request.observation_plan;
            tasks.spawn(async move {
                let result = observe(namespace, metadata, descriptor, plan).await;
                (sequence, result)
            });
        }
        flush(items, state, &request.cancel).await?;
    }
    while !tasks.is_empty() && !request.cancel.is_cancelled() {
        settle(tasks, state, &request.cancel).await?;
        flush(items, state, &request.cancel).await?;
    }
    Ok(())
}

async fn observe(
    namespace: Arc<dyn Namespace>,
    metadata_role: Arc<dyn Metadata>,
    descriptor: SourceDescriptor,
    plan: crate::model::ObservationPlan,
) -> Result<crate::model::ObservedEntry, StorageRoleFailure> {
    let metadata = metadata_role.observe(&descriptor.path, plan).await?;
    let modified = modified(&metadata);
    let entry = if descriptor.kind == EntryKind::Symlink {
        let result = namespace
            .execute(NamespaceRequest::ReadLink(descriptor.path.clone()))
            .await?;
        let NamespaceResult::LinkTarget(target) = result else {
            return Err(StorageRoleFailure::Entry(entry_failure(
                &descriptor.path,
                FailureClass::Protocol,
            )));
        };
        crate::model::ObservedEntry::new_symlink(
            descriptor.path.clone(),
            modified,
            descriptor.source_identity,
            target,
        )
    } else {
        crate::model::ObservedEntry::new(
            descriptor.path.clone(),
            descriptor.kind,
            descriptor.size,
            modified,
            descriptor.source_identity,
        )
    }
    .map_err(|_| {
        StorageRoleFailure::Entry(entry_failure(&descriptor.path, FailureClass::Protocol))
    })?;
    Ok(entry.with_metadata(metadata))
}

fn modified(metadata: &MetadataObservations) -> Option<crate::model::StorageTimestamp> {
    metadata
        .timestamps()
        .value()
        .and_then(|value| value.modified)
}

async fn settle(
    tasks: &mut ObservationTask,
    state: &mut State,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<(), TraversalTerminalFailure> {
    let result = tokio::select! {
        biased;
        () = cancel.cancelled() => return Ok(()),
        result = tasks.join_next() => result,
    };
    match result {
        Some(Ok((sequence, Ok(entry)))) => {
            state
                .pending
                .insert(sequence, TraversalItem::Entry(Box::new(entry)));
        }
        Some(Ok((sequence, Err(StorageRoleFailure::Entry(error))))) => {
            state
                .pending
                .insert(sequence, TraversalItem::EntryFailure(error));
        }
        Some(Ok((_, Err(StorageRoleFailure::Session(error))))) => {
            return Err(TraversalTerminalFailure::Session(error));
        }
        Some(Err(_)) | None => return Err(TraversalTerminalFailure::Internal),
    }
    Ok(())
}

async fn flush(
    sender: &mpsc::Sender<TraversalItem>,
    state: &mut State,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<(), TraversalTerminalFailure> {
    while let Some(item) = state.pending.remove(&state.next_output) {
        match &item {
            TraversalItem::Entry(_) => state.observed += 1,
            TraversalItem::EntryFailure(_) => state.failed += 1,
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            result = sender.send(item) => {
                result.map_err(|_| TraversalTerminalFailure::Internal)?;
            }
        }
        state.next_output += 1;
    }
    Ok(())
}

fn queue_failure(
    state: &mut State,
    error: EntryOperationFailure,
) -> Result<(), TraversalTerminalFailure> {
    let sequence = state.next_sequence;
    state.next_sequence = state
        .next_sequence
        .checked_add(1)
        .ok_or(TraversalTerminalFailure::Internal)?;
    state
        .pending
        .insert(sequence, TraversalItem::EntryFailure(error));
    Ok(())
}

fn entry_failure(path: &StoragePath, class: FailureClass) -> EntryOperationFailure {
    EntryOperationFailure::new(
        path.clone(),
        Operation::Traverse,
        class,
        Transience::Permanent,
        "storage traversal entry failed",
    )
    .unwrap_or_else(|_| unreachable!("static diagnostic is valid"))
}

#[cfg(test)]
mod tests {
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
                NamespaceRequest::List(root) if root == path("dir") => Ok(
                    NamespaceResult::Entries(vec![descriptor("dir/child", EntryKind::File)]),
                ),
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
                StorageRoleFailure::Entry(entry_failure(
                    &StoragePath::root(),
                    FailureClass::Protocol,
                ))
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

    fn request(cancel: tokio_util::sync::CancellationToken) -> TraversalRequest {
        TraversalRequest {
            root: StoragePath::root(),
            order: crate::traversal::TraversalOrder::Admission,
            max_inflight_operations: NonZeroUsize::new(2)
                .unwrap_or_else(|| unreachable!("constant is nonzero")),
            max_buffered_items: NonZeroUsize::new(1)
                .unwrap_or_else(|| unreachable!("constant is nonzero")),
            observation_plan: ObservationPlan::default(),
            cancel,
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
}
