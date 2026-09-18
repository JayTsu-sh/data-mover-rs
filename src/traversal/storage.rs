use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use super::{
    TraversalCandidate, TraversalCompletion, TraversalDecision, TraversalFilter, TraversalItem,
    TraversalOutcome, TraversalRequest, TraversalSession, TraversalSource,
    TraversalTerminalFailure, relative_to,
};
use crate::model::{
    EntryKind, EntryOperationFailure, FailureClass, ObservationMode, ObservationPlan,
    ObservedEntry, Operation, StoragePath, Transience,
};
use crate::storage::{
    CapabilityUnavailable, Metadata, Namespace, NamespaceRequest, NamespaceResult, PreflightPolicy,
    SourceDescriptor, Storage, StorageRoleFailure,
};

mod observe;

use observe::observe;

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

/// One directory waiting to be listed.
#[derive(Clone, Debug)]
struct DirectoryWork {
    path: StoragePath,
    /// Depth of the directory's children; the traversal root's children are 1.
    child_depth: usize,
    /// Whether the filter is still consulted for children (legacy `check_children`).
    filter_children: bool,
}

/// Facts a deferred decision needs once the observation has settled. Deferral only happens
/// for children whose parent still consults the filter.
#[derive(Clone, Copy, Debug)]
struct Deferred {
    depth: usize,
    /// Carried so a failed observation of a directory can still be descended rather than
    /// silently dropping its whole subtree.
    kind: EntryKind,
}

type ObservationTask = JoinSet<(
    u64,
    Result<ObservedEntry, StorageRoleFailure>,
    Option<Deferred>,
)>;

/// A settled sequence slot: an optional item to deliver and an optional directory to list.
struct Settled {
    item: Option<TraversalItem>,
    descend: Option<DirectoryWork>,
}

struct State {
    next_sequence: u64,
    next_output: u64,
    observed: u64,
    failed: u64,
    pending: BTreeMap<u64, Settled>,
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

    /// Entries admitted but not yet emitted: in flight, or settled and waiting in the reorder
    /// buffer behind an earlier one. This, not the in-flight count alone, is what the admission
    /// window bounds; otherwise one slow observation lets the reorder buffer grow without limit.
    fn admitted(&self) -> usize {
        usize::try_from(self.next_sequence - self.next_output).unwrap_or(usize::MAX)
    }

    fn allocate(&mut self) -> Result<u64, TraversalTerminalFailure> {
        let sequence = self.next_sequence;
        self.next_sequence = sequence
            .checked_add(1)
            .ok_or(TraversalTerminalFailure::Internal)?;
        Ok(sequence)
    }
}

/// Per-traversal admission policy derived once from the request.
struct Policy {
    filter: Option<Arc<dyn TraversalFilter>>,
    /// Decisions wait for the metadata observation because the filter needs `modified`.
    deferred: bool,
    observation_plan: ObservationPlan,
}

impl Policy {
    fn new(request: &TraversalRequest) -> Self {
        let deferred = request
            .filter
            .as_ref()
            .is_some_and(|filter| filter.needs_modified());
        let mut observation_plan = request.observation_plan;
        if deferred && observation_plan.timestamps() == ObservationMode::Omit {
            observation_plan = observation_plan.with_timestamps(ObservationMode::InlineOnly);
        }
        Self {
            filter: request.filter.clone(),
            deferred,
            observation_plan,
        }
    }

    fn decide(
        &self,
        consult_filter: bool,
        candidate: &TraversalCandidate<'_>,
    ) -> TraversalDecision {
        match (&self.filter, consult_filter) {
            (Some(filter), true) => filter.decide(candidate),
            _ => TraversalDecision::unfiltered(candidate.kind),
        }
    }
}

struct Runtime<'a> {
    namespace: &'a Arc<dyn Namespace>,
    metadata: &'a Arc<dyn Metadata>,
    request: &'a TraversalRequest,
    policy: Policy,
    items: &'a mpsc::Sender<TraversalItem>,
}

async fn run(
    namespace: Arc<dyn Namespace>,
    metadata: Arc<dyn Metadata>,
    request: TraversalRequest,
    items: mpsc::Sender<TraversalItem>,
    completion: oneshot::Sender<Result<TraversalOutcome, TraversalTerminalFailure>>,
) {
    let mut state = State::new();
    let mut directories = VecDeque::from([DirectoryWork {
        path: request.root.clone(),
        child_depth: 1,
        filter_children: request.filter.is_some(),
    }]);
    let mut tasks = JoinSet::new();
    let runtime = Runtime {
        namespace: &namespace,
        metadata: &metadata,
        request: &request,
        policy: Policy::new(&request),
        items: &items,
    };
    let result = enumerate_and_observe(&runtime, &mut directories, &mut tasks, &mut state).await;
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
    runtime: &Runtime<'_>,
    directories: &mut VecDeque<DirectoryWork>,
    tasks: &mut ObservationTask,
    state: &mut State,
) -> Result<(), TraversalTerminalFailure> {
    loop {
        if runtime.request.cancel.is_cancelled() {
            return Ok(());
        }
        if let Some(directory) = directories.pop_front() {
            list_directory(runtime, directory, directories, tasks, state).await?;
        } else if tasks.is_empty() {
            return Ok(());
        } else {
            settle(runtime, tasks, state).await?;
            flush(runtime, state, directories).await?;
        }
    }
}

async fn list_directory(
    runtime: &Runtime<'_>,
    directory: DirectoryWork,
    directories: &mut VecDeque<DirectoryWork>,
    tasks: &mut ObservationTask,
    state: &mut State,
) -> Result<(), TraversalTerminalFailure> {
    let Some(descriptors) = listed_children(runtime, &directory.path, state).await? else {
        return flush(runtime, state, directories).await;
    };
    for descriptor in descriptors {
        while state.admitted() >= runtime.request.max_inflight_operations.get() {
            if runtime.request.cancel.is_cancelled() {
                return Ok(());
            }
            // With nothing in flight, every admitted entry has settled, so the flush below can
            // always emit the next one and shrink the window.
            if !tasks.is_empty() {
                settle(runtime, tasks, state).await?;
            }
            flush(runtime, state, directories).await?;
        }
        admit(runtime, &directory, descriptor, directories, tasks, state)?;
    }
    flush(runtime, state, directories).await
}

/// Lists one directory, queueing every entry-scoped failure it reports.
///
/// Returns `None` when the directory itself could not be listed, and the children that could
/// be described otherwise; a child the backend could not describe becomes its own failure item
/// without hiding its siblings.
async fn listed_children(
    runtime: &Runtime<'_>,
    directory: &StoragePath,
    state: &mut State,
) -> Result<Option<Vec<SourceDescriptor>>, TraversalTerminalFailure> {
    let listed = runtime
        .namespace
        .execute(NamespaceRequest::List(directory.clone()))
        .await;
    match listed.map(NamespaceResult::into_listing) {
        Ok(Some((entries, failures))) => {
            for failure in failures {
                queue_failure(state, failure)?;
            }
            Ok(Some(entries))
        }
        Ok(None) => {
            queue_failure(state, entry_failure(directory, FailureClass::Protocol))?;
            Ok(None)
        }
        Err(StorageRoleFailure::Entry(error)) => {
            queue_failure(state, error)?;
            Ok(None)
        }
        Err(StorageRoleFailure::Session(error)) => Err(TraversalTerminalFailure::Session(error)),
    }
}

/// Applies the admission policy to one listed child and either spawns its observation,
/// enqueues it for listing without emitting it, or drops it.
fn admit(
    runtime: &Runtime<'_>,
    parent: &DirectoryWork,
    descriptor: SourceDescriptor,
    directories: &mut VecDeque<DirectoryWork>,
    tasks: &mut ObservationTask,
    state: &mut State,
) -> Result<(), TraversalTerminalFailure> {
    let depth = parent.child_depth;
    let deferred = runtime.policy.deferred && parent.filter_children;
    if !deferred {
        let candidate = TraversalCandidate {
            path: relative_to(&runtime.request.root, &descriptor.path),
            name: entry_name(&descriptor.path),
            kind: descriptor.kind,
            size: descriptor.size,
            modified: descriptor
                .inline_timestamps
                .and_then(|value| value.modified),
        };
        let decision = runtime.policy.decide(parent.filter_children, &candidate);
        if let Some(work) = descend_work(runtime.request, &descriptor, depth, decision) {
            directories.push_back(work);
        }
        if !decision.emit {
            return Ok(());
        }
    }
    let sequence = state.allocate()?;
    let namespace = Arc::clone(runtime.namespace);
    let metadata = Arc::clone(runtime.metadata);
    let plan = runtime.policy.observation_plan;
    let context = deferred.then_some(Deferred {
        depth,
        kind: descriptor.kind,
    });
    tasks.spawn(async move {
        let result = observe(namespace, metadata, descriptor, plan).await;
        (sequence, result, context)
    });
    Ok(())
}

fn descend_work(
    request: &TraversalRequest,
    descriptor: &SourceDescriptor,
    depth: usize,
    decision: TraversalDecision,
) -> Option<DirectoryWork> {
    (decision.descend && descriptor.kind == EntryKind::Directory && request.admits_depth(depth))
        .then(|| DirectoryWork {
            path: descriptor.path.clone(),
            child_depth: depth.saturating_add(1),
            filter_children: decision.filter_children,
        })
}

fn entry_name(path: &StoragePath) -> &str {
    path.as_str().rsplit('/').next().unwrap_or_default()
}

async fn settle(
    runtime: &Runtime<'_>,
    tasks: &mut ObservationTask,
    state: &mut State,
) -> Result<(), TraversalTerminalFailure> {
    let result = tokio::select! {
        biased;
        () = runtime.request.cancel.cancelled() => return Ok(()),
        result = tasks.join_next() => result,
    };
    let settled = match result {
        Some(Ok((sequence, Ok(entry), deferred))) => {
            (sequence, settle_entry(runtime, entry, deferred))
        }
        Some(Ok((sequence, Err(StorageRoleFailure::Entry(error)), deferred))) => {
            (sequence, settle_failure(runtime, error, deferred))
        }
        Some(Ok((_, Err(StorageRoleFailure::Session(error)), _))) => {
            return Err(TraversalTerminalFailure::Session(error));
        }
        Some(Err(_)) | None => return Err(TraversalTerminalFailure::Internal),
    };
    state.pending.insert(settled.0, settled.1);
    Ok(())
}

/// A deferred decision whose observation failed cannot be evaluated. Reporting the failure and
/// pruning would silently drop the whole subtree, so a directory is still listed; the caller sees
/// the failure item and keeps a complete enumeration below it.
fn settle_failure(
    runtime: &Runtime<'_>,
    error: EntryOperationFailure,
    deferred: Option<Deferred>,
) -> Settled {
    let descend = deferred
        .filter(|deferred| {
            deferred.kind == EntryKind::Directory && runtime.request.admits_depth(deferred.depth)
        })
        .map(|deferred| DirectoryWork {
            path: error.path().clone(),
            child_depth: deferred.depth.saturating_add(1),
            filter_children: true,
        });
    Settled {
        item: Some(TraversalItem::EntryFailure(error)),
        descend,
    }
}

/// Applies a deferred decision now that the observation (and therefore `modified`) is known.
fn settle_entry(
    runtime: &Runtime<'_>,
    entry: ObservedEntry,
    deferred: Option<Deferred>,
) -> Settled {
    let Some(deferred) = deferred else {
        return Settled {
            item: Some(TraversalItem::Entry(Box::new(entry))),
            descend: None,
        };
    };
    let candidate = TraversalCandidate {
        path: relative_to(&runtime.request.root, entry.path()),
        name: entry_name(entry.path()),
        kind: entry.kind(),
        size: entry.size(),
        modified: entry.modified(),
    };
    let decision = runtime.policy.decide(true, &candidate);
    let descend = (decision.descend
        && entry.kind() == EntryKind::Directory
        && runtime.request.admits_depth(deferred.depth))
    .then(|| DirectoryWork {
        path: entry.path().clone(),
        child_depth: deferred.depth.saturating_add(1),
        filter_children: decision.filter_children,
    });
    Settled {
        item: decision.emit.then(|| TraversalItem::Entry(Box::new(entry))),
        descend,
    }
}

async fn flush(
    runtime: &Runtime<'_>,
    state: &mut State,
    directories: &mut VecDeque<DirectoryWork>,
) -> Result<(), TraversalTerminalFailure> {
    while let Some(settled) = state.pending.remove(&state.next_output) {
        if let Some(work) = settled.descend {
            directories.push_back(work);
        }
        if let Some(item) = settled.item {
            match &item {
                TraversalItem::Entry(_) => state.observed += 1,
                TraversalItem::EntryFailure(_) => state.failed += 1,
            }
            tokio::select! {
                biased;
                () = runtime.request.cancel.cancelled() => return Ok(()),
                result = runtime.items.send(item) => {
                    result.map_err(|_| TraversalTerminalFailure::Internal)?;
                }
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
    let sequence = state.allocate()?;
    state.pending.insert(
        sequence,
        Settled {
            item: Some(TraversalItem::EntryFailure(error)),
            descend: None,
        },
    );
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
mod tests;
