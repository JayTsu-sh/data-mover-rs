use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinError, JoinSet};

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
    CapabilityUnavailable, Metadata, Namespace, NamespaceResult, PreflightPolicy, SourceDescriptor,
    Storage, StorageRoleFailure,
};

mod cursor;
mod observe;

use cursor::{Cursor, Step};
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

/// One directory to list, with the filter state its children inherit.
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

type ListingTask = JoinSet<(StoragePath, Result<NamespaceResult, StorageRoleFailure>)>;

/// Output sequencing: every admitted entry and every queued listing failure owns one sequence
/// number, assigned in output order by the cursor; the reorder buffer releases them strictly in
/// that order. An entry a deferred filter then drops keeps its number and settles as `None`.
struct State {
    next_sequence: u64,
    next_output: u64,
    observed: u64,
    failed: u64,
    /// Settled sequence slots waiting for their turn; `None` settles a slot with no item.
    pending: BTreeMap<u64, Option<TraversalItem>>,
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
    let mut cursor = Cursor::new(DirectoryWork {
        path: request.root.clone(),
        child_depth: 1,
        filter_children: request.filter.is_some(),
    });
    let mut tasks = JoinSet::new();
    let mut listings = JoinSet::new();
    let runtime = Runtime {
        namespace: &namespace,
        metadata: &metadata,
        request: &request,
        policy: Policy::new(&request),
        items: &items,
    };
    let result = drive(&runtime, &mut cursor, &mut listings, &mut tasks, &mut state).await;
    // Never abort a backend operation: it may hold an open handle between two awaits (a CIFS
    // listing holds its directory handle, a CIFS observation opens, reads attributes, then
    // closes), and dropping the future there leaks it. Detached operations run to completion in
    // the background and close what they opened; their results are simply discarded, and both
    // sets are bounded by the request's budgets.
    tasks.detach_all();
    listings.detach_all();
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

/// Advances the cursor as far as it can, emits what is ready, then waits for whatever the
/// cursor is blocked on.
async fn drive(
    runtime: &Runtime<'_>,
    cursor: &mut Cursor,
    listings: &mut ListingTask,
    tasks: &mut ObservationTask,
    state: &mut State,
) -> Result<(), TraversalTerminalFailure> {
    loop {
        if runtime.request.cancel.is_cancelled() {
            return Ok(());
        }
        let step = cursor.advance(runtime, tasks, state)?;
        cursor.start_listings(runtime, listings);
        flush(runtime, state).await?;
        match step {
            Step::Done if tasks.is_empty() => return Ok(()),
            // Nothing in flight means every admitted entry has settled, so the flush above
            // emitted them all and the window is open again.
            Step::WindowFull if tasks.is_empty() => {}
            _ => wait(runtime, cursor, listings, tasks, state).await?,
        }
    }
}

/// Waits for one listing or one observation to finish, or for cancellation.
async fn wait(
    runtime: &Runtime<'_>,
    cursor: &mut Cursor,
    listings: &mut ListingTask,
    tasks: &mut ObservationTask,
    state: &mut State,
) -> Result<(), TraversalTerminalFailure> {
    tokio::select! {
        biased;
        () = runtime.request.cancel.cancelled() => Ok(()),
        Some(joined) = listings.join_next(), if !listings.is_empty() => {
            let (path, result) = joined.map_err(|_| TraversalTerminalFailure::Internal)?;
            cursor.listed(path, result)
        }
        Some(joined) = tasks.join_next(), if !tasks.is_empty() => {
            settle(runtime, cursor, state, joined)
        }
        // The cursor only waits on work it started, so this means that invariant broke.
        else => Err(TraversalTerminalFailure::Internal),
    }
}

/// Applies the admission policy to one listed child: spawns its observation, records where to
/// descend next, or drops it.
fn admit(
    runtime: &Runtime<'_>,
    parent: &DirectoryWork,
    descriptor: SourceDescriptor,
    cursor_slots: &mut VecDeque<cursor::Slot>,
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
            cursor_slots.push_back(cursor::Slot::Descend(work));
        }
        if !decision.emit {
            return Ok(());
        }
    }
    let sequence = state.allocate()?;
    let context = deferred.then_some(Deferred {
        depth,
        kind: descriptor.kind,
    });
    if context.is_some_and(|context| awaits_slot(runtime.request, context)) {
        cursor_slots.push_back(cursor::Slot::Pending(sequence));
    }
    let namespace = Arc::clone(runtime.namespace);
    let metadata = Arc::clone(runtime.metadata);
    let plan = runtime.policy.observation_plan;
    tasks.spawn(async move {
        let result = observe(namespace, metadata, descriptor, plan).await;
        (sequence, result, context)
    });
    Ok(())
}

/// Whether a deferred child can be descended into at all, so its decision fills a slot.
fn awaits_slot(request: &TraversalRequest, deferred: Deferred) -> bool {
    deferred.kind == EntryKind::Directory && request.admits_depth(deferred.depth)
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

type Joined = Result<
    (
        u64,
        Result<ObservedEntry, StorageRoleFailure>,
        Option<Deferred>,
    ),
    JoinError,
>;

fn settle(
    runtime: &Runtime<'_>,
    cursor: &mut Cursor,
    state: &mut State,
    joined: Joined,
) -> Result<(), TraversalTerminalFailure> {
    let (sequence, result, deferred) = joined.map_err(|_| TraversalTerminalFailure::Internal)?;
    let (item, descend) = match result {
        Ok(entry) => settle_entry(runtime, entry, deferred),
        Err(StorageRoleFailure::Entry(error)) => settle_failure(runtime, error, deferred),
        Err(StorageRoleFailure::Session(error)) => {
            return Err(TraversalTerminalFailure::Session(error));
        }
    };
    if deferred.is_some_and(|deferred| awaits_slot(runtime.request, deferred)) {
        cursor.decide(sequence, descend);
    }
    state.pending.insert(sequence, item);
    Ok(())
}

/// A deferred decision whose observation failed cannot be evaluated. Reporting the failure and
/// pruning would silently drop the whole subtree, so a directory is still listed; the caller sees
/// the failure item and keeps a complete enumeration below it.
fn settle_failure(
    runtime: &Runtime<'_>,
    error: EntryOperationFailure,
    deferred: Option<Deferred>,
) -> (Option<TraversalItem>, Option<DirectoryWork>) {
    let descend = deferred
        .filter(|deferred| awaits_slot(runtime.request, *deferred))
        .map(|deferred| DirectoryWork {
            path: error.path().clone(),
            child_depth: deferred.depth.saturating_add(1),
            filter_children: true,
        });
    (Some(TraversalItem::EntryFailure(error)), descend)
}

/// Applies a deferred decision now that the observation (and therefore `modified`) is known.
fn settle_entry(
    runtime: &Runtime<'_>,
    entry: ObservedEntry,
    deferred: Option<Deferred>,
) -> (Option<TraversalItem>, Option<DirectoryWork>) {
    let Some(deferred) = deferred else {
        return (Some(TraversalItem::Entry(Box::new(entry))), None);
    };
    let candidate = TraversalCandidate {
        path: relative_to(&runtime.request.root, entry.path()),
        name: entry_name(entry.path()),
        kind: entry.kind(),
        size: entry.size(),
        modified: entry.modified(),
    };
    let decision = runtime.policy.decide(true, &candidate);
    let descend =
        (decision.descend && awaits_slot(runtime.request, deferred)).then(|| DirectoryWork {
            path: entry.path().clone(),
            child_depth: deferred.depth.saturating_add(1),
            filter_children: decision.filter_children,
        });
    (
        decision.emit.then(|| TraversalItem::Entry(Box::new(entry))),
        descend,
    )
}

/// Emits every settled item that is next in sequence.
async fn flush(runtime: &Runtime<'_>, state: &mut State) -> Result<(), TraversalTerminalFailure> {
    while let Some(settled) = state.pending.remove(&state.next_output) {
        if let Some(item) = settled {
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
    state
        .pending
        .insert(sequence, Some(TraversalItem::EntryFailure(error)));
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
