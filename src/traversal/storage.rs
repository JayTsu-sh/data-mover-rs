use std::collections::VecDeque;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinError, JoinSet};

use super::{
    ChildOrder, TraversalCandidate, TraversalCompletion, TraversalDecision, TraversalFilter,
    TraversalItem, TraversalOrder, TraversalOutcome, TraversalRequest, TraversalSession,
    TraversalSource, TraversalTerminalFailure, TraversalVersions, relative_to,
};
use crate::model::{
    BackendSessionFailure, EntryKind, EntryOperationFailure, FailureClass, ObservationMode,
    ObservationPlan, ObservedEntry, Operation, StoragePath, Transience,
};
use crate::storage::{
    CapabilityUnavailable, Metadata, Namespace, NamespaceResult, PreflightPolicy, SourceDescriptor,
    Storage, StorageRoleFailure,
};

mod cursor;
mod observe;
mod output;
#[cfg(test)]
mod residency;

use cursor::{Cursor, Step};
use observe::observe;
use output::{BlockFacts, ChildDescent, Output, Settled};
#[cfg(test)]
use residency::ResidencyPeak;

/// The driver's handle on the residency probe.
///
/// A field rather than a `#[cfg(test)]` parameter so the one call site stays single: outside
/// tests this is a zero-sized type, cloning it is free, and [`Cursor::residency`] does not
/// exist at all, so nothing walks the stack in a release build.
#[derive(Clone, Default)]
struct Probe {
    #[cfg(test)]
    peak: Arc<ResidencyPeak>,
}

/// Protocol-neutral traversal assembled from one connected storage's namespace and metadata roles.
pub struct StorageTraversalSource {
    namespace: Arc<dyn Namespace>,
    metadata: Arc<dyn Metadata>,
    probe: Probe,
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
            probe: Probe::default(),
        })
    }

    #[cfg(test)]
    fn with_roles(namespace: Arc<dyn Namespace>, metadata: Arc<dyn Metadata>) -> Self {
        Self {
            namespace,
            metadata,
            probe: Probe::default(),
        }
    }

    /// Same as [`Self::with_roles`], plus the handle the driver records residency into.
    #[cfg(test)]
    fn with_roles_probed(
        namespace: Arc<dyn Namespace>,
        metadata: Arc<dyn Metadata>,
    ) -> (Self, Arc<ResidencyPeak>) {
        let source = Self::with_roles(namespace, metadata);
        let peak = Arc::clone(&source.probe.peak);
        (source, peak)
    }
}

impl StorageTraversalSource {
    /// Whether this source can list every stored version ([`TraversalVersions::All`]).
    #[must_use]
    pub fn supports_versions(&self) -> bool {
        self.namespace.supports_versions()
    }
}

/// A session that ends at once, before any I/O, with an `Unsupported` session failure: the
/// request asked for every version of a source that keeps none. Returning its current entries
/// instead would let a history migration believe it saw every version.
fn refuse_versions(request: &TraversalRequest) -> TraversalSession {
    let (producer, session) =
        TraversalSession::bounded(request.max_buffered_items, request.cancel.clone());
    let failure = match BackendSessionFailure::new(
        Operation::Traverse,
        FailureClass::Unsupported,
        Transience::Permanent,
        "this source keeps no versions to traverse",
    ) {
        Ok(failure) => TraversalTerminalFailure::Session(failure),
        Err(_) => TraversalTerminalFailure::Internal,
    };
    producer.finish(Err(failure));
    session
}

impl TraversalSource for StorageTraversalSource {
    fn traverse(&self, request: TraversalRequest) -> TraversalSession {
        if request.versions == TraversalVersions::All && !self.supports_versions() {
            return refuse_versions(&request);
        }
        let (item_tx, item_rx) = mpsc::channel(request.max_buffered_items.get());
        let (completion_tx, completion_rx) = oneshot::channel();
        let cancel = request.cancel.clone();
        tokio::spawn(run(
            Arc::clone(&self.namespace),
            Arc::clone(&self.metadata),
            request,
            item_tx,
            completion_tx,
            self.probe.clone(),
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
/// number, assigned in output order by the cursor. Allocation lives here; releasing the slots
/// in that order is [`Output`]'s job.
struct State {
    next_sequence: u64,
    output: Output,
}

impl State {
    const fn new() -> Self {
        Self {
            next_sequence: 0,
            output: Output::new(),
        }
    }

    /// Entries admitted but not yet emitted: in flight, or settled and waiting in the reorder
    /// buffer behind an earlier one. This, not the in-flight count alone, is what the admission
    /// window bounds; otherwise one slow observation lets the reorder buffer grow without limit.
    fn admitted(&self) -> usize {
        // Saturating, not raw: were the invariant that output never runs ahead of allocation
        // ever to break, a wrapped `u64` would read as `usize::MAX`, hold the window shut for
        // good, and spin `drive` through its `WindowFull` arm, which has no await point.
        usize::try_from(self.next_sequence.saturating_sub(self.output.next())).unwrap_or(usize::MAX)
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
    probe: Probe,
) {
    let mut state = State::new();
    let mut cursor = Cursor::new(
        DirectoryWork {
            path: request.root.clone(),
            child_depth: 1,
            filter_children: request.filter.is_some(),
        },
        match request.order {
            TraversalOrder::Admission => ChildOrder::Listing,
            TraversalOrder::NameBytes => ChildOrder::NameBytes,
        },
    );
    let mut tasks = JoinSet::new();
    let mut listings = JoinSet::new();
    let runtime = Runtime {
        namespace: &namespace,
        metadata: &metadata,
        request: &request,
        policy: Policy::new(&request),
        items: &items,
    };
    let result = drive(
        &runtime,
        &mut cursor,
        &mut listings,
        &mut tasks,
        &mut state,
        &probe,
    )
    .await;
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
                observed_entries: state.output.observed(),
                entry_failures: state.output.failed(),
                directories_listed: state.output.listed(),
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
    probe: &Probe,
) -> Result<(), TraversalTerminalFailure> {
    // The probe is a zero-sized type outside tests, where nothing samples it.
    #[cfg(not(test))]
    let _ = probe;
    loop {
        if runtime.request.cancel.is_cancelled() {
            return Ok(());
        }
        let step = cursor.advance(runtime, tasks, state)?;
        cursor.start_listings(runtime, listings);
        // After `start_listings`, so the listings it just began count towards the peak; before
        // `flush`, so the sample is taken while the block is still held rather than after the
        // reorder buffer has drained it.
        #[cfg(test)]
        probe.peak.record(cursor.residency());
        state.output.flush(runtime).await?;
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
            cursor.listed(runtime, &path, result)
        }
        Some(joined) = tasks.join_next(), if !tasks.is_empty() => {
            settle(runtime, cursor, state, joined)
        }
        // The cursor only waits on work it started, so this means that invariant broke.
        else => Err(TraversalTerminalFailure::Internal),
    }
}

/// The admission decision for one listed child, when it can be made from the listing alone.
///
/// `None` means the filter needs `modified` and still applies below `parent`, so the decision
/// waits for the child's observation. Made once per child, when the listing arrives, so that a
/// prefetched directory's subdirectories are known before the cursor reaches it.
fn immediate_decision(
    runtime: &Runtime<'_>,
    parent: &DirectoryWork,
    descriptor: &SourceDescriptor,
) -> Option<TraversalDecision> {
    if runtime.policy.deferred && parent.filter_children {
        return None;
    }
    let candidate = TraversalCandidate {
        path: relative_to(&runtime.request.root, &descriptor.path),
        name: entry_name(&descriptor.path),
        kind: descriptor.kind,
        size: descriptor.size,
        modified: descriptor
            .inline_timestamps
            .and_then(|value| value.modified),
    };
    Some(runtime.policy.decide(parent.filter_children, &candidate))
}

/// Admits one listed child: spawns its observation, or drops it. `decision` is the child's
/// [`immediate_decision`].
///
/// Where an immediate decision descends to was settled when the listing arrived, so this only
/// tallies what an output slot cannot show: a child the filter hides never gets one, and the
/// output side would never see it. A deferred decision still needs a slot here, because the slot
/// is keyed by the sequence this function allocates.
fn admit(
    runtime: &Runtime<'_>,
    parent: &DirectoryWork,
    child: cursor::Child,
    cursor_slots: &mut VecDeque<cursor::Slot>,
    facts: &mut BlockFacts,
    tasks: &mut ObservationTask,
    state: &mut State,
) -> Result<(), TraversalTerminalFailure> {
    let cursor::Child {
        descriptor,
        decision,
    } = child;
    let depth = parent.child_depth;
    if let Some(decision) = decision
        && !decision.emit
    {
        facts.hidden = facts.hidden.saturating_add(1);
        return Ok(());
    }
    let sequence = state.allocate()?;
    let context = decision.is_none().then_some(Deferred {
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

/// Where one child's subtree goes, given a decision already made.
enum DescendOutcome {
    Into(DirectoryWork),
    Pruned,
    Truncated,
    NotDirectory,
}

/// `max_depth` is weighed before the filter: a directory the request never reaches is truncated
/// whatever a filter would have said about descending into it. [`deferred_descent`] reads the
/// same order, so the two paths agree.
fn descend_outcome(
    request: &TraversalRequest,
    descriptor: &SourceDescriptor,
    depth: usize,
    decision: TraversalDecision,
) -> DescendOutcome {
    if descriptor.kind != EntryKind::Directory {
        return DescendOutcome::NotDirectory;
    }
    if !request.admits_depth(depth) {
        return DescendOutcome::Truncated;
    }
    if !decision.descend {
        return DescendOutcome::Pruned;
    }
    DescendOutcome::Into(DirectoryWork {
        path: descriptor.path.clone(),
        child_depth: depth.saturating_add(1),
        filter_children: decision.filter_children,
    })
}

/// Where a deferred child's subtree goes, now that its observation has settled. `descending` is
/// whether the settled decision produced work.
fn deferred_descent(
    request: &TraversalRequest,
    deferred: Deferred,
    descending: bool,
) -> Option<ChildDescent> {
    if deferred.kind != EntryKind::Directory {
        return None;
    }
    if !request.admits_depth(deferred.depth) {
        return Some(ChildDescent::Truncated);
    }
    Some(if descending {
        ChildDescent::Listed
    } else {
        ChildDescent::Pruned
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
    let (item, below) = match result {
        Ok(entry) => settle_entry(runtime, entry, deferred),
        Err(StorageRoleFailure::Entry(error)) => settle_failure(runtime, error, deferred),
        Err(StorageRoleFailure::Session(error)) => {
            return Err(TraversalTerminalFailure::Session(error));
        }
    };
    let descent =
        deferred.and_then(|deferred| deferred_descent(runtime.request, deferred, below.is_some()));
    if deferred.is_some_and(|deferred| awaits_slot(runtime.request, deferred)) {
        cursor.decide(sequence, below);
    }
    state
        .output
        .settle(sequence, Settled::Child { item, descent });
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

fn queue_failure(
    state: &mut State,
    error: EntryOperationFailure,
) -> Result<(), TraversalTerminalFailure> {
    let sequence = state.allocate()?;
    state.output.settle(
        sequence,
        Settled::Child {
            item: Some(TraversalItem::EntryFailure(error)),
            descent: None,
        },
    );
    Ok(())
}

/// Queues the marker that closes one directory's block.
fn queue_block_end(
    state: &mut State,
    path: StoragePath,
    facts: BlockFacts,
) -> Result<(), TraversalTerminalFailure> {
    let sequence = state.allocate()?;
    state
        .output
        .settle(sequence, Settled::BlockEnd { path, facts });
    Ok(())
}

/// Queues the marker that closes one directory's whole subtree.
fn queue_subtree_end(state: &mut State, path: StoragePath) -> Result<(), TraversalTerminalFailure> {
    let sequence = state.allocate()?;
    state.output.settle(sequence, Settled::SubtreeEnd { path });
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
