use std::sync::Arc;
use std::sync::atomic::AtomicU64;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use super::{
    ScanFailure, TraversalCandidate, TraversalCompletion, TraversalDecision, TraversalFilter,
    TraversalItem, TraversalOutcome, TraversalRequest, TraversalSession, TraversalSource,
    TraversalTerminalFailure, TraversalVersions, relative_to,
};
use crate::model::{
    BackendSessionFailure, EntryKind, EntryOperationFailure, FailureClass, ObservationMode,
    ObservationPlan, ObservedEntry, Operation, StoragePath, Transience,
};
use crate::storage::{
    CapabilityUnavailable, Metadata, Namespace, PreflightPolicy, SourceDescriptor, Storage,
    StorageRoleFailure,
};

mod cursor;
mod observe;
mod output;
#[cfg(test)]
mod residency;

use cursor::{Arrival, Cursor, Decisions, Step};
use observe::{Feed, Job, Listed, Observations, Observed};
use output::{BlockFacts, ChildDescent, Output, Settled};
#[cfg(test)]
use residency::ResidencyPeak;

/// The driver's handle on the residency probe.
///
/// A field rather than a `#[cfg(test)]` parameter so the one call site stays single: outside
/// tests this is a zero-sized type, cloning it is free, and [`Cursor::residency`] does not
/// exist at all, so nothing walks the stack in a release build. In tests only a source built
/// with [`StorageTraversalSource::with_roles_probed`] samples: a sample walks every held
/// block, which would otherwise dominate any test that times the driver.
#[derive(Clone, Default)]
struct Probe {
    #[cfg(test)]
    peak: Option<Arc<ResidencyPeak>>,
    /// Observation tasks the traversal started, for the tests that count hand-offs.
    #[cfg(test)]
    observers: Option<Arc<ObserverCounts>>,
}

#[cfg(test)]
impl Probe {
    fn record_observers(&self, observations: &Observations) {
        if let Some(observers) = self.observers.as_ref() {
            observers
                .started
                .store(observations.spawned, Ordering::SeqCst);
            observers
                .peak_held
                .store(observations.peak_held, Ordering::SeqCst);
        }
    }
}

/// What a counting source records about its observation tasks when a traversal ends.
#[cfg(test)]
#[derive(Debug, Default)]
struct ObserverCounts {
    /// Tasks started.
    started: AtomicUsize,
    /// The most tasks held at once, finished ones included.
    peak_held: AtomicUsize,
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
        let mut source = Self::with_roles(namespace, metadata);
        let peak = Arc::new(ResidencyPeak::default());
        source.probe.peak = Some(Arc::clone(&peak));
        (source, peak)
    }

    /// Same as [`Self::with_roles`], plus a count of the observation tasks each traversal
    /// started, stored when it ends.
    #[cfg(test)]
    fn with_roles_counting(
        namespace: Arc<dyn Namespace>,
        metadata: Arc<dyn Metadata>,
    ) -> (Self, Arc<ObserverCounts>) {
        let mut source = Self::with_roles(namespace, metadata);
        let observers = Arc::new(ObserverCounts::default());
        source.probe.observers = Some(Arc::clone(&observers));
        (source, observers)
    }
}

impl StorageTraversalSource {
    /// Whether this source can list every stored version ([`TraversalVersions::All`]).
    #[must_use]
    pub fn supports_versions(&self) -> bool {
        self.namespace.capabilities().histories
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
    /// The ticket of the descend slot the decision fills, when the child is a directory the
    /// request may descend into at all ([`awaits_slot`]).
    ticket: Option<u64>,
}

type ListingTask = JoinSet<(StoragePath, Arrival)>;

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

/// What the driver and the tasks it starts share for one traversal.
struct Shared {
    namespace: Arc<dyn Namespace>,
    metadata: Arc<dyn Metadata>,
    request: TraversalRequest,
    policy: Policy,
    /// Keys for deferred descend slots, handed out by the listing tasks that place them.
    tickets: AtomicU64,
}

struct Runtime<'a> {
    shared: &'a Arc<Shared>,
    request: &'a TraversalRequest,
    policy: &'a Policy,
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
    let mut cursor = Cursor::new(DirectoryWork {
        path: request.root.clone(),
        child_depth: 1,
        filter_children: request.filter.is_some(),
    });
    let mut observations = Observations::new();
    let mut listings = JoinSet::new();
    let shared = Arc::new(Shared {
        namespace,
        metadata,
        policy: Policy::new(&request),
        request,
        tickets: AtomicU64::new(0),
    });
    let runtime = Runtime {
        shared: &shared,
        request: &shared.request,
        policy: &shared.policy,
        items: &items,
    };
    let result = drive(
        &runtime,
        &mut cursor,
        &mut listings,
        &mut observations,
        &mut state,
        &probe,
    )
    .await;
    #[cfg(test)]
    probe.record_observers(&observations);
    // Never abort a backend operation: it may hold an open handle between two awaits (a CIFS
    // listing holds its directory handle, a CIFS observation opens, reads attributes, then
    // closes), and dropping the future there leaks it. Detached operations run to completion in
    // the background and close what they opened; their results are simply discarded, and both
    // sets are bounded by the request's budgets. Dropping the cursor drops every block's feed,
    // which is what lets each observation task end once it has drained.
    observations.detach();
    listings.detach_all();
    drop(cursor);
    drop(items);
    let request = &shared.request;
    let terminal = if request.cancel.is_cancelled() {
        Ok(TraversalOutcome::Cancelled)
    } else {
        result
            .map(|()| TraversalOutcome::Completed(TraversalCompletion::new(state.output.totals())))
    };
    let _ = completion.send(terminal);
}

/// Advances the cursor as far as it can, emits what is ready, then waits for whatever the
/// cursor is blocked on.
async fn drive(
    runtime: &Runtime<'_>,
    cursor: &mut Cursor,
    listings: &mut ListingTask,
    observations: &mut Observations,
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
        // Takes every listing that finished meanwhile, not only the one the cursor may be
        // waiting for: a prefetched directory's subdirectories are only known, and prefetched in
        // turn, once its block has been taken from its task.
        while let Some(joined) = listings.try_join_next() {
            let (path, arrival) = joined.map_err(|_| TraversalTerminalFailure::Internal)?;
            cursor.listed(&path, arrival)?;
        }
        // A listing ends `Cancelled` only once the token fired (the request carries it), so
        // checking again here keeps such a listing from being admitted as a failure (R10).
        if runtime.request.cancel.is_cancelled() {
            return Ok(());
        }
        let step = cursor.advance(runtime, observations, state)?;
        cursor.start_listings(runtime, listings);
        // After `start_listings`, so the listings it just began count towards the peak; before
        // `flush`, so the sample is taken while the block is still held rather than after the
        // reorder buffer has drained it.
        #[cfg(test)]
        if let Some(peak) = probe.peak.as_ref() {
            peak.record(cursor.residency());
        }
        state.output.flush(runtime).await?;
        match step {
            Step::Done if observations.is_idle() => return Ok(()),
            // Nothing in flight means every admitted entry has settled, so the flush above
            // emitted them all and the window is open again.
            Step::WindowFull if observations.is_idle() => {}
            _ => wait(runtime, cursor, listings, observations, state).await?,
        }
    }
}

/// Waits for one listing or some observations to finish, or for cancellation. Observations that
/// arrived together are settled together, so a burst costs the driver one pass.
async fn wait(
    runtime: &Runtime<'_>,
    cursor: &mut Cursor,
    listings: &mut ListingTask,
    observations: &mut Observations,
    state: &mut State,
) -> Result<(), TraversalTerminalFailure> {
    tokio::select! {
        biased;
        () = runtime.request.cancel.cancelled() => Ok(()),
        Some(joined) = listings.join_next(), if !listings.is_empty() => {
            let (path, arrival) = joined.map_err(|_| TraversalTerminalFailure::Internal)?;
            cursor.listed(&path, arrival)
        }
        observed = observations.next(), if !observations.is_idle() => {
            settle(runtime, cursor.decisions(), state, observed?)?;
            while let Some(observed) = observations.ready() {
                settle(runtime, cursor.decisions(), state, observed)?;
            }
            Ok(())
        }
        // The cursor only waits on work it started, so this means that invariant broke.
        else => Err(TraversalTerminalFailure::Internal),
    }
}

/// The admission decision for one listed child, when it can be made from the listing alone.
///
/// `None` means the filter needs `modified` and still applies below `parent`, so the decision
/// is made from the child's observation instead: at once in the listing's task when that needed
/// no I/O, otherwise once it settles. Made once per child, when the listing arrives, so that a
/// prefetched directory's subdirectories are known before the cursor reaches it.
fn immediate_decision(
    shared: &Shared,
    parent: &DirectoryWork,
    descriptor: &SourceDescriptor,
) -> Option<TraversalDecision> {
    if shared.policy.deferred && parent.filter_children {
        return None;
    }
    let candidate = TraversalCandidate {
        path: relative_to(&shared.request.root, &descriptor.path),
        name: entry_name(&descriptor.path),
        kind: descriptor.kind,
        size: descriptor.size,
        modified: descriptor
            .inline_timestamps
            .and_then(|value| value.modified),
    };
    Some(shared.policy.decide(parent.filter_children, &candidate))
}

/// Admits one listed child into its output slot: settles it at once when its listing's task
/// already observed it, or hands it to its block's observation task. A child a decision made in
/// the listing's task hides was only counted there, and never gets here; a deferred one already
/// has its descend slot, placed in the block's order when the listing was prepared.
fn admit(
    runtime: &Runtime<'_>,
    feed: &mut Option<Feed>,
    child: cursor::Child,
    decisions: &mut Decisions,
    observations: &mut Observations,
    state: &mut State,
) -> Result<(), TraversalTerminalFailure> {
    let cursor::Child { listed, deferred } = child;
    let sequence = state.allocate()?;
    match listed {
        Listed::Observed(result) => settle(runtime, decisions, state, (sequence, result, deferred)),
        Listed::Unobserved(descriptor) => observations.start(
            runtime.shared,
            feed,
            Job {
                sequence,
                descriptor,
                deferred,
            },
        ),
    }
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
    kind: EntryKind,
    path: &StoragePath,
    depth: usize,
    decision: TraversalDecision,
) -> DescendOutcome {
    if kind != EntryKind::Directory {
        return DescendOutcome::NotDirectory;
    }
    if !request.admits_depth(depth) {
        return DescendOutcome::Truncated;
    }
    if !decision.descend {
        return DescendOutcome::Pruned;
    }
    DescendOutcome::Into(DirectoryWork {
        path: path.clone(),
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

/// Puts one observation into its output slot, and records the descend decision a deferred one
/// makes.
fn settle(
    runtime: &Runtime<'_>,
    decisions: &mut Decisions,
    state: &mut State,
    observed: Observed,
) -> Result<(), TraversalTerminalFailure> {
    let (sequence, result, deferred) = observed;
    let (item, below) = match result {
        Ok(entry) => settle_entry(runtime, entry, deferred),
        Err(StorageRoleFailure::Entry(error)) => settle_failure(error, deferred),
        Err(StorageRoleFailure::Session(error)) => {
            return Err(TraversalTerminalFailure::Session(error));
        }
    };
    let descent =
        deferred.and_then(|deferred| deferred_descent(runtime.request, deferred, below.is_some()));
    if let Some(ticket) = deferred.and_then(|deferred| deferred.ticket) {
        decisions.insert(ticket, below);
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
    error: EntryOperationFailure,
    deferred: Option<Deferred>,
) -> (Option<TraversalItem>, Option<DirectoryWork>) {
    let descend = deferred
        .filter(|deferred| deferred.ticket.is_some())
        .map(|deferred| DirectoryWork {
            path: error.path().clone(),
            child_depth: deferred.depth.saturating_add(1),
            filter_children: true,
        });
    (
        Some(TraversalItem::Failure(Box::new(ScanFailure::child(error)))),
        descend,
    )
}

/// What the filter sees of an observed child: the same candidate whether the decision is made in
/// the listing's task or once a deferred observation settles.
fn entry_candidate<'a>(root: &StoragePath, entry: &'a ObservedEntry) -> TraversalCandidate<'a> {
    TraversalCandidate {
        path: relative_to(root, entry.path()),
        name: entry_name(entry.path()),
        kind: entry.kind(),
        size: entry.size(),
        modified: entry.modified(),
    }
}

/// Applies a deferred decision now that the observation (and therefore `modified`) is known.
fn settle_entry(
    runtime: &Runtime<'_>,
    entry: Box<ObservedEntry>,
    deferred: Option<Deferred>,
) -> (Option<TraversalItem>, Option<DirectoryWork>) {
    let Some(deferred) = deferred else {
        return (Some(TraversalItem::Entry(entry)), None);
    };
    let decision = runtime
        .policy
        .decide(true, &entry_candidate(&runtime.request.root, &entry));
    let descend = (decision.descend && deferred.ticket.is_some()).then(|| DirectoryWork {
        path: entry.path().clone(),
        child_depth: deferred.depth.saturating_add(1),
        filter_children: decision.filter_children,
    });
    (
        decision.emit.then_some(TraversalItem::Entry(entry)),
        descend,
    )
}

/// Queues a failure the listing itself produced: a child it could not describe, or the
/// directory's own listing failure.
fn queue_failure(state: &mut State, failure: ScanFailure) -> Result<(), TraversalTerminalFailure> {
    let sequence = state.allocate()?;
    state.output.settle(
        sequence,
        Settled::Child {
            item: Some(TraversalItem::Failure(Box::new(failure))),
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
