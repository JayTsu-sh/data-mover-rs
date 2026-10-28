use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use tokio::sync::oneshot;

use super::channel;

use super::{
    ScanFailure, TraversalCandidate, TraversalDecision, TraversalFilter, TraversalItem,
    TraversalOrder, TraversalRequest, TraversalSession, TraversalSource, TraversalTerminalFailure,
    relative_to,
};
use crate::model::{
    BackendIdentity, EntryKind, EntryOperationFailure, FailureClass, MAX_HISTORY_RECORDS,
    MetadataFamily, MetadataObservations, MetadataRequirements, ObservationMode, ObservationPlan,
    ObservationSupport, ObservedEntry, Operation, StoragePath, Transience,
};
use crate::storage::{
    CapabilityUnavailable, DescentHint, Metadata, Namespace, SourceDescriptor, Storage,
    StorageRoleFailure,
};

mod blocks;
mod cursor;
mod observe;
mod output;
#[cfg(test)]
mod residency;
mod resume;
mod stream;
mod unordered;

#[cfg(test)]
use observe::Observations;
use output::{ChildDescent, ChildItem};
#[cfg(test)]
use residency::ResidencyPeak;
#[cfg(test)]
pub(crate) use resume::ManualClock;
use resume::{Clock, Resume};

/// Test probes and overrides shared by the traversal driver and its output.
///
/// A field rather than a `#[cfg(test)]` parameter so the one call site stays single: outside
/// tests this is a zero-sized type, cloning it is free, and [`cursor::Cursor::residency`] does not
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
    /// The clock a resumable traversal reads instead of the system's.
    #[cfg(test)]
    clock: Option<Arc<ManualClock>>,
    /// A smaller output segment limit so tests can exercise histories with few records.
    #[cfg(test)]
    history_segment: Option<usize>,
    /// A smaller session output channel for exercising backpressure in tests.
    #[cfg(test)]
    output_capacity: Option<NonZeroUsize>,
}

impl Probe {
    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn output_capacity(&self) -> NonZeroUsize {
        #[cfg(test)]
        if let Some(capacity) = self.output_capacity {
            return capacity;
        }
        super::DEFAULT_MAX_BUFFERED_ITEMS
    }

    /// Production output always uses the fixed limit; only tests can override it.
    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn history_segment(&self) -> usize {
        #[cfg(test)]
        if let Some(records) = self.history_segment {
            return records.max(1);
        }
        MAX_HISTORY_RECORDS
    }

    /// The clock a traversal with checkpoints on reads: the system's, or a test's. A method (the
    /// probe is what tests configure a source with) whose receiver only tests read.
    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn clock(&self) -> Clock {
        #[cfg(test)]
        if let Some(clock) = &self.clock {
            return Clock::Manual(Arc::clone(clock));
        }
        Clock::System(Instant::now())
    }
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
    /// Which storage this is, for the checkpoints a resumable traversal binds to it.
    identity: BackendIdentity,
    probe: Probe,
}

impl StorageTraversalSource {
    /// Lends the roles required for traversal using production capability policy.
    ///
    /// # Errors
    /// Returns the first unavailable role before starting backend I/O.
    pub fn new(storage: &Storage) -> Result<Self, CapabilityUnavailable> {
        Ok(Self {
            namespace: storage.namespace()?,
            metadata: storage.metadata()?,
            identity: storage.identity().clone(),
            probe: Probe::default(),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_roles(namespace: Arc<dyn Namespace>, metadata: Arc<dyn Metadata>) -> Self {
        Self {
            namespace,
            metadata,
            identity: crate::storage::backends::local::test_identity("traversal-test"),
            probe: Probe::default(),
        }
    }

    /// This source, delivering a history longer than `records` records in segments of that many
    /// (P7 Q8) instead of [`MAX_HISTORY_RECORDS`], so tests segment small histories.
    #[cfg(test)]
    pub(crate) const fn history_segment(mut self, records: usize) -> Self {
        self.probe.history_segment = Some(records);
        self
    }

    /// This source, using a smaller output channel to exercise consumer backpressure.
    #[cfg(test)]
    pub(crate) const fn output_capacity(mut self, capacity: NonZeroUsize) -> Self {
        self.probe.output_capacity = Some(capacity);
        self
    }

    /// This source, reading `clock` instead of the system's when checkpoints are on.
    #[cfg(test)]
    pub(crate) fn clocked(mut self, clock: &Arc<ManualClock>) -> Self {
        self.probe.clock = Some(Arc::clone(clock));
        self
    }

    /// This source, as another storage with the same roles: its checkpoints are another's.
    #[cfg(test)]
    pub(crate) fn identified(mut self, identity: BackendIdentity) -> Self {
        self.identity = identity;
        self
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

/// A session that ends at once, before any I/O, with `failure`.
fn refuse(request: &TraversalRequest, failure: TraversalTerminalFailure) -> TraversalSession {
    let (producer, session) =
        TraversalSession::bounded(super::DEFAULT_MAX_BUFFERED_ITEMS, request.cancel.clone());
    producer.finish(Err(failure));
    session
}

impl TraversalSource for StorageTraversalSource {
    fn traverse(&self, request: TraversalRequest) -> TraversalSession {
        let capabilities = self.metadata.observation_capabilities();
        let requirements = match super::configuration::validate(&request, capabilities) {
            Ok(requirements) => requirements,
            Err(error) => return refuse(&request, TraversalTerminalFailure::Configuration(error)),
        };
        let listing_resume_key = self.namespace.listing_resume_key();
        let resume = match Resume::prepare(&request, &self.identity, listing_resume_key, || {
            self.probe.clock()
        }) {
            Ok(resume) => resume,
            Err(refusal) => return refuse(&request, TraversalTerminalFailure::Resume(refusal)),
        };
        let drain_on_cancel = resume.as_ref().is_some_and(Resume::emits);
        let (item_tx, item_rx) = channel::channel(self.probe.output_capacity());
        let (completion_tx, completion_rx) = oneshot::channel();
        let cancel = request.cancel.clone();
        let (namespace, metadata) = (Arc::clone(&self.namespace), Arc::clone(&self.metadata));
        let probe = self.probe.clone();
        let order = request.order;
        let shared = Arc::new(Shared::new(
            namespace,
            metadata,
            request,
            requirements,
            resume,
            probe.clone(),
        ));
        match order {
            TraversalOrder::Unordered => {
                tokio::spawn(unordered::run(shared, item_tx, completion_tx, probe));
            }
            TraversalOrder::Blocks => {
                tokio::spawn(blocks::run(shared, item_tx, completion_tx, probe));
            }
        }
        TraversalSession::new(item_rx, completion_rx, cancel, drain_on_cancel)
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
    /// What the listing that returned this directory handed back with it, passed on to the
    /// directory's own listing (P7 §12.7). `None` for the root.
    hint: Option<DescentHint>,
}

/// The traversal root's work: the root's children are depth 1, and the filter applies to them
/// when there is one.
fn root_work(request: &TraversalRequest) -> DirectoryWork {
    DirectoryWork {
        path: request.root.clone(),
        child_depth: 1,
        filter_children: request.filter.is_some(),
        hint: None,
    }
}

impl DirectoryWork {
    /// The same work carrying `hint`, unless it already carries one.
    fn with_hint(mut self, hint: Option<DescentHint>) -> Self {
        if self.hint.is_none() {
            self.hint = hint;
        }
        self
    }
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

/// Per-traversal admission policy derived once from the request.
struct Policy {
    filter: Option<Arc<dyn TraversalFilter>>,
    /// Families that must settle before a filtered child's decision.
    requirements: MetadataRequirements,
    observation_plan: ObservationPlan,
}

impl Policy {
    fn new(
        request: &TraversalRequest,
        requirements: MetadataRequirements,
        metadata: &dyn Metadata,
    ) -> Self {
        // Known unavailable families cannot justify a call. Keep their completed state in the
        // result while preserving the caller's mode for request validation and checkpoints.
        let capabilities = metadata.observation_capabilities();
        let observation_plan =
            MetadataFamily::ALL
                .into_iter()
                .fold(request.observation_plan, |plan, family| {
                    if plan.mode(family).permits_io()
                        && matches!(
                            capabilities.support(family),
                            ObservationSupport::Unsupported | ObservationSupport::NotApplicable
                        )
                    {
                        plan.with(family, ObservationMode::InlineOnly)
                    } else {
                        plan
                    }
                });
        Self {
            filter: request.filter.clone(),
            requirements,
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
    /// Most listings started ahead of the cursor and not yet ended, when a parked listing holds a
    /// server resource ([`Namespace::parked_listing_limit`]).
    parked_listing_limit: Option<usize>,
    /// Checkpoints and where to resume from, when the request asks for either; `None` costs
    /// nothing (P7 §9.1).
    resume: Option<Resume>,
    probe: Probe,
}

impl Shared {
    fn new(
        namespace: Arc<dyn Namespace>,
        metadata: Arc<dyn Metadata>,
        request: TraversalRequest,
        requirements: MetadataRequirements,
        resume: Option<Resume>,
        probe: Probe,
    ) -> Self {
        let parked_listing_limit = namespace.parked_listing_limit().map(NonZeroUsize::get);
        let policy = Policy::new(&request, requirements, metadata.as_ref());
        Self {
            namespace,
            metadata,
            policy,
            request,
            tickets: AtomicU64::new(0),
            parked_listing_limit,
            resume,
            probe,
        }
    }
}

/// The admission decision for one listed child, when it can be made from the listing alone.
///
/// `None` means a filter dependency still needs observation, or that observation failed. The
/// decision then follows the child's full observation. Made once per child, when its page arrives, so that a
/// prefetched directory's subdirectories are known before the cursor reaches it.
fn immediate_decision(
    shared: &Shared,
    parent: &DirectoryWork,
    descriptor: &SourceDescriptor,
) -> Option<(TraversalDecision, MetadataObservations)> {
    let requirements = if parent.filter_children {
        shared.policy.requirements
    } else {
        MetadataRequirements::NONE
    };
    let metadata = observe::dependency_without_io(
        shared.metadata.as_ref(),
        shared.policy.observation_plan.only(requirements),
        descriptor,
    )?
    .ok()?;
    let candidate = candidate_of(
        &shared.request.root,
        &descriptor.path,
        descriptor.kind,
        descriptor.size,
        &metadata,
    );
    Some((
        shared.policy.decide(parent.filter_children, &candidate),
        metadata,
    ))
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
        hint: None,
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

/// The name the filter sees: the path's last non-empty segment, so a directory object `p/d/` is
/// named `d` like its directory (P7 F5, D7).
fn entry_name(path: &StoragePath) -> &str {
    path.as_str()
        .rsplit('/')
        .find(|segment| !segment.is_empty())
        .unwrap_or_default()
}

/// What one child's settled observation comes to: the item its slot holds, where its subtree
/// went when a deferred decision was made now, and the directory to list below it, if any.
struct Resolved {
    item: ChildItem,
    descent: Option<ChildDescent>,
    below: Option<DirectoryWork>,
}

struct ResolutionError {
    terminal: TraversalTerminalFailure,
    observations: Vec<super::ObservationFailure>,
}

/// Resolves one child's observation, applying a deferred decision now that it can be made. A
/// session failure ends the traversal.
fn resolve(
    shared: &Shared,
    result: Result<Box<ObservedEntry>, observe::ObservationError>,
    deferred: Option<Deferred>,
) -> Result<Resolved, ResolutionError> {
    let (result, observations) = match result {
        Ok(entry) => (Ok(entry), Vec::new()),
        Err(error) => (Err(error.failure), error.observations),
    };
    let (item, below) = match result {
        Ok(entry) => settle_entry(shared, entry, deferred),
        // Gone since the listing: nothing to emit or descend into, and no failure (Q12).
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => {
            (ChildItem::Vanished(observations), None)
        }
        Err(StorageRoleFailure::Entry(error)) => settle_failure(error, deferred, observations),
        Err(StorageRoleFailure::Session(error)) => {
            return Err(ResolutionError {
                terminal: TraversalTerminalFailure::Session(error),
                observations,
            });
        }
    };
    let descent = match item {
        ChildItem::Vanished(_) => None,
        _ => deferred
            .and_then(|deferred| deferred_descent(&shared.request, deferred, below.is_some())),
    };
    Ok(Resolved {
        item,
        descent,
        below,
    })
}

/// A deferred decision whose observation failed cannot be evaluated. Reporting the failure and
/// pruning would silently drop the whole subtree, so a directory is still listed; the caller sees
/// the failure item and keeps a complete enumeration below it.
fn settle_failure(
    error: EntryOperationFailure,
    deferred: Option<Deferred>,
    observations: Vec<super::ObservationFailure>,
) -> (ChildItem, Option<DirectoryWork>) {
    let descend = deferred
        .filter(|deferred| deferred.ticket.is_some())
        .map(|deferred| DirectoryWork {
            path: error.path().clone(),
            child_depth: deferred.depth.saturating_add(1),
            filter_children: true,
            hint: None,
        });
    let failure = Box::new(ScanFailure::child(error));
    let item = if observations.is_empty() {
        ChildItem::Item(TraversalItem::Failure(failure))
    } else {
        ChildItem::ObservedFailure {
            failure,
            observations,
        }
    };
    (item, descend)
}

/// What the filter sees of an observed child: the same candidate whether the decision is made in
/// the listing's task or once a deferred observation settles.
fn entry_candidate<'a>(root: &StoragePath, entry: &'a ObservedEntry) -> TraversalCandidate<'a> {
    candidate_of(
        root,
        entry.path(),
        entry.kind(),
        entry.size(),
        entry.metadata(),
    )
}

fn candidate_of<'a>(
    root: &StoragePath,
    path: &'a StoragePath,
    kind: EntryKind,
    size: Option<u64>,
    metadata: &'a MetadataObservations,
) -> TraversalCandidate<'a> {
    TraversalCandidate {
        path: relative_to(root, path),
        name: entry_name(path),
        kind,
        size,
        modified: metadata
            .timestamps()
            .value()
            .and_then(|times| times.modified),
        metadata,
    }
}

/// Applies a deferred decision now that its metadata dependencies have settled.
fn settle_entry(
    shared: &Shared,
    entry: Box<ObservedEntry>,
    deferred: Option<Deferred>,
) -> (ChildItem, Option<DirectoryWork>) {
    let Some(deferred) = deferred else {
        return (ChildItem::Item(TraversalItem::Entry(entry)), None);
    };
    let decision = shared
        .policy
        .decide(true, &entry_candidate(&shared.request.root, &entry));
    let descend = (decision.descend && deferred.ticket.is_some()).then(|| DirectoryWork {
        path: entry.path().clone(),
        child_depth: deferred.depth.saturating_add(1),
        filter_children: decision.filter_children,
        hint: None,
    });
    let item = if decision.emit {
        ChildItem::Item(TraversalItem::Entry(entry))
    } else {
        ChildItem::Filtered(output::observation_failures(&entry))
    };
    (item, descend)
}

/// A throttled session failure scoped to the entry it was met at: throttling asks for fewer
/// requests and never ends a traversal (P7 §8). Any other session failure is returned unchanged.
fn throttling_scoped_to(
    path: &StoragePath,
    operation: Operation,
    failure: StorageRoleFailure,
) -> StorageRoleFailure {
    match failure {
        StorageRoleFailure::Session(session) if session.class() == FailureClass::Throttled => {
            EntryOperationFailure::new(
                path.clone(),
                operation,
                FailureClass::Throttled,
                Transience::Transient,
                session.diagnostic(),
            )
            .map_or(
                StorageRoleFailure::Session(session),
                StorageRoleFailure::Entry,
            )
        }
        other => other,
    }
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
