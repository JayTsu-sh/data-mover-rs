//! The admission cursor: walks the tree depth-first and assigns every output slot its sequence.
//!
//! Output order is a sequence of *blocks*. A block is every child of one listed directory, in
//! the order the request asked for. The root's block comes first; after a directory's block,
//! each subdirectory it descends into contributes its own block, recursively, in that same
//! order. So a directory's children always follow the directory itself, and one listing's
//! children are contiguous. Blocks never interleave, which is why the stream is not a
//! depth-first preorder over paths: every child of a directory precedes every grandchild.
//!
//! Only the cursor allocates sequence numbers, and it does so in exactly that order. Listings
//! and observations may finish in any order; timing decides only *when* the cursor can take the
//! next step, never which sequence a result receives, and the reorder buffer releases results
//! strictly by sequence. That is what makes the output deterministic for a given tree.
//!
//! Listings are prefetched ahead of the cursor in the same depth-first order. A listing is
//! *prepared* by its own task as it arrives: each child's admission decision is made then
//! (unless it waits for a deferred observation), so the subdirectories of a prefetched directory
//! are known, and can be prefetched in turn, before the cursor reaches it; and every child that
//! needs no I/O is observed then too, so the driver takes a finished block in one hand-off.

use std::collections::{HashMap, VecDeque};
use std::ops::ControlFlow;
use std::sync::Arc;

use super::observe::{Feed, Listed, Observations, observe_listed};
use super::output::BlockFacts;
#[cfg(test)]
use super::residency::Residency;
use super::{
    Admitting, DescendOutcome, DirectoryWork, ListingTask, Runtime, Shared, State, admit,
    descend_outcome, entry_failure, entry_name, immediate_decision, queue_block_end, queue_failure,
    queue_subtree_end,
};
use crate::model::{EntryKind, EntryOperationFailure, FailureClass, StoragePath};
use crate::storage::{NamespaceRequest, NamespaceResult, SourceDescriptor, StorageRoleFailure};
use crate::traversal::{
    ChildOrder, TraversalDecision, TraversalOrder, TraversalTerminalFailure, TraversalVersions,
};

/// Why the cursor stopped advancing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Step {
    /// Every block has been admitted.
    Done,
    /// The admission window is full.
    WindowFull,
    /// The next block's listing has not arrived yet.
    WaitListing,
    /// The next descend decision waits for a deferred observation.
    WaitObservation,
}

/// Where the cursor goes after a block, in the block's own order.
pub(super) enum Slot {
    Descend(DirectoryWork),
    /// A deferred decision, keyed by the sequence of the observation that settles it.
    Pending(u64),
}

/// One listed child the output will show, with the decision made when its listing arrived;
/// `None` waits for a deferred observation. A child an immediate decision hides never becomes
/// one: it is only counted.
pub(super) struct Child {
    pub(super) kind: EntryKind,
    pub(super) listed: Listed,
    pub(super) decision: Option<TraversalDecision>,
}

/// Deferred descend decisions by the sequence of the observation that settles them.
pub(super) type Decisions = HashMap<u64, Option<DirectoryWork>>;

/// A listing turned into a block, ready for the cursor.
pub(super) struct Prepared {
    failures: Vec<EntryOperationFailure>,
    children: VecDeque<Child>,
    /// Where the cursor goes after this block, in the block's own order.
    ///
    /// Built here rather than as each child is admitted, so one listing yields one table instead
    /// of two copies of it, and [`descend_outcome`] runs once per child instead of twice. Empty
    /// when the filter defers its decisions: those slots carry an observation's sequence, which
    /// only exists once the child is admitted.
    slots: VecDeque<Slot>,
    /// Subdirectories an immediate decision did not descend into, and those `max_depth` stopped
    /// at. Counted here for the same reason the slots are built here.
    pruned: u64,
    truncated: u64,
    /// Children an immediate decision kept out of the output.
    hidden: u64,
}

/// What a listing's task hands the driver: the block, the directory's own failure, or the end
/// of the session.
pub(super) type Arrival = Result<Result<Prepared, EntryOperationFailure>, TraversalTerminalFailure>;

/// A started listing: its prepared result once it has arrived.
struct Listing {
    arrived: Option<Result<Prepared, EntryOperationFailure>>,
}

/// One directory on the cursor's path from the root.
struct Frame {
    work: DirectoryWork,
    /// Children still to admit, in the block's own order; `None` until the listing is taken.
    children: Option<VecDeque<Child>>,
    /// Subdirectories to descend into once the block is admitted.
    slots: VecDeque<Slot>,
    /// The block's observation task, from its first child that needs I/O until its last child
    /// is admitted.
    feed: Option<Feed>,
    /// What this block knows that no output slot records.
    facts: BlockFacts,
    /// Whether the block's end marker has been queued. `next_after_block` runs once per slot,
    /// so the marker needs a flag of its own to stay one per directory.
    closed: bool,
}

impl Frame {
    /// `child_order` is set here rather than when the listing lands, so that a directory that
    /// could not be listed at all still reports the order the request asked for instead of
    /// falling back to the default and reading as if the option had been ignored.
    fn new(work: DirectoryWork, child_order: ChildOrder) -> Self {
        Self {
            work,
            children: None,
            slots: VecDeque::new(),
            feed: None,
            facts: BlockFacts {
                child_order,
                ..BlockFacts::default()
            },
            closed: false,
        }
    }
}

pub(super) struct Cursor {
    stack: Vec<Frame>,
    /// What every frame reports as its block's order. Per request today; the field lives on
    /// `BlockFacts` so a future per-directory degradation can report itself.
    child_order: ChildOrder,
    /// Deferred descend decisions, filled as their observations settle.
    decisions: Decisions,
    /// Listings started for frames the cursor has not taken yet.
    listings: HashMap<StoragePath, Listing>,
}

/// What to do with the top frame once its block is admitted.
enum Next {
    Pop,
    Descend(DirectoryWork),
    Skip,
    Wait,
}

impl Cursor {
    pub(super) fn new(root: DirectoryWork, child_order: ChildOrder) -> Self {
        Self {
            stack: vec![Frame::new(root, child_order)],
            child_order,
            decisions: HashMap::new(),
            listings: HashMap::new(),
        }
    }

    /// What the cursor is holding: see [`Residency`].
    ///
    /// Walks the whole stack, not just its top: every frame on the path from the root keeps its
    /// own remaining children, so a deep tree holds one block per level.
    #[cfg(test)]
    pub(super) fn residency(&self) -> Residency {
        let mut sample = Residency {
            deferred_decisions: self.decisions.len(),
            ..Residency::default()
        };
        for frame in &self.stack {
            sample.block_children += frame.children.as_ref().map_or(0, VecDeque::len);
            sample.block_capacity += frame.children.as_ref().map_or(0, VecDeque::capacity);
            sample.descend_slots += frame.slots.len();
            sample.path_bytes += frame.work.path.as_str().len();
            if let Some(pending) = frame.children.as_ref() {
                sample.path_bytes += pending
                    .iter()
                    .map(|child| child.listed.path_len())
                    .sum::<usize>();
            }
        }
        for (path, listing) in &self.listings {
            match listing.arrived.as_ref() {
                None => sample.listings_inflight += 1,
                Some(arrived) => {
                    sample.listings_arrived += 1;
                    if let Ok(prepared) = arrived {
                        sample.prefetched_children += prepared.children.len();
                        sample.block_capacity += prepared.children.capacity();
                        sample.prefetched_slots += prepared.slots.len();
                        sample.path_bytes += prepared_path_bytes(prepared);
                    }
                }
            }
            sample.path_bytes += path.as_str().len();
        }
        sample
    }

    /// Where deferred observations record their descend decisions.
    pub(super) const fn decisions(&mut self) -> &mut Decisions {
        &mut self.decisions
    }

    /// Takes a listing its task has prepared. A lost session ends the traversal at once.
    pub(super) fn listed(
        &mut self,
        path: &StoragePath,
        arrival: Arrival,
    ) -> Result<(), TraversalTerminalFailure> {
        let Some(listing) = self.listings.get_mut(path) else {
            return Err(TraversalTerminalFailure::Internal);
        };
        listing.arrived = Some(arrival?);
        Ok(())
    }

    /// Starts the listing the cursor needs next, then prefetches the listings it will need
    /// soonest, in output order.
    ///
    /// The needed listing always starts, even over budget: prefetched listings further along
    /// must never be able to starve the one the cursor is blocked on. Candidates are visited
    /// depth-first from the top frame's slots (the deepest, needed first) down the stack; a
    /// candidate whose listing already arrived is expanded into its own subdirectories.
    ///
    /// Two separate limits apply. At most `budget` listings are in flight, and at most
    /// `2 × budget` listings, in flight and finished together, may wait for the cursor. Counting finished ones against the
    /// in-flight budget would let early-prefetched shallow siblings keep the deeper listings the
    /// cursor needs first from starting.
    pub(super) fn start_listings(&mut self, runtime: &Runtime<'_>, listings: &mut ListingTask) {
        if let Some(frame) = self.stack.last()
            && frame.children.is_none()
            && !self.listings.contains_key(&frame.work.path)
        {
            let work = frame.work.clone();
            spawn_listing(runtime, listings, &mut self.listings, work);
        }
        let budget = listing_budget(runtime);
        // `self.listings` holds every started listing not yet taken, in flight or finished, so
        // capping it at 2 × budget bounds the finished ones there even after everything in
        // flight arrives.
        let room = budget
            .saturating_sub(listings.len())
            .min(budget.saturating_mul(2).saturating_sub(self.listings.len()));
        for work in self.prefetch_candidates(room, budget.saturating_mul(4)) {
            spawn_listing(runtime, listings, &mut self.listings, work);
        }
    }

    /// Up to `room` directories to prefetch, in output order, examining at most `scan_cap`
    /// candidates so a huge fan-out does not make every wake-up cost that fan-out. Undecided
    /// `Pending` slots are skipped without counting: there are at most as many as observations
    /// in flight.
    fn prefetch_candidates(&self, room: usize, scan_cap: usize) -> Vec<DirectoryWork> {
        let mut search = Search {
            listings: &self.listings,
            chosen: Vec::new(),
            room,
            scanned: 0,
            scan_cap,
        };
        if room == 0 {
            return search.chosen;
        }
        for slot in self.stack.iter().rev().flat_map(|frame| frame.slots.iter()) {
            let candidate = match slot {
                Slot::Descend(work) => Some(work),
                Slot::Pending(sequence) => match self.decisions.get(sequence) {
                    Some(decision) => decision.as_ref(),
                    None => continue,
                },
            };
            if search.visit(candidate).is_break() {
                break;
            }
        }
        search.chosen
    }

    /// Admits as much as possible and reports what stopped it.
    pub(super) fn advance(
        &mut self,
        runtime: &Runtime<'_>,
        observations: &mut Observations,
        state: &mut State,
    ) -> Result<Step, TraversalTerminalFailure> {
        loop {
            let Some(frame) = self.stack.last_mut() else {
                return Ok(Step::Done);
            };
            if frame.children.is_none() {
                if !self.take_top_listing(state)? {
                    return Ok(Step::WaitListing);
                }
                continue;
            }
            if frame
                .children
                .as_ref()
                .is_some_and(|children| !children.is_empty())
            {
                match admit_next_child(runtime, frame, &mut self.decisions, observations, state)? {
                    ControlFlow::Break(step) => return Ok(step),
                    ControlFlow::Continue(()) => continue,
                }
            }
            // Every child is admitted, so the block's observation task gets no more.
            frame.feed = None;
            if !frame.closed {
                if state.admitted() >= runtime.request.max_inflight_operations.get() {
                    return Ok(Step::WindowFull);
                }
                queue_block_end(state, frame.work.path.clone(), frame.facts)?;
                frame.closed = true;
                continue;
            }
            match self.next_after_block() {
                Next::Pop => match self.end_top_subtree(runtime, state)? {
                    ControlFlow::Break(step) => return Ok(step),
                    ControlFlow::Continue(()) => {}
                },
                Next::Descend(work) => self.stack.push(Frame::new(work, self.child_order)),
                Next::Skip => {}
                Next::Wait => return Ok(Step::WaitObservation),
            }
        }
    }

    /// Queues the top frame's subtree marker and pops it.
    ///
    /// The window is checked here rather than before [`Self::next_after_block`] because that
    /// call's other branches consume a slot: returning early there would drop a whole subtree,
    /// and only when the window happened to be full. `Next::Pop` changes nothing, so stopping
    /// after it is re-entrant.
    fn end_top_subtree(
        &mut self,
        runtime: &Runtime<'_>,
        state: &mut State,
    ) -> Result<ControlFlow<Step>, TraversalTerminalFailure> {
        if state.admitted() >= runtime.request.max_inflight_operations.get() {
            return Ok(ControlFlow::Break(Step::WindowFull));
        }
        let Some(frame) = self.stack.pop() else {
            return Err(TraversalTerminalFailure::Internal);
        };
        queue_subtree_end(state, frame.work.path)?;
        Ok(ControlFlow::Continue(()))
    }

    /// Takes the top frame's arrived listing as its block, using an empty block when the
    /// directory could not be listed. Returns `false` while the listing is still outstanding.
    fn take_top_listing(&mut self, state: &mut State) -> Result<bool, TraversalTerminalFailure> {
        let Some(frame) = self.stack.last_mut() else {
            return Ok(true);
        };
        let Some(arrived) = self
            .listings
            .get_mut(&frame.work.path)
            .and_then(|listing| listing.arrived.take())
        else {
            return Ok(false);
        };
        self.listings.remove(&frame.work.path);
        match arrived {
            Ok(prepared) => {
                for failure in prepared.failures {
                    queue_failure(state, failure)?;
                }
                frame.facts.pruned = frame.facts.pruned.saturating_add(prepared.pruned);
                frame.facts.truncated = frame.facts.truncated.saturating_add(prepared.truncated);
                frame.facts.hidden = frame.facts.hidden.saturating_add(prepared.hidden);
                frame.slots = prepared.slots;
                frame.children = Some(prepared.children);
            }
            Err(failure) => {
                // A directory that could not be listed still closes like any other: an empty
                // block, then the frame's normal pop. Keeping one close path means its
                // completion items come out of the same code as everyone else's.
                queue_failure(state, failure)?;
                frame.facts.listing_failed = true;
                frame.children = Some(VecDeque::new());
            }
        }
        Ok(true)
    }

    /// Consumes the top frame's next slot once its whole block has been admitted.
    fn next_after_block(&mut self) -> Next {
        let Some(frame) = self.stack.last_mut() else {
            return Next::Pop;
        };
        match frame.slots.front() {
            None => Next::Pop,
            Some(Slot::Pending(sequence)) => {
                let Some(decision) = self.decisions.remove(sequence) else {
                    return Next::Wait;
                };
                frame.slots.pop_front();
                decision.map_or(Next::Skip, Next::Descend)
            }
            Some(Slot::Descend(_)) => match frame.slots.pop_front() {
                Some(Slot::Descend(work)) => Next::Descend(work),
                _ => Next::Skip,
            },
        }
    }
}

/// Admits the top block's next child, or reports that the admission window is full.
fn admit_next_child(
    runtime: &Runtime<'_>,
    frame: &mut Frame,
    decisions: &mut Decisions,
    observations: &mut Observations,
    state: &mut State,
) -> Result<ControlFlow<Step>, TraversalTerminalFailure> {
    if state.admitted() >= runtime.request.max_inflight_operations.get() {
        return Ok(ControlFlow::Break(Step::WindowFull));
    }
    let Some(child) = frame.children.as_mut().and_then(VecDeque::pop_front) else {
        return Ok(ControlFlow::Continue(()));
    };
    let mut block = Admitting {
        work: &frame.work,
        slots: &mut frame.slots,
        feed: &mut frame.feed,
    };
    admit(runtime, &mut block, child, decisions, observations, state)?;
    Ok(ControlFlow::Continue(()))
}

/// Depth-first search over the directories the traversal will list next.
struct Search<'a> {
    listings: &'a HashMap<StoragePath, Listing>,
    chosen: Vec<DirectoryWork>,
    room: usize,
    scanned: usize,
    scan_cap: usize,
}

impl Search<'_> {
    /// Chooses `candidate` if its listing has not started, or expands it into its own known
    /// subdirectories if its listing already arrived. `None` is a decided skip, which still
    /// counts towards the scan cap.
    fn visit(&mut self, candidate: Option<&DirectoryWork>) -> ControlFlow<()> {
        self.scanned += 1;
        if self.scanned > self.scan_cap {
            return ControlFlow::Break(());
        }
        let Some(work) = candidate else {
            return ControlFlow::Continue(());
        };
        match self.listings.get(&work.path) {
            None if !self.chosen.iter().any(|chosen| chosen.path == work.path) => {
                self.chosen.push(work.clone());
                if self.chosen.len() >= self.room {
                    return ControlFlow::Break(());
                }
            }
            Some(Listing {
                arrived: Some(Ok(prepared)),
                ..
            }) => {
                for slot in &prepared.slots {
                    if let Slot::Descend(child) = slot {
                        self.visit(Some(child))?;
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Turns a finished listing into a block, inside the listing's own task: listing-level failures
/// stay failures, every child gets its admission decision now, and every child that needs no
/// I/O is observed now.
fn prepare(
    shared: &Shared,
    work: &DirectoryWork,
    result: Result<NamespaceResult, StorageRoleFailure>,
) -> Arrival {
    let (entries, failures) = match result.map(NamespaceResult::into_listing) {
        Ok(Some(listing)) => listing,
        Ok(None) => return Ok(Err(entry_failure(&work.path, FailureClass::Protocol))),
        Err(StorageRoleFailure::Entry(error)) => return Ok(Err(error)),
        Err(StorageRoleFailure::Session(error)) => {
            return Err(TraversalTerminalFailure::Session(error));
        }
    };
    let plan = shared.policy.observation_plan;
    let mut children = VecDeque::with_capacity(entries.len());
    let mut slots = VecDeque::new();
    let (mut pruned, mut truncated, mut hidden) = (0_u64, 0_u64, 0_u64);
    for descriptor in entries {
        let decision = immediate_decision(shared, work, &descriptor);
        if let Some(decision) = decision {
            match descend_outcome(&shared.request, &descriptor, work.child_depth, decision) {
                DescendOutcome::Into(below) => slots.push_back(Slot::Descend(below)),
                DescendOutcome::Pruned => pruned = pruned.saturating_add(1),
                DescendOutcome::Truncated => truncated = truncated.saturating_add(1),
                DescendOutcome::NotDirectory => {}
            }
            if !decision.emit {
                hidden = hidden.saturating_add(1);
                continue;
            }
        }
        children.push_back(Child {
            kind: descriptor.kind,
            listed: observe_listed(descriptor, plan),
            decision,
        });
    }
    Ok(Ok(Prepared {
        failures,
        children,
        slots,
        pruned,
        truncated,
        hidden,
    }))
}

/// Sorts a listing's described children by the bytes of their final path component.
///
/// Unstable, so a directory of millions does not also pay the half-length temporary a stable
/// sort allocates, which is why the key is total. Names can repeat on S3 (ADR-0006 C22): an
/// object and the prefix of the same name, the object first as in S3's own key order (`a` <
/// `a/`), and in a listing of every version one child per version, kept in listing order (oldest
/// first) by their rank. Anything left tied after that — an NFS readdir that crosses a cookie
/// boundary on a directory being written to can return one child twice — spells the same path,
/// so which one wins cannot change the emitted sequence. `failures` keep their place ahead of the
/// block: a child the listing could not describe has no name to sort by.
fn sort_by_name(result: &mut Result<NamespaceResult, StorageRoleFailure>) {
    let Ok(listing) = result else { return };
    let entries = match listing {
        NamespaceResult::Entries(entries) | NamespaceResult::Listing { entries, .. } => entries,
        NamespaceResult::Completed | NamespaceResult::LinkTarget(_) => return,
    };
    entries.sort_unstable_by(|left, right| sibling_key(left).cmp(&sibling_key(right)));
}

/// The total order [`sort_by_name`] sorts by: name bytes, then files before directories, then
/// listing rank.
fn sibling_key(descriptor: &SourceDescriptor) -> (&str, bool, u32) {
    (
        entry_name(&descriptor.path),
        descriptor.kind == EntryKind::Directory,
        descriptor.listing.rank,
    )
}

/// Upper bound on listings started ahead of the cursor, whatever the request allows.
const LISTING_PREFETCH_CAP: usize = 64;

/// Listings allowed in flight. Separate from the observation window: with the default plan
/// observations never reach the backend, and when they do, a shared budget would let either
/// side starve the other.
fn listing_budget(runtime: &Runtime<'_>) -> usize {
    runtime
        .request
        .max_inflight_operations
        .get()
        .min(LISTING_PREFETCH_CAP)
}

fn spawn_listing(
    runtime: &Runtime<'_>,
    listings: &mut ListingTask,
    started: &mut HashMap<StoragePath, Listing>,
    work: DirectoryWork,
) {
    let path = work.path.clone();
    started.insert(path.clone(), Listing { arrived: None });
    let shared = Arc::clone(runtime.shared);
    // Sorting and preparing ride on the listing's own task rather than the driver's, so a
    // directory of millions is scheduled like the listing I/O already is instead of sitting
    // between two of the driver loop's await points, and several listings prepare in parallel.
    // On a current-thread runtime it still shares the thread; only `spawn_blocking` would change
    // that, and no listing so far has been worth it.
    let cancel = shared
        .namespace
        .listings_are_abortable()
        .then(|| runtime.request.cancel.clone());
    listings.spawn(async move {
        let namespace = shared.namespace.as_ref();
        let listing = match shared.request.versions {
            TraversalVersions::Current => namespace.execute(NamespaceRequest::List(path.clone())),
            TraversalVersions::All => namespace.list_versions(&path),
        };
        // An abortable listing stops at cancellation instead of paging on in the background; the
        // traversal is ending, so what it returns is never used.
        let mut result = match cancel {
            Some(cancel) => tokio::select! {
                biased;
                () = cancel.cancelled() => Err(cancelled_listing(&path)),
                result = listing => result,
            },
            None => listing.await,
        };
        if shared.request.order == TraversalOrder::NameBytes {
            sort_by_name(&mut result);
        }
        let arrival = prepare(&shared, &work, result);
        (path, arrival)
    });
}

fn cancelled_listing(path: &StoragePath) -> StorageRoleFailure {
    StorageRoleFailure::Entry(entry_failure(path, FailureClass::Cancelled))
}

/// Path text held by one arrived-but-untaken listing.
#[cfg(test)]
fn prepared_path_bytes(prepared: &Prepared) -> usize {
    prepared
        .children
        .iter()
        .map(|child| child.listed.path_len())
        .chain(prepared.slots.iter().map(|slot| match slot {
            Slot::Descend(work) => work.path.as_str().len(),
            Slot::Pending(_) => 0,
        }))
        .chain(
            prepared
                .failures
                .iter()
                .map(|failure| failure.path().as_str().len()),
        )
        .sum()
}
