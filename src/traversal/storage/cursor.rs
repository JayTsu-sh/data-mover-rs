//! The admission cursor: walks the tree depth-first and assigns every output slot its sequence.
//!
//! Output order is a sequence of *blocks*. A block is every child of one listed directory, in
//! listing order. The root's block comes first; after a directory's block, each subdirectory
//! it descends into contributes its own block, recursively, in listing order. So a directory's
//! children always follow the directory itself, and one listing's children are contiguous.
//!
//! Only the cursor allocates sequence numbers, and it does so in exactly that order. Listings
//! and observations may finish in any order; timing decides only *when* the cursor can take the
//! next step, never which sequence a result receives, and the reorder buffer releases results
//! strictly by sequence. That is what makes the output deterministic for a given tree.
//!
//! Listings are prefetched ahead of the cursor in the same depth-first order. A listing is
//! *prepared* when it arrives: each child's admission decision is made then (unless it waits for
//! a deferred observation), so the subdirectories of a prefetched directory are known, and can
//! be prefetched in turn, before the cursor reaches it.

use std::collections::{HashMap, VecDeque};
use std::ops::ControlFlow;
use std::sync::Arc;

use super::output::BlockFacts;
use super::{
    DirectoryWork, ListingTask, ObservationTask, Runtime, State, admit, descend_work,
    entry_failure, immediate_decision, queue_block_end, queue_failure, queue_subtree_end,
};
use crate::model::{EntryOperationFailure, FailureClass, StoragePath};
use crate::storage::{NamespaceRequest, NamespaceResult, SourceDescriptor, StorageRoleFailure};
use crate::traversal::{TraversalDecision, TraversalTerminalFailure};

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

/// Where the cursor goes after a block, in listing order.
pub(super) enum Slot {
    Descend(DirectoryWork),
    /// A deferred decision, keyed by the sequence of the observation that settles it.
    Pending(u64),
}

/// One listed child with the decision made when its listing arrived; `None` waits for a
/// deferred observation.
pub(super) struct Child {
    pub(super) descriptor: SourceDescriptor,
    pub(super) decision: Option<TraversalDecision>,
}

/// A listing turned into a block, ready for the cursor.
struct Prepared {
    failures: Vec<EntryOperationFailure>,
    children: VecDeque<Child>,
    /// Subdirectories this block already knows it will descend into, in listing order.
    descend: Vec<DirectoryWork>,
}

/// A started listing: the directory, and its prepared result once it has arrived.
struct Listing {
    work: DirectoryWork,
    arrived: Option<Result<Prepared, EntryOperationFailure>>,
}

/// One directory on the cursor's path from the root.
struct Frame {
    work: DirectoryWork,
    /// Children still to admit, in listing order; `None` until the listing is taken.
    children: Option<VecDeque<Child>>,
    /// Subdirectories to descend into once the block is admitted.
    slots: VecDeque<Slot>,
    /// What this block knows that no output slot records.
    facts: BlockFacts,
    /// Whether the block's end marker has been queued. `next_after_block` runs once per slot,
    /// so the marker needs a flag of its own to stay one per directory.
    closed: bool,
}

impl Frame {
    fn new(work: DirectoryWork) -> Self {
        Self {
            work,
            children: None,
            slots: VecDeque::new(),
            facts: BlockFacts::default(),
            closed: false,
        }
    }
}

pub(super) struct Cursor {
    stack: Vec<Frame>,
    /// Deferred descend decisions, filled as their observations settle.
    decisions: HashMap<u64, Option<DirectoryWork>>,
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
    pub(super) fn new(root: DirectoryWork) -> Self {
        Self {
            stack: vec![Frame::new(root)],
            decisions: HashMap::new(),
            listings: HashMap::new(),
        }
    }

    /// Records a deferred observation's descend decision.
    pub(super) fn decide(&mut self, sequence: u64, descend: Option<DirectoryWork>) {
        self.decisions.insert(sequence, descend);
    }

    /// Prepares a finished listing. A lost session ends the traversal at once.
    pub(super) fn listed(
        &mut self,
        runtime: &Runtime<'_>,
        path: &StoragePath,
        result: Result<NamespaceResult, StorageRoleFailure>,
    ) -> Result<(), TraversalTerminalFailure> {
        let Some(listing) = self.listings.get_mut(path) else {
            return Err(TraversalTerminalFailure::Internal);
        };
        listing.arrived = Some(prepare(runtime, &listing.work, result)?);
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
        tasks: &mut ObservationTask,
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
                match admit_next_child(runtime, frame, tasks, state)? {
                    ControlFlow::Break(step) => return Ok(step),
                    ControlFlow::Continue(()) => continue,
                }
            }
            if !frame.closed {
                if state.admitted() >= runtime.request.max_inflight_operations.get() {
                    return Ok(Step::WindowFull);
                }
                queue_block_end(state, frame.work.path.clone(), frame.facts)?;
                frame.closed = true;
                continue;
            }
            match self.next_after_block() {
                Next::Pop => {
                    // The window check belongs here and not before `next_after_block`: its
                    // other branches consume a slot, so returning early there would drop a
                    // whole subtree. `Next::Pop` changes nothing, so this is re-entrant.
                    if state.admitted() >= runtime.request.max_inflight_operations.get() {
                        return Ok(Step::WindowFull);
                    }
                    let Some(frame) = self.stack.pop() else {
                        return Err(TraversalTerminalFailure::Internal);
                    };
                    queue_subtree_end(state, frame.work.path)?;
                }
                Next::Descend(work) => self.stack.push(Frame::new(work)),
                Next::Skip => {}
                Next::Wait => return Ok(Step::WaitObservation),
            }
        }
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
    tasks: &mut ObservationTask,
    state: &mut State,
) -> Result<ControlFlow<Step>, TraversalTerminalFailure> {
    if state.admitted() >= runtime.request.max_inflight_operations.get() {
        return Ok(ControlFlow::Break(Step::WindowFull));
    }
    let Some(child) = frame.children.as_mut().and_then(VecDeque::pop_front) else {
        return Ok(ControlFlow::Continue(()));
    };
    admit(
        runtime,
        &frame.work,
        child,
        &mut frame.slots,
        &mut frame.facts,
        tasks,
        state,
    )?;
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
                for child in &prepared.descend {
                    self.visit(Some(child))?;
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Turns a finished listing into a block: listing-level failures stay failures, and every child
/// gets its admission decision now.
fn prepare(
    runtime: &Runtime<'_>,
    work: &DirectoryWork,
    result: Result<NamespaceResult, StorageRoleFailure>,
) -> Result<Result<Prepared, EntryOperationFailure>, TraversalTerminalFailure> {
    let (entries, failures) = match result.map(NamespaceResult::into_listing) {
        Ok(Some(listing)) => listing,
        Ok(None) => return Ok(Err(entry_failure(&work.path, FailureClass::Protocol))),
        Err(StorageRoleFailure::Entry(error)) => return Ok(Err(error)),
        Err(StorageRoleFailure::Session(error)) => {
            return Err(TraversalTerminalFailure::Session(error));
        }
    };
    let mut children = VecDeque::with_capacity(entries.len());
    let mut descend = Vec::new();
    for descriptor in entries {
        let decision = immediate_decision(runtime, work, &descriptor);
        if let Some(decision) = decision
            && let Some(below) =
                descend_work(runtime.request, &descriptor, work.child_depth, decision)
        {
            descend.push(below);
        }
        children.push_back(Child {
            descriptor,
            decision,
        });
    }
    Ok(Ok(Prepared {
        failures,
        children,
        descend,
    }))
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
    started.insert(
        path.clone(),
        Listing {
            work,
            arrived: None,
        },
    );
    let namespace = Arc::clone(runtime.namespace);
    listings.spawn(async move {
        let result = namespace
            .execute(NamespaceRequest::List(path.clone()))
            .await;
        (path, result)
    });
}
