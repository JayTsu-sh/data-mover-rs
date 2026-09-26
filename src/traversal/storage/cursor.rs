//! The admission cursor: walks the tree depth-first and assigns every output slot its sequence.
//!
//! Output order is a sequence of *blocks* ([`TraversalOrder::Blocks`], P7 §3). A block is every
//! child of one listed directory, in the order its listing returned them, taken page by page as
//! the pages arrive. Once a directory's listing has ended and its whole block has been admitted,
//! its `DirectoryListed` follows, and then its subdirectories are descended into in the raw UTF-8
//! byte order of their names, each contributing its own block, recursively. So a directory's
//! children always follow the directory itself, one listing's children are contiguous, and blocks
//! never interleave: every child of a directory precedes every grandchild.
//!
//! Only the descent is sorted (§3.1). A block has to remember its subdirectories until it closes
//! anyway, so sorting them costs `O(d log d)` over the subdirectories alone; sorting the block
//! itself would hold the whole directory and delay its first entry until the last page arrived.
//!
//! Only the cursor allocates sequence numbers, and it does so in exactly that order. Listings and
//! observations may finish in any order; timing decides only *when* the cursor can take the next
//! step, never which sequence a result receives, and the reorder buffer releases results strictly
//! by sequence. That is what makes the output deterministic for a given tree and listing order.
//!
//! Listings are prefetched ahead of the cursor in the same depth-first order, each by its own task
//! ([`stream`](super::stream)), which prepares every page as it arrives. A prefetched listing
//! hands over its first page and then stops until the cursor reaches it — holding at most one
//! more, raw, page — so the subdirectories of a small directory are known, and prefetched in
//! turn, before the cursor gets there, while the traversal holds a couple of pages of a large
//! one however large it is (the backend's own read-ahead aside; see the stream module). No
//! ancestor holds an open listing: a block closes only once its listing has ended.
//!
//! [`TraversalOrder::Blocks`]: crate::traversal::TraversalOrder::Blocks

use std::collections::{HashMap, VecDeque};
use std::ops::ControlFlow;
use std::sync::Arc;

use tokio::sync::Semaphore;

use super::observe::{Feed, Observations};
use super::output::BlockFacts;
#[cfg(test)]
use super::residency::Residency;
use super::stream::{self, Arrival, ArrivalSender, Batch, Child, ListingId, Slot};
use super::{DirectoryWork, Runtime, State, admit, queue_block_end, queue_failure};
use crate::model::{EntryOperationFailure, FailureClass, StoragePath};
use crate::storage::DescentHint;
use crate::traversal::{ScanFailure, TraversalTerminalFailure};

/// Why the cursor stopped advancing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Step {
    /// Every block has been admitted.
    Done,
    /// The admission window is full.
    WindowFull,
    /// The next page of the block, or the end of its listing, has not arrived yet.
    WaitListing,
    /// The next descend decision waits for a deferred observation.
    WaitObservation,
}

/// Deferred descend decisions by the ticket of the slot they fill.
pub(super) type Decisions = HashMap<u64, Option<DirectoryWork>>;

/// How a listing ended.
enum Ended {
    Complete,
    Failed(EntryOperationFailure),
}

/// One started listing, as far as it has arrived.
struct Listing {
    /// Added to each time the cursor takes a page, which lets the listing's task hand over the
    /// next one.
    credit: Arc<Semaphore>,
    /// Pages arrived and not yet taken by the cursor.
    pages: VecDeque<Batch>,
    /// Where the cursor goes once the block closes: in listing order while pages arrive, sorted
    /// by name once the listing has ended.
    slots: VecDeque<Slot>,
    /// What every arrived page knew that no output slot records.
    facts: BlockFacts,
    ended: Option<Ended>,
}

impl Listing {
    fn new(credit: Arc<Semaphore>) -> Self {
        Self {
            credit,
            pages: VecDeque::new(),
            slots: VecDeque::new(),
            facts: BlockFacts::default(),
            ended: None,
        }
    }

    /// Takes one arrival. Returns whether the listing has ended; a terminal arrival ends the
    /// traversal.
    fn receive(&mut self, arrival: Arrival) -> Result<bool, TraversalTerminalFailure> {
        let ended = match arrival {
            Arrival::Page(page) => {
                self.facts.absorb(&page.facts);
                self.slots.extend(page.slots);
                self.pages.push_back(page.batch);
                return Ok(false);
            }
            Arrival::End => Ended::Complete,
            Arrival::Failed(failure) => Ended::Failed(failure),
            Arrival::Terminal(failure) => return Err(failure),
        };
        // Every subdirectory is known now, the ones a failed listing returned before it stopped
        // included: they are descended into like any other. Unstable, so ten million
        // subdirectories do not also pay the half-length buffer a stable sort allocates; two
        // slots can only tie when a listing returned the same directory twice, and then either
        // descent lists the same path (S3's object `a` beside the prefix `a/` is no tie: only the
        // prefix has a slot). It runs on the driver: a directory of ten million
        // subdirectories holds every other listing's arrivals for the seconds it takes, like the
        // slots themselves a cost P8/P9 moves out (§6.2).
        self.slots
            .make_contiguous()
            .sort_unstable_by(|left, right| left.name().cmp(right.name()));
        self.ended = Some(ended);
        Ok(true)
    }

    /// Waiting for its next page: nothing buffered, and not ended.
    fn awaiting(&self) -> bool {
        self.ended.is_none() && self.pages.is_empty()
    }
}

/// One directory on the cursor's path from the root.
struct Frame {
    work: DirectoryWork,
    /// The block's listing, once the cursor has taken it.
    listing: Option<(ListingId, Listing)>,
    /// Children of the page being admitted, in listing order.
    children: VecDeque<Child>,
    /// The block's observation task, from its first child that needs I/O until the block closes.
    feed: Option<Feed>,
    /// Whether the block's end has been queued.
    closed: bool,
}

impl Frame {
    const fn new(work: DirectoryWork) -> Self {
        Self {
            work,
            listing: None,
            children: VecDeque::new(),
            feed: None,
            closed: false,
        }
    }

    fn slots(&self) -> impl Iterator<Item = &Slot> {
        self.listing
            .iter()
            .flat_map(|(_, listing)| listing.slots.iter())
    }
}

pub(super) struct Cursor {
    stack: Vec<Frame>,
    /// Deferred descend decisions, filled as their observations settle.
    decisions: Decisions,
    /// Listings started for frames the cursor has not reached yet.
    started: HashMap<ListingId, Listing>,
    /// The same listings by the path they list.
    by_path: HashMap<StoragePath, ListingId>,
    sender: ArrivalSender,
    next_id: ListingId,
    /// Listings whose task has not ended.
    live: usize,
}

/// What to do with the top frame once its block has closed.
enum Next {
    Pop,
    Descend(DirectoryWork),
    Skip,
    Wait,
}

impl Cursor {
    pub(super) fn new(root: DirectoryWork, sender: ArrivalSender) -> Self {
        Self {
            stack: vec![Frame::new(root)],
            decisions: HashMap::new(),
            started: HashMap::new(),
            by_path: HashMap::new(),
            sender,
            next_id: 0,
            live: 0,
        }
    }

    /// Whether any listing's task may still hand something over.
    pub(super) const fn has_live_listings(&self) -> bool {
        self.live > 0
    }

    /// Where deferred observations record their descend decisions.
    pub(super) const fn decisions(&mut self) -> &mut Decisions {
        &mut self.decisions
    }

    /// Takes one arrival from a listing's task. A lost session ends the traversal at once.
    pub(super) fn listed(
        &mut self,
        id: ListingId,
        arrival: Arrival,
    ) -> Result<(), TraversalTerminalFailure> {
        let top = self
            .stack
            .last_mut()
            .and_then(|frame| frame.listing.as_mut())
            .filter(|(top, _)| *top == id)
            .map(|(_, listing)| listing);
        let listing = match top {
            Some(listing) => listing,
            None => self
                .started
                .get_mut(&id)
                .ok_or(TraversalTerminalFailure::Internal)?,
        };
        if listing.receive(arrival)? {
            self.live = self.live.saturating_sub(1);
        }
        Ok(())
    }

    /// Starts the listing the cursor needs next, then prefetches the listings it will need
    /// soonest, in output order.
    ///
    /// The needed listing always starts, even over budget: prefetched listings further along
    /// must never be able to starve the one the cursor is blocked on. Candidates are visited
    /// depth-first from the top frame's slots (the deepest, needed first) down the stack; a
    /// candidate whose listing already started is expanded into the subdirectories it has
    /// returned so far.
    ///
    /// Two separate limits apply. At most `budget` listings wait for a page, and at most
    /// `2 × budget` listings started ahead of the cursor, waiting or stopped after a page, may
    /// wait for it. Counting the stopped ones against the first budget would let early-prefetched
    /// shallow siblings keep the deeper listings the cursor needs first from starting.
    ///
    /// A namespace whose parked listings hold a server resource (CIFS: a directory handle) adds a
    /// third: at most its `parked_listing_limit` listings started ahead and not yet ended, waiting
    /// or stopped. Every parked listing is one of them, so it bounds the parked ones strictly
    /// without guessing which will park; the one the cursor needs is not counted and always
    /// starts, so the limit cannot stall the traversal.
    pub(super) fn start_listings(&mut self, runtime: &Runtime<'_>) {
        if let Some(frame) = self.stack.last()
            && frame.listing.is_none()
            && !self.by_path.contains_key(&frame.work.path)
        {
            let work = frame.work.clone();
            self.spawn(runtime, work);
        }
        let budget = listing_budget(runtime);
        let top_awaiting = self
            .stack
            .last()
            .filter(|frame| !frame.closed)
            .and_then(|frame| frame.listing.as_ref())
            .is_some_and(|(_, listing)| listing.awaiting());
        let awaiting = self
            .started
            .values()
            .filter(|listing| listing.awaiting())
            .count()
            + usize::from(top_awaiting);
        let mut room = budget
            .saturating_sub(awaiting)
            .min(budget.saturating_mul(2).saturating_sub(self.started.len()));
        if let Some(limit) = runtime.shared.parked_listing_limit {
            let unended = self
                .started
                .values()
                .filter(|listing| listing.ended.is_none())
                .count();
            room = room.min(limit.saturating_sub(unended));
        }
        for work in self.prefetch_candidates(room, budget.saturating_mul(4)) {
            self.spawn(runtime, work);
        }
    }

    fn spawn(&mut self, runtime: &Runtime<'_>, work: DirectoryWork) {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        let path = work.path.clone();
        let credit = stream::spawn(runtime.shared, id, work, &self.sender);
        self.by_path.insert(path, id);
        self.started.insert(id, Listing::new(credit));
        self.live += 1;
    }

    /// Up to `room` directories to prefetch, in output order, examining at most `scan_cap`
    /// candidates so a huge fan-out does not make every wake-up cost that fan-out. An undecided
    /// `Pending` slot is passed over like a decided skip and counts towards the cap. Passing over
    /// one, or visiting an open block's slots in listing order before they are sorted, only
    /// changes what is read early, never the output.
    fn prefetch_candidates(&self, room: usize, scan_cap: usize) -> Vec<DirectoryWork> {
        let mut search = Search {
            started: &self.started,
            by_path: &self.by_path,
            chosen: Vec::new(),
            room,
            scanned: 0,
            scan_cap,
        };
        if room == 0 {
            return search.chosen;
        }
        for slot in self.stack.iter().rev().flat_map(Frame::slots) {
            let (candidate, hint) = match slot {
                Slot::Descend(work) => (Some(work), None),
                Slot::Pending { ticket, hint, .. } => (
                    self.decisions.get(ticket).and_then(Option::as_ref),
                    hint.as_ref(),
                ),
            };
            if search.visit(candidate, hint).is_break() {
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
            let below_root = self.stack.len() > 1;
            let Some(frame) = self.stack.last_mut() else {
                return Ok(Step::Done);
            };
            if frame.closed {
                match self.next_after_block() {
                    Next::Pop => {
                        self.stack.pop();
                    }
                    Next::Descend(work) => self.stack.push(Frame::new(work)),
                    Next::Skip => {}
                    Next::Wait => return Ok(Step::WaitObservation),
                }
                continue;
            }
            if frame.listing.is_none() {
                if !self.take_top_listing()? {
                    return Ok(Step::WaitListing);
                }
                continue;
            }
            if !frame.children.is_empty() {
                match admit_next_child(runtime, frame, &mut self.decisions, observations, state)? {
                    ControlFlow::Break(step) => return Ok(step),
                    ControlFlow::Continue(()) => continue,
                }
            }
            if let ControlFlow::Break(step) = next_page_or_close(runtime, frame, state, below_root)?
            {
                return Ok(step);
            }
        }
    }

    /// Takes the top frame's started listing as its block. Returns `false` while it has not been
    /// started.
    fn take_top_listing(&mut self) -> Result<bool, TraversalTerminalFailure> {
        let Some(frame) = self.stack.last_mut() else {
            return Ok(true);
        };
        let Some(id) = self.by_path.remove(&frame.work.path) else {
            return Ok(false);
        };
        let listing = self
            .started
            .remove(&id)
            .ok_or(TraversalTerminalFailure::Internal)?;
        frame.listing = Some((id, listing));
        Ok(true)
    }

    /// Consumes the top frame's next slot once its block has closed.
    fn next_after_block(&mut self) -> Next {
        let Some((_, listing)) = self
            .stack
            .last_mut()
            .and_then(|frame| frame.listing.as_mut())
        else {
            return Next::Pop;
        };
        match listing.slots.front() {
            None => Next::Pop,
            Some(Slot::Pending { ticket, .. }) => {
                let ticket = *ticket;
                let Some(decision) = self.decisions.remove(&ticket) else {
                    return Next::Wait;
                };
                let Some(Slot::Pending { hint, .. }) = listing.slots.pop_front() else {
                    return Next::Skip;
                };
                decision.map_or(Next::Skip, |work| Next::Descend(work.with_hint(hint)))
            }
            Some(Slot::Descend(_)) => match listing.slots.pop_front() {
                Some(Slot::Descend(work)) => Next::Descend(work),
                _ => Next::Skip,
            },
        }
    }
}

/// Once the page being admitted is done: takes the listing's next page, or waits for it, or —
/// the listing having ended — closes the block.
fn next_page_or_close(
    runtime: &Runtime<'_>,
    frame: &mut Frame,
    state: &mut State,
    below_root: bool,
) -> Result<ControlFlow<Step>, TraversalTerminalFailure> {
    let Some((_, listing)) = frame.listing.as_mut() else {
        return Err(TraversalTerminalFailure::Internal);
    };
    if let Some(batch) = listing.pages.pop_front() {
        // Room for the listing's next page, now that this one is the cursor's.
        listing.credit.add_permits(1);
        for failure in batch.failures {
            queue_failure(state, ScanFailure::child(failure))?;
        }
        frame.children = batch.children;
        return Ok(ControlFlow::Continue(()));
    }
    if listing.ended.is_none() {
        return Ok(ControlFlow::Break(Step::WaitListing));
    }
    // Every child is admitted and the listing has ended: the block closes, and its observation
    // task gets no more.
    frame.feed = None;
    if state.admitted() >= runtime.request.max_inflight_operations.get() {
        return Ok(ControlFlow::Break(Step::WindowFull));
    }
    close_block(frame, state, below_root)?;
    Ok(ControlFlow::Continue(()))
}

/// Queues the end of the top frame's block: the listing's own failure when it has one, then the
/// block end. A subdirectory gone before its listing returned anything is a complete, empty
/// listing and no failure (Q12); the traversal root never is, so a mistyped root cannot read as
/// an empty source.
///
/// A listing that failed after returning children is `Interrupted`: its failure says how many it
/// returned (`delivered`, which is the block's `listed`), and it sits after them, inside the block.
fn close_block(
    frame: &mut Frame,
    state: &mut State,
    below_root: bool,
) -> Result<(), TraversalTerminalFailure> {
    let Some((_, listing)) = frame.listing.as_mut() else {
        return Err(TraversalTerminalFailure::Internal);
    };
    let mut facts = listing.facts;
    match listing.ended.take() {
        Some(Ended::Failed(failure))
            if failure.class() == FailureClass::NotFound && below_root && facts.listed == 0 =>
        {
            facts.vanished_listing = true;
        }
        Some(Ended::Failed(failure)) => {
            let failure = ScanFailure::listing(frame.work.path.clone(), failure, facts.listed);
            queue_failure(state, failure)?;
        }
        Some(Ended::Complete) | None => {}
    }
    queue_block_end(state, frame.work.path.clone(), facts)?;
    frame.closed = true;
    // An emptied queue keeps its room; an ancestor has no more use for it.
    frame.children = VecDeque::new();
    Ok(())
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
    let Some(child) = frame.children.pop_front() else {
        return Ok(ControlFlow::Continue(()));
    };
    admit(
        runtime,
        &mut frame.feed,
        child,
        decisions,
        observations,
        state,
    )?;
    Ok(ControlFlow::Continue(()))
}

/// Depth-first search over the directories the traversal will list next.
struct Search<'a> {
    started: &'a HashMap<ListingId, Listing>,
    by_path: &'a HashMap<StoragePath, ListingId>,
    chosen: Vec<DirectoryWork>,
    room: usize,
    scanned: usize,
    scan_cap: usize,
}

impl Search<'_> {
    /// Chooses `candidate` if its listing has not started, or expands it into the subdirectories
    /// its listing has returned so far. `None` is a decided skip, which still counts towards the
    /// scan cap. `hint` is the slot's, for a candidate a deferred decision produced.
    fn visit(
        &mut self,
        candidate: Option<&DirectoryWork>,
        hint: Option<&DescentHint>,
    ) -> ControlFlow<()> {
        self.scanned += 1;
        if self.scanned > self.scan_cap {
            return ControlFlow::Break(());
        }
        let Some(work) = candidate else {
            return ControlFlow::Continue(());
        };
        match self.by_path.get(&work.path) {
            None if !self.chosen.iter().any(|chosen| chosen.path == work.path) => {
                self.chosen.push(work.clone().with_hint(hint.cloned()));
                if self.chosen.len() >= self.room {
                    return ControlFlow::Break(());
                }
            }
            Some(id) => {
                let slots = self.started.get(id).map(|listing| &listing.slots);
                for slot in slots.into_iter().flatten() {
                    if let Slot::Descend(child) = slot {
                        self.visit(Some(child), None)?;
                    }
                }
            }
            None => {}
        }
        ControlFlow::Continue(())
    }
}

/// Upper bound on listings started ahead of the cursor, whatever the request allows.
const LISTING_PREFETCH_CAP: usize = 64;

/// Listings allowed to wait for a page at once. Separate from the observation window: with the
/// default plan observations never reach the backend, and when they do, a shared budget would let
/// either side starve the other.
fn listing_budget(runtime: &Runtime<'_>) -> usize {
    runtime
        .request
        .max_inflight_operations
        .get()
        .min(LISTING_PREFETCH_CAP)
}

#[cfg(test)]
impl Cursor {
    /// What the cursor is holding: see [`Residency`].
    ///
    /// Walks the whole stack, not just its top: every frame on the path from the root keeps its
    /// own slots, so a deep tree holds one set of remaining turns per level.
    pub(super) fn residency(&self) -> Residency {
        let mut sample = Residency {
            deferred_decisions: self.decisions.len(),
            ..Residency::default()
        };
        for frame in &self.stack {
            sample.block_children += frame.children.len();
            sample.block_capacity += frame.children.capacity();
            sample.path_bytes += frame.work.path.as_str().len()
                + frame
                    .children
                    .iter()
                    .map(|child| child.listed.path_len())
                    .sum::<usize>();
            if let Some((_, listing)) = &frame.listing {
                sample.descend_slots += listing.slots.len();
                sample.pages_buffered += listing.pages.len();
                for batch in &listing.pages {
                    sample.block_children += batch.children.len();
                    sample.block_capacity += batch.children.capacity();
                }
                sample.path_bytes += listing_path_bytes(listing);
            }
        }
        for (id, listing) in &self.started {
            if listing.awaiting() {
                sample.listings_inflight += 1;
            } else {
                sample.listings_arrived += 1;
            }
            if listing.ended.is_none() && !listing.pages.is_empty() {
                sample.parked_listings += 1;
            }
            sample.pages_buffered += listing.pages.len();
            sample.prefetched_slots += listing.slots.len();
            for batch in &listing.pages {
                sample.prefetched_children += batch.children.len();
                sample.block_capacity += batch.children.capacity();
            }
            sample.path_bytes += listing_path_bytes(listing);
            let path = self
                .by_path
                .iter()
                .find_map(|(path, known)| (known == id).then_some(path));
            sample.path_bytes += path.map_or(0, |path| path.as_str().len());
        }
        sample
    }
}

/// Path text held by one listing's buffered pages and slots.
#[cfg(test)]
fn listing_path_bytes(listing: &Listing) -> usize {
    let pages: usize = listing
        .pages
        .iter()
        .map(|batch| {
            batch
                .children
                .iter()
                .map(|child| child.listed.path_len())
                .chain(
                    batch
                        .failures
                        .iter()
                        .map(|failure| failure.path().as_str().len()),
                )
                .sum::<usize>()
        })
        .sum();
    let slots: usize = listing
        .slots
        .iter()
        .map(|slot| slot.path().as_str().len())
        .sum();
    pages + slots
}
