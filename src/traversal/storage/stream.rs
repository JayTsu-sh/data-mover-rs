//! One listing's task: reads a directory's listing page by page, prepares each page for the
//! cursor, and hands it over at most [`PAGES_AHEAD`] pages ahead of what the cursor has taken.
//!
//! A page is *prepared* in this task, not in the driver: every child's admission decision is made
//! here (unless it waits for an observation that needs I/O), every child that needs no I/O is
//! observed here, and the subdirectories to descend into are known as soon as the page is, so
//! they can be prefetched in turn. The driver is woken once per page, never once per child.
//!
//! **Bounded.** The task waits for room ([`credit`](spawn)) before it takes a page off the stream,
//! so, however large the directory, what the traversal holds of one listing is the page the
//! cursor is admitting, [`PAGES_AHEAD`] prepared pages handed over and not yet taken, and at most
//! one raw page a peek took off the stream (see [`read`]). A listing the cursor has not reached
//! stops there. The backend's stream reads ahead on its own (one page in its channel and one in
//! the producer's hands, P7 §5.1) — Local (C5), S3's current objects (C6), NFS (C7), HDFS (C8) and
//! CIFS (C9) stream for real; S3's histories are gathered whole per directory first (§5.3, Q9),
//! then built a page at a time. The task never collects a listing whole.
//!
//! **Never aborted.** The task is detached. It ends when the listing ends, or when the driver is
//! gone (the traversal ended): then it drops the stream, and the backend's producer leaves through
//! its own close path, as the listing contract requires. A task that stops any other way (a
//! filter that panics) reports an `Internal` terminal failure on its way out, so the traversal
//! never waits for a page that cannot come.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use futures::{FutureExt as _, StreamExt as _};
use tokio::sync::{Semaphore, mpsc};

use super::observe::{Listed, observe_listed};
use super::output::BlockFacts;
use super::{
    Deferred, DescendOutcome, DirectoryWork, Shared, awaits_slot, descend_outcome, entry_candidate,
    entry_name, immediate_decision, throttling_scoped_to,
};
use crate::model::{EntryKind, EntryOperationFailure, Operation, StoragePath};
use crate::storage::listing::is_vanished;
use crate::storage::{DescentHint, ListRequest, ListingPage, SourceDescriptor, StorageRoleFailure};
use crate::traversal::{TraversalDecision, TraversalTerminalFailure};

/// Pages a listing may hand the driver before the cursor takes the first of them.
const PAGES_AHEAD: usize = 1;

/// Tells one started listing from another, including two listings of the same path.
pub(super) type ListingId = u64;

/// What a listing's task hands the driver.
pub(super) enum Arrival {
    /// One prepared page.
    Page(Page),
    /// The listing ran to its end.
    End,
    /// The listing stopped: this directory's own failure.
    Failed(EntryOperationFailure),
    /// The session is gone, or the task broke: the traversal ends.
    Terminal(TraversalTerminalFailure),
}

pub(super) type ArrivalSender = mpsc::UnboundedSender<(ListingId, Arrival)>;
pub(super) type Arrivals = mpsc::UnboundedReceiver<(ListingId, Arrival)>;

/// Where the cursor goes after a block: one subdirectory, known now or once a deferred decision
/// settles. Slots are gathered in listing order as pages arrive, and sorted by [`Slot::name`] once
/// the listing has ended.
pub(super) enum Slot {
    Descend(DirectoryWork),
    /// A deferred decision, keyed by the ticket this task gave the child. A ticket rather than
    /// the child's output sequence, because the slot is placed when the page is prepared, before
    /// the child is admitted and has a sequence. `path` is what the descent is sorted by; `hint`
    /// is what the decision's work is given if it descends.
    Pending {
        ticket: u64,
        path: StoragePath,
        hint: Option<DescentHint>,
    },
}

impl Slot {
    pub(super) fn path(&self) -> &StoragePath {
        match self {
            Self::Descend(work) => &work.path,
            Self::Pending { path, .. } => path,
        }
    }

    /// The subdirectory's name: the descent key. Every slot of one block shares the directory's
    /// path, so this orders them exactly as their raw UTF-8 names do.
    pub(super) fn name(&self) -> &str {
        entry_name(self.path())
    }
}

/// One listed child the output will show. `deferred` is set when its decision waits for an
/// observation that needs I/O; otherwise the decision was made when the page was prepared. A
/// child a decision made then hides never becomes one: it is only counted.
pub(super) enum Child {
    Observe {
        listed: Listed,
        deferred: Option<Deferred>,
    },
    /// Its decision was counted in page facts, but its diagnostics retain this child's place.
    Filtered(Vec<crate::traversal::ObservationFailure>),
}

impl Child {
    #[cfg(test)]
    pub(super) fn path_len(&self) -> usize {
        match self {
            Self::Observe { listed, .. } => listed.path_len(),
            Self::Filtered(failures) => failures
                .iter()
                .map(|failure| failure.path.as_str().len())
                .sum(),
        }
    }
}

/// What of one page the cursor admits, in this order: the children the listing could not
/// describe, then the described ones in listing order.
pub(super) struct Batch {
    pub(super) failures: Vec<EntryOperationFailure>,
    pub(super) children: VecDeque<Child>,
}

/// A prepared page.
pub(super) struct Page {
    pub(super) batch: Batch,
    /// Subdirectories to descend into, in the page's order.
    pub(super) slots: Vec<Slot>,
    /// What no output slot records: listed, vanished and hidden counts, and the decisions made
    /// here (filtered, pruned, truncated).
    pub(super) facts: BlockFacts,
}

/// Starts the task that lists `work`, and returns the credit the driver adds to each time the
/// cursor takes one of its pages.
pub(super) fn spawn(
    shared: &Arc<Shared>,
    id: ListingId,
    work: DirectoryWork,
    sender: &ArrivalSender,
) -> Arc<Semaphore> {
    let credit = Arc::new(Semaphore::new(PAGES_AHEAD));
    let reporter = Reporter {
        id,
        sender: sender.clone(),
        finished: false,
    };
    // Detached, never aborted: see the module docs.
    drop(tokio::spawn(read(
        Arc::clone(shared),
        work,
        Arc::clone(&credit),
        reporter,
    )));
    credit
}

/// An item of a listing stream.
type Item = Result<ListingPage, StorageRoleFailure>;

/// Reads `work`'s listing: room first, then a page, prepared, then handed over.
///
/// After preparing a page it takes a look, without waiting, at what follows: most directories
/// are one page, and a producer that has returned has its end visible at once (the listing
/// stream needs no channel slot for it), so the end reaches the driver right behind the page, in
/// the same wake-up rather than a second one. An end needs no room. A page found that way is
/// held, raw, until there is room for it: the one page beyond the handed-over one a stopped
/// listing may hold. What the look does not find is taken after waiting for room — an end then
/// arrives only once the cursor has taken the page before it.
async fn read(
    shared: Arc<Shared>,
    work: DirectoryWork,
    credit: Arc<Semaphore>,
    reporter: Reporter,
) {
    // The request carries the traversal's token: the backend stops paging at cancellation, and
    // the traversal is ending then, so what it returns is never used.
    let request = ListRequest::new(work.path.clone(), shared.request.cancel.clone())
        .with_descent_hint(work.hint.clone());
    let mut stream = shared
        .namespace
        .list_for_traversal(request, work.child_depth == 1);
    let mut peeked: Option<Option<Item>> = None;
    loop {
        let next = match peeked.take() {
            Some(end @ (None | Some(Err(_)))) => end,
            held => {
                if !room(&credit, &reporter).await {
                    return;
                }
                if let Some(item) = held {
                    item
                } else {
                    // Dropping the stream mid-request is safe (listing contract), so a traversal
                    // that ended does not wait for one more page to notice.
                    tokio::select! {
                        item = stream.next() => item,
                        () = reporter.sender.closed() => return,
                    }
                }
            }
        };
        let page = match next {
            None => return reporter.finish(Arrival::End),
            Some(Ok(page)) => page,
            Some(Err(failure)) => return reporter.finish(failed(&work.path, failure)),
        };
        let page = prepare(&shared, &work, page);
        peeked = stream.next().now_or_never();
        if !reporter.send(Arrival::Page(page)) {
            return;
        }
    }
}

/// Waits until the driver has room for one more page. `false` once the driver is gone.
async fn room(credit: &Semaphore, reporter: &Reporter) -> bool {
    let permit = tokio::select! {
        permit = credit.acquire() => permit,
        () = reporter.sender.closed() => return false,
    };
    let Ok(permit) = permit else { return false };
    // The driver hands the credit back when the cursor takes the page, not when this permit
    // drops.
    permit.forget();
    true
}

/// A listing's failure as the driver takes it. Throttling never ends a traversal (P7 §8): a
/// listing throttled past its budget is that listing's failure.
pub(super) fn failed(path: &StoragePath, failure: StorageRoleFailure) -> Arrival {
    match throttling_scoped_to(path, Operation::Traverse, failure) {
        StorageRoleFailure::Entry(failure) => Arrival::Failed(failure),
        StorageRoleFailure::Session(failure) => {
            Arrival::Terminal(TraversalTerminalFailure::Session(failure))
        }
    }
}

/// The task's line to the driver. Dropped without [`Reporter::finish`] — the task returned
/// early or unwound — it reports an `Internal` terminal failure, which a driver still waiting on
/// this listing must not miss; a driver that is gone simply never reads it.
struct Reporter {
    id: ListingId,
    sender: ArrivalSender,
    finished: bool,
}

impl Reporter {
    /// Hands over one arrival; `false` once the driver is gone.
    fn send(&self, arrival: Arrival) -> bool {
        self.sender.send((self.id, arrival)).is_ok()
    }

    /// Hands over the listing's last arrival.
    fn finish(mut self, arrival: Arrival) {
        self.finished = true;
        let _ = self.send(arrival);
    }
}

impl Drop for Reporter {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.send(Arrival::Terminal(TraversalTerminalFailure::Internal));
        }
    }
}

/// Turns one page into what the cursor admits: listing-level facts counted, every child's
/// admission decision made now, and every child that needs no I/O observed now.
pub(super) fn prepare(shared: &Shared, work: &DirectoryWork, page: ListingPage) -> Page {
    let internal_hidden = u64::from(page.internal_hidden);
    let (entries, failures) = page.into_hinted_descriptors(&work.path);
    let listed = u64::try_from(entries.len().saturating_add(failures.len())).unwrap_or(u64::MAX);
    // A child gone before the listing could describe it is counted, not reported (Q12).
    let (vanished, failures): (Vec<_>, Vec<_>) = failures
        .into_iter()
        .partition(|failure| is_vanished(&work.path, failure));
    let mut prepared = Page {
        batch: Batch {
            failures,
            children: VecDeque::with_capacity(entries.len()),
        },
        slots: Vec::new(),
        facts: BlockFacts {
            listed,
            vanished: u64::try_from(vanished.len()).unwrap_or(u64::MAX),
            internal_hidden,
            ..BlockFacts::default()
        },
    };
    for (descriptor, hint) in entries {
        prepare_child(shared, work, descriptor, hint, &mut prepared);
    }
    prepared
}

/// Decides and, when it needs no I/O, observes one child, and files it into the page.
///
/// Filter dependencies are resolved independently of observations requested only for output.
/// A dependency answered by the listing is decided before unrelated I/O; otherwise the full
/// observation settles before the decision. Each filtered child is decided exactly once.
fn prepare_child(
    shared: &Shared,
    work: &DirectoryWork,
    descriptor: SourceDescriptor,
    hint: Option<DescentHint>,
    page: &mut Page,
) {
    let kind = descriptor.kind;
    let plan = shared.policy.observation_plan;
    if let Some((decision, metadata)) = immediate_decision(shared, work, &descriptor) {
        let path = &descriptor.path;
        page.tally(shared, kind, path, work.child_depth, decision, hint);
        if decision.emit {
            let listed = observe_listed(shared.metadata.as_ref(), descriptor, plan);
            page.keep(listed, None);
        } else {
            page.hide_with_failures(super::output::metadata_failures(
                &descriptor.path,
                descriptor.version(),
                &metadata,
            ));
        }
        return;
    }
    let listed = observe_listed(shared.metadata.as_ref(), descriptor, plan);
    let Listed::Observed(Ok(entry)) = &listed else {
        return page.defer(shared, work, kind, listed, hint);
    };
    let decision = shared
        .policy
        .decide(true, &entry_candidate(&shared.request.root, entry));
    page.tally(shared, kind, entry.path(), work.child_depth, decision, hint);
    if decision.emit {
        page.keep(listed, None);
    } else {
        page.hide_with_failures(super::output::observation_failures(entry));
    }
}

/// The path a child whose decision waits is known by.
fn listed_path(listed: &Listed) -> Option<&StoragePath> {
    match listed {
        Listed::Unobserved(descriptor) => Some(&descriptor.path),
        Listed::Observed(Ok(entry)) => Some(entry.path()),
        Listed::Observed(Err(error)) => match &error.failure {
            StorageRoleFailure::Entry(failure) => Some(failure.path()),
            StorageRoleFailure::Session(_) => None,
        },
    }
}

impl Page {
    /// Files a child the output will show.
    fn keep(&mut self, listed: Listed, deferred: Option<Deferred>) {
        self.batch
            .children
            .push_back(Child::Observe { listed, deferred });
    }

    /// Counts a child a decision made here keeps out of the output.
    const fn hide(&mut self) {
        self.facts.filtered = self.facts.filtered.saturating_add(1);
    }

    fn hide_with_failures(&mut self, failures: Vec<crate::traversal::ObservationFailure>) {
        self.hide();
        if !failures.is_empty() {
            self.batch.children.push_back(Child::Filtered(failures));
        }
    }

    /// Files a child whose decision waits for its observation, and, when it is a directory the
    /// request may descend into, the `Pending` slot the decision will fill.
    fn defer(
        &mut self,
        shared: &Shared,
        work: &DirectoryWork,
        kind: EntryKind,
        listed: Listed,
        hint: Option<DescentHint>,
    ) {
        let mut deferred = Deferred {
            depth: work.child_depth,
            kind,
            ticket: None,
        };
        // A session failure has no path to sort by; it ends the traversal once it settles.
        if awaits_slot(&shared.request, deferred)
            && let Some(path) = listed_path(&listed)
        {
            let ticket = shared.tickets.fetch_add(1, Ordering::Relaxed);
            deferred.ticket = Some(ticket);
            self.slots.push(Slot::Pending {
                ticket,
                path: path.clone(),
                hint,
            });
        }
        self.keep(listed, Some(deferred));
    }

    /// Records where a decided child's subtree goes.
    fn tally(
        &mut self,
        shared: &Shared,
        kind: EntryKind,
        path: &StoragePath,
        depth: usize,
        decision: TraversalDecision,
        hint: Option<DescentHint>,
    ) {
        match descend_outcome(&shared.request, kind, path, depth, decision) {
            DescendOutcome::Into(below) => self.slots.push(Slot::Descend(below.with_hint(hint))),
            DescendOutcome::Pruned => self.facts.pruned = self.facts.pruned.saturating_add(1),
            DescendOutcome::Truncated => {
                self.facts.truncated = self.facts.truncated.saturating_add(1);
            }
            DescendOutcome::NotDirectory => {}
        }
    }
}
