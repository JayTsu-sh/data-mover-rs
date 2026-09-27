//! One Unordered worker: takes a directory, lists it page by page, hands each page's items over
//! as one batch, and pushes the subdirectories it found onto its own stack.
//!
//! A page is prepared exactly as a Blocks listing's task prepares it ([`stream::prepare`]): the
//! scan root's own object is taken out, children gone before they were described are counted,
//! every decision that needs no I/O is made and every child that needs none is observed, right
//! here. What needs I/O is observed by the page's own task ([`page_io`]). The batch keeps the
//! listing's order — the page's undescribable children first, then the others as listed — and a
//! directory's failure follows the children it delivered.
//!
//! **A directory's entry precedes its children's.** A page's batch is handed to the session
//! before the subdirectories it found are pushed, so no worker can list one of them before its
//! entry is in the session.
//!
//! **One key's segments together.** A key's history is one child of one page, and its segments
//! (P7 Q8) are tallied into the same batch; a batch reaches the session whole, so nothing else
//! lands between them.

use std::sync::Arc;

use futures::{FutureExt as _, StreamExt as _};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::super::observe::Listed;
use super::super::output::{ChildItem, count_listing, tally_child};
use super::super::stream::{self, Arrival, Batch, Child, Page, Slot};
use super::super::{Deferred, DirectoryWork, Resolved, Shared, resolve};
use super::page_io::{self, Job};
use super::pool::Pool;
use crate::model::{FailureClass, ObservedEntry};
use crate::storage::{DescentHint, ListRequest, ListingPage, ListingStream, StorageRoleFailure};
use crate::traversal::channel::ItemSender;
use crate::traversal::{
    BlockCounts, ScanFailure, ScanTotals, TraversalItem, TraversalTerminalFailure,
};

/// How one directory's processing ended.
enum Ended {
    /// Its listing ended and everything it found was handed over or pushed.
    Finished,
    /// The traversal halted first.
    Halted,
}

/// One listing in progress.
#[derive(Default)]
struct Listing {
    /// Children the listing returned so far, described or not: a failure's `delivered`.
    listed: u64,
}

/// An item of a listing stream.
type Item = Result<ListingPage, StorageRoleFailure>;

pub(super) struct Worker {
    pub(super) me: usize,
    pub(super) shared: Arc<Shared>,
    pub(super) pool: Arc<Pool>,
    pub(super) items: ItemSender,
    /// Fires when the traversal halts; this worker fires it when it finished the last directory
    /// or met a terminal failure.
    pub(super) halt: CancellationToken,
    /// This worker's child of `halt`, which every wait of this worker and its listings' requests
    /// watch: waiting on a token of its own keeps 64 workers (and their listings' producers) off
    /// one shared waiter list.
    pub(super) stop: CancellationToken,
    /// Observations that need I/O in flight at once, across every worker.
    pub(super) budget: Arc<Semaphore>,
    /// What this worker counted of the children it handled.
    pub(super) block: BlockCounts,
    /// What it counted of the listings themselves.
    pub(super) totals: ScanTotals,
}

impl Worker {
    /// Works until the traversal halts, and returns what this worker counted.
    pub(super) async fn run(mut self) -> ScanTotals {
        while let Some(work) = self.pool.take(self.me, &self.stop).await {
            match self.directory(&work).await {
                Ok(Ended::Finished) => {
                    if self.pool.finish(self.me) {
                        // Nothing is left anywhere: every worker can stop.
                        self.halt.cancel();
                    } else {
                        self.checkpoint_if_due().await;
                    }
                }
                Ok(Ended::Halted) => break,
                Err(failure) => {
                    self.pool.fail(failure);
                    self.halt.cancel();
                    break;
                }
            }
        }
        let mut totals = self.totals;
        totals.absorb(&self.block);
        totals
    }

    /// Lists `work` to its end.
    async fn directory(&mut self, work: &DirectoryWork) -> Result<Ended, TraversalTerminalFailure> {
        // The halt token stops the backend's paging too: a traversal that ended for any reason
        // tears its listings down.
        let request = ListRequest::new(work.path.clone(), self.stop.clone())
            .with_descent_hint(work.hint.clone());
        let mut stream = self.shared.namespace.list(request);
        let mut listing = Listing::default();
        loop {
            let Some(next) = self.next_item(&mut stream).await else {
                return Ok(Ended::Halted);
            };
            // A listing ends `Cancelled` only once the token fired, and then its end is not a
            // failure of the directory (R10).
            if self.stop.is_cancelled() {
                return Ok(Ended::Halted);
            }
            match next {
                Some(Ok(page)) => {
                    if !self.page(work, page, &mut listing).await? {
                        return Ok(Ended::Halted);
                    }
                }
                None => return self.end(work, listing, None).await,
                Some(Err(failure)) => return self.end(work, listing, Some(failure)).await,
            }
        }
    }

    /// The listing's next item, taken without waiting when it is already there — most directories
    /// are one page, so their end is, right behind it — else waited for. `None` once the
    /// traversal halted first.
    async fn next_item(&self, stream: &mut ListingStream) -> Option<Option<Item>> {
        if let Some(item) = stream.next().now_or_never() {
            return Some(item);
        }
        tokio::select! {
            biased;
            () = self.stop.cancelled() => None,
            item = stream.next() => Some(item),
        }
    }

    /// Hands one page over and pushes the subdirectories it found. `false` once the traversal
    /// halted first.
    async fn page(
        &mut self,
        work: &DirectoryWork,
        page: ListingPage,
        listing: &mut Listing,
    ) -> Result<bool, TraversalTerminalFailure> {
        let Page {
            batch: Batch { failures, children },
            slots,
            facts,
        } = stream::prepare(&self.shared, work, page);
        facts.count_into(&mut self.block);
        listing.listed = listing.listed.saturating_add(facts.listed);
        let Some(settled) = self.observe(children).await? else {
            return Ok(false);
        };
        let (mut below, hints) = split_slots(slots);
        // The page's undescribable children first, then the rest in listing order.
        let mut batch = Vec::with_capacity(failures.len() + settled.len());
        for failure in failures {
            let item = ChildItem::Item(TraversalItem::Failure(Box::new(ScanFailure::child(
                failure,
            ))));
            let segment = self.shared.history_segment;
            tally_child(&mut self.block, item, None, segment).push_into(&mut batch);
        }
        self.settle(settled, hints, &mut batch, &mut below)?;
        if !self.send(batch).await? {
            return Ok(false);
        }
        // Only now: the entries of these directories are in the session.
        self.pool.push(self.me, below);
        self.checkpoint_if_due().await;
        Ok(true)
    }

    /// Sends a checkpoint when one is due and this worker claims it. Off, this is one branch.
    /// Halted meanwhile, the checkpoint is dropped: the traversal's final one covers it.
    async fn checkpoint_if_due(&self) {
        let Some(resume) = self.shared.resume.as_ref() else {
            return;
        };
        if !resume.claim_due() {
            return;
        }
        let Some(item) = super::checkpoint(resume, &self.pool) else {
            return;
        };
        tokio::select! {
            biased;
            () = self.stop.cancelled() => {}
            _ = self.items.send_whole(vec![item]) => {}
        }
    }

    /// Resolves every child's observation into `batch`, and the subdirectories a deferred decision
    /// descends into into `below`, with the hints their listing gave them.
    fn settle(
        &mut self,
        settled: Vec<Settled>,
        mut hints: Vec<(u64, Option<DescentHint>)>,
        batch: &mut Vec<TraversalItem>,
        below: &mut Vec<DirectoryWork>,
    ) -> Result<(), TraversalTerminalFailure> {
        for (result, deferred) in settled {
            let Resolved {
                item,
                descent,
                below: descend,
            } = resolve(&self.shared, result, deferred)?;
            if let Some(descend) = descend {
                let hint = deferred
                    .and_then(|deferred| deferred.ticket)
                    .and_then(|ticket| take_hint(&mut hints, ticket));
                below.push(descend.with_hint(hint));
            }
            let segment = self.shared.history_segment;
            tally_child(&mut self.block, item, descent, segment).push_into(batch);
        }
        Ok(())
    }

    /// Every child's observation in listing order: the ones the page already made, and the ones
    /// that need I/O, made now. `None` once the traversal halted first.
    async fn observe(
        &self,
        children: impl IntoIterator<Item = Child>,
    ) -> Result<Option<Vec<Settled>>, TraversalTerminalFailure> {
        let mut settled = Vec::new();
        let mut jobs: Vec<Job> = Vec::new();
        for (place, Child { listed, deferred }) in children.into_iter().enumerate() {
            match listed {
                Listed::Observed(result) => settled.push((Some(result), deferred)),
                Listed::Unobserved(descriptor) => {
                    settled.push((None, deferred));
                    jobs.push((place, descriptor));
                }
            }
        }
        if !jobs.is_empty() {
            let expected = jobs.len();
            let done = page_io::observe_page(&self.shared, &self.budget, &self.stop, jobs).await?;
            let Some(done) = done else { return Ok(None) };
            if done.len() != expected {
                // Observations are skipped only once the traversal halted; anything else short
                // is a defect, which must end the traversal rather than leave it waiting.
                return if self.stop.is_cancelled() {
                    Ok(None)
                } else {
                    Err(TraversalTerminalFailure::Internal)
                };
            }
            for (place, result) in done {
                if let Some(slot) = settled.get_mut(place) {
                    slot.0 = Some(result);
                }
            }
        }
        settled
            .into_iter()
            .map(|(result, deferred)| {
                result
                    .map(|result| (result, deferred))
                    .ok_or(TraversalTerminalFailure::Internal)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    /// Ends the listing: hands over its failure, when it failed. A subdirectory gone before its
    /// listing returned anything is a complete, empty listing and no failure (Q12); the traversal
    /// root never is, so a mistyped root cannot read as an empty source.
    async fn end(
        &mut self,
        work: &DirectoryWork,
        listing: Listing,
        failure: Option<StorageRoleFailure>,
    ) -> Result<Ended, TraversalTerminalFailure> {
        let Listing { listed } = listing;
        let mut last = Vec::new();
        if let Some(failure) = failure {
            match stream::failed(&work.path, failure) {
                Arrival::Failed(failure)
                    if failure.class() == FailureClass::NotFound
                        && work.child_depth > 1
                        && listed == 0 =>
                {
                    self.totals.vanished_listings = self.totals.vanished_listings.saturating_add(1);
                }
                Arrival::Failed(failure) => {
                    let failure = ScanFailure::listing(work.path.clone(), failure, listed);
                    if let Some(outcome) = failure.listing_outcome() {
                        count_listing(&mut self.totals, outcome);
                    }
                    last.push(TraversalItem::Failure(Box::new(failure)));
                }
                Arrival::Terminal(failure) => return Err(failure),
                Arrival::Page(_) | Arrival::End => return Err(TraversalTerminalFailure::Internal),
            }
        }
        Ok(if self.send(last).await? {
            Ended::Finished
        } else {
            Ended::Halted
        })
    }

    /// Hands `batch` to the session. `false` once the traversal halted first.
    ///
    /// A consumer that dropped the session is noticed even when there is nothing to send (a filter
    /// that hides everything), so the traversal stops listing rather than walk on.
    async fn send(&self, batch: Vec<TraversalItem>) -> Result<bool, TraversalTerminalFailure> {
        if batch.is_empty() {
            return if self.items.is_closed() {
                Err(TraversalTerminalFailure::Internal)
            } else {
                Ok(true)
            };
        }
        tokio::select! {
            biased;
            () = self.stop.cancelled() => Ok(false),
            sent = self.items.send_whole(batch) => sent
                .map(|()| true)
                .map_err(|_| TraversalTerminalFailure::Internal),
        }
    }
}

/// One child's observation and the deferred decision it settles, if any.
type Settled = (
    Result<Box<ObservedEntry>, StorageRoleFailure>,
    Option<Deferred>,
);

/// The subdirectories a page decided to descend into, and the hints of the ones whose decision
/// waited for an observation, by ticket.
fn split_slots(slots: Vec<Slot>) -> (Vec<DirectoryWork>, Vec<(u64, Option<DescentHint>)>) {
    let mut below = Vec::with_capacity(slots.len());
    let mut hints = Vec::new();
    for slot in slots {
        match slot {
            Slot::Descend(work) => below.push(work),
            Slot::Pending { ticket, hint, .. } => hints.push((ticket, hint)),
        }
    }
    (below, hints)
}

fn take_hint(hints: &mut [(u64, Option<DescentHint>)], ticket: u64) -> Option<DescentHint> {
    hints
        .iter_mut()
        .find(|(known, _)| *known == ticket)
        .and_then(|(_, hint)| hint.take())
}
