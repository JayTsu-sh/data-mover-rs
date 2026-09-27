//! The Blocks driver ([`TraversalOrder::Blocks`], P7 §3, §6.2): advances the admission
//! [cursor](super::cursor), emits what the reorder buffer releases, and waits for whatever the
//! cursor is blocked on — a listing's next page or end, or an observation.
//!
//! [`TraversalOrder::Blocks`]: crate::traversal::TraversalOrder::Blocks

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use super::cursor::{Cursor, Decisions, Step};
use super::observe::{Feed, Job, Listed, Observations, Observed};
use super::output::{BlockFacts, ChildItem, Output, Settled};
use super::stream::{self, Arrivals};
use super::{Probe, Resolved, Shared, resolve, root_work};
use crate::model::StoragePath;
use crate::storage::{Metadata, Namespace};
use crate::traversal::channel::ItemSender;
use crate::traversal::{
    ScanFailure, TraversalCompletion, TraversalItem, TraversalOutcome, TraversalRequest,
    TraversalTerminalFailure,
};

/// Output sequencing: every admitted entry and every queued listing failure owns one sequence
/// number, assigned in output order by the cursor. Allocation lives here; releasing the slots
/// in that order is [`Output`]'s job.
pub(super) struct State {
    next_sequence: u64,
    output: Output,
}

impl State {
    pub(super) const fn new() -> Self {
        Self {
            next_sequence: 0,
            output: Output::new(),
        }
    }

    /// Entries admitted but not yet emitted: in flight, or settled and waiting in the reorder
    /// buffer behind an earlier one. This, not the in-flight count alone, is what the admission
    /// window bounds; otherwise one slow observation lets the reorder buffer grow without limit.
    pub(super) fn admitted(&self) -> usize {
        // Saturating, not raw: were the invariant that output never runs ahead of allocation
        // ever to break, a wrapped `u64` would read as `usize::MAX`, hold the window shut for
        // good, and spin `drive` through its `WindowFull` arm, which has no await point.
        usize::try_from(self.next_sequence.saturating_sub(self.output.next())).unwrap_or(usize::MAX)
    }

    pub(super) fn allocate(&mut self) -> Result<u64, TraversalTerminalFailure> {
        let sequence = self.next_sequence;
        self.next_sequence = sequence
            .checked_add(1)
            .ok_or(TraversalTerminalFailure::Internal)?;
        Ok(sequence)
    }
}

pub(super) struct Runtime<'a> {
    pub(super) shared: &'a Arc<Shared>,
    pub(super) request: &'a TraversalRequest,
    pub(super) items: &'a ItemSender,
}

pub(super) async fn run(
    namespace: Arc<dyn Namespace>,
    metadata: Arc<dyn Metadata>,
    request: TraversalRequest,
    items: ItemSender,
    completion: oneshot::Sender<Result<TraversalOutcome, TraversalTerminalFailure>>,
    probe: Probe,
) {
    let mut state = State::new();
    let (sender, mut arrivals) = mpsc::unbounded_channel();
    let mut cursor = Cursor::new(root_work(&request), sender);
    let mut observations = Observations::new();
    let shared = Arc::new(Shared::new(namespace, metadata, request));
    let runtime = Runtime {
        shared: &shared,
        request: &shared.request,
        items: &items,
    };
    let result = drive(
        &runtime,
        &mut cursor,
        &mut arrivals,
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
    // which is what lets each observation task end once it has drained; dropping the arrivals
    // tells every listing's task the traversal is gone, and each drops its stream, which ends
    // the backend's producer through its own close path.
    observations.detach();
    drop(cursor);
    drop(arrivals);
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
    arrivals: &mut Arrivals,
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
        // Takes every page that arrived meanwhile, not only one the cursor may be waiting for:
        // a prefetched directory's subdirectories are only known, and prefetched in turn, once
        // its pages have been taken from its task. Bounded: each listing hands over at most one
        // page per page the cursor took, and one end.
        while let Ok((id, arrival)) = arrivals.try_recv() {
            cursor.listed(id, arrival)?;
        }
        // A listing ends `Cancelled` only once the token fired (the request carries it), so
        // checking again here keeps such a listing from being admitted as a failure (R10).
        if runtime.request.cancel.is_cancelled() {
            return Ok(());
        }
        let step = cursor.advance(runtime, observations, state)?;
        cursor.start_listings(runtime);
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
            _ => wait(runtime, cursor, arrivals, observations, state).await?,
        }
    }
}

/// Waits for a listing's next page or end, or some observations to finish, or for
/// cancellation. Observations that arrived together are settled together, so a burst costs the
/// driver one pass.
async fn wait(
    runtime: &Runtime<'_>,
    cursor: &mut Cursor,
    arrivals: &mut Arrivals,
    observations: &mut Observations,
    state: &mut State,
) -> Result<(), TraversalTerminalFailure> {
    let listening = cursor.has_live_listings();
    tokio::select! {
        biased;
        () = runtime.request.cancel.cancelled() => Ok(()),
        // The cursor keeps a sender, so the channel never closes while it waits.
        Some((id, arrival)) = arrivals.recv(), if listening => cursor.listed(id, arrival),
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

/// Admits one listed child into its output slot: settles it at once when its listing's task
/// already observed it, or hands it to its block's observation task. A child a decision made in
/// the listing's task hides was only counted there, and never gets here; a deferred one already
/// has its descend slot, placed in the block's order when the listing was prepared.
pub(super) fn admit(
    runtime: &Runtime<'_>,
    feed: &mut Option<Feed>,
    child: stream::Child,
    decisions: &mut Decisions,
    observations: &mut Observations,
    state: &mut State,
) -> Result<(), TraversalTerminalFailure> {
    let stream::Child { listed, deferred } = child;
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

/// Puts one observation into its output slot, and records the descend decision a deferred one
/// makes.
fn settle(
    runtime: &Runtime<'_>,
    decisions: &mut Decisions,
    state: &mut State,
    observed: Observed,
) -> Result<(), TraversalTerminalFailure> {
    let (sequence, result, deferred) = observed;
    let Resolved {
        item,
        descent,
        below,
    } = resolve(runtime.shared, result, deferred)?;
    if let Some(ticket) = deferred.and_then(|deferred| deferred.ticket) {
        decisions.insert(ticket, below);
    }
    state
        .output
        .settle(sequence, Settled::Child { item, descent });
    Ok(())
}

/// Queues a failure the listing itself produced: a child it could not describe, or the
/// directory's own listing failure.
pub(super) fn queue_failure(
    state: &mut State,
    failure: ScanFailure,
) -> Result<(), TraversalTerminalFailure> {
    let sequence = state.allocate()?;
    state.output.settle(
        sequence,
        Settled::Child {
            item: ChildItem::Item(TraversalItem::Failure(Box::new(failure))),
            descent: None,
        },
    );
    Ok(())
}

/// Queues the marker that closes one directory's block.
pub(super) fn queue_block_end(
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
