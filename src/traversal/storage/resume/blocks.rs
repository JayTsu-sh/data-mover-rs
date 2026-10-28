//! Checkpoints of a Blocks traversal (P7 §9.2): where its cursor starts, and when and what it
//! checkpoints.
//!
//! - **Start.** At the root; or, resuming, at the checkpoint's position: every ancestor on the
//!   chain is listed again, emitting nothing ([`relist`](super::relist)), to learn the
//!   subdirectories it still descends into; the last directory on the chain is listed whole. An
//!   ancestor's newly encountered child or listing failures are reported on their own and first,
//!   never inside a block (the block it would land in is the next directory's, whose outcome must
//!   stay its own). Cancelled while it relists, the traversal ends at once,
//!   and its final checkpoint is the one it resumed from.
//! - **Periodic.** After each flush, the positions recorded before what has been delivered are
//!   dropped but the latest, and, when a checkpoint is due and the last item delivered closed a
//!   block, that latest one is sent: everything after the position it names comes again from it,
//!   and everything before it was delivered. A block is never split by a periodic checkpoint.
//!   Cancellation winning the race drops it; the final one covers it.
//! - **Final.** A traversal that did not complete sends the latest position before what it
//!   delivered as its last item, not racing cancellation: a resumable session drains.

use std::sync::Arc;

use super::positions::{Level, Positions};
use super::{Resume, descent_name, relist, work_of};
use crate::traversal::checkpoint::FrontierEntry;
use crate::traversal::storage::blocks::{Runtime, State};
use crate::traversal::storage::cursor::Cursor;
use crate::traversal::storage::output::Output;
use crate::traversal::storage::root_work;
use crate::traversal::storage::stream::ArrivalSender;
use crate::traversal::{ObservationFailure, ScanFailure, TraversalItem, TraversalTerminalFailure};

/// The cursor a Blocks traversal drives: at the root, or at the position it resumes from.
///
/// # Errors
/// A session failure met while listing an ancestor again.
pub(crate) async fn start(
    runtime: &Runtime<'_>,
    state: &mut State,
    sender: ArrivalSender,
) -> Result<Cursor, TraversalTerminalFailure> {
    let resume = runtime.shared.resume.as_ref();
    let record = resume.is_some_and(Resume::emits);
    let Some(chain) = resume.and_then(|resume| resume.start.as_deref()) else {
        return Ok(Cursor::new(root_work(runtime.request), sender, record));
    };
    let levels = record.then(|| levels_of(chain));
    let level = |index: usize| {
        levels
            .as_ref()
            .and_then(|levels| levels.get(index))
            .cloned()
    };
    let positions = record.then(|| {
        let mut positions = Positions::default();
        positions.record(0, chain.len().checked_sub(1).and_then(level));
        positions
    });
    let Some((top, ancestors)) = chain.split_last() else {
        return Ok(Cursor::resumed(sender, Vec::new(), None, positions));
    };
    let mut frames = Vec::with_capacity(ancestors.len());
    for (index, (entry, next)) in ancestors.iter().zip(chain.iter().skip(1)).enumerate() {
        let work = work_of(entry);
        let relisted = relist::remaining(runtime.shared, &work, descent_name(&next.path)).await;
        if runtime.request.cancel.is_cancelled() {
            // Nothing is driven any more; the recorded position is the one resumed from.
            return Ok(Cursor::resumed(sender, Vec::new(), None, positions));
        }
        state
            .output
            .count_resumed(&relisted.failures, u64::from(relisted.vanished));
        deliver_failures(runtime, relisted.failures, relisted.observation_failures).await?;
        if let Some(failure) = relisted.terminal {
            return Err(failure);
        }
        frames.push((work, relisted.works, level(index)));
    }
    let top = (work_of(top), level(ancestors.len()));
    Ok(Cursor::resumed(sender, frames, Some(top), positions))
}

/// The chain's levels, each linked to the one before.
fn levels_of(chain: &[FrontierEntry]) -> Vec<Arc<Level>> {
    let mut levels: Vec<Arc<Level>> = Vec::with_capacity(chain.len());
    for entry in chain {
        let level = Level::enter(levels.last(), &work_of(entry));
        levels.push(level);
    }
    levels
}

/// Sends the resumed ancestors' child and listing failures, ahead of anything else.
async fn deliver_failures(
    runtime: &Runtime<'_>,
    failures: Vec<ScanFailure>,
    observations: Vec<ObservationFailure>,
) -> Result<(), TraversalTerminalFailure> {
    if failures.is_empty() && observations.is_empty() {
        return Ok(());
    }
    let batch = failures
        .into_iter()
        .map(|failure| TraversalItem::Failure(Box::new(failure)))
        .chain(
            observations
                .into_iter()
                .map(|failure| TraversalItem::ObservationFailure(Box::new(failure))),
        )
        .collect();
    tokio::select! {
        biased;
        () = runtime.request.cancel.cancelled() => Ok(()),
        sent = runtime.items.send(batch) => sent.map_err(|_| TraversalTerminalFailure::Internal),
    }
}

/// After a flush: forgets positions no checkpoint will use, and sends one when it is due and the
/// output is between two blocks — never inside a block, whose items are delivered contiguously,
/// nor between the segments of one key's history. Off, this is one branch.
pub(crate) async fn checkpoint_if_due(runtime: &Runtime<'_>, cursor: &mut Cursor, output: &Output) {
    let (Some(resume), Some(positions)) = (runtime.shared.resume.as_ref(), cursor.positions_mut())
    else {
        return;
    };
    let delivered = output.delivered();
    positions.prune(delivered);
    if !output.between_blocks() || !resume.claim_due() {
        return;
    }
    let Some(item) = positions
        .latest(delivered)
        .and_then(|chain| resume.checkpoint(&chain))
    else {
        return;
    };
    tokio::select! {
        biased;
        () = runtime.request.cancel.cancelled() => {}
        _ = runtime.items.send(vec![item]) => {}
    }
}

/// The last item of a traversal that did not complete: the latest position before what it
/// delivered, or, when it never got to drive its cursor, the one it resumed from.
pub(crate) async fn final_checkpoint(
    runtime: &Runtime<'_>,
    positions: Option<&Positions>,
    delivered: u64,
) {
    let Some(resume) = runtime
        .shared
        .resume
        .as_ref()
        .filter(|resume| resume.emits())
    else {
        return;
    };
    let chain = match positions {
        Some(positions) => positions.latest(delivered),
        None => resume.start.clone(),
    };
    if let Some(item) = chain.and_then(|chain| resume.checkpoint(&chain)) {
        // Not racing cancellation: a resumable session drains, and a consumer that dropped it
        // makes this fail at once.
        let _ = runtime.items.send(vec![item]).await;
    }
}
