//! Resuming Blocks (P7 §9.2): each ancestor on the checkpoint's chain is listed again, emitting
//! nothing, to learn which of its subdirectories are still to be descended into — the ones named
//! after the next link of the chain, in the byte order Blocks descends in.
//!
//! The listing is prepared exactly as a Blocks listing's task prepares it ([`stream::prepare`]), so
//! the filter, `max_depth` and the descent hints decide what they decided the first time; a
//! subdirectory whose decision waits for an observation that needs I/O is observed here, one at a
//! time (a one-off cost of a resume, and only with a filter that needs `modified`).
//!
//! Its block was delivered before the checkpoint, so none of it is delivered again, and nothing
//! is counted but a listing that failed or found the directory gone: a failure is reported as the
//! directory's [`FailureScope::Listing`](crate::traversal::FailureScope::Listing) failure (its
//! subtree takes no part in deletion), and a directory that is gone has nothing left below it.

use futures::StreamExt as _;

use super::descent_name;
use crate::model::FailureClass;
use crate::storage::{DescentHint, ListRequest, ListingPage};
use crate::traversal::storage::observe::{Listed, observe};
use crate::traversal::storage::stream::{self, Arrival, Child, Page, Slot};
use crate::traversal::storage::{DirectoryWork, Resolved, Shared, resolve};
use crate::traversal::{ScanFailure, TraversalTerminalFailure};

/// What listing one ancestor again found.
#[derive(Default)]
pub(crate) struct Relisted {
    /// Subdirectories still to descend into, in descent order.
    pub(crate) works: Vec<DirectoryWork>,
    /// The listing stopped: the directory's own failure.
    pub(crate) failure: Option<ScanFailure>,
    /// The directory was gone before its listing returned anything.
    pub(crate) vanished: bool,
}

/// Lists `work` again and keeps its subdirectories named after `after`. A cancelled traversal
/// returns what it had; the caller notices the cancellation.
///
/// # Errors
/// A session failure, which ends the traversal.
pub(crate) async fn remaining(
    shared: &Shared,
    work: &DirectoryWork,
    after: &str,
) -> Result<Relisted, TraversalTerminalFailure> {
    let request = ListRequest::new(work.path.clone(), shared.request.cancel.clone())
        .with_descent_hint(work.hint.clone());
    let mut stream = shared.namespace.list(request);
    let mut relisted = Relisted::default();
    let mut listed: u64 = 0;
    while let Some(item) = stream.next().await {
        if shared.request.cancel.is_cancelled() {
            break;
        }
        match item {
            Ok(page) => {
                listed = listed.saturating_add(
                    keep_page(shared, work, page, after, &mut relisted.works).await?,
                );
            }
            Err(failure) => {
                end(
                    work,
                    stream::failed(&work.path, failure),
                    listed,
                    &mut relisted,
                )?;
                break;
            }
        }
    }
    relisted
        .works
        .sort_unstable_by(|left, right| descent_name(&left.path).cmp(descent_name(&right.path)));
    relisted
        .works
        .dedup_by(|later, earlier| later.path == earlier.path);
    Ok(relisted)
}

/// How the listing's failure ends the relisting.
fn end(
    work: &DirectoryWork,
    arrival: Arrival,
    listed: u64,
    relisted: &mut Relisted,
) -> Result<(), TraversalTerminalFailure> {
    match arrival {
        Arrival::Failed(failure)
            if failure.class() == FailureClass::NotFound && work.child_depth > 1 && listed == 0 =>
        {
            relisted.vanished = true;
        }
        Arrival::Failed(failure) => {
            relisted.failure = Some(ScanFailure::listing(work.path.clone(), failure, listed));
        }
        Arrival::Terminal(failure) => return Err(failure),
        Arrival::Page(_) | Arrival::End => return Err(TraversalTerminalFailure::Internal),
    }
    Ok(())
}

/// Keeps one page's subdirectories named after `after`, deciding the ones that wait for an
/// observation now. Returns how many children the page listed.
async fn keep_page(
    shared: &Shared,
    work: &DirectoryWork,
    page: ListingPage,
    after: &str,
    works: &mut Vec<DirectoryWork>,
) -> Result<u64, TraversalTerminalFailure> {
    let Page {
        batch,
        slots,
        facts,
    } = stream::prepare(shared, work, page);
    let mut keep = |below: DirectoryWork| {
        if descent_name(&below.path) > after {
            works.push(below);
        }
    };
    let mut pending = Vec::new();
    for slot in slots {
        match slot {
            Slot::Descend(below) => keep(below),
            Slot::Pending { ticket, hint, .. } => pending.push((ticket, hint)),
        }
    }
    for child in batch.children {
        if let Some(below) = decide_pending(shared, child, &mut pending).await? {
            keep(below);
        }
    }
    Ok(facts.listed)
}

/// The subdirectory a child whose descent waited for its observation descends into, if any,
/// observing it now when that needs I/O. `None` for a child with no pending slot.
async fn decide_pending(
    shared: &Shared,
    child: Child,
    pending: &mut Vec<(u64, Option<DescentHint>)>,
) -> Result<Option<DirectoryWork>, TraversalTerminalFailure> {
    let Child { listed, deferred } = child;
    let Some(ticket) = deferred.and_then(|deferred| deferred.ticket) else {
        return Ok(None);
    };
    let Some(at) = pending.iter().position(|(known, _)| *known == ticket) else {
        return Ok(None);
    };
    let (_, hint) = pending.swap_remove(at);
    let result = match listed {
        Listed::Observed(result) => result,
        Listed::Unobserved(descriptor) => {
            let plan = shared.policy.observation_plan;
            observe(
                shared.namespace.as_ref(),
                shared.metadata.as_ref(),
                *descriptor,
                plan,
                &shared.request.cancel,
            )
            .await
            .map(Box::new)
        }
    };
    let Resolved { below, .. } = resolve(shared, result, deferred)?;
    Ok(below.map(|below| below.with_hint(hint)))
}
