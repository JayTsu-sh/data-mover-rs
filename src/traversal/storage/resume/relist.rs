//! Resuming Blocks (P7 §9.2): each ancestor on the checkpoint's chain is listed again, emitting
//! nothing, to learn which of its subdirectories are still to be descended into — the ones named
//! after the next link of the chain, in the byte order Blocks descends in.
//!
//! The listing is prepared exactly as a Blocks listing's task prepares it ([`stream::prepare`]), so
//! the filter, `max_depth` and the descent hints decide what they decided the first time; a
//! subdirectory whose decision waits for an observation that needs I/O is observed here, one at a
//! time (a one-off cost of a resume, and only with a filter that needs `modified`).
//!
//! Its block was delivered before the checkpoint, so successful children are not delivered again.
//! Newly encountered child and listing failures are reported and counted; a listing failure
//! excludes the directory's subtree from deletion. A directory found gone has nothing left below
//! it. Session failures still end the traversal, even when the child has no descent slot.

use futures::StreamExt as _;

use super::descent_name;
use crate::model::FailureClass;
use crate::storage::{DescentHint, ListRequest, ListingPage};
use crate::traversal::storage::observe::{Listed, observe};
use crate::traversal::storage::output::{ChildItem, observation_failures};
use crate::traversal::storage::stream::{self, Arrival, Child, Page, Slot};
use crate::traversal::storage::{DirectoryWork, Resolved, Shared, resolve};
use crate::traversal::{ObservationFailure, ScanFailure, TraversalItem, TraversalTerminalFailure};

/// What listing one ancestor again found.
#[derive(Default)]
pub(crate) struct Relisted {
    /// Subdirectories still to descend into, in descent order.
    pub(crate) works: Vec<DirectoryWork>,
    /// Newly encountered child failures, followed by the directory's own listing failure.
    pub(crate) failures: Vec<ScanFailure>,
    /// Metadata errors do not replace an entry or affect child/deletion accounting.
    pub(crate) observation_failures: Vec<ObservationFailure>,
    /// The directory was gone before its listing returned anything.
    pub(crate) vanished: bool,
    /// Stops the session after failures already encountered here have been delivered.
    pub(crate) terminal: Option<TraversalTerminalFailure>,
}

/// Lists `work` again and keeps its subdirectories named after `after`. A cancelled traversal
/// returns what it had; the caller notices the cancellation.
pub(crate) async fn remaining(shared: &Shared, work: &DirectoryWork, after: &str) -> Relisted {
    let request = ListRequest::new(work.path.clone(), shared.request.cancel.clone())
        .with_descent_hint(work.hint.clone());
    let mut stream = shared
        .namespace
        .list_for_traversal(request, work.child_depth == 1);
    let mut relisted = Relisted::default();
    let mut listed: u64 = 0;
    while let Some(item) = stream.next().await {
        if shared.request.cancel.is_cancelled() {
            break;
        }
        let ended = item.is_err();
        let result = match item {
            Ok(page) => keep_page(shared, work, page, after, &mut relisted)
                .await
                .map(|count| listed = listed.saturating_add(count)),
            Err(failure) => end(
                work,
                stream::failed(&work.path, failure),
                listed,
                &mut relisted,
            ),
        };
        if let Err(failure) = result {
            relisted.terminal = Some(failure);
            break;
        }
        if ended {
            break;
        }
    }
    relisted
        .works
        .sort_unstable_by(|left, right| descent_name(&left.path).cmp(descent_name(&right.path)));
    relisted
        .works
        .dedup_by(|later, earlier| later.path == earlier.path);
    relisted
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
            relisted
                .failures
                .push(ScanFailure::listing(work.path.clone(), failure, listed));
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
    relisted: &mut Relisted,
) -> Result<u64, TraversalTerminalFailure> {
    let Page {
        batch,
        slots,
        facts,
    } = stream::prepare(shared, work, page);
    let mut keep = |below: DirectoryWork| {
        if descent_name(&below.path) > after {
            relisted.works.push(below);
        }
    };
    relisted
        .failures
        .extend(batch.failures.into_iter().map(ScanFailure::child));
    let mut pending = Vec::new();
    for slot in slots {
        match slot {
            Slot::Descend(below) => keep(below),
            Slot::Pending { ticket, hint, .. } => pending.push((ticket, hint)),
        }
    }
    for child in batch.children {
        if let Some(below) = decide_pending(
            shared,
            child,
            &mut pending,
            &mut relisted.failures,
            &mut relisted.observation_failures,
        )
        .await?
        {
            keep(below);
        }
    }
    Ok(facts.listed)
}

/// Resolves pending descent and retains any already observed failure, including children with no
/// pending slot. Children needed only for the previous block's output cause no additional I/O.
async fn decide_pending(
    shared: &Shared,
    child: Child,
    pending: &mut Vec<(u64, Option<DescentHint>)>,
    failures: &mut Vec<ScanFailure>,
    observations: &mut Vec<ObservationFailure>,
) -> Result<Option<DirectoryWork>, TraversalTerminalFailure> {
    let (listed, deferred) = match child {
        Child::Observe { listed, deferred } => (listed, deferred),
        Child::Filtered(failures) => {
            observations.extend(failures);
            return Ok(None);
        }
    };
    let hint = deferred
        .and_then(|deferred| deferred.ticket)
        .and_then(|ticket| pending.iter().position(|(known, _)| *known == ticket))
        .map(|at| pending.swap_remove(at).1);
    let result = match listed {
        // Ready failures have happened even without a descent slot; session failures never
        // get a slot because they have no child's path. Do not repeat output-only I/O.
        Listed::Observed(result) => result,
        Listed::Unobserved(_) if hint.is_none() => return Ok(None),
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
    if let Ok(entry) = &result {
        observations.extend(observation_failures(entry));
        if hint.is_none() {
            return Ok(None);
        }
    }
    let resolved = match resolve(shared, result, deferred) {
        Ok(resolved) => resolved,
        Err(error) => {
            observations.extend(error.observations);
            return Err(error.terminal);
        }
    };
    let Resolved { item, below, .. } = resolved;
    remember_failure(item, failures, observations);
    Ok(below.map(|below| below.with_hint(hint.flatten())))
}

fn remember_failure(
    item: ChildItem,
    failures: &mut Vec<ScanFailure>,
    observations: &mut Vec<ObservationFailure>,
) {
    match item {
        ChildItem::Item(TraversalItem::Failure(failure)) => failures.push(*failure),
        ChildItem::ObservedFailure {
            failure,
            observations: details,
        } => {
            failures.push(*failure);
            observations.extend(details);
        }
        ChildItem::Vanished(details) => observations.extend(details),
        _ => {}
    }
}
