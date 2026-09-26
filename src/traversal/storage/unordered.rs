//! The Unordered traversal ([`TraversalOrder::Unordered`], P7 §3, §6.1): `K` workers, each
//! listing one directory at a time and handing its pages straight to the session, with no
//! sequence numbers, no reorder buffer, no directory events and no per-directory counts.
//!
//! - **Scheduling** ([`pool`]): each worker pops its own newest directory (depth first) and, when
//!   it has none, steals the shallowest one another holds. One counter of directories not yet
//!   finished says when the traversal is done.
//! - **Listing and output** ([`worker`]): a worker reads its directory's listing page by page,
//!   prepares each page as a Blocks listing's task does, hands the page's items to the session as
//!   one batch, and only then pushes the subdirectories it found — so a directory's entry always
//!   precedes its children's. What needs I/O is observed by one task per page ([`page_io`]).
//! - **Counts**: each worker tallies what it handled; the totals are their sum. Every child a
//!   listing returned is counted exactly once, as in Blocks (§7).
//! - **Bounds**: `K = min(max_inflight_operations, 64)` listings at once, each holding at most a
//!   page in the worker (and, listing every version, the run of one object's versions carried to
//!   the next page), one in the backend's channel and one in its producer; the session holds
//!   `max_buffered_items`, or one batch when a batch alone is larger. A namespace whose
//!   open listing holds a server resource ([`Namespace::parked_listing_limit`]; CIFS: a directory
//!   handle) caps `K` at its limit: a worker keeps its listing open while it waits for the
//!   consumer, so every open listing may be parked, and at most `K` are open.
//! - **Ending**: a halt token (a child of the request's) stops every wait. It fires when the last
//!   directory finishes, on the first terminal failure, or when the request is cancelled; a
//!   listing then drops its stream (the backend's producer leaves through its own close path)
//!   and an observation task finishes what it started. Nothing is ever aborted.
//!
//! [`TraversalOrder::Unordered`]: crate::traversal::TraversalOrder::Unordered
//! [`Namespace::parked_listing_limit`]: crate::storage::Namespace::parked_listing_limit

use std::sync::Arc;

use tokio::sync::{Semaphore, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::{Probe, Shared, root_work};
use crate::traversal::channel::ItemSender;
use crate::traversal::{
    BlockCounts, ScanTotals, TraversalCompletion, TraversalOutcome, TraversalTerminalFailure,
};

mod page_io;
mod pool;
mod worker;

use pool::Pool;
use worker::Worker;

/// Upper bound on workers, whatever the request allows: the same bound Blocks puts on listings
/// in flight, and the legacy walker on its workers.
const MAX_WORKERS: usize = 64;

/// How many workers list at once.
fn worker_count(shared: &Shared) -> usize {
    let workers = shared
        .request
        .max_inflight_operations
        .get()
        .min(MAX_WORKERS);
    shared
        .parked_listing_limit
        .map_or(workers, |limit| workers.min(limit))
        .max(1)
}

/// Runs an Unordered traversal to its end and publishes its outcome.
pub(super) async fn run(
    shared: Arc<Shared>,
    items: ItemSender,
    completion: oneshot::Sender<Result<TraversalOutcome, TraversalTerminalFailure>>,
    probe: Probe,
) {
    let halt = shared.request.cancel.child_token();
    let pool = Arc::new(Pool::new(worker_count(&shared), root_work(&shared.request)));
    // Nothing samples the probe here yet: the residency counters are the Blocks cursor's.
    let _ = probe;
    let workers = spawn_workers(&shared, &pool, &items, &halt);
    // The workers' senders are the session's only ones: it sees its end once they are all gone.
    drop(items);
    let (totals, broken) = join(workers, &halt).await;
    let terminal = if shared.request.cancel.is_cancelled() {
        Ok(TraversalOutcome::Cancelled)
    } else if let Some(failure) = pool.failure() {
        Err(failure)
    } else if broken || !pool.drained() {
        Err(TraversalTerminalFailure::Internal)
    } else {
        Ok(TraversalOutcome::Completed(TraversalCompletion::new(
            totals,
        )))
    };
    let _ = completion.send(terminal);
}

/// Starts one task per slot of `pool`, each with its own sender of `items`.
fn spawn_workers(
    shared: &Arc<Shared>,
    pool: &Arc<Pool>,
    items: &ItemSender,
    halt: &CancellationToken,
) -> JoinSet<ScanTotals> {
    let observations = shared
        .request
        .max_inflight_operations
        .get()
        .min(Semaphore::MAX_PERMITS);
    let budget = Arc::new(Semaphore::new(observations));
    let mut workers = JoinSet::new();
    for me in 0..pool.workers() {
        workers.spawn(
            Worker {
                me,
                shared: Arc::clone(shared),
                pool: Arc::clone(pool),
                items: items.clone(),
                halt: halt.clone(),
                stop: halt.child_token(),
                budget: Arc::clone(&budget),
                block: BlockCounts::new(),
                totals: ScanTotals::new(),
            }
            .run(),
        );
    }
    workers
}

/// Joins every worker — never dropping the set, which would abort them — and sums what they
/// counted. `true` when one of them panicked: it lost the directory it held, so the traversal
/// cannot account for it, and the others are halted rather than left waiting for it.
async fn join(mut workers: JoinSet<ScanTotals>, halt: &CancellationToken) -> (ScanTotals, bool) {
    let mut totals = ScanTotals::new();
    let mut broken = false;
    while let Some(joined) = workers.join_next().await {
        if let Ok(counted) = joined {
            totals.merge(&counted);
        } else {
            broken = true;
            halt.cancel();
        }
    }
    (totals, broken)
}
