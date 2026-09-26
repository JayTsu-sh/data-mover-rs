//! Observations of one page's children that need backend I/O (a symlink's target, metadata the
//! listing did not carry), for an Unordered worker.
//!
//! One detached task per page that has any — none under the default plan — observes them all
//! concurrently, each under the traversal's observation budget (`max_inflight_operations`
//! observations at once across every worker), and hands the results back together. The task is
//! never aborted: a backend observation may hold a handle between two awaits (a CIFS one opens,
//! reads, then closes), so a worker that stops waiting leaves the task to finish what it started
//! and drop the results. An observation not started once the traversal halted is not started.

use std::sync::Arc;

use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use tokio::sync::{Semaphore, oneshot};
use tokio_util::sync::CancellationToken;

use super::super::Shared;
use super::super::observe::observe;
use crate::model::ObservedEntry;
use crate::storage::{SourceDescriptor, StorageRoleFailure};
use crate::traversal::TraversalTerminalFailure;

/// One child to observe, by its place in the page.
pub(super) type Job = (usize, Box<SourceDescriptor>);

/// One child's observation, by its place in the page.
pub(super) type Done = (usize, Result<Box<ObservedEntry>, StorageRoleFailure>);

/// Observes `jobs`, returning every result, or `None` once the traversal halts first.
///
/// # Errors
/// `Internal` when the task stopped without handing its results over (it panicked).
pub(super) async fn observe_page(
    shared: &Arc<Shared>,
    budget: &Arc<Semaphore>,
    halt: &CancellationToken,
    jobs: Vec<Job>,
) -> Result<Option<Vec<Done>>, TraversalTerminalFailure> {
    let (sender, receiver) = oneshot::channel();
    let task = run(
        Arc::clone(shared),
        Arc::clone(budget),
        halt.clone(),
        jobs,
        sender,
    );
    // Detached, never aborted: see the module docs.
    drop(tokio::spawn(task));
    tokio::select! {
        biased;
        () = halt.cancelled() => Ok(None),
        results = receiver => results
            .map(Some)
            .map_err(|_| TraversalTerminalFailure::Internal),
    }
}

async fn run(
    shared: Arc<Shared>,
    budget: Arc<Semaphore>,
    halt: CancellationToken,
    jobs: Vec<Job>,
    results: oneshot::Sender<Vec<Done>>,
) {
    let plan = shared.policy.observation_plan;
    let namespace = shared.namespace.as_ref();
    let metadata = shared.metadata.as_ref();
    let (budget, halt) = (&budget, &halt);
    let mut pending: FuturesUnordered<_> = jobs
        .into_iter()
        .map(|(place, descriptor)| async move {
            // A closed budget cannot happen (nothing closes it); treat it like a halt.
            let permit = budget.acquire().await.ok()?;
            if halt.is_cancelled() {
                return None;
            }
            let result = observe(namespace, metadata, *descriptor, plan).await;
            drop(permit);
            Some((place, result.map(Box::new)))
        })
        .collect();
    let mut done = Vec::with_capacity(pending.len());
    while let Some(result) = pending.next().await {
        done.extend(result);
    }
    // The worker is gone once the traversal halted: the results are simply dropped.
    let _ = results.send(done);
}
