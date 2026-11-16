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
use super::super::controlled::resources::PageCharge;
use super::super::observe::{ObservationError, observe};
use super::super::output::observation_failures;
use crate::model::{FailureClass, ObservedEntry};
use crate::storage::{SourceDescriptor, StorageRoleFailure};
use crate::traversal::consumer::ConsumerState;
use crate::traversal::{ScanFailure, TraversalItem, TraversalTerminalFailure};

/// One child to observe, by its place in the page.
pub(super) type Job = (usize, Box<SourceDescriptor>);

/// One child's observation, by its place in the page.
pub(super) type Done = (usize, Result<Box<ObservedEntry>, ObservationError>);

/// Observes `jobs`, returning every result, or `None` once the traversal halts first.
///
/// # Errors
/// `Internal` when the task stopped without handing its results over (it panicked).
pub(in crate::traversal::storage) async fn observe_page(
    shared: &Arc<Shared>,
    budget: &Arc<Semaphore>,
    halt: &CancellationToken,
    jobs: Vec<Job>,
    consumer: Option<Arc<ConsumerState>>,
    charge: Option<PageCharge>,
) -> Result<Option<Vec<Done>>, TraversalTerminalFailure> {
    let (sender, receiver) = oneshot::channel();
    let task = run(
        Arc::clone(shared),
        Arc::clone(budget),
        halt.clone(),
        jobs,
        consumer,
        charge,
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
    consumer: Option<Arc<ConsumerState>>,
    charge: Option<PageCharge>,
    results: oneshot::Sender<Vec<Done>>,
) {
    let plan = shared.policy.observation_plan;
    let latest_only = shared.request.latest_metadata_only;
    let namespace = shared.namespace.as_ref();
    let metadata = shared.metadata.as_ref();
    let (budget, halt) = (&budget, &halt);
    let consumer = consumer.as_ref();
    let charge = charge.as_ref();
    let mut pending: FuturesUnordered<_> = jobs
        .into_iter()
        .map(|(place, descriptor)| async move {
            // A closed budget cannot happen (nothing closes it); treat it like a halt.
            let permit = budget.acquire().await.ok()?;
            if halt.is_cancelled() || consumer.is_some_and(|state| state.failure().is_some()) {
                return None;
            }
            let result = observe(namespace, metadata, *descriptor, plan, latest_only, halt).await;
            if let Some(consumer) = consumer {
                record_observation(consumer, result.as_ref());
                if let Some(charge) = charge
                    && let Err(failure) = charge.observation(result.as_ref())
                {
                    consumer.record_failure(failure);
                    return None;
                }
            }
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

/// Commit a returned cause before cancellation can win the page aggregation select.
pub(in crate::traversal::storage) fn record_observation(
    consumer: &ConsumerState,
    result: Result<&ObservedEntry, &ObservationError>,
) {
    let Err(error) = result else {
        if let Ok(entry) = result {
            consumer.record_observation(
                observation_failures(entry)
                    .into_iter()
                    .map(|failure| TraversalItem::ObservationFailure(Box::new(failure)))
                    .collect(),
                None,
            );
        }
        return;
    };
    let mut diagnostics: Vec<_> = error
        .observations
        .iter()
        .cloned()
        .map(|failure| TraversalItem::ObservationFailure(Box::new(failure)))
        .collect();
    let terminal = match &error.failure {
        StorageRoleFailure::Entry(failure)
            if !matches!(
                failure.class(),
                FailureClass::NotFound | FailureClass::Cancelled
            ) =>
        {
            diagnostics.push(TraversalItem::Failure(Box::new(ScanFailure::child(
                failure.clone(),
            ))));
            Some(TraversalTerminalFailure::ConsumerSource(Box::new(
                failure.clone(),
            )))
        }
        StorageRoleFailure::Session(failure) if failure.class() != FailureClass::Cancelled => {
            Some(TraversalTerminalFailure::Session(failure.clone()))
        }
        _ => None,
    };
    consumer.record_observation(diagnostics, terminal);
}
