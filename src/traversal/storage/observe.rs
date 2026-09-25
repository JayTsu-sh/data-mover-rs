//! Per-entry observation: turns one listed descriptor into an immutable `ObservedEntry`,
//! consulting the metadata role only when the listing did not already supply what the plan asks.
//!
//! Most children need no I/O at all — nothing beyond what the listing carried was asked for —
//! and those are observed by [`observe_listed`] inside the listing's own task, a whole block at
//! a time, so the driver is woken once per listing rather than once per child. The rest (a
//! symlink's target, metadata the listing did not carry) go to [`Observations`]: one task per
//! block that has any, fed its children as the cursor admits them and running them concurrently.
//! The admission window already bounds how many are admitted and not yet emitted, so it bounds
//! these too.

use std::sync::Arc;

use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use super::{Deferred, Shared, entry_failure};
use crate::model::{
    EntryKind, FailureClass, MAX_MODEL_FIELD_BYTES, MetadataObservation, MetadataObservations,
    MetadataProvenance, ObservationMode, ObservationPlan, ObservedEntry, StoragePath,
    SymlinkTarget,
};
use crate::storage::{Metadata, Namespace, SourceDescriptor, StorageRoleFailure};
use crate::traversal::TraversalTerminalFailure;

/// Whether the metadata role must be consulted for `descriptor` under `plan`, or whether the
/// listing already supplied everything the plan asks for.
fn needs_metadata_role(plan: ObservationPlan, descriptor: &SourceDescriptor) -> bool {
    let optional_requested = [
        plan.acl(),
        plan.xattrs(),
        plan.tags(),
        plan.ownership_mode(),
    ]
    .into_iter()
    .any(|mode| mode != ObservationMode::Omit);
    optional_requested
        || (plan.timestamps() != ObservationMode::Omit && descriptor.inline_timestamps.is_none())
}

fn inline_observations(
    plan: ObservationPlan,
    descriptor: &SourceDescriptor,
) -> Result<MetadataObservations, StorageRoleFailure> {
    let timestamps = match (plan.timestamps(), descriptor.inline_timestamps) {
        (ObservationMode::Omit, _) | (_, None) => MetadataObservation::NotRequested,
        (_, Some(value)) => MetadataObservation::Value {
            value,
            provenance: MetadataProvenance::Inline,
        },
    };
    MetadataObservations::new(
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        timestamps,
    )
    .map_err(|_| StorageRoleFailure::Entry(entry_failure(&descriptor.path, FailureClass::Protocol)))
}

/// A requested family of a child that has nothing behind it to observe (an S3 prefix, a delete
/// marker): not applicable, rather than a failed lookup of an object that does not exist.
fn nothing_behind<T>(mode: ObservationMode) -> MetadataObservation<T> {
    if mode == ObservationMode::Omit {
        MetadataObservation::NotRequested
    } else {
        MetadataObservation::NotApplicable
    }
}

/// Observations of a listing-only child: what the listing gave, and nothing else.
fn listed_observations(
    plan: ObservationPlan,
    descriptor: &SourceDescriptor,
) -> Result<MetadataObservations, StorageRoleFailure> {
    let timestamps = match (plan.timestamps(), descriptor.inline_timestamps) {
        (ObservationMode::Omit, _) => MetadataObservation::NotRequested,
        (_, Some(value)) => MetadataObservation::Value {
            value,
            provenance: MetadataProvenance::Inline,
        },
        (_, None) => MetadataObservation::NotApplicable,
    };
    MetadataObservations::new(
        nothing_behind(plan.acl()),
        nothing_behind(plan.xattrs()),
        nothing_behind(plan.tags()),
        nothing_behind(plan.ownership_mode()),
        timestamps,
    )
    .map_err(|_| StorageRoleFailure::Entry(entry_failure(&descriptor.path, FailureClass::Protocol)))
}

/// The metadata of `descriptor` when the listing alone answers it: a listing-only child, a plan
/// that asks for nothing the listing did not carry, or one the metadata role can answer from the
/// listing's own attributes ([`Metadata::observe_listed`]). `None` means the metadata role must
/// be asked, which is I/O. A listed version is always asked: its observation is bound to it.
fn metadata_without_io(
    metadata_role: &dyn Metadata,
    plan: ObservationPlan,
    descriptor: &SourceDescriptor,
) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
    if descriptor.listing.listing_only {
        return Some(listed_observations(plan, descriptor));
    }
    if !needs_metadata_role(plan, descriptor) {
        return Some(inline_observations(plan, descriptor));
    }
    if descriptor.listing.version.is_none() {
        return metadata_role.observe_listed(descriptor, plan);
    }
    None
}

/// The metadata of `descriptor` from the metadata role, bound to the listed version when the
/// child is one, so an old version is never described by the current object's tags and time.
async fn role_observations(
    metadata_role: &dyn Metadata,
    descriptor: &SourceDescriptor,
    plan: ObservationPlan,
) -> Result<MetadataObservations, StorageRoleFailure> {
    if descriptor.listing.version.is_none() {
        return metadata_role.observe(&descriptor.path, plan).await;
    }
    // A listed version — the latest `"null"` one included, whose selector is `Current` — is
    // observed only while the store still holds exactly what was listed.
    metadata_role
        .observe_copy_bound_version(
            &descriptor.path,
            &descriptor.source_identity,
            &descriptor.version,
            plan,
        )
        .await
        .map(|observed| observed.observations)
}

/// A model check that the ones in [`assemble`] should already have ruled out. Not blamed on any
/// entry's path, which has moved into the entry by then: this is a defect, not a fact about it.
fn internal() -> StorageRoleFailure {
    StorageRoleFailure::Entry(entry_failure(&StoragePath::root(), FailureClass::Internal))
}

/// Builds the entry from its descriptor, moving every field rather than copying it.
fn assemble(
    descriptor: SourceDescriptor,
    metadata: MetadataObservations,
    target: Option<SymlinkTarget>,
) -> Result<ObservedEntry, StorageRoleFailure> {
    let protocol =
        |path: &StoragePath| StorageRoleFailure::Entry(entry_failure(path, FailureClass::Protocol));
    // Every check that can fail runs before the path moves into the entry, so the failure can
    // still name it without a copy of the path kept for the rare case.
    let oversized = descriptor
        .backend_fact
        .as_ref()
        .is_some_and(|fact| fact.len() > MAX_MODEL_FIELD_BYTES);
    if oversized || (descriptor.kind == EntryKind::Symlink) != target.is_some() {
        return Err(protocol(&descriptor.path));
    }
    let SourceDescriptor {
        path,
        kind,
        size,
        source_identity,
        backend_fact,
        listing,
        ..
    } = descriptor;
    let modified = metadata
        .timestamps()
        .value()
        .and_then(|value| value.modified);
    let entry = match target {
        Some(target) => ObservedEntry::new_symlink(path, modified, source_identity, target),
        None => ObservedEntry::new(path, kind, size, modified, source_identity),
    }
    // Unreachable after the checks above; kept typed rather than asserted.
    .map_err(|_| internal())?
    .with_metadata(metadata)
    .with_version(listing.version);
    match backend_fact {
        Some(fact) => entry
            .with_backend_fact_bytes(fact.to_vec())
            .map_err(|_| internal()),
        None => Ok(entry),
    }
}

/// What the listing's task made of one child.
pub(super) enum Listed {
    /// Observed already: nothing about it needed backend I/O.
    Observed(Result<Box<ObservedEntry>, StorageRoleFailure>),
    /// Needs backend I/O (a symlink's target, metadata the listing did not carry), which starts
    /// once the child is admitted.
    Unobserved(Box<SourceDescriptor>),
}

impl Listed {
    /// Bytes of the path the child is known by, for the residency probe.
    #[cfg(test)]
    pub(super) fn path_len(&self) -> usize {
        match self {
            Self::Observed(Ok(entry)) => entry.path().as_str().len(),
            Self::Observed(Err(StorageRoleFailure::Entry(failure))) => {
                failure.path().as_str().len()
            }
            Self::Observed(Err(StorageRoleFailure::Session(_))) => 0,
            Self::Unobserved(descriptor) => descriptor.path.as_str().len(),
        }
    }
}

/// Observes `descriptor` without I/O, inside the listing's task, when nothing about it needs
/// any.
pub(super) fn observe_listed(
    metadata_role: &dyn Metadata,
    descriptor: SourceDescriptor,
    plan: ObservationPlan,
) -> Listed {
    if descriptor.kind == EntryKind::Symlink {
        return Listed::Unobserved(Box::new(descriptor));
    }
    match metadata_without_io(metadata_role, plan, &descriptor) {
        Some(Ok(metadata)) => Listed::Observed(assemble(descriptor, metadata, None).map(Box::new)),
        Some(Err(failure)) => Listed::Observed(Err(failure)),
        None => Listed::Unobserved(Box::new(descriptor)),
    }
}

/// Observes `descriptor`, asking the backend for whatever the listing did not carry.
pub(super) async fn observe(
    namespace: &dyn Namespace,
    metadata_role: &dyn Metadata,
    descriptor: SourceDescriptor,
    plan: ObservationPlan,
) -> Result<ObservedEntry, StorageRoleFailure> {
    let metadata = match metadata_without_io(metadata_role, plan, &descriptor) {
        Some(metadata) => metadata?,
        None => role_observations(metadata_role, &descriptor, plan).await?,
    };
    let target = if descriptor.kind == EntryKind::Symlink {
        Some(namespace.read_link(&descriptor.path).await?)
    } else {
        None
    };
    assemble(descriptor, metadata, target)
}

/// One admitted child whose observation needs backend I/O.
pub(super) struct Job {
    pub(super) sequence: u64,
    pub(super) descriptor: Box<SourceDescriptor>,
    pub(super) deferred: Option<Deferred>,
}

/// A settled observation, keyed by the sequence its child was admitted under.
pub(super) type Observed = (
    u64,
    Result<Box<ObservedEntry>, StorageRoleFailure>,
    Option<Deferred>,
);

/// A block's line to its observation task. Dropping it tells the task no more children come.
pub(super) type Feed = mpsc::UnboundedSender<Job>;

/// The observations that need I/O, and the tasks running them.
pub(super) struct Observations {
    tasks: JoinSet<()>,
    sender: mpsc::UnboundedSender<Observed>,
    results: mpsc::UnboundedReceiver<Observed>,
    /// Children handed to a task whose result has not been received yet.
    outstanding: usize,
    /// Tasks started, for the tests that count hand-offs.
    #[cfg(test)]
    pub(super) spawned: usize,
    /// The most tasks held at once, finished or not, for the test that bounds it.
    #[cfg(test)]
    pub(super) peak_held: usize,
}

impl Observations {
    pub(super) fn new() -> Self {
        let (sender, results) = mpsc::unbounded_channel();
        Self {
            tasks: JoinSet::new(),
            sender,
            results,
            outstanding: 0,
            #[cfg(test)]
            spawned: 0,
            #[cfg(test)]
            peak_held: 0,
        }
    }

    /// Nothing is being observed: every admitted child's result has been received.
    pub(super) const fn is_idle(&self) -> bool {
        self.outstanding == 0
    }

    /// Hands `job` to its block's task, starting the task with the block's first such child.
    pub(super) fn start(
        &mut self,
        shared: &Arc<Shared>,
        feed: &mut Option<Feed>,
        job: Job,
    ) -> Result<(), TraversalTerminalFailure> {
        if feed.is_none() {
            // A finished task keeps its allocation in the set until it is joined, and the
            // results arrive over the channel rather than through a join: without this every
            // block that ever needed I/O would stay behind for the rest of the traversal.
            while let Some(joined) = self.tasks.try_join_next() {
                joined.map_err(|_| TraversalTerminalFailure::Internal)?;
            }
        }
        let feed = feed.get_or_insert_with(|| {
            let (feed, jobs) = mpsc::unbounded_channel();
            self.tasks
                .spawn(observe_block(Arc::clone(shared), jobs, self.sender.clone()));
            #[cfg(test)]
            {
                self.spawned += 1;
                self.peak_held = self.peak_held.max(self.tasks.len());
            }
            feed
        });
        // A task only ends once its feed is dropped, so a refused job means it panicked.
        feed.send(job)
            .map_err(|_| TraversalTerminalFailure::Internal)?;
        self.outstanding += 1;
        Ok(())
    }

    /// The next observation to settle. Only called while something is outstanding.
    pub(super) async fn next(&mut self) -> Result<Observed, TraversalTerminalFailure> {
        loop {
            tokio::select! {
                Some(observed) = self.results.recv() => {
                    self.outstanding = self.outstanding.saturating_sub(1);
                    return Ok(observed);
                }
                // A task that panicked loses the children it held: the traversal cannot account
                // for them, so it fails rather than waiting for them forever.
                Some(joined) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    joined.map_err(|_| TraversalTerminalFailure::Internal)?;
                }
            }
        }
    }

    /// An observation that has already arrived, without waiting.
    pub(super) fn ready(&mut self) -> Option<Observed> {
        if self.outstanding == 0 {
            return None;
        }
        let observed = self.results.try_recv().ok()?;
        self.outstanding -= 1;
        Some(observed)
    }

    /// Lets every task run to completion in the background. Never abort a backend operation: it
    /// may hold an open handle between two awaits (a CIFS observation opens, reads attributes,
    /// then closes), and dropping the future there leaks it. Each task ends once its block's feed
    /// is dropped and what it holds has finished.
    pub(super) fn detach(&mut self) {
        self.tasks.detach_all();
    }
}

/// Observes one block's children as they are fed, all of them concurrently, reporting each as
/// it settles.
async fn observe_block(
    shared: Arc<Shared>,
    mut jobs: mpsc::UnboundedReceiver<Job>,
    results: mpsc::UnboundedSender<Observed>,
) {
    let plan = shared.policy.observation_plan;
    let namespace = shared.namespace.as_ref();
    let metadata = shared.metadata.as_ref();
    let mut inflight = FuturesUnordered::new();
    let mut open = true;
    loop {
        tokio::select! {
            job = jobs.recv(), if open => match job {
                // The traversal is ending and discards every result: a child not started yet
                // holds no handle, so it is simply not started.
                Some(_) if shared.request.cancel.is_cancelled() => {}
                Some(Job { sequence, descriptor, deferred }) => inflight.push(async move {
                    let result = observe(namespace, metadata, *descriptor, plan).await;
                    (sequence, result.map(Box::new), deferred)
                }),
                None => open = false,
            },
            Some(observed) = inflight.next(), if !inflight.is_empty() => {
                // The driver is gone once the traversal ended: the result is simply dropped.
                let _ = results.send(observed);
            }
            else => break,
        }
    }
}
