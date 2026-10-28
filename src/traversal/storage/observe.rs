//! Per-entry observation: turns one listed descriptor into an immutable `ObservedEntry`,
//! consulting the metadata role only when the listing did not already supply what the plan asks.
//!
//! Most children need no I/O at all — nothing beyond what the listing carried was asked for —
//! and those are observed by [`observe_listed`] inside the listing's own task, a page at a time,
//! so the driver is woken once per page rather than once per child. The rest (a
//! symlink's target, metadata the listing did not carry) go to [`Observations`]: one task per
//! block that has any, fed its children as the cursor admits them and running them concurrently.
//! The admission window already bounds how many are admitted and not yet emitted, so it bounds
//! these too.
//!
//! **A key with its history** (P7 §10.4, R2): when the plan asks for more than the listing gave,
//! each version of the history is observed on its own, bound to that version, one after another
//! within the key's one observation (so a key takes one of the traversal's observation slots,
//! however many versions it has); delete markers are not observed. A version whose observation
//! fails keeps the failure on its own record (`MetadataObservation::Failed`, whatever the mode)
//! and the key is still delivered; the entry's own metadata is its current version's.

use std::sync::Arc;

use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::{Deferred, Shared, entry_failure, throttling_scoped_to};
use crate::model::{
    EntryKind, EntryOperationFailure, FailureClass, MAX_MODEL_FIELD_BYTES, MetadataObservation,
    MetadataObservations, MetadataProvenance, ObjectHistory, ObservationMode, ObservationPlan,
    ObservedEntry, Operation, StoragePath, SymlinkTarget, TimestampMetadata, Transience,
    VersionRecord,
};
use crate::storage::{Metadata, Namespace, SourceDescriptor, StorageRoleFailure};
use crate::traversal::{ObservationFailure, TraversalTerminalFailure};

/// Facts obtained before a later operation failed, carried only on the internal result path.
#[derive(Debug)]
pub(super) struct ObservationError {
    pub(super) failure: StorageRoleFailure,
    pub(super) observations: Vec<ObservationFailure>,
}

impl From<StorageRoleFailure> for ObservationError {
    fn from(failure: StorageRoleFailure) -> Self {
        Self {
            failure,
            observations: Vec::new(),
        }
    }
}

mod marker;
mod metadata;
pub(super) use metadata::{dependency_without_io, without_io as metadata_without_io};

/// Only modes authorizing additional calls require a version to be observed.
fn record_needs_role(plan: ObservationPlan, record: &VersionRecord) -> bool {
    !record.is_delete_marker()
        && ([
            plan.acl(),
            plan.xattrs(),
            plan.tags(),
            plan.ownership_mode(),
        ]
        .into_iter()
        .any(ObservationMode::permits_io)
            || (plan.timestamps().permits_io() && record.modified().is_none()))
}

/// What a record whose observation failed carries: the failure, in every family the plan asked
/// for.
fn failed_observations(
    plan: ObservationPlan,
    path: &StoragePath,
    class: FailureClass,
    transience: Transience,
) -> Result<MetadataObservations, StorageRoleFailure> {
    fn failed<T>(
        mode: ObservationMode,
        class: FailureClass,
        transience: Transience,
    ) -> MetadataObservation<T> {
        if mode == ObservationMode::Omit {
            MetadataObservation::NotRequested
        } else {
            MetadataObservation::Failed { class, transience }
        }
    }
    MetadataObservations::new(
        failed(plan.acl(), class, transience),
        failed(plan.xattrs(), class, transience),
        failed(plan.tags(), class, transience),
        failed(plan.ownership_mode(), class, transience),
        failed(plan.timestamps(), class, transience),
    )
    .map_err(|_| StorageRoleFailure::Entry(entry_failure(path, FailureClass::Protocol)))
}

/// Observes each version of the key's history that the plan needs more of, bound to that version,
/// one after another; a failure stays on its record, except a session failure, which ends the
/// traversal. Cancelled part way, the key is abandoned (the traversal is ending and delivers
/// nothing more). The entry's own metadata is its current version's, or what the listing said.
async fn observe_history(
    metadata_role: &dyn Metadata,
    mut descriptor: SourceDescriptor,
    mut history: Box<ObjectHistory>,
    plan: ObservationPlan,
    cancel: &CancellationToken,
) -> Result<ObservedEntry, ObservationError> {
    let history_path =
        ObjectHistory::source_path(&descriptor.path, descriptor.kind).map_err(|_| {
            StorageRoleFailure::Entry(super::entry_failure(
                &descriptor.path,
                FailureClass::Unsupported,
            ))
        })?;
    let path = &history_path;
    let capabilities = metadata_role.observation_capabilities();
    let mut observations = Vec::new();
    for record in history.records_mut() {
        if record.is_delete_marker() {
            continue;
        }
        let inline = metadata::record_inline(record.modified(), plan, capabilities);
        let requested = metadata::io_plan(plan, &inline, capabilities);
        if !requested.permits_io() {
            record.set_metadata(inline);
            continue;
        }
        if cancel.is_cancelled() {
            return Err(ObservationError {
                failure: StorageRoleFailure::Entry(cancelled(path)),
                observations,
            });
        }
        let Some(version) = record.source() else {
            continue;
        };
        let observed = metadata_role
            .observe_copy_bound_version(path, record.identity(), &version, requested)
            .await
            .map_err(|failure| throttling_scoped_to(path, Operation::Observe, failure))
            .and_then(|observed| {
                metadata::validate_history(
                    &descriptor,
                    plan,
                    metadata::merge(inline.clone(), observed.observations, requested),
                )
            });
        let mut metadata = match observed {
            Ok(observed) => observed,
            Err(StorageRoleFailure::Entry(failure)) => metadata::merge(
                inline,
                failed_observations(requested, path, failure.class(), failure.transience())?,
                requested,
            ),
            Err(session @ StorageRoleFailure::Session(_)) => {
                return Err(ObservationError {
                    failure: session,
                    observations,
                });
            }
        };
        preserve_record_timestamp(&mut metadata, record, plan);
        observations.extend(super::output::metadata_failures(path, &version, &metadata));
        record.set_metadata(metadata);
    }
    let mut metadata = match history.current().and_then(VersionRecord::metadata) {
        Some(current) => current.clone(),
        None => match metadata::inline(metadata_role, &descriptor, plan) {
            Ok(metadata) => metadata,
            Err(failure) => {
                return Err(ObservationError {
                    failure,
                    observations,
                });
            }
        },
    };
    // The current version's time could not be observed: the key keeps the one its listing gave,
    // as a key without a history would (its entry never goes without a time the listing had).
    if metadata.timestamps.value().is_none()
        && plan.timestamps() != ObservationMode::Omit
        && let Some(value) = descriptor.inline_timestamps
    {
        metadata.retain_failure(crate::model::MetadataFamily::Timestamps);
        metadata.timestamps = MetadataObservation::Value {
            value,
            provenance: MetadataProvenance::Inline,
        };
    }
    descriptor.listing.history = Some(history);
    assemble(descriptor, metadata, None).map_err(|mut error| {
        for failure in error.observations {
            if !observations.contains(&failure) {
                observations.push(failure);
            }
        }
        error.observations = observations;
        error
    })
}

fn preserve_record_timestamp(
    metadata: &mut MetadataObservations,
    record: &VersionRecord,
    plan: ObservationPlan,
) {
    if plan.timestamps() != ObservationMode::Omit
        && metadata.timestamps.value().is_none()
        && let Some(modified) = record.modified()
    {
        metadata.retain_failure(crate::model::MetadataFamily::Timestamps);
        metadata.timestamps = MetadataObservation::Value {
            value: TimestampMetadata {
                accessed: None,
                modified: Some(modified),
                created: None,
            },
            provenance: MetadataProvenance::Inline,
        };
    }
}

/// The failure a key abandoned by cancellation settles with; never delivered.
fn cancelled(path: &StoragePath) -> EntryOperationFailure {
    EntryOperationFailure::new(
        path.clone(),
        Operation::Observe,
        FailureClass::Cancelled,
        Transience::Transient,
        "the traversal was cancelled while the key's versions were observed",
    )
    .unwrap_or_else(|_| unreachable!("the static observation diagnostic is valid"))
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
) -> Result<ObservedEntry, ObservationError> {
    let observations =
        super::output::metadata_failures(&descriptor.path, descriptor.version(), &metadata);
    assemble_entry(descriptor, metadata, target).map_err(|failure| ObservationError {
        failure,
        observations,
    })
}

fn assemble_entry(
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
        rename_evidence,
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
    .with_rename_evidence(rename_evidence)
    .with_history(listing.history);
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
    Observed(Result<Box<ObservedEntry>, ObservationError>),
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
            Self::Observed(Err(error)) => match &error.failure {
                StorageRoleFailure::Entry(failure) => failure.path().as_str().len(),
                StorageRoleFailure::Session(_) => 0,
            },
            Self::Unobserved(descriptor) => descriptor.path.as_str().len(),
        }
    }
}

/// Observes `descriptor` without I/O, inside the listing's task, when nothing about it needs
/// any.
pub(super) fn observe_listed(
    metadata_role: &dyn Metadata,
    mut descriptor: SourceDescriptor,
    plan: ObservationPlan,
) -> Listed {
    if let Some(marker) = descriptor.listing.directory_marker.take() {
        return marker::listed(metadata_role, descriptor, *marker, plan);
    }
    if descriptor.kind == EntryKind::Symlink {
        return Listed::Unobserved(Box::new(descriptor));
    }
    match metadata_without_io(metadata_role, plan, &descriptor) {
        Some(Ok(metadata)) => {
            if plan != ObservationPlan::default()
                && let Some(history) = descriptor.listing.history.as_deref_mut()
            {
                let capabilities = metadata_role.observation_capabilities();
                for record in history.records_mut() {
                    if !record.is_delete_marker() {
                        record.set_metadata(metadata::record_inline(
                            record.modified(),
                            plan,
                            capabilities,
                        ));
                    }
                }
            }
            Listed::Observed(assemble(descriptor, metadata, None).map(Box::new))
        }
        Some(Err(failure)) => Listed::Observed(Err(failure.into())),
        None => Listed::Unobserved(Box::new(descriptor)),
    }
}

/// Observes `descriptor`, asking the backend for whatever the listing did not carry. `cancel` is
/// the traversal's: a key's history stops being observed once it fires.
pub(super) async fn observe(
    namespace: &dyn Namespace,
    metadata_role: &dyn Metadata,
    mut descriptor: SourceDescriptor,
    plan: ObservationPlan,
    cancel: &CancellationToken,
) -> Result<ObservedEntry, ObservationError> {
    if let Some(marker) = descriptor.listing.directory_marker.take() {
        return marker::observe(namespace, metadata_role, descriptor, *marker, plan).await;
    }
    if let Some(history) = descriptor.listing.history.take() {
        return observe_history(metadata_role, descriptor, history, plan, cancel).await;
    }
    match observe_io(namespace, metadata_role, &descriptor, plan).await {
        Ok((metadata, target)) => assemble(descriptor, metadata, target),
        Err(mut error) => {
            error.failure = confirm_vanished(namespace, &descriptor.path, error.failure).await;
            Err(error)
        }
    }
}

/// The metadata and link target of `descriptor` that need backend I/O.
async fn observe_io(
    namespace: &dyn Namespace,
    metadata_role: &dyn Metadata,
    descriptor: &SourceDescriptor,
    plan: ObservationPlan,
) -> Result<(MetadataObservations, Option<SymlinkTarget>), ObservationError> {
    // A throttled request is this child's failure, never the traversal's end (P7 §8).
    let scoped = |failure| throttling_scoped_to(&descriptor.path, Operation::Observe, failure);
    let metadata = if let Some(metadata) = metadata_without_io(metadata_role, plan, descriptor) {
        metadata?
    } else {
        let inline = metadata::inline(metadata_role, descriptor, plan)?;
        let requested = metadata::io_plan(plan, &inline, metadata_role.observation_capabilities());
        let observed = metadata_role
            .observe(&descriptor.path, requested)
            .await
            .map_err(scoped)?;
        metadata::validate(
            descriptor,
            plan,
            metadata::merge(inline, observed, requested),
        )?
    };
    let target = if descriptor.kind == EntryKind::Symlink {
        Some(
            match namespace.read_link(&descriptor.path).await.map_err(scoped) {
                Ok(target) => target,
                Err(failure) => {
                    return Err(ObservationError {
                        failure,
                        observations: super::output::metadata_failures(
                            &descriptor.path,
                            descriptor.version(),
                            &metadata,
                        ),
                    });
                }
            },
        )
    } else {
        None
    };
    Ok((metadata, target))
}

/// A `NotFound` met while observing a child makes it *vanished* — no item, and a consumer may
/// treat it as deleted (P7 Q12) — so it is believed only once the child itself is confirmed gone:
/// a sub-read can report `NotFound` about something else (an extended attribute removed between
/// listing and reading it, a version deleted from a key that is still there). When the child is
/// still there, the observation is that child's failure (`Conflict` / `Transient`: it changed
/// while observed), never a vanish. Costs one `stat`, only on this path.
async fn confirm_vanished(
    namespace: &dyn Namespace,
    path: &StoragePath,
    failure: StorageRoleFailure,
) -> StorageRoleFailure {
    let StorageRoleFailure::Entry(error) = &failure else {
        return failure;
    };
    if error.class() != FailureClass::NotFound {
        return failure;
    }
    // A throttled confirmation is no confirmation, and never the traversal's end (P7 §8).
    let confirmed = namespace
        .stat(path)
        .await
        .map_err(|failure| throttling_scoped_to(path, Operation::Observe, failure));
    match confirmed {
        Err(StorageRoleFailure::Entry(gone)) if gone.class() == FailureClass::NotFound => failure,
        Err(session @ StorageRoleFailure::Session(_)) => session,
        Ok(_) | Err(StorageRoleFailure::Entry(_)) => StorageRoleFailure::Entry(not_vanished(
            path,
            format!(
                "{}; the entry itself was not confirmed gone, so it did not vanish",
                error.diagnostic()
            ),
        )),
    }
}

/// A child's failure saying it changed while observed but did not vanish. Falls back to a static
/// diagnostic, never to the `NotFound` it replaces, which would read as a vanish.
fn not_vanished(path: &StoragePath, diagnostic: String) -> EntryOperationFailure {
    let failure = |diagnostic| {
        EntryOperationFailure::new(
            path.clone(),
            Operation::Observe,
            FailureClass::Conflict,
            Transience::Transient,
            diagnostic,
        )
    };
    failure(diagnostic).unwrap_or_else(|_| {
        failure("a part of the entry vanished while it was observed".to_owned())
            .unwrap_or_else(|_| unreachable!("the static observation diagnostic is valid"))
    })
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
    Result<Box<ObservedEntry>, ObservationError>,
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
    let cancel = &shared.request.cancel;
    let mut inflight = FuturesUnordered::new();
    let mut open = true;
    loop {
        tokio::select! {
            job = jobs.recv(), if open => match job {
                // The traversal is ending and discards every result: a child not started yet
                // holds no handle, so it is simply not started.
                Some(_) if shared.request.cancel.is_cancelled() => {}
                Some(Job { sequence, descriptor, deferred }) => inflight.push(async move {
                    let result = observe(namespace, metadata, *descriptor, plan, cancel).await;
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
