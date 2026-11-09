//! Shared checks that a transfer source still matches the advertised observation.

use super::{TransferFailure, TransferPhase, TransferSide};
use crate::model::{
    EntryKind, EntryOperationFailure, FailureClass, ObservedEntry, Operation, Transience,
};
use crate::storage::{ReadSource, SourceDescriptor, StorageRoleFailure};
use crate::transfer::model::ObservationConstraint;

/// Describes the observed path and checks it is the advertised object.
pub(super) async fn describe(
    source: &dyn ReadSource,
    observation: &ObservedEntry,
) -> Result<SourceDescriptor, TransferFailure> {
    refuse_without_current_object(observation)?;
    let descriptor = source.describe(observation.path()).await.map_err(|error| {
        TransferFailure::role(TransferPhase::Describe, TransferSide::Source, error)
    })?;
    validate_observation(source, observation, &descriptor)?;
    Ok(descriptor)
}

/// The expert halves copy the current object only. An observation of a key with its history
/// (P7 §10) is copied when the history names a current object — the entry-level facts are that
/// object's, as a listing of current objects gives them — and refused otherwise: a key deleted now
/// (its latest record is a delete marker) holds nothing to copy, and a segment that is not the
/// key's last does not say what is current. An older version is copied with
/// `TransferRequest::with_source_version`.
fn refuse_without_current_object(observation: &ObservedEntry) -> Result<(), TransferFailure> {
    let Some(history) = observation.history() else {
        return Ok(());
    };
    let reason = if !history.is_complete() {
        "the observation is a segment of a key's history that is not its last; expert transfers \
         copy the current object only"
    } else if history.current().is_none() {
        "the observation is a key deleted now (its latest record is a delete marker), which holds \
         nothing to copy"
    } else {
        return Ok(());
    };
    Err(TransferFailure::orchestration(
        TransferPhase::Describe,
        reason,
    ))
}

pub(crate) fn validate_observation(
    source: &dyn ReadSource,
    observation: &ObservedEntry,
    descriptor: &SourceDescriptor,
) -> Result<(), TransferFailure> {
    validate_constraint(
        source,
        ObservationConstraint::from_entry(observation),
        descriptor,
    )
}

pub(super) fn validate_constraint(
    source: &dyn ReadSource,
    observation: ObservationConstraint<'_>,
    descriptor: &SourceDescriptor,
) -> Result<(), TransferFailure> {
    // Stable file identities survive in-place edits. Compare modification times as well when
    // both operations supplied them; backends such as S3 match content through their identity.
    let modified_changed = observation
        .modified
        .zip(
            descriptor
                .inline_timestamps
                .and_then(|times| times.modified),
        )
        .is_some_and(|(observed, described)| {
            let quantum = observed
                .precision()
                .quantum_nanos()
                .max(described.precision().quantum_nanos());
            observed.unix_nanos().div_euclid(quantum) != described.unix_nanos().div_euclid(quantum)
        });
    if modified_changed
        || observation.kind != EntryKind::File
        || descriptor.kind != EntryKind::File
        || observation.path != &descriptor.path
        || observation.size != descriptor.size
        || !source.observation_matches(observation.identity, descriptor)
    {
        return Err(TransferFailure::orchestration(
            TransferPhase::Describe,
            "source differs from advertised observation",
        ));
    }
    Ok(())
}

/// 可观察的删除标记／虚拟前缀没有数据，必须在元数据与目标访问之前拒绝普通传输。
pub(super) fn refuse_unreadable(descriptor: &SourceDescriptor) -> Result<(), TransferFailure> {
    if !descriptor.listing.listing_only {
        return Ok(());
    }
    let error = EntryOperationFailure::new(
        descriptor.path.clone(),
        Operation::Observe,
        FailureClass::NotFound,
        Transience::Permanent,
        "selected source has no readable object content",
    )
    .map_err(|_| {
        TransferFailure::orchestration(
            TransferPhase::Describe,
            "invalid non-data source diagnostic",
        )
    })?;
    Err(TransferFailure::role(
        TransferPhase::Describe,
        TransferSide::Source,
        StorageRoleFailure::Entry(error),
    ))
}
