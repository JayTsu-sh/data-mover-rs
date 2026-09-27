//! The source half's check that the object it is about to stream is the one the observation
//! advertised.

use super::{TransferFailure, TransferPhase, TransferSide};
use crate::model::{EntryKind, ObservedEntry};
use crate::storage::{ReadSource, SourceDescriptor};

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

fn validate_observation(
    source: &dyn ReadSource,
    observation: &ObservedEntry,
    descriptor: &SourceDescriptor,
) -> Result<(), TransferFailure> {
    if observation.kind() != EntryKind::File
        || descriptor.kind != EntryKind::File
        || observation.path() != &descriptor.path
        || observation.size() != descriptor.size
        || !source.observation_matches(observation.source_identity(), descriptor)
    {
        return Err(TransferFailure::orchestration(
            TransferPhase::Describe,
            "source differs from advertised observation",
        ));
    }
    Ok(())
}
