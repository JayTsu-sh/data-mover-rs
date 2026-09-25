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
    refuse_listed_versions(observation)?;
    let descriptor = source.describe(observation.path()).await.map_err(|error| {
        TransferFailure::role(TransferPhase::Describe, TransferSide::Source, error)
    })?;
    validate_observation(source, observation, &descriptor)?;
    Ok(descriptor)
}

/// The expert halves copy the current version only. An observation of one listed version from a
/// traversal of every version (`TraversalVersions::All`) is refused unless it is the current one:
/// an older version is copied with `TransferRequest::with_source_version`, and a delete marker
/// holds nothing to copy.
fn refuse_listed_versions(observation: &ObservedEntry) -> Result<(), TransferFailure> {
    let Some(version) = observation.version() else {
        return Ok(());
    };
    let reason = if version.is_delete_marker() {
        "the observation is a delete marker, which holds nothing to copy"
    } else if !version.is_latest() {
        "the observation is an older version; expert transfers copy the current version only \
         (copy it with TransferRequest::with_source_version)"
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
