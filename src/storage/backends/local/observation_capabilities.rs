//! Platform observation support and the metadata copied by default.

use crate::model::{
    MetadataCapabilities, MetadataFamily as F, ObservationMode, ObservationPlan,
    ObservationSupport as S,
};

pub(super) fn observation_capabilities() -> MetadataCapabilities {
    MetadataCapabilities::default()
        .with(
            F::Acl,
            if cfg!(any(unix, windows)) {
                S::Supported
            } else {
                S::Unsupported
            },
        )
        .with(
            F::Xattrs,
            if cfg!(unix) {
                S::Supported
            } else {
                S::Unsupported
            },
        )
        .with(F::Tags, S::NotApplicable)
        .with(
            F::OwnershipMode,
            if cfg!(unix) {
                S::Supported
            } else {
                S::NotApplicable
            },
        )
        .with(F::Timestamps, S::Supported)
}

pub(super) fn copied_metadata_observation_plan() -> Option<ObservationPlan> {
    #[cfg(unix)]
    {
        Some(
            ObservationPlan::default()
                .with_ownership_mode(ObservationMode::InlineOnly)
                .with_timestamps(ObservationMode::InlineOnly),
        )
    }
    #[cfg(windows)]
    {
        // Ask explicitly so the observation records `NotApplicable` for
        // Windows' non-numeric ownership model. Leaving this omitted
        // would produce `NotRequested`, which AllowKnownLoss correctly
        // refuses because it cannot distinguish unsupported ownership
        // from an observation bug.
        Some(
            ObservationPlan::default()
                .with_ownership_mode(ObservationMode::InlineOnly)
                .with_timestamps(ObservationMode::InlineOnly),
        )
    }
    #[cfg(not(any(unix, windows)))]
    {
        None
    }
}
