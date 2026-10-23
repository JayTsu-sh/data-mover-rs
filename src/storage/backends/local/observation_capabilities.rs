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

pub(super) fn copied_metadata_observation_plan() -> ObservationPlan {
    // Explicit ownership also records NotApplicable on Windows, whose ownership is non-numeric.
    // Omitting it would leave NotRequested and prevent AllowKnownLoss from accepting the result.
    ObservationPlan::default()
        .with_ownership_mode(ObservationMode::InlineOnly)
        .with_timestamps(ObservationMode::InlineOnly)
}
