use std::fmt;

use crate::model::{
    MetadataCapabilities, MetadataFamily, MetadataRequirements, ObservationMode, ObservationSupport,
};

use super::TraversalRequest;

/// A traversal configuration refused before listing or metadata I/O.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TraversalConfigurationError {
    /// The filter reads a family omitted by the caller's observation plan.
    OmittedFilterMetadata(MetadataFamily),
    /// The caller requires a family this connected backend cannot observe.
    UnsupportedMetadata(MetadataFamily),
}

impl fmt::Display for TraversalConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OmittedFilterMetadata(family) => {
                write!(
                    formatter,
                    "the filter requires omitted metadata: {family:?}"
                )
            }
            Self::UnsupportedMetadata(family) => {
                write!(formatter, "required metadata is unsupported: {family:?}")
            }
        }
    }
}

impl std::error::Error for TraversalConfigurationError {}

pub(super) fn validate(
    request: &TraversalRequest,
    capabilities: MetadataCapabilities,
) -> Result<MetadataRequirements, TraversalConfigurationError> {
    let requirements = request
        .filter
        .as_ref()
        .map_or(MetadataRequirements::NONE, |filter| {
            filter.required_metadata()
        });
    for family in requirements.iter() {
        if request.observation_plan.mode(family) == ObservationMode::Omit {
            return Err(TraversalConfigurationError::OmittedFilterMetadata(family));
        }
    }
    for family in MetadataFamily::ALL {
        if request.observation_plan.mode(family) == ObservationMode::Required
            && capabilities.support(family) == ObservationSupport::Unsupported
        {
            return Err(TraversalConfigurationError::UnsupportedMetadata(family));
        }
    }
    Ok(requirements)
}
