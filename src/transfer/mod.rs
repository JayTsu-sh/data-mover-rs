//! Streaming transfer requests, resumability, state, and outcomes.

#[allow(dead_code)]
mod engine;
mod model;

pub use crate::storage::ExistingDestinationPolicy;
pub use crate::storage::RecoveryIdentity;
pub use crate::storage::{SourceQosGroup, SourceQosPolicy, SourceQosStats, SourceQosValueError};
pub use engine::{
    ExpertDestinationRequest, ExpertDestinationSession, ExpertDestinationTransferred,
    ExpertSourceEvidence, ExpertSourceOffer, ExpertSourcePayload, ExpertSourceRequest,
    ExpertSourceSession, TransferFailure, TransferOutcome, TransferPhase, TransferSide, transfer,
};
pub use model::{
    InflightLimits, PayloadShapingPolicy, RecoveryContext, RecoveryProvider, RecoveryRegistrar,
    RecoveryRegistrationFailure, Resumability, TransferIdentity, TransferRequest,
    TransferValueError,
};

#[cfg(test)]
mod hdfs_tests;
#[cfg(test)]
mod s3_native_tests;
#[cfg(test)]
mod tests;
