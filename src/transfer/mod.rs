//! Streaming transfer requests, resumability, state, and outcomes.

#[allow(dead_code)]
mod engine;
mod identity;
mod model;
mod recovery_store;

pub use crate::model::SourceVersion;
pub use crate::storage::{PrepareFact, RestartReason};
pub use crate::storage::{SourceQosGroup, SourceQosPolicy, SourceQosStats, SourceQosValueError};
pub use engine::{
    EffectiveRecovery, ExpertDestinationRequest, ExpertDestinationSession,
    ExpertDestinationTransferred, ExpertSourceEvidence, ExpertSourceOffer, ExpertSourcePayload,
    ExpertSourceRequest, ExpertSourceSession, TransferFailure, TransferOutcome, TransferPhase,
    TransferRoute, TransferSide, transfer,
};
pub use identity::TransferIdentity;
pub use model::{
    CopiedMetadataRequest, InflightLimits, PayloadShapingPolicy, ReadBackVerification,
    TransferPolicy, TransferRequest, TransferValueError,
};

#[cfg(test)]
mod at_destination_tests;
#[cfg(test)]
mod hdfs_policy_tests;
#[cfg(test)]
mod hdfs_tests;
#[cfg(test)]
mod s3_native_tests;
#[cfg(test)]
mod source_version_tests;
#[cfg(test)]
mod tests;
