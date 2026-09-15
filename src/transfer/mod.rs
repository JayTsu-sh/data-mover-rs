//! Streaming transfer requests, resumability, state, and outcomes.

#[allow(dead_code)]
mod engine;
mod model;
mod recovery_store;

pub use crate::storage::{SourceQosGroup, SourceQosPolicy, SourceQosStats, SourceQosValueError};
pub use engine::{
    EffectiveRecovery, ExpertDestinationRequest, ExpertDestinationSession,
    ExpertDestinationTransferred, ExpertSourceEvidence, ExpertSourceOffer, ExpertSourcePayload,
    ExpertSourceRequest, ExpertSourceSession, TransferFailure, TransferOutcome, TransferPhase,
    TransferRoute, TransferSide, transfer,
};
pub use model::{
    InflightLimits, PayloadShapingPolicy, ReadBackVerification, TransferIdentity, TransferPolicy,
    TransferRequest, TransferValueError,
};

#[cfg(test)]
mod hdfs_tests;
#[cfg(test)]
mod s3_native_tests;
#[cfg(test)]
mod tests;
