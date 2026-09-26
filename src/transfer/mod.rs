//! Streaming transfer requests, resumability, state, and outcomes.

#[allow(dead_code)]
mod engine;
mod identity;
mod model;

pub use crate::model::SourceVersion;
pub use crate::storage::{DeleteMarkerOutcome, PrepareFact, RestartReason};
pub use crate::storage::{SourceQosGroup, SourceQosPolicy, SourceQosStats, SourceQosValueError};
pub use engine::{
    DIRECTORY_OBJECT_SKIP_REASON, DirectoryObjectAction, EffectiveRecovery,
    ExpertDestinationRequest, ExpertDestinationSession, ExpertDestinationTransferred,
    ExpertSourceEvidence, ExpertSourceOffer, ExpertSourcePayload, ExpertSourceRequest,
    ExpertSourceSession, TransferFailure, TransferOutcome, TransferPhase, TransferRoute,
    TransferSide, destination_keeps_history, directory_object_action, transfer,
    write_delete_marker,
};
pub use identity::TransferIdentity;
pub use model::{
    CopiedMetadataRequest, InflightLimits, PayloadShapingPolicy, ReadBackVerification,
    TransferPolicy, TransferRequest, TransferValueError,
};

#[cfg(test)]
mod at_destination_tests;
#[cfg(test)]
mod delete_marker_tests;
#[cfg(test)]
mod directory_object_tests;
#[cfg(test)]
mod hdfs_policy_tests;
#[cfg(test)]
mod hdfs_tests;
#[cfg(test)]
mod s3_at_destination_tests;
#[cfg(test)]
mod s3_direct_tests;
#[cfg(test)]
mod s3_expert_listing_tests;
#[cfg(test)]
mod s3_native_final_tests;
#[cfg(test)]
mod s3_native_tests;
#[cfg(test)]
mod s3_single_put_tests;
#[cfg(test)]
mod s3_versioning_tests;
#[cfg(test)]
mod source_version_tests;
#[cfg(test)]
mod tests;
