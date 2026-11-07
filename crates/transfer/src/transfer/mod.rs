//! Streaming transfer requests, resumability, state, and outcomes.

#[allow(dead_code)]
mod engine;
pub(crate) use engine::validate_observation;
mod identity;
mod model;

pub use crate::model::SourceVersion;
pub use crate::storage::{
    DeleteMarkerOutcome, PrepareFact, ReadBackMethod, RestartReason, UnpinnedReadReason,
    UploadDigest, UploadDigestGap,
};
pub use crate::storage::{SourceQosGroup, SourceQosPolicy, SourceQosStats, SourceQosValueError};
pub use engine::{
    DIRECTORY_OBJECT_SKIP_REASON, DIRECTORY_OBJECT_TOO_LARGE_REASON, DirectPatchChunk,
    DirectPatchError, DirectPatchInput, DirectPatchProgress, DirectPatchSession,
    DirectoryObjectAction, DirectoryObjectDestination, EffectiveRecovery, ExpertDestinationRequest,
    ExpertDestinationSession, ExpertDestinationTransferred, ExpertDestinationWrite,
    ExpertInputProgress, ExpertSourceEvidence, ExpertSourceOffer, ExpertSourcePayload,
    ExpertSourceRequest, ExpertSourceSession, TransferFailure, TransferOutcome, TransferPhase,
    TransferRoute, TransferSide, TransferVerification, destination_keeps_history,
    directory_object_action, transfer, write_delete_marker,
};
pub use identity::TransferIdentity;
pub use model::{
    CopiedMetadataRequest, MetadataFailurePolicy, MetadataTransferMode, PublicationDurability,
    ReadBackVerification, TransferPolicy, TransferRequest, TransferValueError,
};

#[cfg(test)]
mod at_destination_tests;
#[cfg(test)]
mod checkpoint_policy_tests;
#[cfg(test)]
mod delete_marker_tests;
#[cfg(test)]
mod directory_object_tests;
#[cfg(test)]
mod hdfs_policy_tests;
#[cfg(test)]
mod hdfs_tests;
#[cfg(test)]
mod observation_tests;
#[cfg(test)]
mod s3_at_destination_tests;
#[cfg(test)]
mod s3_copied_tags_tests;
#[cfg(test)]
mod s3_direct_native_tests;
#[cfg(test)]
mod s3_direct_tests;
#[cfg(test)]
mod s3_expert_listing_tests;
#[cfg(test)]
mod s3_nas_tests;
#[cfg(test)]
mod s3_native_final_tests;
#[cfg(test)]
mod s3_native_tests;
#[cfg(test)]
mod s3_single_put_tests;
#[cfg(test)]
mod s3_source_qos_tests;
#[cfg(test)]
mod s3_versioning_tests;
#[cfg(test)]
mod source_version_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod write_progress_tests;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod caller_metadata_tests;
#[cfg(all(test, unix))]
#[allow(clippy::unwrap_used)]
mod metadata_failure_tests;
