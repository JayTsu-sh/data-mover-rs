//! Connected storage construction, roles, capabilities, and backend adapters.

pub(crate) mod artifacts;
pub(crate) mod backends;
mod capability;
mod copied_metadata;
mod create_dir;
mod delete_tree;
mod descriptor;
pub(crate) mod discovery;
pub(crate) mod durability;
pub(crate) mod endpoint;
mod factory;
mod handle;
mod native;
pub(crate) mod pointer;
mod roles;

pub(crate) use native::{
    NativeAffinity, NativeEndpoint, NativePair, NativeSourceBinding, NativeStageEvidence,
    NativeStageFailure,
};

pub use crate::runtime::qos::{
    SourceQosBudget, SourceQosGroup, SourceQosPolicy, SourceQosStats, SourceQosValueError,
};
pub use capability::{
    BackendCapabilities, Capability, CapabilityAvailability, CapabilityUnavailable,
    CapabilityValueError, PreflightPolicy, UnsupportedReason, ValidationGate,
};
pub use copied_metadata::{
    CopiedAclTarget, CopiedMetadataObservation, CopiedMetadataTarget, CopiedOwnershipTarget,
    CopiedTimestampTarget, CopiedValueTarget,
};
pub use create_dir::{CreateDirectoryAllFailure, create_directory_all};
pub use delete_tree::{
    DeleteTreeCompletion, DeleteTreeItem, DeleteTreeOutcome, DeleteTreeRequest, DeleteTreeSession,
    DeleteTreeTerminalFailure, delete_tree,
};
pub(crate) use descriptor::ListingFacts;
pub use descriptor::SourceDescriptor;
pub use discovery::{DestinationPrepareRequest, PrepareFact, RestartReason, ResumeMode};
pub use factory::{
    BackendConfig, BackendConnectError, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy,
    HdfsBackendConfig, LocalBackendConfig, NfsBackendConfig, S3BackendConfig, connect_backend,
    endpoint_identity,
};
pub use handle::Storage;
pub(crate) use roles::DeferredCheckpoint;
pub use roles::{
    ByteStream, CheckpointObservation, FinalDestination, Metadata, MetadataMutation, Namespace,
    NamespaceRequest, NamespaceResult, PositionedByteStream, PositionedChunk, PrepareRequest,
    PreparedStage, PublicationDisposition, PublicationEvidence, PublicationFailure, PublishRequest,
    ReadRequest, ReadSource, StagedDestination, StagedMetadataApplicationFailure,
    StorageRoleFailure, VerificationEvidence, VerificationPoint, VerifyRequest, WriteEvidence,
};

pub use crate::runtime::read_budget::ReadBudget;

// Keep backend prefetch coupled to storage admission, not runtime internals.
pub(crate) use crate::runtime::inflight::InflightAdmission as ReadAdmission;
#[cfg(test)]
pub(crate) use crate::runtime::inflight::{InflightConfig, InflightRuntime};
