//! Connected storage construction, roles, capabilities, and backend adapters.

#[doc(hidden)]
pub mod artifacts;
mod capability;
#[doc(hidden)]
pub mod checkpoint;
#[doc(hidden)]
pub mod checkpoint_stream;
#[cfg(test)]
mod checkpoint_stream_tests;
pub mod config;
mod copied_metadata;
#[doc(hidden)]
pub mod create_dir;
#[doc(hidden)]
pub mod delete_tree;
mod descriptor;
#[doc(hidden)]
pub mod discovery;
#[doc(hidden)]
pub mod durability;
#[doc(hidden)]
pub mod endpoint;
mod factory;
mod handle;
pub mod listing;
mod native;
mod permissions;
pub use permissions::{PermissionFacts, PrincipalIdentity};
#[doc(hidden)]
pub mod pointer;
#[doc(hidden)]
pub mod range_hash;
#[doc(hidden)]
pub mod roles;
mod write_progress;
pub use write_progress::{WriteProgress, WriteProgressObserver};
#[doc(hidden)]
pub mod throttle;

pub use native::{
    NativeAffinity, NativeEndpoint, NativePair, NativeSourceBinding, NativeStageEvidence,
    NativeStageFailure,
};

pub use crate::runtime::qos::{
    SourceQosBudget, SourceQosGroup, SourceQosPolicy, SourceQosStats, SourceQosValueError,
};
pub use capability::{
    BackendCapabilities, Capability, CapabilityAvailability, CapabilityUnavailable,
    CapabilityValueError, UnsupportedReason,
};
pub use config::hdfs::{HdfsConfig, HdfsEndpointKind, HdfsKerberosCredentials, HdfsLocation};
pub use copied_metadata::{
    CopiedAclTarget, CopiedMetadataObservation, CopiedMetadataTarget, CopiedOwnershipTarget,
    CopiedTimestampTarget, CopiedValueTarget,
};
pub use create_dir::{CreateDirectoryAllFailure, create_directory_all};
pub use delete_tree::{
    DeleteTreeCompletion, DeleteTreeItem, DeleteTreeOutcome, DeleteTreeRequest, DeleteTreeSession,
    DeleteTreeTerminalFailure, delete_tree,
};
pub use descriptor::SourceDescriptor;
pub use descriptor::{InlineOwnership, ListingFacts};
pub use discovery::{DestinationPrepareRequest, PrepareFact, RestartReason, ResumeMode};
pub use factory::{
    BackendConfig, BackendConnectError, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy,
    HdfsBackendConfig, LocalBackendConfig, NfsBackendConfig, S3BackendConfig, endpoint_identity,
};
pub use handle::{Storage, StorageBuildError, StorageBuilder};
pub use listing::{
    ChildFailure, CollectLimits, DescentHint, ListRequest, ListedChild, ListingPage, ListingStream,
    PageSink, collect_listing,
};
pub use roles::DeferredCheckpoint;
pub use roles::{
    ByteStream, CaseSensitivity, CheckpointObservation, CreateSymlinkRequest, DeleteMarkerOutcome,
    FinalDestination, Metadata, MetadataMutation, NameNormalization, NameSemantics, Namespace,
    NamespaceCapabilities, PositionedByteStream, PositionedChunk, PrepareRequest, PreparedStage,
    PublicationDisposition, PublicationEvidence, PublicationFailure, PublishRequest,
    ReadBackMethod, ReadRequest, ReadSource, StagedDestination, StagedMetadataApplicationFailure,
    StorageRoleFailure, UnpinnedReadReason, UploadDigest, UploadDigestGap, VerificationEvidence,
    VerificationPoint, VerifyRequest, WriteEvidence,
};

pub use crate::runtime::read_budget::ReadBudget;

// Keep backend prefetch coupled to storage admission, not runtime internals.
pub use crate::runtime::inflight::InflightAdmission as ReadAdmission;
#[cfg(any(test, feature = "test-support"))]
pub use crate::runtime::inflight::{InflightConfig, InflightRuntime};

#[doc(hidden)]
pub mod defaults;
