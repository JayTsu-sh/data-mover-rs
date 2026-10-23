//! Connected storage construction, roles, capabilities, and backend adapters.

pub(crate) mod artifacts;
pub(crate) mod backends;
mod capability;
pub(crate) mod checkpoint;
pub(crate) mod checkpoint_stream;
#[cfg(test)]
mod checkpoint_stream_tests;
mod copied_metadata;
mod create_dir;
mod delete_tree;
mod descriptor;
pub(crate) mod discovery;
pub(crate) mod durability;
pub(crate) mod endpoint;
mod factory;
mod handle;
pub mod listing;
mod native;
mod permissions;
pub use permissions::{PermissionFacts, PrincipalIdentity};
pub(crate) mod pointer;
pub(crate) mod range_hash;
mod roles;
mod write_progress;
pub use write_progress::{WriteProgress, WriteProgressObserver};
pub(crate) mod throttle;

pub(crate) use native::{
    NativeAffinity, NativeEndpoint, NativePair, NativeSourceBinding, NativeStageEvidence,
    NativeStageFailure,
};

pub use crate::runtime::qos::{
    SourceQosBudget, SourceQosGroup, SourceQosPolicy, SourceQosStats, SourceQosValueError,
};
// Configuration and resource discovery are the intentional public backend-specific seam.
pub use backends::hdfs::{
    HdfsConfig, HdfsEndpointKind, HdfsKerberosCredentials, HdfsLocation, build_hdfs_client,
};
#[cfg(windows)]
pub use backends::local::windows_acl;
pub use backends::nfs::list_exports;
pub use backends::s3::{S3BucketInfo, list_buckets};
pub use capability::{
    BackendCapabilities, Capability, CapabilityAvailability, CapabilityUnavailable,
    CapabilityValueError, UnsupportedReason,
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
pub use descriptor::SourceDescriptor;
pub(crate) use descriptor::{InlineOwnership, ListingFacts};
pub use discovery::{DestinationPrepareRequest, PrepareFact, RestartReason, ResumeMode};
pub use factory::{
    BackendConfig, BackendConnectError, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy,
    HdfsBackendConfig, LocalBackendConfig, NfsBackendConfig, S3BackendConfig, connect_backend,
    endpoint_identity,
};
pub use handle::Storage;
pub use listing::{
    ChildFailure, CollectLimits, DescentHint, ListRequest, ListedChild, ListingPage, ListingStream,
    PageSink, collect_listing,
};
pub(crate) use roles::DeferredCheckpoint;
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
pub(crate) use crate::runtime::inflight::InflightAdmission as ReadAdmission;
#[cfg(test)]
pub(crate) use crate::runtime::inflight::{InflightConfig, InflightRuntime};
