//! Connected storage construction, roles, capabilities, and backend adapters.

pub(crate) mod backends;
mod capability;
mod factory;
mod handle;
mod native;
mod roles;

pub(crate) use native::{
    NativeAffinity, NativeEndpoint, NativePair, NativeRecoveryMode, NativeSourceBinding,
    NativeStageEvidence, NativeStageFailure,
};

pub use crate::runtime::qos::{
    SourceQosBudget, SourceQosGroup, SourceQosPolicy, SourceQosStats, SourceQosValueError,
};
pub use capability::{
    BackendCapabilities, Capability, CapabilityAvailability, CapabilityUnavailable,
    CapabilityValueError, PreflightPolicy, UnsupportedReason, ValidationGate,
};
pub use factory::{
    BackendConfig, BackendConnectError, CifsBackendConfig, HdfsBackendConfig, LocalBackendConfig,
    NfsBackendConfig, S3BackendConfig, connect_backend,
};
pub use handle::Storage;
pub use roles::{
    ByteStream, CheckpointObservation, ExistingDestinationPolicy, FinalDestination, Metadata,
    MetadataMutation, Namespace, NamespaceRequest, NamespaceResult, PrepareRequest, PreparedStage,
    PublicationDisposition, PublicationEvidence, PublicationFailure, PublishRequest, ReadRequest,
    ReadSource, RecoverRequest, RecoveryIdentity, RecoveryValueError, SourceDescriptor,
    StagedDestination, StorageRoleFailure, VerificationEvidence, VerifyRequest, WriteEvidence,
};
