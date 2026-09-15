//! Connected storage construction, roles, capabilities, and backend adapters.

pub(crate) mod artifacts;
pub(crate) mod backends;
mod capability;
pub(crate) mod durability;
mod factory;
mod handle;
mod native;
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
pub use factory::{
    BackendConfig, BackendConnectError, CifsBackendConfig, HdfsBackendConfig, LocalBackendConfig,
    NfsBackendConfig, S3BackendConfig, connect_backend,
};
pub use handle::Storage;
pub use roles::{
    ByteStream, CheckpointObservation, CopiedMetadataTarget, FinalDestination, Metadata,
    MetadataMutation, Namespace, NamespaceRequest, NamespaceResult, PrepareRequest, PreparedStage,
    PublicationDisposition, PublicationEvidence, PublicationFailure, PublishRequest, ReadRequest,
    ReadSource, RecoverRequest, RecoveryIdentity, RecoveryValueError, SourceDescriptor,
    StagedDestination, StagedMetadataApplicationFailure, StorageRoleFailure, VerificationEvidence,
    VerifyRequest, WriteEvidence,
};
pub(crate) use roles::{CheckpointRegistration, DeferredCheckpoint};

/// Local transfer files use a reserved hidden namespace in destination parents.
pub(crate) fn is_local_transfer_artifact(name: &std::ffi::OsStr) -> bool {
    name.to_str()
        .is_some_and(|name| name.starts_with(".data-mover-"))
}

pub use crate::runtime::read_budget::ReadBudget;

// Keep backend prefetch coupled to storage admission, not runtime internals.
pub(crate) use crate::runtime::inflight::InflightAdmission as ReadAdmission;
#[cfg(test)]
pub(crate) use crate::runtime::inflight::{InflightConfig, InflightRuntime};
