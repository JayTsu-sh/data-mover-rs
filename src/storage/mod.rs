//! Connected storage construction, roles, capabilities, and backend adapters.

use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;

pub(crate) mod backends;
mod capability;
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
pub use handle::Storage;
pub use roles::{
    ByteStream, CheckpointObservation, ExistingDestinationPolicy, FinalDestination, Metadata,
    MetadataMutation, Namespace, NamespaceRequest, NamespaceResult, PrepareRequest, PreparedStage,
    PublicationDisposition, PublicationEvidence, PublicationFailure, PublishRequest, ReadRequest,
    ReadSource, RecoverRequest, RecoveryIdentity, RecoveryValueError, SourceDescriptor,
    StagedDestination, StorageRoleFailure, VerificationEvidence, VerifyRequest, WriteEvidence,
};

use crate::model::BackendIdentity;

/// Typed configuration for a connected Local transfer endpoint.
#[derive(Clone, Debug)]
pub struct LocalTransferConfig {
    pub root: PathBuf,
    pub identity: BackendIdentity,
    pub write_concurrency: NonZeroUsize,
}

/// Failure while connecting the Local transfer roles.
#[derive(Debug)]
pub enum LocalTransferConnectError {
    Source(StorageRoleFailure),
    Destination(StorageRoleFailure),
    Capability(CapabilityValueError),
    Invariant,
}

impl fmt::Display for LocalTransferConnectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(_) => formatter.write_str("Local source role connection failed"),
            Self::Destination(_) => formatter.write_str("Local destination role connection failed"),
            Self::Capability(error) => error.fmt(formatter),
            Self::Invariant => formatter.write_str("Local connected roles contradict capabilities"),
        }
    }
}

impl std::error::Error for LocalTransferConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(error) | Self::Destination(error) => Some(error),
            Self::Capability(error) => Some(error),
            Self::Invariant => None,
        }
    }
}

/// Connects architecture-ready Local source and staged-destination roles.
///
/// # Errors
/// Returns a typed role, capability, or construction failure without falling back to legacy
/// `StorageEnum` behavior.
pub fn connect_local_transfer(
    config: LocalTransferConfig,
) -> Result<Storage, LocalTransferConnectError> {
    backends::local::connect_transfer(config)
}
