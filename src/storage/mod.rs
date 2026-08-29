//! Connected storage construction, roles, capabilities, and backend adapters.

pub(crate) mod backends;
mod capability;
mod handle;
mod roles;

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
