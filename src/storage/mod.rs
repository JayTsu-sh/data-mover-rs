//! Connected storage construction, roles, capabilities, and backend adapters.

pub(crate) mod backends;
mod capability;
mod handle;
mod roles;

pub use capability::{
    BackendCapabilities, Capability, CapabilityAvailability, CapabilityUnavailable,
    CapabilityValueError, PreflightPolicy, UnsupportedReason, ValidationGate,
};
pub use handle::Storage;
pub use roles::{
    ByteStream, CheckpointObservation, FinalDestination, Metadata, Namespace, NamespaceRequest,
    NamespaceResult, PrepareRequest, PreparedStage, PublicationEvidence, ReadRequest, ReadSource,
    SourceDescriptor, StagedDestination, StorageRoleFailure, VerificationEvidence, VerifyRequest,
    WriteEvidence,
};
