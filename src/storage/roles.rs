use std::ops::Range;
use std::pin::Pin;
use std::{error::Error, fmt};

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use tokio_util::sync::CancellationToken;

use crate::runtime::qos::SourceQosBudget;

use crate::model::{
    AclMetadata, BackendSessionFailure, EntryKind, EntryOperationFailure, ExtendedAttribute,
    MappedOwnership, MetadataObservations, ObjectTag, ObservationPlan, OwnershipMode,
    SourceIdentity, StoragePath, TimestampMetadata,
};

/// A bounded payload stream. Implementations own request sizing and backpressure.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, StorageRoleFailure>> + Send>>;

/// A stable neutral source description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceDescriptor {
    pub path: StoragePath,
    pub kind: EntryKind,
    pub size: Option<u64>,
    pub source_identity: SourceIdentity,
}

/// One bounded sequential or range read.
#[derive(Clone, Debug)]
pub struct ReadRequest {
    pub path: StoragePath,
    pub range: Option<Range<u64>>,
    pub expected_source: Option<SourceIdentity>,
    pub cancel: CancellationToken,
    pub source_qos: Option<SourceQosBudget>,
}

/// Backend-neutral failure scope for a role operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageRoleFailure {
    Entry(EntryOperationFailure),
    Session(BackendSessionFailure),
}

impl fmt::Display for StorageRoleFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Entry(error) => error.fmt(formatter),
            Self::Session(error) => error.fmt(formatter),
        }
    }
}

impl Error for StorageRoleFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Entry(error) => Some(error),
            Self::Session(error) => Some(error),
        }
    }
}

/// Source streaming role. Protocol handles and retry details remain behind this interface.
#[async_trait]
pub trait ReadSource: Send + Sync {
    async fn describe(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure>;
    async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure>;
}

/// Request to prepare unpublished destination state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrepareRequest {
    pub final_destination: FinalDestination,
    pub source: SourceDescriptor,
    pub recovery_binding: [u8; 32],
}

/// Failure to reconstruct an opaque recovery identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryValueError;

impl fmt::Display for RecoveryValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("recovery identity must be non-empty and bounded")
    }
}

impl Error for RecoveryValueError {}

/// Versioned opaque backend recovery identity persisted without interpretation.
#[derive(Clone, Eq, PartialEq)]
pub struct RecoveryIdentity(Bytes);

impl RecoveryIdentity {
    /// Reconstructs a bounded identity from persisted bytes.
    ///
    /// # Errors
    /// Returns an error for empty or oversized identities.
    pub fn from_bytes(bytes: impl Into<Bytes>) -> Result<Self, RecoveryValueError> {
        let bytes = bytes.into();
        if bytes.is_empty() || bytes.len() > 4096 {
            Err(RecoveryValueError)
        } else {
            Ok(Self(bytes))
        }
    }

    /// Returns opaque bytes for persistence.
    #[must_use]
    pub const fn as_bytes(&self) -> &Bytes {
        &self.0
    }
}

impl fmt::Debug for RecoveryIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RecoveryIdentity(<opaque>)")
    }
}

/// Inputs used by a backend to revalidate one recovery identity.
#[derive(Clone, Debug)]
pub struct RecoverRequest {
    pub identity: RecoveryIdentity,
    pub final_destination: FinalDestination,
    pub source: SourceDescriptor,
    pub recovery_binding: [u8; 32],
}

/// A typed destination that remains unchanged until publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalDestination(StoragePath);

impl FinalDestination {
    #[must_use]
    pub const fn new(path: StoragePath) -> Self {
        Self(path)
    }
    #[must_use]
    pub const fn path(&self) -> &StoragePath {
        &self.0
    }
}

/// Opaque linear prepared destination state bound to one backend and final destination.
pub struct PreparedStage {
    pub(crate) owner: crate::model::BackendIdentity,
    pub(crate) final_destination: FinalDestination,
    pub(crate) token: Bytes,
    pub(crate) recovery_binding: [u8; 32],
    pub(crate) write_offset: u64,
    pub(crate) claim: std::sync::Mutex<Option<std::fs::File>>,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StageBindingError;

#[allow(dead_code)]
impl PreparedStage {
    pub(crate) fn new(
        owner: crate::model::BackendIdentity,
        final_destination: FinalDestination,
        token: Bytes,
        recovery_binding: [u8; 32],
        write_offset: u64,
        claim: Option<std::fs::File>,
    ) -> Self {
        Self {
            owner,
            final_destination,
            token,
            recovery_binding,
            write_offset,
            claim: std::sync::Mutex::new(claim),
        }
    }

    pub(crate) fn validate_owner(
        &self,
        owner: &crate::model::BackendIdentity,
    ) -> Result<(), StageBindingError> {
        if &self.owner == owner {
            Ok(())
        } else {
            Err(StageBindingError)
        }
    }

    pub(crate) fn release_claim(&self) {
        self.claim
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
}

impl fmt::Debug for PreparedStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedStage")
            .field("owner", &self.owner)
            .field("final_destination", &self.final_destination)
            .field("token", &"<redacted>")
            .field("recovery_binding", &"<redacted>")
            .field("write_offset", &self.write_offset)
            .field("claim", &"<exclusive-lock>")
            .finish()
    }
}

/// Evidence returned after backend persistence barriers complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteEvidence {
    pub persisted_bytes: u64,
}

/// Backend-observed reusable work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointObservation {
    pub durable_prefix: u64,
}

/// Requested staged-content verification.
#[derive(Clone, Debug)]
pub struct VerifyRequest {
    pub expected_size: u64,
    pub expected_blake3: [u8; 32],
    pub cancel: CancellationToken,
}

/// Policy for a destination path that already exists at publication time.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ExistingDestinationPolicy {
    /// Atomically replace an existing destination.
    #[default]
    Overwrite,
    /// Keep equivalent existing content, otherwise fail.
    VerifyOrSkip,
    /// Fail without changing an existing destination.
    FailIfExists,
}

/// Evidence that staged content passed verification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerificationEvidence {
    pub verified_bytes: u64,
    pub blake3: [u8; 32],
}

/// Inputs required to publish verified staged content.
#[derive(Clone, Debug)]
pub struct PublishRequest {
    pub policy: ExistingDestinationPolicy,
    pub expected_size: u64,
    pub expected_blake3: [u8; 32],
    pub cancel: CancellationToken,
}

/// Publication failure with an explicit atomic commit boundary.
#[derive(Clone, Debug)]
pub struct PublicationFailure {
    pub error: StorageRoleFailure,
    pub final_destination_changed: bool,
}

/// Result of publication when equivalent existing content may be retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationDisposition {
    Published,
    ExistingEquivalent,
}

/// Evidence that verified staged state was published.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationEvidence {
    pub final_destination: StoragePath,
    pub disposition: PublicationDisposition,
}

/// Destination role owning prepare, write, checkpoint, verify, publish, and discard.
#[async_trait]
pub trait StagedDestination: Send + Sync {
    async fn prepare(&self, request: PrepareRequest) -> Result<PreparedStage, StorageRoleFailure>;
    async fn recovery_identity(
        &self,
        stage: &PreparedStage,
    ) -> Result<RecoveryIdentity, StorageRoleFailure>;
    async fn recover(&self, request: RecoverRequest) -> Result<PreparedStage, StorageRoleFailure>;
    async fn write(
        &self,
        stage: &PreparedStage,
        input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure>;
    async fn observe_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<CheckpointObservation, StorageRoleFailure>;
    async fn verify(
        &self,
        stage: &PreparedStage,
        request: VerifyRequest,
    ) -> Result<VerificationEvidence, StorageRoleFailure>;
    async fn publish(
        &self,
        stage: &PreparedStage,
        request: PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure>;
    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure>;
}

/// One coherent namespace operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NamespaceRequest {
    Stat(StoragePath),
    List(StoragePath),
    CreateDirectory(StoragePath),
    Delete(StoragePath),
    Rename { from: StoragePath, to: StoragePath },
}

/// Neutral namespace result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NamespaceResult {
    Completed,
    Entries(Vec<SourceDescriptor>),
}

/// Coherent namespace role with typed verb availability behind one interface.
#[async_trait]
pub trait Namespace: Send + Sync {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure>;
}

/// Metadata observation and application role. It never implicitly refetches omitted facts.
#[async_trait]
pub trait Metadata: Send + Sync {
    async fn observe(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure>;
    async fn apply(
        &self,
        path: &StoragePath,
        mutation: MetadataMutation,
        cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure>;
}

/// One backend-neutral metadata mutation compiled before target side effects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataMutation {
    Acl(AclMetadata),
    Xattrs(Vec<ExtendedAttribute>),
    Tags(Vec<ObjectTag>),
    NumericOwnership(OwnershipMode),
    MappedOwnership(MappedOwnership),
    Timestamps(TimestampMetadata),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{BackendIdentity, BackendKind};

    #[test]
    fn prepared_stage_binds_owner_and_final_destination() -> Result<(), Box<dyn Error>> {
        let owner = BackendIdentity::new(BackendKind::S3, "destination")?;
        let other = BackendIdentity::new(BackendKind::S3, "other")?;
        let destination = FinalDestination::new(StoragePath::new("bucket/key")?);
        let stage = PreparedStage::new(
            owner.clone(),
            destination.clone(),
            Bytes::from_static(b"secret-token"),
            [0; 32],
            0,
            None,
        );

        assert_eq!(stage.final_destination, destination);
        assert!(stage.validate_owner(&owner).is_ok());
        assert!(stage.validate_owner(&other).is_err());
        assert!(!format!("{stage:?}").contains("secret-token"));
        Ok(())
    }
}
