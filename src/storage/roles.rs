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
    FailureClass, MappedOwnership, MetadataObservations, ObjectTag, ObservationPlan, Operation,
    OwnershipMode, SourceIdentity, StoragePath, SymlinkTarget, TimePrecision, TimestampMetadata,
    Transience,
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
    pub(crate) backend_fact: Option<Bytes>,
    /// Content-change observation, separate from stable inode/file-handle identity.
    pub(crate) content_version: Option<Bytes>,
}

impl SourceDescriptor {
    /// Creates a neutral source descriptor without backend-private facts.
    #[must_use]
    pub fn new(
        path: StoragePath,
        kind: EntryKind,
        size: Option<u64>,
        source_identity: SourceIdentity,
    ) -> Self {
        Self {
            path,
            kind,
            size,
            source_identity,
            backend_fact: None,
            content_version: None,
        }
    }

    pub(crate) fn with_backend_fact(mut self, fact: Bytes) -> Self {
        self.backend_fact = Some(fact);
        self
    }
}

/// One bounded sequential or range read.
#[derive(Clone, Debug)]
pub struct ReadRequest {
    pub path: StoragePath,
    pub range: Option<Range<u64>>,
    pub expected_source: Option<SourceIdentity>,
    /// Caller-side upper bound; the backend may negotiate a smaller chunk.
    pub maximum_chunk_bytes: usize,
    /// Caller-side upper bound for backend read operations within this stream.
    pub read_inflight: usize,
    /// Shared pre-allocation admission, supplied by the transfer engine.
    pub read_budget: Option<super::ReadBudget>,
    pub cancel: CancellationToken,
    pub source_qos: Option<SourceQosBudget>,
}

/// Backend-neutral failure scope for a role operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageRoleFailure {
    Entry(EntryOperationFailure),
    Session(BackendSessionFailure),
}

/// Failure while applying an ordered batch of staged metadata mutations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedMetadataApplicationFailure {
    pub failed_index: usize,
    pub completed: usize,
    pub error: Option<StorageRoleFailure>,
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
    /// Whether this implementation reserves every prefetched read using `ReadRequest::read_budget`.
    /// Other sources are polled serially with admission owned by the producer.
    fn supports_read_budget(&self) -> bool {
        false
    }

    /// Largest payload the connected backend can return from one source read operation.
    /// The transfer planner combines this backend limit with the caller's inflight budget.
    fn maximum_read_chunk_bytes(&self) -> usize {
        usize::MAX
    }
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
    /// Caller-persisted identity for one recovery attempt, stable across process restart.
    pub claim_token: [u8; 32],
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
    pub(crate) recovery_enabled: std::sync::atomic::AtomicBool,
    pub(crate) registration_owned: std::sync::atomic::AtomicBool,
    pub(crate) deferred_checkpoint: Option<DeferredCheckpoint>,
    /// Whether the caller requires final publication persistence barriers.
    pub(crate) durable_publication: bool,
    /// Direct targets are already visible and must never enter stage cleanup or recovery.
    pub(crate) direct: bool,
    pub(crate) backend_state: Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>,
    pub(crate) claim: std::sync::Mutex<Option<std::fs::File>>,
    pub(crate) recovery_lease: std::sync::Mutex<Option<std::sync::Arc<std::fs::File>>>,
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
            recovery_enabled: std::sync::atomic::AtomicBool::new(true),
            registration_owned: std::sync::atomic::AtomicBool::new(true),
            deferred_checkpoint: None,
            durable_publication: true,
            direct: false,
            backend_state: None,
            claim: std::sync::Mutex::new(claim),
            recovery_lease: std::sync::Mutex::new(None),
        }
    }

    pub(crate) fn disable_recovery(self) -> Self {
        self.recovery_enabled
            .store(false, std::sync::atomic::Ordering::Release);
        self.registration_owned
            .store(false, std::sync::atomic::Ordering::Release);
        self
    }

    pub(crate) fn recovery_enabled(&self) -> bool {
        self.recovery_enabled
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn owns_recovery_registration(&self) -> bool {
        self.registration_owned
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) const fn recovery_binding(&self) -> [u8; 32] {
        self.recovery_binding
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

    pub(crate) fn retain_recovery_lease(&self, lease: std::sync::Arc<std::fs::File>) {
        *self
            .recovery_lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(lease);
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
            .field("recovery_enabled", &self.recovery_enabled())
            .field("claim", &"<exclusive-lock>")
            .field("recovery_lease", &"<exclusive-lock>")
            .field("registration_owned", &self.owns_recovery_registration())
            .field("deferred_checkpoint", &self.deferred_checkpoint.is_some())
            .field("durable_publication", &self.durable_publication)
            .field("direct", &self.direct)
            .field("backend_state", &"<opaque>")
            .finish()
    }
}

/// Evidence of completed backend writes under the stage publication policy.
/// `AtomicReplace` Local writes may still reside in the OS cache; this is not checkpoint evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteEvidence {
    /// Completed bytes; crash durability depends on the requested publication policy.
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

pub(crate) struct DeferredCheckpoint {
    pub(crate) interval_bytes: u64,
    pub(crate) source_size: u64,
    pub(crate) registration: std::sync::Arc<dyn CheckpointRegistration>,
}

#[async_trait]
pub(crate) trait CheckpointRegistration: Send + Sync {
    async fn register(
        &self,
        stage: &PreparedStage,
        identity: RecoveryIdentity,
    ) -> Result<(), StorageRoleFailure>;
}

/// Evidence that staged content passed verification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerificationEvidence {
    pub verified_bytes: u64,
    pub blake3: [u8; 32],
}

/// Inputs required to atomically replace the final destination with verified staged content.
#[derive(Clone, Debug)]
pub struct PublishRequest {
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

/// Result of publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationDisposition {
    Published,
}

/// Evidence that staged state was published; content verification is reported separately.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationEvidence {
    pub final_destination: StoragePath,
    pub disposition: PublicationDisposition,
}

/// Destination role owning prepare, write, checkpoint, verify, publish, and discard.
#[async_trait]
pub trait StagedDestination: Send + Sync {
    /// Whether this backend can prepare an in-place target for the shared writer.
    fn supports_direct(&self) -> bool {
        false
    }

    /// Opens a final target without staging. The returned handle must be marked direct.
    async fn prepare_direct(
        &self,
        request: PrepareRequest,
        _cancel: CancellationToken,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        Err(StorageRoleFailure::Entry(
            EntryOperationFailure::new(
                request.final_destination.path().clone(),
                Operation::Prepare,
                FailureClass::Unsupported,
                Transience::Permanent,
                "direct writes are unsupported by this destination",
            )
            .unwrap_or_else(|_| unreachable!("static diagnostic is valid")),
        ))
    }
    /// Target capabilities for baseline metadata copied by the ordinary transfer entry.
    fn copied_metadata_target(&self) -> Option<CopiedMetadataTarget> {
        None
    }

    /// Automatic checkpoint spacing, when deferred recovery is supported by this destination.
    fn automatic_checkpoint_interval_bytes(&self) -> Option<u64> {
        None
    }

    /// Writes one complete source chunk without requiring a separate producer task.
    async fn write_single(
        &self,
        stage: &PreparedStage,
        data: Bytes,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        self.write(
            stage,
            Box::pin(futures::stream::once(async move { Ok(data) })),
        )
        .await
    }

    async fn prepare(&self, request: PrepareRequest) -> Result<PreparedStage, StorageRoleFailure>;
    /// Prepares unpublished state that must never be resumed after this attempt.
    ///
    /// The default preserves backend staging and atomic-publication behavior while marking the
    /// returned state as ineligible for recovery. Backends with persistent checkpoint setup may
    /// override this method to omit that work.
    async fn prepare_ephemeral(
        &self,
        request: PrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        self.prepare(request)
            .await
            .map(PreparedStage::disable_recovery)
    }
    async fn recovery_identity(
        &self,
        stage: &PreparedStage,
    ) -> Result<RecoveryIdentity, StorageRoleFailure>;
    /// Transfers recovery authority out of the current process.
    ///
    /// The default identity snapshot is sufficient for backends whose authority is held by the
    /// stage itself. Backends with adapter-local claims may override this to release that claim
    /// only after the identity has been created successfully.
    async fn handoff_recovery(
        &self,
        stage: &PreparedStage,
    ) -> Result<RecoveryIdentity, StorageRoleFailure> {
        self.recovery_identity(stage).await
    }
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
    /// Applies one compiled metadata mutation to the unpublished staged object.
    ///
    /// The default is a truthful refusal. A backend must override this method before advertising
    /// the corresponding metadata target capability to the orchestration layer.
    async fn apply_metadata(
        &self,
        stage: &PreparedStage,
        _mutation: MetadataMutation,
        cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        let class = if cancel.is_cancelled() {
            FailureClass::Cancelled
        } else {
            FailureClass::Unsupported
        };
        let failure = EntryOperationFailure::new(
            stage.final_destination.path().clone(),
            Operation::Metadata,
            class,
            if class == FailureClass::Cancelled {
                Transience::Transient
            } else {
                Transience::Permanent
            },
            "staged metadata mutation is unavailable",
        )
        .unwrap_or_else(|_| unreachable!("the static staged-metadata diagnostic is valid"));
        Err(StorageRoleFailure::Entry(failure))
    }
    /// Applies an ordered metadata batch to the same unpublished staged object.
    ///
    /// Backends may override this to share one stage handle and one persistence barrier. The
    /// default preserves the per-mutation behavior of existing implementations.
    async fn apply_metadata_batch(
        &self,
        stage: &PreparedStage,
        mutations: Vec<MetadataMutation>,
        cancel: CancellationToken,
    ) -> Result<(), StagedMetadataApplicationFailure> {
        for (completed, mutation) in mutations.into_iter().enumerate() {
            if cancel.is_cancelled() {
                return Err(StagedMetadataApplicationFailure {
                    failed_index: completed,
                    completed,
                    error: None,
                });
            }
            if let Err(error) = self.apply_metadata(stage, mutation, cancel.clone()).await {
                return Err(StagedMetadataApplicationFailure {
                    failed_index: completed,
                    completed,
                    error: Some(error),
                });
            }
        }
        Ok(())
    }
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
    ReadLink(StoragePath),
    CreateDirectory(StoragePath),
    Delete(StoragePath),
    Rename { from: StoragePath, to: StoragePath },
}

/// Neutral namespace result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NamespaceResult {
    Completed,
    Entries(Vec<SourceDescriptor>),
    LinkTarget(SymlinkTarget),
}

/// Coherent namespace role with typed verb availability behind one interface.
#[async_trait]
pub trait Namespace: Send + Sync {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure>;
}

/// Destination capabilities used by the ordinary baseline metadata copy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CopiedMetadataTarget {
    pub timestamp_precision: TimePrecision,
}

/// Metadata observation and application role. It never implicitly refetches omitted facts.
#[async_trait]
pub trait Metadata: Send + Sync {
    /// Source observations requested by the ordinary transfer entry for baseline metadata copy.
    fn copied_metadata_observation_plan(&self) -> Option<ObservationPlan> {
        None
    }

    async fn observe(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure>;
    /// Observes metadata only when the same operation confirms the expected source identity.
    /// Roles that advertise copied metadata should override this method.
    async fn observe_bound(
        &self,
        path: &StoragePath,
        expected: &SourceIdentity,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let _ = expected;
        self.observe(path, plan).await
    }
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
