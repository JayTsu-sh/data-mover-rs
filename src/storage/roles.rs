use std::ops::Range;
use std::pin::Pin;
use std::{error::Error, fmt};

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use tokio_util::sync::CancellationToken;

use super::SourceDescriptor;
use super::copied_metadata::CopiedMetadataTarget;
use crate::runtime::qos::SourceQosBudget;

mod metadata;
mod namespace;
mod stage;
mod verification;
mod version;
pub use metadata::{Metadata, MetadataMutation};
pub use namespace::{
    CaseSensitivity, CreateSymlinkRequest, NameNormalization, NameSemantics, Namespace,
    NamespaceCapabilities,
};
pub(crate) use stage::DeferredCheckpoint;
pub use stage::PreparedStage;
pub use verification::{ReadBackMethod, UnpinnedReadReason, UploadDigest, UploadDigestGap};
use version::version_unsupported;

use crate::model::{
    BackendSessionFailure, EntryOperationFailure, FailureClass, Operation, SourceIdentity,
    SourceVersion, StoragePath, Transience,
};

/// A bounded payload stream. Implementations own request sizing and backpressure.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, StorageRoleFailure>> + Send>>;

/// A payload and its absolute file offset; completion order need not match file order.
#[derive(Clone, Debug)]
pub struct PositionedChunk {
    pub offset: u64,
    pub data: Bytes,
}

/// Completion-ordered payloads for destinations supporting positioned writes.
pub type PositionedByteStream =
    Pin<Box<dyn Stream<Item = Result<PositionedChunk, StorageRoleFailure>> + Send>>;

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
    /// The source version to read: the one the describe pinned ([`SourceDescriptor::version`]).
    /// `Id` only reaches a source whose [`ReadSource::supports_source_versions`] is true; every
    /// other source refuses it rather than read the current version instead.
    pub version: SourceVersion,
}

/// Backend-neutral failure scope for a role operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageRoleFailure {
    Entry(EntryOperationFailure),
    Session(BackendSessionFailure),
}

/// Failure while applying an ordered batch of staged metadata mutations.
///
/// The caller resends what was not applied after a refusal, so these fields are a
/// contract, not diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedMetadataApplicationFailure {
    /// Index in the batch of the mutation that failed, or the batch length when no single
    /// mutation did and the batch failed as a whole (see [`Self::whole_batch`]).
    pub failed_index: usize,
    /// How many leading mutations were applied, never more than `failed_index`. It may be less:
    /// a backend that rejects the batch before touching it reports zero, and the mutations
    /// between the two are then resent in order.
    pub completed: usize,
    /// `None` means the batch stopped because it was cancelled.
    pub error: Option<StorageRoleFailure>,
}

impl StagedMetadataApplicationFailure {
    /// A failure of a batch of `len` mutations as a whole — the stage could not be opened, or the
    /// persistence barrier over the mutations it applied failed, whether the batch ran to the end
    /// or stopped at a refusal — with the first `completed` applied but not made durable.
    ///
    /// It is never the destination declining one write, so application stops instead of going
    /// on to the families after it. Reporting it against the last mutation would make it read as
    /// that family being refused.
    #[must_use]
    pub fn whole_batch(len: usize, completed: usize, error: Option<StorageRoleFailure>) -> Self {
        Self {
            failed_index: len,
            completed: completed.min(len),
            error,
        }
    }
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
    /// Backend-owned read window. Ordered streams must also bound completed reads waiting
    /// for a predecessor by this window; stopping consumption must stop further prefetch.
    fn read_concurrency(&self) -> usize {
        1
    }
    /// Whether reads can retain offsets and be delivered as each operation completes.
    fn supports_positioned_read(&self) -> bool {
        false
    }

    /// Returns exact, nonoverlapping coverage of the requested range using absolute offsets.
    /// Implementations must honor the same pre-allocation budget as ordered reads.
    async fn read_positioned(
        &self,
        request: ReadRequest,
    ) -> Result<PositionedByteStream, StorageRoleFailure> {
        Err(StorageRoleFailure::Entry(
            EntryOperationFailure::new(
                request.path,
                Operation::Read,
                FailureClass::Unsupported,
                Transience::Permanent,
                "positioned I/O is unavailable",
            )
            .unwrap_or_else(|_| unreachable!("the static positioned-I/O diagnostic is valid")),
        ))
    }
    /// Whether this implementation reserves every prefetched read using `ReadRequest::read_budget`.
    /// Other sources are polled serially with admission owned by the producer.
    fn supports_read_budget(&self) -> bool {
        false
    }

    /// Largest payload the connected backend can return from one source read operation.
    /// The transfer uses this ceiling independently of the destination's write sizing.
    fn maximum_read_chunk_bytes(&self) -> usize {
        1024 * 1024
    }
    async fn describe(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure>;
    /// Whether this source keeps versions a transfer can select ([`SourceVersion::Id`]).
    fn supports_source_versions(&self) -> bool {
        false
    }
    /// Describes one version of `path`. The descriptor pins the version it describes, which a
    /// versioned source resolves from `Current` to the version it found.
    ///
    /// The default serves sources without versions: `Current` is [`ReadSource::describe`], and
    /// `Id` is `Unsupported`.
    async fn describe_version(
        &self,
        path: &StoragePath,
        version: &SourceVersion,
    ) -> Result<SourceDescriptor, StorageRoleFailure> {
        match version {
            SourceVersion::Current => self.describe(path).await,
            SourceVersion::Id(_) => Err(version_unsupported(path, Operation::Observe)),
        }
    }
    async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure>;
    /// Whether `observed` — the identity an enumeration of this source reported for
    /// `described.path` — names the object a later `describe` found (`described`). The default
    /// is identity equality. A source whose listing cannot report the identity a describe pins
    /// (S3: `ListObjectsV2` has no version id, a versioned describe pins one) overrides it to
    /// recognise the same unchanged object; a changed object must still not match.
    fn observation_matches(&self, observed: &SourceIdentity, described: &SourceDescriptor) -> bool {
        observed == &described.source_identity
    }
}

/// Request to prepare unpublished destination state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrepareRequest {
    pub final_destination: FinalDestination,
    pub source: SourceDescriptor,
    pub recovery_binding: [u8; 32],
}

/// A typed destination that remains unchanged until publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalDestination {
    path: StoragePath,
    // A trusted source metadata plan will set permissions before publication. Keep
    // fresh data private until then; display-grade descriptor modes never select this.
    private_creation: bool,
}

impl FinalDestination {
    #[must_use]
    pub const fn new(path: StoragePath) -> Self {
        Self {
            path,
            private_creation: false,
        }
    }
    #[must_use]
    pub const fn path(&self) -> &StoragePath {
        &self.path
    }

    pub(crate) const fn with_private_creation(mut self, private: bool) -> Self {
        self.private_creation = private;
        self
    }

    pub(crate) const fn private_creation(&self) -> bool {
        self.private_creation
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
    /// For a destination that verifies after publication ([`VerificationPoint::AfterPublish`]):
    /// what the publication reported, so the read can be pinned to the object it created (its
    /// version). `None` before publication.
    pub published: Option<PublicationEvidence>,
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
    /// Content identity used to reconcile an ambiguous remote commit. Native
    /// transfers may omit it when obtaining one would require client-side I/O.
    pub expected_blake3: Option<[u8; 32]>,
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
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationEvidence {
    pub final_destination: StoragePath,
    pub disposition: PublicationDisposition,
    /// The version the publication created, for a destination that versions its objects (an S3
    /// `versionId`; never the literal `"null"` of an unversioned bucket).
    pub version: Option<String>,
}

/// When read-back verification reads the destination: before publication, from the stage — or,
/// for a destination that writes at the final name (S3), only after publication, from the final
/// object. A failure after publication cannot be undone; it is reported with
/// `final_destination_changed`.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerificationPoint {
    BeforePublish,
    AfterPublish,
}

/// Destination role owning prepare, write, checkpoint, verify, publish, and discard.
#[async_trait]
pub trait StagedDestination: Send + Sync {
    /// Whether this adapter can retain source-derived hash evidence with recovery state.
    /// Only the ordinary eligible Checkpointed engine selects this optional interface.
    fn supports_hash_checkpoint(&self) -> bool {
        false
    }

    /// Refines the capability for the actual prepared operation (notably single PUT vs MPU).
    fn stage_supports_hash_checkpoint(&self, stage: &PreparedStage) -> bool {
        self.supports_hash_checkpoint() && !stage.direct
    }

    /// Whether payload may become recoverable independently of a committed prefix pointer.
    fn requires_hash_intent(&self) -> bool {
        false
    }

    /// Anchors the stage's frozen source hash revision without advancing destination durability.
    /// The engine does not mutate source hash evidence until this call settles. All pointer
    /// updates for an armed stage serialize through its checkpoint gate.
    async fn persist_hash_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<(), StorageRoleFailure> {
        Err(super::checkpoint_stream::failure(
            stage.final_destination.path(),
            FailureClass::Unsupported,
            "source hash checkpoints are unsupported",
        ))
    }

    /// Writes missing logical ranges. Other transfer policies never call this interface.
    async fn write_checkpointed(
        &self,
        stage: &PreparedStage,
        input: PositionedByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        if !stage.checkpoint_enabled() {
            return Err(super::checkpoint_stream::failure(
                stage.final_destination.path(),
                FailureClass::Unsupported,
                "checkpointed writes were not armed",
            ));
        }
        if self.supports_positioned_write() {
            self.write_positioned(stage, input).await
        } else {
            self.write(stage, super::checkpoint_stream::ordered(stage, input))
                .await
        }
    }

    /// Releases only owned recovery metadata after publication and required verification.
    async fn release_hash_checkpoint(
        &self,
        _stage: &PreparedStage,
    ) -> Result<(), StorageRoleFailure> {
        Ok(())
    }

    /// Whether the destination accepts nonoverlapping chunks in completion order.
    fn supports_positioned_write(&self) -> bool {
        false
    }

    /// Writes nonoverlapping ranges without assuming delivery order. Successful completion
    /// requires contiguous coverage from the stage's recovery offset; checkpoints must never
    /// use sparse file length as proof of a durable prefix.
    async fn write_positioned(
        &self,
        stage: &PreparedStage,
        _input: PositionedByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        Err(StorageRoleFailure::Entry(
            EntryOperationFailure::new(
                stage.final_destination.path().clone(),
                Operation::Write,
                FailureClass::Unsupported,
                Transience::Permanent,
                "positioned I/O is unavailable",
            )
            .unwrap_or_else(|_| unreachable!("the static positioned-I/O diagnostic is valid")),
        ))
    }
    /// Prepares by looking at the destination first: resumes an equal binding found there, or
    /// cleans up what it finds and starts fresh. The returned stage reports the
    /// [`PrepareFact`](super::PrepareFact). Every destination keeps its recovery state beside the
    /// final file (ADR-0006): nothing about the transfer is recorded where data-mover runs.
    async fn prepare_at_destination(
        &self,
        request: super::DestinationPrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure>;
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
    /// Target capabilities for baseline metadata copied by the ordinary transfer entry. `None`
    /// takes the destination out of metadata altogether — no plan, no report — so a destination
    /// that stores nothing should say so with a target whose `stores_nothing()` holds: every
    /// family is then reported as skipped because of it.
    fn copied_metadata_target(&self) -> Option<CopiedMetadataTarget> {
        None
    }
    /// Whether a copy may give the file this owner and group, when `copied_metadata_target`
    /// declares numeric ownership. Answered per file from facts fixed when the destination
    /// connected: an unprivileged Local writer keeps its own files' owners but cannot give a file
    /// to someone else. A `false` carries the mode alone and reports the loss instead of failing
    /// the file on the refused write.
    fn may_set_owner(&self, _uid: u32, _gid: u32) -> bool {
        true
    }

    /// Automatic checkpoint spacing, when deferred recovery is supported by this destination.
    fn automatic_checkpoint_interval_bytes(&self) -> Option<u64> {
        None
    }

    /// Whether this destination can keep a transfer's progress for a later attempt to resume
    /// (ADR-0006). `false` for a destination that cannot (an S3 bucket without multipart uploads:
    /// an ONTAP NAS bucket, P7 §14.2): a `TransferPolicy::Checkpointed` transfer to it reports
    /// `EffectiveRecovery::UnsupportedByDestination`, prepares with `ResumeMode::Restart` and no
    /// recovery, and every attempt starts from zero.
    fn resumable_at_destination(&self) -> bool {
        true
    }

    /// Why this destination cannot write a file at `path` holding `size` bytes (`None`: unknown),
    /// decided from the name and size alone; `None` when it can. A transfer asks once the source
    /// is described, before any destination I/O, and fails that entry at `Preflight` with
    /// `Unsupported` (permanent) — no fallback. An ONTAP NAS bucket refuses objects over 5 GiB
    /// (one `PutObject`, no multipart) and keys it would store under another name (P7 §14.2 N0).
    /// The default refuses nothing.
    fn final_refusal(&self, _path: &StoragePath, _size: Option<u64>) -> Option<String> {
        None
    }

    /// The most bytes a directory object (a final path ending in `/`, P7 F5) written here may
    /// hold, for a destination that writes one only in a single request: S3 sends it as one
    /// `PutObject` or `CopyObject`, at most 5 GiB (P7 D13). The transfer refuses a larger one, or
    /// one of unknown size, at `Preflight` before any destination I/O. `None` (the default): no
    /// such limit — a destination that holds no directory objects refuses every one anyway.
    fn directory_object_limit(&self) -> Option<u64> {
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

    async fn write(
        &self,
        stage: &PreparedStage,
        input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure>;
    async fn observe_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<CheckpointObservation, StorageRoleFailure>;
    /// Reads the content back and compares it with the source's evidence. Before publication
    /// (the default [`VerificationPoint`]) it reads the stage; a destination that verifies after
    /// publication reads the final object, pinned by `request.published`, and is called with a
    /// stage that is already published.
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
    /// Checks metadata that a backend applies after publishing, without changing the final.
    ///
    /// The default needs no extra observation: rename-based backends finish staged metadata
    /// before anchoring `ReadyToPublish`. Backends with deferred publication metadata must
    /// override this to reject recovery when those mutations have not actually committed.
    async fn validate_recovered_metadata(
        &self,
        _stage: &PreparedStage,
        _mutations: Vec<MetadataMutation>,
        _cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        Ok(())
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
    /// Makes the stage the final object. On success nothing of the stage is left at the
    /// destination — no stage, no pointer — so a verification that fails after publication can
    /// drop the stage without cleaning anything up.
    async fn publish(
        &self,
        stage: &PreparedStage,
        request: PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure>;
    /// Removes an unpublished stage. A stage kept at the destination removes its pointer
    /// before the staged content, so a clean-up that fails halfway never leaves a pointer that
    /// would resume the stage (ADR-0006).
    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure>;
    /// When read-back verification of `stage` happens (see [`VerificationPoint`]).
    fn verification_point(&self, _stage: &PreparedStage) -> VerificationPoint {
        VerificationPoint::BeforePublish
    }
    /// How [`Self::verify`] reads `stage` back (P7 §14.2 N4). The default is
    /// [`ReadBackMethod::Pinned`]: a destination that reads its private stage before
    /// publication, or pins the object it wrote. A destination that cannot pin a read says so.
    fn read_back_method(&self, _stage: &PreparedStage) -> ReadBackMethod {
        ReadBackMethod::Pinned
    }
    /// Whether the store confirmed that `stage`'s published bytes are the bytes sent (P7 §14.2
    /// N4). The default, for a destination without such a digest (a filesystem write), is
    /// [`UploadDigest::Unchecked`]`(`[`UploadDigestGap::NotApplicable`]`)`.
    fn upload_digest(&self, _stage: &PreparedStage) -> UploadDigest {
        UploadDigest::Unchecked(UploadDigestGap::NotApplicable)
    }
    /// Whether the destination keeps every record of a key right now — a bucket whose versioning is
    /// `Enabled` — so a source history can be replayed onto it (P7 H1, D12). Asked of the store
    /// now, not remembered. The default is `false`: a filesystem keeps one state per file.
    async fn keeps_history(&self) -> Result<bool, StorageRoleFailure> {
        Ok(false)
    }
    /// Writes a delete marker on `path` (P7 H1): replays a source history's delete marker onto a
    /// destination that keeps histories — a versioned object store — as the key's newest record.
    /// Refused (`Unsupported`) unless the destination keeps every record right now: anywhere else
    /// the same request would remove data. Even then a store may remove data instead of covering
    /// it; the outcome says so ([`DeleteMarkerOutcome::NullVersionRemoved`]). The default refuses
    /// it: a filesystem has no delete markers.
    async fn write_delete_marker(
        &self,
        path: &StoragePath,
        _cancel: CancellationToken,
    ) -> Result<DeleteMarkerOutcome, StorageRoleFailure> {
        Err(StorageRoleFailure::Entry(
            EntryOperationFailure::new(
                path.clone(),
                Operation::Publish,
                FailureClass::Unsupported,
                Transience::Permanent,
                "this destination keeps no delete markers",
            )
            .unwrap_or_else(|_| unreachable!("static diagnostic is valid")),
        ))
    }
}

/// What writing a delete marker on a destination did ([`StagedDestination::write_delete_marker`],
/// P7 H1). Every outcome but [`Self::Written`] is a store's own behaviour a caller replaying a
/// history reports — none is silent.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DeleteMarkerOutcome {
    /// A new delete marker, now the key's newest record.
    Written {
        /// The marker's version id.
        version_id: String,
    },
    /// The key's newest record already was a delete marker and the store added none (`MinIO` and
    /// Ceph RGW / DXN merge consecutive markers): that marker now stands for both.
    Merged {
        /// The existing marker's version id.
        version_id: String,
    },
    /// The store removed a version of the key instead of only covering it with a marker (DXN
    /// drops the `"null"` version of a key whose only record was that version): the key's history
    /// lost it.
    NullVersionRemoved {
        /// The removed version's id (`"null"` on DXN; another id is reported as it is).
        version_id: String,
        /// The marker written, if the store also wrote one.
        marker: Option<String>,
    },
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
