use std::fmt;
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use futures::StreamExt as _;

use super::identity::{BindingSource, binding_hash};
use super::model::{InflightLimits, RecoveryContext, RecoveryRegistrationFailure};
use super::{ReadBackVerification, TransferPolicy, TransferRequest};
use crate::model::{
    EntryKind, EntryOperationFailure, FailureClass, Operation, SourceIdentity, SourceVersion,
    StoragePath, Transience,
};
use crate::runtime::inflight::{
    InflightConfig, InflightFailure, InflightRuntime, OrderedChunks, ReadRange, SequentialRanges,
};
use crate::storage::{
    CheckpointObservation, FinalDestination, NativePair, PreflightPolicy, PrepareRequest,
    PreparedStage, PublicationDisposition, PublicationEvidence, PublishRequest, ReadRequest,
    ReadSource, SourceDescriptor, SourceQosBudget, SourceQosStats, StagedDestination,
    StorageRoleFailure, VerifyRequest, WriteEvidence,
};
use negotiation::{CopiedMetadataPlan, apply_copied_metadata, copied_metadata_plan};

mod automatic;
mod expert;
mod native;
mod negotiation;
mod positioned;
mod single;
mod version;
pub use expert::{
    ExpertDestinationRequest, ExpertDestinationSession, ExpertDestinationTransferred,
    ExpertSourceEvidence, ExpertSourceOffer, ExpertSourcePayload, ExpertSourceRequest,
    ExpertSourceSession,
};

/// Lifecycle stage at which one transfer attempt stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferPhase {
    Preflight,
    Describe,
    Prepare,
    RecoveryRegistration,
    RecoveryCompletion,
    Transfer,
    Checkpoint,
    Verify,
    Metadata,
    Publish,
}

/// Side responsible for a transfer failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferSide {
    Source,
    Destination,
    Orchestration,
}

/// Payload route actually selected by data-mover.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferRoute {
    Streaming,
    Native,
}

/// Effective recovery behavior after route and source-chunk planning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectiveRecovery {
    /// The request explicitly chose non-checkpointed transfer.
    Disabled,
    /// Checkpointing was requested but one source read can cover the complete payload.
    SkippedSingleSourceChunk,
    /// Automatic recovery was not worthwhile for the known payload size.
    SkippedBelowCheckpointThreshold,
    /// Streaming transfer retains durable intermediate progress.
    Checkpointed,
    /// Server-internal native copy restarts through its own operation semantics.
    NotApplicableNative,
}

/// Backend-neutral data path selected during planning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransferDataPath {
    Streaming,
    Native,
}

#[derive(Clone, Copy)]
struct TransferPlan {
    data_path: TransferDataPath,
    source_size: u64,
    chunk_bytes: usize,
    recovery_enabled: bool,
    effective_recovery: EffectiveRecovery,
    automatic_interval: Option<u64>,
}

/// Structured failure from the unified transfer lifecycle.
#[derive(Debug)]
pub struct TransferFailure {
    phase: TransferPhase,
    side: TransferSide,
    message: &'static str,
    role: Option<Box<StorageRoleFailure>>,
    registration: Option<Box<RecoveryRegistrationFailure>>,
    metadata: Option<Box<crate::metadata::MetadataApplicationFailure>>,
    /// Why copied metadata was refused while planning, before anything was written.
    refusal: Option<crate::metadata::MetadataPlanError>,
    failed_stage: Option<Box<FailedStage>>,
    committed_cleanup: Option<Box<FailedStage>>,
    final_destination_changed: bool,
    source_qos: SourceQosStats,
}

struct FailedStage {
    destination: Arc<dyn StagedDestination>,
    stage: PreparedStage,
}

impl fmt::Debug for FailedStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FailedStage(<opaque>)")
    }
}

impl TransferFailure {
    fn role(phase: TransferPhase, side: TransferSide, role: StorageRoleFailure) -> Self {
        Self {
            phase,
            side,
            message: "storage role failed",
            role: Some(Box::new(role)),
            registration: None,
            metadata: None,
            refusal: None,
            failed_stage: None,
            committed_cleanup: None,
            final_destination_changed: false,
            source_qos: SourceQosStats::default(),
        }
    }

    fn orchestration(phase: TransferPhase, message: &'static str) -> Self {
        Self {
            phase,
            side: TransferSide::Orchestration,
            message,
            role: None,
            registration: None,
            metadata: None,
            refusal: None,
            failed_stage: None,
            committed_cleanup: None,
            final_destination_changed: false,
            source_qos: SourceQosStats::default(),
        }
    }

    fn capability(side: TransferSide, message: &'static str) -> Self {
        Self {
            phase: TransferPhase::Preflight,
            side,
            message,
            role: None,
            registration: None,
            metadata: None,
            refusal: None,
            failed_stage: None,
            committed_cleanup: None,
            final_destination_changed: false,
            source_qos: SourceQosStats::default(),
        }
    }

    fn registration(error: RecoveryRegistrationFailure) -> Self {
        Self {
            phase: TransferPhase::RecoveryRegistration,
            side: TransferSide::Orchestration,
            message: "recovery identity registration failed",
            role: None,
            registration: Some(Box::new(error)),
            metadata: None,
            refusal: None,
            failed_stage: None,
            committed_cleanup: None,
            final_destination_changed: false,
            source_qos: SourceQosStats::default(),
        }
    }

    fn metadata(error: crate::metadata::MetadataApplicationFailure) -> Self {
        Self {
            phase: TransferPhase::Metadata,
            side: TransferSide::Destination,
            message: "staged metadata application failed",
            role: None,
            registration: None,
            metadata: Some(Box::new(error)),
            refusal: None,
            failed_stage: None,
            committed_cleanup: None,
            final_destination_changed: false,
            source_qos: SourceQosStats::default(),
        }
    }

    fn with_stage(mut self, destination: Arc<dyn StagedDestination>, stage: PreparedStage) -> Self {
        if stage.direct {
            self.final_destination_changed = true;
            return self;
        }
        self.failed_stage = Some(Box::new(FailedStage { destination, stage }));
        self
    }

    fn with_committed_cleanup(
        mut self,
        destination: Arc<dyn StagedDestination>,
        stage: PreparedStage,
    ) -> Self {
        if stage.direct {
            self.final_destination_changed = true;
            return self;
        }
        self.committed_cleanup = Some(Box::new(FailedStage { destination, stage }));
        self
    }

    #[must_use]
    pub const fn phase(&self) -> TransferPhase {
        self.phase
    }

    #[must_use]
    pub const fn side(&self) -> TransferSide {
        self.side
    }

    /// Whether the failed attempt retains unpublished state eligible for a later resume.
    #[must_use]
    pub fn has_recoverable_stage(&self) -> bool {
        self.failed_stage
            .as_ref()
            .is_some_and(|failed| failed.stage.recovery_enabled())
    }

    /// Whether the failed attempt retains unpublished state that may be explicitly discarded.
    #[must_use]
    pub const fn has_unpublished_stage(&self) -> bool {
        self.failed_stage.is_some()
    }

    /// Whether publication committed but staged artifacts still require cleanup.
    #[must_use]
    pub const fn has_pending_cleanup(&self) -> bool {
        self.committed_cleanup.is_some()
    }

    /// Whether publication committed or a direct attempt may have modified the final target.
    #[must_use]
    pub const fn final_destination_changed(&self) -> bool {
        self.final_destination_changed
    }

    /// Actual source work charged before this attempt failed.
    #[must_use]
    pub const fn source_qos(&self) -> SourceQosStats {
        self.source_qos
    }

    /// Returns the metadata application failure, every failed family with it, and its partial family report.
    #[must_use]
    pub fn metadata_failure(&self) -> Option<&crate::metadata::MetadataApplicationFailure> {
        self.metadata.as_deref()
    }

    /// Why copied metadata was refused while planning — which family, under which policy, and
    /// which end lacks it — when that is what failed.
    #[must_use]
    pub const fn metadata_refusal(&self) -> Option<crate::metadata::MetadataPlanError> {
        self.refusal
    }

    fn with_source_qos(mut self, source_qos: SourceQosStats) -> Self {
        self.source_qos = source_qos;
        self
    }

    /// Consumes this failure and discards its unpublished stage, whether recoverable or ephemeral.
    ///
    /// # Errors
    /// Returns a storage-role failure if no unpublished stage exists or cleanup fails.
    pub async fn discard_stage(mut self) -> Result<(), StorageRoleFailure> {
        let failed = self
            .failed_stage
            .take()
            .ok_or_else(|| source_failure(&StoragePath::root(), FailureClass::InvalidInput))?;
        let binding = failed.stage.recovery_binding();
        let recovery_enabled = failed.stage.owns_recovery_registration();
        failed.destination.discard(failed.stage).await?;
        if recovery_enabled {
            super::recovery_store::complete(binding)
                .await
                .map_err(|_| source_failure(&StoragePath::root(), FailureClass::Internal))?;
        }
        Ok(())
    }

    /// Consumes a post-commit failure and idempotently removes staged artifacts only.
    ///
    /// This never removes or rolls back the published final destination.
    ///
    /// # Errors
    /// Returns a storage-role failure if no committed cleanup is pending or cleanup fails.
    pub async fn cleanup_published_stage(mut self) -> Result<(), StorageRoleFailure> {
        let pending = self
            .committed_cleanup
            .take()
            .ok_or_else(|| source_failure(&StoragePath::root(), FailureClass::InvalidInput))?;
        let binding = pending.stage.recovery_binding();
        let recovery_enabled = pending.stage.owns_recovery_registration();
        pending.destination.discard(pending.stage).await?;
        if recovery_enabled {
            super::recovery_store::complete(binding)
                .await
                .map_err(|_| source_failure(&StoragePath::root(), FailureClass::Internal))?;
        }
        Ok(())
    }
}

impl fmt::Display for TransferFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "transfer {:?} failed on {:?}: {}",
            self.phase, self.side, self.message
        )?;
        if self.phase == TransferPhase::Metadata {
            negotiation::write_metadata_detail(
                formatter,
                self.refusal,
                self.metadata.as_deref(),
                self.role.as_deref(),
            )?;
        }
        Ok(())
    }
}

/// A metadata-phase failure has no `source()`: its Display already carries the refusal, the failed
/// write, or the failed read, with the storage diagnostic, and a chain printer would repeat them.
/// `metadata_refusal()` and `metadata_failure()` reach them as values.
impl std::error::Error for TransferFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if self.phase == TransferPhase::Metadata {
            return None;
        }
        self.role
            .as_ref()
            .map(|error| error.as_ref() as &(dyn std::error::Error + 'static))
            .or_else(|| {
                self.registration
                    .as_ref()
                    .map(|error| error.as_ref() as &(dyn std::error::Error + 'static))
            })
            .or_else(|| {
                self.metadata
                    .as_ref()
                    .map(|error| error.as_ref() as &(dyn std::error::Error + 'static))
            })
    }
}

pub(crate) struct Transferred {
    identity: super::TransferIdentity,
    destination: Arc<dyn StagedDestination>,
    stage: PreparedStage,
    write: WriteEvidence,
    checkpoint: CheckpointObservation,
    #[allow(dead_code)]
    source: SourceDescriptor,
    data_path: TransferDataPath,
    source_blake3: Option<[u8; 32]>,
    source_qos: Option<SourceQosBudget>,
    native_bytes: u64,
    native_requests: u64,
    effective_recovery: EffectiveRecovery,
    copied_metadata_plan: Option<CopiedMetadataPlan>,
}

/// Successful final outcome of one transfer attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferOutcome {
    /// The identity the transfer ran under: derived from its endpoints and paths, or the
    /// caller's override.
    pub identity: super::TransferIdentity,
    pub final_destination: StoragePath,
    pub disposition: PublicationDisposition,
    pub transferred_bytes: u64,
    /// Present only when destination read-back verification was performed.
    pub blake3: Option<[u8; 32]>,
    pub read_back: ReadBackVerification,
    pub source_qos: SourceQosStats,
    pub metadata: Option<crate::metadata::MetadataApplicationReport>,
    pub route: TransferRoute,
    pub recovery: EffectiveRecovery,
}

/// Transfers, optionally verifies by read-back, and publishes one request.
///
/// When the source and destination advertise baseline metadata support, copies compatible mode,
/// ownership, and mtime facts automatically. Local and NFS preserve numeric uid/gid and mode;
/// HDFS preserves mode while retaining its destination-native owner/group principals. Metadata
/// is observed against the described source identity and applied before publication; callers
/// need not apply it again. A destination that stores none of it (S3) has every family reported
/// as skipped, without the source being read for metadata.
/// Access time and change time are not part of any copy. ACLs and extended attributes are not
/// part of the default one either, but a caller can ask for them with
/// [`TransferRequest::with_copied_metadata`]; whether they are carried then depends on what both
/// ends can do. See `.claude/docs/metadata-negotiation.md`.
///
/// # Errors
/// Returns a phase- and side-attributed failure while retaining an owned staged state whenever
/// publication has not completed.
pub async fn transfer(request: TransferRequest) -> Result<TransferOutcome, TransferFailure> {
    let read_back = request.read_back;
    let cancel = request.cancel.clone();
    let transferred = run_until_transferred(request).await?;
    let identity = transferred.identity;
    let route = transfer_route(transferred.data_path);
    let recovery = transferred.effective_recovery;
    let source_qos = transferred_source_qos(&transferred);
    let transferred = if read_back == ReadBackVerification::Enabled {
        verify_transferred(transferred, cancel.clone(), source_qos)
            .await?
            .0
    } else {
        transferred
    };
    let expected_size = transferred.checkpoint.durable_prefix;
    let source_digest = transferred.source_blake3;
    let blake3 = if read_back == ReadBackVerification::Enabled {
        source_digest
    } else {
        None
    };
    let metadata = match apply_copied_metadata(
        &transferred,
        transferred.copied_metadata_plan.as_ref(),
        cancel.clone(),
    )
    .await
    {
        Ok(report) => report,
        Err(error) => {
            return Err(TransferFailure::metadata(error)
                .with_stage(Arc::clone(&transferred.destination), transferred.stage)
                .with_source_qos(source_qos));
        }
    };
    if transferred.stage.recovery_enabled()
        && let Err(error) =
            super::recovery_store::mark_publishing(transferred.stage.recovery_binding()).await
    {
        return Err(TransferFailure::registration(error)
            .with_stage(Arc::clone(&transferred.destination), transferred.stage)
            .with_source_qos(source_qos));
    }
    let publication = transferred
        .destination
        .publish(
            &transferred.stage,
            PublishRequest {
                expected_size,
                expected_blake3: source_digest,
                cancel: cancel.clone(),
            },
        )
        .await;
    let recovery_binding = transferred.stage.recovery_binding();
    let recovery_enabled = transferred.stage.recovery_enabled();
    let PublicationEvidence {
        final_destination,
        disposition,
    } = match publication {
        Ok(evidence) => evidence,
        Err(publication) => {
            let mut failure = TransferFailure::role(
                TransferPhase::Publish,
                TransferSide::Destination,
                publication.error,
            );
            failure.final_destination_changed = publication.final_destination_changed;
            if publication.final_destination_changed {
                return Err(failure
                    .with_committed_cleanup(Arc::clone(&transferred.destination), transferred.stage)
                    .with_source_qos(source_qos));
            }
            return Err(failure
                .with_stage(Arc::clone(&transferred.destination), transferred.stage)
                .with_source_qos(source_qos));
        }
    };
    if complete_published_recovery(recovery_enabled, recovery_binding)
        .await
        .is_err()
    {
        return Err(recovery_completion_failure(transferred, source_qos));
    }
    Ok(TransferOutcome {
        identity,
        final_destination,
        disposition,
        transferred_bytes: expected_size,
        blake3,
        read_back,
        source_qos,
        metadata,
        route,
        recovery,
    })
}

const fn transfer_route(data_path: TransferDataPath) -> TransferRoute {
    match data_path {
        TransferDataPath::Streaming => TransferRoute::Streaming,
        TransferDataPath::Native => TransferRoute::Native,
    }
}

async fn verify_transferred(
    transferred: Transferred,
    cancel: tokio_util::sync::CancellationToken,
    source_qos: SourceQosStats,
) -> Result<(Transferred, crate::storage::VerificationEvidence), TransferFailure> {
    if cancel.is_cancelled() {
        return Err(TransferFailure::orchestration(
            TransferPhase::Verify,
            "transfer was cancelled before verification",
        )
        .with_stage(Arc::clone(&transferred.destination), transferred.stage)
        .with_source_qos(source_qos));
    }
    let Some(source_blake3) = transferred.source_blake3 else {
        return Err(TransferFailure::orchestration(
            TransferPhase::Verify,
            "source digest is unavailable for verification",
        )
        .with_stage(Arc::clone(&transferred.destination), transferred.stage)
        .with_source_qos(source_qos));
    };
    let verification = transferred
        .destination
        .verify(
            &transferred.stage,
            VerifyRequest {
                expected_size: transferred.checkpoint.durable_prefix,
                expected_blake3: source_blake3,
                cancel: cancel.clone(),
            },
        )
        .await;
    let verification = match verification {
        Ok(verification) => verification,
        Err(error) => {
            return Err(TransferFailure::role(
                TransferPhase::Verify,
                TransferSide::Destination,
                error,
            )
            .with_stage(Arc::clone(&transferred.destination), transferred.stage)
            .with_source_qos(source_qos));
        }
    };
    if verification.verified_bytes != transferred.checkpoint.durable_prefix
        || verification.blake3 != source_blake3
    {
        return Err(TransferFailure::orchestration(
            TransferPhase::Verify,
            "destination verification evidence differs from source evidence",
        )
        .with_stage(Arc::clone(&transferred.destination), transferred.stage)
        .with_source_qos(source_qos));
    }
    if cancel.is_cancelled() {
        return Err(TransferFailure::orchestration(
            TransferPhase::Verify,
            "transfer was cancelled before publication",
        )
        .with_stage(Arc::clone(&transferred.destination), transferred.stage)
        .with_source_qos(source_qos));
    }
    Ok((transferred, verification))
}

async fn complete_published_recovery(
    enabled: bool,
    binding: [u8; 32],
) -> Result<(), RecoveryRegistrationFailure> {
    if enabled {
        super::recovery_store::complete(binding).await
    } else {
        Ok(())
    }
}

fn recovery_completion_failure(
    transferred: Transferred,
    source_qos: SourceQosStats,
) -> TransferFailure {
    let mut failure = TransferFailure::orchestration(
        TransferPhase::RecoveryCompletion,
        "published transfer recovery state could not be cleared",
    );
    failure.final_destination_changed = true;
    failure
        .with_committed_cleanup(transferred.destination, transferred.stage)
        .with_source_qos(source_qos)
}

fn transferred_source_qos(transferred: &Transferred) -> SourceQosStats {
    transferred.source_qos.as_ref().map_or(
        SourceQosStats {
            logical_bytes: transferred.checkpoint.durable_prefix,
            native_bytes: transferred.native_bytes,
            native_requests: transferred.native_requests,
            ..SourceQosStats::default()
        },
        SourceQosBudget::stats,
    )
}

impl Transferred {
    pub(crate) const fn durable_prefix(&self) -> u64 {
        self.checkpoint.durable_prefix
    }

    pub(crate) const fn data_path(&self) -> TransferDataPath {
        self.data_path
    }

    pub(crate) async fn discard(self) -> Result<(), StorageRoleFailure> {
        let binding = self.stage.recovery_binding();
        let recovery_enabled = self.stage.owns_recovery_registration();
        self.destination.discard(self.stage).await?;
        if recovery_enabled {
            super::recovery_store::complete(binding)
                .await
                .map_err(|_| source_failure(&StoragePath::root(), FailureClass::Internal))?;
        }
        Ok(())
    }
}

pub(crate) async fn run_until_transferred(
    request: TransferRequest,
) -> Result<Transferred, TransferFailure> {
    let source_qos = request
        .source_qos
        .as_ref()
        .map(crate::runtime::qos::SourceQosGroup::transfer_budget);
    run_until_transferred_inner(request, source_qos.clone())
        .await
        .map_err(|failure| match source_qos {
            Some(budget) => failure.with_source_qos(budget.stats()),
            None => failure,
        })
}

async fn run_until_transferred_inner(
    request: TransferRequest,
    source_qos: Option<SourceQosBudget>,
) -> Result<Transferred, TransferFailure> {
    let TransferRoles {
        source,
        destination,
    } = lend_transfer_roles(&request)?;
    let descriptor = describe_source(&request, &*source, source_qos.as_ref()).await?;
    let copied_metadata_plan = copied_metadata_plan(&request, &descriptor).await?;
    let recovery_binding = recovery_binding(&request, &descriptor);
    let (plan, native_pair, recovery) = plan_with_recovery(
        &request,
        &*source,
        &*destination,
        &descriptor,
        recovery_binding,
    )
    .await?;
    if request.transfer_policy == TransferPolicy::AtomicReplace {
        discard_prior_recovery(
            &destination,
            &request.final_path,
            &descriptor,
            recovery_binding,
        )
        .await?;
    }
    if let Some(pair) = native_pair {
        return native::transfer_native(
            &request,
            native::NativeTransferInput {
                source,
                destination,
                descriptor,
                pair,
                recovery_binding,
                source_qos,
                plan,
                recovery,
                copied_metadata_plan,
            },
        )
        .await;
    }
    let mut stage = select_stage(
        &request,
        &destination,
        &descriptor,
        recovery_binding,
        plan.recovery_enabled,
        recovery.as_ref(),
    )
    .await?;
    stage.durable_publication = request.transfer_policy == TransferPolicy::Checkpointed;
    if let Some(interval_bytes) = plan.automatic_interval {
        stage.deferred_checkpoint = Some(crate::storage::DeferredCheckpoint {
            interval_bytes,
            source_size: plan.source_size,
            registration: Arc::new(automatic::Registration::new(
                recovery_binding,
                request.final_path.clone(),
            )),
        });
    }
    let registration =
        register_prepared_stage(&request, &destination, &stage, recovery.as_ref()).await;
    if let Err(error) = registration {
        return Err(error.with_stage(destination, stage));
    }
    let evidence = match transfer_stage(
        &request,
        source,
        &destination,
        &descriptor,
        &stage,
        plan,
        source_qos.clone(),
    )
    .await
    {
        Ok(evidence) => evidence,
        Err(error) => return Err(error.with_stage(destination, stage)),
    };
    let effective_recovery = final_recovery(&stage, plan);
    Ok(Transferred {
        identity: request.identity,
        destination,
        stage,
        write: evidence.write,
        checkpoint: evidence.checkpoint,
        source: descriptor,
        data_path: plan.data_path,
        source_blake3: evidence.source_blake3,
        source_qos,
        native_bytes: 0,
        native_requests: 0,
        effective_recovery,
        copied_metadata_plan,
    })
}

fn final_recovery(stage: &PreparedStage, plan: TransferPlan) -> EffectiveRecovery {
    if stage.recovery_enabled() {
        EffectiveRecovery::Checkpointed
    } else {
        plan.effective_recovery
    }
}

async fn describe_source(
    request: &TransferRequest,
    source: &dyn ReadSource,
    source_qos: Option<&SourceQosBudget>,
) -> Result<SourceDescriptor, TransferFailure> {
    if request.cancel.is_cancelled() {
        return Err(TransferFailure::orchestration(
            TransferPhase::Preflight,
            "transfer was cancelled",
        ));
    }
    version::preflight_source_version(request, source)?;
    // A versioned source pins the version it describes, `Current` included.
    let descriptor = source
        .describe_version(&request.source_path, &request.source_version)
        .await
        .map_err(|error| {
            TransferFailure::role(TransferPhase::Describe, TransferSide::Source, error)
        })?;
    if let (Some(budget), Some(size)) = (source_qos, descriptor.size) {
        budget.set_logical_bytes(size);
    }
    if request.cancel.is_cancelled() {
        return Err(TransferFailure::orchestration(
            TransferPhase::Describe,
            "transfer was cancelled before prepare",
        ));
    }
    Ok(descriptor)
}

fn plan_request(
    request: &TransferRequest,
    source: &dyn ReadSource,
    destination: &dyn StagedDestination,
    descriptor: &SourceDescriptor,
) -> Result<(TransferPlan, Option<NativePair>), TransferFailure> {
    let native_pair = (request.transfer_policy != TransferPolicy::Direct)
        .then(|| native::eligible_native_pair(request))
        .flatten();
    let mut plan = plan_transfer(
        descriptor,
        request.inflight,
        source.maximum_read_chunk_bytes(),
        native_pair.is_some(),
        request.transfer_policy == TransferPolicy::Checkpointed,
    )?;
    if request.transfer_policy == TransferPolicy::Checkpointed
        && native_pair.is_none()
        && let Some(interval) = destination
            .automatic_checkpoint_interval_bytes()
            .filter(|value| *value > 0)
    {
        plan.recovery_enabled = false;
        plan.effective_recovery = if plan.source_size <= plan.chunk_bytes as u64 {
            EffectiveRecovery::SkippedSingleSourceChunk
        } else {
            EffectiveRecovery::SkippedBelowCheckpointThreshold
        };
        if plan.source_size > interval && plan.source_size > plan.chunk_bytes as u64 {
            plan.automatic_interval = Some(interval);
        }
    }
    Ok((plan, native_pair))
}

async fn plan_with_recovery(
    request: &TransferRequest,
    source: &dyn ReadSource,
    destination: &dyn StagedDestination,
    descriptor: &SourceDescriptor,
    recovery_binding: [u8; 32],
) -> Result<(TransferPlan, Option<NativePair>, Option<RecoveryContext>), TransferFailure> {
    let (mut plan, native_pair) = plan_request(request, source, destination, descriptor)?;
    let recovery = if plan.automatic_interval.is_some() {
        super::recovery_store::open_existing(recovery_binding)
            .await
            .map_err(TransferFailure::registration)?
    } else {
        open_recovery_context(recovery_binding, plan.recovery_enabled).await?
    };
    if recovery.is_some() && plan.automatic_interval.is_some() {
        plan.recovery_enabled = true;
    }
    Ok((plan, native_pair, recovery))
}

async fn open_recovery_context(
    recovery_binding: [u8; 32],
    recovery_enabled: bool,
) -> Result<Option<RecoveryContext>, TransferFailure> {
    if !recovery_enabled {
        return Ok(None);
    }
    super::recovery_store::open(recovery_binding)
        .await
        .map(Some)
        .map_err(TransferFailure::registration)
}

async fn register_prepared_stage(
    request: &TransferRequest,
    destination: &Arc<dyn StagedDestination>,
    stage: &PreparedStage,
    recovery: Option<&RecoveryContext>,
) -> Result<(), TransferFailure> {
    let Some(recovery) = recovery else {
        return Ok(());
    };
    let identity = destination
        .recovery_identity(stage)
        .await
        .map_err(|error| {
            TransferFailure::role(
                TransferPhase::RecoveryRegistration,
                TransferSide::Destination,
                error,
            )
        })?;
    recovery
        .registrar
        .register(identity)
        .await
        .map_err(TransferFailure::registration)?;
    stage.retain_recovery_lease(Arc::clone(&recovery.lease));
    if request.cancel.is_cancelled() {
        return Err(TransferFailure::orchestration(
            TransferPhase::RecoveryRegistration,
            "transfer was cancelled after recovery registration",
        ));
    }
    Ok(())
}

async fn select_stage(
    request: &TransferRequest,
    destination: &Arc<dyn StagedDestination>,
    descriptor: &SourceDescriptor,
    recovery_binding: [u8; 32],
    recovery_enabled: bool,
    recovery: Option<&RecoveryContext>,
) -> Result<PreparedStage, TransferFailure> {
    let prepare = || PrepareRequest {
        final_destination: FinalDestination::new(request.final_path.clone()),
        source: descriptor.clone(),
        recovery_binding,
    };
    if let Some(identity) = recovery.and_then(|context| context.identity.clone()) {
        debug_assert!(recovery_enabled);
        let recovered = destination
            .recover(crate::storage::RecoverRequest {
                identity,
                final_destination: FinalDestination::new(request.final_path.clone()),
                source: descriptor.clone(),
                recovery_binding,
                claim_token: recovery.map_or([0; 32], |context| context.claim),
            })
            .await;
        match recovered {
            Ok(stage) => return Ok(stage),
            Err(error)
                if recovery.is_some_and(|context| context.publication_pending)
                    && role_failure_class(&error) == FailureClass::NotFound =>
            {
                super::recovery_store::complete(recovery_binding)
                    .await
                    .map_err(TransferFailure::registration)?;
            }
            Err(error) => {
                return Err(TransferFailure::role(
                    TransferPhase::Prepare,
                    TransferSide::Destination,
                    error,
                ));
            }
        }
    }
    let prepared = if request.transfer_policy == TransferPolicy::Direct {
        destination
            .prepare_direct(prepare(), request.cancel.clone())
            .await
    } else if recovery_enabled {
        destination.prepare(prepare()).await
    } else {
        destination.prepare_ephemeral(prepare()).await
    };
    prepared.map_err(|error| {
        TransferFailure::role(TransferPhase::Prepare, TransferSide::Destination, error)
    })
}

async fn discard_prior_recovery(
    destination: &Arc<dyn StagedDestination>,
    final_path: &StoragePath,
    descriptor: &SourceDescriptor,
    recovery_binding: [u8; 32],
) -> Result<(), TransferFailure> {
    let Some(recovery) = super::recovery_store::open_existing(recovery_binding)
        .await
        .map_err(TransferFailure::registration)?
    else {
        return Ok(());
    };
    let Some(identity) = recovery.identity.clone() else {
        return Ok(());
    };
    match destination
        .recover(crate::storage::RecoverRequest {
            identity,
            final_destination: FinalDestination::new(final_path.clone()),
            source: descriptor.clone(),
            recovery_binding,
            claim_token: recovery.claim,
        })
        .await
    {
        Ok(stage) => destination.discard(stage).await.map_err(|error| {
            TransferFailure::role(TransferPhase::Prepare, TransferSide::Destination, error)
        })?,
        Err(error)
            if recovery.publication_pending
                && role_failure_class(&error) == FailureClass::NotFound => {}
        Err(error) => {
            return Err(TransferFailure::role(
                TransferPhase::Prepare,
                TransferSide::Destination,
                error,
            ));
        }
    }
    super::recovery_store::complete(recovery_binding)
        .await
        .map_err(TransferFailure::registration)
}

fn role_failure_class(error: &StorageRoleFailure) -> FailureClass {
    match error {
        StorageRoleFailure::Entry(error) => error.class(),
        StorageRoleFailure::Session(error) => error.class(),
    }
}

fn recovery_binding(request: &TransferRequest, descriptor: &SourceDescriptor) -> [u8; 32] {
    recovery_binding_for(
        &request.identity,
        &request.destination,
        &request.final_path,
        descriptor,
    )
}

pub(super) fn recovery_binding_for(
    identity: &super::TransferIdentity,
    destination: &crate::storage::Storage,
    final_path: &StoragePath,
    descriptor: &SourceDescriptor,
) -> [u8; 32] {
    binding_hash(
        identity,
        &BindingSource {
            path: &descriptor.path,
            identity_key: descriptor.source_identity.identity_key(),
            size: descriptor.size,
            content_version: descriptor.content_version.as_deref(),
        },
        destination.identity(),
        final_path,
    )
}

struct TransferRoles {
    source: Arc<dyn ReadSource>,
    destination: Arc<dyn StagedDestination>,
}

fn lend_transfer_roles(request: &TransferRequest) -> Result<TransferRoles, TransferFailure> {
    let policy = PreflightPolicy::production();
    let source = request.source.read_source(&policy).map_err(|_| {
        TransferFailure::capability(TransferSide::Source, "source capability unavailable")
    })?;
    let destination = request
        .destination
        .staged_destination(&policy)
        .map_err(|_| {
            TransferFailure::capability(
                TransferSide::Destination,
                "destination capability unavailable",
            )
        })?;
    if request.transfer_policy == TransferPolicy::Direct && !destination.supports_direct() {
        return Err(TransferFailure::capability(
            TransferSide::Destination,
            "Direct requires a destination that supports direct writes",
        ));
    }
    Ok(TransferRoles {
        source,
        destination,
    })
}

fn plan_transfer(
    descriptor: &SourceDescriptor,
    limits: InflightLimits,
    backend_chunk_bytes: usize,
    native: bool,
    recovery_allowed: bool,
) -> Result<TransferPlan, TransferFailure> {
    if descriptor.kind != EntryKind::File {
        return Err(TransferFailure::orchestration(
            TransferPhase::Describe,
            "ordinary transfer source must be a file",
        ));
    }
    let source_size = descriptor.size.ok_or_else(|| {
        TransferFailure::orchestration(TransferPhase::Describe, "source has no byte size")
    })?;
    let chunk_bytes = limits
        .negotiated_chunk_ceiling()
        .min(backend_chunk_bytes.max(1));
    Ok(TransferPlan {
        data_path: if native {
            TransferDataPath::Native
        } else {
            TransferDataPath::Streaming
        },
        source_size,
        chunk_bytes,
        automatic_interval: None,
        recovery_enabled: recovery_allowed && !native && source_size > chunk_bytes as u64,
        effective_recovery: if native {
            EffectiveRecovery::NotApplicableNative
        } else if !recovery_allowed {
            EffectiveRecovery::Disabled
        } else if source_size <= chunk_bytes as u64 {
            EffectiveRecovery::SkippedSingleSourceChunk
        } else {
            EffectiveRecovery::Checkpointed
        },
    })
}

async fn transfer_stage(
    request: &TransferRequest,
    source: Arc<dyn ReadSource>,
    destination: &Arc<dyn StagedDestination>,
    descriptor: &SourceDescriptor,
    stage: &PreparedStage,
    plan: TransferPlan,
    source_qos: Option<SourceQosBudget>,
) -> Result<TransferEvidence, TransferFailure> {
    if plan.source_size <= plan.chunk_bytes as u64 && stage.write_offset == 0 {
        return single::transfer(request, source, destination, descriptor, stage, source_qos).await;
    }
    let write_start = stage.write_offset;
    if write_start > plan.source_size {
        return Err(TransferFailure::orchestration(
            TransferPhase::Checkpoint,
            "recovered prefix exceeds source size",
        ));
    }
    let (write, source_blake3) = if source.supports_read_budget()
        && source.supports_positioned_read()
        && destination.supports_positioned_write()
    {
        positioned::transfer(
            request,
            source,
            destination,
            descriptor,
            stage,
            plan.source_size,
            source_qos,
        )
        .await?
    } else {
        let (runtime, ordered) = inflight_channel(
            request.inflight,
            write_start,
            plan.source_size,
            request.cancel.clone(),
        )?;
        let source_failure = Arc::new(Mutex::new(None));
        let producer = tokio::spawn(produce(ProducerRequest {
            source: Arc::clone(&source),
            path: descriptor.path.clone(),
            source_identity: descriptor.source_identity.clone(),
            version: descriptor.version.clone(),
            cancel: request.cancel.clone(),
            runtime,
            failure: Arc::clone(&source_failure),
            size: plan.source_size,
            write_start,
            source_qos,
            // Fresh streaming transfers already visit every byte, so retain a
            // commit digest without another read. A disabled-verification resume
            // must not reread its durable prefix solely to reconstruct that hash.
            hash_content: request.needs_source_digest() || write_start == 0,
        }));
        let stream = ordered_stream(ordered, source_failure, descriptor.path.clone());
        settle_transfer(destination.write(stage, stream).await, producer).await?
    };
    let checkpoint = if stage.recovery_enabled() {
        destination
            .observe_checkpoint(stage)
            .await
            .map_err(|error| {
                TransferFailure::role(TransferPhase::Checkpoint, TransferSide::Destination, error)
            })?
    } else {
        CheckpointObservation {
            durable_prefix: write.persisted_bytes,
        }
    };
    if checkpoint.durable_prefix != plan.source_size || write.persisted_bytes != plan.source_size {
        return Err(TransferFailure::orchestration(
            TransferPhase::Checkpoint,
            "durable bytes differ from source size",
        ));
    }
    Ok(TransferEvidence {
        write,
        checkpoint,
        source_blake3,
    })
}

struct TransferEvidence {
    write: WriteEvidence,
    checkpoint: CheckpointObservation,
    source_blake3: Option<[u8; 32]>,
}

async fn settle_transfer(
    write: Result<WriteEvidence, StorageRoleFailure>,
    producer: tokio::task::JoinHandle<Result<Option<[u8; 32]>, TransferFailure>>,
) -> Result<(WriteEvidence, Option<[u8; 32]>), TransferFailure> {
    let write = match write {
        Ok(write) => write,
        Err(error) => {
            producer.abort();
            let _ = producer.await;
            return Err(TransferFailure::role(
                TransferPhase::Transfer,
                failure_side(&error),
                error,
            ));
        }
    };
    let producer_result = producer.await.map_err(|_| {
        TransferFailure::orchestration(TransferPhase::Transfer, "source producer stopped")
    })?;
    let digest = producer_result?;
    Ok((write, digest))
}

fn inflight_channel(
    limits: InflightLimits,
    start: u64,
    size: u64,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<(InflightRuntime, OrderedChunks), TransferFailure> {
    let config =
        InflightConfig::new(limits.chunks, limits.bytes, limits.operations).map_err(|_| {
            TransferFailure::orchestration(TransferPhase::Preflight, "invalid inflight limits")
        })?;
    InflightRuntime::channel(config, start, size, cancel)
        .map_err(|_| TransferFailure::orchestration(TransferPhase::Preflight, "invalid byte range"))
}

struct ProducerRequest {
    source: Arc<dyn ReadSource>,
    path: StoragePath,
    source_identity: SourceIdentity,
    /// The version the describe pinned; every read of this producer reads it.
    version: SourceVersion,
    cancel: tokio_util::sync::CancellationToken,
    runtime: InflightRuntime,
    failure: Arc<Mutex<Option<StorageRoleFailure>>>,
    size: u64,
    write_start: u64,
    source_qos: Option<SourceQosBudget>,
    hash_content: bool,
}

async fn open_producer_stream(
    request: &ProducerRequest,
    read_start: u64,
) -> Result<
    (
        crate::storage::ByteStream,
        crate::storage::ReadBudget,
        Option<crate::storage::ReadAdmission>,
    ),
    TransferFailure,
> {
    let budgeted = request.source.supports_read_budget();
    let budget = crate::storage::ReadBudget::new(request.runtime.clone());
    let read_inflight = if budgeted {
        request.runtime.read_depth()
    } else {
        1
    };
    let maximum_chunk_bytes = request.runtime.negotiated_chunk_ceiling();
    let serial_admission = if budgeted {
        None
    } else {
        request
            .runtime
            .reserve_read(maximum_chunk_bytes, true)
            .await
            .map_err(|error| inflight_transfer_failure(&error, &request.path))?
    };
    let stream = request
        .source
        .read(ReadRequest {
            path: request.path.clone(),
            range: Some(read_start..request.size),
            expected_source: Some(request.source_identity.clone()),
            maximum_chunk_bytes,
            read_inflight,
            read_budget: budgeted.then(|| budget.clone()),
            cancel: request.cancel.clone(),
            source_qos: request.source_qos.clone(),
            version: request.version.clone(),
        })
        .await;
    let stream = match stream {
        Ok(stream) => stream,
        Err(error) => return fail_source_producer(request, error).await,
    };
    Ok((stream, budget, serial_admission))
}

async fn produce(request: ProducerRequest) -> Result<Option<[u8; 32]>, TransferFailure> {
    let mut hasher = request.hash_content.then(blake3::Hasher::new);
    let read_start = if request.hash_content {
        0
    } else {
        request.write_start
    };
    let budgeted = request.source.supports_read_budget();
    let maximum_chunk_bytes = request.runtime.negotiated_chunk_ceiling();
    let (mut stream, budget, mut serial_admission) =
        open_producer_stream(&request, read_start).await?;
    let mut offset = read_start;
    loop {
        let admission = if budgeted {
            None
        } else if let Some(admission) = serial_admission.take() {
            Some(admission)
        } else {
            request
                .runtime
                .reserve_read(maximum_chunk_bytes, true)
                .await
                .map_err(|error| inflight_transfer_failure(&error, &request.path))?
        };
        let Some(item) = stream.next().await else {
            break;
        };
        let bytes = match item {
            Ok(bytes) if !bytes.is_empty() => bytes,
            Ok(_) => {
                return Err(TransferFailure::orchestration(
                    TransferPhase::Transfer,
                    "source emitted an empty chunk",
                ));
            }
            Err(error) => return fail_source_producer(&request, error).await,
        };
        let admission = if budgeted {
            budget.take(offset)
        } else {
            admission
        }
        .ok_or_else(|| {
            TransferFailure::orchestration(TransferPhase::Transfer, "source omitted read admission")
        })?;
        let next = offset.checked_add(bytes.len() as u64).ok_or_else(|| {
            TransferFailure::orchestration(TransferPhase::Transfer, "source offset overflowed")
        })?;
        if next > request.size {
            return Err(TransferFailure::orchestration(
                TransferPhase::Transfer,
                "source emitted more bytes than described",
            ));
        }
        if let Some(hasher) = &mut hasher {
            hasher.update(&bytes);
        }
        if next > request.write_start {
            let skip =
                usize::try_from(request.write_start.saturating_sub(offset)).map_err(|_| {
                    TransferFailure::orchestration(
                        TransferPhase::Transfer,
                        "recovery prefix exceeds addressable range",
                    )
                })?;
            let output = bytes.slice(skip..);
            let output_offset = offset + skip as u64;
            request
                .runtime
                .complete_read(admission, output_offset, output)
                .await
                .map_err(|error| inflight_transfer_failure(&error, &request.path))?;
        }
        offset = next;
    }
    if offset != request.size {
        return Err(TransferFailure::orchestration(
            TransferPhase::Transfer,
            "source emitted fewer bytes than described",
        ));
    }
    Ok(hasher.map(|hasher| *hasher.finalize().as_bytes()))
}

async fn fail_source_producer<T>(
    request: &ProducerRequest,
    error: StorageRoleFailure,
) -> Result<T, TransferFailure> {
    *request
        .failure
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error.clone());
    let _ = request.runtime.fail(InflightFailure::Upstream).await;
    Err(TransferFailure::role(
        TransferPhase::Transfer,
        TransferSide::Source,
        error,
    ))
}

async fn read_exact_range(
    source: &dyn ReadSource,
    path: &StoragePath,
    source_identity: &SourceIdentity,
    version: &SourceVersion,
    cancel: &tokio_util::sync::CancellationToken,
    source_qos: Option<SourceQosBudget>,
    range: ReadRange,
) -> Result<Bytes, StorageRoleFailure> {
    let mut stream = source
        .read(ReadRequest {
            path: path.clone(),
            range: Some(range.offset..range.offset + range.length as u64),
            expected_source: Some(source_identity.clone()),
            maximum_chunk_bytes: range.length,
            read_inflight: 1,
            read_budget: None,
            cancel: cancel.clone(),
            source_qos,
            version: version.clone(),
        })
        .await?;
    let mut output = BytesMut::with_capacity(range.length);
    while let Some(item) = stream.next().await {
        let item = item?;
        let remaining = range.length - output.len();
        if item.len() > remaining {
            return Err(source_failure(path, FailureClass::Corruption));
        }
        output.extend_from_slice(&item);
    }
    if output.len() != range.length {
        return Err(source_failure(path, FailureClass::Corruption));
    }
    Ok(output.freeze())
}

fn ordered_stream(
    ordered: OrderedChunks,
    failure: Arc<Mutex<Option<StorageRoleFailure>>>,
    path: StoragePath,
) -> crate::storage::ByteStream {
    Box::pin(futures::stream::unfold(ordered, move |mut ordered| {
        let failure = Arc::clone(&failure);
        let path = path.clone();
        async move {
            let item = ordered.next().await?;
            let item = item.map_err(|error| {
                failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
                    .unwrap_or_else(|| inflight_role_failure(&error, &path))
            });
            Some((item, ordered))
        }
    }))
}

fn inflight_transfer_failure(error: &InflightFailure, path: &StoragePath) -> TransferFailure {
    let role = inflight_role_failure(error, path);
    TransferFailure::role(TransferPhase::Transfer, TransferSide::Source, role)
}

fn inflight_role_failure(error: &InflightFailure, path: &StoragePath) -> StorageRoleFailure {
    let class = if *error == InflightFailure::Cancelled {
        FailureClass::Cancelled
    } else {
        FailureClass::Internal
    };
    source_failure(path, class)
}

fn source_failure(path: &StoragePath, class: FailureClass) -> StorageRoleFailure {
    let transience = if class == FailureClass::Cancelled {
        Transience::Transient
    } else {
        Transience::Permanent
    };
    let error = EntryOperationFailure::new(
        path.clone(),
        Operation::Read,
        class,
        transience,
        "transfer source stream failed",
    )
    .unwrap_or_else(|_| unreachable!("static diagnostic is valid"));
    StorageRoleFailure::Entry(error)
}

fn failure_side(error: &StorageRoleFailure) -> TransferSide {
    match error {
        StorageRoleFailure::Entry(error) if error.operation() == Operation::Read => {
            TransferSide::Source
        }
        StorageRoleFailure::Session(error) if error.operation() == Operation::Read => {
            TransferSide::Source
        }
        StorageRoleFailure::Entry(_) | StorageRoleFailure::Session(_) => TransferSide::Destination,
    }
}

#[cfg(test)]
mod plan_tests {
    use super::*;
    use crate::model::{BackendIdentity, IdentityStrength};
    use crate::storage::backends::local::{
        test_destination_storage_with_role, test_source_storage,
    };

    fn descriptor(size: u64) -> Result<SourceDescriptor, Box<dyn std::error::Error>> {
        let backend = BackendIdentity::new("local".parse()?, "source-chunk-plan")?;
        let identity =
            SourceIdentity::new(backend, IdentityStrength::StableWithinBackend, b"entry")?;
        Ok(SourceDescriptor::new(
            StoragePath::new("source.bin")?,
            EntryKind::File,
            Some(size),
            identity,
        ))
    }

    #[test]
    fn single_chunk_is_decided_only_from_the_effective_source_read_size()
    -> Result<(), Box<dyn std::error::Error>> {
        const SOURCE_CHUNK: usize = 6 * 1024 * 1024;
        let plan = plan_transfer(
            &descriptor(SOURCE_CHUNK as u64)?,
            InflightLimits::new(1, SOURCE_CHUNK, 1)?,
            SOURCE_CHUNK,
            false,
            true,
        )?;

        assert_eq!(plan.chunk_bytes, SOURCE_CHUNK);
        assert!(!plan.recovery_enabled);
        assert_eq!(
            plan.effective_recovery,
            EffectiveRecovery::SkippedSingleSourceChunk
        );
        Ok(())
    }

    #[test]
    fn native_copy_never_enables_checkpoint_recovery_regardless_of_size()
    -> Result<(), Box<dyn std::error::Error>> {
        let plan = plan_transfer(
            &descriptor(6 * 1024 * 1024 * 1024)?,
            InflightLimits::new(2, 2 * 1024 * 1024, 2)?,
            5 * 1024 * 1024,
            true,
            true,
        )?;

        assert!(!plan.recovery_enabled);
        assert_eq!(
            plan.effective_recovery,
            EffectiveRecovery::NotApplicableNative
        );
        Ok(())
    }

    #[tokio::test]
    async fn publishing_record_with_missing_stage_restarts_instead_of_stranding()
    -> Result<(), Box<dyn std::error::Error>> {
        let nonce = uuid::Uuid::new_v4();
        let source_root = std::env::temp_dir().join(format!("data-mover-publish-source-{nonce}"));
        let destination_root =
            std::env::temp_dir().join(format!("data-mover-publish-destination-{nonce}"));
        std::fs::create_dir(&source_root)?;
        std::fs::create_dir(&destination_root)?;
        let payload = vec![0x6a; 2 * 64 * 1024 + 1];
        std::fs::write(source_root.join("source.bin"), &payload)?;
        let (source, _) = test_source_storage(&source_root, "publishing-source")?;
        let (destination, role) =
            test_destination_storage_with_role(&destination_root, "publishing-destination")?;
        role.set_automatic_checkpoint_interval(64 * 1024);
        let request = TransferRequest::new(
            source,
            StoragePath::new("source.bin")?,
            destination,
            StoragePath::new("final.bin")?,
            InflightLimits::new(2, 2 * 64 * 1024, 2)?,
            tokio_util::sync::CancellationToken::new(),
        )
        .with_identity_override(crate::transfer::TransferIdentity::from_label(
            "publishing-recovery",
        )?)
        .with_transfer_policy(TransferPolicy::Checkpointed);

        let transferred = run_until_transferred(request.clone()).await?;
        let source_qos = transferred_source_qos(&transferred);
        let (transferred, verification) = verify_transferred(
            transferred,
            tokio_util::sync::CancellationToken::new(),
            source_qos,
        )
        .await?;
        crate::transfer::recovery_store::mark_publishing(transferred.stage.recovery_binding())
            .await?;
        transferred
            .destination
            .publish(
                &transferred.stage,
                PublishRequest {
                    expected_size: verification.verified_bytes,
                    expected_blake3: Some(verification.blake3),
                    cancel: tokio_util::sync::CancellationToken::new(),
                },
            )
            .await
            .map_err(|failure| failure.error)?;
        drop(transferred);

        let outcome = transfer(request).await?;
        assert_eq!(outcome.transferred_bytes, payload.len() as u64);
        assert_eq!(std::fs::read(destination_root.join("final.bin"))?, payload);
        std::fs::remove_dir_all(source_root)?;
        std::fs::remove_dir_all(destination_root)?;
        Ok(())
    }
}
