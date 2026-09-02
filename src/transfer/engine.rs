use std::fmt;
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use futures::StreamExt as _;

use super::{RecoveryContext, RecoveryRegistrationFailure, Resumability, TransferRequest};
use crate::model::{
    EntryKind, EntryOperationFailure, FailureClass, Operation, SourceIdentity, StoragePath,
    Transience,
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

mod expert;
mod native;
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
            failed_stage: None,
            committed_cleanup: None,
            final_destination_changed: false,
            source_qos: SourceQosStats::default(),
        }
    }

    fn with_stage(mut self, destination: Arc<dyn StagedDestination>, stage: PreparedStage) -> Self {
        self.failed_stage = Some(Box::new(FailedStage { destination, stage }));
        self
    }

    fn with_committed_cleanup(
        mut self,
        destination: Arc<dyn StagedDestination>,
        stage: PreparedStage,
    ) -> Self {
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

    /// Whether publication crossed its atomic commit point before failing.
    #[must_use]
    pub const fn final_destination_changed(&self) -> bool {
        self.final_destination_changed
    }

    /// Actual source work charged before this attempt failed.
    #[must_use]
    pub const fn source_qos(&self) -> SourceQosStats {
        self.source_qos
    }

    /// Returns the fail-fast metadata application error and its partial family report.
    #[must_use]
    pub fn metadata_failure(&self) -> Option<&crate::metadata::MetadataApplicationFailure> {
        self.metadata.as_deref()
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
        failed.destination.discard(failed.stage).await
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
        pending.destination.discard(pending.stage).await
    }

    /// Exports a versioned opaque identity for a recoverable unpublished stage.
    ///
    /// # Errors
    /// On failure, returns both the original transfer failure and the export error so cleanup
    /// authority is never lost.
    pub async fn into_recovery_identity(
        self,
    ) -> Result<crate::storage::RecoveryIdentity, (Self, StorageRoleFailure)> {
        let Some(failed) = self
            .failed_stage
            .as_ref()
            .filter(|failed| failed.stage.recovery_enabled())
        else {
            let error = source_failure(&StoragePath::root(), FailureClass::InvalidInput);
            return Err((self, error));
        };
        match failed.destination.handoff_recovery(&failed.stage).await {
            Ok(identity) => Ok(identity),
            Err(error) => Err((self, error)),
        }
    }
}

impl fmt::Display for TransferFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "transfer {:?} failed on {:?}: {}",
            self.phase, self.side, self.message
        )
    }
}

impl std::error::Error for TransferFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
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
    source_blake3: [u8; 32],
    source_qos: Option<SourceQosBudget>,
    native_bytes: u64,
    native_requests: u64,
}

/// Successful final outcome of one transfer attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferOutcome {
    pub final_destination: StoragePath,
    pub disposition: PublicationDisposition,
    pub transferred_bytes: u64,
    pub blake3: [u8; 32],
    pub source_qos: SourceQosStats,
    pub metadata: Option<crate::metadata::MetadataApplicationReport>,
}

/// Transfers, verifies, and publishes one request.
///
/// # Errors
/// Returns a phase- and side-attributed failure while retaining an owned staged state whenever
/// publication has not completed.
pub async fn transfer(request: TransferRequest) -> Result<TransferOutcome, TransferFailure> {
    let cancel = request.cancel.clone();
    let policy = request.existing_destination;
    let transferred = run_until_transferred(request).await?;
    let source_qos = transferred_source_qos(&transferred);
    if cancel.is_cancelled() {
        return Err(TransferFailure::orchestration(
            TransferPhase::Verify,
            "transfer was cancelled before verification",
        )
        .with_stage(Arc::clone(&transferred.destination), transferred.stage)
        .with_source_qos(source_qos));
    }
    let verification_result = transferred
        .destination
        .verify(
            &transferred.stage,
            VerifyRequest {
                expected_size: transferred.checkpoint.durable_prefix,
                expected_blake3: transferred.source_blake3,
                cancel: cancel.clone(),
            },
        )
        .await;
    let verification = match verification_result {
        Ok(evidence) => evidence,
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
        || verification.blake3 != transferred.source_blake3
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
    let expected_size = verification.verified_bytes;
    let blake3 = verification.blake3;
    let publication = transferred
        .destination
        .publish(
            &transferred.stage,
            PublishRequest {
                policy,
                expected_size,
                expected_blake3: blake3,
                cancel,
            },
        )
        .await;
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
    Ok(TransferOutcome {
        final_destination,
        disposition,
        transferred_bytes: expected_size,
        blake3,
        source_qos,
        metadata: None,
    })
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
        self.destination.discard(self.stage).await
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
    validate_recovery_configuration(&request)?;
    let TransferRoles {
        source,
        destination,
    } = lend_transfer_roles(&request)?;
    if request.cancel.is_cancelled() {
        return Err(TransferFailure::orchestration(
            TransferPhase::Preflight,
            "transfer was cancelled",
        ));
    }
    let descriptor = source
        .describe(&request.source_path)
        .await
        .map_err(|error| {
            TransferFailure::role(TransferPhase::Describe, TransferSide::Source, error)
        })?;
    if let (Some(budget), Some(size)) = (&source_qos, descriptor.size) {
        budget.set_logical_bytes(size);
    }
    if request.cancel.is_cancelled() {
        return Err(TransferFailure::orchestration(
            TransferPhase::Describe,
            "transfer was cancelled before prepare",
        ));
    }
    let (plan, native_pair, recovery) = plan_with_recovery(&request, &*source, &descriptor).await?;
    let recovery_binding = recovery_binding(&request, &descriptor);
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
            },
        )
        .await;
    }
    let stage = select_stage(
        &request,
        &destination,
        &descriptor,
        recovery_binding,
        plan.recovery_enabled,
        recovery.as_ref(),
    )
    .await?;
    let registration =
        register_prepared_stage(&request, &destination, &stage, recovery.as_ref()).await;
    if let Err(error) = registration {
        return Err(error.with_stage(destination, stage));
    }
    let identity = request.identity.clone();
    let result = transfer_stage(
        &request,
        source,
        &destination,
        &descriptor,
        &stage,
        plan,
        source_qos.clone(),
    )
    .await;
    match result {
        Ok(evidence) => Ok(Transferred {
            identity,
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
        }),
        Err(error) => Err(error.with_stage(destination, stage)),
    }
}

fn validate_recovery_configuration(request: &TransferRequest) -> Result<(), TransferFailure> {
    if request.resumability == Resumability::Disabled && request.recovery_provider.is_some() {
        return Err(TransferFailure::orchestration(
            TransferPhase::Preflight,
            "disabled resumability cannot accept a recovery provider",
        ));
    }
    Ok(())
}

fn plan_request(
    request: &TransferRequest,
    source: &dyn ReadSource,
    descriptor: &SourceDescriptor,
) -> Result<(TransferPlan, Option<NativePair>), TransferFailure> {
    let native_pair = native::eligible_native_pair(request);
    let native_checkpointed = native_pair.as_ref().is_some_and(|pair| {
        descriptor.size.is_some_and(|size| {
            pair.recovery_mode(size) == crate::storage::NativeRecoveryMode::Checkpointed
        })
    });
    let plan = plan_transfer(
        descriptor,
        request.inflight,
        source.maximum_read_chunk_bytes(),
        native_pair.is_some(),
        native_checkpointed,
        request.resumability == Resumability::Enabled,
    )?;
    if plan.recovery_enabled && request.recovery_provider.is_none() {
        return Err(TransferFailure::orchestration(
            TransferPhase::Preflight,
            "recoverable transfer requires a recovery provider",
        ));
    }
    Ok((plan, native_pair))
}

async fn plan_with_recovery(
    request: &TransferRequest,
    source: &dyn ReadSource,
    descriptor: &SourceDescriptor,
) -> Result<(TransferPlan, Option<NativePair>, Option<RecoveryContext>), TransferFailure> {
    let (mut plan, mut native_pair) = plan_request(request, source, descriptor)?;
    let recovery = open_recovery_context(request, plan.recovery_enabled).await?;
    if recovery
        .as_ref()
        .is_some_and(|context| context.identity.is_some())
        && native_pair.is_some()
    {
        native_pair = None;
        plan = plan_transfer(
            descriptor,
            request.inflight,
            source.maximum_read_chunk_bytes(),
            false,
            false,
            request.resumability == Resumability::Enabled,
        )?;
    }
    Ok((plan, native_pair, recovery))
}

async fn open_recovery_context(
    request: &TransferRequest,
    recovery_enabled: bool,
) -> Result<Option<RecoveryContext>, TransferFailure> {
    if !recovery_enabled {
        return Ok(None);
    }
    let provider = request.recovery_provider.as_ref().ok_or_else(|| {
        TransferFailure::orchestration(
            TransferPhase::Preflight,
            "recoverable transfer requires a recovery provider",
        )
    })?;
    provider
        .open()
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
            Err(error) => {
                return Err(TransferFailure::role(
                    TransferPhase::Prepare,
                    TransferSide::Destination,
                    error,
                ));
            }
        }
    }
    let prepared = if recovery_enabled {
        destination.prepare(prepare()).await
    } else {
        destination.prepare_ephemeral(prepare()).await
    };
    prepared.map_err(|error| {
        TransferFailure::role(TransferPhase::Prepare, TransferSide::Destination, error)
    })
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
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"data-mover/recovery-binding/v1\0");
    hasher.update(&(identity.as_bytes().len() as u64).to_le_bytes());
    hasher.update(identity.as_bytes());
    hasher.update(descriptor.source_identity.identity_key().as_bytes());
    hasher.update(descriptor.path.as_str().as_bytes());
    hasher.update(&descriptor.size.unwrap_or(u64::MAX).to_le_bytes());
    let destination_identity = destination.identity();
    hasher.update(destination_identity.kind().as_str().as_bytes());
    hasher.update(&(destination_identity.stable_id().len() as u64).to_le_bytes());
    hasher.update(destination_identity.stable_id().as_bytes());
    hasher.update(final_path.as_str().as_bytes());
    *hasher.finalize().as_bytes()
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
    Ok(TransferRoles {
        source,
        destination,
    })
}

fn plan_transfer(
    descriptor: &SourceDescriptor,
    limits: super::InflightLimits,
    backend_chunk_bytes: usize,
    native: bool,
    native_checkpointed: bool,
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
        recovery_enabled: recovery_allowed
            && if native {
                native_checkpointed
            } else {
                source_size > chunk_bytes as u64
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
    let write_start = stage.write_offset;
    if write_start > plan.source_size {
        return Err(TransferFailure::orchestration(
            TransferPhase::Checkpoint,
            "recovered prefix exceeds source size",
        ));
    }
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
        cancel: request.cancel.clone(),
        runtime,
        failure: Arc::clone(&source_failure),
        size: plan.source_size,
        write_start,
        source_qos,
    }));
    let stream = ordered_stream(ordered, source_failure, descriptor.path.clone());
    let (write, source_blake3) =
        settle_transfer(destination.write(stage, stream).await, producer).await?;
    let checkpoint = if plan.recovery_enabled {
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
    source_blake3: [u8; 32],
}

async fn settle_transfer(
    write: Result<WriteEvidence, StorageRoleFailure>,
    producer: tokio::task::JoinHandle<Result<[u8; 32], TransferFailure>>,
) -> Result<(WriteEvidence, [u8; 32]), TransferFailure> {
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
    limits: super::InflightLimits,
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
    cancel: tokio_util::sync::CancellationToken,
    runtime: InflightRuntime,
    failure: Arc<Mutex<Option<StorageRoleFailure>>>,
    size: u64,
    write_start: u64,
    source_qos: Option<SourceQosBudget>,
}

async fn produce(request: ProducerRequest) -> Result<[u8; 32], TransferFailure> {
    let mut hasher = blake3::Hasher::new();
    let read_inflight = request.runtime.operation_budget();
    let maximum_chunk_bytes = request.runtime.negotiated_chunk_ceiling();
    let stream = request
        .source
        .read(ReadRequest {
            path: request.path.clone(),
            range: Some(0..request.size),
            expected_source: Some(request.source_identity.clone()),
            maximum_chunk_bytes,
            read_inflight,
            cancel: request.cancel.clone(),
            source_qos: request.source_qos.clone(),
        })
        .await;
    let mut stream = match stream {
        Ok(stream) => stream,
        Err(error) => return fail_source_producer(&request, error).await,
    };
    let mut offset = 0_u64;
    while let Some(item) = stream.next().await {
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
        let next = offset.checked_add(bytes.len() as u64).ok_or_else(|| {
            TransferFailure::orchestration(TransferPhase::Transfer, "source offset overflowed")
        })?;
        if next > request.size {
            return Err(TransferFailure::orchestration(
                TransferPhase::Transfer,
                "source emitted more bytes than described",
            ));
        }
        hasher.update(&bytes);
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
            let admission = request
                .runtime
                .admit(output_offset, output.len())
                .await
                .map_err(|error| inflight_transfer_failure(&error, &request.path))?;
            admission
                .complete(output)
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
    Ok(*hasher.finalize().as_bytes())
}

async fn fail_source_producer(
    request: &ProducerRequest,
    error: StorageRoleFailure,
) -> Result<[u8; 32], TransferFailure> {
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
            cancel: cancel.clone(),
            source_qos,
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
