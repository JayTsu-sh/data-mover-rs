use std::fmt;
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use futures::StreamExt as _;

use super::identity::{BindingSource, binding_hash};
use super::model::InflightLimits;
use super::{ReadBackVerification, TransferPolicy, TransferRequest};
use crate::model::{
    EntryKind, EntryOperationFailure, FailureClass, Operation, SourceIdentity, SourceVersion,
    StoragePath, Transience,
};
use crate::runtime::inflight::{
    InflightConfig, InflightFailure, InflightRuntime, OrderedChunks, ReadRange, SequentialRanges,
};
use crate::storage::{
    CheckpointObservation, NativePair, PreflightPolicy, PrepareFact, PreparedStage,
    PublicationDisposition, PublicationEvidence, PublishRequest, ReadRequest, ReadSource,
    SourceDescriptor, SourceQosBudget, SourceQosStats, StagedDestination, StorageRoleFailure,
    VerificationPoint, VerifyRequest, WriteEvidence,
};
use negotiation::{CopiedMetadataPlan, apply_copied_metadata, copied_metadata_plan};

mod at_destination;
mod expert;
mod guard;
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

    /// Whether the failed attempt retains unpublished state eligible for a later resume. `false`
    /// does not mean nothing can be resumed: a destination may keep its pointer from prepare
    /// (S3 since ADR-0006 C16), so an unrecoverable stage that is dropped instead of discarded
    /// may still be resumed by the next attempt. Discard it to start over.
    #[must_use]
    pub fn has_recoverable_stage(&self) -> bool {
        self.failed_stage
            .as_ref()
            .is_some_and(|failed| failed.stage.recovery_enabled())
    }

    /// Whether the failed attempt retains unpublished state that may be explicitly discarded.
    /// A resumed stage kept at the destination that failed verification is not retained: it was
    /// cleaned up in place, so its pointer cannot resume it again.
    #[must_use]
    pub const fn has_unpublished_stage(&self) -> bool {
        self.failed_stage.is_some()
    }

    /// Whether publication committed — or may have: an S3 completion that failed without a
    /// definite refusal is reported so (ADR-0006 C15c) — and staged artifacts still require
    /// cleanup. Not proof that the new content is published.
    #[must_use]
    pub const fn has_pending_cleanup(&self) -> bool {
        self.committed_cleanup.is_some()
    }

    /// Whether publication committed — or may have (an S3 completion that failed without a
    /// definite refusal, ADR-0006 C15c) — or a direct attempt may have modified the final
    /// target. Not proof that the new content is published: retry according to the error's
    /// transience.
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
        failed.destination.discard(failed.stage).await
    }

    /// Consumes a failure after publication committed — or may have — and idempotently removes
    /// staged artifacts only.
    ///
    /// This never removes or rolls back the final destination. After an S3 completion that may
    /// not have committed (ADR-0006 C15c), cleanup aborts the upload: the final key then keeps
    /// whatever it held, so retry the transfer instead when the failure is transient.
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
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferOutcome {
    /// The identity the transfer ran under: derived from its endpoints and paths, or the
    /// caller's override.
    pub identity: super::TransferIdentity,
    pub final_destination: StoragePath,
    /// What prepare found at the destination and did about it.
    pub prepare: PrepareFact,
    /// Bytes the destination already held and the transfer did not write again
    /// (`prepare.reused_bytes()`); the source may still have been read for them, for the digest.
    pub reused_bytes: u64,
    pub disposition: PublicationDisposition,
    pub transferred_bytes: u64,
    /// Present only when destination read-back verification was performed.
    pub blake3: Option<[u8; 32]>,
    pub read_back: ReadBackVerification,
    pub source_qos: SourceQosStats,
    pub metadata: Option<crate::metadata::MetadataApplicationReport>,
    pub route: TransferRoute,
    pub recovery: EffectiveRecovery,
    /// The version the publication created, for a destination that versions its objects.
    pub destination_version: Option<String>,
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
/// publication has not completed — except a resumed stage kept at the destination that failed
/// verification, which is cleaned up in place.
pub async fn transfer(request: TransferRequest) -> Result<TransferOutcome, TransferFailure> {
    let read_back = request.read_back;
    let cancel = request.cancel.clone();
    let transferred = run_until_transferred(request).await?;
    let identity = transferred.identity;
    let route = transfer_route(transferred.data_path);
    let recovery = transferred.effective_recovery;
    let source_qos = transferred_source_qos(&transferred);
    let verify_after = read_back == ReadBackVerification::Enabled
        && transferred
            .destination
            .verification_point(&transferred.stage)
            == VerificationPoint::AfterPublish;
    let transferred = if read_back == ReadBackVerification::Enabled && !verify_after {
        verify_transferred(transferred, cancel.clone(), source_qos)
            .await?
            .0
    } else {
        transferred
    };
    let expected_size = transferred.checkpoint.durable_prefix;
    let source_digest = transferred.source_blake3;
    let prepare = transferred.stage.prepare_fact;
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
    if verify_after && (cancel.is_cancelled() || source_digest.is_none()) {
        // Nothing is published yet: stop here, keeping the stage, rather than publish content
        // that could not be verified afterwards.
        return Err(TransferFailure::orchestration(
            TransferPhase::Verify,
            "transfer was cancelled, or has no source digest, before publication",
        )
        .with_stage(Arc::clone(&transferred.destination), transferred.stage)
        .with_source_qos(source_qos));
    }
    let (evidence, transferred) = publish_transferred(
        transferred,
        expected_size,
        source_digest,
        cancel.clone(),
        source_qos,
    )
    .await?;
    if verify_after {
        verify_published(
            &transferred.destination,
            &transferred.stage,
            &evidence,
            expected_size,
            source_digest,
            cancel,
        )
        .await
        .map_err(|failure| failure.with_source_qos(source_qos))?;
    }
    let PublicationEvidence {
        final_destination,
        disposition,
        version: destination_version,
        ..
    } = evidence;
    Ok(TransferOutcome {
        identity,
        final_destination,
        prepare,
        reused_bytes: prepare.reused_bytes(),
        disposition,
        transferred_bytes: expected_size,
        blake3,
        read_back,
        source_qos,
        metadata,
        route,
        recovery,
        destination_version,
    })
}

/// Read-back verification of content already published at its final name (a destination whose
/// [`VerificationPoint`] is `AfterPublish`). Nothing can be rolled back any more: a failure —
/// including cancellation — is reported with `final_destination_changed` and keeps no stage.
pub(super) async fn verify_published(
    destination: &Arc<dyn StagedDestination>,
    stage: &PreparedStage,
    published: &PublicationEvidence,
    expected_size: u64,
    source_digest: Option<[u8; 32]>,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<(), TransferFailure> {
    let changed = |mut failure: TransferFailure| {
        failure.final_destination_changed = true;
        failure
    };
    let Some(expected_blake3) = source_digest else {
        return Err(changed(TransferFailure::orchestration(
            TransferPhase::Verify,
            "source digest is unavailable for verification",
        )));
    };
    let verification = destination
        .verify(
            stage,
            VerifyRequest {
                expected_size,
                expected_blake3,
                cancel,
                published: Some(published.clone()),
            },
        )
        .await
        .map_err(|error| {
            changed(TransferFailure::role(
                TransferPhase::Verify,
                TransferSide::Destination,
                error,
            ))
        })?;
    if verification.verified_bytes != expected_size || verification.blake3 != expected_blake3 {
        return Err(changed(TransferFailure::orchestration(
            TransferPhase::Verify,
            "published content differs from source evidence",
        )));
    }
    Ok(())
}

/// Publishes verified staged content.
async fn publish_transferred(
    transferred: Transferred,
    expected_size: u64,
    source_digest: Option<[u8; 32]>,
    cancel: tokio_util::sync::CancellationToken,
    source_qos: SourceQosStats,
) -> Result<(PublicationEvidence, Transferred), TransferFailure> {
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
    let evidence = match publication {
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
    Ok((evidence, transferred))
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
                published: None,
            },
        )
        .await;
    let verification = match verification {
        Ok(verification) => verification,
        Err(error) if content_mismatch(&error) => {
            let failure =
                TransferFailure::role(TransferPhase::Verify, TransferSide::Destination, error)
                    .with_source_qos(source_qos);
            return Err(discard_stale_resume(transferred, failure).await);
        }
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
        let failure = TransferFailure::orchestration(
            TransferPhase::Verify,
            "destination verification evidence differs from source evidence",
        )
        .with_source_qos(source_qos);
        return Err(discard_stale_resume(transferred, failure).await);
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

/// Whether a failed verification judged the staged content — the backend read it and it differs —
/// rather than failing to look. Backends report a mismatch as `Corruption`, not as evidence.
pub(super) fn content_mismatch(error: &StorageRoleFailure) -> bool {
    matches!(error, StorageRoleFailure::Entry(entry) if entry.class() == FailureClass::Corruption)
}

/// A resumed stage kept at the destination that fails verification is stale — its pointer would
/// resume it again on every retry, surviving restarts. It is cleaned up in place, and the failure
/// keeps no stage; any other failing stage stays with the failure as before.
async fn discard_stale_resume(
    transferred: Transferred,
    failure: TransferFailure,
) -> TransferFailure {
    discard_stale_stage(transferred.destination, transferred.stage, failure).await
}

/// [`discard_stale_resume`] for a bare stage, shared with the expert destination half.
pub(super) async fn discard_stale_stage(
    destination: Arc<dyn StagedDestination>,
    stage: PreparedStage,
    failure: TransferFailure,
) -> TransferFailure {
    let resumed_here =
        stage.at_destination && matches!(stage.prepare_fact, PrepareFact::Resumed { .. });
    if !resumed_here {
        return failure.with_stage(destination, stage);
    }
    // `discard` removes the pointer before the stage (the backend contract), so even a clean-up
    // that fails halfway leaves no pointer to resume from: the next prepare restarts. The failure
    // to report is the verification either way.
    let _cleanup = destination.discard(stage).await;
    failure
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

    #[cfg(test)]
    pub(crate) fn recovery_enabled(&self) -> bool {
        self.stage.recovery_enabled()
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
    let TransferRoles {
        source,
        destination,
    } = lend_transfer_roles(&request)?;
    let descriptor = describe_source(&request, &*source, source_qos.as_ref()).await?;
    let copied_metadata_plan = copied_metadata_plan(&request, &descriptor).await?;
    let recovery_binding = recovery_binding(&request, &descriptor);
    at_destination::run_until_transferred(
        &request,
        (source, destination),
        descriptor,
        copied_metadata_plan,
        recovery_binding,
        source_qos,
    )
    .await
}

fn final_recovery(stage: &PreparedStage, plan: TransferPlan) -> EffectiveRecovery {
    if stage.recovery_enabled() {
        EffectiveRecovery::Checkpointed
    } else if plan.effective_recovery == EffectiveRecovery::Checkpointed {
        // The destination declined recovery for this stage (reachable only for a destination
        // without an automatic interval: `plan_request` already reports the others).
        EffectiveRecovery::SkippedBelowCheckpointThreshold
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
    // S3's `pointer_before_checkpoint` mirrors when this arms the interval (a `Discover` prepare
    // of a known size over it): keep the two in step. A native copy arms none; its S3 fill turns
    // recovery on itself (ADR-0006 C18).
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
    use crate::storage::RestartReason;
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

    /// A publication that committed but failed before removing its pointer leaves a pointer
    /// without a stage; the next transfer restarts instead of stranding on it.
    #[tokio::test]
    async fn a_pointer_left_after_publication_restarts_instead_of_stranding()
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
        .with_transfer_policy(TransferPolicy::Checkpointed);

        let transferred = run_until_transferred(request.clone()).await?;
        let source_qos = transferred_source_qos(&transferred);
        let (transferred, verification) = verify_transferred(
            transferred,
            tokio_util::sync::CancellationToken::new(),
            source_qos,
        )
        .await?;
        role.fail_after_publication_commit();
        let failed = transferred
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
            .err()
            .ok_or("the injected post-commit failure")?;
        assert!(failed.final_destination_changed);
        drop(transferred);

        let outcome = transfer(request).await?;
        assert_eq!(
            outcome.prepare,
            PrepareFact::Restarted {
                reason: RestartReason::PointerWithoutStage
            }
        );
        assert_eq!(outcome.transferred_bytes, payload.len() as u64);
        assert_eq!(std::fs::read(destination_root.join("final.bin"))?, payload);
        // The unlocked claim the dropped stage left goes with the restarted run's artifacts.
        for entry in std::fs::read_dir(&destination_root)? {
            let name = entry?.file_name();
            assert!(
                !name.to_string_lossy().starts_with(".data-mover-"),
                "{name:?}"
            );
        }
        std::fs::remove_dir_all(source_root)?;
        std::fs::remove_dir_all(destination_root)?;
        Ok(())
    }
}
