//! Transport-neutral source half of the unified transfer lifecycle.

use std::sync::{Arc, Mutex};

use bytes::Bytes;

use super::{
    ProducerRequest, TransferFailure, TransferOutcome, TransferPhase, TransferSide,
    inflight_channel, inflight_role_failure, produce, recovery_binding_for,
};
use crate::metadata::MetadataPlan;
use crate::model::observation::PrivateBackendEntryFacts;
use crate::model::{EntryIdentityKey, EntryKind, ObservedEntry, StoragePath};
use crate::storage::{
    CheckpointObservation, ExistingDestinationPolicy, FinalDestination, PreflightPolicy,
    PrepareRequest, PublicationEvidence, PublishRequest, ReadSource, SourceDescriptor,
    SourceQosBudget, SourceQosGroup, SourceQosStats, StagedDestination, Storage, VerifyRequest,
    WriteEvidence,
};
use crate::transfer::{InflightLimits, RecoveryProvider, Resumability, TransferIdentity};

/// Inputs owned by the source process for one expert transfer attempt.
#[derive(Clone)]
pub struct ExpertSourceRequest {
    identity: TransferIdentity,
    source: Storage,
    observation: ObservedEntry,
    inflight: InflightLimits,
    cancel: tokio_util::sync::CancellationToken,
    source_qos: Option<SourceQosGroup>,
}

impl ExpertSourceRequest {
    #[must_use]
    pub fn new(
        identity: TransferIdentity,
        source: Storage,
        observation: ObservedEntry,
        inflight: InflightLimits,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            identity,
            source,
            observation,
            inflight,
            cancel,
            source_qos: None,
        }
    }

    #[must_use]
    pub fn with_source_qos(mut self, group: SourceQosGroup) -> Self {
        self.source_qos = Some(group);
        self
    }
}

/// Source facts advertised to the destination before it prepares staged state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpertSourceOffer {
    pub source_size: u64,
    pub maximum_chunk_bytes: usize,
    pub identity_key: EntryIdentityKey,
}

/// Completed source-stream evidence transported after the payload terminal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpertSourceEvidence {
    pub source_size: u64,
    pub blake3: [u8; 32],
    pub identity_key: EntryIdentityKey,
    pub source_qos: SourceQosStats,
}

/// Revalidated source state for a single transport-neutral payload stream.
pub struct ExpertSourceSession {
    _identity: TransferIdentity,
    source: Arc<dyn ReadSource>,
    descriptor: SourceDescriptor,
    inflight: InflightLimits,
    cancel: tokio_util::sync::CancellationToken,
    source_qos: Option<SourceQosBudget>,
    offer: ExpertSourceOffer,
}

impl ExpertSourceSession {
    /// Lends the source role and revalidates the advertised observation before payload I/O.
    ///
    /// # Errors
    /// Returns a typed preflight/describe failure if the source changed or is unavailable.
    pub async fn open(request: ExpertSourceRequest) -> Result<Self, TransferFailure> {
        let source = request
            .source
            .read_source(&PreflightPolicy::production())
            .map_err(|_| {
                TransferFailure::capability(TransferSide::Source, "source capability unavailable")
            })?;
        if request.cancel.is_cancelled() {
            return Err(TransferFailure::orchestration(
                TransferPhase::Preflight,
                "expert source transfer was cancelled",
            ));
        }
        let descriptor = source
            .describe(request.observation.path())
            .await
            .map_err(|error| {
                TransferFailure::role(TransferPhase::Describe, TransferSide::Source, error)
            })?;
        validate_observation(&request.observation, &descriptor)?;
        let source_size = descriptor.size.ok_or_else(|| {
            TransferFailure::orchestration(TransferPhase::Describe, "source has no byte size")
        })?;
        let maximum_chunk_bytes = request
            .inflight
            .negotiated_chunk_ceiling()
            .min(source.maximum_read_chunk_bytes().max(1));
        let source_qos = request
            .source_qos
            .as_ref()
            .map(SourceQosGroup::transfer_budget);
        if let Some(budget) = &source_qos {
            budget.set_logical_bytes(source_size);
        }
        let offer = ExpertSourceOffer {
            source_size,
            maximum_chunk_bytes,
            identity_key: descriptor.source_identity.identity_key(),
        };
        Ok(Self {
            _identity: request.identity,
            source,
            descriptor,
            inflight: request.inflight,
            cancel: request.cancel,
            source_qos,
            offer,
        })
    }

    #[must_use]
    pub const fn offer(&self) -> ExpertSourceOffer {
        self.offer
    }

    /// Starts a full source hash while emitting only bytes at or after the destination's durable prefix.
    ///
    /// # Errors
    /// Returns before I/O when the requested prefix is outside the advertised source.
    pub fn stream_from(self, durable_prefix: u64) -> Result<ExpertSourcePayload, TransferFailure> {
        if durable_prefix > self.offer.source_size {
            return Err(TransferFailure::orchestration(
                TransferPhase::Checkpoint,
                "destination durable prefix exceeds source size",
            ));
        }
        let (runtime, ordered) = inflight_channel(
            self.inflight,
            durable_prefix,
            self.offer.source_size,
            self.cancel.clone(),
        )?;
        let failure = Arc::new(Mutex::new(None));
        let producer = tokio::spawn(produce(ProducerRequest {
            source: self.source,
            path: self.descriptor.path.clone(),
            source_identity: self.descriptor.source_identity,
            cancel: self.cancel,
            runtime,
            failure: Arc::clone(&failure),
            size: self.offer.source_size,
            write_start: durable_prefix,
            source_qos: self.source_qos.clone(),
        }));
        Ok(ExpertSourcePayload {
            ordered,
            producer,
            failure,
            path: self.descriptor.path,
            source_qos: self.source_qos,
            offer: self.offer,
            exhausted: false,
        })
    }
}

/// Bounded source payload consumed by a caller-owned transport.
pub struct ExpertSourcePayload {
    ordered: crate::runtime::inflight::OrderedChunks,
    producer: tokio::task::JoinHandle<Result<[u8; 32], TransferFailure>>,
    failure: Arc<Mutex<Option<crate::storage::StorageRoleFailure>>>,
    path: crate::model::StoragePath,
    source_qos: Option<SourceQosBudget>,
    offer: ExpertSourceOffer,
    exhausted: bool,
}

impl ExpertSourcePayload {
    /// Returns the next ordered payload chunk. Transport backpressure directly stops source reads.
    ///
    /// # Errors
    /// Returns a source-attributed transfer failure.
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>, TransferFailure> {
        match self.ordered.next().await {
            Some(Ok(bytes)) => Ok(Some(bytes)),
            Some(Err(error)) => {
                let role = self
                    .failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
                    .unwrap_or_else(|| inflight_role_failure(&error, &self.path));
                Err(TransferFailure::role(
                    TransferPhase::Transfer,
                    TransferSide::Source,
                    role,
                ))
            }
            None => {
                self.exhausted = true;
                Ok(None)
            }
        }
    }

    /// Returns source hash and `QoS` evidence only after the payload stream was fully consumed.
    ///
    /// # Errors
    /// Returns an orchestration or source failure when the stream was abandoned or source I/O failed.
    pub async fn finish(self) -> Result<ExpertSourceEvidence, TransferFailure> {
        if !self.exhausted {
            self.producer.abort();
            let _ = self.producer.await;
            return Err(TransferFailure::orchestration(
                TransferPhase::Transfer,
                "expert source payload was not fully consumed",
            ));
        }
        let blake3 = self.producer.await.map_err(|_| {
            TransferFailure::orchestration(TransferPhase::Transfer, "source producer stopped")
        })??;
        Ok(ExpertSourceEvidence {
            source_size: self.offer.source_size,
            blake3,
            identity_key: self.offer.identity_key,
            source_qos: self.source_qos.as_ref().map_or(
                SourceQosStats {
                    logical_bytes: self.offer.source_size,
                    ..SourceQosStats::default()
                },
                SourceQosBudget::stats,
            ),
        })
    }
}

fn validate_observation(
    observation: &ObservedEntry,
    descriptor: &SourceDescriptor,
) -> Result<(), TransferFailure> {
    if observation.kind() != EntryKind::File
        || descriptor.kind != EntryKind::File
        || observation.path() != &descriptor.path
        || observation.size() != descriptor.size
        || observation.identity_key() != descriptor.source_identity.identity_key()
    {
        return Err(TransferFailure::orchestration(
            TransferPhase::Describe,
            "source differs from advertised observation",
        ));
    }
    Ok(())
}

/// Inputs owned by the destination process for one expert transfer attempt.
#[derive(Clone)]
pub struct ExpertDestinationRequest {
    identity: TransferIdentity,
    source: ObservedEntry,
    source_maximum_chunk_bytes: usize,
    destination: Storage,
    final_path: StoragePath,
    inflight: InflightLimits,
    cancel: tokio_util::sync::CancellationToken,
    existing_destination: ExistingDestinationPolicy,
    resumability: Resumability,
    recovery_provider: Option<Arc<dyn RecoveryProvider>>,
    metadata_plan: Option<MetadataPlan>,
}

impl ExpertDestinationRequest {
    #[must_use]
    pub fn new(
        identity: TransferIdentity,
        source: ObservedEntry,
        source_maximum_chunk_bytes: usize,
        destination: Storage,
        final_path: StoragePath,
        inflight: InflightLimits,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            identity,
            source,
            source_maximum_chunk_bytes,
            destination,
            final_path,
            inflight,
            cancel,
            existing_destination: ExistingDestinationPolicy::default(),
            resumability: Resumability::default(),
            recovery_provider: None,
            metadata_plan: None,
        }
    }

    #[must_use]
    pub fn with_existing_destination_policy(mut self, policy: ExistingDestinationPolicy) -> Self {
        self.existing_destination = policy;
        self
    }

    #[must_use]
    pub fn with_recovery(
        mut self,
        resumability: Resumability,
        provider: Option<Arc<dyn RecoveryProvider>>,
    ) -> Self {
        self.resumability = resumability;
        self.recovery_provider = provider;
        self
    }

    /// Supplies a plan compiled from the source observation and destination target profile.
    #[must_use]
    pub fn with_metadata_plan(mut self, plan: MetadataPlan) -> Self {
        self.metadata_plan = Some(plan);
        self
    }
}

/// Prepared destination half. Its opaque stage never crosses the process boundary.
pub struct ExpertDestinationSession {
    destination: Arc<dyn StagedDestination>,
    source: SourceDescriptor,
    source_size: u64,
    maximum_chunk_bytes: usize,
    stage: crate::storage::PreparedStage,
    recovery_enabled: bool,
    existing_destination: ExistingDestinationPolicy,
    cancel: tokio_util::sync::CancellationToken,
    metadata_plan: Option<MetadataPlan>,
}

impl ExpertDestinationSession {
    /// Prepares or recovers staged state and durably registers its identity before payload.
    ///
    /// # Errors
    /// Returns a phase-attributed preflight, recovery, registration, or destination failure.
    pub async fn prepare(request: ExpertDestinationRequest) -> Result<Self, TransferFailure> {
        if request.source_maximum_chunk_bytes == 0 {
            return Err(TransferFailure::orchestration(
                TransferPhase::Preflight,
                "expert source chunk ceiling must be non-zero",
            ));
        }
        if request.resumability == Resumability::Disabled && request.recovery_provider.is_some() {
            return Err(TransferFailure::orchestration(
                TransferPhase::Preflight,
                "disabled resumability cannot accept a recovery provider",
            ));
        }
        if request.existing_destination == ExistingDestinationPolicy::VerifyOrSkip
            && request
                .metadata_plan
                .as_ref()
                .is_some_and(MetadataPlan::has_mutations)
        {
            return Err(TransferFailure::orchestration(
                TransferPhase::Preflight,
                "VerifyOrSkip cannot retain an existing object when staged metadata must change",
            ));
        }
        let source = descriptor_from_observation(&request.source)?;
        let source_size = source.size.ok_or_else(|| {
            TransferFailure::orchestration(TransferPhase::Describe, "source has no byte size")
        })?;
        let maximum_chunk_bytes = request
            .inflight
            .negotiated_chunk_ceiling()
            .min(request.source_maximum_chunk_bytes);
        let recovery_enabled = request.resumability == Resumability::Enabled
            && source_size > maximum_chunk_bytes as u64;
        if recovery_enabled && request.recovery_provider.is_none() {
            return Err(TransferFailure::orchestration(
                TransferPhase::Preflight,
                "recoverable transfer requires a recovery provider",
            ));
        }
        let destination = request
            .destination
            .staged_destination(&PreflightPolicy::production())
            .map_err(|_| {
                TransferFailure::capability(
                    TransferSide::Destination,
                    "destination capability unavailable",
                )
            })?;
        if request.cancel.is_cancelled() {
            return Err(TransferFailure::orchestration(
                TransferPhase::Preflight,
                "expert destination transfer was cancelled",
            ));
        }
        let stage =
            prepare_destination_stage(&request, &destination, &source, recovery_enabled).await?;
        Ok(Self {
            destination,
            source,
            source_size,
            maximum_chunk_bytes,
            stage,
            recovery_enabled,
            existing_destination: request.existing_destination,
            cancel: request.cancel,
            metadata_plan: request.metadata_plan,
        })
    }

    #[must_use]
    pub const fn write_offset(&self) -> u64 {
        self.stage.write_offset
    }

    #[must_use]
    pub const fn maximum_chunk_bytes(&self) -> usize {
        self.maximum_chunk_bytes
    }

    #[must_use]
    pub const fn recovery_enabled(&self) -> bool {
        self.recovery_enabled
    }

    /// Explicitly discards a prepared stage when the remote session is cancelled before writing.
    ///
    /// # Errors
    /// Returns the destination cleanup failure.
    pub async fn discard(self) -> Result<(), crate::storage::StorageRoleFailure> {
        self.destination.discard(self.stage).await
    }

    /// Writes the caller-owned bounded transport stream into the prepared destination.
    ///
    /// # Errors
    /// Returns a destination/checkpoint failure while preserving cleanup authority.
    pub async fn write(
        self,
        stream: crate::storage::ByteStream,
    ) -> Result<ExpertDestinationTransferred, TransferFailure> {
        let write_result = self.destination.write(&self.stage, stream).await;
        let write = match write_result {
            Ok(write) => write,
            Err(error) => {
                return Err(TransferFailure::role(
                    TransferPhase::Transfer,
                    TransferSide::Destination,
                    error,
                )
                .with_stage(self.destination, self.stage));
            }
        };
        let checkpoint_result = if self.recovery_enabled {
            self.destination.observe_checkpoint(&self.stage).await
        } else {
            Ok(CheckpointObservation {
                durable_prefix: write.persisted_bytes,
            })
        };
        let checkpoint = match checkpoint_result {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                return Err(TransferFailure::role(
                    TransferPhase::Checkpoint,
                    TransferSide::Destination,
                    error,
                )
                .with_stage(self.destination, self.stage));
            }
        };
        if checkpoint.durable_prefix != self.source_size
            || write.persisted_bytes != self.source_size
        {
            return Err(TransferFailure::orchestration(
                TransferPhase::Checkpoint,
                "durable bytes differ from source size",
            )
            .with_stage(self.destination, self.stage));
        }
        Ok(ExpertDestinationTransferred {
            destination: self.destination,
            source: self.source,
            source_size: self.source_size,
            stage: self.stage,
            _write: write,
            checkpoint,
            existing_destination: self.existing_destination,
            cancel: self.cancel,
            metadata_plan: self.metadata_plan,
        })
    }
}

async fn prepare_destination_stage(
    request: &ExpertDestinationRequest,
    destination: &Arc<dyn StagedDestination>,
    source: &SourceDescriptor,
    recovery_enabled: bool,
) -> Result<crate::storage::PreparedStage, TransferFailure> {
    let recovery = if recovery_enabled {
        let provider = request.recovery_provider.as_ref().ok_or_else(|| {
            TransferFailure::orchestration(
                TransferPhase::Preflight,
                "recoverable transfer requires a recovery provider",
            )
        })?;
        Some(
            provider
                .open()
                .await
                .map_err(TransferFailure::registration)?,
        )
    } else {
        None
    };
    let binding = recovery_binding_for(
        &request.identity,
        &request.destination,
        &request.final_path,
        source,
    );
    let prepare = || PrepareRequest {
        final_destination: FinalDestination::new(request.final_path.clone()),
        source: source.clone(),
        recovery_binding: binding,
    };
    let stage_result = if let Some(identity) = recovery
        .as_ref()
        .and_then(|context| context.identity.clone())
    {
        destination
            .recover(crate::storage::RecoverRequest {
                identity,
                final_destination: FinalDestination::new(request.final_path.clone()),
                source: source.clone(),
                recovery_binding: binding,
                claim_token: recovery.as_ref().map_or([0; 32], |context| context.claim),
            })
            .await
    } else if recovery_enabled {
        destination.prepare(prepare()).await
    } else {
        destination.prepare_ephemeral(prepare()).await
    };
    let stage = stage_result.map_err(|error| {
        TransferFailure::role(TransferPhase::Prepare, TransferSide::Destination, error)
    })?;
    if let Some(recovery) = &recovery {
        let identity = match destination.recovery_identity(&stage).await {
            Ok(identity) => identity,
            Err(error) => {
                return Err(TransferFailure::role(
                    TransferPhase::RecoveryRegistration,
                    TransferSide::Destination,
                    error,
                )
                .with_stage(Arc::clone(destination), stage));
            }
        };
        if let Err(error) = recovery.registrar.register(identity).await {
            return Err(
                TransferFailure::registration(error).with_stage(Arc::clone(destination), stage)
            );
        }
        if request.cancel.is_cancelled() {
            return Err(TransferFailure::orchestration(
                TransferPhase::RecoveryRegistration,
                "transfer was cancelled after recovery registration",
            )
            .with_stage(Arc::clone(destination), stage));
        }
    }
    Ok(stage)
}

/// Destination state after durable payload and before verification/publication.
pub struct ExpertDestinationTransferred {
    destination: Arc<dyn StagedDestination>,
    source: SourceDescriptor,
    source_size: u64,
    stage: crate::storage::PreparedStage,
    _write: WriteEvidence,
    checkpoint: CheckpointObservation,
    existing_destination: ExistingDestinationPolicy,
    cancel: tokio_util::sync::CancellationToken,
    metadata_plan: Option<MetadataPlan>,
}

impl ExpertDestinationTransferred {
    fn evidence_matches(&self, evidence: ExpertSourceEvidence) -> bool {
        evidence.source_size == self.source_size
            && evidence.identity_key == self.source.source_identity.identity_key()
    }

    async fn verify_stage(&self, evidence: ExpertSourceEvidence) -> Result<(), TransferFailure> {
        if !self.evidence_matches(evidence) {
            return Err(TransferFailure::orchestration(
                TransferPhase::Verify,
                "source evidence differs from prepared observation",
            ));
        }
        if self.cancel.is_cancelled() {
            return Err(TransferFailure::orchestration(
                TransferPhase::Verify,
                "transfer was cancelled before verification",
            ));
        }
        let verification = self
            .destination
            .verify(
                &self.stage,
                VerifyRequest {
                    expected_size: self.checkpoint.durable_prefix,
                    expected_blake3: evidence.blake3,
                    cancel: self.cancel.clone(),
                },
            )
            .await
            .map_err(|error| {
                TransferFailure::role(TransferPhase::Verify, TransferSide::Destination, error)
            })?;
        if verification.verified_bytes != self.source_size || verification.blake3 != evidence.blake3
        {
            return Err(TransferFailure::orchestration(
                TransferPhase::Verify,
                "destination verification evidence differs from source evidence",
            ));
        }
        if self.cancel.is_cancelled() {
            return Err(TransferFailure::orchestration(
                TransferPhase::Verify,
                "transfer was cancelled before metadata application",
            ));
        }
        Ok(())
    }

    async fn apply_metadata_stage(
        &self,
    ) -> Result<Option<crate::metadata::MetadataApplicationReport>, TransferFailure> {
        let metadata = if let Some(plan) = &self.metadata_plan {
            Some(
                plan.apply_to_stage(self.destination.as_ref(), &self.stage, self.cancel.clone())
                    .await
                    .map_err(TransferFailure::metadata)?,
            )
        } else {
            None
        };
        if self.cancel.is_cancelled() {
            return Err(TransferFailure::orchestration(
                TransferPhase::Metadata,
                "transfer was cancelled before publication",
            ));
        }
        Ok(metadata)
    }

    /// Verifies complete staged content against source evidence and publishes `FinalDestination`.
    ///
    /// # Errors
    /// Returns a verify/publish failure with truthful staged and final-destination disposition.
    pub async fn complete(
        self,
        evidence: ExpertSourceEvidence,
    ) -> Result<TransferOutcome, TransferFailure> {
        if let Err(failure) = self.verify_stage(evidence).await {
            return Err(failure
                .with_stage(Arc::clone(&self.destination), self.stage)
                .with_source_qos(evidence.source_qos));
        }
        let metadata = match self.apply_metadata_stage().await {
            Ok(metadata) => metadata,
            Err(failure) => {
                return Err(failure
                    .with_stage(Arc::clone(&self.destination), self.stage)
                    .with_source_qos(evidence.source_qos));
            }
        };
        let publication = self
            .destination
            .publish(
                &self.stage,
                PublishRequest {
                    policy: self.existing_destination,
                    expected_size: self.source_size,
                    expected_blake3: evidence.blake3,
                    cancel: self.cancel,
                },
            )
            .await;
        let PublicationEvidence {
            final_destination,
            disposition,
        } = match publication {
            Ok(publication) => publication,
            Err(publication) => {
                let mut failure = TransferFailure::role(
                    TransferPhase::Publish,
                    TransferSide::Destination,
                    publication.error,
                );
                failure.final_destination_changed = publication.final_destination_changed;
                if publication.final_destination_changed {
                    return Err(failure
                        .with_committed_cleanup(self.destination, self.stage)
                        .with_source_qos(evidence.source_qos));
                }
                return Err(failure
                    .with_stage(self.destination, self.stage)
                    .with_source_qos(evidence.source_qos));
            }
        };
        Ok(TransferOutcome {
            final_destination,
            disposition,
            transferred_bytes: self.source_size,
            blake3: evidence.blake3,
            source_qos: evidence.source_qos,
            metadata,
        })
    }
}

fn descriptor_from_observation(
    observation: &ObservedEntry,
) -> Result<SourceDescriptor, TransferFailure> {
    if observation.kind() != EntryKind::File {
        return Err(TransferFailure::orchestration(
            TransferPhase::Describe,
            "ordinary expert transfer source must be a file",
        ));
    }
    let descriptor = SourceDescriptor::new(
        observation.path().clone(),
        observation.kind(),
        observation.size(),
        observation.source_identity().clone(),
    );
    let fact = match observation.backend_facts() {
        PrivateBackendEntryFacts::None => None,
        PrivateBackendEntryFacts::Local(bytes)
        | PrivateBackendEntryFacts::Nfs(bytes)
        | PrivateBackendEntryFacts::Cifs(bytes)
        | PrivateBackendEntryFacts::S3(bytes)
        | PrivateBackendEntryFacts::Hdfs(bytes) => Some(Bytes::copy_from_slice(bytes)),
    };
    Ok(match fact {
        Some(fact) => descriptor.with_backend_fact(fact),
        None => descriptor,
    })
}
