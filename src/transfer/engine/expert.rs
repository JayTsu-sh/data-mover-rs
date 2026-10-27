//! Transport-neutral source half of the unified transfer lifecycle.

use std::sync::Arc;

use bytes::Bytes;

use super::at_destination;
use super::directory_object::{refuse_unholdable, refuse_unwritable};
use super::plan::expert_recovery;
use super::producer::{PayloadReceiver, payload_channel};
use super::{
    ProducerRequest, TransferFailure, TransferOutcome, TransferPhase, TransferSide, produce,
    recovery_binding_for,
};
use crate::metadata::MetadataPlan;
use crate::model::observation::PrivateBackendEntryFacts;
use crate::model::{EntryIdentityKey, EntryKind, ObservedEntry, SourceVersion, StoragePath};
use crate::storage::{
    CheckpointObservation, FinalDestination, PrepareFact, PrepareRequest, PublicationEvidence,
    PublishRequest, ReadSource, SourceDescriptor, SourceQosBudget, SourceQosGroup, SourceQosStats,
    StagedDestination, Storage, VerificationPoint, VerifyRequest, WriteEvidence,
    WriteProgressObserver,
};
use crate::transfer::{TransferIdentity, TransferPolicy};

use super::observation;

/// Inputs owned by the source process for one expert transfer attempt.
#[derive(Clone)]
pub struct ExpertSourceRequest {
    source: Storage,
    observation: ObservedEntry,
    cancel: tokio_util::sync::CancellationToken,
    source_qos: Option<SourceQosGroup>,
}

impl ExpertSourceRequest {
    #[must_use]
    pub fn new(
        source: Storage,
        observation: ObservedEntry,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            source,
            observation,
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
    source: Arc<dyn ReadSource>,
    descriptor: SourceDescriptor,
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
        let source = request.source.read_source().map_err(|_| {
            TransferFailure::capability(TransferSide::Source, "source capability unavailable")
        })?;
        // Before any I/O, as the ordinary entry: a directory object only on a source holding them.
        refuse_unholdable(
            request.observation.path(),
            &request.source,
            TransferSide::Source,
        )?;
        if request.cancel.is_cancelled() {
            return Err(TransferFailure::orchestration(
                TransferPhase::Preflight,
                "expert source transfer was cancelled",
            ));
        }
        let descriptor = observation::describe(source.as_ref(), &request.observation).await?;
        let source_size = descriptor.size.ok_or_else(|| {
            TransferFailure::orchestration(TransferPhase::Describe, "source has no byte size")
        })?;
        let maximum_chunk_bytes = source.maximum_read_chunk_bytes().max(1);
        let source_qos = request
            .source_qos
            .as_ref()
            .map(SourceQosGroup::transfer_budget);
        if let Some(budget) = &source_qos {
            budget.set_logical_bytes(source_size);
        }
        // The observation's key, which the destination half compares its evidence with; it
        // names the object the describe pinned (`validate_observation`).
        let offer = ExpertSourceOffer {
            source_size,
            maximum_chunk_bytes,
            identity_key: request.observation.identity_key(),
        };
        Ok(Self {
            source,
            descriptor,
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
        let (sender, receiver) = payload_channel(self.cancel.clone(), self.descriptor.path.clone());
        let producer = tokio::spawn(produce(ProducerRequest {
            hash_content: true,
            source: self.source,
            path: self.descriptor.path.clone(),
            version: self.descriptor.version,
            source_identity: self.descriptor.source_identity,
            cancel: self.cancel,
            sender,
            size: self.offer.source_size,
            write_start: durable_prefix,
            source_qos: self.source_qos.clone(),
        }));
        Ok(ExpertSourcePayload {
            receiver,
            producer,
            source_qos: self.source_qos,
            offer: self.offer,
            exhausted: false,
        })
    }
}

/// Bounded source payload consumed by a caller-owned transport.
pub struct ExpertSourcePayload {
    receiver: PayloadReceiver,
    producer: tokio::task::JoinHandle<Result<Option<[u8; 32]>, TransferFailure>>,
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
        match self.receiver.next().await {
            Some(Ok(bytes)) => Ok(Some(bytes)),
            Some(Err(error)) => Err(TransferFailure::role(
                TransferPhase::Transfer,
                TransferSide::Source,
                error,
            )),
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
        let blake3 = self
            .producer
            .await
            .map_err(|_| {
                TransferFailure::orchestration(TransferPhase::Transfer, "source producer stopped")
            })??
            .ok_or_else(|| {
                TransferFailure::orchestration(
                    TransferPhase::Transfer,
                    "expert source digest is unavailable",
                )
            })?;
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

/// Inputs owned by the destination process for one expert transfer attempt.
#[derive(Clone)]
pub struct ExpertDestinationRequest {
    identity: TransferIdentity,
    source: ObservedEntry,
    source_maximum_chunk_bytes: usize,
    destination: Storage,
    final_path: StoragePath,
    cancel: tokio_util::sync::CancellationToken,
    transfer_policy: TransferPolicy,
    metadata_plan: Option<MetadataPlan>,
    write_observer: Option<WriteProgressObserver>,
}

impl ExpertDestinationRequest {
    /// The transfer identity is derived, as for
    /// [`TransferRequest::new`](crate::transfer::TransferRequest::new), from the source endpoint
    /// and path the observation carries and from the destination endpoint and final path.
    #[must_use]
    pub fn new(
        source: ObservedEntry,
        source_maximum_chunk_bytes: usize,
        destination: Storage,
        final_path: StoragePath,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Self {
        // The expert halves copy the current version only.
        let identity = TransferIdentity::derive(
            source.source_identity().backend(),
            source.path(),
            &SourceVersion::Current,
            destination.identity(),
            &final_path,
        );
        Self {
            identity,
            source,
            source_maximum_chunk_bytes,
            destination,
            final_path,
            cancel,
            transfer_policy: TransferPolicy::default(),
            metadata_plan: None,
            write_observer: None,
        }
    }

    #[must_use]
    pub const fn with_transfer_policy(mut self, policy: TransferPolicy) -> Self {
        self.transfer_policy = policy;
        self
    }

    /// Names the transfer with a caller-chosen identity instead of the derived one.
    #[must_use]
    pub const fn with_identity_override(mut self, identity: TransferIdentity) -> Self {
        self.identity = identity;
        self
    }

    /// Supplies a plan compiled from the source observation and destination target profile.
    #[must_use]
    pub fn with_metadata_plan(mut self, plan: MetadataPlan) -> Self {
        self.metadata_plan = Some(plan);
        self
    }

    /// Observes successful destination writes and recorded recovery prefixes. Buffered S3
    /// single-PUT content is confirmed only during publication, when the service accepts it.
    /// Notifications never authorize resume; a reconnect still discovers destination state.
    #[must_use]
    pub fn with_write_observer(mut self, observer: WriteProgressObserver) -> Self {
        self.write_observer = Some(observer);
        self
    }
}

/// Prepared destination half. Its opaque stage never crosses the process boundary.
pub struct ExpertDestinationSession {
    identity: TransferIdentity,
    destination: Arc<dyn StagedDestination>,
    source: SourceDescriptor,
    source_size: u64,
    maximum_chunk_bytes: usize,
    stage: crate::storage::PreparedStage,
    recovery_enabled: bool,
    effective_recovery: super::EffectiveRecovery,
    cancel: tokio_util::sync::CancellationToken,
    metadata_plan: Option<MetadataPlan>,
}

impl ExpertDestinationSession {
    /// Prepares the stage at the destination, under the per-file lease: a `Checkpointed` transfer
    /// that keeps checkpoints resumes what an earlier attempt left there; otherwise what is found
    /// is cleaned up and the stage starts from zero.
    ///
    /// # Errors
    /// Returns a phase-attributed preflight or destination failure.
    pub async fn prepare(request: ExpertDestinationRequest) -> Result<Self, TransferFailure> {
        if request.source_maximum_chunk_bytes == 0 {
            return Err(TransferFailure::orchestration(
                TransferPhase::Preflight,
                "expert source chunk ceiling must be non-zero",
            ));
        }
        if request.transfer_policy == TransferPolicy::Direct {
            return Err(TransferFailure::capability(
                TransferSide::Destination,
                "Direct requires the ordinary transfer entry with source identity validation",
            ));
        }
        // Before any I/O: a directory object only on a destination that holds them (P7 F5).
        refuse_unholdable(
            &request.final_path,
            &request.destination,
            TransferSide::Destination,
        )?;
        let source = descriptor_from_observation(&request.source)?;
        let source_size = source.size.ok_or_else(|| {
            TransferFailure::orchestration(TransferPhase::Describe, "source has no byte size")
        })?;
        let maximum_chunk_bytes = request.source_maximum_chunk_bytes;
        let destination = request.destination.staged_destination().map_err(|_| {
            TransferFailure::capability(
                TransferSide::Destination,
                "destination capability unavailable",
            )
        })?;
        // Before any destination I/O: what this destination cannot write, by name and size (a
        // directory object one request cannot carry, P7 D13; an ONTAP NAS bucket's limits, §14.2).
        refuse_unwritable(&request.final_path, source.size, &*destination)?;
        let (recovery_enabled, effective_recovery) = expert_recovery(
            request.transfer_policy,
            destination.resumable_at_destination(),
            source_size,
            maximum_chunk_bytes,
        );
        if request.cancel.is_cancelled() {
            return Err(TransferFailure::orchestration(
                TransferPhase::Preflight,
                "expert destination transfer was cancelled",
            ));
        }
        let mut stage =
            prepare_destination_stage(&request, &destination, &source, recovery_enabled).await?;
        // The destination may decline recovery for this stage (a single S3 PUT).
        let recovery_enabled = recovery_enabled && stage.recovery_enabled();
        let effective_recovery = match effective_recovery {
            super::EffectiveRecovery::Checkpointed if !recovery_enabled => {
                super::EffectiveRecovery::SkippedBelowCheckpointThreshold
            }
            effective_recovery => effective_recovery,
        };
        stage.durable_publication = request.transfer_policy == TransferPolicy::Checkpointed;
        stage.write_observer = request.write_observer;
        if stage.write_offset > 0 && stage.recovery_enabled() {
            stage.checkpoint_recorded(stage.write_offset);
        }
        Ok(Self {
            identity: request.identity,
            destination,
            source,
            source_size,
            maximum_chunk_bytes,
            stage,
            recovery_enabled,
            effective_recovery,
            cancel: request.cancel,
            metadata_plan: request.metadata_plan,
        })
    }

    /// The identity this transfer runs under.
    #[must_use]
    pub const fn identity(&self) -> TransferIdentity {
        self.identity
    }

    /// What prepare found at the destination and did about it.
    #[must_use]
    pub const fn prepare_fact(&self) -> PrepareFact {
        self.stage.prepare_fact
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
                "completed bytes differ from source size",
            )
            .with_stage(self.destination, self.stage));
        }
        if self.stage.recovery_enabled() {
            self.stage.checkpoint_recorded(checkpoint.durable_prefix);
        }
        Ok(ExpertDestinationTransferred {
            identity: self.identity,
            destination: self.destination,
            source: self.source,
            source_size: self.source_size,
            stage: self.stage,
            _write: write,
            checkpoint,
            cancel: self.cancel,
            metadata_plan: self.metadata_plan,
            effective_recovery: self.effective_recovery,
        })
    }
}

/// The expert destination half's prepare, under the per-file lease.
async fn prepare_destination_stage(
    request: &ExpertDestinationRequest,
    destination: &Arc<dyn StagedDestination>,
    source: &SourceDescriptor,
    recovery_enabled: bool,
) -> Result<crate::storage::PreparedStage, TransferFailure> {
    let binding = recovery_binding_for(
        &request.identity,
        &request.destination,
        &request.final_path,
        source,
    );
    let lease = at_destination::acquire_for(request.destination.identity(), &request.final_path)?;
    let spec = at_destination::Spec {
        policy: request.transfer_policy,
        identity: request.identity,
        resumable: request.transfer_policy == TransferPolicy::Checkpointed && recovery_enabled,
        recoverable: recovery_enabled,
        checkpoint_hash: false,
        cancel: request.cancel.clone(),
    };
    let prepare = PrepareRequest {
        final_destination: FinalDestination::new(request.final_path.clone()).with_private_creation(
            request
                .metadata_plan
                .as_ref()
                .is_some_and(MetadataPlan::carries_mode),
        ),
        source: source.clone(),
        recovery_binding: binding,
    };
    at_destination::prepare_with(destination, prepare, &spec, lease).await
}

/// Destination state after completed writes and before verification/publication.
/// Local `AtomicReplace` does not promise crash-durable payload.
pub struct ExpertDestinationTransferred {
    identity: TransferIdentity,
    destination: Arc<dyn StagedDestination>,
    source: SourceDescriptor,
    source_size: u64,
    stage: crate::storage::PreparedStage,
    _write: WriteEvidence,
    checkpoint: CheckpointObservation,
    cancel: tokio_util::sync::CancellationToken,
    metadata_plan: Option<MetadataPlan>,
    effective_recovery: super::EffectiveRecovery,
}

/// What a failed verification says about the staged bytes.
enum StagedContent {
    /// They were read and do not match the source.
    Mismatched,
    /// The failure came before they could be judged.
    NotJudged,
}

impl ExpertDestinationTransferred {
    fn evidence_matches(&self, evidence: ExpertSourceEvidence) -> bool {
        evidence.source_size == self.source_size
            && evidence.identity_key == self.source.source_identity.identity_key()
    }

    /// Everything before publication: verification of the stage (unless the destination verifies
    /// after publication, when only the evidence is checked), then metadata.
    async fn before_publication(
        &self,
        evidence: ExpertSourceEvidence,
        verify_after: bool,
    ) -> Result<Option<crate::metadata::MetadataApplicationReport>, (TransferFailure, StagedContent)>
    {
        if verify_after {
            self.check_evidence(evidence)
                .map_err(|failure| (failure, StagedContent::NotJudged))?;
        } else {
            self.verify_stage(evidence).await?;
        }
        self.apply_metadata_stage()
            .await
            .map_err(|failure| (failure, StagedContent::NotJudged))
    }

    /// Publishes the stage. A failure says whether the final destination changed (clean-up owed)
    /// or not (the stage is kept).
    async fn publish_stage(
        &self,
        evidence: ExpertSourceEvidence,
    ) -> Result<PublicationEvidence, (TransferFailure, bool)> {
        self.destination
            .publish(
                &self.stage,
                PublishRequest {
                    expected_size: self.source_size,
                    expected_blake3: Some(evidence.blake3),
                    cancel: self.cancel.clone(),
                },
            )
            .await
            .map_err(|publication| {
                let changed = publication.final_destination_changed;
                let mut failure = TransferFailure::role(
                    TransferPhase::Publish,
                    TransferSide::Destination,
                    publication.error,
                );
                failure.final_destination_changed = changed;
                (failure, changed)
            })
    }

    /// The source evidence must be the prepared observation's, and the transfer not cancelled.
    fn check_evidence(&self, evidence: ExpertSourceEvidence) -> Result<(), TransferFailure> {
        if !self.evidence_matches(evidence) {
            return Err(TransferFailure::orchestration(
                TransferPhase::Verify,
                "source evidence differs from prepared observation",
            ));
        }
        if self.cancel.is_cancelled() {
            return Err(TransferFailure::orchestration(
                TransferPhase::Verify,
                "transfer was cancelled before verification or publication",
            ));
        }
        Ok(())
    }

    /// Verifies the stage; a failure says whether the staged bytes themselves did not match
    /// (the only failure that makes a resumed stage stale).
    async fn verify_stage(
        &self,
        evidence: ExpertSourceEvidence,
    ) -> Result<(), (TransferFailure, StagedContent)> {
        self.check_evidence(evidence)
            .map_err(|failure| (failure, StagedContent::NotJudged))?;
        let verification = self
            .destination
            .verify(
                &self.stage,
                VerifyRequest {
                    expected_size: self.checkpoint.durable_prefix,
                    expected_blake3: evidence.blake3,
                    cancel: self.cancel.clone(),
                    published: None,
                },
            )
            .await
            .map_err(|error| {
                let content = if super::content_mismatch(&error) {
                    StagedContent::Mismatched
                } else {
                    StagedContent::NotJudged
                };
                let failure =
                    TransferFailure::role(TransferPhase::Verify, TransferSide::Destination, error);
                (failure, content)
            })?;
        if verification.verified_bytes != self.source_size || verification.blake3 != evidence.blake3
        {
            return Err((
                TransferFailure::orchestration(
                    TransferPhase::Verify,
                    "destination verification evidence differs from source evidence",
                ),
                StagedContent::Mismatched,
            ));
        }
        if self.cancel.is_cancelled() {
            return Err((
                TransferFailure::orchestration(
                    TransferPhase::Verify,
                    "transfer was cancelled before metadata application",
                ),
                StagedContent::NotJudged,
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
        let verify_after =
            self.destination.verification_point(&self.stage) == VerificationPoint::AfterPublish;
        let metadata = match self.before_publication(evidence, verify_after).await {
            Ok(metadata) => metadata,
            Err((failure, StagedContent::Mismatched)) => {
                let failure = failure.with_source_qos(evidence.source_qos);
                return Err(
                    super::discard_stale_stage(self.destination, self.stage, failure).await,
                );
            }
            Err((failure, StagedContent::NotJudged)) => {
                return Err(failure
                    .with_stage(Arc::clone(&self.destination), self.stage)
                    .with_source_qos(evidence.source_qos));
            }
        };
        let publication = match self.publish_stage(evidence).await {
            Ok(publication) => publication,
            Err((failure, true)) => {
                return Err(failure
                    .with_committed_cleanup(self.destination, self.stage)
                    .with_source_qos(evidence.source_qos));
            }
            Err((failure, false)) => {
                return Err(failure
                    .with_stage(self.destination, self.stage)
                    .with_source_qos(evidence.source_qos));
            }
        };
        if verify_after {
            super::verify_published(
                &self.destination,
                &self.stage,
                &publication,
                self.source_size,
                Some(evidence.blake3),
                self.cancel,
            )
            .await
            .map_err(|failure| failure.with_source_qos(evidence.source_qos))?;
        }
        let verification = super::TransferVerification::of(&*self.destination, &self.stage, true);
        let PublicationEvidence {
            final_destination,
            disposition,
            version: destination_version,
            ..
        } = publication;
        let prepare = self.stage.prepare_fact;
        Ok(TransferOutcome {
            identity: self.identity,
            final_destination,
            prepare,
            reused_bytes: prepare.reused_bytes(),
            disposition,
            transferred_bytes: self.source_size,
            blake3: Some(evidence.blake3),
            read_back: super::ReadBackVerification::Enabled,
            source_qos: evidence.source_qos,
            metadata,
            route: super::TransferRoute::Streaming,
            recovery: self.effective_recovery,
            destination_version,
            verification,
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
    )
    .with_rename_evidence(observation.rename_evidence().copied());
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
