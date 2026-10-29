use super::guard::DestinationLease;
use super::{
    Arc, CopiedMetadataPlan, EffectiveRecovery, NativePair, ReadSource, SequentialRanges,
    SourceDescriptor, SourceQosBudget, SourceQosStats, StagedDestination, TransferFailure,
    TransferPhase, TransferPlan, TransferPolicy, TransferRequest, TransferSide, Transferred,
    final_recovery, read_exact_range,
};
use crate::storage::{
    DestinationPrepareRequest, FinalDestination, NativeSourceBinding, NativeStageEvidence,
    PrepareRequest, PreparedStage, ResumeMode, WriteEvidence,
};

pub(super) fn eligible_native_pair(request: &TransferRequest) -> Option<NativePair> {
    request.source.native_pair(&request.destination)
}

pub(super) struct NativeTransferInput {
    pub source: Arc<dyn ReadSource>,
    pub destination: Arc<dyn StagedDestination>,
    pub descriptor: SourceDescriptor,
    pub pair: NativePair,
    pub recovery_binding: [u8; 32],
    pub source_qos: Option<SourceQosBudget>,
    pub plan: TransferPlan,
    pub copied_metadata_plan: Option<CopiedMetadataPlan>,
}

/// The native route's stage (ADR-0006 C18): the destination's native endpoint prepares it at the
/// final file, looking at what an earlier attempt — native or streamed — left there: a
/// `Checkpointed` copy resumes an equal binding, any other policy cleans up and starts from zero.
/// Nothing is recorded where data-mover runs; the stage carries the per-file lease.
async fn prepare_native_stage(
    request: &TransferRequest,
    input: &NativeTransferInput,
    lease: DestinationLease,
) -> Result<PreparedStage, TransferFailure> {
    let resume = if request.transfer_policy == TransferPolicy::Checkpointed
        && input.destination.resumable_at_destination()
    {
        ResumeMode::Discover
    } else {
        ResumeMode::Restart
    };
    let prepare = PrepareRequest {
        final_destination: FinalDestination::new(request.final_path().clone())
            .with_private_creation(
                input
                    .copied_metadata_plan
                    .as_ref()
                    .is_some_and(CopiedMetadataPlan::carries_mode),
            ),
        source: input.descriptor.clone(),
        recovery_binding: input.recovery_binding,
    };
    let mut stage = input
        .pair
        .prepare_native(
            DestinationPrepareRequest::new(prepare, *request.identity().as_bytes())
                .with_resume(resume)
                .with_recoverable(false)
                .with_checkpoint_hash(defer_source_hash(request, input)),
        )
        .await
        .map_err(|error| {
            TransferFailure::role(TransferPhase::Prepare, TransferSide::Destination, error)
        })?;
    if !stage.at_destination() {
        let fact = stage.prepare_fact();
        stage.mark_at_destination(fact);
    }
    stage.set_exclusive(Some(Box::new(lease)));
    Ok(stage)
}

pub(super) async fn transfer_native(
    request: &TransferRequest,
    input: NativeTransferInput,
    lease: DestinationLease,
) -> Result<Transferred, TransferFailure> {
    let binding = input
        .pair
        .bind_source(&input.descriptor)
        .await
        .map_err(|error| {
            TransferFailure::role(TransferPhase::Describe, TransferSide::Source, error)
        })?;
    let digest = initial_digest(request, &input).await?;
    let mut stage = prepare_native_stage(request, &input, lease).await?;
    if (stage.restored_checkpoint().is_some() || hash_checkpoint_eligible(request, &input))
        && input.destination.stage_supports_hash_checkpoint(&stage)
    {
        let binding = super::negotiation::checkpoint_metadata_binding(
            request,
            input.copied_metadata_plan.as_ref(),
        );
        stage = super::publication::bind_prepared_checkpoint_metadata(
            stage,
            &input.destination,
            binding,
        )?;
    }
    let digest = match prepared_digest(request, &input, &stage, digest).await {
        Ok(digest) => digest,
        Err(error) => return Err(error.with_stage(input.destination, stage)),
    };
    copy_and_finish(request, input, stage, binding, digest).await
}

async fn initial_digest(
    request: &TransferRequest,
    input: &NativeTransferInput,
) -> Result<Option<[u8; 32]>, TransferFailure> {
    if request.needs_source_digest() && !defer_source_hash(request, input) {
        return hash_source(
            &*input.source,
            &input.descriptor,
            request,
            input.plan,
            input.source_qos.clone(),
        )
        .await
        .map(Some);
    }
    Ok(None)
}

async fn prepared_digest(
    request: &TransferRequest,
    input: &NativeTransferInput,
    stage: &PreparedStage,
    digest: Option<[u8; 32]>,
) -> Result<Option<[u8; 32]>, TransferFailure> {
    let checkpoint_hash =
        stage.restored_checkpoint().is_some() || hash_checkpoint_eligible(request, input);
    if checkpoint_hash && input.destination.stage_supports_hash_checkpoint(stage) {
        return super::checkpointed::hash_only(
            request,
            &input.source,
            &input.destination,
            &input.descriptor,
            stage,
            input.plan,
            input.source_qos.clone(),
        )
        .await
        .map(Some);
    }
    if defer_source_hash(request, input) && request.needs_source_digest() {
        return hash_source(
            &*input.source,
            &input.descriptor,
            request,
            input.plan,
            input.source_qos.clone(),
        )
        .await
        .map(Some);
    }
    Ok(digest)
}

async fn copy_and_finish(
    request: &TransferRequest,
    input: NativeTransferInput,
    stage: PreparedStage,
    binding: NativeSourceBinding,
    digest: Option<[u8; 32]>,
) -> Result<Transferred, TransferFailure> {
    if stage.recovered_publication().is_some() {
        let native = NativeStageEvidence {
            write: WriteEvidence {
                persisted_bytes: input.plan.source_size,
            },
            native_bytes: 0,
            native_requests: 0,
            single_operation: false,
        };
        return finish_native(request, input, stage, native, digest).await;
    }
    let native = match input
        .pair
        .copy_into_stage(binding, &stage, request.cancel.clone())
        .await
    {
        Ok(native) => native,
        Err(failure) => {
            let stats = match &input.source_qos {
                Some(qos) => {
                    qos.record_native(failure.native_bytes, failure.native_requests);
                    qos.stats()
                }
                None => SourceQosStats {
                    logical_bytes: input.plan.source_size,
                    native_bytes: failure.native_bytes,
                    native_requests: failure.native_requests,
                    ..SourceQosStats::default()
                },
            };
            return Err(native_failure(failure, &input.destination, stage, stats));
        }
    };
    if let Some(qos) = &input.source_qos {
        qos.record_native(native.native_bytes, native.native_requests);
    }
    finish_native(request, input, stage, native, digest).await
}

fn hash_checkpoint_eligible(request: &TransferRequest, input: &NativeTransferInput) -> bool {
    request.transfer_policy == TransferPolicy::Checkpointed
        && input.destination.supports_hash_checkpoint()
        && input.pair.supports_hash_checkpoint(&input.descriptor)
        && input.descriptor.size.is_some_and(|size| {
            size > input.source.maximum_read_chunk_bytes() as u64
                && input
                    .destination
                    .automatic_checkpoint_interval_bytes()
                    .is_none_or(|interval| size > interval)
        })
}

fn defer_source_hash(request: &TransferRequest, input: &NativeTransferInput) -> bool {
    request.transfer_policy == TransferPolicy::Checkpointed
        && input.destination.supports_hash_checkpoint()
        && input.destination.resumable_at_destination()
}

fn native_failure(
    failure: crate::storage::NativeStageFailure,
    destination: &Arc<dyn StagedDestination>,
    stage: PreparedStage,
    stats: SourceQosStats,
) -> TransferFailure {
    let transfer = TransferFailure::role(
        TransferPhase::Transfer,
        TransferSide::Destination,
        failure.error,
    )
    .with_source_qos(stats);
    transfer.with_stage(Arc::clone(destination), stage)
}

async fn finish_native(
    request: &TransferRequest,
    input: NativeTransferInput,
    stage: PreparedStage,
    native: crate::storage::NativeStageEvidence,
    digest: Option<[u8; 32]>,
) -> Result<Transferred, TransferFailure> {
    if native.write.persisted_bytes != input.plan.source_size {
        return Err(TransferFailure::orchestration(
            TransferPhase::Checkpoint,
            "native write evidence differs from source size",
        )
        .with_stage(input.destination, stage));
    }
    if stage.checkpoint_enabled() {
        stage.checkpoint_barrier(input.plan.source_size);
        if let Err(error) = input.destination.persist_hash_checkpoint(&stage).await {
            return Err(TransferFailure::role(
                TransferPhase::Checkpoint,
                TransferSide::Destination,
                error,
            )
            .with_stage(input.destination, stage));
        }
    }
    let native_stats = SourceQosStats {
        logical_bytes: input.plan.source_size,
        native_bytes: native.native_bytes,
        native_requests: native.native_requests,
        ..SourceQosStats::default()
    };
    let checkpoint = if input.plan.recovery_enabled {
        match input.destination.observe_checkpoint(&stage).await {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                return Err(TransferFailure::role(
                    TransferPhase::Checkpoint,
                    TransferSide::Destination,
                    error,
                )
                .with_stage(input.destination, stage)
                .with_source_qos(native_stats));
            }
        }
    } else {
        crate::storage::CheckpointObservation {
            durable_prefix: native.write.persisted_bytes,
        }
    };
    if checkpoint.durable_prefix != input.plan.source_size {
        return Err(TransferFailure::orchestration(
            TransferPhase::Checkpoint,
            "native durable bytes differ from source size",
        )
        .with_stage(input.destination, stage)
        .with_source_qos(native_stats));
    }
    let effective_recovery = recovery(&stage, input.plan, native.single_operation);
    Ok(Transferred {
        identity: request.identity(),
        destination: input.destination,
        stage,
        write: native.write,
        checkpoint,
        source: input.descriptor,
        data_path: input.plan.data_path,
        source_blake3: digest,
        source_qos: input.source_qos,
        native_bytes: native.native_bytes,
        native_requests: native.native_requests,
        effective_recovery,
        copied_metadata_plan: input.copied_metadata_plan,
    })
}

/// What a native copy reports (ADR-0006 C18): what a streamed copy of the same stage would —
/// `Checkpointed` once the fill turned recovery on, `SkippedBelowCheckpointThreshold` for a
/// `Checkpointed` upload that never reached the interval, `Disabled` for any other policy — except
/// a `Checkpointed` copy that is one server-side operation, which has no recovery state:
/// `NotApplicableNative`. A destination that keeps no progress reports `UnsupportedByDestination`
/// either way.
fn recovery(
    stage: &PreparedStage,
    plan: TransferPlan,
    single_operation: bool,
) -> EffectiveRecovery {
    match final_recovery(stage, plan) {
        EffectiveRecovery::Disabled => EffectiveRecovery::Disabled,
        EffectiveRecovery::UnsupportedByDestination => EffectiveRecovery::UnsupportedByDestination,
        _ if single_operation => EffectiveRecovery::NotApplicableNative,
        streamed => streamed,
    }
}

async fn hash_source(
    source: &dyn ReadSource,
    descriptor: &SourceDescriptor,
    request: &TransferRequest,
    plan: TransferPlan,
    source_qos: Option<SourceQosBudget>,
) -> Result<[u8; 32], TransferFailure> {
    let ranges = SequentialRanges::new(0, plan.source_size, plan.chunk_bytes).map_err(|_| {
        TransferFailure::orchestration(TransferPhase::Preflight, "invalid source range")
    })?;
    let mut hasher = blake3::Hasher::new();
    for range in ranges {
        let bytes = read_exact_range(
            source,
            &descriptor.path,
            &descriptor.source_identity,
            &descriptor.version,
            &request.cancel,
            source_qos.clone(),
            range,
        )
        .await
        .map_err(|error| {
            TransferFailure::role(TransferPhase::Transfer, TransferSide::Source, error)
        })?;
        hasher.update(&bytes);
    }
    Ok(*hasher.finalize().as_bytes())
}
