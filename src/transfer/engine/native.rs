use super::{
    Arc, NativePair, ReadSource, RecoveryContext, SequentialRanges, SourceDescriptor,
    SourceQosBudget, SourceQosStats, StagedDestination, TransferFailure, TransferPhase,
    TransferPlan, TransferRequest, TransferSide, Transferred, read_exact_range,
    register_prepared_stage, select_stage,
};

pub(super) fn eligible_native_pair(request: &TransferRequest) -> Option<NativePair> {
    if request.payload_shaping == super::super::PayloadShapingPolicy::RequireClientShaped {
        None
    } else {
        request.source.native_pair(&request.destination)
    }
}

pub(super) struct NativeTransferInput {
    pub source: Arc<dyn ReadSource>,
    pub destination: Arc<dyn StagedDestination>,
    pub descriptor: SourceDescriptor,
    pub pair: NativePair,
    pub recovery_binding: [u8; 32],
    pub source_qos: Option<SourceQosBudget>,
    pub plan: TransferPlan,
    pub recovery: Option<RecoveryContext>,
}

pub(super) async fn transfer_native(
    request: &TransferRequest,
    input: NativeTransferInput,
) -> Result<Transferred, TransferFailure> {
    let binding = input
        .pair
        .bind_source(&input.descriptor)
        .await
        .map_err(|error| {
            TransferFailure::role(TransferPhase::Describe, TransferSide::Source, error)
        })?;
    let digest = hash_source(
        &*input.source,
        &input.descriptor,
        request,
        input.plan,
        input.source_qos.clone(),
    )
    .await?;
    let stage = select_stage(
        request,
        &input.destination,
        &input.descriptor,
        input.recovery_binding,
        input.plan.recovery_enabled,
        input.recovery.as_ref(),
    )
    .await?;
    if let Err(error) =
        register_prepared_stage(request, &input.destination, &stage, input.recovery.as_ref()).await
    {
        return Err(error.with_stage(input.destination, stage));
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

fn native_failure(
    failure: crate::storage::NativeStageFailure,
    destination: &Arc<dyn StagedDestination>,
    stage: crate::storage::PreparedStage,
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
    stage: crate::storage::PreparedStage,
    native: crate::storage::NativeStageEvidence,
    digest: [u8; 32],
) -> Result<Transferred, TransferFailure> {
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
    Ok(Transferred {
        identity: request.identity.clone(),
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
    })
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
