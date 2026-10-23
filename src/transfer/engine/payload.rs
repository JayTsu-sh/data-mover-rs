//! The payload phase: moves the source bytes into a prepared stage and checks the durable result.

use std::sync::Arc;

use super::failure::failure_side;
use super::producer::{ProducerRequest, payload_channel, produce};
use super::{TransferFailure, TransferPhase, TransferPlan, TransferSide, single};
use crate::storage::{
    CheckpointObservation, PreparedStage, ReadSource, SourceDescriptor, SourceQosBudget,
    StagedDestination, StorageRoleFailure, WriteEvidence,
};
use crate::transfer::TransferRequest;

pub(super) async fn transfer_stage(
    request: &TransferRequest,
    source: Arc<dyn ReadSource>,
    destination: &Arc<dyn StagedDestination>,
    descriptor: &SourceDescriptor,
    stage: &PreparedStage,
    plan: TransferPlan,
    source_qos: Option<SourceQosBudget>,
) -> Result<TransferEvidence, TransferFailure> {
    if super::checkpointed::eligible(request, destination.as_ref(), stage, plan) {
        return super::checkpointed::transfer(
            request,
            &source,
            destination,
            descriptor,
            stage,
            plan,
            source_qos,
        )
        .await;
    }
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
    let (sender, receiver) = payload_channel(request.cancel.clone(), descriptor.path.clone());
    let producer = tokio::spawn(produce(ProducerRequest {
        source,
        path: descriptor.path.clone(),
        source_identity: descriptor.source_identity.clone(),
        version: descriptor.version.clone(),
        cancel: request.cancel.clone(),
        sender,
        size: plan.source_size,
        write_start,
        source_qos,
        hash_content: request.needs_source_digest() || write_start == 0,
    }));
    let (write, source_blake3) = settle_transfer(
        destination.write(stage, receiver.into_stream()).await,
        producer,
    )
    .await?;
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

pub(super) struct TransferEvidence {
    pub(super) write: WriteEvidence,
    pub(super) checkpoint: CheckpointObservation,
    pub(super) source_blake3: Option<[u8; 32]>,
}

pub(super) async fn settle_transfer(
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
