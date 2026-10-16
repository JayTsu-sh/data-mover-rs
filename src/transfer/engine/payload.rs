//! The payload phase: moves the source bytes into a prepared stage and checks the durable result.

use std::sync::{Arc, Mutex};

use super::failure::failure_side;
use super::producer::{ProducerRequest, ordered_stream, produce};
use super::{TransferFailure, TransferPhase, TransferPlan, TransferSide, positioned, single};
use crate::runtime::inflight::{InflightConfig, InflightRuntime, OrderedChunks};
use crate::storage::{
    CheckpointObservation, PreparedStage, ReadSource, SourceDescriptor, SourceQosBudget,
    StagedDestination, StorageRoleFailure, WriteEvidence,
};
use crate::transfer::{InflightLimits, TransferRequest};

pub(super) async fn transfer_stage(
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

pub(super) fn inflight_channel(
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
