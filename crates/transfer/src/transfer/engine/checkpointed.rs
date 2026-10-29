//! Ordinary checkpointed payloads and the source-only phase of native copies.
//! Source evidence H precedes payload exposure; adoptable writes additionally
//! require a durable anchor. Independently proven D decides payload read skips.

use std::{future::Future, sync::Arc};
use tokio_util::sync::CancellationToken;

use super::payload::TransferEvidence;
use super::{TransferFailure, TransferPhase, TransferPlan, TransferSide};
use crate::model::FailureClass;
use crate::storage::{
    CheckpointObservation, PrepareFact, PreparedStage, ReadSource, SourceDescriptor,
    SourceQosBudget, StagedDestination, WriteEvidence,
};
use crate::transfer::{TransferPolicy, TransferRequest};

mod channel;
mod producer;
mod read;
mod state;
#[cfg(test)]
mod tests;

pub(super) fn eligible(
    request: &TransferRequest,
    destination: &dyn StagedDestination,
    stage: &PreparedStage,
    plan: TransferPlan,
) -> bool {
    request.transfer_policy == TransferPolicy::Checkpointed
        && !stage.direct()
        && destination.stage_supports_hash_checkpoint(stage)
        && (stage.restored_checkpoint().is_some()
            || plan.recovery_enabled
            || plan.automatic_interval.is_some())
        && !legacy(stage)
}

fn legacy(stage: &PreparedStage) -> bool {
    stage.restored_checkpoint().is_none()
        && (stage.write_offset() != 0
            || matches!(stage.prepare_fact(), PrepareFact::Resumed { .. }))
}

pub(super) async fn transfer(
    request: &TransferRequest,
    source: &Arc<dyn ReadSource>,
    destination: &Arc<dyn StagedDestination>,
    descriptor: &SourceDescriptor,
    stage: &PreparedStage,
    plan: TransferPlan,
    source_qos: Option<SourceQosBudget>,
) -> Result<TransferEvidence, TransferFailure> {
    if !eligible(request, destination.as_ref(), stage, plan) {
        return Err(checkpoint_error(
            "stage is not eligible for checkpointed streaming",
        ));
    }
    let context = Context::new(
        &request.cancel,
        source.as_ref(),
        destination.as_ref(),
        descriptor,
        stage,
        plan.source_size,
        source_qos,
    )?;
    let recovered = state::initialize(&context)?;
    if stage.recovered_publication().is_some() {
        return Ok(recovered_evidence(
            context.size,
            recovered.hash.finalize().map_err(hash_error)?,
        ));
    }
    let (sender, receiver) = channel::channel(request.cancel.clone(), descriptor.path.clone());
    let producing = producer::produce(&context, recovered, Some(sender));
    let writing = write(&context, receiver);
    let (produced, written) = tokio::join!(producing, writing);
    let (digest, write) = settle(produced, written)?;
    state::finish(&context, write, digest).await
}

async fn write(
    context: &Context<'_>,
    receiver: crate::storage::PositionedByteStream,
) -> Result<WriteEvidence, TransferFailure> {
    // The stream supplies cancellation/errors. Let the adapter drain already
    // issued writes and settle checkpoint transactions before releasing fences.
    context
        .destination
        .write_checkpointed(context.stage, receiver)
        .await
        .map_err(|error| {
            TransferFailure::role(
                TransferPhase::Transfer,
                super::failure::failure_side(&error),
                error,
            )
        })
}

fn settle(
    produced: Result<[u8; 32], TransferFailure>,
    written: Result<WriteEvidence, TransferFailure>,
) -> Result<([u8; 32], WriteEvidence), TransferFailure> {
    match (produced, written) {
        (Err(hash_failure), _) if hash_failure.phase() == TransferPhase::Checkpoint => {
            Err(hash_failure)
        }
        (_, Err(destination_failure)) => Err(destination_failure),
        (Err(source_failure), _) => Err(source_failure),
        (Ok(digest), Ok(write)) => Ok((digest, write)),
    }
}

/// Complete and persist source H before the caller starts native multipart copies.
/// This never calls a destination payload write and never promotes H into D.
/// Legacy resumed stages without H are refused as Unsupported without arming;
/// the caller must explicitly choose legacy full-source hashing if desired.
pub(super) async fn hash_only(
    request: &TransferRequest,
    source: &Arc<dyn ReadSource>,
    destination: &Arc<dyn StagedDestination>,
    descriptor: &SourceDescriptor,
    stage: &PreparedStage,
    plan: TransferPlan,
    source_qos: Option<SourceQosBudget>,
) -> Result<[u8; 32], TransferFailure> {
    if legacy(stage) {
        return Err(TransferFailure::role(
            TransferPhase::Checkpoint,
            TransferSide::Destination,
            crate::storage::checkpoint_stream::failure(
                stage.final_destination().path(),
                FailureClass::Unsupported,
                "legacy native recovery has no source hash evidence",
            ),
        ));
    }
    if request.transfer_policy != TransferPolicy::Checkpointed
        || stage.direct()
        || !destination.stage_supports_hash_checkpoint(stage)
    {
        return Err(checkpoint_error(
            "stage is not eligible for persistent source hashing",
        ));
    }
    let context = Context::new(
        &request.cancel,
        source.as_ref(),
        destination.as_ref(),
        descriptor,
        stage,
        plan.source_size,
        source_qos,
    )?;
    let recovered = state::initialize(&context)?;
    if stage.recovered_publication().is_some() {
        return recovered.hash.finalize().map_err(hash_error);
    }
    producer::produce(&context, recovered, None).await
}

struct Context<'a> {
    source: &'a dyn ReadSource,
    destination: &'a dyn StagedDestination,
    descriptor: &'a SourceDescriptor,
    stage: &'a PreparedStage,
    cancel: &'a CancellationToken,
    size: u64,
    maximum: usize,
    concurrency: usize,
    window: usize,
    source_qos: Option<SourceQosBudget>,
}

impl<'a> Context<'a> {
    fn new(
        cancel: &'a CancellationToken,
        source: &'a dyn ReadSource,
        destination: &'a dyn StagedDestination,
        descriptor: &'a SourceDescriptor,
        stage: &'a PreparedStage,
        size: u64,
        source_qos: Option<SourceQosBudget>,
    ) -> Result<Self, TransferFailure> {
        let maximum = source.maximum_read_chunk_bytes().max(1);
        let concurrency = source.read_concurrency().max(1);
        let window = maximum
            .checked_mul(concurrency)
            .ok_or_else(|| checkpoint_error("source read window overflows"))?;
        if descriptor.size != Some(size) || stage.write_offset() > size {
            return Err(checkpoint_error("source length and recovered stage differ"));
        }
        Ok(Self {
            source,
            destination,
            descriptor,
            stage,
            cancel,
            size,
            maximum,
            concurrency,
            window,
            source_qos,
        })
    }

    /// Interrupt only source/channel waits, never a destination transaction.
    async fn until<T>(
        &self,
        sender: Option<&channel::Sender>,
        future: impl Future<Output = Result<T, TransferFailure>>,
    ) -> Result<T, TransferFailure> {
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => Err(self.cancelled()),
            () = channel::closed(sender) => Err(self.cancelled()),
            result = future => result,
        }
    }

    fn cancelled(&self) -> TransferFailure {
        self.source_error(FailureClass::Cancelled)
    }

    fn source_error(&self, class: FailureClass) -> TransferFailure {
        TransferFailure::role(
            TransferPhase::Transfer,
            TransferSide::Source,
            super::failure::source_failure(&self.descriptor.path, class),
        )
    }

    fn corruption(&self) -> TransferFailure {
        self.source_error(FailureClass::Corruption)
    }
}

fn checkpoint_error(message: &'static str) -> TransferFailure {
    TransferFailure::orchestration(TransferPhase::Checkpoint, message)
}

fn hash_error(_: crate::storage::range_hash::RangeHashError) -> TransferFailure {
    checkpoint_error("invalid or incomplete source hash evidence")
}

fn recovered_evidence(size: u64, digest: [u8; 32]) -> TransferEvidence {
    TransferEvidence {
        write: WriteEvidence {
            persisted_bytes: size,
        },
        checkpoint: CheckpointObservation {
            durable_prefix: size,
        },
        source_blake3: Some(digest),
    }
}
