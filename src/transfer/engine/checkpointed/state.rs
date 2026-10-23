use super::{
    Context, TransferEvidence, TransferFailure, TransferPhase, TransferSide, WriteEvidence,
    channel, checkpoint_error, hash_error, legacy,
};
use crate::storage::checkpoint::{CheckpointData, MAX_CHECKPOINT_BYTES, insert_range};
use crate::storage::range_hash::RangeHash;
use bytes::Bytes;
use std::ops::Range;

// Leave room for independently confirmed ranges within the admission budget,
// including islands that have not yet joined a contiguous durable prefix.
const MAX_ADMITTED_RANGE_RECORDS: usize = 10_000;

pub(super) struct Recovered {
    pub(super) hash: RangeHash,
    pub(super) durable: Vec<Range<u64>>,
}

pub(super) fn initialize(context: &Context<'_>) -> Result<Recovered, TransferFailure> {
    let stage = context.stage;
    let (hash, mut durable) = if let Some(data) = stage.restored_checkpoint() {
        if data.size != context.size {
            return Err(checkpoint_error(
                "checkpoint source length differs from planned size",
            ));
        }
        let hash = RangeHash::decode(&data.hash, context.size).map_err(hash_error)?;
        (hash, data.ranges)
    } else {
        if legacy(stage) || stage.recovered_publication.is_some() {
            return Err(checkpoint_error(
                "legacy recovery has no source hash evidence",
            ));
        }
        (RangeHash::new(context.size), Vec::new())
    };
    insert_range(&mut durable, 0..stage.write_offset);
    if durable.iter().any(|range| !hash.covers(range.clone())) {
        return Err(checkpoint_error(
            "adopted destination bytes lack source hash evidence",
        ));
    }
    let encoded = Bytes::from(hash.encode().map_err(hash_error)?);
    capacity(
        encoded.len(),
        durable.len().saturating_add(MAX_ADMITTED_RANGE_RECORDS),
    )?;
    stage.set_checkpoint_window(context.window);
    stage.enable_checkpoint(context.size, encoded);
    stage.checkpoint_barrier(stage.write_offset);
    Ok(Recovered { hash, durable })
}

/// Reserve space for every range that outstanding/new chunks could create in D,
/// including islands confirmed before their predecessors. This is deliberately
/// conservative until a real backend barrier reports those chunks as durable.
pub(super) fn capacity(hash_len: usize, ranges: usize) -> Result<(), TransferFailure> {
    // Use the actual outer envelope rather than treating RangeHash's own larger
    // codec limit as permission to emit an oversized destination checkpoint.
    let envelope = CheckpointData::default().encode().len();
    let len = ranges
        .checked_mul(16)
        .and_then(|n| n.checked_add(hash_len))
        .and_then(|n| n.checked_add(envelope));
    if len.is_none_or(|n| n > MAX_CHECKPOINT_BYTES) {
        return Err(checkpoint_error(
            "source hash and destination ranges exceed checkpoint limit",
        ));
    }
    Ok(())
}

pub(super) async fn anchor(
    context: &Context<'_>,
    hash: &RangeHash,
    reserved_ranges: usize,
    sender: Option<&channel::Sender>,
) -> Result<(), TransferFailure> {
    publish(context, hash, reserved_ranges)?;
    persist(context, sender).await
}

pub(super) fn publish(
    context: &Context<'_>,
    hash: &RangeHash,
    reserved_ranges: usize,
) -> Result<(), TransferFailure> {
    let encoded = Bytes::from(hash.encode().map_err(hash_error)?);
    let ranges = context
        .stage
        .checkpoint_ranges()
        .len()
        .checked_add(reserved_ranges.max(MAX_ADMITTED_RANGE_RECORDS))
        .ok_or_else(|| checkpoint_error("checkpoint range count overflows"))?;
    capacity(encoded.len(), ranges)?;
    context.stage.set_source_hash(encoded);
    Ok(())
}

async fn persist(
    context: &Context<'_>,
    sender: Option<&channel::Sender>,
) -> Result<(), TransferFailure> {
    // The producer cannot mutate H while the adapter anchors this revision.
    // All adapters serialize their pointer updates through checkpoint_guard.
    if context.cancel.is_cancelled() || sender.is_some_and(channel::Sender::is_closed) {
        return Err(context.cancelled());
    }
    // Once begun, settle the entire pointer transaction, including blocking
    // file work. Dropping this future could release its fence before that work.
    context
        .destination
        .persist_hash_checkpoint(context.stage)
        .await
        .map_err(|error| {
            TransferFailure::role(TransferPhase::Checkpoint, TransferSide::Destination, error)
        })
}

pub(super) async fn finish(
    context: &Context<'_>,
    write: WriteEvidence,
    digest: [u8; 32],
) -> Result<TransferEvidence, TransferFailure> {
    if write.persisted_bytes != context.size {
        return Err(checkpoint_error(
            "destination byte count differs from source size",
        ));
    }
    if context.cancel.is_cancelled() {
        return Err(context.cancelled());
    }
    // Successful checkpointed writes have settled their final data barrier.
    // Commit full D with H before observation: deferred recovery may still be
    // disabled when EOF arrives before any intermediate checkpoint was needed.
    context.stage.checkpoint_barrier(context.size);
    let hash = context
        .stage
        .checkpoint_snapshot()
        .ok_or_else(|| checkpoint_error("armed checkpoint disappeared"))?;
    if hash.len() > MAX_CHECKPOINT_BYTES {
        return Err(checkpoint_error("final checkpoint exceeds its size limit"));
    }
    persist(context, None).await?;
    let checkpoint = context
        .destination
        .observe_checkpoint(context.stage)
        .await
        .map_err(|error| {
            TransferFailure::role(TransferPhase::Checkpoint, TransferSide::Destination, error)
        })?;
    if checkpoint.durable_prefix != context.size {
        return Err(checkpoint_error(
            "destination final barrier did not cover the source",
        ));
    }
    Ok(TransferEvidence {
        write,
        checkpoint,
        source_blake3: Some(digest),
    })
}
