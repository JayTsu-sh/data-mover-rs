use std::{collections::VecDeque, ops::Range};

use super::{Context, TransferFailure, channel, checkpoint_error, hash_error, read, state};
use crate::storage::checkpoint::{insert_range, missing_ranges};
use crate::storage::range_hash::RangeHash;

pub(super) async fn produce(
    context: &Context<'_>,
    recovered: state::Recovered,
    sender: Option<channel::Sender>,
) -> Result<[u8; 32], TransferFailure> {
    let result = produce_inner(context, recovered, sender.as_ref()).await;
    if let Err(failure) = &result
        && let Some(sender) = &sender
    {
        let role = failure.role.as_deref().cloned().unwrap_or_else(|| {
            super::super::failure::source_failure(
                &context.descriptor.path,
                crate::model::FailureClass::Corruption,
            )
        });
        // Do not select on cancellation: the writer must see a terminal error
        // and drain its work. A failed writer drops the receiver and unblocks us.
        let _ = sender.send(Err(role)).await;
    }
    result
}

async fn produce_inner(
    context: &Context<'_>,
    recovered: state::Recovered,
    sender: Option<&channel::Sender>,
) -> Result<[u8; 32], TransferFailure> {
    if let Some(sender) = sender
        && !context.destination.requires_hash_intent()
    {
        return stream(context, recovered, sender).await;
    }
    batches(context, recovered, sender).await
}

async fn batches(
    context: &Context<'_>,
    recovered: state::Recovered,
    sender: Option<&channel::Sender>,
) -> Result<[u8; 32], TransferFailure> {
    let mut hash = recovered.hash;
    let schedule = if sender.is_some() {
        missing_ranges(&recovered.durable, 0..context.size)
    } else {
        hash.missing_ranges(0..context.size)
    };
    let mut schedule = VecDeque::from(schedule);
    let mut outstanding: Vec<Range<u64>> = Vec::new();
    state::anchor(context, &hash, 0, sender).await?;
    while let Some(ranges) = next_window(&mut schedule, context.window) {
        let batch = read::collect(context, &ranges, sender).await?;
        for chunk in &batch {
            observe_missing(&mut hash, chunk)?;
        }
        if sender.is_some() {
            let durable = context.stage.checkpoint_ranges();
            outstanding.retain(|range| !missing_ranges(&durable, range.clone()).is_empty());
            for chunk in &batch {
                insert_range(
                    &mut outstanding,
                    chunk.offset..chunk.offset + chunk.data.len() as u64,
                );
            }
        }
        state::anchor(context, &hash, outstanding.len(), sender).await?;
        if let Some(sender) = sender {
            for chunk in batch {
                context
                    .until(Some(sender), async {
                        sender
                            .send(Ok(chunk))
                            .await
                            .map_err(|_| context.cancelled())
                    })
                    .await?;
            }
        }
    }
    if context.cancel.is_cancelled() {
        return Err(context.cancelled());
    }
    hash.finalize().map_err(hash_error)
}

async fn stream(
    context: &Context<'_>,
    recovered: state::Recovered,
    sender: &channel::Sender,
) -> Result<[u8; 32], TransferFailure> {
    let mut hash = recovered.hash;
    let mut schedule = VecDeque::from(missing_ranges(&recovered.durable, 0..context.size));
    while let Some(ranges) = next_window(&mut schedule, context.window) {
        for range in ranges {
            let mut input = read::open(context, range, Some(sender)).await?;
            while let Some(chunk) = input.next(context, Some(sender)).await? {
                observe_missing(&mut hash, &chunk)?;
                // A real backend data barrier persists this H revision with D.
                // No batch collection or extra metadata request delays completion.
                state::publish(context, &hash, 0)?;
                context
                    .until(Some(sender), async {
                        sender
                            .send(Ok(chunk))
                            .await
                            .map_err(|_| context.cancelled())
                    })
                    .await?;
            }
        }
    }
    if context.cancel.is_cancelled() {
        return Err(context.cancelled());
    }
    hash.finalize().map_err(hash_error)
}

fn next_window(schedule: &mut VecDeque<Range<u64>>, maximum: usize) -> Option<Vec<Range<u64>>> {
    let end = schedule.front()?.start.saturating_add(maximum as u64);
    let mut ranges = Vec::new();
    while let Some(range) = schedule.front_mut() {
        if range.start >= end {
            break;
        }
        let take = range.start..range.end.min(end);
        range.start = take.end;
        ranges.push(take);
        if range.start == range.end {
            schedule.pop_front();
        }
    }
    Some(ranges)
}

fn observe_missing(
    hash: &mut RangeHash,
    chunk: &crate::storage::PositionedChunk,
) -> Result<(), TransferFailure> {
    let end = chunk
        .offset
        .checked_add(chunk.data.len() as u64)
        .ok_or_else(|| checkpoint_error("source offset overflows"))?;
    for range in hash.missing_ranges(chunk.offset..end) {
        let start = usize::try_from(range.start - chunk.offset)
            .map_err(|_| checkpoint_error("hash slice overflows"))?;
        let end = usize::try_from(range.end - chunk.offset)
            .map_err(|_| checkpoint_error("hash slice overflows"))?;
        hash.observe(range.start, &chunk.data[start..end])
            .map_err(hash_error)?;
    }
    Ok(())
}
