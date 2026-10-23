use futures::StreamExt as _;
use std::ops::Range;

use super::{Context, TransferFailure, TransferPhase, TransferSide, channel, state};
use crate::model::FailureClass;
use crate::storage::checkpoint::{insert_range, missing_ranges};
use crate::storage::{PositionedByteStream, PositionedChunk, ReadRequest};

pub(super) async fn collect(
    context: &Context<'_>,
    ranges: &[Range<u64>],
    sender: Option<&channel::Sender>,
) -> Result<Vec<PositionedChunk>, TransferFailure> {
    let mut batch = Vec::new();
    let mut total = 0_usize;
    for range in ranges {
        let mut input = open(context, range.clone(), sender).await?;
        while let Some(chunk) = input.next(context, sender).await? {
            total = total
                .checked_add(chunk.data.len())
                .filter(|n| *n <= context.window)
                .ok_or_else(|| context.corruption())?;
            // Limit per-window record overhead even for a pathological source
            // returning one byte per item; payload bytes alone aren't a budget.
            state::capacity(0, batch.len() + 1)?;
            batch.push(chunk);
        }
    }
    Ok(batch)
}

fn validate(
    context: &Context<'_>,
    chunk: &PositionedChunk,
    range: &Range<u64>,
    received: &[Range<u64>],
) -> Result<u64, TransferFailure> {
    let end = chunk
        .offset
        .checked_add(chunk.data.len() as u64)
        .filter(|end| *end <= range.end)
        .ok_or_else(|| context.corruption())?;
    let missing = missing_ranges(received, chunk.offset..end);
    if chunk.data.is_empty()
        || chunk.data.len() > context.maximum
        || chunk.offset < range.start
        || missing.len() != 1
        || missing.first() != Some(&(chunk.offset..end))
    {
        return Err(context.corruption());
    }
    Ok(end)
}

pub(super) struct Reader {
    input: PositionedByteStream,
    range: Range<u64>,
    received: Vec<Range<u64>>,
}

impl Reader {
    pub(super) async fn next(
        &mut self,
        context: &Context<'_>,
        sender: Option<&channel::Sender>,
    ) -> Result<Option<PositionedChunk>, TransferFailure> {
        let chunk = context
            .until(sender, async {
                self.input.next().await.transpose().map_err(|error| {
                    TransferFailure::role(TransferPhase::Transfer, TransferSide::Source, error)
                })
            })
            .await?;
        if let Some(chunk) = &chunk {
            let end = validate(context, chunk, &self.range, &self.received)?;
            insert_range(&mut self.received, chunk.offset..end);
        } else if !missing_ranges(&self.received, self.range.clone()).is_empty() {
            return Err(context.corruption());
        }
        Ok(chunk)
    }
}

pub(super) async fn open(
    context: &Context<'_>,
    range: Range<u64>,
    sender: Option<&channel::Sender>,
) -> Result<Reader, TransferFailure> {
    let input = open_stream(context, range.clone(), sender).await?;
    Ok(Reader {
        input,
        range,
        received: Vec::new(),
    })
}

async fn open_stream(
    context: &Context<'_>,
    range: Range<u64>,
    sender: Option<&channel::Sender>,
) -> Result<PositionedByteStream, TransferFailure> {
    let request = ReadRequest {
        path: context.descriptor.path.clone(),
        range: Some(range.clone()),
        expected_source: Some(context.descriptor.source_identity.clone()),
        maximum_chunk_bytes: context.maximum,
        read_inflight: context.concurrency,
        read_budget: None,
        cancel: context.cancel.clone(),
        source_qos: context.source_qos.clone(),
        version: context.descriptor.version.clone(),
    };
    context
        .until(sender, async {
            let source_error =
                |error| TransferFailure::role(TransferPhase::Transfer, TransferSide::Source, error);
            if context.source.supports_positioned_read() {
                return context
                    .source
                    .read_positioned(request)
                    .await
                    .map_err(source_error);
            }
            let input = context.source.read(request).await.map_err(source_error)?;
            let mut offset = range.start;
            let path = context.descriptor.path.clone();
            let positioned = input.map(move |item| {
                item.and_then(|data| {
                    let start = offset;
                    offset = offset
                        .checked_add(data.len() as u64)
                        .filter(|end| *end <= range.end)
                        .ok_or_else(|| {
                            super::super::failure::source_failure(&path, FailureClass::Corruption)
                        })?;
                    Ok(PositionedChunk {
                        offset: start,
                        data,
                    })
                })
            });
            Ok(positioned.boxed())
        })
        .await
}
