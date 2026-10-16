//! Positioned SMB writes; only acknowledged contiguous bytes can become recoverable.
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{StreamExt as _, future::BoxFuture, stream::FuturesUnordered};

use super::{
    at_destination::write_pointer,
    source::{classify, entry_failure},
    staged::{CifsStageFile, CifsStagedDestination},
    writer,
};
use crate::{
    model::{FailureClass, Operation},
    storage::{PositionedByteStream, PositionedChunk, PreparedStage, StorageRoleFailure},
};

/// Completed/received intervals collapse as soon as their predecessors arrive.
struct Frontier {
    prefix: u64,
    pending: BTreeMap<u64, u64>,
}

impl Frontier {
    fn new(prefix: u64) -> Self {
        Self {
            prefix,
            pending: BTreeMap::new(),
        }
    }

    fn insert(&mut self, start: u64, length: usize) -> Result<(), ()> {
        let end = start.checked_add(length as u64).ok_or(())?;
        if length == 0
            || start < self.prefix
            || self
                .pending
                .range(..=start)
                .next_back()
                .is_some_and(|(_, end)| *end > start)
            || self
                .pending
                .range(start..)
                .next()
                .is_some_and(|(next, _)| *next < end)
        {
            return Err(());
        }
        let mut start = start;
        let mut end = end;
        if let Some((&previous, &previous_end)) = self.pending.range(..start).next_back()
            && previous_end == start
        {
            self.pending.remove(&previous);
            start = previous;
        }
        if let Some(next_end) = self.pending.remove(&end) {
            end = next_end;
        }
        self.pending.insert(start, end);
        while let Some(end) = self.pending.remove(&self.prefix) {
            self.prefix = end;
        }
        Ok(())
    }
}

// Retain offsets around the existing async-trait future without another heap allocation.
struct PendingWrite<'a> {
    offset: u64,
    length: usize,
    future: BoxFuture<'a, smb_domain::Result<()>>,
}

impl Future for PendingWrite<'_> {
    type Output = (u64, usize, smb_domain::Result<()>);

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.future
            .as_mut()
            .poll(context)
            .map(|result| (self.offset, self.length, result))
    }
}

struct Writer<'a> {
    adapter: &'a CifsStagedDestination,
    stage: &'a PreparedStage,
    file: &'a dyn CifsStageFile,
    maximum: usize,
    received: Frontier,
    completed: Frontier,
    writes: FuturesUnordered<PendingWrite<'a>>,
    pending: Option<PositionedChunk>,
    done: bool,
    failure: Option<StorageRoleFailure>,
    interval: Option<u64>,
    checkpoint_at: Option<u64>,
}

pub(super) async fn write(
    adapter: &CifsStagedDestination,
    stage: &PreparedStage,
    file: &dyn CifsStageFile,
    mut input: PositionedByteStream,
) -> Result<u64, StorageRoleFailure> {
    let mut writer = Writer::new(adapter, stage, file)?;
    writer.run(&mut input).await;
    writer.finish().await
}

impl<'a> Writer<'a> {
    fn new(
        adapter: &'a CifsStagedDestination,
        stage: &'a PreparedStage,
        file: &'a dyn CifsStageFile,
    ) -> Result<Self, StorageRoleFailure> {
        let maximum = file.maximum_write_chunk() as usize;
        if maximum == 0 {
            return Err(entry_failure(
                stage.final_destination.path(),
                Operation::Write,
                FailureClass::Protocol,
            ));
        }
        let interval = stage
            .deferred_checkpoint
            .as_ref()
            .map(|value| value.interval_bytes);
        Ok(Self {
            adapter,
            stage,
            file,
            maximum,
            received: Frontier::new(stage.write_offset),
            completed: Frontier::new(stage.write_offset),
            writes: FuturesUnordered::new(),
            pending: None,
            done: false,
            failure: None,
            interval,
            checkpoint_at: interval.map(|step| stage.write_offset.saturating_add(step)),
        })
    }

    fn invalid(&self) -> StorageRoleFailure {
        entry_failure(
            self.stage.final_destination.path(),
            Operation::Write,
            FailureClass::Corruption,
        )
    }

    async fn run(&mut self, input: &mut PositionedByteStream) {
        loop {
            if self.failure.is_some() {
                break;
            }
            if self
                .checkpoint_at
                .is_some_and(|offset| self.completed.prefix >= offset)
            {
                self.save_checkpoint().await;
                if self.failure.is_some() {
                    break;
                }
            }
            if self.issue_pending() {
                continue;
            }
            if self.done && self.pending.is_none() && self.writes.is_empty() {
                break;
            }
            self.poll(input).await;
        }
    }

    fn accept_write(&mut self, offset: u64, length: usize, result: smb_domain::Result<()>) {
        let error = match result {
            Ok(()) => self
                .completed
                .insert(offset, length)
                .err()
                .map(|()| self.invalid()),
            Err(error) => Some(classify(
                self.stage.final_destination.path(),
                Operation::Write,
                &error,
            )),
        };
        if let Some(error) = error {
            self.failure.get_or_insert(error);
        }
    }

    async fn save_checkpoint(&mut self) {
        while let Some((offset, length, result)) = self.writes.next().await {
            self.accept_write(offset, length, result);
        }
        if self.failure.is_some() {
            return;
        }
        if self
            .stage
            .deferred_checkpoint
            .as_ref()
            .is_some_and(|value| {
                self.checkpoint_at
                    .is_some_and(|offset| offset < value.source_size)
            })
            && let Err(error) =
                writer::save(self.adapter, self.stage, self.file, self.completed.prefix).await
        {
            self.failure = Some(error);
            return;
        }
        self.checkpoint_at = self
            .interval
            .map(|step| self.completed.prefix.saturating_add(step));
    }

    fn issue_pending(&mut self) -> bool {
        let Some(chunk) = self.pending.as_mut() else {
            return false;
        };
        if self.writes.len() >= self.adapter.write_inflight {
            return false;
        }
        let offset = chunk.offset;
        let data = chunk.data.split_to(chunk.data.len().min(self.maximum));
        let length = data.len();
        chunk.offset += length as u64;
        self.writes.push(PendingWrite {
            offset,
            length,
            future: self.file.write_all_at(offset, data),
        });
        if chunk.data.is_empty() {
            self.pending = None;
        }
        true
    }

    async fn poll(&mut self, input: &mut PositionedByteStream) {
        tokio::select! {
            result = self.writes.next(), if !self.writes.is_empty() => {
                if let Some((offset, length, result)) = result { self.accept_write(offset, length, result); }
            }
            item = input.next(), if self.pending.is_none() && !self.done && self.writes.len() < self.adapter.write_inflight => {
                match item {
                    Some(Ok(chunk)) => {
                        if self.received.insert(chunk.offset, chunk.data.len()).is_err() { self.failure = Some(self.invalid()); }
                        else { self.pending = Some(chunk); }
                    }
                    Some(Err(error)) => self.failure = Some(error),
                    None => self.done = true,
                }
            }
        }
    }

    async fn finish(mut self) -> Result<u64, StorageRoleFailure> {
        // Preserve the original input failure, but await every issued write before closing.
        while let Some((_, _, result)) = self.writes.next().await {
            if let Err(error) = result {
                self.failure.get_or_insert_with(|| {
                    classify(
                        self.stage.final_destination.path(),
                        Operation::Write,
                        &error,
                    )
                });
            }
        }
        if let Some(error) = self.failure.take() {
            return Err(error);
        }
        if !self.received.pending.is_empty()
            || !self.completed.pending.is_empty()
            || self.received.prefix != self.completed.prefix
        {
            return Err(self.invalid());
        }
        // A recorded prefix must be flushed data, whether or not publication asks for durability.
        if self.stage.durable_publication || self.stage.recovery_enabled() {
            self.file.flush().await.map_err(|error| {
                classify(
                    self.stage.final_destination.path(),
                    Operation::Write,
                    &error,
                )
            })?;
        }
        if self.stage.recovery_enabled() {
            write_pointer(self.adapter, self.stage, self.completed.prefix, false).await?;
        }
        Ok(self.completed.prefix)
    }
}
#[cfg(test)]
mod tests {
    use super::Frontier;

    #[test]
    fn frontier_does_not_publish_holes_and_coalesces_ranges() {
        let mut value = Frontier::new(0);
        assert!(value.insert(8, 4).is_ok());
        assert!(value.insert(4, 4).is_ok());
        assert_eq!(value.prefix, 0);
        assert_eq!(value.pending.len(), 1);
        assert!(value.insert(6, 2).is_err());
        assert!(value.insert(0, 4).is_ok());
        assert_eq!(value.prefix, 12);
        assert!(value.pending.is_empty());
        assert!(value.insert(4, 4).is_err());
    }
}
