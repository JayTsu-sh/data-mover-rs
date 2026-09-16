//! Completion-ordered local writes with durable contiguous recovery progress.
#[cfg(test)]
use std::sync::atomic::Ordering;
use std::{collections::BTreeMap, fs::File, io, sync::Arc};

use futures::{
    FutureExt as _, StreamExt as _,
    future::{BoxFuture, pending},
};
use tokio::task::{JoinError, JoinSet};

use super::{
    LOCAL_DURABLE_CHECKPOINT_INTERVAL_BYTES, LOCAL_MAX_WRITE_CHUNK_BYTES, LocalStagedDestination,
    failure, io_failure,
};
use crate::model::{FailureClass, Operation, StoragePath};
use crate::storage::{
    PositionedByteStream, PositionedChunk, PreparedStage, StorageRoleFailure, WriteEvidence,
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

type Writes = JoinSet<(u64, usize, io::Result<u64>)>;

fn accept(
    result: Result<(u64, usize, io::Result<u64>), JoinError>,
    completed: &mut Frontier,
    path: &StoragePath,
) -> Result<(), StorageRoleFailure> {
    let (offset, length, result) =
        result.map_err(|_| failure(path, Operation::Write, FailureClass::Internal))?;
    let count = result.map_err(|error| io_failure(path, Operation::Write, &error))?;
    if count != length as u64 || completed.insert(offset, length).is_err() {
        return Err(failure(path, Operation::Write, FailureClass::Corruption));
    }
    Ok(())
}

async fn drain(
    writes: &mut Writes,
    completed: &mut Frontier,
    path: &StoragePath,
    error: &mut Option<StorageRoleFailure>,
) {
    while let Some(result) = writes.join_next().await {
        if let Err(problem) = accept(result, completed, path) {
            error.get_or_insert(problem);
        }
    }
}

struct Writer<'a> {
    adapter: &'a LocalStagedDestination,
    stage: &'a PreparedStage,
    file: Arc<File>,
    fresh: bool,
    received: Frontier,
    completed: Frontier,
    writes: Writes,
    pending: Option<PositionedChunk>,
    done: bool,
    error: Option<StorageRoleFailure>,
    checkpoint_failed: bool,
    checkpoint: Option<BoxFuture<'a, Result<(), StorageRoleFailure>>>,
    interval: Option<u64>,
    checkpoint_at: Option<u64>,
    checkpoint_boundary: u64,
}

pub(super) async fn write(
    adapter: &LocalStagedDestination,
    stage: &PreparedStage,
    mut input: PositionedByteStream,
) -> Result<WriteEvidence, StorageRoleFailure> {
    let file = adapter.open_stage_file_for(stage, Operation::Write).await?;
    let fresh = LocalStagedDestination::take_fresh_write(stage);
    if stage.direct {
        LocalStagedDestination::sync_written_file(
            Arc::clone(&file),
            0,
            stage.final_destination.path(),
            true,
            false,
        )
        .await?;
    }
    let mut writer = Writer::new(adapter, stage, file, fresh);
    writer.run(&mut input).await;
    writer.finish().await
}

impl<'a> Writer<'a> {
    fn new(
        adapter: &'a LocalStagedDestination,
        stage: &'a PreparedStage,
        file: Arc<File>,
        fresh: bool,
    ) -> Self {
        let interval = stage
            .deferred_checkpoint
            .as_ref()
            .map(|value| value.interval_bytes)
            .or_else(|| {
                stage
                    .recovery_enabled()
                    .then_some(LOCAL_DURABLE_CHECKPOINT_INTERVAL_BYTES)
            });
        Self {
            adapter,
            stage,
            file,
            fresh,
            received: Frontier::new(stage.write_offset),
            completed: Frontier::new(stage.write_offset),
            writes: Writes::new(),
            pending: None,
            done: false,
            error: None,
            checkpoint_failed: false,
            checkpoint: None,
            interval,
            checkpoint_at: interval.and_then(|step| stage.write_offset.checked_add(step)),
            checkpoint_boundary: stage.write_offset,
        }
    }

    fn invalid(&self) -> StorageRoleFailure {
        failure(
            self.stage.final_destination.path(),
            Operation::Write,
            FailureClass::Corruption,
        )
    }

    async fn run(&mut self, input: &mut PositionedByteStream) {
        loop {
            if self.error.is_some() {
                break;
            }
            if self.checkpoint_at.is_some_and(|offset| {
                self.completed.prefix >= offset && self.checkpoint_boundary >= offset
            }) {
                self.start_checkpoint().await;
                if self.error.is_some() {
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

    async fn start_checkpoint(&mut self) {
        if let Some(checkpoint) = self.checkpoint.take()
            && let Err(error) = checkpoint.await
        {
            self.error = Some(error);
            self.checkpoint_failed = true;
            return;
        }
        drain(
            &mut self.writes,
            &mut self.completed,
            self.stage.final_destination.path(),
            &mut self.error,
        )
        .await;
        if self.error.is_some() {
            return;
        }
        if self.stage.deferred_checkpoint.as_ref().is_none_or(|value| {
            self.checkpoint_at
                .is_some_and(|offset| offset < value.source_size)
        }) {
            self.checkpoint = Some(
                self.adapter
                    .persist_synced_progress(
                        self.stage,
                        Arc::clone(&self.file),
                        self.completed.prefix,
                    )
                    .boxed(),
            );
        }
        self.checkpoint_at = self
            .interval
            .and_then(|step| self.completed.prefix.checked_add(step));
    }

    fn issue_pending(&mut self) -> bool {
        let Some(chunk) = self.pending.as_mut() else {
            return false;
        };
        if self.writes.len() >= self.adapter.write_concurrency {
            return false;
        }
        let offset = chunk.offset;
        let data = chunk
            .data
            .split_to(chunk.data.len().min(LOCAL_MAX_WRITE_CHUNK_BYTES));
        let length = data.len();
        chunk.offset += length as u64;
        // Keep earlier eligible boundaries even when draining ACKs jumps directly to EOF.
        if self
            .stage
            .deferred_checkpoint
            .as_ref()
            .is_none_or(|value| chunk.offset < value.source_size)
        {
            self.checkpoint_boundary = self.checkpoint_boundary.max(chunk.offset);
        }
        let target = Arc::clone(&self.file);
        let probe = Arc::clone(&self.adapter.write_probe);
        self.writes.spawn_blocking(move || {
            (
                offset,
                length,
                LocalStagedDestination::write_piece(&target, &probe, offset, &data),
            )
        });
        if chunk.data.is_empty() {
            self.pending = None;
        }
        true
    }

    async fn poll(&mut self, input: &mut PositionedByteStream) {
        tokio::select! {
            biased;
            result = async { match self.checkpoint.as_mut() { Some(value) => value.await, None => pending().await } }, if self.checkpoint.is_some() => {
                self.checkpoint = None;
                if let Err(error) = result { self.error = Some(error); self.checkpoint_failed = true; }
            }
            result = self.writes.join_next(), if !self.writes.is_empty() => {
                if let Some(result) = result && let Err(error) = accept(result, &mut self.completed, self.stage.final_destination.path()) { self.error = Some(error); }
            }
            item = input.next(), if self.pending.is_none() && !self.done && self.writes.len() < self.adapter.write_concurrency => {
                self.accept_input(item);
            }
        }
    }

    fn accept_input(&mut self, item: Option<Result<PositionedChunk, StorageRoleFailure>>) {
        match item {
            Some(Ok(chunk)) => {
                if self
                    .received
                    .insert(chunk.offset, chunk.data.len())
                    .is_err()
                {
                    self.error = Some(self.invalid());
                } else {
                    self.pending = Some(chunk);
                }
            }
            Some(Err(error)) => self.error = Some(error),
            None => self.done = true,
        }
    }

    async fn finish(mut self) -> Result<WriteEvidence, StorageRoleFailure> {
        drain(
            &mut self.writes,
            &mut self.completed,
            self.stage.final_destination.path(),
            &mut self.error,
        )
        .await;
        // Persistence must settle before failure truncation, final sync or cleanup.
        if let Some(checkpoint) = self.checkpoint.take()
            && let Err(error) = checkpoint.await
        {
            self.error.get_or_insert(error);
            self.checkpoint_failed = true;
        }
        if self.error.is_none()
            && (!self.received.pending.is_empty()
                || !self.completed.pending.is_empty()
                || self.received.prefix != self.completed.prefix)
        {
            self.error = Some(self.invalid());
        }
        if let Some(error) = self.error.take() {
            self.preserve_failed_prefix().await?;
            return Err(error);
        }
        self.sync_finished_file().await?;
        if self.stage.recovery_enabled() {
            self.adapter
                .persist_checkpoint(self.stage, self.completed.prefix)
                .await?;
        }
        Ok(WriteEvidence {
            persisted_bytes: self.completed.prefix,
        })
    }

    async fn preserve_failed_prefix(&mut self) -> Result<(), StorageRoleFailure> {
        if !self.checkpoint_failed {
            LocalStagedDestination::sync_written_file(
                Arc::clone(&self.file),
                self.completed.prefix,
                self.stage.final_destination.path(),
                true,
                self.stage.durable_publication,
            )
            .await?;
            if self.stage.recovery_enabled() {
                self.adapter
                    .persist_checkpoint(self.stage, self.completed.prefix)
                    .await?;
            }
        }
        Ok(())
    }

    async fn sync_finished_file(&mut self) -> Result<(), StorageRoleFailure> {
        if self.stage.durable_publication || !self.fresh {
            #[cfg(test)]
            if self.stage.durable_publication {
                self.adapter
                    .write_probe
                    .final_data_sync_calls
                    .fetch_add(1, Ordering::SeqCst);
            }
            LocalStagedDestination::sync_written_file(
                Arc::clone(&self.file),
                self.completed.prefix,
                self.stage.final_destination.path(),
                !self.fresh,
                self.stage.durable_publication,
            )
            .await?;
        }
        Ok(())
    }
}
