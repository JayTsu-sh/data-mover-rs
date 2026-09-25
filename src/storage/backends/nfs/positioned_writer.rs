//! Shared sequential/positioned NFS writer; protocol COMMIT and verifier recovery stay behind the file role.
use std::{collections::BTreeMap, future::pending, sync::Arc};

use bytes::Bytes;
use futures::{
    Stream, StreamExt as _,
    future::{BoxFuture, ready},
    stream::FuturesUnordered,
};

use super::{
    NfsProtocolFailure, NfsStageFile, NfsStagedDestinationAdapter, NfsWriteFuture,
    NfsWriteProgress, deferred_checkpoint_due, failure, next_deferred_checkpoint, role_failure,
};
use crate::model::{FailureClass, Operation, Transience};
use crate::storage::{ByteStream, PositionedChunk, PreparedStage, StorageRoleFailure};

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

    fn insert(&mut self, start: u64, length: u64) -> Result<(), ()> {
        let end = start.checked_add(length).ok_or(())?;
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

impl NfsStagedDestinationAdapter {
    pub(super) async fn consume_input(
        &self,
        stage: &PreparedStage,
        input: &mut ByteStream,
        handle: &Arc<dyn NfsStageFile>,
        concurrency: usize,
        maximum_chunk_bytes: usize,
    ) -> NfsWriteProgress {
        // Retain the historical sequential entry while sharing durability and retry logic.
        let mut offset = stage.write_offset;
        let nonempty = input.filter(|item| ready(!matches!(item, Ok(data) if data.is_empty())));
        let mut positioned = Box::pin(nonempty.map(move |item| {
            item.and_then(|data| {
                let start = offset;
                offset = offset.checked_add(data.len() as u64).ok_or_else(|| {
                    failure(
                        stage.final_destination.path(),
                        FailureClass::InvalidInput,
                        Transience::Permanent,
                    )
                })?;
                Ok(PositionedChunk {
                    offset: start,
                    data,
                })
            })
        }));
        self.consume_positioned_stream(
            stage,
            &mut positioned,
            handle,
            concurrency,
            maximum_chunk_bytes,
        )
        .await
    }

    pub(super) async fn consume_positioned_stream<S>(
        &self,
        stage: &PreparedStage,
        input: &mut S,
        handle: &Arc<dyn NfsStageFile>,
        concurrency: usize,
        maximum_chunk_bytes: usize,
    ) -> NfsWriteProgress
    where
        S: Stream<Item = Result<PositionedChunk, StorageRoleFailure>> + Unpin + Send,
    {
        let mut writer = Writer::new(self, stage, handle, concurrency, maximum_chunk_bytes);
        writer.run(input).await;
        writer.finish().await
    }
}

struct Writer<'a> {
    adapter: &'a NfsStagedDestinationAdapter,
    stage: &'a PreparedStage,
    handle: &'a Arc<dyn NfsStageFile>,
    concurrency: usize,
    maximum_chunk_bytes: usize,
    writes: FuturesUnordered<NfsWriteFuture>,
    issued: u64,
    persisted: u64,
    completed: Frontier,
    issued_frontier: Frontier,
    input_finished: bool,
    pending_input: Option<PositionedChunk>,
    first_failure: Option<StorageRoleFailure>,
    checkpoint_due: bool,
    deferred_writes_used: bool,
    next_checkpoint: Option<u64>,
    checkpoint: Option<BoxFuture<'a, Result<(), StorageRoleFailure>>>,
}

impl<'a> Writer<'a> {
    fn new(
        adapter: &'a NfsStagedDestinationAdapter,
        stage: &'a PreparedStage,
        handle: &'a Arc<dyn NfsStageFile>,
        concurrency: usize,
        maximum_chunk_bytes: usize,
    ) -> Self {
        Self {
            adapter,
            stage,
            handle,
            concurrency,
            maximum_chunk_bytes,
            writes: FuturesUnordered::new(),
            issued: stage.write_offset,
            persisted: stage.write_offset,
            completed: Frontier::new(stage.write_offset),
            issued_frontier: Frontier::new(stage.write_offset),
            input_finished: false,
            pending_input: None,
            first_failure: None,
            checkpoint_due: false,
            deferred_writes_used: false,
            next_checkpoint: next_deferred_checkpoint(stage, stage.write_offset),
            checkpoint: None,
        }
    }

    fn invalid(&self) -> StorageRoleFailure {
        failure(
            self.stage.final_destination.path(),
            FailureClass::Corruption,
            Transience::Unknown,
        )
    }

    async fn run<S>(&mut self, input: &mut S)
    where
        S: Stream<Item = Result<PositionedChunk, StorageRoleFailure>> + Unpin + Send,
    {
        loop {
            if self.checkpoint_due && self.writes.is_empty() && self.first_failure.is_none() {
                self.start_checkpoint().await;
            }
            let accept_input = !self.input_finished
                && self.first_failure.is_none()
                && !self.checkpoint_due
                && self.writes.len() < self.concurrency;
            if !accept_input && self.writes.is_empty() {
                break;
            }
            self.poll(input, accept_input).await;
        }
    }

    async fn start_checkpoint(&mut self) {
        // Settle the previous record before the next periodic data barrier.
        if let Some(checkpoint) = self.checkpoint.take() {
            self.first_failure = checkpoint.await.err();
        }
        if self.first_failure.is_none() {
            self.first_failure = NfsStagedDestinationAdapter::checkpoint_prefix(
                self.stage,
                self.handle,
                self.persisted,
                self.issued_frontier.prefix,
            )
            .await
            .err();
        }
        if self.first_failure.is_none() {
            self.checkpoint = Some(Box::pin(
                self.adapter
                    .persist_deferred_checkpoint_record(self.stage, self.persisted),
            ));
            self.next_checkpoint = next_deferred_checkpoint(self.stage, self.persisted);
        }
        self.checkpoint_due = false;
    }

    async fn poll<S>(&mut self, input: &mut S, accept_input: bool)
    where
        S: Stream<Item = Result<PositionedChunk, StorageRoleFailure>> + Unpin + Send,
    {
        tokio::select! {
            biased;
            result = async { match self.checkpoint.as_mut() { Some(value) => value.await, None => pending().await } }, if self.checkpoint.is_some() => {
                self.checkpoint = None;
                if self.first_failure.is_none() { self.first_failure = result.err(); }
            }
            Some((offset, expected, result)) = self.writes.next(), if !self.writes.is_empty() => {
                self.accept_write(offset, expected, result);
            }
            item = async {
                if let Some(chunk) = self.pending_input.take() { Some(Ok(chunk)) }
                else { input.next().await }
            }, if accept_input => { self.accept_input(item); }
        }
    }

    fn accept_write(
        &mut self,
        offset: u64,
        expected: u64,
        result: Result<u64, NfsProtocolFailure>,
    ) {
        let error = match result {
            Ok(written) if written == expected => {
                if self.completed.insert(offset, written).is_err() {
                    Some(self.invalid())
                } else {
                    self.persisted = self.completed.prefix;
                    None
                }
            }
            Ok(_) => Some(self.invalid()),
            Err(error) => Some(role_failure(
                self.stage.final_destination.path(),
                Operation::Write,
                error,
            )),
        };
        if self.first_failure.is_none() {
            self.first_failure = error;
        }
    }

    fn accept_input(&mut self, item: Option<Result<PositionedChunk, StorageRoleFailure>>) {
        let mut chunk = match item {
            Some(Ok(chunk)) => chunk,
            Some(Err(error)) => {
                self.first_failure = Some(error);
                return;
            }
            None => {
                self.input_finished = true;
                return;
            }
        };
        if chunk.data.is_empty() {
            self.first_failure = Some(self.invalid());
            return;
        }
        let offset = chunk.offset;
        let piece = chunk
            .data
            .split_to(chunk.data.len().min(self.maximum_chunk_bytes));
        let expected = piece.len() as u64;
        if self.issued_frontier.insert(offset, expected).is_err() {
            self.first_failure = Some(self.invalid());
            return;
        }
        chunk.offset = offset + expected;
        self.issued = self.issued.max(chunk.offset);
        if !chunk.data.is_empty() {
            self.pending_input = Some(chunk);
        }
        self.submit_piece(offset, expected, piece);
        self.checkpoint_due = deferred_checkpoint_due(
            self.stage,
            self.next_checkpoint,
            self.issued_frontier.prefix,
        );
    }

    fn submit_piece(&mut self, offset: u64, expected: u64, piece: Bytes) {
        let recovery_enabled = self.stage.recovery_enabled();
        let checkpoint_pending =
            self.stage.deferred_checkpoint.is_some() || self.stage.durable_publication;
        self.deferred_writes_used |= recovery_enabled || checkpoint_pending;
        let write_handle = Arc::clone(self.handle);
        let periodic_checkpoint = self.stage.deferred_checkpoint.is_some();
        self.writes.push(Box::pin(async move {
            // A periodic checkpoint owns COMMIT cadence even once recovery is on.
            let result = if recovery_enabled && !periodic_checkpoint {
                write_handle.write_deferred_at(offset, piece).await
            } else if checkpoint_pending {
                write_handle.write_until_checkpoint_at(offset, piece).await
            } else {
                write_handle.write_uncommitted_at(offset, piece).await
            };
            (offset, expected, result)
        }));
    }

    async fn finish(mut self) -> NfsWriteProgress {
        // Settle the record before final close, failure truncation, or publication.
        if let Some(checkpoint) = self.checkpoint.take() {
            let result = checkpoint.await;
            if self.first_failure.is_none() {
                self.first_failure = result.err();
            }
        }
        if self.first_failure.is_none()
            && (!self.issued_frontier.pending.is_empty() || !self.completed.pending.is_empty())
        {
            self.first_failure = Some(self.invalid());
        }
        NfsWriteProgress::finished(
            self.issued,
            self.persisted,
            self.first_failure,
            self.deferred_writes_used,
        )
    }
}
