//! Positioned payload aggregation in the upload's frozen part geometry. Only armed ordinary
//! Checkpointed transfers enter here; legacy ordered writes keep their existing protocol.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::FuturesUnordered;

use super::super::S3Body;
use super::super::source::{classified_entry, role_failure};
use super::final_upload::FinalUpload;
use super::hash_checkpoint::part_range;
use super::parts::{UploadedParts, upload};
use super::{S3Protocol, S3StagedDestination};
use crate::model::{FailureClass, Operation, Transience};
use crate::storage::checkpoint::{insert_range, missing_ranges};
use crate::storage::{
    PositionedByteStream, PositionedChunk, PreparedStage, StorageRoleFailure, WriteEvidence,
};

fn invalid(stage: &PreparedStage, diagnostic: &str) -> StorageRoleFailure {
    classified_entry(
        stage.final_destination.path(),
        Operation::Write,
        FailureClass::InvalidInput,
        Transience::Permanent,
        diagnostic,
    )
}

#[derive(Default)]
struct PartialPart {
    fragments: BTreeMap<u64, Bytes>,
    bytes: u64,
}

impl PartialPart {
    fn insert(&mut self, offset: u64, bytes: Bytes) {
        self.bytes += bytes.len() as u64;
        self.fragments.insert(offset, bytes);
    }

    fn body(self) -> S3Body {
        S3Body::new(self.fragments.into_values().collect())
    }
}

struct Assembly<'a> {
    stage: &'a PreparedStage,
    upload: &'a FinalUpload,
    size: u64,
    partial: BTreeMap<i32, PartialPart>,
    /// Recovered, dispatched and buffered payload coverage; rejects duplicates before mutation.
    received: Vec<Range<u64>>,
    pending: BTreeSet<i32>,
    sent: UploadedParts,
    buffered: u64,
    maximum_buffered: u64,
    resumed: usize,
}

impl<'a> Assembly<'a> {
    fn new(
        stage: &'a PreparedStage,
        upload: &'a FinalUpload,
        inflight: usize,
    ) -> Result<Self, StorageRoleFailure> {
        let size = upload
            .expected_size
            .ok_or_else(|| invalid(stage, "S3 hash recovery requires a fixed source size"))?;
        let current = upload.lock();
        if current.written.is_some() || current.published.is_some() {
            return Err(invalid(stage, "S3 upload is already written"));
        }
        Ok(Self {
            stage,
            upload,
            size,
            partial: BTreeMap::new(),
            pending: BTreeSet::new(),
            received: stage.checkpoint_ranges(),
            resumed: current.parts.len(),
            sent: UploadedParts {
                parts: current.parts.clone(),
                bytes: 0,
                md5_etags: true,
            },
            buffered: 0,
            maximum_buffered: (upload.part_size as u64).saturating_mul(inflight.max(1) as u64 + 1),
        })
    }

    fn receive(
        &mut self,
        chunk: PositionedChunk,
    ) -> Result<Vec<(i32, S3Body)>, StorageRoleFailure> {
        let end = chunk
            .offset
            .checked_add(chunk.data.len() as u64)
            .filter(|end| *end <= self.size)
            .ok_or_else(|| {
                invalid(
                    self.stage,
                    "positioned S3 payload exceeds the source length",
                )
            })?;
        if end == chunk.offset {
            return Ok(Vec::new());
        }
        let missing = missing_ranges(&self.received, chunk.offset..end);
        if missing.len() != 1 || missing[0] != (chunk.offset..end) {
            return Err(invalid(
                self.stage,
                "positioned S3 payload overlaps received or recovered bytes",
            ));
        }
        if self.buffered.saturating_add(chunk.data.len() as u64) > self.maximum_buffered {
            return Err(classified_entry(
                self.stage.final_destination.path(),
                Operation::Write,
                FailureClass::Capacity,
                Transience::Permanent,
                "positioned S3 payload exceeds its bounded part window",
            ));
        }
        insert_range(&mut self.received, chunk.offset..end);
        self.sent.bytes += chunk.data.len() as u64;
        self.split(chunk)
    }

    fn split(
        &mut self,
        mut chunk: PositionedChunk,
    ) -> Result<Vec<(i32, S3Body)>, StorageRoleFailure> {
        let mut ready = Vec::new();
        while !chunk.data.is_empty() {
            let number = i32::try_from(chunk.offset / self.upload.part_size as u64 + 1)
                .ok()
                .filter(|number| *number <= 10_000)
                .ok_or_else(|| invalid(self.stage, "S3 multipart part limit exceeded"))?;
            let range = part_range(self.upload, number, self.size);
            let length = usize::try_from(range.end - chunk.offset)
                .unwrap_or(usize::MAX)
                .min(chunk.data.len());
            let partial = self.partial.entry(number).or_default();
            partial.insert(chunk.offset, chunk.data.split_to(length));
            chunk.offset += length as u64;
            self.buffered += length as u64;
            if partial.bytes == range.end - range.start {
                self.ensure_anchored(range)?;
                if let Some(part) = self.partial.remove(&number) {
                    self.buffered -= part.bytes;
                    self.pending.insert(number);
                    ready.push((number, part.body()));
                }
            }
        }
        Ok(ready)
    }

    fn ensure_anchored(&self, range: Range<u64>) -> Result<(), StorageRoleFailure> {
        if !self
            .upload
            .lock()
            .anchored_hash
            .as_ref()
            .is_some_and(|hash| hash.covers(range))
        {
            return Err(invalid(
                self.stage,
                "S3 part dispatch requires persisted source hash intent",
            ));
        }
        Ok(())
    }

    async fn record(
        &mut self,
        result: Result<(i32, String, bool), super::super::S3ProtocolFailure>,
    ) -> Result<(), StorageRoleFailure> {
        let (number, etag, md5) = result.map_err(|error| {
            role_failure(self.stage.final_destination.path(), Operation::Write, error)
        })?;
        let _guard = self.stage.checkpoint_guard().await;
        self.pending.remove(&number);
        let range = part_range(self.upload, number, self.size);
        self.stage
            .confirmed_write(range.start, range.end - range.start);
        self.sent.md5_etags &= md5;
        self.sent.parts.push((number, etag.clone()));
        self.upload.lock().parts.push((number, etag));
        // UploadPart independently proves durability. The next intent batch snapshots these
        // receipts with H; no per-part metadata PUT is needed.
        self.stage.checkpoint_barrier(0);
        self.stage
            .recovery_enabled
            .store(true, std::sync::atomic::Ordering::Release);
        self.stage
            .checkpoint_recorded(self.stage.checkpoint_prefix());
        Ok(())
    }

    async fn finish(mut self) -> Result<WriteEvidence, StorageRoleFailure> {
        if !self.partial.is_empty() || !missing_ranges(&self.received, 0..self.size).is_empty() {
            return Err(invalid(
                self.stage,
                "positioned S3 input leaves incomplete multipart ranges",
            ));
        }
        self.sent.parts.sort_by_key(|part| part.0);
        let _guard = self.stage.checkpoint_guard().await;
        self.upload.record_written(
            self.stage.final_destination.path(),
            self.sent,
            self.resumed,
            self.size,
        )?;
        // Every issued request has drained. UploadPart's successful receipt is S3's barrier.
        self.stage.checkpoint_barrier(self.size);
        self.stage
            .recovery_enabled
            .store(true, std::sync::atomic::Ordering::Release);
        self.stage.checkpoint_recorded(self.size);
        Ok(WriteEvidence {
            persisted_bytes: self.size,
        })
    }
}

impl<P: S3Protocol + 'static> S3StagedDestination<P> {
    pub(super) async fn write_checkpointed_parts(
        &self,
        stage: &PreparedStage,
        target: &FinalUpload,
        mut input: PositionedByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        let limit = target.streamed_inflight(self.protocol.write_concurrency());
        let mut assembly = Assembly::new(stage, target, limit)?;
        let mut inflight = FuturesUnordered::new();
        loop {
            let next = tokio::select! {
                biased;
                completed = inflight.next(), if !inflight.is_empty() => {
                    if let Some(completed) = completed { assembly.record(completed).await?; }
                    continue;
                }
                next = input.next() => next,
            };
            let Some(next) = next else {
                break;
            };
            let chunk = match next {
                Ok(chunk) => chunk,
                Err(error) => {
                    // As with the ordered path, already issued parts are allowed to settle.
                    while let Some(completed) = inflight.next().await {
                        assembly.record(completed).await?;
                    }
                    return Err(error);
                }
            };
            for (number, body) in assembly.receive(chunk)? {
                inflight.push(upload(
                    self.protocol.clone(),
                    stage.final_destination.path().as_str().to_string(),
                    target.upload_id.clone(),
                    number,
                    body,
                ));
                if inflight.len() >= limit.max(1)
                    && let Some(completed) = inflight.next().await
                {
                    assembly.record(completed).await?;
                }
            }
        }
        while let Some(completed) = inflight.next().await {
            assembly.record(completed).await?;
        }
        assembly.finish().await
    }
}
