//! Streaming a byte stream into the parts of one multipart upload, a few parts in flight: shared
//! by the `Direct` upload on the final key and the checkpointed upload on the final key (ADR-0006
//! C15b), which also asks to be told when its first checkpoint is reached.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::task;

use super::super::source::{entry, role_failure};
use super::super::{S3Body, S3Protocol, S3ProtocolFailure};
use super::{S3StagedDestination, single};
use crate::model::{FailureClass, Operation, StoragePath, Transience};
use crate::storage::{ByteStream, PreparedStage, StorageRoleFailure};

/// The upload the parts go to.
pub(super) struct PartTarget<'a> {
    pub(super) stage: Option<&'a PreparedStage>,
    pub(super) path: &'a StoragePath,
    pub(super) key: &'a str,
    pub(super) upload_id: &'a str,
    pub(super) part_size: usize,
    /// Parts in flight at most (at least one); the writer also buffers the part being filled.
    pub(super) max_inflight: usize,
    /// Reached once, as soon as the parts this call sent and the service acknowledged hold at
    /// least `.0` bytes.
    pub(super) checkpoint: Option<(u64, &'a dyn PartsCheckpoint)>,
}

/// What an upload does at its first checkpoint (the checkpointed upload on the final key writes
/// its pointer there).
#[async_trait]
pub(super) trait PartsCheckpoint: Sync {
    async fn reached(&self) -> Result<(), StorageRoleFailure>;
}

/// What [`S3StagedDestination::upload_parts`] sent.
pub(super) struct UploadedParts {
    /// Every part (number, `ETag`), sorted by number.
    pub(super) parts: Vec<(i32, String)>,
    /// The bytes the input held.
    pub(super) bytes: u64,
    /// Whether the service reported every part this call sent with its MD5 as `ETag` — a store
    /// that does (no server-side encryption with KMS) gives the completed object the composite
    /// `ETag` of the parts.
    pub(super) md5_etags: bool,
}

/// One uploaded part, and whether its `ETag` is its MD5.
type SentPart = (i32, String, bool);

impl<P: S3Protocol + 'static> S3StagedDestination<P> {
    /// Uploads `input` as the parts after `parts` (numbering continues from the highest one),
    /// every part `part_size` bytes but the last, each with `Content-MD5`. An upload that would
    /// otherwise have no part gets one empty part.
    ///
    /// A failed input waits for the parts in flight, then fails; a failed part — or a failed
    /// checkpoint — fails at once.
    pub(super) async fn upload_parts(
        &self,
        target: &PartTarget<'_>,
        parts: Vec<(i32, String)>,
        mut input: ByteStream,
    ) -> Result<UploadedParts, StorageRoleFailure> {
        let mut progress = Progress::new(target, parts);
        let mut buffered = Vec::new();
        let mut buffered_bytes = 0;
        let mut number = progress
            .sent
            .parts
            .iter()
            .map(|part| part.0)
            .max()
            .unwrap_or(0)
            + 1;
        let mut inflight = FuturesUnordered::new();
        loop {
            let chunk = loop {
                tokio::select! {
                    biased;
                    completed = inflight.next(), if !inflight.is_empty() => {
                        if let Some(completed) = completed { progress.record(completed).await?; }
                    }
                    chunk = input.next() => break chunk,
                }
            };
            let Some(chunk) = chunk else { break };
            let mut chunk = match chunk {
                Ok(chunk) => chunk,
                Err(input_failure) => {
                    while inflight.next().await.is_some() {}
                    return Err(input_failure);
                }
            };
            progress.sent.bytes += chunk.len() as u64;
            while !chunk.is_empty() {
                let take = chunk.len().min(target.part_size - buffered_bytes);
                buffered.push(chunk.split_to(take));
                buffered_bytes += take;
                if buffered_bytes < target.part_size {
                    continue;
                }
                let part = S3Body::new(std::mem::take(&mut buffered));
                buffered_bytes = 0;
                inflight.push(self.upload_part_to(target, number, part));
                number += 1;
                if inflight.len() >= target.max_inflight.max(1) {
                    let Some(completed) = inflight.next().await else {
                        return Err(entry(
                            target.path,
                            Operation::Write,
                            "S3 inflight upload disappeared",
                        ));
                    };
                    progress.record(completed).await?;
                }
            }
        }
        if !buffered.is_empty() || (progress.sent.parts.is_empty() && inflight.is_empty()) {
            inflight.push(self.upload_part_to(target, number, S3Body::new(buffered)));
        }
        while let Some(part) = inflight.next().await {
            progress.record(part).await?;
        }
        progress.sent.parts.sort_by_key(|part| part.0);
        Ok(progress.sent)
    }

    fn upload_part_to(
        &self,
        target: &PartTarget<'_>,
        number: i32,
        part: S3Body,
    ) -> impl Future<Output = (Result<SentPart, S3ProtocolFailure>, u64)> + use<P> {
        let length = part.len() as u64;
        let observer = target.stage.and_then(|stage| stage.write_observer.clone());
        let offset = u64::try_from(number.saturating_sub(1)).unwrap_or(0) * target.part_size as u64;
        let sent = upload(
            self.protocol.clone(),
            target.key.to_string(),
            target.upload_id.to_string(),
            number,
            part,
        );
        async move {
            let result = sent.await;
            if result.is_ok()
                && let Some(observer) = observer
            {
                observer.confirmed(offset, length);
            }
            (result, length)
        }
    }
}

/// The parts sent so far, and the checkpoint still to reach.
struct Progress<'a> {
    stage: Option<&'a PreparedStage>,
    next_part: i32,
    durable_prefix: u64,
    completed: HashMap<i32, u64>,
    path: &'a StoragePath,
    sent: UploadedParts,
    acknowledged: u64,
    checkpoint: Option<(u64, &'a dyn PartsCheckpoint)>,
}

impl<'a> Progress<'a> {
    fn new(target: &PartTarget<'a>, parts: Vec<(i32, String)>) -> Self {
        Self {
            stage: target.stage,
            next_part: i32::try_from(parts.len())
                .unwrap_or(i32::MAX)
                .saturating_add(1),
            durable_prefix: parts.len() as u64 * target.part_size as u64,
            completed: HashMap::new(),
            path: target.path,
            sent: UploadedParts {
                parts,
                bytes: 0,
                md5_etags: true,
            },
            acknowledged: 0,
            checkpoint: target.checkpoint,
        }
    }

    /// Records one acknowledged part; reaches the checkpoint once its bytes are acknowledged.
    async fn record(
        &mut self,
        (part, length): (Result<SentPart, S3ProtocolFailure>, u64),
    ) -> Result<(), StorageRoleFailure> {
        let part = part.map_err(|error| role_failure(self.path, Operation::Write, error))?;
        let number = part.0;
        self.sent.record(part);
        self.acknowledged += length;
        if let Some((at, checkpoint)) = self.checkpoint
            && self.acknowledged >= at
        {
            self.checkpoint = None;
            checkpoint.reached().await?;
        }
        self.notify_checkpoint(number, length);
        Ok(())
    }

    fn notify_checkpoint(&mut self, number: i32, length: u64) {
        let Some(stage) = self.stage.filter(|stage| stage.write_observer.is_some()) else {
            return;
        };
        self.completed.insert(number, length);
        while let Some(length) = self.completed.remove(&self.next_part) {
            self.durable_prefix += length;
            self.next_part += 1;
        }
        if stage.recovery_enabled() && self.durable_prefix > 0 {
            stage.checkpoint_recorded(self.durable_prefix);
        }
    }
}

impl UploadedParts {
    fn record(&mut self, (number, etag, md5_etag): SentPart) {
        self.md5_etags &= md5_etag;
        self.parts.push((number, etag));
    }
}

pub(super) async fn upload<P: S3Protocol>(
    protocol: Arc<P>,
    key: String,
    upload_id: String,
    number: i32,
    bytes: S3Body,
) -> Result<SentPart, S3ProtocolFailure> {
    if !(1..=10_000).contains(&number) {
        return Err(S3ProtocolFailure::entry(
            FailureClass::InvalidInput,
            Transience::Permanent,
            "S3 multipart part limit exceeded",
        ));
    }
    // A part may be hundreds of MiB: hash it off the async workers.
    let hashed = bytes.clone();
    let digest = task::spawn_blocking(move || hashed.md5())
        .await
        .map_err(|_| S3ProtocolFailure::protocol("S3 part digest task stopped"))?;
    let etag = protocol
        .upload_part(
            &key,
            &upload_id,
            number,
            bytes,
            &BASE64_STANDARD.encode(digest),
        )
        .await?;
    let md5_etag = single::same_etag(&etag, &format!("{digest:x}"));
    Ok((number, etag, md5_etag))
}
