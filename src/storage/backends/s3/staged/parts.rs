//! Streaming a byte stream into the parts of one multipart upload, a few parts in flight: shared
//! by the `Direct` upload on the final key and the checkpointed upload on the final key (ADR-0006
//! C15b), which also asks to be told when its first checkpoint is reached.

use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use md5::{Digest as _, Md5};
use tokio::task;

use super::super::source::{entry, role_failure};
use super::super::{S3Protocol, S3ProtocolFailure};
use super::{S3StagedDestination, single};
use crate::model::{FailureClass, Operation, StoragePath, Transience};
use crate::storage::{ByteStream, StorageRoleFailure};

/// The upload the parts go to.
pub(super) struct PartTarget<'a> {
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
        let mut buffered = BytesMut::with_capacity(target.part_size);
        let mut number = progress
            .sent
            .parts
            .iter()
            .map(|part| part.0)
            .max()
            .unwrap_or(0)
            + 1;
        let mut inflight = FuturesUnordered::new();
        while let Some(chunk) = input.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(input_failure) => {
                    while inflight.next().await.is_some() {}
                    return Err(input_failure);
                }
            };
            progress.sent.bytes += chunk.len() as u64;
            buffered.extend_from_slice(&chunk);
            while buffered.len() >= target.part_size {
                let part = buffered.split_to(target.part_size).freeze();
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
            inflight.push(self.upload_part_to(target, number, buffered.freeze()));
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
        part: Bytes,
    ) -> impl Future<Output = (Result<SentPart, S3ProtocolFailure>, u64)> + use<P> {
        let length = part.len() as u64;
        let sent = upload(
            self.protocol.clone(),
            target.key.to_string(),
            target.upload_id.to_string(),
            number,
            part,
        );
        async move { (sent.await, length) }
    }
}

/// The parts sent so far, and the checkpoint still to reach.
struct Progress<'a> {
    path: &'a StoragePath,
    sent: UploadedParts,
    acknowledged: u64,
    checkpoint: Option<(u64, &'a dyn PartsCheckpoint)>,
}

impl<'a> Progress<'a> {
    fn new(target: &PartTarget<'a>, parts: Vec<(i32, String)>) -> Self {
        Self {
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
        self.sent.record(part);
        self.acknowledged += length;
        if let Some((at, checkpoint)) = self.checkpoint
            && self.acknowledged >= at
        {
            self.checkpoint = None;
            checkpoint.reached().await?;
        }
        Ok(())
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
    bytes: Bytes,
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
    let digest = task::spawn_blocking(move || Md5::digest(&hashed))
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
