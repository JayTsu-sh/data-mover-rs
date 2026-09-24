//! Streaming a byte stream into the parts of one multipart upload, a few parts in flight: shared
//! by the temp-key stage and the `Direct` upload on the final key.

use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use md5::{Digest as _, Md5};
use tokio::task;

use super::super::source::{entry, role_failure};
use super::super::{S3Protocol, S3ProtocolFailure};
use super::{MAX_INFLIGHT_PARTS, S3StagedDestination, single};
use crate::model::{FailureClass, Operation, StoragePath, Transience};
use crate::storage::{ByteStream, StorageRoleFailure};

/// The upload the parts go to.
pub(super) struct PartTarget<'a> {
    pub(super) path: &'a StoragePath,
    pub(super) key: &'a str,
    pub(super) upload_id: &'a str,
    pub(super) part_size: usize,
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
    /// A failed input waits for the parts in flight, then fails; a failed part fails at once.
    pub(super) async fn upload_parts(
        &self,
        target: &PartTarget<'_>,
        parts: Vec<(i32, String)>,
        mut input: ByteStream,
    ) -> Result<UploadedParts, StorageRoleFailure> {
        let failed = |error| role_failure(target.path, Operation::Write, error);
        let mut sent = UploadedParts {
            parts,
            bytes: 0,
            md5_etags: true,
        };
        let mut buffered = BytesMut::with_capacity(target.part_size);
        let mut number = sent.parts.iter().map(|part| part.0).max().unwrap_or(0) + 1;
        let mut inflight = FuturesUnordered::new();
        while let Some(chunk) = input.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(input_failure) => {
                    while inflight.next().await.is_some() {}
                    return Err(input_failure);
                }
            };
            sent.bytes += chunk.len() as u64;
            buffered.extend_from_slice(&chunk);
            while buffered.len() >= target.part_size {
                let part = buffered.split_to(target.part_size).freeze();
                inflight.push(self.upload_part_to(target, number, part));
                number += 1;
                if inflight.len() >= MAX_INFLIGHT_PARTS {
                    let Some(completed) = inflight.next().await else {
                        return Err(entry(
                            target.path,
                            Operation::Write,
                            "S3 inflight upload disappeared",
                        ));
                    };
                    sent.record(completed.map_err(failed)?);
                }
            }
        }
        if !buffered.is_empty() || (sent.parts.is_empty() && inflight.is_empty()) {
            inflight.push(self.upload_part_to(target, number, buffered.freeze()));
        }
        while let Some(part) = inflight.next().await {
            sent.record(part.map_err(failed)?);
        }
        sent.parts.sort_by_key(|part| part.0);
        Ok(sent)
    }

    fn upload_part_to(
        &self,
        target: &PartTarget<'_>,
        number: i32,
        part: Bytes,
    ) -> impl Future<Output = Result<SentPart, S3ProtocolFailure>> + use<P> {
        upload(
            self.protocol.clone(),
            target.key.to_string(),
            target.upload_id.to_string(),
            number,
            part,
        )
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
