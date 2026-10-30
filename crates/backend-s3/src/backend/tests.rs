#![allow(clippy::wildcard_imports)] // Fake protocols share their parent adapter helpers.
//! The in-crate S3 used by every S3 role test: an in-memory bucket with multipart uploads, tags
//! and injectable failures, plus the identity helpers the tests share.

#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::fmt::Write as _;
use std::mem;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD};
use bytes::Bytes;
use md5::{Digest as _, Md5};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::{
    BackendKind, MetadataObservation, ObjectTag, ObservationMode, ObservationPlan, StoragePath,
};
use crate::storage::{
    FinalDestination, MetadataMutation, PrepareRequest, PublishRequest, ReadRequest, VerifyRequest,
};

use memory_nas::NasRefused;

pub(super) type UploadParts = HashMap<String, (String, Vec<(i32, Bytes)>)>;
/// MD5s by (address, length) of a body, each with a clone that keeps the body allocated.
type Md5Cache = HashMap<(usize, usize), (Bytes, [u8; 16])>;
type BodyFragments = Vec<(i32, Vec<Bytes>)>;

pub struct RangeGate {
    pub offset: u64,
    pub started: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

#[derive(Default)]
pub struct MemoryS3 {
    state: Arc<MemoryS3State>,
    prefix: String,
}

impl std::ops::Deref for MemoryS3 {
    type Target = MemoryS3State;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

#[derive(Default)]
pub struct MemoryS3State {
    pub declared_size: Mutex<Option<u64>>,
    pub read_window: Option<usize>,
    pub range_gate: Mutex<Option<Arc<RangeGate>>>,
    pub body_fragments: Mutex<Option<BodyFragments>>,
    pub version: Mutex<Option<String>>,
    pub last_modified: Mutex<Option<crate::model::StorageTimestamp>>,
    pub range_observations: Mutex<Vec<S3ObjectFacts>>,
    pub range_dispatches: Mutex<Vec<(tokio::time::Instant, Range<u64>)>>,
    pub objects: Mutex<HashMap<String, Bytes>>,
    pub(super) uploads: Mutex<UploadParts>,
    tags: Mutex<HashMap<String, Vec<ObjectTag>>>,
    pub tag_reads: Mutex<u32>,
    pub tag_write_failure: Mutex<Option<S3ProtocolFailure>>,
    /// `AbortMultipartUpload` requests that got past `abort_failure`, whether or not the upload
    /// was still there.
    pub aborts: Mutex<u32>,
    pub(super) abort_failure: Mutex<Option<S3ProtocolFailure>>,
    pub head_versions: Mutex<Vec<Option<String>>>,
    pub head_failure: Mutex<Option<(String, S3ProtocolFailure)>>,
    /// Deleting this key — by version or not — fails with this failure (every time, until
    /// cleared), deleting nothing.
    pub delete_failure: Mutex<Option<(String, S3ProtocolFailure)>>,
    pub copy_commits_then_fails: Mutex<bool>,
    pub native_copies: Mutex<u64>,
    pub native_failure: Mutex<Option<S3ProtocolFailure>>,
    /// `CopyObject` replies carry no version header, even in a versioned bucket (DXN's Ceph RGW).
    pub copy_omits_version: Mutex<bool>,
    /// `UploadPartCopy` requests received, and the part whose copy cancels a token (see
    /// [`memory_native`]).
    pub part_copies: Mutex<u32>,
    part_copies_active: Mutex<u32>,
    pub part_copies_peak: Mutex<u32>,
    cancel_on_part_copy: Mutex<Option<(i32, CancellationToken)>>,
    /// Stored versions by (key, version id); `None` is a delete marker.
    pub versions: Mutex<HashMap<(String, String), Option<Bytes>>>,
    /// The version id each tag read asked for.
    pub tag_versions: Mutex<Vec<Option<String>>>,
    pub tagged_versions: Mutex<HashMap<(String, String), Vec<ObjectTag>>>,
    /// A tag read of this version id fails with this failure (every time, until cleared).
    pub tag_failure: Mutex<Option<(String, S3ProtocolFailure)>>,
    /// Behave like a store that ignores `?versionId=` and answers with the current object.
    pub ignores_version_id: Mutex<bool>,
    /// The next `put_object` fails with `BadDigest`, as if its body was corrupted in flight.
    pub bad_digest_next_put: Mutex<bool>,
    /// The next `put_object` stores its object, then loses the reply (a transient connectivity
    /// failure).
    pub put_commits_then_fails: Mutex<bool>,
    /// The next `put_object` is stored twice, as when the SDK re-sends a PUT whose first attempt
    /// committed but lost its reply: a versioned bucket keeps two identical versions.
    pub put_stored_twice: Mutex<bool>,
    /// The next `put_object` stores nothing and loses its reply (a transient connectivity failure).
    pub put_fails_without_commit: Mutex<bool>,
    /// `put_object` replies carry no version header (some stores, even with versioning suspended).
    pub put_omits_version: Mutex<bool>,
    /// The next `complete_multipart` stores its object, then loses the reply (a transient
    /// connectivity failure).
    pub complete_commits_then_fails: Mutex<bool>,
    /// Written to the key right after that lost completion (a later writer).
    pub written_after_lost_completion: Mutex<Option<Bytes>>,
    /// The next `complete_multipart` fails with this, completing nothing.
    pub complete_failure: Mutex<Option<S3ProtocolFailure>>,
    /// The next `complete_multipart` that stores its object reports this `ETag` instead of the
    /// composite one.
    pub complete_etag: Mutex<Option<String>>,
    /// Uploading this part number fails with this failure (every time, until cleared).
    pub part_failure: Mutex<Option<(i32, S3ProtocolFailure)>>,
    /// The failing part answers only once every lower-numbered part of its upload is stored (a
    /// slow failure), so a write it cuts leaves exactly the parts before it.
    pub part_failure_waits: Mutex<bool>,
    /// Issue upload ids as `MinIO` does — base64url(`<deployment>.<id>`) — while
    /// `ListMultipartUploads` reports the bare id; every call accepts either spelling.
    pub minio_upload_ids: Mutex<bool>,
    /// Complete a multipart upload to a key ending in '/' under the key without it, over whatever
    /// is there — as `RustFS` 1.0.0 does (P7 D13).
    pub rustfs_multipart_directory_objects: Mutex<bool>,
    /// `PutObject` requests received, multipart uploads begun, parts uploaded and completions
    /// that stored an object.
    pub puts: Mutex<u32>,
    pub multipart_begins: Mutex<u32>,
    pub part_uploads: Mutex<u32>,
    pub completes: Mutex<u32>,
    /// `ListObjectVersions` requests of one key (`list_versions`).
    pub version_lists: Mutex<u32>,
    /// Contents a multipart upload completed, by their MD5: real S3 gives such an object the
    /// `ETag` `"<MD5 of the part MD5s>-<parts>"`, not the MD5 of its bytes.
    multipart_etags: std::sync::Mutex<HashMap<[u8; 16], String>>,
    /// MD5s of stored bodies by (address, length), each with a clone that keeps it allocated.
    md5_cache: std::sync::Mutex<Md5Cache>,
    /// Source of upload ids and minted version ids, so none repeats.
    next_id: Mutex<u64>,
    /// The versioned mode (ADR-0006 C17): see [`memory_versions`].
    bucket: std::sync::Mutex<memory_versions::VersionedBucket>,
    /// Paging and failures of delimiter listings (ADR-0006 C22): see [`memory_listing`].
    listing: std::sync::Mutex<memory_listing::ListingKnobs>,
    /// The ONTAP NAS bucket mode (P7 §14.2): see [`memory_nas`].
    nas: std::sync::Mutex<memory_nas::NasBucket>,
    /// Streamed `PutObject`s (P7 §14.2 N3): see [`memory_streaming`].
    streaming: std::sync::Mutex<memory_streaming::StreamingKnobs>,
}

impl MemoryS3 {
    #[must_use]
    pub fn with_read_window(read_window: usize) -> Self {
        Self {
            state: Arc::new(MemoryS3State {
                read_window: Some(read_window),
                ..MemoryS3State::default()
            }),
            ..Self::default()
        }
    }

    /// A read-only source root of the same bucket, as a real S3 URL's prefix.
    #[must_use]
    pub fn source_root(self: &Arc<Self>, prefix: &str) -> Arc<Self> {
        Arc::new(Self {
            state: self.state.clone(),
            prefix: format!("{}/", prefix.trim_end_matches('/')),
        })
    }

    fn full_key(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }
}

/// The `ETag` S3 reports for an object written by one `PutObject`: the quoted hex MD5 of its body.
/// The `ETag` of a single `PutObject` of `bytes` (no server-side encryption): its quoted MD5.
#[must_use]
pub fn etag_of(bytes: &[u8]) -> String {
    format!("\"{:x}\"", Md5::digest(bytes))
}

/// The deployment id the fake's `MinIO`-style upload ids carry.
const MINIO_DEPLOYMENT: &str = "28af438e-c010-40e1-b507-6a63171ca083";

/// The id an upload is stored under, from either of its `MinIO` spellings.
fn bare_upload_id(id: &str) -> String {
    URL_SAFE_NO_PAD
        .decode(id)
        .ok()
        .and_then(|decoded| String::from_utf8(decoded).ok())
        .and_then(|decoded| {
            decoded
                .strip_prefix(&format!("{MINIO_DEPLOYMENT}."))
                .map(str::to_string)
        })
        .unwrap_or_else(|| id.to_string())
}

fn hex(digest: &[u8]) -> String {
    digest.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

/// The base64 `Content-MD5` of `bytes`.
#[must_use]
pub fn content_md5(bytes: &[u8]) -> String {
    BASE64_STANDARD.encode(Md5::digest(bytes))
}

impl MemoryS3 {
    /// Stores `bytes` as version `id` of `key` and makes it the current object.
    pub async fn put_version(&self, key: &str, id: &str, bytes: Bytes) {
        self.versions
            .lock()
            .await
            .insert((key.to_string(), id.to_string()), Some(bytes.clone()));
        self.objects.lock().await.insert(key.to_string(), bytes);
        *self.version.lock().await = Some(id.to_string());
        self.record_version(key, id);
    }

    /// Adds a delete marker as version `id` of `key`; the current object is gone.
    pub async fn put_delete_marker(&self, key: &str, id: &str) {
        self.versions
            .lock()
            .await
            .insert((key.to_string(), id.to_string()), None);
        self.objects.lock().await.remove(key);
        self.record_version(key, id);
    }

    async fn injected_delete_failure(&self, key: &str) -> S3Result<()> {
        match &*self.delete_failure.lock().await {
            Some((failed, failure)) if failed == key => Err(failure.clone()),
            _ => Ok(()),
        }
    }

    async fn next_id(&self) -> u64 {
        let mut next = self.next_id.lock().await;
        *next += 1;
        *next
    }

    /// Stores a written object: on a versioned fake (its current version is a real one) the write
    /// mints a new current version, as a versioned bucket does; otherwise it reports none.
    /// The `ETag` S3 reports for an object holding `bytes`: the composite one if a multipart
    /// upload completed it, else its MD5.
    fn etag_for(&self, bytes: &Bytes) -> String {
        let digest = self.md5_of(bytes);
        self.multipart_etags
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&digest)
            .cloned()
            .unwrap_or_else(|| format!("\"{}\"", hex(&digest)))
    }

    /// The MD5 of a stored body, computed once per allocation: every range read checks the
    /// object's `ETag`, and hashing the whole object each time made large-object tests slow. The
    /// cache holds a clone, so the allocation (and with it the key) cannot be reused while cached.
    fn md5_of(&self, bytes: &Bytes) -> [u8; 16] {
        let key = (bytes.as_ptr() as usize, bytes.len());
        let mut cache = self
            .md5_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache
            .entry(key)
            .or_insert_with(|| (bytes.clone(), Md5::digest(bytes).into()))
            .1
    }

    async fn store_written(&self, key: &str, bytes: Bytes) -> Option<String> {
        self.nas_written(key, bytes.len());
        if let Some(id) = self.versioned_store(key, &bytes).await {
            return Some(id);
        }
        let versioned = self
            .version
            .lock()
            .await
            .as_deref()
            .is_some_and(is_real_version_id);
        if !versioned {
            self.objects.lock().await.insert(key.to_string(), bytes);
            return None;
        }
        let id = format!("v-put-{}", self.next_id().await);
        self.put_version(key, &id, bytes).await;
        Some(id)
    }

    /// Waits (up to a few seconds) until parts `1..number` of upload `id` are stored.
    async fn wait_for_parts_below(&self, id: &str, number: i32) {
        for _ in 0..5_000 {
            let stored = self.uploads.lock().await.get(id).is_some_and(|upload| {
                (1..number).all(|lower| upload.1.iter().any(|part| part.0 == lower))
            });
            if stored {
                return;
            }
            sleep(Duration::from_millis(1)).await;
        }
    }

    /// The bytes of `key` at `version` when that version is stored, else of the current object.
    async fn bytes_at(&self, key: &str, version: Option<&str>) -> S3Result<Bytes> {
        if let Some(version) = version
            && let Some(stored) = self
                .versions
                .lock()
                .await
                .get(&(key.to_string(), version.to_string()))
        {
            return stored.clone().ok_or_else(not_found);
        }
        self.objects
            .lock()
            .await
            .get(key)
            .cloned()
            .ok_or_else(not_found)
    }
}

fn missing_upload(diagnostic: &str) -> S3ProtocolFailure {
    S3ProtocolFailure::entry(
        crate::model::FailureClass::NotFound,
        crate::model::Transience::Permanent,
        diagnostic,
    )
}

/// The smallest part S3 accepts anywhere but last: 5 MiB.
const MIN_PART_SIZE: usize = 5 * 1024 * 1024;

/// The stored parts a completion names, in its order — refused as S3 refuses them: a list out of
/// ascending order (`InvalidPartOrder`) or naming a part the upload does not hold under that
/// `ETag` (`InvalidPart`) is a permanent `Conflict`, a part other than the last below 5 MiB
/// (`EntityTooSmall`) a permanent `Corruption`.
fn listed_parts(stored: &[(i32, Bytes)], listed: &[(i32, String)]) -> S3Result<Vec<Bytes>> {
    let refused = |class, diagnostic| {
        S3ProtocolFailure::entry(class, crate::model::Transience::Permanent, diagnostic)
    };
    if listed.is_empty() || listed.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(refused(
            crate::model::FailureClass::Conflict,
            "InvalidPartOrder",
        ));
    }
    let mut parts = Vec::with_capacity(listed.len());
    for (number, etag) in listed {
        let bytes = stored
            .iter()
            .find(|part| part.0 == *number && etag_of(&part.1) == *etag)
            .map(|part| part.1.clone())
            .ok_or_else(|| refused(crate::model::FailureClass::Conflict, "InvalidPart"))?;
        parts.push(bytes);
    }
    if parts[..parts.len() - 1]
        .iter()
        .any(|part| part.len() < MIN_PART_SIZE)
    {
        return Err(refused(
            crate::model::FailureClass::Corruption,
            "EntityTooSmall",
        ));
    }
    Ok(parts)
}

fn not_found() -> S3ProtocolFailure {
    S3ProtocolFailure::entry(
        crate::model::FailureClass::NotFound,
        crate::model::Transience::Permanent,
        "not found",
    )
}

#[async_trait]
impl S3Protocol for MemoryS3 {
    fn read_concurrency(&self) -> usize {
        self.read_window.unwrap_or(DEFAULT_READ_CONCURRENCY)
    }
    fn versioning_enabled(&self) -> bool {
        self.versioning_is_enabled()
    }
    fn versioning_at_connect(&self) -> S3VersioningAtConnect {
        self.versioning_answer_at_connect()
    }
    fn bucket_capabilities(&self) -> S3BucketCapabilities {
        self.capabilities()
    }
    async fn head(&self, key: &str) -> S3Result<S3ObjectFacts> {
        let full_key = self.full_key(key);
        let key = full_key.as_str();
        self.head_versions.lock().await.push(None);
        self.nas_head_served();
        if let Some((failed_key, failure)) = &*self.head_failure.lock().await
            && failed_key == key
        {
            return Err(failure.clone());
        }
        if let Some(facts) = self.versioned_head(key).await {
            return facts;
        }
        let objects = self.objects.lock().await;
        let bytes = objects.get(key).ok_or_else(|| {
            S3ProtocolFailure::entry(
                crate::model::FailureClass::NotFound,
                crate::model::Transience::Permanent,
                "not found",
            )
        })?;
        Ok(S3ObjectFacts {
            size: self
                .declared_size
                .lock()
                .await
                .unwrap_or(bytes.len() as u64),
            etag: self.object_etag(key, bytes),
            version_id: self.version.lock().await.clone(),
            last_modified: *self.last_modified.lock().await,
        })
    }
    async fn head_version(&self, key: &str, version_id: &str) -> S3Result<S3ObjectFacts> {
        self.head_versions
            .lock()
            .await
            .push(Some(version_id.to_owned()));
        if *self.ignores_version_id.lock().await {
            return self.head(key).await;
        }
        let relative_key = key;
        let full_key = self.full_key(key);
        let key = full_key.as_str();
        let stored = self
            .versions
            .lock()
            .await
            .get(&(key.to_string(), version_id.to_string()))
            .cloned();
        let bytes = match stored {
            Some(stored) => stored.ok_or_else(not_found)?,
            None if self.version.lock().await.as_deref() == Some(version_id) => {
                return self.head(relative_key).await;
            }
            None => return Err(not_found()),
        };
        Ok(S3ObjectFacts {
            size: bytes.len() as u64,
            etag: self.etag_for(&bytes),
            version_id: Some(version_id.to_string()),
            last_modified: *self.last_modified.lock().await,
        })
    }
    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
        observed: &S3ObjectFacts,
    ) -> S3Result<Bytes> {
        let full_key = self.full_key(key);
        let key = full_key.as_str();
        self.range_observations.lock().await.push(observed.clone());
        self.range_dispatches
            .lock()
            .await
            .push((tokio::time::Instant::now(), range.clone()));
        let gate = self
            .range_gate
            .lock()
            .await
            .clone()
            .filter(|gate| gate.offset == range.start);
        if let Some(gate) = gate {
            gate.started.notify_one();
            gate.release.notified().await;
        }
        let bytes = self.bytes_at(key, observed.version_id.as_deref()).await?;
        // Pinned by `If-Match` on an object bucket; checked after the fact in NAS mode, as the
        // real protocol checks the response `ETag`: either way another object is a `Conflict`.
        if observed.etag != self.object_etag(key, &bytes) {
            return Err(S3ProtocolFailure::entry(
                crate::model::FailureClass::Conflict,
                crate::model::Transience::Permanent,
                "source changed",
            ));
        }
        let start = usize::try_from(range.start).map_err(|_| {
            S3ProtocolFailure::entry(
                crate::model::FailureClass::InvalidInput,
                crate::model::Transience::Permanent,
                "range too large",
            )
        })?;
        let end = usize::try_from(range.end).map_err(|_| {
            S3ProtocolFailure::entry(
                crate::model::FailureClass::InvalidInput,
                crate::model::Transience::Permanent,
                "range too large",
            )
        })?;
        self.nas_range_served(key, observed).await?;
        Ok(bytes.slice(start..end))
    }
    async fn put_object(
        &self,
        key: &str,
        body: crate::storage::backends::s3::S3Body,
        content_md5_base64: &str,
    ) -> S3Result<S3WriteFacts> {
        let body = body.into_bytes();
        *self.puts.lock().await += 1;
        let corrupted = mem::take(&mut *self.bad_digest_next_put.lock().await);
        if corrupted || content_md5(&body) != content_md5_base64 {
            return Err(S3ProtocolFailure::corrupted_upload(
                "S3 PutObject request failed",
            ));
        }
        if mem::take(&mut *self.put_fails_without_commit.lock().await) {
            return Err(S3ProtocolFailure::session(
                crate::model::FailureClass::Connectivity,
                crate::model::Transience::Transient,
                "put response lost",
            ));
        }
        let etag = etag_of(&body);
        self.multipart_etags
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&<[u8; 16]>::from(Md5::digest(&body)));
        if mem::take(&mut *self.put_stored_twice.lock().await) {
            self.store_written(key, body.clone()).await;
        }
        let omit_version = *self.put_omits_version.lock().await;
        let version = self
            .store_written(key, body)
            .await
            .filter(|_| !omit_version);
        if mem::take(&mut *self.put_commits_then_fails.lock().await) {
            return Err(S3ProtocolFailure::session(
                crate::model::FailureClass::Connectivity,
                crate::model::Transience::Transient,
                "put response lost",
            ));
        }
        Ok(S3WriteFacts::new(etag, version))
    }
    fn stall_timeout(&self) -> Duration {
        self.memory_stall_timeout()
    }
    async fn put_object_streaming(
        &self,
        key: &str,
        len: u64,
        chunks: tokio::sync::mpsc::Receiver<Bytes>,
    ) -> S3Result<S3StreamedPut> {
        self.memory_put_streaming(key, len, chunks).await
    }
    async fn begin_multipart(&self, key: &str) -> S3Result<String> {
        *self.multipart_begins.lock().await += 1;
        self.nas_refusal(NasRefused::NotImplemented)?;
        let id = format!("upload-{}", self.next_id().await);
        self.uploads
            .lock()
            .await
            .insert(id.clone(), (key.to_string(), Vec::new()));
        if *self.minio_upload_ids.lock().await {
            return Ok(URL_SAFE_NO_PAD.encode(format!("{MINIO_DEPLOYMENT}.{id}")));
        }
        Ok(id)
    }
    async fn upload_part(
        &self,
        _key: &str,
        id: &str,
        number: i32,
        bytes: crate::storage::backends::s3::S3Body,
        content_md5_base64: &str,
    ) -> S3Result<String> {
        if let Some(recorded) = self.body_fragments.lock().await.as_mut() {
            recorded.push((number, bytes.fragments().to_vec()));
        }
        let bytes = bytes.into_bytes();
        *self.part_uploads.lock().await += 1;
        let id = bare_upload_id(id);
        let id = id.as_str();
        let failing = self.part_failure.lock().await.clone();
        if let Some((_, failure)) = failing.filter(|(failed, _)| *failed == number) {
            if *self.part_failure_waits.lock().await {
                self.wait_for_parts_below(id, number).await;
            }
            return Err(failure);
        }
        if content_md5(&bytes) != content_md5_base64 {
            return Err(S3ProtocolFailure::corrupted_upload(
                "S3 UploadPart request failed",
            ));
        }
        let etag = etag_of(&bytes);
        let mut uploads = self.uploads.lock().await;
        let parts = &mut uploads
            .get_mut(id)
            .ok_or_else(|| missing_upload("S3 UploadPart request failed"))?
            .1;
        // Uploading a part number again replaces the part, as S3 does.
        parts.retain(|part| part.0 != number);
        parts.push((number, bytes));
        Ok(etag)
    }
    async fn complete_multipart(
        &self,
        _key: &str,
        id: &str,
        parts: &[(i32, String)],
    ) -> S3Result<S3WriteFacts> {
        let id = bare_upload_id(id);
        let id = id.as_str();
        if let Some(failure) = self.complete_failure.lock().await.take() {
            return Err(failure);
        }
        let (key, stored) = self
            .uploads
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| missing_upload("S3 CompleteMultipartUpload request failed"))?;
        let listed = listed_parts(&stored, parts)?;
        self.uploads.lock().await.remove(id);
        let mut part_digests = Vec::new();
        for part in &listed {
            part_digests.extend_from_slice(&Md5::digest(part));
        }
        let composite = format!("\"{:x}-{}\"", Md5::digest(&part_digests), listed.len());
        let bytes: Vec<u8> = listed
            .iter()
            .flat_map(|part| part.iter().copied())
            .collect();
        self.multipart_etags
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(Md5::digest(&bytes).into(), composite.clone());
        let rekeyed = *self.rustfs_multipart_directory_objects.lock().await;
        let key = match key.strip_suffix('/') {
            Some(file) if rekeyed => file.to_string(),
            _ => key.clone(),
        };
        let version = self.store_written(&key, Bytes::from(bytes)).await;
        *self.completes.lock().await += 1;
        if mem::take(&mut *self.complete_commits_then_fails.lock().await) {
            if let Some(later) = self.written_after_lost_completion.lock().await.take() {
                self.store_written(&key, later).await;
            }
            return Err(S3ProtocolFailure::session(
                crate::model::FailureClass::Connectivity,
                crate::model::Transience::Transient,
                "complete response lost",
            ));
        }
        let reported = self.complete_etag.lock().await.take().unwrap_or(composite);
        Ok(S3WriteFacts::new(reported, version))
    }
    async fn abort_multipart(&self, _key: &str, id: &str) -> S3Result<()> {
        let id = bare_upload_id(id);
        let id = id.as_str();
        if let Some(failure) = self.abort_failure.lock().await.clone() {
            return Err(failure);
        }
        *self.aborts.lock().await += 1;
        // An upload that completed or was aborted is gone: `NoSuchUpload`, as S3 answers.
        self.uploads
            .lock()
            .await
            .remove(id)
            .map(drop)
            .ok_or_else(|| missing_upload("S3 AbortMultipartUpload request failed"))
    }
    async fn list_parts(&self, _key: &str, id: &str) -> S3Result<Vec<S3PartFacts>> {
        self.nas_refusal(NasRefused::Denied)?;
        let id = bare_upload_id(id);
        let id = id.as_str();
        Ok(self
            .uploads
            .lock()
            .await
            .get(id)
            .ok_or_else(|| {
                S3ProtocolFailure::entry(
                    crate::model::FailureClass::NotFound,
                    crate::model::Transience::Permanent,
                    "missing upload",
                )
            })?
            .1
            .iter()
            .map(|(number, bytes)| S3PartFacts {
                number: *number,
                size: bytes.len() as u64,
                etag: etag_of(bytes),
            })
            .collect())
    }
    async fn list_uploads(&self, key: &str) -> S3Result<Vec<String>> {
        self.nas_refusal(NasRefused::Denied)?;
        let mut ids: Vec<String> = self
            .uploads
            .lock()
            .await
            .iter()
            .filter(|(_, upload)| upload.0 == key)
            .map(|(id, _)| id.clone())
            .collect();
        ids.sort();
        Ok(ids)
    }
    async fn copy_from(&self, source: &S3NativeCopySource, to: &str) -> S3Result<S3WriteFacts> {
        self.memory_copy_from(source, to).await
    }
    async fn upload_part_copy(
        &self,
        source: &S3NativeCopySource,
        key: &str,
        upload_id: &str,
        number: i32,
        range: Range<u64>,
    ) -> S3Result<String> {
        self.memory_upload_part_copy(source, (key, upload_id), number, range)
            .await
    }
    async fn bucket_versioning(&self) -> S3Result<S3BucketVersioning> {
        // A NAS bucket answers 501, which the real protocol reads as versioning off.
        if self.is_nas() {
            self.nas().versioning_requests += 1;
            return Ok(S3BucketVersioning::Off);
        }
        Ok(self.bucket_versioning_now())
    }

    async fn write_delete_marker(&self, key: &str) -> S3Result<S3DeleteReply> {
        self.delete_marker_in_memory(key).await
    }

    async fn delete_object(&self, key: &str) -> S3Result<()> {
        self.record_plain_delete(key);
        self.injected_delete_failure(key).await?;
        if !self.versioned_delete(key).await? {
            self.objects.lock().await.remove(key);
        }
        Ok(())
    }
    async fn delete_version(&self, key: &str, version_id: &str) -> S3Result<()> {
        self.injected_delete_failure(key).await?;
        self.delete_one_version(key, version_id).await
    }
    async fn list_versions(&self, key: &str) -> S3Result<Vec<S3VersionFacts>> {
        *self.version_lists.lock().await += 1;
        self.nas_refusal(NasRefused::NotImplemented)?;
        self.versions_of(key).await
    }
    async fn list_objects_page(&self, prefix: &str, token: Option<&str>) -> S3Result<S3ObjectPage> {
        let mut page = self
            .list_objects_page_in_memory(&self.full_key(prefix), token)
            .await?;
        for object in &mut page.objects {
            object.key = object
                .key
                .strip_prefix(&self.prefix)
                .unwrap_or(&object.key)
                .to_owned();
        }
        for prefix in &mut page.prefixes {
            *prefix = prefix
                .strip_prefix(&self.prefix)
                .unwrap_or(prefix)
                .to_owned();
        }
        Ok(page)
    }
    async fn list_versions_page(
        &self,
        prefix: &str,
        marker: Option<&S3VersionMarker>,
    ) -> S3Result<S3VersionPage> {
        self.nas_refusal(NasRefused::NotImplemented)?;
        let mut page = self
            .list_versions_page_in_memory(&self.full_key(prefix), marker)
            .await?;
        for version in page.versions.iter_mut().chain(&mut page.markers) {
            version.key = version
                .key
                .strip_prefix(&self.prefix)
                .unwrap_or(&version.key)
                .to_owned();
        }
        for prefix in &mut page.prefixes {
            *prefix = prefix
                .strip_prefix(&self.prefix)
                .unwrap_or(prefix)
                .to_owned();
        }
        Ok(page)
    }
    async fn get_tags(&self, key: &str, version_id: Option<&str>) -> S3Result<Vec<ObjectTag>> {
        let full_key = self.full_key(key);
        let key = full_key.as_str();
        *self.tag_reads.lock().await += 1;
        self.tag_versions
            .lock()
            .await
            .push(version_id.map(str::to_string));
        if let Some((failing, failure)) = &*self.tag_failure.lock().await
            && version_id == Some(failing.as_str())
        {
            return Err(failure.clone());
        }
        if let Some(version) = version_id
            && let Some(tags) = self
                .tagged_versions
                .lock()
                .await
                .get(&(key.to_owned(), version.to_owned()))
        {
            return Ok(tags.clone());
        }
        Ok(self.tags.lock().await.get(key).cloned().unwrap_or_default())
    }
    async fn put_tags(&self, key: &str, tags: &[ObjectTag]) -> S3Result<()> {
        if let Some(failure) = self.tag_write_failure.lock().await.clone() {
            return Err(failure);
        }
        self.tags
            .lock()
            .await
            .insert(key.to_string(), tags.to_vec());
        Ok(())
    }
}

/// S3 roles that send every object through a multipart upload, however small: for tests of that
/// path with payloads below the single-PUT threshold.
pub fn connect_multipart_only(
    protocol: Arc<MemoryS3>,
    identity: BackendIdentity,
    native_context: Option<S3NativeContext>,
) -> Result<crate::storage::Storage, Box<dyn std::error::Error>> {
    connect_configured(
        protocol,
        identity,
        native_context,
        S3TagSupport::Supported,
        None,
    )
}

/// The endpoint of one in-memory bucket, named after it: engine tests that run at once and write
/// the same key must not share the engine's per-file lease (keyed on endpoint and path).
#[must_use]
pub fn endpoint_of(protocol: &Arc<MemoryS3>) -> BackendIdentity {
    BackendIdentity::new(
        BackendKind::S3,
        format!("memory-bucket-{:p}", Arc::as_ptr(protocol)),
    )
    .expect("valid identity")
}

#[must_use]
pub fn identity() -> BackendIdentity {
    BackendIdentity::new(BackendKind::S3, "memory-bucket").expect("valid identity")
}

#[must_use]
pub fn native_context() -> S3NativeContext {
    S3NativeContext::new("memory://s3", "standard", "memory".into(), None)
}

/// Source roles rooted under `prefix`; the destination can keep using the bucket root.
pub fn source_storage(
    protocol: &Arc<MemoryS3>,
    prefix: &str,
) -> Result<Storage, Box<dyn std::error::Error>> {
    let rooted = protocol.source_root(prefix);
    let identity = BackendIdentity::new(
        BackendKind::S3,
        format!("memory-source-{:p}/{prefix}", Arc::as_ptr(protocol)),
    )?;
    connect(
        rooted,
        identity,
        Some(S3NativeContext::new(
            "memory://s3",
            "standard",
            "memory".into(),
            Some(prefix.into()),
        )),
    )
}

#[path = "memory_tests.rs"]
mod memory;

#[path = "memory_native.rs"]
mod memory_native;

#[path = "memory_listing.rs"]
mod memory_listing;

#[path = "connect_versioning_tests.rs"]
mod connect_versioning_tests;

#[path = "memory_nas.rs"]
mod memory_nas;
#[path = "memory_streaming.rs"]
mod memory_streaming;
#[path = "memory_versions.rs"]
mod memory_versions;
#[path = "nas_source_tests.rs"]
mod nas_source_tests;
pub use memory_versions::Versioning;

#[path = "role_tests.rs"]
mod roles;

#[path = "version_tests.rs"]
mod versions;

#[path = "multipart_tests.rs"]
mod multipart;

#[path = "namespace_tests.rs"]
mod namespace;

#[path = "listing_failure_tests.rs"]
mod listing_failures;

#[path = "streaming_tests.rs"]
mod streaming;

#[path = "directory_object_tests.rs"]
mod directory_objects;
