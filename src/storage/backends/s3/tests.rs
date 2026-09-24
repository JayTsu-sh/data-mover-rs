//! The in-crate S3 used by every S3 role test: an in-memory bucket with multipart uploads, tags
//! and injectable failures, plus the identity / policy helpers the tests share.

#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::mem;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::Bytes;
use md5::{Digest as _, Md5};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::{
    BackendKind, MetadataObservation, ObjectTag, ObservationMode, ObservationPlan, StoragePath,
};
use crate::storage::{
    FinalDestination, MetadataMutation, PrepareRequest, PublishRequest, ReadRequest, VerifyRequest,
};

pub(super) type UploadParts = HashMap<String, (String, Vec<(i32, Bytes)>)>;

#[derive(Default)]
pub(crate) struct MemoryS3 {
    pub(crate) version: Mutex<Option<String>>,
    pub(crate) last_modified: Mutex<Option<crate::model::StorageTimestamp>>,
    pub(crate) range_observations: Mutex<Vec<S3ObjectFacts>>,
    pub(crate) objects: Mutex<HashMap<String, Bytes>>,
    pub(super) uploads: Mutex<UploadParts>,
    tags: Mutex<HashMap<String, Vec<ObjectTag>>>,
    tag_reads: Mutex<u32>,
    pub(crate) aborts: Mutex<u32>,
    pub(super) abort_failure: Mutex<Option<S3ProtocolFailure>>,
    pub(crate) head_failure: Mutex<Option<(String, S3ProtocolFailure)>>,
    pub(crate) copy_commits_then_fails: Mutex<bool>,
    pub(crate) native_copies: Mutex<u64>,
    pub(crate) native_failure: Mutex<Option<S3ProtocolFailure>>,
    /// Stored versions by (key, version id); `None` is a delete marker.
    pub(crate) versions: Mutex<HashMap<(String, String), Option<Bytes>>>,
    /// The version id each tag read asked for.
    pub(crate) tag_versions: Mutex<Vec<Option<String>>>,
    /// Behave like a store that ignores `?versionId=` and answers with the current object.
    pub(crate) ignores_version_id: Mutex<bool>,
    /// The next `put_object` fails with `BadDigest`, as if its body was corrupted in flight.
    pub(crate) bad_digest_next_put: Mutex<bool>,
    /// The next `put_object` stores its object, then loses the reply (a transient connectivity
    /// failure).
    pub(crate) put_commits_then_fails: Mutex<bool>,
    /// `PutObject` requests received, and multipart uploads begun.
    pub(crate) puts: Mutex<u32>,
    pub(crate) multipart_begins: Mutex<u32>,
    /// Contents a multipart upload completed, by their MD5: real S3 gives such an object the
    /// `ETag` `"<MD5 of the part MD5s>-<parts>"`, not the MD5 of its bytes.
    multipart_etags: std::sync::Mutex<HashMap<[u8; 16], String>>,
    /// Source of upload ids and minted version ids, so none repeats.
    next_id: Mutex<u64>,
}

/// The `ETag` S3 reports for an object written by one `PutObject`: the quoted hex MD5 of its body.
/// The `ETag` of a single `PutObject` of `bytes` (no server-side encryption): its quoted MD5.
pub(crate) fn etag_of(bytes: &[u8]) -> String {
    format!("\"{:x}\"", Md5::digest(bytes))
}

/// The base64 `Content-MD5` of `bytes`.
pub(crate) fn content_md5(bytes: &[u8]) -> String {
    BASE64_STANDARD.encode(Md5::digest(bytes))
}

impl MemoryS3 {
    /// Stores `bytes` as version `id` of `key` and makes it the current object.
    pub(crate) async fn put_version(&self, key: &str, id: &str, bytes: Bytes) {
        self.versions
            .lock()
            .await
            .insert((key.to_string(), id.to_string()), Some(bytes.clone()));
        self.objects.lock().await.insert(key.to_string(), bytes);
        *self.version.lock().await = Some(id.to_string());
    }

    /// Adds a delete marker as version `id` of `key`; the current object is gone.
    pub(crate) async fn put_delete_marker(&self, key: &str, id: &str) {
        self.versions
            .lock()
            .await
            .insert((key.to_string(), id.to_string()), None);
        self.objects.lock().await.remove(key);
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
    fn etag_for(&self, bytes: &[u8]) -> String {
        self.multipart_etags
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&<[u8; 16]>::from(Md5::digest(bytes)))
            .cloned()
            .unwrap_or_else(|| etag_of(bytes))
    }

    async fn store_written(&self, key: &str, bytes: Bytes) -> Option<String> {
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

fn not_found() -> S3ProtocolFailure {
    S3ProtocolFailure::entry(
        crate::model::FailureClass::NotFound,
        crate::model::Transience::Permanent,
        "not found",
    )
}

#[async_trait]
impl S3Protocol for MemoryS3 {
    async fn head(&self, key: &str) -> S3Result<S3ObjectFacts> {
        if let Some((failed_key, failure)) = &*self.head_failure.lock().await
            && failed_key == key
        {
            return Err(failure.clone());
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
            size: bytes.len() as u64,
            etag: self.etag_for(bytes),
            version_id: self.version.lock().await.clone(),
            last_modified: *self.last_modified.lock().await,
        })
    }
    async fn head_version(&self, key: &str, version_id: &str) -> S3Result<S3ObjectFacts> {
        if *self.ignores_version_id.lock().await {
            return self.head(key).await;
        }
        let stored = self
            .versions
            .lock()
            .await
            .get(&(key.to_string(), version_id.to_string()))
            .cloned();
        let bytes = match stored {
            Some(stored) => stored.ok_or_else(not_found)?,
            None if self.version.lock().await.as_deref() == Some(version_id) => {
                return self.head(key).await;
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
        self.range_observations.lock().await.push(observed.clone());
        let bytes = self.bytes_at(key, observed.version_id.as_deref()).await?;
        if observed.etag != self.etag_for(&bytes) {
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
        Ok(bytes.slice(start..end))
    }
    async fn put_object(
        &self,
        key: &str,
        body: Bytes,
        content_md5_base64: &str,
    ) -> S3Result<S3WriteFacts> {
        *self.puts.lock().await += 1;
        let corrupted = mem::take(&mut *self.bad_digest_next_put.lock().await);
        if corrupted || content_md5(&body) != content_md5_base64 {
            return Err(S3ProtocolFailure::corrupted_upload(
                "S3 PutObject request failed",
            ));
        }
        let etag = etag_of(&body);
        self.multipart_etags
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&<[u8; 16]>::from(Md5::digest(&body)));
        let version = self.store_written(key, body).await;
        if mem::take(&mut *self.put_commits_then_fails.lock().await) {
            return Err(S3ProtocolFailure::session(
                crate::model::FailureClass::Connectivity,
                crate::model::Transience::Transient,
                "put response lost",
            ));
        }
        Ok(S3WriteFacts::new(etag, version))
    }
    async fn begin_multipart(&self, key: &str) -> S3Result<String> {
        *self.multipart_begins.lock().await += 1;
        let id = format!("upload-{}", self.next_id().await);
        self.uploads
            .lock()
            .await
            .insert(id.clone(), (key.to_string(), Vec::new()));
        Ok(id)
    }
    async fn upload_part(
        &self,
        _key: &str,
        id: &str,
        number: i32,
        bytes: Bytes,
    ) -> S3Result<String> {
        self.uploads
            .lock()
            .await
            .get_mut(id)
            .ok_or_else(|| S3ProtocolFailure::protocol("missing upload"))?
            .1
            .push((number, bytes));
        Ok(format!("etag-{number}"))
    }
    async fn complete_multipart(
        &self,
        _key: &str,
        id: &str,
        _parts: &[(i32, String)],
    ) -> S3Result<()> {
        let (key, mut parts) = self
            .uploads
            .lock()
            .await
            .remove(id)
            .ok_or_else(|| S3ProtocolFailure::protocol("missing upload"))?;
        parts.sort_by_key(|part| part.0);
        let mut part_digests = Vec::new();
        for part in &parts {
            part_digests.extend_from_slice(&Md5::digest(&part.1));
        }
        let composite = format!("\"{:x}-{}\"", Md5::digest(&part_digests), parts.len());
        let bytes: Vec<u8> = parts.into_iter().flat_map(|part| part.1).collect();
        self.multipart_etags
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(Md5::digest(&bytes).into(), composite);
        self.objects.lock().await.insert(key, Bytes::from(bytes));
        Ok(())
    }
    async fn abort_multipart(&self, _key: &str, id: &str) -> S3Result<()> {
        if let Some(failure) = self.abort_failure.lock().await.clone() {
            return Err(failure);
        }
        self.uploads.lock().await.remove(id);
        *self.aborts.lock().await += 1;
        Ok(())
    }
    async fn list_parts(&self, _key: &str, id: &str) -> S3Result<Vec<S3PartFacts>> {
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
                etag: format!("etag-{number}"),
            })
            .collect())
    }
    async fn copy_object(&self, from: &str, to: &str) -> S3Result<()> {
        let bytes = self
            .objects
            .lock()
            .await
            .get(from)
            .cloned()
            .ok_or_else(|| S3ProtocolFailure::protocol("not found"))?;
        self.objects.lock().await.insert(to.to_string(), bytes);
        if *self.copy_commits_then_fails.lock().await {
            Err(S3ProtocolFailure::session(
                crate::model::FailureClass::Connectivity,
                crate::model::Transience::Transient,
                "copy response lost",
            ))
        } else {
            Ok(())
        }
    }
    async fn native_copy(
        &self,
        source: &S3NativeCopySource,
        to: &str,
        _multipart_upload_id: Option<&str>,
        cancel: &CancellationToken,
    ) -> S3NativeCopyResult {
        if let Some(failure) = self.native_failure.lock().await.clone() {
            return Err(S3NativeCopyFailure {
                error: failure,
                bytes: 0,
                requests: 1,
            });
        }
        if cancel.is_cancelled() {
            return Err(S3NativeCopyFailure {
                error: S3ProtocolFailure::entry(
                    crate::model::FailureClass::Cancelled,
                    crate::model::Transience::Permanent,
                    "native copy cancelled",
                ),
                bytes: 0,
                requests: 0,
            });
        }
        let bytes = self
            .bytes_at(&source.key, source.version_id.as_deref())
            .await
            .map_err(|_| S3NativeCopyFailure {
                error: S3ProtocolFailure::protocol("not found"),
                bytes: 0,
                requests: 1,
            })?;
        if self.etag_for(&bytes) != source.etag || bytes.len() as u64 != source.size {
            return Err(S3NativeCopyFailure {
                error: S3ProtocolFailure::entry(
                    crate::model::FailureClass::Conflict,
                    crate::model::Transience::Permanent,
                    "native source changed",
                ),
                bytes: 0,
                requests: 1,
            });
        }
        self.objects.lock().await.insert(to.to_string(), bytes);
        *self.native_copies.lock().await += 1;
        Ok(S3NativeCopyEvidence {
            bytes: source.size,
            requests: 1,
        })
    }
    async fn delete_object(&self, key: &str) -> S3Result<()> {
        self.objects.lock().await.remove(key);
        Ok(())
    }
    async fn get_tags(&self, key: &str, version_id: Option<&str>) -> S3Result<Vec<ObjectTag>> {
        *self.tag_reads.lock().await += 1;
        self.tag_versions
            .lock()
            .await
            .push(version_id.map(str::to_string));
        Ok(self.tags.lock().await.get(key).cloned().unwrap_or_default())
    }
    async fn put_tags(&self, key: &str, tags: &[ObjectTag]) -> S3Result<()> {
        self.tags
            .lock()
            .await
            .insert(key.to_string(), tags.to_vec());
        Ok(())
    }
}

/// S3 roles that send every object through a multipart upload, however small: for tests of that
/// path with payloads below the single-PUT threshold.
pub(crate) fn connect_multipart_only(
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

pub(crate) fn identity() -> BackendIdentity {
    BackendIdentity::new(BackendKind::S3, "memory-bucket").expect("valid identity")
}

pub(super) fn validation_policy() -> crate::storage::PreflightPolicy {
    crate::storage::PreflightPolicy::production()
}

pub(crate) fn native_context() -> S3NativeContext {
    S3NativeContext::new("memory://s3", "standard", "memory".into(), None)
}

/// A single PUT stores the body and reports what real S3 does: the quoted hex MD5 as `ETag`, and
/// no version on an unversioned bucket.
#[tokio::test]
async fn memory_put_object_reports_md5_etag_and_no_version() {
    let s3 = MemoryS3::default();
    let body = Bytes::from_static(b"payload");
    let facts = s3
        .put_object("key", body.clone(), &content_md5(&body))
        .await
        .expect("put succeeds");
    assert_eq!(facts.etag, "\"321c3cf486ed509164edec1e1981fec8\"");
    assert_eq!(facts.version_id, None);
    let head = s3.head("key").await.expect("object exists");
    assert_eq!(head.etag, facts.etag);
}

/// On a versioned fake each PUT mints a new current version and reports it.
#[tokio::test]
async fn memory_put_object_on_a_versioned_fake_reports_a_new_version() {
    let s3 = MemoryS3::default();
    s3.put_version("key", "v1", Bytes::from_static(b"old"))
        .await;
    let body = Bytes::from_static(b"new");
    let facts = s3
        .put_object("key", body.clone(), &content_md5(&body))
        .await
        .expect("put succeeds");
    let version = facts
        .version_id
        .expect("a versioned write reports its version");
    assert_ne!(version, "v1");
    assert_eq!(s3.version.lock().await.as_deref(), Some(version.as_str()));
}

/// A `Content-MD5` that does not match the body, or the injected corruption, is `BadDigest`: a
/// transient `Corruption` of the entry, storing nothing. The injection covers one PUT only.
#[tokio::test]
async fn memory_put_object_refuses_a_bad_digest() {
    let s3 = MemoryS3::default();
    let body = Bytes::from_static(b"payload");
    let expected = S3ProtocolFailure::corrupted_upload("S3 PutObject request failed");
    let wrong = s3
        .put_object("key", body.clone(), &content_md5(b"other"))
        .await;
    assert_eq!(wrong, Err(expected.clone()));
    *s3.bad_digest_next_put.lock().await = true;
    let injected = s3
        .put_object("key", body.clone(), &content_md5(&body))
        .await;
    assert_eq!(injected, Err(expected));
    assert!(s3.objects.lock().await.is_empty());
    s3.put_object("key", body.clone(), &content_md5(&body))
        .await
        .expect("the injection covers one PUT");
}

/// Two uploads on one key get distinct ids.
#[tokio::test]
async fn memory_upload_ids_do_not_collide_on_one_key() {
    let s3 = MemoryS3::default();
    let first = s3.begin_multipart("key").await.expect("begin");
    let second = s3.begin_multipart("key").await.expect("begin");
    assert_ne!(first, second);
    assert_eq!(s3.uploads.lock().await.len(), 2);
}

#[path = "role_tests.rs"]
mod roles;

#[path = "version_tests.rs"]
mod versions;
