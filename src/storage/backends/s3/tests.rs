//! The in-crate S3 used by every S3 role test: an in-memory bucket with multipart uploads, tags
//! and injectable failures, plus the identity / policy helpers the tests share.

#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
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
    version: Mutex<Option<String>>,
    pub(crate) last_modified: Mutex<Option<crate::model::StorageTimestamp>>,
    range_observations: Mutex<Vec<S3ObjectFacts>>,
    pub(crate) objects: Mutex<HashMap<String, Bytes>>,
    pub(super) uploads: Mutex<UploadParts>,
    tags: Mutex<HashMap<String, Vec<ObjectTag>>>,
    tag_reads: Mutex<u32>,
    pub(super) aborts: Mutex<u32>,
    pub(super) abort_failure: Mutex<Option<S3ProtocolFailure>>,
    head_failure: Mutex<Option<(String, S3ProtocolFailure)>>,
    pub(crate) copy_commits_then_fails: Mutex<bool>,
    pub(crate) native_copies: Mutex<u64>,
    pub(crate) native_failure: Mutex<Option<S3ProtocolFailure>>,
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
            etag: blake3::hash(bytes).to_hex().to_string(),
            version_id: self.version.lock().await.clone(),
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
        let objects = self.objects.lock().await;
        let bytes = objects.get(key).ok_or_else(|| {
            S3ProtocolFailure::entry(
                crate::model::FailureClass::NotFound,
                crate::model::Transience::Permanent,
                "not found",
            )
        })?;
        if observed.etag != blake3::hash(bytes).to_hex().as_str() {
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
    async fn begin_multipart(&self, key: &str) -> S3Result<String> {
        let id = format!("upload-{key}");
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
        let bytes: Vec<u8> = parts.into_iter().flat_map(|part| part.1).collect();
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
            .objects
            .lock()
            .await
            .get(&source.key)
            .cloned()
            .ok_or_else(|| S3NativeCopyFailure {
                error: S3ProtocolFailure::protocol("not found"),
                bytes: 0,
                requests: 1,
            })?;
        if blake3::hash(&bytes).to_hex().as_str() != source.etag
            || bytes.len() as u64 != source.size
        {
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
    async fn get_tags(&self, key: &str) -> S3Result<Vec<ObjectTag>> {
        *self.tag_reads.lock().await += 1;
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

pub(crate) fn identity() -> BackendIdentity {
    BackendIdentity::new(BackendKind::S3, "memory-bucket").expect("valid identity")
}

pub(super) fn validation_policy() -> crate::storage::PreflightPolicy {
    crate::storage::PreflightPolicy::production()
}

pub(crate) fn native_context() -> S3NativeContext {
    S3NativeContext::new("memory://s3", "standard", "memory".into(), None)
}

#[path = "role_tests.rs"]
mod roles;
