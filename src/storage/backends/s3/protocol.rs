use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::model::{FailureClass, ObjectTag, StorageTimestamp, Transience};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum S3ProtocolFailure {
    Entry {
        class: FailureClass,
        transience: Transience,
        diagnostic: String,
    },
    Session {
        class: FailureClass,
        transience: Transience,
        diagnostic: String,
    },
}

impl S3ProtocolFailure {
    pub(crate) fn entry(
        class: FailureClass,
        transience: Transience,
        diagnostic: impl Into<String>,
    ) -> Self {
        Self::Entry {
            class,
            transience,
            diagnostic: diagnostic.into(),
        }
    }
    pub(crate) fn session(
        class: FailureClass,
        transience: Transience,
        diagnostic: impl Into<String>,
    ) -> Self {
        Self::Session {
            class,
            transience,
            diagnostic: diagnostic.into(),
        }
    }
    pub(crate) fn protocol(diagnostic: impl Into<String>) -> Self {
        Self::session(FailureClass::Protocol, Transience::Unknown, diagnostic)
    }
    /// The body the server received does not match the `Content-MD5` sent with it (`BadDigest`,
    /// `InvalidDigest`): the upload was corrupted in flight, and resending it may succeed.
    pub(crate) fn corrupted_upload(diagnostic: impl Into<String>) -> Self {
        Self::entry(FailureClass::Corruption, Transience::Transient, diagnostic)
    }
}

pub(crate) type S3Result<T> = Result<T, S3ProtocolFailure>;
pub(crate) const S3_NATIVE_COPY_SINGLE_MAX: u64 = 5 * 1024 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct S3ObjectFacts {
    pub size: u64,
    pub etag: String,
    pub version_id: Option<String>,
    /// The `Last-Modified` the same response gave, so it belongs to this `etag` / `version_id`;
    /// `None` when the server sent none.
    pub last_modified: Option<StorageTimestamp>,
}

/// Whether a reported `versionId` names a real version: not empty and not the literal `"null"`,
/// which an unversioned bucket, or an object written before versioning, reports.
pub(crate) fn is_real_version_id(version: &str) -> bool {
    !version.is_empty() && version != "null"
}

/// What a write that created an object reported about it.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "used by the single PUT, ADR-0006 C14b")
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct S3WriteFacts {
    pub etag: String,
    /// The version the write created; `None` when the response named no real version.
    pub version_id: Option<String>,
}

impl S3WriteFacts {
    /// Keeps `reported_version` only when it is a real version id (see [`is_real_version_id`]).
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "used by the single PUT, ADR-0006 C14b")
    )]
    pub(crate) fn new(etag: String, reported_version: Option<String>) -> Self {
        Self {
            etag,
            version_id: reported_version.filter(|version| is_real_version_id(version)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct S3PartFacts {
    pub number: i32,
    pub size: u64,
    pub etag: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct S3NativeCopySource {
    pub bucket: String,
    pub key: String,
    pub etag: String,
    pub version_id: Option<String>,
    pub size: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct S3NativeCopyEvidence {
    pub bytes: u64,
    pub requests: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct S3NativeCopyFailure {
    pub error: S3ProtocolFailure,
    pub bytes: u64,
    pub requests: u64,
}

pub(crate) type S3NativeCopyResult = Result<S3NativeCopyEvidence, S3NativeCopyFailure>;

#[async_trait]
pub(crate) trait S3Protocol: Send + Sync {
    async fn head(&self, key: &str) -> S3Result<S3ObjectFacts>;
    /// HEAD one stored version. A delete marker, or a version the store does not have, is a
    /// `NotFound` entry failure — a per-entry outcome, never a session failure.
    async fn head_version(&self, key: &str, version_id: &str) -> S3Result<S3ObjectFacts>;
    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
        observed: &S3ObjectFacts,
    ) -> S3Result<Bytes>;
    /// One `PutObject` of `body` to `key`, carrying `content_md5_base64` so the server rejects a
    /// body corrupted in flight (`BadDigest`, a transient `Corruption` entry failure).
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "used by the single PUT, ADR-0006 C14b")
    )]
    async fn put_object(
        &self,
        key: &str,
        body: Bytes,
        content_md5_base64: &str,
    ) -> S3Result<S3WriteFacts>;
    async fn begin_multipart(&self, key: &str) -> S3Result<String>;
    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        bytes: Bytes,
    ) -> S3Result<String>;
    async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[(i32, String)],
    ) -> S3Result<()>;
    async fn abort_multipart(&self, key: &str, upload_id: &str) -> S3Result<()>;
    async fn list_parts(&self, key: &str, upload_id: &str) -> S3Result<Vec<S3PartFacts>>;
    async fn copy_object(&self, from: &str, to: &str) -> S3Result<()>;
    async fn native_copy(
        &self,
        source: &S3NativeCopySource,
        to: &str,
        multipart_upload_id: Option<&str>,
        cancel: &CancellationToken,
    ) -> S3NativeCopyResult;
    async fn delete_object(&self, key: &str) -> S3Result<()>;
    /// Tags of the current object, or of one stored version.
    async fn get_tags(&self, key: &str, version_id: Option<&str>) -> S3Result<Vec<ObjectTag>>;
    async fn put_tags(&self, key: &str, tags: &[ObjectTag]) -> S3Result<()>;
}
