use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;

use crate::model::{FailureClass, ObjectTag, Transience};

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
}

pub(crate) type S3Result<T> = Result<T, S3ProtocolFailure>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct S3ObjectFacts {
    pub size: u64,
    pub etag: String,
    pub version_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct S3PartFacts {
    pub number: i32,
    pub size: u64,
    pub etag: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum S3ClaimOutcome {
    Acquired,
    AlreadyOwned,
    Conflict,
}

#[async_trait]
pub(crate) trait S3Protocol: Send + Sync {
    async fn head(&self, key: &str) -> S3Result<S3ObjectFacts>;
    async fn get_range(&self, key: &str, range: Range<u64>) -> S3Result<Bytes>;
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
    async fn delete_object(&self, key: &str) -> S3Result<()>;
    async fn get_tags(&self, key: &str) -> S3Result<Vec<ObjectTag>>;
    async fn put_tags(&self, key: &str, tags: &[ObjectTag]) -> S3Result<()>;
    async fn claim(&self, key: &str, token: [u8; 32]) -> S3Result<S3ClaimOutcome>;
    async fn release_claim(&self, key: &str) -> S3Result<()>;
}
