use std::ops::Range;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use md5::{Digest as _, Md5};
use tokio::sync::mpsc;

use super::S3BucketCapabilities;
use crate::model::{FailureClass, ObjectTag, StorageTimestamp, Transience};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum S3ProtocolFailure {
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
    pub fn entry(
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
    pub fn session(
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
    pub fn protocol(diagnostic: impl Into<String>) -> Self {
        Self::session(FailureClass::Protocol, Transience::Unknown, diagnostic)
    }
    /// The body the server received does not match the `Content-MD5` sent with it (`BadDigest`,
    /// `InvalidDigest`): the upload was corrupted in flight, and resending it may succeed.
    pub fn corrupted_upload(diagnostic: impl Into<String>) -> Self {
        Self::entry(FailureClass::Corruption, Transience::Transient, diagnostic)
    }
}

pub type S3Result<T> = Result<T, S3ProtocolFailure>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3ObjectFacts {
    pub size: u64,
    pub etag: String,
    pub version_id: Option<String>,
    /// The `Last-Modified` the same response gave, so it belongs to this `etag` / `version_id`;
    /// `None` when the server sent none.
    pub last_modified: Option<StorageTimestamp>,
}

/// Whether a reported `versionId` names a real version: not empty and not the literal `"null"`,
/// which an unversioned bucket, or an object written before versioning, reports.
#[must_use]
pub fn is_real_version_id(version: &str) -> bool {
    !version.is_empty() && version != "null"
}

/// What a write that created an object reported about it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3WriteFacts {
    pub etag: String,
    /// The version the write created; `None` when the response named no real version.
    pub version_id: Option<String>,
    /// The version id exactly as the response spelled it — `"null"` included, which a bucket with
    /// versioning suspended reports — or `None` when it named none (an unversioned bucket). In a
    /// bucket that was suspended when connected, a reply naming none is taken as `"null"` (`RustFS`
    /// 1.0.0 omits the header; see `S3Storage::write_facts`). A delete of exactly this object
    /// names it ([`S3Protocol::delete_version`]).
    pub reported_version: Option<String>,
}

impl S3WriteFacts {
    /// Keeps `reported_version` as the object's version only when it is a real version id (see
    /// [`is_real_version_id`]).
    #[must_use]
    pub fn new(etag: String, reported_version: Option<String>) -> Self {
        Self {
            etag,
            version_id: reported_version
                .clone()
                .filter(|version| is_real_version_id(version)),
            reported_version,
        }
    }
}

/// What a streamed `PutObject` reported ([`S3Protocol::put_object_streaming`]).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3StreamedPut {
    pub facts: S3WriteFacts,
    /// Whether the reply's `ETag` can be the MD5 of the body: not under server-side encryption
    /// with a KMS or customer key, whose `ETag`s are not MD5s.
    pub etag_is_md5: bool,
}

/// One stored version, or delete marker, of a key as `ListObjectVersions` reports it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3VersionFacts {
    /// As the listing spells it: `"null"` for the object an unversioned (or suspended) bucket
    /// holds.
    pub version_id: String,
    pub is_latest: bool,
    pub delete_marker: bool,
    /// Zero for a delete marker.
    pub size: u64,
    /// Empty for a delete marker.
    pub etag: String,
}

/// One current object of a delimiter listing (`ListObjectsV2`). Keys are relative to the storage
/// root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3ListedObject {
    pub key: String,
    pub size: u64,
    pub etag: String,
    /// The listing's `LastModified` (millisecond precision); `None` when it gave none.
    pub last_modified: Option<StorageTimestamp>,
}

/// A listed element a page could not decode: an object without a valid size, or a key (or common
/// prefix) outside the storage root. Reported as that one child's failure, beside its siblings;
/// it never fails the page or the session (P7 §8).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3RejectedEntry {
    /// The key relative to the storage root; `None` when the server sent no key, or one outside
    /// the root.
    pub key: Option<String>,
    /// The key or prefix exactly as the server sent it (empty when it sent none).
    pub raw_key: String,
    pub reason: &'static str,
}

/// One page of a delimiter listing of current objects under one prefix.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct S3ObjectPage {
    pub objects: Vec<S3ListedObject>,
    /// Common prefixes, relative to the storage root, each ending in `/`.
    pub prefixes: Vec<String>,
    /// Elements the page could not decode.
    pub rejected: Vec<S3RejectedEntry>,
    /// The continuation token of the next page, passed back as is; `None` on the last page.
    pub next: Option<String>,
}

/// One version or delete marker of a delimiter listing of versions (`ListObjectVersions`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3ListedVersion {
    pub key: String,
    pub facts: S3VersionFacts,
    /// The listing's `LastModified` (millisecond precision); `None` when it gave none.
    pub last_modified: Option<StorageTimestamp>,
    /// Where the server placed this entry in its response: the index among the page's
    /// `<Version>` and `<DeleteMarker>` elements counted together, in document order. `None`
    /// when the response's document order could not be read (the key's history is then
    /// inferred from `LastModified`).
    pub position: Option<u64>,
}

/// Where the next page of a versions listing starts. Both values are opaque server tokens, passed
/// back as is: `MinIO` answers `NextKeyMarker` with a token such as `p/z[minio_cache:v2,return:]`,
/// which is not a key and must never be compared with one or have the storage prefix removed.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct S3VersionMarker {
    pub key_marker: String,
    pub version_id_marker: Option<String>,
}

/// One page of a delimiter listing of versions under one prefix. Versions and delete markers are
/// each in listing order (keys ascending, each key's entries newest first); the SDK parses them
/// apart, so how the two interleave is carried by each entry's `position` (the server's document
/// order), read from the raw response.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct S3VersionPage {
    pub versions: Vec<S3ListedVersion>,
    pub markers: Vec<S3ListedVersion>,
    /// Common prefixes, relative to the storage root, each ending in `/`.
    pub prefixes: Vec<String>,
    /// Versions, delete markers and prefixes the page could not decode.
    pub rejected: Vec<S3RejectedEntry>,
    /// `None` on the last page.
    pub next: Option<S3VersionMarker>,
}

/// The `ETag` S3 gives an object completed from parts whose `ETag`s are `part_etags`, in part
/// order: the quoted hex MD5 of the concatenated binary part MD5s, a dash and the part count
/// (`"<md5>-<n>"`). `None` when there are no parts or any part `ETag` is not a quoted 32-hex MD5
/// (server-side encryption with KMS and some stores report other values): then no composite can
/// be computed.
#[must_use]
pub fn composite_etag(part_etags: &[String]) -> Option<String> {
    if part_etags.is_empty() {
        return None;
    }
    let mut digests = Vec::with_capacity(part_etags.len() * 16);
    for etag in part_etags {
        digests.extend_from_slice(&md5_of_etag(etag)?);
    }
    Some(format!(
        "\"{:x}-{}\"",
        Md5::digest(&digests),
        part_etags.len()
    ))
}

/// The binary MD5 a quoted 32-hex `ETag` spells; `None` for any other shape.
fn md5_of_etag(etag: &str) -> Option<[u8; 16]> {
    let hex = etag.strip_prefix('"')?.strip_suffix('"')?;
    if hex.len() != 32 {
        return None;
    }
    let mut digest = [0_u8; 16];
    for (index, byte) in digest.iter_mut().enumerate() {
        let pair = hex.get(index * 2..index * 2 + 2)?;
        if !pair.bytes().all(|digit| digit.is_ascii_hexdigit()) {
            return None;
        }
        *byte = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(digest)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3PartFacts {
    pub number: i32,
    pub size: u64,
    pub etag: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3NativeCopySource {
    pub bucket: String,
    pub key: String,
    pub etag: String,
    pub version_id: Option<String>,
    pub size: u64,
}

/// What `GetBucketVersioning` answered when the bucket was connected (P7 §10.1, Q26, Q-A): it
/// decides, once per connection, whether a listing gives each key its history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum S3VersioningAtConnect {
    /// `Enabled`.
    Enabled,
    /// `Suspended`: the histories written while it was enabled stay.
    Suspended,
    /// Never enabled: the answer names no status.
    Never,
    /// The store has no versioning at all (501, `NotImplemented`, `UnsupportedOperation`).
    NoVersioning,
    /// The server refused to say (403, or another explicit refusal), answered with a status this
    /// build does not know, or the request could not be built (it then fails again when the
    /// bucket is listed, with that request's own failure).
    Refused,
    /// No answer after the SDK's retries: a timeout, a lost connection (`status` `None`),
    /// throttling or a server error (`status` the HTTP status).
    Unreachable { status: Option<u16> },
}

impl S3VersioningAtConnect {
    /// Whether the bucket's listings give each key its history: `Enabled`, `Suspended`, and a
    /// refusal to say (Q26: `ListObjectVersions` answers an unversioned bucket with one `"null"`
    /// record per key — correct, only slower).
    #[must_use]
    pub const fn keeps_histories(self) -> bool {
        matches!(self, Self::Enabled | Self::Suspended | Self::Refused)
    }
}

/// A bucket's versioning as `GetBucketVersioning` answers it now (P7 H1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum S3BucketVersioning {
    Enabled,
    Suspended,
    /// Never enabled, or a store without versioning.
    Off,
}

/// What a `DeleteObject` without a version id replied (P7 H1).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct S3DeleteReply {
    /// `x-amz-delete-marker: true`: the reply names a delete marker.
    pub delete_marker: bool,
    /// `x-amz-version-id`, as spelled (`"null"` included).
    pub version_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct S3NativeCopyEvidence {
    pub bytes: u64,
    pub requests: u64,
}

#[async_trait]
pub trait S3Protocol: Send + Sync {
    fn read_concurrency(&self) -> usize {
        super::DEFAULT_READ_CONCURRENCY
    }

    fn write_concurrency(&self) -> usize {
        super::DEFAULT_WRITE_CONCURRENCY
    }

    fn maximum_read_chunk_bytes(&self) -> usize {
        8 * 1024 * 1024
    }
    /// Whether the bucket had versioning enabled when it was connected (`GetBucketVersioning`
    /// answered `Enabled`). A write there whose reply names no version is settled
    /// through the key's versions (ADR-0006 C18: DXN's `CopyObject` sends no `x-amz-version-id`).
    fn versioning_enabled(&self) -> bool;
    /// What `GetBucketVersioning` answered when the bucket was connected (P7 Q-A); fixed for the
    /// connection.
    fn versioning_at_connect(&self) -> S3VersioningAtConnect;
    /// Whether a listing gives each key its whole history (P7 §10.1, Q26): the bucket's profile
    /// keeps versions (never an ONTAP NAS bucket) and the answer at connection says it keeps
    /// histories ([`S3VersioningAtConnect::keeps_histories`]). Fixed for the connection.
    fn lists_histories(&self) -> bool {
        self.bucket_capabilities().versions && self.versioning_at_connect().keeps_histories()
    }
    /// What the bucket can do, from the URL's profile (P7 §14.2); fixed for the connection.
    fn bucket_capabilities(&self) -> S3BucketCapabilities;
    /// Checks bucket access independently of whether the configured prefix contains objects.
    async fn head_bucket(&self) -> S3Result<()> {
        Err(S3ProtocolFailure::entry(
            FailureClass::Unsupported,
            Transience::Permanent,
            "bucket connectivity probe is unsupported by this protocol",
        ))
    }
    async fn head(&self, key: &str) -> S3Result<S3ObjectFacts>;
    /// HEAD one stored version. A delete marker, or a version the store does not have, is a
    /// `NotFound` entry failure — a per-entry outcome, never a session failure.
    async fn head_version(&self, key: &str, version_id: &str) -> S3Result<S3ObjectFacts>;
    /// The bytes `range` of the object `observed` describes. Pinned by the store where the bucket
    /// has conditional reads (`?versionId=` when `observed` names a version, `If-Match` on its
    /// `ETag`: a replaced object answers 412, a permanent `Conflict`). Without them no condition is
    /// sent and the response is checked instead: its `ETag` must be `observed.etag`, its
    /// `Content-Range` total `observed.size` and its `Last-Modified` `observed.last_modified` when
    /// both have one — any difference is a permanent `Conflict`. A body that then breaks off (short
    /// of what it announced, or the connection reset) is followed by one `HEAD`: another object is
    /// a permanent `Conflict` (an ONTAP NAS bucket cuts a GET short once a replacement commits, P7
    /// §14.2 N0), the described one a transient lost connection, and a failed `HEAD` its own
    /// failure ([`super::broken_body_failure`]).
    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
        observed: &S3ObjectFacts,
    ) -> S3Result<Bytes>;
    /// Exactly one GET attempt. The source owns `QoS` retries and charges every attempt;
    /// SDK implementations must disable their internal retry loop for this operation.
    async fn get_range_once(
        &self,
        key: &str,
        range: Range<u64>,
        observed: &S3ObjectFacts,
    ) -> S3Result<Bytes> {
        self.get_range(key, range, observed).await
    }
    /// One `PutObject` of `body` to `key`, carrying `content_md5_base64` so the server rejects a
    /// body corrupted in flight (`BadDigest`, a transient `Corruption` entry failure).
    async fn put_object(
        &self,
        key: &str,
        body: super::S3Body,
        content_md5_base64: &str,
    ) -> S3Result<S3WriteFacts>;
    /// How long a streamed upload may make no progress — a chunk not taken, a reply not given —
    /// before it is abandoned: the connection's read timeout.
    fn stall_timeout(&self) -> Duration;
    /// One `PutObject` of exactly `len` bytes read from `chunks` as they come (P7 §14.2 N3):
    /// `Content-Length: len`, the payload unsigned (`UNSIGNED-PAYLOAD`), no `Content-MD5`, no
    /// retry, and no timeout of its own — the caller holds bytes back as long as it likes and
    /// watches the progress itself. The object commits only once all `len` bytes arrived: a
    /// sender dropped before that aborts the request (the connection closes) and nothing is
    /// stored. On a bucket whose keys are file paths, a file / directory clash on the key's path
    /// is that entry's permanent `Conflict`.
    async fn put_object_streaming(
        &self,
        key: &str,
        len: u64,
        chunks: mpsc::Receiver<Bytes>,
    ) -> S3Result<S3StreamedPut>;
    async fn begin_multipart(&self, key: &str) -> S3Result<String>;
    /// Uploads one part carrying `content_md5_base64`, so the server rejects a part corrupted in
    /// flight (`BadDigest`, a transient `Corruption` entry failure). Returns the part's `ETag`.
    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        bytes: super::S3Body,
        content_md5_base64: &str,
    ) -> S3Result<String>;
    /// Completes an upload from `parts` (number, `ETag`), in ascending part order. A part the
    /// upload does not hold, or a list out of order, is a permanent `Conflict`
    /// (`InvalidPart` / `InvalidPartOrder`); a part other than the last below the minimum part
    /// size is a permanent `Corruption` (`EntityTooSmall`). Reports the object's `ETag` and the
    /// version the completion created.
    async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[(i32, String)],
    ) -> S3Result<S3WriteFacts>;
    async fn abort_multipart(&self, key: &str, upload_id: &str) -> S3Result<()>;
    async fn list_parts(&self, key: &str, upload_id: &str) -> S3Result<Vec<S3PartFacts>>;
    /// The ids of the multipart uploads in progress on exactly `key` — not on keys that merely
    /// start with it (some stores list by prefix, `MinIO` by exact key).
    async fn list_uploads(&self, key: &str) -> S3Result<Vec<String>>;
    /// One `CopyObject` of `source` to `to` (ADR-0006 C18), pinned by
    /// `x-amz-copy-source-if-match` to its `ETag` and, when it names one, to its version
    /// (`?versionId=`). Reports the new object's `ETag` and version. A source that changed is a
    /// permanent `Conflict` (412), one that is gone a `NotFound`.
    async fn copy_from(&self, source: &S3NativeCopySource, to: &str) -> S3Result<S3WriteFacts>;
    /// One `UploadPartCopy` of the bytes `range` of `source`, pinned as [`Self::copy_from`] pins
    /// it, as part `part_number` of upload `upload_id` on `key`. Returns the part's `ETag`.
    async fn upload_part_copy(
        &self,
        source: &S3NativeCopySource,
        key: &str,
        upload_id: &str,
        part_number: i32,
        range: Range<u64>,
    ) -> S3Result<String>;
    /// An unversioned `DeleteObject`: in a versioned bucket it adds a delete marker and keeps every
    /// version. Transfer recovery uses it only for pointers; namespace deletion also uses it for
    /// final keys explicitly selected by the caller.
    async fn delete_object(&self, key: &str) -> S3Result<()>;
    /// Deletes at most 100 current keys; result order matches input. History remains intact.
    async fn delete_objects(&self, keys: &[String]) -> Vec<S3Result<()>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.delete_object(key).await);
        }
        results
    }
    fn namespace_rename_support(&self) -> Result<(), &'static str> {
        Ok(())
    }
    /// Renames one current object using server-side copy, preserving its metadata and tags.
    /// A failed copy preserves the source; source deletion failure leaves both keys.
    async fn rename_object(&self, _from: &str, _to: &str) -> S3Result<()> {
        Err(S3ProtocolFailure::entry(
            FailureClass::Unsupported,
            Transience::Permanent,
            "object rename is unsupported by this protocol",
        ))
    }

    /// Deletes one stored version (or delete marker) of `key` for good, adding no delete marker.
    /// A version the store does not hold is not an error (S3 answers 204). A version Object Lock
    /// protects is a permanent `PermissionDenied` entry failure (AWS answers 403 `AccessDenied`,
    /// `MinIO` 400 `InvalidRequest`).
    async fn delete_version(&self, key: &str, version_id: &str) -> S3Result<()>;
    /// `GetBucketVersioning` asked now, not the state at connection (P7 H1: a delete marker is
    /// written only while the bucket keeps every record).
    async fn bucket_versioning(&self) -> S3Result<S3BucketVersioning>;
    /// An unversioned `DeleteObject` of `key` sent on purpose to write a delete marker on a final
    /// key (P7 H1), with what the reply names.
    async fn write_delete_marker(&self, key: &str) -> S3Result<S3DeleteReply>;
    /// Every stored version and delete marker of exactly `key` — not of keys that merely start
    /// with it — in no guaranteed order: `is_latest` marks the current entry. An unversioned
    /// bucket lists its object as version `"null"`.
    async fn list_versions(&self, key: &str) -> S3Result<Vec<S3VersionFacts>>;
    /// One page of `ListObjectsV2` with delimiter `/` under `prefix` (relative to the storage root;
    /// empty, or ending in `/`), continuing from `token`.
    async fn list_objects_page(&self, prefix: &str, token: Option<&str>) -> S3Result<S3ObjectPage>;
    /// One page of `ListObjectVersions` with delimiter `/` under `prefix`, continuing from
    /// `marker`. A page can end inside one key's entries.
    async fn list_versions_page(
        &self,
        prefix: &str,
        marker: Option<&S3VersionMarker>,
    ) -> S3Result<S3VersionPage>;
    /// Tags of the current object, or of one stored version.
    async fn get_tags(&self, key: &str, version_id: Option<&str>) -> S3Result<Vec<ObjectTag>>;
    async fn put_tags(&self, key: &str, tags: &[ObjectTag]) -> S3Result<()>;
}
