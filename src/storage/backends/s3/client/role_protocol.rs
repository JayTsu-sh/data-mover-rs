use super::{CompletedPart, ProvideErrorMetadata, S3Storage, build_copy_source};
use std::time::Duration;

use aws_smithy_types::byte_stream::AggregatedBytes;
use bytes::Bytes;

use crate::model::{FailureClass, Transience};
use crate::storage::backends::s3::{
    S3NativeCopySource, S3ObjectPage, S3ProtocolFailure, S3Result, S3VersionFacts, S3VersionMarker,
    S3VersionPage, S3WriteFacts,
};
use crate::time_util::http_last_modified;

macro_rules! classify_sdk {
    ($error:expr, $diagnostic:literal) => {
        if matches!(
            &$error,
            aws_smithy_runtime_api::client::result::SdkError::TimeoutError(_)
                | aws_smithy_runtime_api::client::result::SdkError::DispatchFailure(_)
        ) {
            s3_role_transport_failure($diagnostic)
        } else {
            s3_role_remote_failure(
                $error
                    .raw_response()
                    .map(|response| response.status().as_u16()),
                $error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code),
                $diagnostic,
            )
        }
    };
}

mod delete_batch;
mod delete_marker;
mod hints;
mod listing;
mod multipart;
mod native;
pub(super) mod rename;
mod streaming_put;
#[cfg(test)]
mod throttling_tests;
mod unpinned;
#[cfg(test)]
mod unpinned_wire_tests;
mod versions;
#[cfg(test)]
mod write_facts_tests;

fn decode_parts(
    parts: &[aws_sdk_s3::types::Part],
) -> crate::storage::backends::s3::S3Result<Vec<crate::storage::backends::s3::S3PartFacts>> {
    parts
        .iter()
        .map(|part| {
            Ok(crate::storage::backends::s3::S3PartFacts {
                number: part.part_number().ok_or_else(|| {
                    crate::storage::backends::s3::S3ProtocolFailure::protocol(
                        "S3 part is missing its number",
                    )
                })?,
                size: part
                    .size()
                    .and_then(|size| u64::try_from(size).ok())
                    .ok_or_else(|| {
                        crate::storage::backends::s3::S3ProtocolFailure::protocol(
                            "S3 part has invalid size",
                        )
                    })?,
                etag: part
                    .e_tag()
                    .ok_or_else(|| {
                        crate::storage::backends::s3::S3ProtocolFailure::protocol(
                            "S3 part is missing its ETag",
                        )
                    })?
                    .to_string(),
            })
        })
        .collect()
}

fn continuation_marker(
    response: &aws_sdk_s3::operation::list_parts::ListPartsOutput,
    previous: Option<&str>,
) -> crate::storage::backends::s3::S3Result<Option<String>> {
    if response.is_truncated() != Some(true) {
        return Ok(None);
    }
    let next = response.next_part_number_marker().ok_or_else(|| {
        crate::storage::backends::s3::S3ProtocolFailure::protocol(
            "truncated S3 ListParts response has no continuation marker",
        )
    })?;
    if previous == Some(next) {
        return Err(crate::storage::backends::s3::S3ProtocolFailure::protocol(
            "S3 ListParts continuation marker did not advance",
        ));
    }
    Ok(Some(next.to_string()))
}

impl S3Storage {
    /// What a write reported: its `ETag` and its version as spelled — `"null"` when the reply
    /// named none in a bucket with versioning suspended. Every write there creates the `"null"`
    /// version; AWS and `MinIO` say so (`x-amz-version-id: null`), `RustFS` 1.0.0 leaves the header
    /// out, and a pointer whose `"null"` version goes unnamed is deleted with a plain `DELETE`,
    /// which in a suspended bucket leaves a `"null"` delete marker behind.
    pub(super) fn write_facts(&self, etag: &str, version: Option<&str>) -> S3WriteFacts {
        let reported = version
            .map(str::to_string)
            .or_else(|| self.versioning_suspended.then(|| "null".to_string()));
        S3WriteFacts::new(etag.to_string(), reported)
    }

    /// HEAD the current object, or one stored version of it.
    async fn head_object(
        &self,
        key: &str,
        version_id: Option<&str>,
    ) -> crate::storage::backends::s3::S3Result<crate::storage::backends::s3::S3ObjectFacts> {
        let full_key = self.build_full_key(key);
        let response = self
            .client
            .head_object()
            .bucket(&self.bucket_name)
            .key(&full_key)
            .set_version_id(version_id.map(str::to_string))
            .send()
            .await
            .map_err(|error| {
                let failure = classify_sdk!(error, "S3 HeadObject request failed");
                if version_id.is_some() {
                    versioned_failure(
                        error.raw_response().map(|r| r.status().as_u16()),
                        error
                            .as_service_error()
                            .and_then(ProvideErrorMetadata::code),
                        failure,
                    )
                } else {
                    failure
                }
            })?;
        let size = response
            .content_length()
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(|| {
                s3_role_entry(
                    crate::model::FailureClass::Corruption,
                    "S3 HeadObject returned invalid content length",
                )
            })?;
        Ok(crate::storage::backends::s3::S3ObjectFacts {
            size,
            etag: response.e_tag().unwrap_or_default().to_string(),
            version_id: response.version_id().map(str::to_string),
            last_modified: response
                .last_modified()
                .and_then(|time| http_last_modified(time.secs())),
        })
    }
}

#[async_trait::async_trait]
impl crate::storage::backends::s3::S3Protocol for S3Storage {
    fn read_concurrency(&self) -> usize {
        self.transfer_concurrency.read()
    }

    fn write_concurrency(&self) -> usize {
        self.transfer_concurrency.write()
    }

    fn maximum_read_chunk_bytes(&self) -> usize {
        self.read_chunk_bytes
    }
    fn versioning_enabled(&self) -> bool {
        self.is_bucket_versioned
    }

    fn versioning_at_connect(&self) -> crate::storage::backends::s3::S3VersioningAtConnect {
        self.versioning_at_connect
    }

    fn bucket_capabilities(&self) -> crate::storage::backends::s3::S3BucketCapabilities {
        super::architecture::bucket_capabilities(self.options)
    }

    async fn head_bucket(&self) -> S3Result<()> {
        self.client
            .head_bucket()
            .bucket(&self.bucket_name)
            .send()
            .await
            .map_err(|e| classify_sdk!(e, "S3 connectivity HeadBucket failed"))?;
        Ok(())
    }

    async fn head(
        &self,
        key: &str,
    ) -> crate::storage::backends::s3::S3Result<crate::storage::backends::s3::S3ObjectFacts> {
        self.head_object(key, None).await
    }

    async fn head_version(
        &self,
        key: &str,
        version_id: &str,
    ) -> crate::storage::backends::s3::S3Result<crate::storage::backends::s3::S3ObjectFacts> {
        self.head_object(key, Some(version_id)).await
    }

    async fn get_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
        observed: &crate::storage::backends::s3::S3ObjectFacts,
    ) -> crate::storage::backends::s3::S3Result<Bytes> {
        if range.start == range.end {
            return Ok(Bytes::new());
        }
        let pinned = self.bucket_capabilities().conditional_reads;
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(self.build_full_key(key))
            .range(format!("bytes={}-{}", range.start, range.end - 1))
            .set_version_id(observed.version_id.clone())
            .set_if_match(pinned.then(|| observed.etag.clone()))
            .send()
            .await
            .map_err(|error| {
                let failure = hints::not_implemented(
                    self.options,
                    classify_sdk!(error, "S3 GetObject range request failed"),
                );
                if observed.version_id.is_some() {
                    versioned_failure(
                        error.raw_response().map(|r| r.status().as_u16()),
                        error
                            .as_service_error()
                            .and_then(ProvideErrorMetadata::code),
                        failure,
                    )
                } else {
                    failure
                }
            })?;
        if !pinned {
            return unpinned::read(response, range.end - range.start, observed, || {
                self.head_object(key, None)
            })
            .await;
        }
        response
            .body
            .collect()
            .await
            .map(AggregatedBytes::into_bytes)
            .map_err(|error| s3_role_session(error.to_string()))
    }

    async fn put_object(
        &self,
        key: &str,
        body: crate::storage::backends::s3::S3Body,
        content_md5_base64: &str,
    ) -> S3Result<S3WriteFacts> {
        streaming_put::put_object(self, key, body, content_md5_base64).await
    }

    fn stall_timeout(&self) -> Duration {
        super::s3_timeout_config()
            .read_timeout()
            .unwrap_or(Duration::from_secs(super::DEFAULT_S3_READ_TIMEOUT_SECS))
    }

    async fn put_object_streaming(
        &self,
        key: &str,
        len: u64,
        chunks: tokio::sync::mpsc::Receiver<Bytes>,
    ) -> S3Result<crate::storage::backends::s3::S3StreamedPut> {
        streaming_put::put_object_streaming(self, key, len, chunks).await
    }

    async fn begin_multipart(&self, key: &str) -> crate::storage::backends::s3::S3Result<String> {
        let full_key = self.build_full_key(key);
        if full_key.ends_with('/') {
            // P7 D13: a directory object is written only by one request. The destination never
            // asks for this; refused here unsent, since the stores differ (MinIO refuses such an
            // upload, RustFS stores a completed one under the key without its '/').
            return Err(s3_role_entry(
                FailureClass::Unsupported,
                "an S3 directory object (a key ending in '/') is never a multipart upload",
            ));
        }
        self.client
            .create_multipart_upload()
            .bucket(&self.bucket_name)
            .key(full_key)
            .send()
            .await
            .map_err(|error| {
                hints::not_implemented(
                    self.options,
                    classify_sdk!(error, "S3 CreateMultipartUpload request failed"),
                )
            })?
            .upload_id()
            .map(str::to_string)
            .ok_or_else(|| {
                s3_role_entry(
                    crate::model::FailureClass::Corruption,
                    "S3 CreateMultipartUpload response omitted upload identity",
                )
            })
    }

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        bytes: crate::storage::backends::s3::S3Body,
        content_md5_base64: &str,
    ) -> S3Result<String> {
        self.role_upload_part(key, upload_id, part_number, bytes, content_md5_base64)
            .await
    }

    async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[(i32, String)],
    ) -> S3Result<S3WriteFacts> {
        // The request that stores the object: never on a directory object (P7 D13), even for an
        // upload this process did not begin.
        if self.build_full_key(key).ends_with('/') {
            return Err(s3_role_entry(
                FailureClass::Unsupported,
                "an S3 directory object (a key ending in '/') is never a multipart upload",
            ));
        }
        self.role_complete_multipart(key, upload_id, parts).await
    }

    async fn list_uploads(&self, key: &str) -> S3Result<Vec<String>> {
        self.role_list_uploads(key).await
    }

    async fn abort_multipart(
        &self,
        key: &str,
        upload_id: &str,
    ) -> crate::storage::backends::s3::S3Result<()> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket_name)
            .key(self.build_full_key(key))
            .upload_id(upload_id)
            .send()
            .await
            .map(|_| ())
            .map_err(|error| classify_sdk!(error, "S3 AbortMultipartUpload request failed"))
    }

    async fn list_parts(
        &self,
        key: &str,
        upload_id: &str,
    ) -> crate::storage::backends::s3::S3Result<Vec<crate::storage::backends::s3::S3PartFacts>>
    {
        let key = self.build_full_key(key);
        let mut marker = None;
        let mut parts = Vec::new();
        loop {
            let response = self
                .client
                .list_parts()
                .bucket(&self.bucket_name)
                .key(&key)
                .upload_id(upload_id)
                .set_part_number_marker(marker.clone())
                .send()
                .await
                .map_err(|error| {
                    hints::denied_listing(
                        self.options,
                        classify_sdk!(error, "S3 ListParts request failed"),
                    )
                })?;
            parts.extend(decode_parts(response.parts())?);
            let Some(next) = continuation_marker(&response, marker.as_deref())? else {
                return Ok(parts);
            };
            marker = Some(next);
        }
    }

    async fn copy_from(&self, source: &S3NativeCopySource, to: &str) -> S3Result<S3WriteFacts> {
        native::copy_from(self, source, to).await
    }

    async fn upload_part_copy(
        &self,
        source: &S3NativeCopySource,
        key: &str,
        upload_id: &str,
        part_number: i32,
        range: std::ops::Range<u64>,
    ) -> S3Result<String> {
        native::upload_part_copy(self, source, key, upload_id, part_number, range).await
    }

    async fn delete_objects(&self, keys: &[String]) -> Vec<S3Result<()>> {
        delete_batch::delete(self, keys).await
    }

    fn namespace_rename_support(&self) -> Result<(), &'static str> {
        if self.options.compat == super::S3Compatibility::Dxn {
            Err("DXN namespace rename is not supported by the verified profile")
        } else {
            Ok(())
        }
    }

    async fn rename_object(&self, from: &str, to: &str) -> S3Result<()> {
        rename::rename(self, from, to).await
    }

    async fn delete_object(&self, key: &str) -> crate::storage::backends::s3::S3Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(self.build_full_key(key))
            .send()
            .await
            .map(|_| ())
            .map_err(|error| classify_sdk!(error, "S3 DeleteObject request failed"))
    }

    async fn delete_version(&self, key: &str, version_id: &str) -> S3Result<()> {
        self.role_delete_version(key, version_id).await
    }

    async fn bucket_versioning(
        &self,
    ) -> S3Result<crate::storage::backends::s3::S3BucketVersioning> {
        self.role_bucket_versioning().await
    }

    async fn write_delete_marker(
        &self,
        key: &str,
    ) -> S3Result<crate::storage::backends::s3::S3DeleteReply> {
        self.role_write_delete_marker(key).await
    }

    async fn list_versions(&self, key: &str) -> S3Result<Vec<S3VersionFacts>> {
        self.role_list_versions(key).await
    }

    async fn list_objects_page(&self, prefix: &str, token: Option<&str>) -> S3Result<S3ObjectPage> {
        self.role_list_objects_page(prefix, token).await
    }

    async fn list_versions_page(
        &self,
        prefix: &str,
        marker: Option<&S3VersionMarker>,
    ) -> S3Result<S3VersionPage> {
        self.role_list_versions_page(prefix, marker).await
    }

    async fn get_tags(
        &self,
        key: &str,
        version_id: Option<&str>,
    ) -> crate::storage::backends::s3::S3Result<Vec<crate::model::ObjectTag>> {
        let response = self
            .client
            .get_object_tagging()
            .bucket(&self.bucket_name)
            .key(self.build_full_key(key))
            .set_version_id(version_id.map(str::to_string))
            .send()
            .await
            .map_err(|error| classify_sdk!(error, "S3 GetObjectTagging request failed"))?;
        response
            .tag_set()
            .iter()
            .map(|tag| {
                crate::model::ObjectTag::new(tag.key(), tag.value()).map_err(|e| {
                    s3_role_entry(crate::model::FailureClass::Corruption, &e.to_string())
                })
            })
            .collect()
    }

    async fn put_tags(
        &self,
        key: &str,
        tags: &[crate::model::ObjectTag],
    ) -> crate::storage::backends::s3::S3Result<()> {
        let tag_set = tags
            .iter()
            .map(|tag| {
                aws_sdk_s3::types::Tag::builder()
                    .key(tag.key())
                    .value(tag.value())
                    .build()
                    .map_err(|error| {
                        s3_role_entry(crate::model::FailureClass::InvalidInput, &error.to_string())
                    })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let tagging = aws_sdk_s3::types::Tagging::builder()
            .set_tag_set(Some(tag_set))
            .build()
            .map_err(|error| {
                s3_role_entry(crate::model::FailureClass::InvalidInput, &error.to_string())
            })?;
        self.client
            .put_object_tagging()
            .bucket(&self.bucket_name)
            .key(self.build_full_key(key))
            .tagging(tagging)
            .send()
            .await
            .map(|_| ())
            .map_err(|error| classify_sdk!(error, "S3 PutObjectTagging request failed"))
    }
}

/// Remaps the failure of a request that named a version. S3 answers a request for a delete marker
/// with 405, and a malformed version id with 400 `InvalidArgument`; both concern this one entry,
/// where the generic mapping would call them session-wide protocol failures. Only versioned
/// requests are remapped — some S3-compatible stores send 405 for operations they do not support —
/// and a 400 only with that code: S3 also answers 400 for an expired token or a wrong region, which
/// concern the whole session. A HEAD carries no code, so its 400 keeps the generic mapping.
fn versioned_failure(
    status: Option<u16>,
    code: Option<&str>,
    failure: S3ProtocolFailure,
) -> S3ProtocolFailure {
    match (status, code) {
        (Some(405), _) => s3_role_entry(
            crate::model::FailureClass::NotFound,
            "the S3 version is a delete marker",
        ),
        (Some(400), Some("InvalidArgument")) => s3_role_entry(
            crate::model::FailureClass::InvalidInput,
            "the S3 version id is invalid",
        ),
        _ => failure,
    }
}

fn s3_role_session(diagnostic: String) -> crate::storage::backends::s3::S3ProtocolFailure {
    crate::storage::backends::s3::S3ProtocolFailure::session(
        crate::model::FailureClass::Connectivity,
        crate::model::Transience::Transient,
        diagnostic,
    )
}

fn s3_role_transport_failure(
    diagnostic: &'static str,
) -> crate::storage::backends::s3::S3ProtocolFailure {
    s3_role_session(diagnostic.to_string())
}

fn s3_role_entry(
    class: crate::model::FailureClass,
    diagnostic: &str,
) -> crate::storage::backends::s3::S3ProtocolFailure {
    crate::storage::backends::s3::S3ProtocolFailure::entry(
        class,
        crate::model::Transience::Permanent,
        diagnostic,
    )
}

pub(super) fn s3_role_remote_failure(
    status: Option<u16>,
    code: Option<&str>,
    diagnostic: &'static str,
) -> crate::storage::backends::s3::S3ProtocolFailure {
    use crate::model::{FailureClass, Transience};
    match (status, code) {
        (Some(401), _) | (_, Some("InvalidAccessKeyId" | "SignatureDoesNotMatch")) => {
            crate::storage::backends::s3::S3ProtocolFailure::session(
                FailureClass::Authentication,
                Transience::Permanent,
                diagnostic,
            )
        }
        (Some(403), _) | (_, Some("AccessDenied")) => {
            crate::storage::backends::s3::S3ProtocolFailure::entry(
                FailureClass::PermissionDenied,
                Transience::Permanent,
                diagnostic,
            )
        }
        (Some(404), _) | (_, Some("NoSuchKey" | "NoSuchUpload" | "NoSuchVersion")) => {
            crate::storage::backends::s3::S3ProtocolFailure::entry(
                FailureClass::NotFound,
                Transience::Permanent,
                diagnostic,
            )
        }
        // The body did not match its digest: corrupted in flight, a resend may succeed.
        (_, Some("BadDigest")) => {
            crate::storage::backends::s3::S3ProtocolFailure::corrupted_upload(diagnostic)
        }
        // The completion named a part the upload does not hold, or out of order: our part list
        // disagrees with the server's, and sending it again cannot change that.
        (_, Some("InvalidPart" | "InvalidPartOrder")) => {
            S3ProtocolFailure::entry(FailureClass::Conflict, Transience::Permanent, diagnostic)
        }
        // A part other than the last is below the minimum part size: the parts are wrong.
        (_, Some("EntityTooSmall")) => {
            S3ProtocolFailure::entry(FailureClass::Corruption, Transience::Permanent, diagnostic)
        }
        // The digest header itself is malformed: our request is wrong, and stays wrong.
        (_, Some("InvalidDigest")) => crate::storage::backends::s3::S3ProtocolFailure::entry(
            FailureClass::InvalidInput,
            Transience::Permanent,
            diagnostic,
        ),
        (Some(409 | 412 | 416), _) => crate::storage::backends::s3::S3ProtocolFailure::entry(
            FailureClass::Conflict,
            Transience::Permanent,
            diagnostic,
        ),
        // The store does not implement the request (ONTAP NAS buckets: CreateMultipartUpload and
        // a conditional GET answer 501 `NotImplemented`): a permanent limit of the store, not a
        // lost connection that a retry could cure.
        (Some(501), _) | (_, Some("NotImplemented")) => {
            S3ProtocolFailure::entry(FailureClass::Unsupported, Transience::Permanent, diagnostic)
        }
        // The service asks for fewer requests: back off, it is no lost session (P7 §8, Q11). The
        // scope stays the session's so callers outside a listing keep their behaviour; a listing
        // backs off within its budget, and the traversal never ends on one.
        (Some(429 | 503), _) => throttled(diagnostic),
        (_, Some(code)) if is_throttling_code(code) => throttled(diagnostic),
        (Some(500..=599), _) => crate::storage::backends::s3::S3ProtocolFailure::session(
            FailureClass::Connectivity,
            Transience::Transient,
            diagnostic,
        ),
        _ => crate::storage::backends::s3::S3ProtocolFailure::session(
            FailureClass::Protocol,
            Transience::Unknown,
            diagnostic,
        ),
    }
}

/// Error codes S3 and S3-compatible stores (and the AWS SDK's own throttling list) use to ask for
/// fewer requests, whatever HTTP status carried them.
fn is_throttling_code(code: &str) -> bool {
    matches!(
        code,
        "SlowDown"
            | "ServiceUnavailable"
            | "Throttling"
            | "ThrottlingException"
            | "ThrottledException"
            | "RequestThrottled"
            | "RequestThrottledException"
            | "TooManyRequests"
            | "TooManyRequestsException"
            | "RequestLimitExceeded"
            | "BandwidthLimitExceeded"
    )
}

fn throttled(diagnostic: &'static str) -> S3ProtocolFailure {
    S3ProtocolFailure::session(FailureClass::Throttled, Transience::Transient, diagnostic)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::backends::s3::S3Protocol as _;
    use crate::storage::backends::s3::S3ProtocolFailure;

    /// A request that named a version: 405 is a delete marker and 400 `InvalidArgument` a
    /// malformed version id, both about this one entry. A 400 without that code (an expired token,
    /// a wrong region — or any HEAD, which carries no code) keeps the generic mapping.
    #[test]
    fn versioned_request_failures_concern_the_entry() {
        use crate::model::{FailureClass, Transience};
        let generic = || s3_role_remote_failure(Some(400), None, "request");
        assert!(matches!(generic(), S3ProtocolFailure::Session { .. }));
        for (status, code, class) in [
            (405, None, FailureClass::NotFound),
            (400, Some("InvalidArgument"), FailureClass::InvalidInput),
        ] {
            assert!(
                matches!(
                    versioned_failure(Some(status), code, generic()),
                    S3ProtocolFailure::Entry { class: got, transience: Transience::Permanent, .. }
                        if got == class
                ),
                "{status}"
            );
        }
        for (status, code) in [(400, None), (400, Some("ExpiredToken")), (503, None)] {
            assert!(
                matches!(
                    versioned_failure(Some(status), code, generic()),
                    S3ProtocolFailure::Session { .. }
                ),
                "{status} {code:?}"
            );
        }
        assert!(matches!(
            s3_role_remote_failure(Some(404), Some("NoSuchVersion"), "get"),
            S3ProtocolFailure::Entry {
                class: FailureClass::NotFound,
                ..
            }
        ));
    }

    #[test]
    fn http_errors_keep_scope_class_and_transience() {
        use crate::model::{FailureClass, Transience};
        assert!(matches!(
            s3_role_remote_failure(Some(404), Some("NoSuchKey"), "head"),
            S3ProtocolFailure::Entry {
                class: FailureClass::NotFound,
                transience: Transience::Permanent,
                ..
            }
        ));
        assert!(matches!(
            s3_role_remote_failure(Some(403), Some("AccessDenied"), "get"),
            S3ProtocolFailure::Entry {
                class: FailureClass::PermissionDenied,
                transience: Transience::Permanent,
                ..
            }
        ));
        assert!(matches!(
            s3_role_remote_failure(Some(502), None, "request"),
            S3ProtocolFailure::Session {
                class: FailureClass::Connectivity,
                transience: Transience::Transient,
                ..
            }
        ));
        assert!(matches!(
            s3_role_remote_failure(Some(401), None, "request"),
            S3ProtocolFailure::Session {
                class: FailureClass::Authentication,
                transience: Transience::Permanent,
                ..
            }
        ));
        assert!(matches!(
            s3_role_transport_failure("timeout"),
            S3ProtocolFailure::Session {
                class: FailureClass::Connectivity,
                transience: Transience::Transient,
                ..
            }
        ));
    }

    /// 501 `NotImplemented` (ONTAP NAS buckets: `CreateMultipartUpload`, a GET with `If-Match`,
    /// a copy between a NAS and an object bucket) is a permanent `Unsupported` of the entry, not
    /// the transient `Connectivity` of the other 5xx.
    #[test]
    fn not_implemented_is_a_permanent_unsupported() {
        use crate::model::{FailureClass, Transience};
        for (status, code) in [
            (Some(501), Some("NotImplemented")),
            (Some(501), None),
            (Some(400), Some("NotImplemented")),
        ] {
            assert_eq!(
                s3_role_remote_failure(status, code, "create multipart upload"),
                S3ProtocolFailure::entry(
                    FailureClass::Unsupported,
                    Transience::Permanent,
                    "create multipart upload"
                ),
                "{status:?} {code:?}"
            );
        }
    }

    /// DXN (Ceph RGW) answers a retried `CompleteMultipartUpload` whose first attempt is still
    /// completing with 500 `InternalError` ("This multipart completion is already in progress";
    /// `MinIO` answers 404 `NoSuchUpload`), and `ListParts` after the completion with 404
    /// `NoSuchKey` (`MinIO`: `NoSuchUpload`). The first is no refusal before commit — a transient
    /// session failure, which the completion settles as changed — and the second is the same
    /// `NotFound` as `NoSuchUpload`, which sends the settlement to the key's versions.
    #[test]
    fn dxn_answers_to_a_completion_in_progress_and_after_it() {
        use crate::model::{FailureClass, Transience};
        assert!(matches!(
            s3_role_remote_failure(Some(500), Some("InternalError"), "complete"),
            S3ProtocolFailure::Session {
                class: FailureClass::Connectivity,
                transience: Transience::Transient,
                ..
            }
        ));
        for code in ["NoSuchKey", "NoSuchUpload"] {
            assert_eq!(
                s3_role_remote_failure(Some(404), Some(code), "list parts"),
                S3ProtocolFailure::entry(
                    FailureClass::NotFound,
                    Transience::Permanent,
                    "list parts"
                ),
                "{code}"
            );
        }
    }

    /// A body that does not match its `Content-MD5` was corrupted in flight: an entry failure a
    /// resend may cure, whatever the status code carrying it.
    #[test]
    fn digest_mismatch_is_a_transient_corruption_of_the_entry() {
        use crate::model::{FailureClass, Transience};
        for status in [Some(400), None] {
            assert!(
                matches!(
                    s3_role_remote_failure(status, Some("BadDigest"), "put"),
                    S3ProtocolFailure::Entry {
                        class: FailureClass::Corruption,
                        transience: Transience::Transient,
                        ..
                    }
                ),
                "{status:?}"
            );
        }
        // A malformed digest header is our own request's fault: resending it cannot help.
        assert!(matches!(
            s3_role_remote_failure(Some(400), Some("InvalidDigest"), "put"),
            S3ProtocolFailure::Entry {
                class: FailureClass::InvalidInput,
                transience: Transience::Permanent,
                ..
            }
        ));
    }

    /// Real lab: a `PutObject` whose `Content-MD5` does not match its body is refused with
    /// `BadDigest` and leaves no object; the matching digest then writes it, reporting the quoted
    /// hex MD5 as its `ETag`.
    #[tokio::test]
    #[ignore = "requires the shared standard S3 lab"]
    async fn bad_content_md5_is_bad_digest_on_lab() -> Result<(), Box<dyn std::error::Error>> {
        use crate::model::{FailureClass, Transience};
        use base64::Engine as _;
        use base64::engine::general_purpose::STANDARD;
        use md5::{Digest as _, Md5};
        let backend = S3Storage::new(&std::env::var("LAB_S3_ARCHITECTURE_URL")?, None).await?;
        let key = format!(
            "{}.c14a-bad-digest",
            std::env::var("LAB_S3_ARCHITECTURE_KEY")?
        );
        let body = Bytes::from_static(b"c14a single put payload");
        let digest = Md5::digest(&body);
        let wrong = STANDARD.encode(Md5::digest(b"something else"));
        let refused = backend.put_object(&key, body.clone().into(), &wrong).await;
        assert!(
            matches!(
                refused,
                Err(S3ProtocolFailure::Entry {
                    class: FailureClass::Corruption,
                    transience: Transience::Transient,
                    ..
                })
            ),
            "{refused:?}"
        );
        assert!(
            backend.head(&key).await.is_err(),
            "a refused PUT stores nothing"
        );
        let written = backend
            .put_object(&key, body.into(), &STANDARD.encode(digest))
            .await
            .map_err(|failure| std::io::Error::other(format!("{failure:?}")))?;
        let deleted = backend.delete_object(&key).await;
        assert_eq!(written.etag, format!("\"{digest:x}\""));
        deleted.map_err(|failure| std::io::Error::other(format!("{failure:?}")))?;
        Ok(())
    }
}
