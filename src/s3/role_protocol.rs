use super::{
    COPY_PART_SIZE, COPY_SINGLE_MAX, CompletedPart, ProvideErrorMetadata, S3Storage,
    build_copy_source,
};
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::storage::backends::s3::{
    S3NativeCopyEvidence, S3NativeCopyFailure, S3NativeCopyResult, S3NativeCopySource,
    S3ProtocolFailure, S3Result, S3VersionFacts, S3WriteFacts,
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

mod multipart;
mod native;
mod versions;

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
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(self.build_full_key(key))
            .range(format!("bytes={}-{}", range.start, range.end - 1))
            .set_version_id(observed.version_id.clone())
            .if_match(&observed.etag)
            .send()
            .await
            .map_err(|error| {
                let failure = classify_sdk!(error, "S3 GetObject range request failed");
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
        response
            .body
            .collect()
            .await
            .map(aws_smithy_types::byte_stream::AggregatedBytes::into_bytes)
            .map_err(|error| s3_role_session(error.to_string()))
    }

    async fn put_object(
        &self,
        key: &str,
        body: Bytes,
        content_md5_base64: &str,
    ) -> S3Result<S3WriteFacts> {
        let response = self
            .client
            .put_object()
            .bucket(&self.bucket_name)
            .key(self.build_full_key(key))
            .body(aws_sdk_s3::primitives::ByteStream::from(body))
            .content_md5(content_md5_base64)
            .send()
            .await
            .map_err(|error| classify_sdk!(error, "S3 PutObject request failed"))?;
        // The object was written; only the response is malformed.
        let etag = response
            .e_tag()
            .ok_or_else(|| S3ProtocolFailure::protocol("S3 PutObject response omitted ETag"))?;
        Ok(S3WriteFacts::new(
            etag.to_string(),
            response.version_id().map(str::to_string),
        ))
    }

    async fn begin_multipart(&self, key: &str) -> crate::storage::backends::s3::S3Result<String> {
        self.client
            .create_multipart_upload()
            .bucket(&self.bucket_name)
            .key(self.build_full_key(key))
            .send()
            .await
            .map_err(|error| classify_sdk!(error, "S3 CreateMultipartUpload request failed"))?
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
        bytes: Bytes,
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
                .map_err(|error| classify_sdk!(error, "S3 ListParts request failed"))?;
            parts.extend(decode_parts(response.parts())?);
            let Some(next) = continuation_marker(&response, marker.as_deref())? else {
                return Ok(parts);
            };
            marker = Some(next);
        }
    }

    async fn copy_object(
        &self,
        from: &str,
        to: &str,
    ) -> crate::storage::backends::s3::S3Result<()> {
        let from = self.build_full_key(from);
        let to = self.build_full_key(to);
        let size = self
            .client
            .head_object()
            .bucket(&self.bucket_name)
            .key(&from)
            .send()
            .await
            .map_err(|error| classify_sdk!(error, "S3 publication source HeadObject failed"))?
            .content_length()
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(|| {
                s3_role_entry(
                    crate::model::FailureClass::Corruption,
                    "S3 publication source has invalid size",
                )
            })?;
        if size <= COPY_SINGLE_MAX {
            self.client
                .copy_object()
                .bucket(&self.bucket_name)
                .key(to)
                .copy_source(build_copy_source(&self.bucket_name, &from))
                .send()
                .await
                .map(|_| ())
                .map_err(|error| classify_sdk!(error, "S3 CopyObject request failed"))
        } else {
            self.multipart_copy_object(&from, &to, size, COPY_PART_SIZE, None)
                .await
                .map_err(|error| s3_role_legacy_failure(error.to_string()))
        }
    }

    async fn native_copy(
        &self,
        source: &S3NativeCopySource,
        to: &str,
        multipart_upload_id: Option<&str>,
        cancel: &CancellationToken,
    ) -> S3NativeCopyResult {
        native::copy(self, source, to, multipart_upload_id, cancel).await
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

    async fn list_versions(&self, key: &str) -> S3Result<Vec<S3VersionFacts>> {
        self.role_list_versions(key).await
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

fn s3_role_legacy_failure(diagnostic: String) -> crate::storage::backends::s3::S3ProtocolFailure {
    crate::storage::backends::s3::S3ProtocolFailure::session(
        crate::model::FailureClass::Protocol,
        crate::model::Transience::Unknown,
        diagnostic,
    )
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
        (Some(429 | 500..=599), _) | (_, Some("SlowDown" | "ServiceUnavailable")) => {
            crate::storage::backends::s3::S3ProtocolFailure::session(
                FailureClass::Connectivity,
                Transience::Transient,
                diagnostic,
            )
        }
        _ => crate::storage::backends::s3::S3ProtocolFailure::session(
            FailureClass::Protocol,
            Transience::Unknown,
            diagnostic,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::SourceVersion;
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
        let refused = backend.put_object(&key, body.clone(), &wrong).await;
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
            .put_object(&key, body, &STANDARD.encode(digest))
            .await
            .map_err(|failure| std::io::Error::other(format!("{failure:?}")))?;
        let deleted = backend.delete_object(&key).await;
        assert_eq!(written.etag, format!("\"{digest:x}\""));
        deleted.map_err(|failure| std::io::Error::other(format!("{failure:?}")))?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires the shared standard S3 lab"]
    async fn standard_s3_invalid_manifest_is_aborted_and_restartable()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = prepare_invalid_manifest_fixture().await?;
        assert_restartable_after_rejection(fixture).await
    }

    #[tokio::test]
    #[ignore = "requires the shared standard S3 lab"]
    async fn standard_s3_native_multipart_copy_uses_owned_upload()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend = S3Storage::new(&std::env::var("LAB_S3_ARCHITECTURE_URL")?, None).await?;
        let source_path = std::env::var("LAB_S3_ARCHITECTURE_KEY")?;
        let destination = format!("{source_path}.native-multipart");
        let facts = backend
            .head(&source_path)
            .await
            .map_err(|failure| std::io::Error::other(format!("{failure:?}")))?;
        let source = S3NativeCopySource {
            bucket: backend.bucket_name.clone(),
            key: backend.build_full_key(&source_path),
            etag: facts.etag,
            version_id: facts.version_id,
            size: facts.size,
        };
        let upload_id = backend
            .begin_multipart(&destination)
            .await
            .map_err(|failure| std::io::Error::other(format!("{failure:?}")))?;
        let result = native::copy_multipart_with_part_size(
            &backend,
            &source,
            &destination,
            &upload_id,
            5 * 1024 * 1024,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| std::io::Error::other(format!("{failure:?}")))?;
        assert!(result.requests > 3);
        let copied = backend
            .head(&destination)
            .await
            .map_err(|failure| std::io::Error::other(format!("{failure:?}")))?;
        assert_eq!(copied.size, source.size);
        backend
            .delete_object(&destination)
            .await
            .map_err(|failure| std::io::Error::other(format!("{failure:?}")))?;
        Ok(())
    }

    struct InvalidManifestFixture {
        backend: S3Storage,
        prepare: crate::storage::PrepareRequest,
        recovery: crate::storage::RecoveryIdentity,
    }

    async fn prepare_invalid_manifest_fixture()
    -> Result<InvalidManifestFixture, Box<dyn std::error::Error>> {
        use crate::model::{
            BackendIdentity, BackendKind, EntryKind, IdentityStrength, SourceIdentity, StoragePath,
        };
        use crate::storage::{FinalDestination, PreflightPolicy, PrepareRequest, SourceDescriptor};
        let backend = S3Storage::new(&std::env::var("LAB_S3_ARCHITECTURE_URL")?, None).await?;
        let identity = BackendIdentity::new(BackendKind::S3, "standard-s3-invalid-recovery")?;
        let storage = backend.architecture_storage()?;
        let destination = storage.staged_destination(&PreflightPolicy::production())?;
        let source = SourceDescriptor {
            path: StoragePath::new("generated-source")?,
            kind: EntryKind::File,
            size: None,
            source_identity: SourceIdentity::new(
                identity,
                IdentityStrength::PathScoped,
                b"invalid-source",
            )?,
            backend_fact: None,
            content_version: None,
            inline_timestamps: None,
            inline_mode: None,
            version: SourceVersion::Current,
        };
        let path = StoragePath::new(format!(
            "{}.manifest",
            std::env::var("LAB_S3_ARCHITECTURE_KEY")?
        ))?;
        let prepare = PrepareRequest {
            final_destination: FinalDestination::new(path),
            source: source.clone(),
            recovery_binding: [7; 32],
        };
        let stage = destination.prepare(prepare.clone()).await?;
        let recovery = destination.recovery_identity(&stage).await?;
        let (key, upload_id) = split_recovery(&recovery)?;
        backend
            .upload_part_with_stream(
                &backend.build_full_key(&key),
                &upload_id,
                2,
                vec![Bytes::from(vec![3; 8 * 1024 * 1024])],
                8 * 1024 * 1024,
            )
            .await?;
        Ok(InvalidManifestFixture {
            backend,
            prepare,
            recovery,
        })
    }

    async fn assert_restartable_after_rejection(
        fixture: InvalidManifestFixture,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::storage::{PreflightPolicy, RecoverRequest};
        let reconnected = fixture
            .backend
            .architecture_storage()?
            .staged_destination(&PreflightPolicy::production())?;
        let result = reconnected
            .recover(RecoverRequest {
                identity: fixture.recovery,
                final_destination: fixture.prepare.final_destination.clone(),
                source: fixture.prepare.source.clone(),
                recovery_binding: fixture.prepare.recovery_binding,
                claim_token: [1; 32],
            })
            .await;
        assert!(
            matches!(result, Err(crate::storage::StorageRoleFailure::Entry(ref failure))
            if failure.class() == crate::model::FailureClass::Corruption)
        );
        let fresh = reconnected.prepare(fixture.prepare).await?;
        reconnected.discard(fresh).await?;
        Ok(())
    }

    fn split_recovery(
        identity: &crate::storage::RecoveryIdentity,
    ) -> Result<(String, String), Box<dyn std::error::Error>> {
        let split = identity
            .as_bytes()
            .iter()
            .position(|byte| *byte == 0)
            .ok_or("missing recovery separator")?;
        Ok((
            String::from_utf8(identity.as_bytes()[..split].to_vec())?,
            String::from_utf8(identity.as_bytes()[split + 1..].to_vec())?,
        ))
    }
}
