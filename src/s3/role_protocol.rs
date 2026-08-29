use super::{
    COPY_PART_SIZE, COPY_SINGLE_MAX, CompletedPart, ProvideErrorMetadata, S3Storage,
    build_copy_source,
};
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::storage::backends::s3::{
    S3NativeCopyEvidence, S3NativeCopyFailure, S3NativeCopyResult, S3NativeCopySource,
    S3ProtocolFailure, S3Result,
};

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

mod native;

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

#[async_trait::async_trait]
impl crate::storage::backends::s3::S3Protocol for S3Storage {
    async fn head(
        &self,
        key: &str,
    ) -> crate::storage::backends::s3::S3Result<crate::storage::backends::s3::S3ObjectFacts> {
        let full_key = self.build_full_key(key);
        let response = self
            .client
            .head_object()
            .bucket(&self.bucket_name)
            .key(&full_key)
            .send()
            .await
            .map_err(|error| classify_sdk!(error, "S3 HeadObject request failed"))?;
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
        })
    }

    async fn get_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
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
            .send()
            .await
            .map_err(|error| classify_sdk!(error, "S3 GetObject range request failed"))?;
        response
            .body
            .collect()
            .await
            .map(aws_smithy_types::byte_stream::AggregatedBytes::into_bytes)
            .map_err(|error| s3_role_session(error.to_string()))
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
    ) -> crate::storage::backends::s3::S3Result<String> {
        self.client
            .upload_part()
            .bucket(&self.bucket_name)
            .key(self.build_full_key(key))
            .upload_id(upload_id)
            .part_number(part_number)
            .body(aws_sdk_s3::primitives::ByteStream::from(bytes))
            .send()
            .await
            .map_err(|error| classify_sdk!(error, "S3 UploadPart request failed"))?
            .e_tag()
            .map(str::to_string)
            .ok_or_else(|| {
                s3_role_entry(
                    crate::model::FailureClass::Corruption,
                    "S3 UploadPart response omitted ETag",
                )
            })
    }

    async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[(i32, String)],
    ) -> crate::storage::backends::s3::S3Result<()> {
        let completed: Vec<CompletedPart> = parts
            .iter()
            .map(|(number, etag)| {
                CompletedPart::builder()
                    .part_number(*number)
                    .e_tag(etag)
                    .build()
            })
            .collect();
        let upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(completed))
            .build();
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket_name)
            .key(self.build_full_key(key))
            .upload_id(upload_id)
            .multipart_upload(upload)
            .send()
            .await
            .map(|_| ())
            .map_err(|error| classify_sdk!(error, "S3 CompleteMultipartUpload request failed"))
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

    async fn get_tags(
        &self,
        key: &str,
    ) -> crate::storage::backends::s3::S3Result<Vec<crate::model::ObjectTag>> {
        let response = self
            .client
            .get_object_tagging()
            .bucket(&self.bucket_name)
            .key(self.build_full_key(key))
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

    async fn claim(
        &self,
        key: &str,
        token: [u8; 32],
    ) -> crate::storage::backends::s3::S3Result<crate::storage::backends::s3::S3ClaimOutcome> {
        let key = self.build_full_key(key);
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&key)
            .if_none_match("*")
            .body(aws_sdk_s3::primitives::ByteStream::from(
                Bytes::copy_from_slice(&token),
            ))
            .send()
            .await;
        match result {
            Ok(_) => Ok(crate::storage::backends::s3::S3ClaimOutcome::Acquired),
            Err(error)
                if error
                    .raw_response()
                    .is_some_and(|response| response.status().as_u16() == 412) =>
            {
                let existing = self
                    .client
                    .get_object()
                    .bucket(&self.bucket_name)
                    .key(&key)
                    .send()
                    .await
                    .map_err(|error| classify_sdk!(error, "S3 recovery claim read failed"))?
                    .body
                    .collect()
                    .await
                    .map_err(|e| s3_role_session(format!("S3 recovery claim body failed: {e}")))?
                    .into_bytes();
                if existing.as_ref() == token {
                    Ok(crate::storage::backends::s3::S3ClaimOutcome::AlreadyOwned)
                } else {
                    Ok(crate::storage::backends::s3::S3ClaimOutcome::Conflict)
                }
            }
            Err(error) => Err(classify_sdk!(error, "S3 recovery claim request failed")),
        }
    }

    async fn release_claim(&self, key: &str) -> crate::storage::backends::s3::S3Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(self.build_full_key(key))
            .send()
            .await
            .map(|_| ())
            .map_err(|error| classify_sdk!(error, "S3 recovery claim cleanup failed"))
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
        (Some(404), _) | (_, Some("NoSuchKey" | "NoSuchUpload")) => {
            crate::storage::backends::s3::S3ProtocolFailure::entry(
                FailureClass::NotFound,
                Transience::Permanent,
                diagnostic,
            )
        }
        (Some(409 | 412), _) => crate::storage::backends::s3::S3ProtocolFailure::entry(
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
    use crate::storage::backends::s3::S3Protocol as _;
    use crate::storage::backends::s3::S3ProtocolFailure;

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
        identity: crate::model::BackendIdentity,
        prepare: crate::storage::PrepareRequest,
        recovery: crate::storage::RecoveryIdentity,
        key: String,
    }

    async fn prepare_invalid_manifest_fixture()
    -> Result<InvalidManifestFixture, Box<dyn std::error::Error>> {
        use crate::model::{
            BackendIdentity, BackendKind, EntryKind, IdentityStrength, SourceIdentity, StoragePath,
        };
        use crate::storage::{FinalDestination, PreflightPolicy, PrepareRequest, SourceDescriptor};
        let backend = S3Storage::new(&std::env::var("LAB_S3_ARCHITECTURE_URL")?, None).await?;
        let identity = BackendIdentity::new(BackendKind::S3, "standard-s3-invalid-recovery")?;
        let storage = backend.architecture_storage(identity.clone())?;
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
            identity: storage.identity().clone(),
            prepare,
            recovery,
            key,
        })
    }

    async fn assert_restartable_after_rejection(
        fixture: InvalidManifestFixture,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::storage::{PreflightPolicy, RecoverRequest};
        let reconnected = fixture
            .backend
            .architecture_storage(fixture.identity)?
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
        assert_eq!(
            fixture
                .backend
                .claim(&format!("{}.claim", fixture.key), [2; 32])
                .await
                .map_err(|failure| std::io::Error::other(format!("{failure:?}")))?,
            crate::storage::backends::s3::S3ClaimOutcome::Acquired
        );
        fixture
            .backend
            .release_claim(&format!("{}.claim", fixture.key))
            .await
            .map_err(|failure| std::io::Error::other(format!("{failure:?}")))?;
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
