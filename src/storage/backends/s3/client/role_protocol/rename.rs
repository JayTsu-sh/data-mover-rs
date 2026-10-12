//! Same-bucket rename preserves object metadata and tags without downloading object bytes.
use super::{
    ProvideErrorMetadata, S3NativeCopySource, S3ProtocolFailure, S3Result, S3Storage,
    s3_role_remote_failure, s3_role_transport_failure,
};
use crate::model::{FailureClass, Transience};
use crate::storage::backends::s3::S3Protocol;
use crate::storage::backends::s3::client::copy_request::state_empty_body;
use aws_sdk_s3::types::{MetadataDirective, TaggingDirective};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

pub(super) async fn rename(storage: &S3Storage, from: &str, to: &str) -> S3Result<()> {
    rename_with_limits(storage, from, to, 5 * 1024 * 1024 * 1024, 64 * 1024 * 1024).await
}

pub(in crate::storage::backends::s3::client) async fn rename_with_limits(
    storage: &S3Storage,
    from: &str,
    to: &str,
    single_max: u64,
    requested_part_size: u64,
) -> S3Result<()> {
    let facts = S3Protocol::head(storage, from).await?;
    if from == to {
        return Ok(());
    }
    let source = S3NativeCopySource {
        bucket: storage.bucket_name.clone(),
        key: storage.build_full_key(from),
        etag: facts.etag,
        version_id: facts.version_id,
        size: facts.size,
    };
    if source.size <= single_max {
        let response = storage
            .client
            .copy_object()
            .bucket(&storage.bucket_name)
            .key(storage.build_full_key(to))
            .copy_source(super::native::copy_source(&source))
            .copy_source_if_match(&source.etag)
            .metadata_directive(MetadataDirective::Copy)
            .tagging_directive(TaggingDirective::Copy)
            .customize()
            .mutate_request(state_empty_body)
            .send()
            .await
            .map_err(|e| classify_sdk!(e, "S3 rename CopyObject failed"))?;
        // HTTP success alone does not acknowledge publication: some servers return a
        // malformed or empty success body. Keep the source unless the copy was acknowledged.
        response
            .copy_object_result()
            .and_then(|result| result.e_tag())
            .filter(|etag| !etag.trim().is_empty())
            .ok_or_else(|| {
                S3ProtocolFailure::protocol("S3 rename CopyObject response omitted ETag")
            })?;
    } else {
        if to.ends_with('/') || !S3Protocol::bucket_capabilities(storage).multipart {
            return Err(S3ProtocolFailure::entry(
                FailureClass::Unsupported,
                Transience::Permanent,
                "S3 multipart rename is unsupported for this destination",
            ));
        }
        multipart(storage, &source, from, to, requested_part_size).await?;
    }
    // Detect a source replacement before deletion. Callers must still exclude concurrent
    // writers: the source HEAD and DELETE are separate operations.
    let current = S3Protocol::head(storage, from).await?;
    if current.etag != source.etag
        || current.version_id != source.version_id
        || current.size != source.size
    {
        return Err(S3ProtocolFailure::entry(
            FailureClass::Conflict,
            Transience::Permanent,
            "S3 rename source changed before deletion; destination was copied",
        ));
    }
    S3Protocol::delete_object(storage, from).await
}

async fn multipart(
    storage: &S3Storage,
    source: &S3NativeCopySource,
    from: &str,
    to: &str,
    requested_part_size: u64,
) -> S3Result<()> {
    let head = storage
        .client
        .head_object()
        .bucket(&storage.bucket_name)
        .key(&source.key)
        .set_version_id(source.version_id.clone())
        .if_match(&source.etag)
        .send()
        .await
        .map_err(|e| classify_sdk!(e, "S3 rename metadata read failed"))?;
    let tags = S3Protocol::get_tags(storage, from, source.version_id.as_deref()).await?;
    let tagging = tags
        .iter()
        .map(|tag| {
            format!(
                "{}={}",
                utf8_percent_encode(tag.key(), NON_ALPHANUMERIC),
                utf8_percent_encode(tag.value(), NON_ALPHANUMERIC)
            )
        })
        .collect::<Vec<_>>()
        .join("&");
    let mut create = storage
        .client
        .create_multipart_upload()
        .bucket(&storage.bucket_name)
        .key(storage.build_full_key(to))
        .set_metadata(head.metadata().cloned())
        .set_content_type(head.content_type().map(str::to_owned))
        .set_content_encoding(head.content_encoding().map(str::to_owned))
        .set_cache_control(head.cache_control().map(str::to_owned))
        .set_content_disposition(head.content_disposition().map(str::to_owned))
        .set_content_language(head.content_language().map(str::to_owned));
    if !tagging.is_empty() {
        create = create.tagging(tagging);
    }
    let upload = create
        .send()
        .await
        .map_err(|e| classify_sdk!(e, "S3 rename multipart creation failed"))?
        .upload_id
        .ok_or_else(|| S3ProtocolFailure::protocol("S3 rename omitted upload ID"))?;
    let result = async {
        let part_size = requested_part_size.max(source.size.div_ceil(10_000));
        if !(5 * 1024 * 1024..=5 * 1024 * 1024 * 1024).contains(&part_size) {
            return Err(S3ProtocolFailure::protocol(
                "S3 rename exceeds multipart size limit",
            ));
        }
        let mut parts = Vec::new();
        let mut start = 0;
        while start < source.size {
            let end = (start + part_size).min(source.size);
            let number = i32::try_from(parts.len() + 1)
                .map_err(|_| S3ProtocolFailure::protocol("S3 rename part overflow"))?;
            let etag =
                S3Protocol::upload_part_copy(storage, source, to, &upload, number, start..end)
                    .await?;
            parts.push((number, etag));
            start = end;
        }
        S3Protocol::complete_multipart(storage, to, &upload, &parts).await?;
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = S3Protocol::abort_multipart(storage, to, &upload).await;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AssertTestValue;
    use aws_smithy_runtime_api::client::http::{HttpConnector, HttpConnectorFuture};
    use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
    use aws_smithy_runtime_api::http::StatusCode;
    use aws_smithy_types::body::SdkBody;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    /// Force the production multipart rename protocol with a lab-sized object and part limit.
    #[tokio::test]
    #[ignore = "requires DM_S3_RENAME_TEST_URL pointing at a scoped standard S3 lab prefix"]
    async fn multipart_rename_preserves_content_and_metadata_on_lab()
    -> Result<(), Box<dyn std::error::Error>> {
        let storage = S3Storage::new(&std::env::var("DM_S3_RENAME_TEST_URL")?, None).await?;
        if storage.prefix.as_deref().is_none_or(str::is_empty) {
            return Err("rename lab test requires a nonempty isolated prefix".into());
        }
        let from = "multipart rename source %?# 中文.bin";
        let to = "multipart renamed %?# 中文.bin";
        let source_key = storage.build_full_key(from);
        let destination_key = storage.build_full_key(to);
        let body = vec![0x5a; 20 * 1024 * 1024];
        let outcome: Result<(), Box<dyn std::error::Error>> = async {
            storage
                .client
                .put_object()
                .bucket(&storage.bucket_name)
                .key(&source_key)
                .metadata("test-metadata", "preserved")
                .tagging("test-tag=preserved")
                .content_type("application/x-data-mover-test")
                .body(aws_sdk_s3::primitives::ByteStream::from(body.clone()))
                .send()
                .await?;
            rename_with_limits(&storage, from, to, 8 * 1024 * 1024, 5 * 1024 * 1024)
                .await
                .map_err(|failure| std::io::Error::other(format!("{failure:?}")))?;
            let destination = storage
                .client
                .get_object()
                .bucket(&storage.bucket_name)
                .key(&destination_key)
                .send()
                .await?;
            if destination.content_type() != Some("application/x-data-mover-test")
                || destination
                    .metadata()
                    .and_then(|metadata| metadata.get("test-metadata"))
                    .map(String::as_str)
                    != Some("preserved")
            {
                return Err("multipart rename changed object metadata".into());
            }
            if destination.body.collect().await?.into_bytes() != body {
                return Err("multipart rename changed payload bytes".into());
            }
            let tags = storage
                .client
                .get_object_tagging()
                .bucket(&storage.bucket_name)
                .key(&destination_key)
                .send()
                .await?;
            if !tags
                .tag_set()
                .iter()
                .any(|tag| tag.key() == "test-tag" && tag.value() == "preserved")
            {
                return Err("multipart rename changed object tags".into());
            }
            if !matches!(
                S3Protocol::head(&storage, from).await,
                Err(S3ProtocolFailure::Entry {
                    class: FailureClass::NotFound,
                    ..
                })
            ) {
                return Err("multipart rename did not remove its source".into());
            }
            Ok(())
        }
        .await;
        // Cleanup remains confined to these two fixture keys, including a failed SDK operation.
        let mut cleanup: Result<(), Box<dyn std::error::Error>> = Ok(());
        for key in [&source_key, &destination_key] {
            if let Err(error) = storage
                .client
                .delete_object()
                .bucket(&storage.bucket_name)
                .key(key)
                .send()
                .await
            {
                cleanup = Err(error.into());
            }
        }
        outcome.and(cleanup)
    }

    type Request = (String, Vec<(String, String)>);
    #[derive(Clone, Debug, Default)]
    struct Recorder {
        fail_copy: bool,
        copy_body: Option<&'static str>,
        changed_source: bool,
        heads: Arc<AtomicUsize>,
        requests: Arc<Mutex<Vec<Request>>>,
    }
    impl HttpConnector for Recorder {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let method = request.method().to_owned();
            self.requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((
                    method.clone(),
                    request
                        .headers()
                        .iter()
                        .map(|(k, v)| (k.to_owned(), v.to_owned()))
                        .collect(),
                ));
            let failed = method == "PUT" && self.fail_copy;
            let body = if failed {
                "<Error><Code>PreconditionFailed</Code></Error>"
            } else if method == "PUT" {
                self.copy_body
                    .unwrap_or("<CopyObjectResult><ETag>&quot;copy&quot;</ETag></CopyObjectResult>")
            } else {
                ""
            };
            let mut response = HttpResponse::new(
                StatusCode::try_from(if failed { 412 } else { 200 }).assert_value("HTTP status"),
                SdkBody::from(body),
            );
            if method == "HEAD" {
                let changed = self.changed_source && self.heads.fetch_add(1, Ordering::SeqCst) > 0;
                response.headers_mut().insert("content-length", "10");
                response
                    .headers_mut()
                    .insert("etag", if changed { "\"changed\"" } else { "\"source\"" });
            }
            HttpConnectorFuture::ready(Ok(response))
        }
    }

    #[tokio::test]
    async fn rename_preserves_source_when_copy_success_omits_valid_etag() {
        for body in [
            "",
            "<CopyObjectResult/>",
            "<CopyObjectResult><ETag></ETag></CopyObjectResult>",
            "<CopyObjectResult><ETag>   </ETag></CopyObjectResult>",
        ] {
            let recorder = Recorder {
                copy_body: Some(body),
                ..Recorder::default()
            };
            let storage = super::super::native::tests::storage(recorder.clone());
            assert!(rename(&storage, "from", "to").await.is_err());
            let requests = recorder
                .requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(
                requests
                    .iter()
                    .map(|(method, _)| method.as_str())
                    .collect::<Vec<_>>(),
                ["HEAD", "PUT"],
                "a malformed copy acknowledgement must stop before source deletion: {body:?}"
            );
        }
    }

    #[tokio::test]
    async fn rename_preserves_source_on_copy_failure_or_replacement() {
        for (fail_copy, changed_source) in [(true, false), (false, true), (false, false)] {
            let recorder = Recorder {
                fail_copy,
                changed_source,
                ..Recorder::default()
            };
            let storage = super::super::native::tests::storage(recorder.clone());
            let result = rename(&storage, "from", "to").await;
            assert_eq!(result.is_ok(), !fail_copy && !changed_source);
            let requests = recorder
                .requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(
                requests.iter().any(|(method, _)| method == "DELETE"),
                result.is_ok()
            );
            assert!(
                !requests.iter().any(|(method, _)| method == "GET"),
                "copy stays server-side"
            );
            let (_, headers) = requests
                .iter()
                .find(|(method, _)| method == "PUT")
                .assert_value("copy request");
            for name in ["x-amz-metadata-directive", "x-amz-tagging-directive"] {
                assert!(
                    headers
                        .iter()
                        .any(|(key, value)| key == name && value == "COPY")
                );
            }
            assert!(
                headers.iter().any(
                    |(key, value)| key == "x-amz-copy-source-if-match" && value == "\"source\""
                )
            );
        }
    }
}
