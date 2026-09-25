//! Native S3→S3 copies to the final key (ADR-0006 C18): one `CopyObject`, or `UploadPartCopy`
//! parts of an upload on the final key, each pinned to the bound source.

use std::ops::Range;

use aws_sdk_s3::types::{MetadataDirective, TaggingDirective};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

use super::{
    CompletedPart, ProvideErrorMetadata, S3NativeCopySource, S3ProtocolFailure, S3Result,
    S3Storage, S3WriteFacts, build_copy_source, s3_role_remote_failure, s3_role_transport_failure,
};

fn copy_source(source: &S3NativeCopySource) -> String {
    let mut value = build_copy_source(&source.bucket, &source.key);
    if let Some(version) = &source.version_id {
        value.push_str("?versionId=");
        value.push_str(&utf8_percent_encode(version, NON_ALPHANUMERIC).to_string());
    }
    value
}

async fn copy_part(
    storage: &S3Storage,
    source: &S3NativeCopySource,
    key: &str,
    upload_id: &str,
    number: i32,
    range: std::ops::RangeInclusive<u64>,
) -> S3Result<CompletedPart> {
    let response = storage
        .client
        .upload_part_copy()
        .bucket(&storage.bucket_name)
        .key(key)
        .upload_id(upload_id)
        .part_number(number)
        .copy_source(copy_source(source))
        .copy_source_if_match(&source.etag)
        .copy_source_range(format!("bytes={}-{}", range.start(), range.end()))
        .send()
        .await;
    let response =
        response.map_err(|error| classify_sdk!(error, "S3 native UploadPartCopy failed"))?;
    completed_part(&response, number)
}

/// One `CopyObject` of `source` to `to`, pinned to its `ETag` and version (ADR-0006 C18). The
/// source's user metadata, content type and tags are **not** copied (`REPLACE` with none): a larger
/// native copy (`UploadPartCopy`) and a streamed one carry none either, so the destination's
/// metadata must not depend on the size; tags the metadata plan asks for are set afterwards.
pub(super) async fn copy_from(
    storage: &S3Storage,
    source: &S3NativeCopySource,
    to: &str,
) -> S3Result<S3WriteFacts> {
    let response = storage
        .client
        .copy_object()
        .bucket(&storage.bucket_name)
        .key(storage.build_full_key(to))
        .copy_source(copy_source(source))
        .copy_source_if_match(&source.etag)
        .metadata_directive(MetadataDirective::Replace)
        .tagging_directive(TaggingDirective::Replace)
        .send()
        .await
        .map_err(|error| classify_sdk!(error, "S3 native CopyObject request failed"))?;
    // The object was written; only the response is malformed.
    let etag = response
        .copy_object_result()
        .and_then(|result| result.e_tag())
        .ok_or_else(|| S3ProtocolFailure::protocol("S3 CopyObject response omitted ETag"))?;
    Ok(S3WriteFacts::new(
        etag.to_string(),
        response.version_id().map(str::to_string),
    ))
}

/// One `UploadPartCopy` of `range` of `source` into part `number` of an upload on `key` (a key
/// relative to the storage root); returns the part's `ETag`.
pub(super) async fn upload_part_copy(
    storage: &S3Storage,
    source: &S3NativeCopySource,
    key: &str,
    upload_id: &str,
    number: i32,
    range: Range<u64>,
) -> S3Result<String> {
    let last = range
        .end
        .checked_sub(1)
        .filter(|last| *last >= range.start)
        .ok_or_else(|| S3ProtocolFailure::protocol("empty S3 UploadPartCopy range"))?;
    let part = copy_part(
        storage,
        source,
        &storage.build_full_key(key),
        upload_id,
        number,
        range.start..=last,
    )
    .await?;
    part.e_tag()
        .map(str::to_string)
        .ok_or_else(|| S3ProtocolFailure::protocol("native UploadPartCopy response has no ETag"))
}

fn completed_part(
    response: &aws_sdk_s3::operation::upload_part_copy::UploadPartCopyOutput,
    number: i32,
) -> S3Result<CompletedPart> {
    let etag = response
        .copy_part_result()
        .and_then(|part| part.e_tag())
        .ok_or_else(|| S3ProtocolFailure::protocol("native UploadPartCopy response has no ETag"))?;
    Ok(CompletedPart::builder()
        .part_number(number)
        .e_tag(etag)
        .build())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use aws_credential_types::Credentials;
    use aws_sdk_s3::config::Builder;
    use aws_smithy_runtime_api::client::http::{
        HttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn,
    };
    use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
    use aws_smithy_runtime_api::client::result::ConnectorError;
    use aws_smithy_runtime_api::http::StatusCode;
    use aws_smithy_types::body::SdkBody;
    use aws_types::region::Region;

    use super::*;

    fn storage(connector: impl HttpConnector + Clone + 'static) -> S3Storage {
        let http_client = http_client_fn(move |_settings, _components| {
            SharedHttpConnector::new(connector.clone())
        });
        let config = Builder::new()
            .behavior_version_latest()
            .credentials_provider(Credentials::new("ak", "sk", None, None, "native-test"))
            .region(Region::new("us-east-1"))
            .endpoint_url("http://native.test")
            .force_path_style(true)
            .http_client(http_client)
            .build();
        S3Storage {
            storage_type: super::super::super::StorageType::S3,
            compatibility: super::super::super::S3Compatibility::Standard,
            endpoint: "http://native.test".into(),
            bucket_name: "destination".into(),
            prefix: None,
            client: aws_sdk_s3::Client::from_conf(config),
            hcp_client: None,
            block_size: super::super::super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: super::super::super::DEFAULT_TRANSFER_CONCURRENCY,
            is_bucket_versioned: false,
        }
    }

    fn source() -> S3NativeCopySource {
        S3NativeCopySource {
            bucket: "source".into(),
            key: "object.bin".into(),
            etag: "\"source-etag\"".into(),
            version_id: None,
            size: 10,
        }
    }

    /// The headers a copy request carried that pin or shape it, as (name, value).
    type Seen = Arc<Mutex<Vec<Vec<(String, String)>>>>;

    /// Answers `CopyObject` and `UploadPartCopy` and records each request's copy headers.
    #[derive(Clone, Debug, Default)]
    struct WireConnector {
        seen: Seen,
    }

    impl HttpConnector for WireConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let copy_headers = request
                .headers()
                .iter()
                .filter(|(name, _)| {
                    name.starts_with("x-amz-copy-source") || name.ends_with("directive")
                })
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect();
            self.seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(copy_headers);
            let body = if request.uri().contains("uploadId") {
                r"<CopyPartResult><ETag>&quot;part&quot;</ETag></CopyPartResult>"
            } else {
                r"<CopyObjectResult><ETag>&quot;copy&quot;</ETag></CopyObjectResult>"
            };
            let response = StatusCode::try_from(200)
                .map(|status| HttpResponse::new(status, SdkBody::from(body)))
                .map_err(|error| ConnectorError::other(Box::new(error), None));
            HttpConnectorFuture::ready(response)
        }
    }

    fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
        headers
            .iter()
            .find(|(found, _)| found == name)
            .map(|(_, value)| value.as_str())
    }

    /// Both copies pin the source: `x-amz-copy-source-if-match` carries the bound `ETag`, and
    /// `x-amz-copy-source` names the bound version when there is one (none otherwise).
    /// `CopyObject` replaces the source's metadata and tags with none (ADR-0006 C18).
    #[tokio::test]
    async fn copies_pin_the_source_and_copy_no_metadata() {
        for version in [None, Some("v 1")] {
            let connector = WireConnector::default();
            let storage = storage(connector.clone());
            let mut pinned = source();
            pinned.version_id = version.map(str::to_string);
            let copied = copy_from(&storage, &pinned, "to").await;
            assert_eq!(copied.map(|facts| facts.etag), Ok("\"copy\"".to_string()));
            let part = upload_part_copy(&storage, &pinned, "to", "upload", 1, 0..10).await;
            assert_eq!(part, Ok("\"part\"".to_string()));
            let seen = connector
                .seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            assert_eq!(seen.len(), 2);
            for headers in &seen {
                assert_eq!(
                    header(headers, "x-amz-copy-source-if-match"),
                    Some("\"source-etag\"")
                );
                let named = header(headers, "x-amz-copy-source").unwrap_or_default();
                assert!(named.starts_with("source/object.bin"), "{named}");
                assert_eq!(
                    named.contains("?versionId=v%201"),
                    version.is_some(),
                    "{named}"
                );
                assert_eq!(named.contains("versionId"), version.is_some(), "{named}");
            }
            assert_eq!(
                header(&seen[0], "x-amz-metadata-directive"),
                Some("REPLACE")
            );
            assert_eq!(header(&seen[0], "x-amz-tagging-directive"), Some("REPLACE"));
            assert_eq!(
                header(&seen[1], "x-amz-copy-source-range"),
                Some("bytes=0-9")
            );
        }
    }

    /// A range the source no longer holds (it shrank) is `InvalidRange`, 416: the source changed,
    /// a permanent `Conflict`, not an unknown protocol failure.
    #[test]
    fn an_invalid_range_is_a_permanent_conflict() {
        let failure = s3_role_remote_failure(Some(416), Some("InvalidRange"), "copy");
        assert_eq!(
            failure,
            S3ProtocolFailure::entry(
                crate::model::FailureClass::Conflict,
                crate::model::Transience::Permanent,
                "copy",
            )
        );
    }
}
