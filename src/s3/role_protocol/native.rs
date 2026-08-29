use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use tokio_util::sync::CancellationToken;

use super::{
    COPY_PART_SIZE, CompletedPart, ProvideErrorMetadata, S3NativeCopyEvidence, S3NativeCopyFailure,
    S3NativeCopyResult, S3NativeCopySource, S3ProtocolFailure, S3Result, S3Storage,
    build_copy_source, s3_role_remote_failure, s3_role_transport_failure,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CopyStrategy {
    Single,
    Multipart,
}

const fn strategy(size: u64) -> CopyStrategy {
    if size <= super::COPY_SINGLE_MAX {
        CopyStrategy::Single
    } else {
        CopyStrategy::Multipart
    }
}

pub(super) async fn copy(
    storage: &S3Storage,
    source: &S3NativeCopySource,
    to: &str,
    multipart_upload_id: Option<&str>,
    cancel: &CancellationToken,
) -> S3NativeCopyResult {
    match strategy(source.size) {
        CopyStrategy::Single => copy_single(storage, source, to, cancel).await,
        CopyStrategy::Multipart => {
            let upload_id = multipart_upload_id.ok_or_else(|| {
                failure(
                    S3ProtocolFailure::protocol("native multipart copy has no owned upload ID"),
                    0,
                    0,
                )
            })?;
            copy_multipart(storage, source, to, upload_id, cancel).await
        }
    }
}

fn copy_source(source: &S3NativeCopySource) -> String {
    let mut value = build_copy_source(&source.bucket, &source.key);
    if let Some(version) = &source.version_id {
        value.push_str("?versionId=");
        value.push_str(&utf8_percent_encode(version, NON_ALPHANUMERIC).to_string());
    }
    value
}

fn cancelled() -> S3ProtocolFailure {
    S3ProtocolFailure::entry(
        crate::model::FailureClass::Cancelled,
        crate::model::Transience::Permanent,
        "S3 native copy cancelled",
    )
}

fn failure(error: S3ProtocolFailure, bytes: u64, requests: u64) -> S3NativeCopyFailure {
    S3NativeCopyFailure {
        error,
        bytes,
        requests,
    }
}

pub(super) async fn copy_single(
    storage: &S3Storage,
    source: &S3NativeCopySource,
    to: &str,
    cancel: &CancellationToken,
) -> S3NativeCopyResult {
    if cancel.is_cancelled() {
        return Err(failure(cancelled(), 0, 0));
    }
    storage
        .client
        .copy_object()
        .bucket(&storage.bucket_name)
        .key(storage.build_full_key(to))
        .copy_source(copy_source(source))
        .copy_source_if_match(&source.etag)
        .send()
        .await
        .map_err(|error| {
            failure(
                classify_sdk!(error, "S3 native CopyObject request failed"),
                0,
                1,
            )
        })?;
    if cancel.is_cancelled() {
        return Err(failure(cancelled(), source.size, 1));
    }
    Ok(S3NativeCopyEvidence {
        bytes: source.size,
        requests: 1,
    })
}

pub(super) async fn copy_multipart(
    storage: &S3Storage,
    source: &S3NativeCopySource,
    to: &str,
    upload_id: &str,
    cancel: &CancellationToken,
) -> S3NativeCopyResult {
    copy_multipart_with_part_size(storage, source, to, upload_id, COPY_PART_SIZE, cancel).await
}

pub(super) async fn copy_multipart_with_part_size(
    storage: &S3Storage,
    source: &S3NativeCopySource,
    to: &str,
    upload_id: &str,
    part_size: u64,
    cancel: &CancellationToken,
) -> S3NativeCopyResult {
    if cancel.is_cancelled() {
        return Err(failure(cancelled(), 0, 0));
    }
    let key = storage.build_full_key(to);
    let (parts, copied_bytes, part_requests) =
        copy_parts(storage, source, &key, upload_id, part_size, cancel).await?;
    storage
        .complete_multipart_upload(&key, upload_id, &parts)
        .await
        .map_err(|error| {
            failure(
                S3ProtocolFailure::protocol(error.to_string()),
                copied_bytes,
                part_requests + 1,
            )
        })?;
    Ok(S3NativeCopyEvidence {
        bytes: source.size,
        requests: part_requests + 1,
    })
}

async fn copy_parts(
    storage: &S3Storage,
    source: &S3NativeCopySource,
    key: &str,
    upload_id: &str,
    part_size: u64,
    cancel: &CancellationToken,
) -> Result<(Vec<CompletedPart>, u64, u64), S3NativeCopyFailure> {
    let ranges = super::super::multipart_rename::copy_part_ranges(source.size, part_size);
    let mut parts = Vec::with_capacity(ranges.len());
    let mut copied_bytes = 0;
    let mut requests = 0;
    for (index, (start, end)) in ranges.into_iter().enumerate() {
        if cancel.is_cancelled() {
            return Err(failure(cancelled(), copied_bytes, requests));
        }
        let number = i32::try_from(index + 1).map_err(|_| {
            failure(
                S3ProtocolFailure::protocol("native multipart part overflow"),
                copied_bytes,
                requests,
            )
        })?;
        let part = copy_part(storage, source, key, upload_id, number, start..=end)
            .await
            .map_err(|error| failure(error, copied_bytes, requests + 1))?;
        copied_bytes += end - start + 1;
        requests += 1;
        parts.push(part);
    }
    Ok((parts, copied_bytes, requests))
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

    #[derive(Clone, Copy, Debug)]
    enum ResponseMode {
        Success,
        FailParts,
        FailComplete,
        CancelAfterFirst,
    }

    #[derive(Clone, Debug)]
    struct MultipartConnector {
        methods: Arc<Mutex<Vec<String>>>,
        mode: ResponseMode,
        cancel: CancellationToken,
    }

    impl HttpConnector for MultipartConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let method = request.method().to_string();
            self.methods
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(method.clone());
            if matches!(self.mode, ResponseMode::CancelAfterFirst) {
                self.cancel.cancel();
            }
            if matches!(self.mode, ResponseMode::FailParts) && method == "PUT"
                || matches!(self.mode, ResponseMode::FailComplete) && method == "POST"
            {
                return connector_failure();
            }
            connector_response(&method)
        }
    }

    fn connector_failure() -> HttpConnectorFuture {
        HttpConnectorFuture::ready(Err(ConnectorError::io(Box::new(std::io::Error::other(
            "injected request failure",
        )))))
    }

    fn connector_response(method: &str) -> HttpConnectorFuture {
        let body = if method == "PUT" {
            r"<CopyPartResult><ETag>&quot;part&quot;</ETag></CopyPartResult>"
        } else {
            r"<CompleteMultipartUploadResult><ETag>&quot;complete&quot;</ETag></CompleteMultipartUploadResult>"
        };
        let response = StatusCode::try_from(200)
            .map(|status| HttpResponse::new(status, SdkBody::from(body)))
            .map_err(|error| ConnectorError::other(Box::new(error), None));
        HttpConnectorFuture::ready(response)
    }

    fn storage(connector: MultipartConnector) -> S3Storage {
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
            key: "large.bin".into(),
            etag: "\"source-etag\"".into(),
            version_id: None,
            size: super::super::COPY_SINGLE_MAX + 1,
        }
    }

    #[test]
    fn native_strategy_changes_only_above_copy_object_limit() {
        assert_eq!(
            strategy(super::super::COPY_SINGLE_MAX),
            CopyStrategy::Single
        );
        assert_eq!(
            strategy(super::super::COPY_SINGLE_MAX + 1),
            CopyStrategy::Multipart
        );
        let ranges = super::super::super::multipart_rename::copy_part_ranges(
            super::super::COPY_SINGLE_MAX + 1,
            COPY_PART_SIZE,
        );
        assert!(ranges.len() > 1);
        assert_eq!(ranges.first().map(|range| range.0), Some(0));
        assert_eq!(
            ranges.last().map(|range| range.1),
            Some(super::super::COPY_SINGLE_MAX)
        );
        assert_eq!(
            u64::try_from(ranges.len()).ok().map(|parts| parts + 2),
            Some(8)
        );
    }

    #[tokio::test]
    async fn multipart_requests_complete_and_report_all_native_calls() {
        let methods = Arc::new(Mutex::new(Vec::new()));
        let cancel = CancellationToken::new();
        let connector = MultipartConnector {
            methods: Arc::clone(&methods),
            mode: ResponseMode::Success,
            cancel: cancel.clone(),
        };
        let evidence = copy(
            &storage(connector),
            &source(),
            "stage",
            Some("upload-1"),
            &cancel,
        )
        .await
        .unwrap_or_else(|error| panic!("multipart copy failed: {error:?}"));
        // The stage owner accounts for CreateMultipartUpload separately.
        assert_eq!(evidence.requests, 7);
        let methods = methods
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(methods.iter().filter(|method| *method == "PUT").count(), 6);
        assert_eq!(methods.iter().filter(|method| *method == "POST").count(), 1);
    }

    #[tokio::test]
    async fn multipart_part_and_complete_failures_leave_abort_to_stage_owner() {
        for mode in [ResponseMode::FailParts, ResponseMode::FailComplete] {
            let methods = Arc::new(Mutex::new(Vec::new()));
            let cancel = CancellationToken::new();
            let connector = MultipartConnector {
                methods: Arc::clone(&methods),
                mode,
                cancel: cancel.clone(),
            };
            let Err(failure) = copy(
                &storage(connector),
                &source(),
                "stage",
                Some("owned"),
                &cancel,
            )
            .await
            else {
                panic!("injected multipart failure succeeded");
            };
            match mode {
                ResponseMode::FailParts => {
                    assert_eq!(failure.bytes, 0);
                    assert_eq!(failure.requests, 1);
                }
                ResponseMode::FailComplete => {
                    assert_eq!(failure.bytes, source().size);
                    assert_eq!(failure.requests, 7);
                }
                _ => unreachable!("test only exercises failure modes"),
            }
            let methods = methods
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(!methods.iter().any(|method| method == "DELETE"));
        }
    }

    #[tokio::test]
    async fn multipart_cancellation_stops_before_another_part() {
        let methods = Arc::new(Mutex::new(Vec::new()));
        let cancel = CancellationToken::new();
        let connector = MultipartConnector {
            methods: Arc::clone(&methods),
            mode: ResponseMode::CancelAfterFirst,
            cancel: cancel.clone(),
        };
        let Err(failure) = copy(
            &storage(connector),
            &source(),
            "stage",
            Some("owned"),
            &cancel,
        )
        .await
        else {
            panic!("cancelled multipart copy succeeded");
        };
        assert_eq!(failure.bytes, COPY_PART_SIZE);
        assert_eq!(failure.requests, 1);
        assert_eq!(
            methods
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            1
        );
    }
}
