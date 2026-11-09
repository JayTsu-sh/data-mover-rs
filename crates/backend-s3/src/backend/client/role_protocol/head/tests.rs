//! 用真实 SDK 解码器验证删除标记 HEAD 响应，不能仅依赖内存协议替身。
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_smithy_runtime_api::client::http::{
    HttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::{body::SdkBody, retry::RetryConfig};

use crate::model::FailureClass;
use crate::storage::backends::s3::client::tests::make_test_storage;
use crate::storage::backends::s3::{S3Protocol, S3ProtocolFailure};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Clone, Debug)]
struct HeadResponse {
    status: u16,
    headers: Vec<(&'static str, &'static str)>,
}

impl HttpConnector for HeadResponse {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        assert_eq!(request.method(), "HEAD");
        let reply = self.clone();
        HttpConnectorFuture::new(async move {
            let status = StatusCode::try_from(reply.status).unwrap_or_else(|e| panic!("{e}"));
            let mut response = HttpResponse::new(status, SdkBody::empty());
            for (name, value) in reply.headers {
                response.headers_mut().insert(name, value);
            }
            Ok(response)
        })
    }
}

fn storage(status: u16, headers: Vec<(&'static str, &'static str)>) -> impl S3Protocol {
    let reply = HeadResponse { status, headers };
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url("http://replay.invalid")
        .credentials_provider(Credentials::new("ak", "sk", None, None, "replay"))
        .retry_config(RetryConfig::disabled())
        .http_client(http_client_fn(move |_, _| {
            SharedHttpConnector::new(reply.clone())
        }))
        .build();
    let mut storage = make_test_storage(None);
    storage.client = Client::from_conf(config);
    storage
}

fn marker_headers() -> Vec<(&'static str, &'static str)> {
    vec![
        ("x-amz-delete-marker", "true"),
        ("x-amz-version-id", "marker-1"),
        ("last-modified", "Mon, 09 Nov 2026 08:00:00 GMT"),
    ]
}

#[tokio::test]
async fn exact_marker_error_and_success_responses_preserve_non_data_facts() -> TestResult {
    for status in [200, 404, 405] {
        let facts = storage(status, marker_headers())
            .head_version("key", "marker-1")
            .await
            .map_err(|error| format!("{error:?}"))?;
        assert!(facts.delete_marker);
        assert_eq!(facts.version_id.as_deref(), Some("marker-1"));
        assert!(facts.last_modified.is_some());
        assert_eq!(facts.size, 0);
        assert!(facts.etag.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn marker_response_requires_an_explicit_matching_version_and_type() -> TestResult {
    for headers in [
        vec![("x-amz-delete-marker", "true")],
        vec![
            ("x-amz-delete-marker", "true"),
            ("x-amz-version-id", "other"),
        ],
        vec![
            ("x-amz-delete-marker", "unknown"),
            ("x-amz-version-id", "marker-1"),
        ],
    ] {
        let result = storage(405, headers).head_version("key", "marker-1").await;
        assert!(
            matches!(
                result,
                Err(S3ProtocolFailure::Session {
                    class: FailureClass::Protocol,
                    ..
                })
            ),
            "{result:?}"
        );
    }
    for status in [403, 500] {
        assert!(
            storage(status, marker_headers())
                .head_version("key", "marker-1")
                .await
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn current_marker_stays_missing_and_real_empty_data_remains_data() -> TestResult {
    for status in [200, 404] {
        let result = storage(status, marker_headers()).head("key").await;
        assert!(
            matches!(
                result,
                Err(S3ProtocolFailure::Entry {
                    class: FailureClass::NotFound,
                    ..
                })
            ),
            "{result:?}"
        );
    }
    let facts = storage(200, vec![("content-length", "0"), ("etag", "empty-etag")])
        .head("key")
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert!(!facts.delete_marker);
    assert_eq!(facts.size, 0);
    assert_eq!(facts.etag, "empty-etag");
    Ok(())
}
