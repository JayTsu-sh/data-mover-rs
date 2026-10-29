//! An HTTP success with an unreadable versioning body is no usable answer, not a refusal.

use std::sync::Arc;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_smithy_runtime_api::client::http::{
    HttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::retry::RetryConfig;

use crate::model::{BackendSessionFailure, FailureClass, Transience};
use crate::storage::backends::s3::client::S3Storage;
use crate::storage::backends::s3::client::tests::make_test_storage;
use crate::storage::backends::s3::client::url_options::S3UrlOptions;
use crate::storage::backends::s3::{S3VersioningAtConnect, connect, tests::identity};

#[derive(Clone, Debug)]
struct VersioningResponse {
    status: u16,
    body: &'static str,
}

impl HttpConnector for VersioningResponse {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        assert!(request.uri().contains("versioning"));
        let reply = self.clone();
        HttpConnectorFuture::new(async move {
            let status = StatusCode::try_from(reply.status).unwrap_or_else(|e| panic!("{e}"));
            let body = SdkBody::from(reply.body);
            Ok(HttpResponse::new(status, body))
        })
    }
}

fn client(status: u16, body: &'static str) -> Client {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url("http://replay.invalid")
        .credentials_provider(Credentials::new("ak", "sk", None, None, "replay"))
        .retry_config(RetryConfig::disabled())
        .http_client(http_client_fn(move |_, _| {
            SharedHttpConnector::new(VersioningResponse { status, body })
        }))
        .build();
    Client::from_conf(config)
}

#[tokio::test]
async fn unreadable_success_response_refuses_role_connection_as_transient()
-> Result<(), Box<dyn std::error::Error>> {
    let mut storage = make_test_storage(None);
    // A different root is rejected by the operation decoder, then wrapped as an unmodeled
    // ServiceError. This exercises the real SDK shape, not just our pure classification seam.
    storage.client = client(200, "<UnexpectedResponse/>");
    let error = storage
        .client
        .get_bucket_versioning()
        .bucket("test-bucket")
        .send()
        .await
        .err()
        .ok_or("expected SDK decode failure")?;
    assert!(error.as_service_error().is_some(), "{error:?}");
    assert_eq!(
        error
            .raw_response()
            .map(|response| response.status().as_u16()),
        Some(200)
    );
    storage.versioning_at_connect = S3Storage::detect_bucket_versioning(
        &storage.client,
        "test-bucket",
        "http://replay.invalid",
        S3UrlOptions::default(),
    )
    .await?;
    assert_eq!(
        storage.versioning_at_connect,
        S3VersioningAtConnect::Unreachable { status: Some(200) }
    );
    let Err(error) = connect(Arc::new(storage), identity(), None) else {
        panic!("a malformed versioning answer must not silently enable histories");
    };
    let failure = error
        .downcast_ref::<BackendSessionFailure>()
        .ok_or("expected a typed session failure")?;
    assert_eq!(failure.class(), FailureClass::Connectivity);
    assert_eq!(failure.transience(), Transience::Transient);
    Ok(())
}

#[tokio::test]
async fn genuine_versioning_answers_refusals_and_unsupported_responses_keep_their_meaning()
-> Result<(), Box<dyn std::error::Error>> {
    let cases = [
        (
            200,
            "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
            S3VersioningAtConnect::Enabled,
        ),
        (
            200,
            "<VersioningConfiguration/>",
            S3VersioningAtConnect::Never,
        ),
        (
            403,
            "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>",
            S3VersioningAtConnect::Refused,
        ),
        (
            501,
            "<Error><Code>NotImplemented</Code><Message>unsupported</Message></Error>",
            S3VersioningAtConnect::NoVersioning,
        ),
    ];
    for (status, body, expected) in cases {
        let actual = S3Storage::detect_bucket_versioning(
            &client(status, body),
            "test-bucket",
            "http://replay.invalid",
            S3UrlOptions::default(),
        )
        .await?;
        assert_eq!(actual, expected, "HTTP {status}: {body}");
    }
    Ok(())
}
