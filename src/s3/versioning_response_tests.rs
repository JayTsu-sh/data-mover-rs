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
use crate::s3::S3Storage;
use crate::s3::tests::make_test_storage;
use crate::s3::url_options::S3UrlOptions;
use crate::storage::backends::s3::{S3VersioningAtConnect, connect, tests::identity};

#[derive(Clone, Debug)]
struct BrokenVersioningResponse;

impl HttpConnector for BrokenVersioningResponse {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        assert!(request.uri().contains("versioning"));
        HttpConnectorFuture::new(async {
            let status = StatusCode::try_from(200_u16).unwrap_or_else(|e| panic!("{e}"));
            // The SDK tolerates a truncated closing tag after Status. A different root
            // is rejected by the operation deserializer and reliably exercises ResponseError.
            let body = SdkBody::from("<UnexpectedResponse/>");
            Ok(HttpResponse::new(status, body))
        })
    }
}

#[tokio::test]
async fn unreadable_success_response_refuses_role_connection_as_transient()
-> Result<(), Box<dyn std::error::Error>> {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url("http://replay.invalid")
        .credentials_provider(Credentials::new("ak", "sk", None, None, "replay"))
        .retry_config(RetryConfig::disabled())
        .http_client(http_client_fn(|_, _| {
            SharedHttpConnector::new(BrokenVersioningResponse)
        }))
        .build();
    let mut storage = make_test_storage(None);
    storage.client = Client::from_conf(config);
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
