//! Actual SDK attempts must distinguish transport loss, safe service retries and uncertain writes.
use std::io::{Error as IoError, ErrorKind};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use aws_config::{BehaviorVersion, SdkConfig};
use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_sdk_s3::{Client, config::retry::RetryConfig, primitives::ByteStream, types::Tagging};
use aws_smithy_runtime_api::client::http::{
    HttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use aws_types::region::Region;

use super::{S3Compatibility, build_s3_config};

#[derive(Clone, Copy, Debug)]
enum Reply {
    Network,
    Timeout,
    Response,
    ServiceThenOk,
    Service,
    ServiceTimeout(&'static str),
}

#[derive(Clone, Debug)]
struct Attempts {
    calls: Arc<AtomicUsize>,
    reply: Reply,
}
impl HttpConnector for Attempts {
    fn call(&self, _: HttpRequest) -> HttpConnectorFuture {
        let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
        let failure = IoError::from(ErrorKind::ConnectionReset);
        let result = match self.reply {
            Reply::Network => Err(ConnectorError::io(Box::new(failure))),
            Reply::Timeout => Err(ConnectorError::timeout(Box::new(failure))),
            Reply::Response => Ok(response(200, "broken xml")),
            Reply::ServiceTimeout(code) => {
                Ok(response(400, format!("<Error><Code>{code}</Code></Error>")))
            }
            Reply::ServiceThenOk if attempt > 0 => Ok(response(200, "<ListBucketResult/>")),
            _ => Ok(response(503, "<Error><Code>SlowDown</Code></Error>")),
        };
        HttpConnectorFuture::ready(result)
    }
}
fn response(status: u16, body: impl Into<SdkBody>) -> HttpResponse {
    let mut response = HttpResponse::new(StatusCode::try_from(status).unwrap(), body.into());
    response
        .headers_mut()
        .insert("content-type", "application/xml");
    response.headers_mut().insert("etag", "\"part\"");
    response
}
fn client(reply: Reply) -> (Client, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let connector = Attempts {
        calls: calls.clone(),
        reply,
    };
    let sdk = SdkConfig::builder()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            "ak",
            "sk",
            None,
            None,
            "retry-test",
        )))
        .region(Region::new("us-east-1"))
        .endpoint_url("http://retry.test")
        .http_client(http_client_fn(move |_, _| {
            SharedHttpConnector::new(connector.clone())
        }))
        .retry_config(
            RetryConfig::standard()
                .with_max_attempts(3)
                .with_initial_backoff(Duration::from_millis(1)),
        )
        .build();
    (
        Client::from_conf(build_s3_config(&sdk, S3Compatibility::Standard)),
        calls,
    )
}

#[tokio::test]
async fn sdk_network_timeout_and_response_failures_are_not_retried() {
    for reply in [
        Reply::Network,
        Reply::Timeout,
        Reply::Response,
        Reply::ServiceTimeout("RequestTimeout"),
        Reply::ServiceTimeout("RequestTimeoutException"),
    ] {
        let (client, calls) = client(reply);
        assert!(
            client
                .list_objects_v2()
                .bucket("bucket")
                .send()
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1, "{reply:?}");
    }
}
#[tokio::test]
async fn sdk_safe_reads_and_parts_keep_service_retries() {
    let (client, calls) = client(Reply::ServiceThenOk);
    assert!(
        client
            .list_objects_v2()
            .bucket("bucket")
            .send()
            .await
            .is_ok()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let (client, calls) = self::client(Reply::ServiceThenOk);
    assert!(
        client
            .upload_part()
            .bucket("bucket")
            .key("key")
            .upload_id("u")
            .part_number(1)
            .body(ByteStream::from_static(b"part"))
            .send()
            .await
            .is_ok()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn sdk_uncertain_writes_are_never_replayed() {
    for operation in 0..7 {
        let (client, calls) = client(Reply::Service);
        let failed = uncertain_write(&client, operation).await;
        assert!(failed);
        assert_eq!(calls.load(Ordering::SeqCst), 1, "operation {operation}");
    }
}
async fn uncertain_write(client: &Client, operation: u8) -> bool {
    match operation {
        0 => client
            .put_object()
            .bucket("bucket")
            .key("key")
            .body(ByteStream::from_static(b"part"))
            .send()
            .await
            .is_err(),
        1 => client
            .copy_object()
            .bucket("bucket")
            .key("key")
            .copy_source("bucket/source")
            .send()
            .await
            .is_err(),
        2 => client
            .create_multipart_upload()
            .bucket("bucket")
            .key("key")
            .send()
            .await
            .is_err(),
        3 => client
            .complete_multipart_upload()
            .bucket("bucket")
            .key("key")
            .upload_id("u")
            .send()
            .await
            .is_err(),
        _ => other_uncertain_write(client, operation).await,
    }
}
async fn other_uncertain_write(client: &Client, operation: u8) -> bool {
    match operation {
        4 => client
            .delete_object()
            .bucket("bucket")
            .key("key")
            .send()
            .await
            .is_err(),
        5 => client
            .delete_object()
            .bucket("bucket")
            .key("key")
            .version_id("null")
            .send()
            .await
            .is_err(),
        _ => client
            .put_object_tagging()
            .bucket("bucket")
            .key("key")
            .tagging(
                Tagging::builder()
                    .set_tag_set(Some(Vec::new()))
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .is_err(),
    }
}
#[tokio::test]
async fn sdk_pinned_deletes_keep_service_retries() {
    let (client, calls) = client(Reply::ServiceThenOk);
    assert!(
        client
            .delete_object()
            .bucket("bucket")
            .key("key")
            .version_id("immutable-version")
            .send()
            .await
            .is_ok()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
