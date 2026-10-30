//! P7 D13 on the wire: no multipart upload — streamed or `UploadPartCopy` — is ever sent to a key
//! ending in '/'. The role protocol refuses one before any request, as `Unsupported`; the same
//! call on a key without the '/' does reach the server.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

use super::{S3Storage, S3UrlOptions};
use crate::model::{FailureClass, Transience};
use crate::storage::backends::s3::{S3Protocol as _, S3ProtocolFailure, S3VersioningAtConnect};

/// Counts the requests that reach the wire and answers each as `CreateMultipartUpload` would.
#[derive(Clone, Debug, Default)]
struct Wire {
    requests: Arc<AtomicUsize>,
}

impl Wire {
    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

impl HttpConnector for Wire {
    fn call(&self, _request: HttpRequest) -> HttpConnectorFuture {
        self.requests.fetch_add(1, Ordering::SeqCst);
        let body = "<InitiateMultipartUploadResult><UploadId>u</UploadId>\
                    </InitiateMultipartUploadResult>";
        let response = StatusCode::try_from(200)
            .map(|status| HttpResponse::new(status, SdkBody::from(body)))
            .map_err(|error| ConnectorError::other(Box::new(error), None));
        HttpConnectorFuture::ready(response)
    }
}

fn storage(wire: &Wire) -> S3Storage {
    let wire = wire.clone();
    let http_client =
        http_client_fn(move |_settings, _components| SharedHttpConnector::new(wire.clone()));
    let config = Builder::new()
        .behavior_version_latest()
        .credentials_provider(Credentials::new("ak", "sk", None, None, "d13-test"))
        .region(Region::new("us-east-1"))
        .endpoint_url("http://d13.test")
        .force_path_style(true)
        .http_client(http_client)
        .build();
    S3Storage {
        read_chunk_bytes: 8 * 1024 * 1024,
        transfer_concurrency: crate::transfer_concurrency::TransferConcurrency::defaults(4, 4),
        options: S3UrlOptions::default(),
        endpoint: "http://d13.test".into(),
        bucket_name: "destination".into(),
        prefix: None,
        client: aws_sdk_s3::Client::from_conf(config),
        single_attempt_client: std::sync::OnceLock::new(),
        http_client_capture: crate::backend::client::qos_client::HttpClientCapture::default(),
        is_bucket_versioned: false,
        versioning_suspended: false,
        versioning_at_connect: S3VersioningAtConnect::Never,
    }
}

fn refused<T>(result: &Result<T, S3ProtocolFailure>) -> bool {
    matches!(
        result,
        Err(S3ProtocolFailure::Entry {
            class: FailureClass::Unsupported,
            transience: Transience::Permanent,
            ..
        })
    )
}

/// The role protocol refuses `CreateMultipartUpload` and `CompleteMultipartUpload` on `p/d/`
/// unsent — that entry's `Unsupported` — also where the storage prefix makes the key sent end in
/// '/' (the root under `pre/`), and sends them for `p/d`.
#[tokio::test]
async fn the_role_protocol_never_begins_a_multipart_upload_to_a_directory_object() {
    let wire = Wire::default();
    let mut storage = storage(&wire);
    assert!(refused(&storage.begin_multipart("p/d/").await));
    assert!(refused(
        &storage
            .complete_multipart("p/d/", "u", &[(1, "\"e\"".to_string())])
            .await
    ));
    storage.prefix = Some("pre/".to_string());
    assert!(refused(&storage.begin_multipart("").await));
    storage.prefix = None;
    assert_eq!(wire.requests(), 0);
    assert_eq!(storage.begin_multipart("p/d").await, Ok("u".to_string()));
    assert_eq!(wire.requests(), 1);
}
