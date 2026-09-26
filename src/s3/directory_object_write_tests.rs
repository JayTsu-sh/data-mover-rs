//! P7 D13 on the wire: no multipart upload — streamed or `UploadPartCopy` — is ever sent to a key
//! ending in '/'. The role protocol and the legacy writers refuse one before any request, as
//! `Unsupported`; the same calls on a key without the '/' do reach the server.

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

use super::{DEFAULT_BLOCK_SIZE, DEFAULT_TRANSFER_CONCURRENCY, S3Storage, S3UrlOptions};
use crate::error::StorageError;
use crate::model::{FailureClass, Transience};
use crate::storage::backends::s3::{S3Protocol as _, S3ProtocolFailure};

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
        options: S3UrlOptions::default(),
        endpoint: "http://d13.test".into(),
        bucket_name: "destination".into(),
        prefix: None,
        client: aws_sdk_s3::Client::from_conf(config),
        hcp_client: None,
        block_size: DEFAULT_BLOCK_SIZE,
        transfer_concurrency: DEFAULT_TRANSFER_CONCURRENCY,
        is_bucket_versioned: false,
        versioning_suspended: false,
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

fn unsupported(result: &crate::Result<()>) -> bool {
    matches!(result, Err(StorageError::UnsupportedType(reason)) if reason.contains("p/d/"))
}

/// The legacy writers — `create_multipart_upload`, `multipart_begin`, the `UploadPartCopy`
/// rename, the resumable path (which would take an open upload over) and
/// `complete_multipart_upload` (whoever began the upload) — refuse a key ending in '/' as
/// `UnsupportedType` before any request.
#[tokio::test]
async fn the_legacy_writers_never_begin_a_multipart_upload_to_a_directory_object() {
    let wire = Wire::default();
    let storage = storage(&wire);
    let begun = storage
        .create_multipart_upload("p/d/", None)
        .await
        .map(drop);
    assert!(unsupported(&begun), "{begun:?}");
    let begun = storage.multipart_begin("p/d/", None).await.map(drop);
    assert!(unsupported(&begun), "{begun:?}");
    let copied = storage
        .multipart_copy_object("src", "p/d/", 6 * 1024 * 1024, 5 * 1024 * 1024, None)
        .await;
    assert!(unsupported(&copied), "{copied:?}");
    let resumed = storage
        .prepare_resumable_upload("p/d/", 6 * 1024 * 1024, 5 * 1024 * 1024, None, None)
        .await
        .map(drop);
    assert!(unsupported(&resumed), "{resumed:?}");
    let completed = storage
        .complete_multipart_upload("p/d/", "u", &[])
        .await
        .map(drop);
    assert!(unsupported(&completed), "{completed:?}");
    assert_eq!(wire.requests(), 0);
    let begun = storage.create_multipart_upload("p/d", None).await;
    assert_eq!(begun.ok().as_deref(), Some("u"));
    assert_eq!(wire.requests(), 1);
}
