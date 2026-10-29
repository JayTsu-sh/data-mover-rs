//! The version a write reports: as the reply spells it, and `"null"` in a bucket with versioning
//! suspended whose reply names none (`RustFS` 1.0.0), so the upload pointer written there is
//! deleted by version instead of hidden behind a `"null"` delete marker.

use aws_smithy_runtime_api::client::http::{HttpConnector, HttpConnectorFuture};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use bytes::Bytes;

use super::native::tests::storage;
use crate::storage::backends::s3::S3NativeCopySource;
use crate::storage::backends::s3::S3Protocol;

/// Answers every `PutObject` / `CopyObject` with an `ETag`, and with `x-amz-version-id` when it
/// has one.
#[derive(Clone, Debug)]
struct PutReply {
    version: Option<&'static str>,
}

impl HttpConnector for PutReply {
    fn call(&self, _request: HttpRequest) -> HttpConnectorFuture {
        let version = self.version;
        let response = StatusCode::try_from(200)
            .map(|status| {
                // PutObject reads headers only; CopyObject reads the ETag from this body.
                let body = "<CopyObjectResult><ETag>&quot;e&quot;</ETag></CopyObjectResult>";
                let mut response = HttpResponse::new(status, SdkBody::from(body));
                response.headers_mut().insert("etag", "\"e\"");
                if let Some(version) = version {
                    response.headers_mut().insert("x-amz-version-id", version);
                }
                response
            })
            .map_err(|error| ConnectorError::other(Box::new(error), None));
        HttpConnectorFuture::ready(response)
    }
}

async fn reported(suspended: bool, version: Option<&'static str>) -> Option<String> {
    let mut storage = storage(PutReply { version });
    storage.versioning_suspended = suspended;
    let body = Bytes::from_static(b"pointer");
    storage
        .put_object("k.upload", body.into(), "1B2M2Y8AsgTpgAmY7PhCfg==")
        .await
        .map_err(|failure| format!("{failure:?}"))
        .and_then(|facts| {
            // A `"null"` version is never the object's real version.
            match facts.version_id.as_deref() {
                None | Some("v1") => Ok(facts.reported_version),
                other => Err(format!("version_id {other:?}")),
            }
        })
        .unwrap_or_else(|error| Some(format!("error: {error}")))
}

/// `CopyObject` goes through the same rule (so does `CompleteMultipartUpload`).
#[tokio::test]
async fn a_copy_into_a_suspended_bucket_reports_the_null_version() {
    let mut storage = storage(PutReply { version: None });
    storage.versioning_suspended = true;
    let source = S3NativeCopySource {
        bucket: "source".into(),
        key: "object.bin".into(),
        etag: "\"source-etag\"".into(),
        version_id: None,
        size: 1,
    };
    let reported = storage
        .copy_from(&source, "to")
        .await
        .map(|facts| (facts.reported_version, facts.version_id));
    assert_eq!(reported, Ok((Some("null".to_string()), None)));
}

#[tokio::test]
async fn a_suspended_bucket_reports_the_null_version_its_writes_create() {
    // RustFS: suspended, no header — the write made the "null" version all the same.
    assert_eq!(reported(true, None).await.as_deref(), Some("null"));
    // AWS / MinIO: suspended, "null" named.
    assert_eq!(reported(true, Some("null")).await.as_deref(), Some("null"));
    // Never versioned: no version, and none is made up.
    assert_eq!(reported(false, None).await, None);
    // Enabled: the real version, as named.
    assert_eq!(reported(false, Some("v1")).await.as_deref(), Some("v1"));
}
