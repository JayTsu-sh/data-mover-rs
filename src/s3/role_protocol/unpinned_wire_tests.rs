//! The ranged `GET` on the wire (P7 §14.2): an object bucket pins it with `If-Match`; an ONTAP NAS
//! bucket, which answers any conditional GET with 501, is sent none and has the response checked.

use std::sync::{Arc, Mutex, PoisonError};

use aws_smithy_runtime_api::client::http::{HttpConnector, HttpConnectorFuture};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;

use super::native::tests::storage;
use crate::model::{FailureClass, Transience};
use crate::s3::url_options::{S3BucketType, S3Compatibility};
use crate::storage::backends::s3::{S3ObjectFacts, S3Protocol as _, S3ProtocolFailure};
use crate::time_util::http_last_modified;

/// What the server answers a ranged GET with: `ETag`, `Content-Range`, `Last-Modified`, and the
/// body (which may be shorter than the `Content-Length` it announces).
#[derive(Clone, Debug)]
struct Answer {
    etag: &'static str,
    content_range: &'static str,
    last_modified: &'static str,
    content_length: &'static str,
    body: &'static [u8],
}

const GOOD: Answer = Answer {
    etag: "\"nas-1\"",
    content_range: "bytes 0-9/100",
    last_modified: "Sat, 26 Sep 2026 10:00:00 GMT",
    content_length: "10",
    body: b"0123456789",
};

/// Records whether each request carried `If-Match`, and answers with its [`Answer`].
#[derive(Clone, Debug)]
struct RangeServer {
    if_match: Arc<Mutex<Vec<Option<String>>>>,
    answer: Answer,
}

impl RangeServer {
    fn new(answer: Answer) -> Self {
        Self {
            if_match: Arc::default(),
            answer,
        }
    }

    fn if_match_sent(&self) -> Vec<Option<String>> {
        self.if_match
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl HttpConnector for RangeServer {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        self.if_match
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(request.headers().get("if-match").map(str::to_string));
        let answer = self.answer.clone();
        let response = StatusCode::try_from(206)
            .map(|status| {
                let mut response = HttpResponse::new(status, SdkBody::from(answer.body));
                let headers = response.headers_mut();
                headers.insert("etag", answer.etag);
                headers.insert("content-range", answer.content_range);
                headers.insert("last-modified", answer.last_modified);
                headers.insert("content-length", answer.content_length);
                response
            })
            .map_err(|error| ConnectorError::other(Box::new(error), None));
        HttpConnectorFuture::ready(response)
    }
}

fn described() -> S3ObjectFacts {
    S3ObjectFacts {
        size: 100,
        etag: "\"nas-1\"".into(),
        version_id: None,
        // 2026-09-26T10:00:00Z, the answer's `Last-Modified`.
        last_modified: http_last_modified(1_790_416_800),
    }
}

fn nas(server: &RangeServer) -> crate::s3::S3Storage {
    let mut storage = storage(server.clone());
    storage.options.compat = S3Compatibility::Ontap;
    storage.options.bucket_type = S3BucketType::Nas;
    storage
}

fn is_conflict(result: &crate::storage::backends::s3::S3Result<bytes::Bytes>) -> bool {
    matches!(
        result,
        Err(S3ProtocolFailure::Entry {
            class: FailureClass::Conflict,
            transience: Transience::Permanent,
            ..
        })
    )
}

/// An object bucket pins the read with `If-Match`; a NAS bucket sends no condition and accepts a
/// response that describes the described object.
#[tokio::test]
async fn only_an_object_bucket_sends_if_match() {
    let server = RangeServer::new(GOOD);
    let read = storage(server.clone())
        .get_range("key", 0..10, &described())
        .await;
    assert_eq!(read.ok().as_deref(), Some(&b"0123456789"[..]));
    assert_eq!(server.if_match_sent(), vec![Some("\"nas-1\"".to_string())]);

    let server = RangeServer::new(GOOD);
    let read = nas(&server).get_range("key", 0..10, &described()).await;
    assert_eq!(read.ok().as_deref(), Some(&b"0123456789"[..]));
    assert_eq!(server.if_match_sent(), vec![None]);
}

/// On a NAS bucket another `ETag`, another size in `Content-Range`, another `Last-Modified`, or a
/// body shorter than announced is the object replaced during the read: a permanent `Conflict`.
#[tokio::test]
async fn a_nas_response_that_is_not_the_described_object_is_a_conflict() {
    for answer in [
        Answer {
            etag: "\"nas-2\"",
            ..GOOD
        },
        Answer {
            content_range: "bytes 0-9/101",
            ..GOOD
        },
        Answer {
            last_modified: "Sat, 26 Sep 2026 10:00:01 GMT",
            ..GOOD
        },
        Answer {
            body: b"01234",
            ..GOOD
        },
    ] {
        let server = RangeServer::new(answer.clone());
        let read = nas(&server).get_range("key", 0..10, &described()).await;
        assert!(is_conflict(&read), "{answer:?}: {read:?}");
    }
}
