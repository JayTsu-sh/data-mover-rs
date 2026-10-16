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
use crate::storage::backends::s3::client::url_options::{S3BucketType, S3Compatibility};
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

/// What the server answers a `HEAD` with: the described object, another one, or an error status.
#[derive(Clone, Copy, Debug)]
enum HeadAnswer {
    Described,
    Replaced,
    Status(u16),
}

/// Records whether each GET carried `If-Match` and how many `HEAD`s came, and answers a GET with
/// its [`Answer`], a `HEAD` with its [`HeadAnswer`].
#[derive(Clone, Debug)]
struct RangeServer {
    if_match: Arc<Mutex<Vec<Option<String>>>>,
    heads: Arc<Mutex<u32>>,
    answer: Answer,
    head: HeadAnswer,
}

impl RangeServer {
    fn new(answer: Answer) -> Self {
        Self::with_head(answer, HeadAnswer::Described)
    }

    fn with_head(answer: Answer, head: HeadAnswer) -> Self {
        Self {
            if_match: Arc::default(),
            heads: Arc::default(),
            answer,
            head,
        }
    }

    fn heads(&self) -> u32 {
        *self.heads.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn head_response(&self) -> Result<HttpResponse, ConnectorError> {
        *self.heads.lock().unwrap_or_else(PoisonError::into_inner) += 1;
        let (status, etag) = match self.head {
            HeadAnswer::Described => (200, "\"nas-1\""),
            HeadAnswer::Replaced => (200, "\"nas-2\""),
            HeadAnswer::Status(status) => (status, "\"nas-1\""),
        };
        StatusCode::try_from(status)
            .map(|status| {
                let mut response = HttpResponse::new(status, SdkBody::empty());
                let headers = response.headers_mut();
                headers.insert("etag", etag);
                headers.insert("last-modified", GOOD.last_modified);
                headers.insert("content-length", "100");
                response
            })
            .map_err(|error| ConnectorError::other(Box::new(error), None))
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
        if request.method() == "HEAD" {
            return HttpConnectorFuture::ready(self.head_response());
        }
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

fn nas(server: &RangeServer) -> crate::storage::backends::s3::client::S3Storage {
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

/// On a NAS bucket another `ETag`, another size in `Content-Range` or another `Last-Modified` is
/// the object replaced during the read: a permanent `Conflict`, known without a `HEAD`.
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
    ] {
        let server = RangeServer::new(answer.clone());
        let read = nas(&server).get_range("key", 0..10, &described()).await;
        assert!(is_conflict(&read), "{answer:?}: {read:?}");
        assert_eq!(server.heads(), 0, "{answer:?}");
    }
}

/// A NAS body shorter than its `Content-Length` is followed by one `HEAD`: the described object
/// means the connection was lost (transient, retried); another object a replacement (a permanent
/// `Conflict`); a failed `HEAD` keeps its own class.
#[tokio::test]
async fn a_short_nas_body_is_classified_by_one_head() {
    let short = Answer {
        body: b"01234",
        ..GOOD
    };
    let cases = [
        (
            HeadAnswer::Described,
            FailureClass::Connectivity,
            Transience::Transient,
        ),
        (
            HeadAnswer::Replaced,
            FailureClass::Conflict,
            Transience::Permanent,
        ),
        (
            HeadAnswer::Status(404),
            FailureClass::NotFound,
            Transience::Permanent,
        ),
        (
            HeadAnswer::Status(403),
            FailureClass::PermissionDenied,
            Transience::Permanent,
        ),
    ];
    for (head, expected_class, expected_transience) in cases {
        let server = RangeServer::with_head(short.clone(), head);
        let read = nas(&server).get_range("key", 0..10, &described()).await;
        let classified = match read {
            Err(
                S3ProtocolFailure::Entry {
                    class, transience, ..
                }
                | S3ProtocolFailure::Session {
                    class, transience, ..
                },
            ) => Some((class, transience)),
            Ok(_) => None,
        };
        assert_eq!(
            classified,
            Some((expected_class, expected_transience)),
            "{head:?}"
        );
        assert_eq!(server.heads(), 1, "{head:?}");
    }
}
