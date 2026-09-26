//! The streamed `PutObject` on the wire (P7 §14.2 N3): `Content-Length` = the object's size, an
//! unsigned payload, no `Content-MD5`, no chunked or `aws-chunked` body, the bytes as sent; a body
//! the caller abandons never completes; on a NAS bucket a file / directory clash is a name
//! conflict.

use std::future::poll_fn;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use aws_smithy_runtime_api::client::http::{HttpConnector, HttpConnectorFuture};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use bytes::Bytes;
use http_body::Body as _;
use tokio::sync::mpsc;

use super::super::native::tests::storage;
use super::{put_object, put_object_streaming};
use crate::model::{FailureClass, Transience};
use crate::s3::url_options::{S3BucketType, S3Compatibility};
use crate::storage::backends::s3::S3ProtocolFailure;

/// A request as the server saw it: its headers, and its body when the body arrived whole.
type Request = (Vec<(String, String)>, Result<Vec<u8>, String>);
type Seen = Arc<Mutex<Vec<Request>>>;

/// Reads each request's body to its end, then answers with `status` (and `code` in an S3 error
/// body when it is not 200).
#[derive(Clone, Debug)]
struct PutServer {
    seen: Seen,
    status: u16,
    code: &'static str,
}

impl PutServer {
    fn answering(status: u16, code: &'static str) -> Self {
        Self {
            seen: Arc::default(),
            status,
            code,
        }
    }

    fn seen(&self) -> Vec<Request> {
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl HttpConnector for PutServer {
    fn call(&self, mut request: HttpRequest) -> HttpConnectorFuture {
        let headers: Vec<(String, String)> = request
            .headers()
            .iter()
            .map(|(name, value)| (name.to_ascii_lowercase(), value.to_string()))
            .collect();
        let mut body = request.take_body();
        let seen = Arc::clone(&self.seen);
        let (status, code) = (self.status, self.code);
        HttpConnectorFuture::new(async move {
            let mut bytes = Vec::new();
            let mut outcome = Ok(());
            while let Some(frame) = poll_fn(|context| Pin::new(&mut body).poll_frame(context)).await
            {
                match frame {
                    Ok(frame) => {
                        if let Ok(data) = frame.into_data() {
                            bytes.extend_from_slice(&data);
                        }
                    }
                    Err(error) => {
                        outcome = Err(error.to_string());
                        break;
                    }
                }
            }
            let failed = outcome.clone().err();
            seen.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((headers, outcome.map(|()| bytes)));
            if let Some(error) = failed {
                return Err(ConnectorError::io(error.into()));
            }
            let body = if status == 200 {
                SdkBody::empty()
            } else {
                SdkBody::from(format!("<Error><Code>{code}</Code></Error>"))
            };
            StatusCode::try_from(status)
                .map(|status| {
                    let mut response = HttpResponse::new(status, body);
                    response
                        .headers_mut()
                        .insert("etag", "\"0123456789abcdef0123456789abcdef\"");
                    response
                })
                .map_err(|error| ConnectorError::other(Box::new(error), None))
        })
    }
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(found, _)| found == name)
        .map(|(_, value)| value.as_str())
}

/// Sends `chunks` into a streamed PUT of `len` bytes, dropping the sender after them.
async fn put(
    storage: &crate::s3::S3Storage,
    len: u64,
    chunks: &[&'static [u8]],
) -> crate::storage::backends::s3::S3Result<crate::storage::backends::s3::S3StreamedPut> {
    let (sender, receiver) = mpsc::channel(1);
    let chunks: Vec<Bytes> = chunks
        .iter()
        .map(|chunk| Bytes::from_static(chunk))
        .collect();
    let feeder = tokio::spawn(async move {
        for chunk in chunks {
            if sender.send(chunk).await.is_err() {
                break;
            }
        }
    });
    let result = put_object_streaming(storage, "key", len, receiver).await;
    let _ = feeder.await;
    result
}

/// The request states the object's size, signs no payload, carries no digest and no chunked
/// encoding, and delivers the bytes as sent; the reply's `ETag` is reported, comparable as an MD5.
#[tokio::test]
async fn a_streamed_put_states_its_length_and_signs_no_payload() {
    let server = PutServer::answering(200, "");
    let reply = put(&storage(server.clone()), 10, &[b"01234", b"56789"]).await;
    let reply = reply.ok().map(|put| (put.facts.etag, put.etag_is_md5));
    assert_eq!(
        reply,
        Some(("\"0123456789abcdef0123456789abcdef\"".to_string(), true))
    );
    let seen = server.seen();
    assert_eq!(seen.len(), 1, "one request, no retry");
    let (headers, body) = &seen[0];
    assert_eq!(header(headers, "content-length"), Some("10"));
    assert_eq!(
        header(headers, "x-amz-content-sha256"),
        Some("UNSIGNED-PAYLOAD")
    );
    assert_eq!(header(headers, "content-md5"), None);
    assert_eq!(header(headers, "transfer-encoding"), None);
    assert_eq!(header(headers, "content-encoding"), None);
    assert_eq!(body.as_deref(), Ok(&b"0123456789"[..]));
}

/// A body abandoned before its declared length never arrives whole: the request fails, and the
/// server never saw a complete body.
#[tokio::test]
async fn an_abandoned_body_never_completes() {
    let server = PutServer::answering(200, "");
    let reply = put(&storage(server.clone()), 10, &[b"01234"]).await;
    assert!(reply.is_err(), "{reply:?}");
    assert!(server.seen().iter().all(|(_, body)| body.is_err()));
}

/// On a NAS bucket a PUT refused because a file and a directory clash — 404 `NoSuchKey`, 409
/// `NameCollision` — is that entry's permanent `Conflict`; on an object bucket a 404 stays
/// `NotFound`.
#[tokio::test]
async fn a_nas_name_clash_is_a_permanent_conflict() {
    for (status, code) in [(404, "NoSuchKey"), (409, "NameCollision")] {
        let server = PutServer::answering(status, code);
        let mut nas = storage(server);
        nas.options.compat = S3Compatibility::Ontap;
        nas.options.bucket_type = S3BucketType::Nas;
        let reply = put(&nas, 10, &[b"0123456789"]).await;
        assert!(
            matches!(
                reply,
                Err(S3ProtocolFailure::Entry {
                    class: FailureClass::Conflict,
                    transience: Transience::Permanent,
                    ref diagnostic,
                }) if diagnostic.contains("clash")
            ),
            "{status}: {reply:?}"
        );
    }
    let reply = put(
        &storage(PutServer::answering(404, "NoSuchKey")),
        10,
        &[b"0123456789"],
    )
    .await;
    assert!(
        matches!(
            reply,
            Err(S3ProtocolFailure::Entry {
                class: FailureClass::NotFound,
                ..
            })
        ),
        "{reply:?}"
    );
}

/// On a NAS bucket another 404 code (`NoSuchBucket`) is no name clash: it keeps its own mapping.
#[tokio::test]
async fn another_nas_404_is_no_name_clash() {
    let mut nas = storage(PutServer::answering(404, "NoSuchBucket"));
    nas.options.compat = S3Compatibility::Ontap;
    nas.options.bucket_type = S3BucketType::Nas;
    let reply = put(&nas, 10, &[b"0123456789"]).await;
    assert!(
        matches!(
            reply,
            Err(S3ProtocolFailure::Entry {
                class: FailureClass::NotFound,
                ..
            })
        ),
        "{reply:?}"
    );
}

/// The buffered `PutObject` maps a NAS name clash the same way.
#[tokio::test]
async fn a_nas_name_clash_on_a_buffered_put_is_a_permanent_conflict() {
    for (status, code) in [(404, "NoSuchKey"), (409, "NameCollision")] {
        let mut nas = storage(PutServer::answering(status, code));
        nas.options.compat = S3Compatibility::Ontap;
        nas.options.bucket_type = S3BucketType::Nas;
        let reply = put_object(
            &nas,
            "key",
            Bytes::from_static(b"x"),
            "LYPNsHeaiHdHPRi0lN+BWA==",
        )
        .await;
        assert!(
            matches!(
                reply,
                Err(S3ProtocolFailure::Entry {
                    class: FailureClass::Conflict,
                    transience: Transience::Permanent,
                    ..
                })
            ),
            "{status}: {reply:?}"
        );
    }
}
