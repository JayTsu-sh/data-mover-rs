use std::error::Error as StdError;
use std::io::{Error as IoError, ErrorKind};
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll};

use aws_sdk_s3::Client;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_smithy_runtime_api::client::http::{HttpConnector, HttpConnectorFuture};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::{ConnectorError, SdkError};
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use http_body::{Body, Frame};
use hyper::{Error as HttpError, client::conn::http2};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use tokio::io::duplex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::timeout;

use super::native::tests::storage as mock_storage;
use super::*;
use crate::backend::S3ObjectFacts;
use crate::backend::client::url_options::{S3BucketType, S3Compatibility};
use crate::backend::client::{retry_policy, tests::make_test_storage};

#[tokio::test]
async fn truncated_http_body_is_connection_loss_but_malformed_xml_is_protocol()
-> Result<(), Box<dyn StdError>> {
    for (length, expected) in [
        (1000, FailureClass::Connectivity),
        (6, FailureClass::Protocol),
    ] {
        let server = TcpListener::bind("127.0.0.1:0").await?;
        let address = server.local_addr()?;
        let responder = tokio::spawn(async move {
            let (mut socket, _) = server.accept().await?;
            let mut request = Vec::new();
            let mut byte = [0];
            while !request.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).await?;
                request.push(byte[0]);
            }
            socket.write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {length}\r\nConnection: close\r\n\r\nbroken"
            ).as_bytes()).await?;
            socket.shutdown().await
        });
        let mut storage = make_test_storage(None);
        storage.client = Client::from_conf(
            storage
                .client
                .config()
                .to_builder()
                .endpoint_url(format!("http://{address}"))
                .retry_config(RetryConfig::disabled())
                .build(),
        );
        let result = storage.role_list_objects_page("", None).await;
        responder.await??;
        assert!(
            matches!(result, Err(S3ProtocolFailure::Session { class, .. })
            if class == expected),
            "{result:?}"
        );
    }
    Ok(())
}

#[test]
fn response_error_wrapping_an_io_timeout_is_a_timeout() -> Result<(), Box<dyn StdError>> {
    let response = HttpResponse::new(StatusCode::try_from(200)?, SdkBody::empty());
    let error: SdkError<ListObjectsV2Error, HttpResponse> =
        SdkError::response_error(IoError::from(ErrorKind::TimedOut), response);
    let failure = classify_sdk!(error, "S3 response body failed");
    assert!(
        matches!(
            failure,
            S3ProtocolFailure::Session {
                class: FailureClass::Timeout,
                transience: Transience::Transient,
                ..
            }
        ),
        "{failure:?}"
    );
    Ok(())
}

#[derive(Debug)]
struct FailedBody(ErrorKind);
impl Body for FailedBody {
    type Data = Bytes;
    type Error = IoError;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, IoError>>> {
        Poll::Ready(Some(Err(IoError::from(self.0))))
    }
}

#[derive(Clone, Debug)]
struct BodyErrorServer {
    kind: ErrorKind,
    calls: Arc<AtomicUsize>,
}
impl HttpConnector for BodyErrorServer {
    fn call(&self, _: HttpRequest) -> HttpConnectorFuture {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let result = StatusCode::try_from(200)
            .map(|status| {
                let mut response =
                    HttpResponse::new(status, SdkBody::from_body_1_x(FailedBody(self.kind)));
                response
                    .headers_mut()
                    .insert("content-type", "application/xml");
                response.headers_mut().insert("content-length", "1000");
                response.headers_mut().insert("etag", "\"observed\"");
                response
                    .headers_mut()
                    .insert("content-range", "bytes 0-999/1000");
                response
            })
            .map_err(|error| ConnectorError::other(Box::new(error), None));
        HttpConnectorFuture::ready(result)
    }
}

fn body_error_storage(kind: ErrorKind) -> (S3Storage, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut storage = mock_storage(BodyErrorServer {
        kind,
        calls: calls.clone(),
    });
    let builder = storage
        .client
        .config()
        .to_builder()
        .retry_config(RetryConfig::standard().with_max_attempts(3));
    storage.client = Client::from_conf(retry_policy::configure(builder).build());
    (storage, calls)
}

#[tokio::test]
async fn sdk_listing_body_failures_keep_the_typed_cause_without_replay() {
    for (kind, expected) in [
        (ErrorKind::TimedOut, FailureClass::Timeout),
        (ErrorKind::UnexpectedEof, FailureClass::Connectivity),
        (ErrorKind::ConnectionReset, FailureClass::Connectivity),
    ] {
        let (storage, calls) = body_error_storage(kind);
        let result = storage.role_list_objects_page("", None).await;
        assert!(
            matches!(result, Err(S3ProtocolFailure::Session { class, .. })
            if class == expected),
            "{kind:?}: {result:?}"
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }
}

#[tokio::test]
async fn sdk_payload_body_timeout_is_a_timeout_without_replay() {
    let (storage, calls) = body_error_storage(ErrorKind::TimedOut);
    let facts = S3ObjectFacts {
        delete_marker: false,
        size: 1000,
        etag: "\"observed\"".into(),
        version_id: None,
        last_modified: None,
    };
    let result = storage.get_range("key", 0..1000, &facts).await;
    assert!(
        matches!(
            result,
            Err(S3ProtocolFailure::Session {
                class: FailureClass::Timeout,
                ..
            })
        ),
        "{result:?}"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

async fn hyper_keepalive_timeout() -> Result<HttpError, Box<dyn StdError>> {
    let (socket, mut peer) = duplex(1024);
    let server = tokio::spawn(async move {
        // A valid empty HTTP/2 SETTINGS frame, then a peer that never ACKs PING.
        peer.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0]).await?;
        let mut bytes = [0; 1024];
        while peer.read(&mut bytes).await? != 0 {}
        Ok::<_, IoError>(())
    });
    let mut builder = http2::Builder::new(TokioExecutor::new());
    builder
        .timer(TokioTimer::new())
        .keep_alive_interval(Duration::from_millis(10))
        .keep_alive_timeout(Duration::from_millis(5))
        .keep_alive_while_idle(true);
    let (_sender, connection) = builder
        .handshake::<_, SdkBody>(TokioIo::new(socket))
        .await?;
    let error = timeout(Duration::from_secs(1), connection)
        .await?
        .err()
        .ok_or("expected hyper keepalive timeout")?;
    server.await??;
    assert!(error.is_timeout(), "{error:?}");
    Ok(error)
}

#[tokio::test]
async fn sdk_response_wrapping_a_hyper_timeout_preserves_timeout() -> Result<(), Box<dyn StdError>>
{
    let response = HttpResponse::new(StatusCode::try_from(200)?, SdkBody::empty());
    let error: SdkError<ListObjectsV2Error, HttpResponse> =
        SdkError::response_error(hyper_keepalive_timeout().await?, response);
    let failure = classify_sdk!(error, "S3 HTTP/2 response failed");
    assert!(
        matches!(
            failure,
            S3ProtocolFailure::Session {
                class: FailureClass::Timeout,
                ..
            }
        ),
        "{failure:?}"
    );
    Ok(())
}

#[tokio::test]
async fn nas_body_timeout_is_reported_without_a_follow_up_head() {
    let (mut storage, calls) = body_error_storage(ErrorKind::TimedOut);
    storage.options.compat = S3Compatibility::Ontap;
    storage.options.bucket_type = S3BucketType::Nas;
    let facts = S3ObjectFacts {
        delete_marker: false,
        size: 1000,
        etag: "\"observed\"".into(),
        version_id: None,
        last_modified: None,
    };
    let result = storage.get_range("key", 0..1000, &facts).await;
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "a typed body timeout must not wait for a follow-up HEAD"
    );
    assert!(
        matches!(
            result,
            Err(S3ProtocolFailure::Session {
                class: FailureClass::Timeout,
                ..
            })
        ),
        "{result:?}"
    );
}
