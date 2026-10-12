//! `PutObject`: the buffered one (`Content-MD5`), and one streamed (P7 §14.2 N3, F5h): the body comes from a channel as the caller reads
//! it, so an object up to 5 GiB is written in one request without being held in memory.
//!
//! - `Content-Length` is the object's size: a store refuses a chunked body (an ONTAP NAS bucket
//!   answers `Transfer-Encoding: chunked` with 501), and no body shorter than it commits.
//! - The payload is not signed (`x-amz-content-sha256: UNSIGNED-PAYLOAD`): its SHA-256 would have
//!   to be known before the first byte is sent. No `Content-MD5` either, for the same reason: the
//!   caller computes the MD5 as the bytes pass and compares it with the reply's `ETag`. Over
//!   `s3://` (plain HTTP) nothing but that comparison protects the body.
//! - The SDK neither retries (the body cannot be replayed) nor times the request (no operation or
//!   read timeout, no stalled-stream protection): the caller holds the last byte for as long as it
//!   chooses, and watches the body's progress itself.
//! - The request checksum is `WhenRequired` (as on the client, and forced for this request):
//!   anything else turns the body into `aws-chunked` with a trailing checksum, which an ONTAP NAS
//!   bucket refuses.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::config::{Builder, RequestChecksumCalculation, StalledStreamProtectionConfig};
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ServerSideEncryption;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::body::SdkBody;
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use tokio::sync::mpsc;

use super::{
    ProvideErrorMetadata, S3ProtocolFailure, S3Result, S3Storage, s3_role_remote_failure,
    s3_role_transport_failure,
};
use crate::model::{FailureClass, Transience};
use crate::storage::backends::s3::{S3Protocol as _, S3StreamedPut, S3WriteFacts};

/// The body of a streamed `PutObject`: exactly the declared number of bytes, as the caller sends
/// them. The caller going away before all of them were sent is an error, not an end: the request
/// is aborted and the connection closed, so the store never sees a complete body and keeps what
/// it had.
struct ChannelBody {
    chunks: mpsc::Receiver<Bytes>,
    remaining: u64,
}

impl Body for ChannelBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        if self.remaining == 0 {
            return Poll::Ready(None);
        }
        let Some(chunk) = ready!(self.chunks.poll_recv(context)) else {
            return Poll::Ready(Some(Err(io::Error::other(
                "the upload was abandoned before its declared length",
            ))));
        };
        let Some(remaining) = self.remaining.checked_sub(chunk.len() as u64) else {
            return Poll::Ready(Some(Err(io::Error::other(
                "the upload has more bytes than its declared length",
            ))));
        };
        self.remaining = remaining;
        Poll::Ready(Some(Ok(Frame::data(chunk))))
    }

    fn is_end_stream(&self) -> bool {
        self.remaining == 0
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining)
    }
}

/// This request's settings over the client's: no timeout but the connect timeout, no retry, no
/// stalled-stream protection (it would fail a held last byte after a few seconds), and a checksum
/// only where one is required — `PutObject` requires none, so the body stays plain instead of
/// `aws-chunked` with a trailing checksum (an ONTAP NAS bucket refuses that encoding).
fn unwatched() -> Builder {
    Builder::default()
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .timeout_config(
            TimeoutConfig::builder()
                .disable_operation_timeout()
                .disable_operation_attempt_timeout()
                .disable_read_timeout()
                .build(),
        )
        .retry_config(RetryConfig::disabled())
        .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
}

/// Whether a reply's `ETag` can be the MD5 of the body: not when the object is encrypted with a
/// KMS key or a customer key (their `ETag`s are not MD5s).
fn etag_is_md5(encryption: Option<&ServerSideEncryption>, customer_key: bool) -> bool {
    !customer_key
        && !matches!(
            encryption,
            Some(ServerSideEncryption::AwsKms | ServerSideEncryption::AwsKmsDsse)
        )
}

impl S3Storage {
    /// The failure of a `PutObject` (buffered or streamed). On a bucket whose keys are file paths
    /// (an ONTAP NAS bucket, P7 §14.2 N0) a file and a directory can clash on the key's path: a
    /// file `a` where the key `a/x` needs a directory answers 404 `NoSuchKey`, a directory `a` (or
    /// a file under it) where the key `a` or `a/` needs a file answers 409 `NameCollision`. Both
    /// are that entry's name conflict — permanent, neither "not found" nor retryable. The code
    /// decides; a status alone only when no code came (any other 404 / 409 code, such as
    /// `NoSuchBucket`, keeps its own mapping).
    pub(super) fn put_refusal(
        &self,
        status: Option<u16>,
        code: Option<&str>,
        failure: S3ProtocolFailure,
    ) -> S3ProtocolFailure {
        let clash = matches!(
            (status, code),
            (_, Some("NoSuchKey" | "NameCollision")) | (Some(404 | 409), None)
        );
        if !clash || !self.bucket_capabilities().file_backed_keys {
            return failure;
        }
        let (S3ProtocolFailure::Entry { diagnostic, .. }
        | S3ProtocolFailure::Session { diagnostic, .. }) = failure;
        S3ProtocolFailure::entry(
            FailureClass::Conflict,
            Transience::Permanent,
            format!(
                "{diagnostic}: bucket_type=nas: a file and a directory clash on this key's path (a \
                 file where a directory is needed, or a directory where the file must go)"
            ),
        )
    }
}

/// How a `PutObject` (streamed or not) failed, name clashes included ([`S3Storage::put_refusal`]).
fn put_failure(
    storage: &S3Storage,
    error: &SdkError<PutObjectError, HttpResponse>,
    streamed: bool,
) -> S3ProtocolFailure {
    let failure = if streamed {
        classify_sdk!(error, "S3 PutObject (streamed) request failed")
    } else {
        classify_sdk!(error, "S3 PutObject request failed")
    };
    storage.put_refusal(
        error
            .raw_response()
            .map(|response| response.status().as_u16()),
        error
            .as_service_error()
            .and_then(ProvideErrorMetadata::code),
        failure,
    )
}

/// One buffered `PutObject` of `body` to `key`, carrying `Content-MD5` so the server refuses a
/// body corrupted in flight.
pub(super) async fn put_object(
    storage: &S3Storage,
    key: &str,
    body: Bytes,
    content_md5_base64: &str,
) -> S3Result<S3WriteFacts> {
    let response = storage
        .client
        .put_object()
        .bucket(&storage.bucket_name)
        .key(storage.build_full_key(key))
        .body(ByteStream::from(body))
        .content_md5(content_md5_base64)
        .send()
        .await
        .map_err(|error| put_failure(storage, &error, false))?;
    // The object was written; only the response is malformed.
    let etag = response
        .e_tag()
        .ok_or_else(|| S3ProtocolFailure::protocol("S3 PutObject response omitted ETag"))?;
    Ok(storage.write_facts(etag, response.version_id()))
}

/// One `PutObject` of `len` bytes read from `chunks` to `key` (see the module documentation).
pub(super) async fn put_object_streaming(
    storage: &S3Storage,
    key: &str,
    len: u64,
    chunks: mpsc::Receiver<Bytes>,
) -> S3Result<S3StreamedPut> {
    let length = i64::try_from(len).map_err(|_| {
        S3ProtocolFailure::entry(
            FailureClass::InvalidInput,
            Transience::Permanent,
            "the S3 object is too large for one request",
        )
    })?;
    let body = SdkBody::from_body_1_x(ChannelBody {
        chunks,
        remaining: len,
    });
    let response = storage
        .client
        .put_object()
        .bucket(&storage.bucket_name)
        .key(storage.build_full_key(key))
        .content_length(length)
        .body(ByteStream::new(body))
        .customize()
        .config_override(unwatched())
        .disable_payload_signing()
        .send()
        .await
        .map_err(|error| put_failure(storage, &error, true))?;
    // The object was written; only the response is malformed.
    let etag = response
        .e_tag()
        .ok_or_else(|| S3ProtocolFailure::protocol("S3 PutObject response omitted ETag"))?;
    Ok(S3StreamedPut {
        facts: storage.write_facts(etag, response.version_id()),
        etag_is_md5: etag_is_md5(
            response.server_side_encryption(),
            response.sse_customer_algorithm().is_some(),
        ),
    })
}

#[cfg(test)]
#[path = "streaming_put_tests.rs"]
mod tests;
