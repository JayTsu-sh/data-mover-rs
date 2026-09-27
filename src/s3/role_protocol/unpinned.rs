//! Ranged reads of a bucket without conditional reads (P7 §14.2: an ONTAP NAS bucket answers any
//! `If-Match` / `If-None-Match` with 501). No condition is sent; the response is checked against
//! what was described instead, and a header that differs means the object was replaced (a
//! permanent `Conflict`, the class a 412 gets where reads are pinned):
//!
//! - its `ETag`, `Content-Range` total and `Last-Modified` must show the described object, by the
//!   rule every NAS read shares ([`object_change`]). On a NAS bucket the `ETag` is derived from
//!   the file's mtime and size, so every S3 or NFS write changes it — one that restores the mtime
//!   does not (N0);
//! - its body must be as long as announced. A NAS bucket cuts a GET 0.3–0.7 s after a replacement
//!   commits, having sent a clean prefix of the old content (N0), but a network drop breaks a body
//!   off too: one `HEAD` after a broken body tells the two apart ([`broken_body_failure`]).

use std::future::Future;

use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_smithy_types::byte_stream::AggregatedBytes;
use bytes::Bytes;

use crate::model::{FailureClass, StorageTimestamp, Transience};
use crate::storage::backends::s3::{
    S3ObjectChange, S3ObjectFacts, S3ProtocolFailure, S3Result, S3SeenObject, broken_body_failure,
    object_change,
};
use crate::time_util::http_last_modified;

/// Why an unpinned read was refused.
fn replaced(diagnostic: impl Into<String>) -> S3ProtocolFailure {
    S3ProtocolFailure::entry(FailureClass::Conflict, Transience::Permanent, diagnostic)
}

/// The body of the response to an unpinned ranged `GET` of `expected` bytes, once its headers
/// describe the described object. A body that broke off is classified by what `head` (one `HEAD`
/// of the key) answers after it.
pub(super) async fn read<H, F>(
    response: GetObjectOutput,
    expected: u64,
    observed: &S3ObjectFacts,
    head: H,
) -> S3Result<Bytes>
where
    H: FnOnce() -> F,
    F: Future<Output = S3Result<S3ObjectFacts>>,
{
    check_headers(
        &UnpinnedResponse {
            etag: response.e_tag(),
            content_range: response.content_range(),
            last_modified: response
                .last_modified()
                .and_then(|time| http_last_modified(time.secs())),
        },
        observed,
    )?;
    let body = response.body.collect().await;
    match checked_body(body.map(AggregatedBytes::into_bytes), expected) {
        Ok(bytes) => Ok(bytes),
        Err(cause) => Err(broken_body_failure(head().await, observed, &cause)),
    }
}

/// The total of a `Content-Range: bytes <first>-<last>/<total>` header; `None` for any other
/// shape, or an unknown total (`*`).
fn content_range_total(header: &str) -> Option<u64> {
    let (_, total) = header.strip_prefix("bytes ")?.rsplit_once('/')?;
    total.parse().ok()
}

/// What the response of an unpinned ranged `GET` said about the object it read.
struct UnpinnedResponse<'a> {
    etag: Option<&'a str>,
    content_range: Option<&'a str>,
    last_modified: Option<StorageTimestamp>,
}

/// Checks the response headers against the described object, before its body is read.
fn check_headers(response: &UnpinnedResponse<'_>, observed: &S3ObjectFacts) -> S3Result<()> {
    let seen = S3SeenObject {
        etag: response.etag,
        size: response.content_range.and_then(content_range_total),
        last_modified: response.last_modified,
    };
    let diagnostic = match object_change(&seen, observed) {
        None => return Ok(()),
        Some(S3ObjectChange::ETag) => {
            "the S3 object's ETag changed during the read: it was replaced"
        }
        Some(S3ObjectChange::Size) => {
            "the S3 object's size changed during the read: it was replaced"
        }
        Some(S3ObjectChange::LastModified) => {
            "the S3 object's Last-Modified changed during the read: it was replaced"
        }
    };
    Err(replaced(diagnostic))
}

/// The body of an unpinned read, which must be `expected` bytes; otherwise how it broke off: cut
/// short, or not readable to its end.
fn checked_body<E: std::fmt::Display>(
    body: Result<Bytes, E>,
    expected: u64,
) -> Result<Bytes, String> {
    match body {
        Ok(bytes) if bytes.len() as u64 == expected => Ok(bytes),
        Ok(bytes) => Err(format!(
            "it ended after {} of {expected} bytes",
            bytes.len()
        )),
        Err(error) => Err(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observed() -> S3ObjectFacts {
        S3ObjectFacts {
            size: 100,
            etag: "\"nas-1\"".into(),
            version_id: None,
            last_modified: http_last_modified(1_790_000_000),
        }
    }

    fn response<'a>(
        etag: Option<&'a str>,
        range: Option<&'a str>,
        secs: Option<i64>,
    ) -> UnpinnedResponse<'a> {
        UnpinnedResponse {
            etag,
            content_range: range,
            last_modified: secs.and_then(http_last_modified),
        }
    }

    fn conflict(result: &S3Result<()>) -> bool {
        matches!(
            result,
            Err(S3ProtocolFailure::Entry {
                class: FailureClass::Conflict,
                transience: Transience::Permanent,
                ..
            })
        )
    }

    /// The described object passes; another `ETag`, size or `Last-Modified` is a permanent
    /// `Conflict`, and so is a response without an `ETag`. A header that is absent, or a total the
    /// server does not know, is not held against the read.
    #[test]
    fn a_response_must_describe_the_described_object() {
        let facts = observed();
        let good = Some("bytes 0-9/100");
        assert!(
            check_headers(
                &response(Some("\"nas-1\""), good, Some(1_790_000_000)),
                &facts
            )
            .is_ok()
        );
        assert!(check_headers(&response(Some("nas-1"), None, None), &facts).is_ok());
        assert!(
            check_headers(
                &response(Some("\"nas-1\""), Some("bytes 0-9/*"), None),
                &facts
            )
            .is_ok()
        );
        assert!(conflict(&check_headers(
            &response(Some("\"nas-2\""), good, None),
            &facts
        )));
        assert!(conflict(&check_headers(
            &response(None, good, None),
            &facts
        )));
        assert!(conflict(&check_headers(
            &response(Some("\"nas-1\""), Some("bytes 0-9/101"), None),
            &facts
        )));
        assert!(conflict(&check_headers(
            &response(Some("\"nas-1\""), good, Some(1_790_000_001)),
            &facts
        )));
    }

    /// A body shorter than the range, or one that failed mid-way, broke off (and says how).
    #[test]
    fn a_short_or_broken_body_broke_off() {
        let full = checked_body::<&str>(Ok(Bytes::from_static(b"0123456789")), 10);
        assert_eq!(full.ok(), Some(Bytes::from_static(b"0123456789")));
        assert_eq!(
            checked_body::<&str>(Ok(Bytes::from_static(b"01234")), 10),
            Err("it ended after 5 of 10 bytes".to_string())
        );
        assert_eq!(
            checked_body(Err("connection reset"), 10),
            Err("connection reset".to_string())
        );
    }
}
