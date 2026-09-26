//! Ranged reads of a bucket without conditional reads (P7 §14.2: an ONTAP NAS bucket answers any
//! `If-Match` / `If-None-Match` with 501). No condition is sent; the response is checked against
//! what was described instead, and any difference means the object was replaced (a permanent
//! `Conflict`, the class a 412 gets where reads are pinned):
//!
//! - its `ETag` must be the described one (on a NAS bucket it is derived from the file's mtime and
//!   size, so every S3 or NFS write changes it — one that restores the mtime does not, N0);
//! - its `Content-Range` total must be the described size;
//! - its `Last-Modified` must be the described one when both have one (whole seconds);
//! - its body must be as long as announced: a NAS bucket cuts a GET 0.3–0.7 s after a replacement
//!   commits, having sent a clean prefix of the old content (N0), so a short body is that
//!   replacement, not a lost connection to retry.

use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_smithy_types::byte_stream::AggregatedBytes;
use bytes::Bytes;

use crate::model::{FailureClass, StorageTimestamp, Transience};
use crate::storage::backends::s3::{S3ObjectFacts, S3ProtocolFailure, S3Result};
use crate::time_util::http_last_modified;

/// Why an unpinned read was refused.
fn replaced(diagnostic: impl Into<String>) -> S3ProtocolFailure {
    S3ProtocolFailure::entry(FailureClass::Conflict, Transience::Permanent, diagnostic)
}

/// The body of the response to an unpinned ranged `GET` of `expected` bytes, once its headers
/// describe the described object.
pub(super) async fn read(
    response: GetObjectOutput,
    expected: u64,
    observed: &S3ObjectFacts,
) -> S3Result<Bytes> {
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
    checked_body(body.map(AggregatedBytes::into_bytes), expected)
}

/// Whether two `ETag`s are the same, ignoring surrounding quotes.
fn same_etag(left: &str, right: &str) -> bool {
    left.trim_matches('"') == right.trim_matches('"')
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
    if !response
        .etag
        .is_some_and(|etag| same_etag(etag, &observed.etag))
    {
        return Err(replaced(
            "the S3 object's ETag changed during the read: it was replaced",
        ));
    }
    if response
        .content_range
        .and_then(content_range_total)
        .is_some_and(|total| total != observed.size)
    {
        return Err(replaced(
            "the S3 object's size changed during the read: it was replaced",
        ));
    }
    if let (Some(read), Some(described)) = (response.last_modified, observed.last_modified)
        && read != described
    {
        return Err(replaced(
            "the S3 object's Last-Modified changed during the read: it was replaced",
        ));
    }
    Ok(())
}

/// The body of an unpinned read, which must be `expected` bytes: a body cut short, or one that
/// could not be read to its end, is the replacement the store cut the read for (the cause, if
/// any, goes into the diagnostic).
fn checked_body<E: std::fmt::Display>(body: Result<Bytes, E>, expected: u64) -> S3Result<Bytes> {
    match body {
        Ok(bytes) if bytes.len() as u64 == expected => Ok(bytes),
        Ok(bytes) => Err(replaced(format!(
            "the S3 object's body ended after {} of {expected} bytes: it was replaced during the \
             read",
            bytes.len()
        ))),
        Err(error) => Err(replaced(format!(
            "the S3 object's body broke off ({error}): it was replaced during the read"
        ))),
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

    /// A body shorter than the range, or one that failed mid-way, is the replacement's cut.
    #[test]
    fn a_short_or_broken_body_is_a_replacement() {
        let full = checked_body::<&str>(Ok(Bytes::from_static(b"0123456789")), 10);
        assert_eq!(full.ok(), Some(Bytes::from_static(b"0123456789")));
        for body in [Ok(Bytes::from_static(b"01234")), Err("connection reset")] {
            assert!(matches!(
                checked_body(body, 10),
                Err(S3ProtocolFailure::Entry {
                    class: FailureClass::Conflict,
                    transience: Transience::Permanent,
                    ..
                })
            ));
        }
    }
}
