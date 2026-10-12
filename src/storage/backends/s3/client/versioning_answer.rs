//! What `GetBucketVersioning` answered when a bucket was connected (P7 Q26, Q-A): the one request
//! every connection makes, whose answer decides whether the role-based listing gives each key its
//! history.

use aws_sdk_s3::types::BucketVersioningStatus;

use super::versioning_is_unsupported;
use crate::storage::backends::s3::S3VersioningAtConnect;

/// Error codes that mean "try again", whatever their HTTP status (S3 answers `RequestTimeout`
/// with 400).
const TRANSIENT_CODES: [&str; 6] = [
    "RequestTimeout",
    "SlowDown",
    "Throttling",
    "ThrottlingException",
    "InternalError",
    "ServiceUnavailable",
];

/// The answer `status` names: an unknown status is treated as a refusal to say (histories are
/// listed: correct whatever the bucket's state, only slower).
pub(super) fn answered(status: Option<&BucketVersioningStatus>) -> S3VersioningAtConnect {
    match status {
        None => S3VersioningAtConnect::Never,
        Some(BucketVersioningStatus::Enabled) => S3VersioningAtConnect::Enabled,
        Some(BucketVersioningStatus::Suspended) => S3VersioningAtConnect::Suspended,
        Some(_) => S3VersioningAtConnect::Refused,
    }
}

/// A failed `GetBucketVersioning`, by its error code, HTTP status and whether the request went
/// out but yielded no usable answer (`lost`: a timeout, dispatch failure, or unreadable response):
/// no versioning at all (501, `NotImplemented`, `UnsupportedOperation`); no answer (lost, throttling,
/// a server error or a code that asks to retry); otherwise a refusal to say — 403 and every other
/// answer, and a request that could not be built at all (it fails again, with its own failure,
/// when the bucket is listed; calling it transient would have a caller connect in a loop).
pub(super) fn failed(code: Option<&str>, status: Option<u16>, lost: bool) -> S3VersioningAtConnect {
    // The SDK wraps XML decode failures in GetBucketVersioningError::Unhandled,
    // surfaced as ServiceError even for HTTP 200. Success status plus an error
    // is an unusable answer, not a server refusal or proof of unsupported versioning.
    if matches!(status, Some(200..=299)) {
        return S3VersioningAtConnect::Unreachable { status };
    }
    if versioning_is_unsupported(code, status) {
        return S3VersioningAtConnect::NoVersioning;
    }
    let unreachable = S3VersioningAtConnect::Unreachable { status };
    if lost || code.is_some_and(|code| TRANSIENT_CODES.contains(&code)) {
        return unreachable;
    }
    match status {
        Some(408 | 429 | 500..=599) => unreachable,
        None | Some(_) => S3VersioningAtConnect::Refused,
    }
}

#[cfg(test)]
mod tests {
    use super::{BucketVersioningStatus, S3VersioningAtConnect, answered, failed};

    #[test]
    fn a_status_names_the_state_and_no_status_is_never_versioned() {
        assert_eq!(answered(None), S3VersioningAtConnect::Never);
        assert_eq!(
            answered(Some(&BucketVersioningStatus::Enabled)),
            S3VersioningAtConnect::Enabled
        );
        assert_eq!(
            answered(Some(&BucketVersioningStatus::Suspended)),
            S3VersioningAtConnect::Suspended
        );
        let unknown = BucketVersioningStatus::from("Paused");
        assert_eq!(answered(Some(&unknown)), S3VersioningAtConnect::Refused);
    }

    #[test]
    fn a_failure_is_no_versioning_no_answer_or_a_refusal() {
        use S3VersioningAtConnect::{NoVersioning, Refused, Unreachable};
        let lost = |status| Unreachable { status };
        let cases = [
            (Some("NotImplemented"), Some(501), false, NoVersioning),
            (None, Some(501), false, NoVersioning),
            (Some("UnsupportedOperation"), Some(400), false, NoVersioning),
            (Some("AccessDenied"), Some(403), false, Refused),
            (Some("NoSuchBucket"), Some(404), false, Refused),
            (None, Some(400), false, Refused),
            (None, Some(408), false, lost(Some(408))),
            (None, Some(200), true, lost(Some(200))),
            (None, Some(200), false, lost(Some(200))),
            (None, Some(204), false, lost(Some(204))),
            // A request that could not be built fails again when listed; not transient.
            (None, None, false, Refused),
            (None, None, true, lost(None)),
            (Some("InternalError"), Some(500), false, lost(Some(500))),
            (None, Some(503), false, lost(Some(503))),
            (Some("SlowDown"), Some(503), false, lost(Some(503))),
            (None, Some(429), false, lost(Some(429))),
            (Some("RequestTimeout"), Some(400), false, lost(Some(400))),
        ];
        for (code, status, lost, expected) in cases {
            assert_eq!(
                failed(code, status, lost),
                expected,
                "{code:?} {status:?} {lost}"
            );
        }
    }
}

#[cfg(test)]
#[path = "versioning_response_tests.rs"]
mod response_tests;
