//! How the role protocol classifies a service that asks for fewer requests (P7 C3).

use super::s3_role_remote_failure;
use crate::model::{FailureClass, Transience};
use crate::storage::backends::s3::S3ProtocolFailure;

/// The service asking for fewer requests is `Throttled` / `Transient` (P7 §8), not a lost
/// connection: by status (429, 503) or by code, whatever status carried it. Other 5xx stay
/// `Connectivity`. The scope stays the session's; a listing backs off and scopes it to itself.
#[test]
fn throttling_is_its_own_transient_class() {
    for (status, code) in [
        (Some(503), Some("SlowDown")),
        (Some(503), None),
        (Some(429), None),
        (Some(400), Some("RequestLimitExceeded")),
        (None, Some("SlowDown")),
        (Some(500), Some("ServiceUnavailable")),
    ] {
        assert!(
            matches!(
                s3_role_remote_failure(status, code, "list"),
                S3ProtocolFailure::Session {
                    class: FailureClass::Throttled,
                    transience: Transience::Transient,
                    ..
                }
            ),
            "{status:?} {code:?}"
        );
    }
    for status in [500, 502, 504] {
        assert!(matches!(
            s3_role_remote_failure(Some(status), None, "list"),
            S3ProtocolFailure::Session {
                class: FailureClass::Connectivity,
                ..
            }
        ));
    }
}

#[test]
fn sdk_retryable_request_timeouts_keep_their_transient_class() {
    for code in ["RequestTimeout", "RequestTimeoutException"] {
        assert!(matches!(
            s3_role_remote_failure(Some(400), Some(code), "read"),
            S3ProtocolFailure::Session {
                class: FailureClass::Connectivity,
                transience: Transience::Transient,
                ..
            }
        ));
    }
}
