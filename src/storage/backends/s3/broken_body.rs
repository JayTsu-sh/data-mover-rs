//! A ranged read without conditional reads (P7 §14.2: an ONTAP NAS bucket) whose body broke off —
//! shorter than its `Content-Length` / `Content-Range`, or the connection reset mid-body — is
//! told apart by one `HEAD` sent after it (coordinator decision 2026-09-27):
//!
//! - another object than described ([`object_change`]: another `ETag`, size or — when both have
//!   one — `Last-Modified`): the object was replaced — N0 saw ONTAP cut in-flight GETs 0.3–0.7 s
//!   after a replacement commits — a permanent `Conflict`, the class a 412 gets where reads are
//!   pinned;
//! - the described object: an ordinary lost connection, as transient as a pinned read's broken
//!   body, so the read is retried;
//! - the `HEAD` failed: its own failure, which says more about the object or the connection now
//!   than the broken body does.

use crate::model::{FailureClass, Transience};

use super::{S3ObjectFacts, S3ProtocolFailure, S3Result, S3SeenObject, object_change};

/// The failure of an unpinned ranged read of `described` whose body broke off (`cause` says how),
/// given what the `HEAD` sent after it answered.
pub(crate) fn broken_body_failure(
    head: S3Result<S3ObjectFacts>,
    described: &S3ObjectFacts,
    cause: &str,
) -> S3ProtocolFailure {
    match head {
        Ok(current) if object_change(&S3SeenObject::of(&current), described).is_none() => {
            S3ProtocolFailure::session(
                FailureClass::Connectivity,
                Transience::Transient,
                format!(
                    "the S3 object's body broke off ({cause}); a HEAD found it unchanged: the \
                     connection was lost"
                ),
            )
        }
        Ok(_) => S3ProtocolFailure::entry(
            FailureClass::Conflict,
            Transience::Permanent,
            format!(
                "the S3 object's body broke off ({cause}); a HEAD found it changed: it was \
                 replaced during the read"
            ),
        ),
        Err(S3ProtocolFailure::Entry {
            class,
            transience,
            diagnostic,
        }) => S3ProtocolFailure::entry(
            class,
            transience,
            format!("the S3 object's body broke off ({cause}); the HEAD after it: {diagnostic}"),
        ),
        Err(S3ProtocolFailure::Session {
            class,
            transience,
            diagnostic,
        }) => S3ProtocolFailure::session(
            class,
            transience,
            format!("the S3 object's body broke off ({cause}); the HEAD after it: {diagnostic}"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{StorageTimestamp, TimePrecision};

    /// `Last-Modified` at `seconds` past the epoch (whole seconds, as HTTP gives it).
    fn http_last_modified(seconds: i64) -> Option<StorageTimestamp> {
        StorageTimestamp::new(i128::from(seconds) * 1_000_000_000, TimePrecision::Seconds).ok()
    }

    fn described() -> S3ObjectFacts {
        S3ObjectFacts {
            size: 10,
            etag: "\"nas-1\"".into(),
            version_id: None,
            last_modified: http_last_modified(1_790_000_000),
        }
    }

    fn class_of(failure: &S3ProtocolFailure) -> (bool, FailureClass, Transience) {
        match failure {
            S3ProtocolFailure::Entry {
                class, transience, ..
            } => (true, *class, *transience),
            S3ProtocolFailure::Session {
                class, transience, ..
            } => (false, *class, *transience),
        }
    }

    /// The described object (quotes aside, `Last-Modified` absent on one side) is a lost
    /// connection; another `ETag`, size or `Last-Modified` a replacement.
    #[test]
    fn the_head_tells_a_lost_connection_from_a_replacement() {
        let lost = (false, FailureClass::Connectivity, Transience::Transient);
        let replaced = (true, FailureClass::Conflict, Transience::Permanent);
        let same = described();
        let unquoted = S3ObjectFacts {
            etag: "nas-1".into(),
            last_modified: None,
            ..described()
        };
        for current in [same, unquoted] {
            let failure = broken_body_failure(Ok(current), &described(), "5 of 10 bytes");
            assert_eq!(class_of(&failure), lost, "{failure:?}");
        }
        for current in [
            S3ObjectFacts {
                etag: "\"nas-2\"".into(),
                ..described()
            },
            S3ObjectFacts {
                size: 11,
                ..described()
            },
            S3ObjectFacts {
                last_modified: http_last_modified(1_790_000_001),
                ..described()
            },
        ] {
            let failure = broken_body_failure(Ok(current), &described(), "5 of 10 bytes");
            assert_eq!(class_of(&failure), replaced, "{failure:?}");
        }
    }

    /// A failed `HEAD` keeps its own scope, class and transience, and says what broke first.
    #[test]
    fn a_failed_head_keeps_its_own_class() {
        for head in [
            S3ProtocolFailure::entry(FailureClass::NotFound, Transience::Permanent, "gone"),
            S3ProtocolFailure::session(
                FailureClass::Connectivity,
                Transience::Transient,
                "timed out",
            ),
            S3ProtocolFailure::entry(
                FailureClass::PermissionDenied,
                Transience::Permanent,
                "denied",
            ),
        ] {
            let failure = broken_body_failure(Err(head.clone()), &described(), "reset");
            assert_eq!(class_of(&failure), class_of(&head));
            let (S3ProtocolFailure::Entry { diagnostic, .. }
            | S3ProtocolFailure::Session { diagnostic, .. }) = failure;
            assert!(diagnostic.contains("(reset)"), "{diagnostic}");
        }
    }
}
