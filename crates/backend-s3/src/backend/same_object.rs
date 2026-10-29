//! Whether a later response still shows the object that was described, on a bucket without
//! conditional reads (P7 §14.2: an ONTAP NAS bucket). One rule for the three places that ask: the
//! headers of an unpinned ranged `GET` (`role_protocol/unpinned.rs`), the `HEAD` after a body that
//! broke off ([`super::broken_body_failure`]) and the `HEAD` after the last range
//! (`source.rs::confirm_unchanged`):
//!
//! - the `ETag` must be there and the described one, surrounding quotes ignored (on a NAS bucket it
//!   is derived from the file's mtime and size, so every S3 or NFS write changes it);
//! - the size must be the described one, when the response gave one (a `HEAD` always does; a
//!   ranged `GET` through its `Content-Range` total);
//! - the `Last-Modified` must be the described one when both have one (whole seconds).

use crate::model::StorageTimestamp;

use super::S3ObjectFacts;

/// What a later response said about an object; `None` for a fact it did not give.
#[derive(Clone, Copy, Debug)]
pub struct S3SeenObject<'a> {
    pub etag: Option<&'a str>,
    pub size: Option<u64>,
    pub last_modified: Option<StorageTimestamp>,
}

impl<'a> S3SeenObject<'a> {
    /// Everything a `HEAD` described.
    #[must_use]
    pub fn of(facts: &'a S3ObjectFacts) -> Self {
        Self {
            etag: Some(&facts.etag),
            size: Some(facts.size),
            last_modified: facts.last_modified,
        }
    }
}

/// The first fact, in the order `ETag`, size, `Last-Modified`, on which a response shows another
/// object than the described one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum S3ObjectChange {
    ETag,
    Size,
    LastModified,
}

/// How `seen` differs from `described`, or `None` when it is the described object.
#[must_use]
pub fn object_change(seen: &S3SeenObject<'_>, described: &S3ObjectFacts) -> Option<S3ObjectChange> {
    let same_etag = seen
        .etag
        .is_some_and(|etag| etag.trim_matches('"') == described.etag.trim_matches('"'));
    if !same_etag {
        return Some(S3ObjectChange::ETag);
    }
    if seen.size.is_some_and(|size| size != described.size) {
        return Some(S3ObjectChange::Size);
    }
    match (seen.last_modified, described.last_modified) {
        (Some(now), Some(then)) if now != then => Some(S3ObjectChange::LastModified),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::TimePrecision;

    /// `Last-Modified` at `seconds` past the epoch (whole seconds, as HTTP gives it).
    fn at(seconds: i64) -> Option<StorageTimestamp> {
        StorageTimestamp::new(i128::from(seconds) * 1_000_000_000, TimePrecision::Seconds).ok()
    }

    fn described() -> S3ObjectFacts {
        S3ObjectFacts {
            size: 10,
            etag: "\"nas-1\"".into(),
            version_id: None,
            last_modified: at(1_790_000_000),
        }
    }

    fn seen(etag: Option<&str>, size: Option<u64>, seconds: Option<i64>) -> Option<S3ObjectChange> {
        let seen = S3SeenObject {
            etag,
            size,
            last_modified: seconds.and_then(at),
        };
        object_change(&seen, &described())
    }

    /// Quotes, an absent size and a `Last-Modified` on one side only are not held against the
    /// object; an absent or other `ETag`, another size or `Last-Modified` are, `ETag` first.
    #[test]
    fn the_first_differing_fact_is_the_change() {
        assert_eq!(
            object_change(&S3SeenObject::of(&described()), &described()),
            None
        );
        assert_eq!(seen(Some("nas-1"), None, None), None);
        assert_eq!(seen(Some("\"nas-1\""), Some(10), Some(1_790_000_000)), None);
        assert_eq!(seen(None, Some(10), None), Some(S3ObjectChange::ETag));
        assert_eq!(
            seen(Some("nas-2"), Some(11), None),
            Some(S3ObjectChange::ETag)
        );
        assert_eq!(
            seen(Some("nas-1"), Some(11), Some(1)),
            Some(S3ObjectChange::Size)
        );
        assert_eq!(
            seen(Some("nas-1"), Some(10), Some(1_790_000_001)),
            Some(S3ObjectChange::LastModified)
        );
        let undated = S3ObjectFacts {
            last_modified: None,
            ..described()
        };
        let later = S3SeenObject {
            last_modified: at(1),
            ..S3SeenObject::of(&undated)
        };
        assert_eq!(object_change(&later, &undated), None);
    }
}
