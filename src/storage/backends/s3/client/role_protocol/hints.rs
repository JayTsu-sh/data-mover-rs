//! Hints when a URL does not say `bucket_type=nas` but the bucket is an ONTAP NAS bucket (P7 §14.2
//! N6). Without the profile such a bucket fails as it did before it: a conditional GET,
//! `CreateMultipartUpload` or `CopyObject` answers 501, `ListMultipartUploads` / `ListParts` 403.
//! The failure keeps its class; its diagnostic says what to add to the URL. A 501 has no other
//! reading on any store, so it always carries the hint; a 403 is also an ordinary policy denial,
//! so it carries it only where the URL already says `compat=ontap`.

use crate::model::FailureClass;
use crate::storage::backends::s3::S3ProtocolFailure;
use crate::storage::backends::s3::client::url_options::{
    S3BucketType, S3Compatibility, S3UrlOptions,
};

const HINT: &str = "if this is an ONTAP NAS bucket, add ?compat=ontap&bucket_type=nas to the URL";

fn with_hint(failure: S3ProtocolFailure) -> S3ProtocolFailure {
    match failure {
        S3ProtocolFailure::Entry {
            class,
            transience,
            diagnostic,
        } => S3ProtocolFailure::Entry {
            class,
            transience,
            diagnostic: format!("{diagnostic} ({HINT})"),
        },
        S3ProtocolFailure::Session {
            class,
            transience,
            diagnostic,
        } => S3ProtocolFailure::Session {
            class,
            transience,
            diagnostic: format!("{diagnostic} ({HINT})"),
        },
    }
}

fn class(failure: &S3ProtocolFailure) -> FailureClass {
    match failure {
        S3ProtocolFailure::Entry { class, .. } | S3ProtocolFailure::Session { class, .. } => *class,
    }
}

/// A request an ONTAP NAS bucket does not implement (a conditional GET, `CreateMultipartUpload`,
/// `CopyObject` / `UploadPartCopy`), refused with 501 (`Unsupported`) on a URL without the NAS
/// profile: the hint is added.
pub(super) fn not_implemented(
    options: S3UrlOptions,
    failure: S3ProtocolFailure,
) -> S3ProtocolFailure {
    if options.bucket_type == S3BucketType::Object && class(&failure) == FailureClass::Unsupported {
        with_hint(failure)
    } else {
        failure
    }
}

/// `ListMultipartUploads` / `ListParts` refused with 403 (`PermissionDenied`) on a `compat=ontap`
/// URL without `bucket_type=nas`: the hint is added (anywhere else a 403 is a policy denial).
pub(super) fn denied_listing(
    options: S3UrlOptions,
    failure: S3ProtocolFailure,
) -> S3ProtocolFailure {
    if options.compat == S3Compatibility::Ontap
        && options.bucket_type == S3BucketType::Object
        && class(&failure) == FailureClass::PermissionDenied
    {
        with_hint(failure)
    } else {
        failure
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Transience;

    fn options(compat: S3Compatibility, bucket_type: S3BucketType) -> S3UrlOptions {
        S3UrlOptions {
            compat,
            bucket_type,
            ..S3UrlOptions::default()
        }
    }

    fn diagnostic(failure: &S3ProtocolFailure) -> &str {
        match failure {
            S3ProtocolFailure::Entry { diagnostic, .. }
            | S3ProtocolFailure::Session { diagnostic, .. } => diagnostic,
        }
    }

    /// A 501 carries the hint under any compat, but not once the URL says `bucket_type=nas`; a
    /// 403 of a listing carries it only under `compat=ontap`; other classes never; the class and
    /// transience stay.
    #[test]
    fn hints_go_where_a_nas_bucket_is_likely() {
        let unsupported =
            || S3ProtocolFailure::entry(FailureClass::Unsupported, Transience::Permanent, "cmu");
        let denied = || {
            S3ProtocolFailure::entry(FailureClass::PermissionDenied, Transience::Permanent, "lmu")
        };
        let standard = options(S3Compatibility::Standard, S3BucketType::Object);
        let ontap = options(S3Compatibility::Ontap, S3BucketType::Object);
        let nas = options(S3Compatibility::Ontap, S3BucketType::Nas);
        let hinted = not_implemented(standard, unsupported());
        assert!(diagnostic(&hinted).contains("bucket_type=nas"));
        assert_eq!(class(&hinted), FailureClass::Unsupported);
        assert!(diagnostic(&not_implemented(ontap, unsupported())).contains("bucket_type=nas"));
        assert_eq!(not_implemented(nas, unsupported()), unsupported());
        assert_eq!(not_implemented(standard, denied()), denied());
        assert!(diagnostic(&denied_listing(ontap, denied())).contains("bucket_type=nas"));
        assert_eq!(denied_listing(standard, denied()), denied());
        assert_eq!(denied_listing(nas, denied()), denied());
        assert_eq!(denied_listing(ontap, unsupported()), unsupported());
    }
}
