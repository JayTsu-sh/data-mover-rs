use std::sync::Arc;

use super::{S3BucketType, S3Compatibility, S3Storage, S3UrlOptions};
use crate::model::BackendIdentity;
use crate::storage::Storage;
use crate::storage::backends::s3::{S3BucketCapabilities, S3NativeContext, S3TagSupport};
use crate::storage::endpoint::{self, EndpointError};

/// The canonical endpoint of an S3 storage, from the same parts the backend addresses: its
/// `http(s)://host[:port]` endpoint, bucket, and prefix as stored (empty = bucket root).
pub(super) fn identity(
    http_endpoint: &str,
    bucket: &str,
    prefix: &str,
) -> Result<BackendIdentity, EndpointError> {
    endpoint::s3(
        http_endpoint,
        bucket,
        (!prefix.is_empty()).then_some(prefix),
    )
}

/// Connects the S3 roles of `storage`; sources of at most `single_put_threshold` bytes are
/// written as one `PutObject`.
pub(super) fn connect(
    storage: &S3Storage,
    single_put_threshold: u64,
) -> Result<Storage, Box<dyn std::error::Error>> {
    let identity = identity(
        &storage.endpoint,
        &storage.bucket_name,
        storage.prefix.as_deref().unwrap_or_default(),
    )?;
    let compat = storage.options.compat;
    let capabilities = bucket_capabilities(storage.options);
    let native = native_label(storage.options).map(|label| {
        let bucket = storage.bucket_name.clone();
        let prefix = storage.prefix.clone();
        if capabilities.native_same_bucket_only {
            S3NativeContext::within_bucket(&storage.endpoint, label, bucket, prefix)
        } else {
            S3NativeContext::new(&storage.endpoint, label, bucket, prefix)
        }
    });
    crate::storage::backends::s3::connect_configured(
        Arc::new(storage.clone()),
        identity,
        native,
        match compat {
            S3Compatibility::Dxn => S3TagSupport::Unsupported,
            S3Compatibility::Standard | S3Compatibility::StorageGrid | S3Compatibility::Ontap => {
                S3TagSupport::Supported
            }
        },
        Some(single_put_threshold),
    )
}

/// What the bucket a URL names can do (P7 §14.2): only `bucket_type` changes it — `compat` changes
/// requests, never capabilities.
pub(super) const fn bucket_capabilities(options: S3UrlOptions) -> S3BucketCapabilities {
    match options.bucket_type {
        S3BucketType::Object => S3BucketCapabilities::OBJECT,
        S3BucketType::Nas => S3BucketCapabilities::NAS,
    }
}

/// The native-copy affinity label of a profile: two storages copy natively only on the same
/// endpoint under the same label. `StorageGRID` never copies natively; ONTAP has its own, so a
/// copy between a `compat=ontap` URL and a standard URL of the same endpoint streams, and an ONTAP
/// NAS bucket another one (with its bucket, see [`S3NativeContext::within_bucket`]), so it never
/// pairs with an object bucket.
fn native_label(options: S3UrlOptions) -> Option<&'static str> {
    match (options.compat, options.bucket_type) {
        (S3Compatibility::Ontap, S3BucketType::Nas) => Some("ontap-nas"),
        (S3Compatibility::Standard, _) => Some("standard"),
        (S3Compatibility::Dxn, _) => Some("dxn"),
        (S3Compatibility::Ontap, _) => Some("ontap"),
        (S3Compatibility::StorageGrid, _) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{S3BucketType, S3Compatibility, S3UrlOptions, native_label};

    fn options(compat: S3Compatibility, bucket_type: S3BucketType) -> S3UrlOptions {
        S3UrlOptions {
            compat,
            bucket_type,
            ..S3UrlOptions::default()
        }
    }

    /// Native copies pair only under one label: `compat=ontap` and a standard URL of the same
    /// endpoint stream, `StorageGRID` never copies natively, and an ONTAP NAS bucket has a label of
    /// its own.
    #[test]
    fn native_labels() {
        let object = |compat| native_label(options(compat, S3BucketType::Object));
        assert_eq!(object(S3Compatibility::Standard), Some("standard"));
        assert_eq!(object(S3Compatibility::Dxn), Some("dxn"));
        assert_eq!(object(S3Compatibility::Ontap), Some("ontap"));
        assert_eq!(object(S3Compatibility::StorageGrid), None);
        assert_eq!(
            native_label(options(S3Compatibility::Ontap, S3BucketType::Nas)),
            Some("ontap-nas")
        );
    }
}
