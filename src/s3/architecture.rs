use std::sync::Arc;

use super::{S3Compatibility, S3Storage};
use crate::model::BackendIdentity;
use crate::storage::Storage;
pub(super) use crate::storage::backends::s3::DEFAULT_SINGLE_PUT_THRESHOLD;
use crate::storage::backends::s3::{S3NativeContext, S3TagSupport};
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
    let native = native_label(compat).map(|label| {
        S3NativeContext::new(
            &storage.endpoint,
            label,
            storage.bucket_name.clone(),
            storage.prefix.clone(),
        )
    });
    crate::storage::backends::s3::connect_configured(
        Arc::new(storage.clone()),
        identity,
        native,
        match compat {
            S3Compatibility::Dxn => S3TagSupport::Unsupported,
            S3Compatibility::Standard | S3Compatibility::StorageGrid | S3Compatibility::Hcp => {
                S3TagSupport::Supported
            }
        },
        Some(single_put_threshold),
    )
}

/// The native-copy affinity label of a profile: two storages copy natively only on the same
/// endpoint under the same label. `StorageGRID` never copies natively; HCP keeps the standard
/// label (its `CopyObject` is standard S3).
fn native_label(compat: S3Compatibility) -> Option<&'static str> {
    match compat {
        S3Compatibility::Standard | S3Compatibility::Hcp => Some("standard"),
        S3Compatibility::Dxn => Some("dxn"),
        S3Compatibility::StorageGrid => None,
    }
}
