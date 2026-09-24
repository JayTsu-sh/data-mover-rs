use std::sync::Arc;

use super::{S3Compatibility, S3Storage};
use crate::model::BackendIdentity;
use crate::storage::Storage;
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

pub(super) fn connect(storage: &S3Storage) -> Result<Storage, Box<dyn std::error::Error>> {
    let identity = identity(
        &storage.endpoint,
        &storage.bucket_name,
        storage.prefix.as_deref().unwrap_or_default(),
    )?;
    let native = matches!(
        storage.compatibility,
        S3Compatibility::Standard | S3Compatibility::Dxn
    )
    .then(|| {
        S3NativeContext::new(
            &storage.endpoint,
            match storage.compatibility {
                S3Compatibility::Dxn => "dxn",
                _ => "standard",
            },
            storage.bucket_name.clone(),
            storage.prefix.clone(),
        )
    });
    crate::storage::backends::s3::connect_with_tag_support(
        Arc::new(storage.clone()),
        identity,
        native,
        match storage.compatibility {
            S3Compatibility::Dxn => S3TagSupport::Unsupported,
            _ => S3TagSupport::Supported,
        },
    )
}
