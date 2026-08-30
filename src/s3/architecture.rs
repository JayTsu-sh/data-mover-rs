use std::sync::Arc;

use super::{S3Compatibility, S3Storage};
use crate::model::BackendIdentity;
use crate::storage::Storage;
use crate::storage::backends::s3::{S3NativeContext, S3TagSupport};

pub(super) fn connect(
    storage: &S3Storage,
    identity: BackendIdentity,
) -> Result<Storage, Box<dyn std::error::Error>> {
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
