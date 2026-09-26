//! Delete-marker replay requests of the role protocol (P7 H1): the bucket's versioning as it is
//! now, and a plain `DeleteObject` sent to write a delete marker, with what its reply names.

use aws_sdk_s3::types::BucketVersioningStatus;

use super::{
    ProvideErrorMetadata, S3Result, S3Storage, s3_role_remote_failure, s3_role_transport_failure,
};
use crate::s3::versioning_is_unsupported;
use crate::storage::backends::s3::{S3BucketVersioning, S3DeleteReply};

impl S3Storage {
    /// `GetBucketVersioning`, asked now — of every store, HCP included (a namespace's versioning
    /// can be off there too; the legacy read path's "HCP always versions" shortcut must not decide
    /// a delete). A bucket that never had versioning, or a store without it (501 /
    /// `NotImplemented`: the ONTAP NAS bucket), is `Off`.
    pub(super) async fn role_bucket_versioning(&self) -> S3Result<S3BucketVersioning> {
        match self
            .client
            .get_bucket_versioning()
            .bucket(&self.bucket_name)
            .send()
            .await
        {
            Ok(reply) => Ok(match reply.status() {
                Some(BucketVersioningStatus::Enabled) => S3BucketVersioning::Enabled,
                Some(BucketVersioningStatus::Suspended) => S3BucketVersioning::Suspended,
                _ => S3BucketVersioning::Off,
            }),
            Err(error) => {
                let unsupported = versioning_is_unsupported(
                    error
                        .as_service_error()
                        .and_then(ProvideErrorMetadata::code),
                    error
                        .raw_response()
                        .map(|response| response.status().as_u16()),
                );
                if unsupported {
                    return Ok(S3BucketVersioning::Off);
                }
                Err(classify_sdk!(
                    error,
                    "S3 GetBucketVersioning request failed"
                ))
            }
        }
    }

    /// A `DeleteObject` without a version id, and what its reply names: `x-amz-delete-marker`
    /// and `x-amz-version-id`.
    pub(super) async fn role_write_delete_marker(&self, key: &str) -> S3Result<S3DeleteReply> {
        let reply = self
            .client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(self.build_full_key(key))
            .send()
            .await
            .map_err(|error| {
                classify_sdk!(error, "S3 DeleteObject (delete marker) request failed")
            })?;
        Ok(S3DeleteReply {
            delete_marker: reply.delete_marker().unwrap_or(false),
            version_id: reply.version_id().map(str::to_string),
        })
    }
}
