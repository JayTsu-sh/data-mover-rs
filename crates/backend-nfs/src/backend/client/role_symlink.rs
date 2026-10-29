//! Creates links with typed protocol failures and fused NFS creation/attribute requests.

use std::path::Path;

use super::{MAX_STALE_RETRIES, NFSStorage, classify_role_error, is_retryable_with_invalidation};
use crate::model::{FailureClass, StorageTimestamp, SymlinkTargetEncoding, Transience};
use crate::storage::CreateSymlinkRequest;
use crate::storage::backends::nfs::protocol::classify_error;
use crate::storage::backends::nfs::source::NfsProtocolFailure;

fn invalid() -> NfsProtocolFailure {
    NfsProtocolFailure::new(FailureClass::InvalidInput, Transience::Permanent)
}

fn timestamp(value: Option<StorageTimestamp>) -> Result<Option<nfs_rs::Time>, NfsProtocolFailure> {
    value
        .map(|value| {
            Ok(nfs_rs::Time {
                seconds: value
                    .unix_nanos()
                    .div_euclid(1_000_000_000)
                    .try_into()
                    .map_err(|_| invalid())?,
                nseconds: value
                    .unix_nanos()
                    .rem_euclid(1_000_000_000)
                    .try_into()
                    .map_err(|_| invalid())?,
            })
        })
        .transpose()
}

impl NFSStorage {
    pub(super) async fn role_create_symlink(
        &self,
        request: &CreateSymlinkRequest,
    ) -> Result<(), NfsProtocolFailure> {
        if request.target.encoding() != SymlinkTargetEncoding::UnixBytes
            || request.target.as_bytes().contains(&0)
            || request.uid == Some(u32::MAX)
            || request.gid == Some(u32::MAX)
        {
            return Err(invalid());
        }
        let target = std::str::from_utf8(request.target.as_bytes()).map_err(|_| invalid())?;
        let native = Path::new(request.path.as_str());
        let parent = native.parent().ok_or_else(invalid)?;
        let name = native
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(invalid)?;
        let accessed = timestamp(request.accessed)?;
        let modified = timestamp(request.modified)?;
        for attempt in 0..=MAX_STALE_RETRIES {
            let parent_handle = self.role_lookup_fh(parent).await?.fh;
            match self
                .mount
                .symlink_with_attrs(
                    target,
                    parent_handle,
                    name,
                    request.uid,
                    request.gid,
                    accessed,
                    modified,
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(error)
                    if is_retryable_with_invalidation(&error) && attempt < MAX_STALE_RETRIES =>
                {
                    self.forget_role_directory(&request.path)
                        .await
                        .map_err(classify_role_error)?;
                }
                Err(error) => return Err(classify_error(error)),
            }
        }
        unreachable!("bounded attempts return on their last result")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::TimePrecision;

    fn timestamp_at(nanos: i128) -> StorageTimestamp {
        StorageTimestamp::new(nanos, TimePrecision::Nanoseconds)
            .unwrap_or_else(|error| panic!("{error}"))
    }

    #[test]
    fn timestamp_conversion_preserves_nfs_second_and_nanosecond_fields() {
        let converted = timestamp(Some(timestamp_at(1_700_000_000_123_456_789)))
            .unwrap_or_else(|_| panic!("timestamp fits NFS time fields"));
        assert_eq!(
            converted,
            Some(nfs_rs::Time {
                seconds: 1_700_000_000,
                nseconds: 123_456_789,
            })
        );
        assert_eq!(
            timestamp(None).unwrap_or_else(|_| panic!("omitted timestamp")),
            None
        );
    }

    #[test]
    fn timestamp_conversion_rejects_values_outside_nfs_time_range() {
        assert!(timestamp(Some(timestamp_at(-1))).is_err());
        assert!(
            timestamp(Some(timestamp_at(
                (i128::from(u32::MAX) + 1) * 1_000_000_000
            )))
            .is_err()
        );
    }
}
