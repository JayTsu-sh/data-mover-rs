use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use bytes::Bytes;

use super::{
    MAX_STALE_RETRIES, NFSFileHandle, NFSStorage, classify_role_error, invalidate_path_cache,
    is_retryable_with_invalidation,
};
use crate::model::{AclEncoding, AclMetadata, FailureClass, StoragePath, Transience};
use crate::storage::backends::nfs::acl;
use crate::storage::backends::nfs::metadata::{NfsMetadataInline, NfsMetadataProtocol};
use crate::storage::backends::nfs::protocol::classify_error;
use crate::storage::backends::nfs::source::NfsProtocolFailure;

impl NFSStorage {
    /// Whether the negotiated mount exposes native POSIX or `NFSv4` ACL operations.
    #[must_use]
    pub fn supports_acl(&self) -> bool {
        let capabilities = self.mount.capabilities();
        capabilities.acl || capabilities.posix_acl
    }

    pub(crate) fn acl_encoding(&self) -> Option<AclEncoding> {
        let capabilities = self.mount.capabilities();
        if capabilities.acl {
            Some(AclEncoding::NfsV4)
        } else if capabilities.posix_acl {
            Some(AclEncoding::Posix)
        } else {
            None
        }
    }

    async fn invalidate_acl_lookup(
        &self,
        path: &Path,
        observed_generation: u64,
    ) -> Result<(), NfsProtocolFailure> {
        self.maybe_refresh_root_fh(observed_generation)
            .await
            .map_err(classify_role_error)?;
        let cache_root = self.get_root_fh();
        let components = Self::collect_components(path).map_err(classify_role_error)?;
        invalidate_path_cache(&components, &cache_root);
        Ok(())
    }

    /// SETATTR for the metadata role. The server's refusal is classified from the nfs-rs error
    /// itself — a refused chown is `PermissionDenied` and names `NFS4ERR_PERM` — rather than from
    /// the legacy string wrapping, which made every refusal `Protocol` with no status.
    async fn setattr_role(
        &self,
        path: &crate::model::StoragePath,
        atime: Option<i64>,
        mtime: Option<i64>,
        uid: Option<u32>,
        gid: Option<u32>,
        mode: Option<u32>,
    ) -> Result<(), NfsProtocolFailure> {
        let native = Path::new(path.as_str());
        let object = self.lookup_fh(native).await.map_err(classify_role_error)?;
        let handle = NFSFileHandle::new(object.fh, PathBuf::from(native));
        self.setattr_retrying(&handle, atime, mtime, uid, gid, mode)
            .await
            .map_err(classify_role_error)?
            .map_err(classify_error)
    }
}

/// What the metadata role reads from one GETATTR's attributes.
pub(super) fn metadata_inline(
    file_handle: Bytes,
    attrs: &super::Attr,
    owners_are_strings: bool,
) -> NfsMetadataInline {
    NfsMetadataInline {
        file_handle,
        symlink: attrs.type_ == super::FType3::NF3LNK as u32,
        uid: super::owner_id(&attrs.owner, attrs.uid, owners_are_strings),
        gid: super::owner_id(&attrs.owner_group, attrs.gid, owners_are_strings),
        mode: attrs.file_mode,
        atime: super::time_to_i64(attrs.atime),
        mtime: super::time_to_i64(attrs.mtime),
        ctime: super::time_to_i64(attrs.ctime),
    }
}

#[async_trait]
impl NfsMetadataProtocol for NFSStorage {
    async fn stat(
        &self,
        path: &crate::model::StoragePath,
    ) -> Result<NfsMetadataInline, NfsProtocolFailure> {
        let (file_handle, attrs) = self.role_lookup_with_attrs(path).await?;
        Ok(metadata_inline(
            file_handle,
            &attrs,
            self.owners_are_strings(),
        ))
    }

    fn supports_acl(&self) -> bool {
        NFSStorage::supports_acl(self)
    }

    fn acl_encoding(&self) -> Option<AclEncoding> {
        NFSStorage::acl_encoding(self)
    }

    async fn get_native_acl(&self, path: &StoragePath) -> Result<AclMetadata, NfsProtocolFailure> {
        if self.acl_encoding() != Some(AclEncoding::Posix) {
            let value = self.get_acl(path).await?;
            return acl::encode(&value).map_err(|_| NfsProtocolFailure::protocol());
        }
        let native = Path::new(path.as_str());
        for attempt in 0..=MAX_STALE_RETRIES {
            let object = self.lookup_fh(native).await.map_err(classify_role_error)?;
            let generation = self.refresh_generation.load(Ordering::Acquire);
            match self.mount.get_posix_acl(object.fh.clone()).await {
                Ok(mut value) => {
                    if value.access.is_empty() {
                        let attrs = self
                            .mount
                            .getattr(object.fh)
                            .await
                            .map_err(classify_error)?;
                        value.access = acl::posix_from_mode(attrs.file_mode);
                    }
                    return acl::encode_posix(&value).map_err(|_| NfsProtocolFailure::protocol());
                }
                Err(error)
                    if is_retryable_with_invalidation(&error) && attempt < MAX_STALE_RETRIES =>
                {
                    self.invalidate_acl_lookup(native, generation).await?;
                }
                Err(error) => return Err(classify_error(error)),
            }
        }
        unreachable!("bounded ACL retry loop always returns")
    }

    async fn set_native_acl(
        &self,
        path: &StoragePath,
        value: &AclMetadata,
    ) -> Result<(), NfsProtocolFailure> {
        if value.encoding() != AclEncoding::Posix {
            let value = acl::decode(value).map_err(|_| invalid_acl())?;
            return self.set_acl(path, &value).await;
        }
        let value = acl::decode_posix(value).map_err(|_| invalid_acl())?;
        let native = Path::new(path.as_str());
        for attempt in 0..=MAX_STALE_RETRIES {
            let object = self.lookup_fh(native).await.map_err(classify_role_error)?;
            let generation = self.refresh_generation.load(Ordering::Acquire);
            match self.mount.set_posix_acl(object.fh, &value).await {
                Ok(()) => return Ok(()),
                Err(error)
                    if is_retryable_with_invalidation(&error) && attempt < MAX_STALE_RETRIES =>
                {
                    self.invalidate_acl_lookup(native, generation).await?;
                }
                Err(error) => return Err(classify_error(error)),
            }
        }
        unreachable!("bounded ACL retry loop always returns")
    }

    fn supports_xattrs(&self) -> bool {
        NFSStorage::supports_xattr(self)
    }

    async fn get_acl(
        &self,
        path: &crate::model::StoragePath,
    ) -> Result<nfs_rs::Acl, NfsProtocolFailure> {
        let native = Path::new(path.as_str());
        for attempt in 0..=MAX_STALE_RETRIES {
            let object = self.lookup_fh(native).await.map_err(classify_role_error)?;
            let generation = self.refresh_generation.load(Ordering::Acquire);
            match self.mount.getacl(object.fh).await {
                Ok(acl) => return Ok(acl),
                Err(error)
                    if is_retryable_with_invalidation(&error) && attempt < MAX_STALE_RETRIES =>
                {
                    self.invalidate_acl_lookup(native, generation).await?;
                }
                Err(error) => {
                    return Err(classify_error(error));
                }
            }
        }
        unreachable!("bounded ACL retry loop always returns")
    }

    async fn get_xattrs(
        &self,
        path: &crate::model::StoragePath,
    ) -> Result<Vec<crate::model::ExtendedAttribute>, NfsProtocolFailure> {
        let native = Path::new(path.as_str());
        let names = self.list_xattr(native).await.map_err(classify_role_error)?;
        let mut values = Vec::with_capacity(names.len());
        for name in names {
            let value = self
                .get_xattr(native, &name)
                .await
                .map_err(classify_role_error)?;
            values.push(
                crate::model::ExtendedAttribute::new(name.into_bytes(), value.to_vec())
                    .map_err(|_| NfsProtocolFailure::protocol())?,
            );
        }
        Ok(values)
    }

    async fn set_acl(
        &self,
        path: &crate::model::StoragePath,
        acl: &nfs_rs::Acl,
    ) -> Result<(), NfsProtocolFailure> {
        let native = Path::new(path.as_str());
        for attempt in 0..=MAX_STALE_RETRIES {
            let object = self.lookup_fh(native).await.map_err(classify_role_error)?;
            let generation = self.refresh_generation.load(Ordering::Acquire);
            match self.mount.setacl(object.fh, acl).await {
                Ok(()) => return Ok(()),
                Err(error)
                    if is_retryable_with_invalidation(&error) && attempt < MAX_STALE_RETRIES =>
                {
                    self.invalidate_acl_lookup(native, generation).await?;
                }
                Err(error) => {
                    return Err(classify_error(error));
                }
            }
        }
        unreachable!("bounded ACL retry loop always returns")
    }

    async fn set_xattr(
        &self,
        path: &crate::model::StoragePath,
        value: &crate::model::ExtendedAttribute,
    ) -> Result<(), NfsProtocolFailure> {
        let name = std::str::from_utf8(value.name()).map_err(|_| NfsProtocolFailure::protocol())?;
        NFSStorage::set_xattr(
            self,
            Path::new(path.as_str()),
            name,
            Bytes::copy_from_slice(value.value()),
        )
        .await
        .map_err(classify_role_error)
    }

    async fn remove_xattr(
        &self,
        path: &crate::model::StoragePath,
        name: &str,
    ) -> Result<(), NfsProtocolFailure> {
        let native = Path::new(path.as_str());
        for attempt in 0..=MAX_STALE_RETRIES {
            let object = self.role_lookup_fh(native).await?;
            let generation = self.refresh_generation.load(Ordering::Acquire);
            match self.mount.removexattr(object.fh, name).await {
                Ok(()) => return Ok(()),
                Err(error)
                    if is_retryable_with_invalidation(&error) && attempt < MAX_STALE_RETRIES =>
                {
                    self.invalidate_acl_lookup(native, generation).await?;
                }
                Err(error) => return Err(classify_error(error)),
            }
        }
        unreachable!("bounded xattr retry loop always returns")
    }

    async fn set_numeric_ownership(
        &self,
        path: &crate::model::StoragePath,
        value: crate::model::OwnershipMode,
    ) -> Result<(), NfsProtocolFailure> {
        self.setattr_role(
            path,
            None,
            None,
            Some(value.uid),
            Some(value.gid),
            Some(value.mode),
        )
        .await
    }

    async fn set_mode(
        &self,
        path: &crate::model::StoragePath,
        mode: u32,
    ) -> Result<(), NfsProtocolFailure> {
        self.setattr_role(path, None, None, None, None, Some(mode))
            .await
    }

    async fn set_timestamps(
        &self,
        path: &crate::model::StoragePath,
        value: crate::model::TimestampMetadata,
    ) -> Result<(), NfsProtocolFailure> {
        let atime = value
            .accessed
            .map(|time| i64::try_from(time.unix_nanos()))
            .transpose()
            .map_err(|_| NfsProtocolFailure::protocol())?;
        let mtime = value
            .modified
            .map(|time| i64::try_from(time.unix_nanos()))
            .transpose()
            .map_err(|_| NfsProtocolFailure::protocol())?;
        self.setattr_role(path, atime, mtime, None, None, None)
            .await
    }
}

fn invalid_acl() -> NfsProtocolFailure {
    NfsProtocolFailure::new(FailureClass::InvalidInput, Transience::Permanent)
}
