//! Cross-backend metadata mapping and copied-entry completion.

use std::path::Path;

use crate::error::StorageError;
use crate::{EntryEnum, Result, StorageEnum};

impl StorageEnum {
    /// Update metadata selectively (timestamps, ownership, permissions).
    /// Pass `None` to skip updating a specific field.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn set_metadata(
        &self,
        relative_path: &Path,
        atime: Option<i64>,
        mtime: Option<i64>,
        uid: Option<u32>,
        gid: Option<u32>,
        mode: Option<u32>,
    ) -> Result<()> {
        match self {
            Self::Local(s) => {
                s.set_metadata(relative_path, atime, mtime, uid, gid, mode)
                    .await
            }
            Self::NFS(s) => {
                s.update_metadata(relative_path, atime, mtime, uid, gid, mode)
                    .await
            }
            Self::CIFS(s) => {
                s.update_metadata(relative_path, atime, mtime, uid, gid, mode)
                    .await
            }
            Self::S3(_) => Ok(()),
            Self::HDFS(s) => {
                if uid.is_some() || gid.is_some() {
                    return Err(StorageError::UnsupportedType(
                        "HDFS uses string owner/group identities and cannot map numeric uid/gid without an identity mapper"
                            .to_string(),
                    ));
                }
                s.set_metadata(relative_path, atime, mtime, mode).await
            }
        }
    }

    /// Update file metadata (timestamps, ownership, permissions) from an entry.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn set_entry_metadata(&self, entry: &EntryEnum) -> Result<()> {
        match (self, entry) {
            (Self::Local(s), EntryEnum::NAS(e)) => {
                s.set_metadata(
                    &e.relative_path,
                    Some(e.atime),
                    Some(e.mtime),
                    e.uid,
                    e.gid,
                    Some(e.mode),
                )
                .await
            }
            (Self::NFS(s), EntryEnum::NAS(e)) => {
                s.update_metadata(
                    &e.relative_path,
                    Some(e.atime),
                    Some(e.mtime),
                    e.uid,
                    e.gid,
                    Some(e.mode),
                )
                .await
            }
            (Self::CIFS(s), EntryEnum::NAS(e)) => {
                s.update_metadata(
                    &e.relative_path,
                    Some(e.atime),
                    Some(e.mtime),
                    e.uid,
                    e.gid,
                    Some(e.mode),
                )
                .await
            }
            (Self::Local(s), EntryEnum::S3(e)) => {
                s.set_metadata(
                    Path::new(&e.relative_path),
                    Some(e.mtime),
                    Some(e.mtime),
                    None,
                    None,
                    None,
                )
                .await
            }
            (Self::NFS(s), EntryEnum::S3(e)) => {
                s.update_metadata(
                    Path::new(&e.relative_path),
                    Some(e.mtime),
                    Some(e.mtime),
                    None,
                    None,
                    None,
                )
                .await
            }
            (Self::CIFS(s), EntryEnum::S3(e)) => {
                s.update_metadata(
                    Path::new(&e.relative_path),
                    Some(e.mtime),
                    Some(e.mtime),
                    None,
                    None,
                    None,
                )
                .await
            }
            (Self::HDFS(s), EntryEnum::NAS(e)) => {
                s.set_permission(&e.relative_path, e.mode & 0o7777).await?;
                s.set_mtime(&e.relative_path, e.mtime).await
            }
            (Self::HDFS(s), EntryEnum::S3(e)) => {
                s.set_mtime(Path::new(&e.relative_path), e.mtime).await
            }
            (storage, EntryEnum::HDFS(entry)) => storage.set_hdfs_entry_metadata(entry).await,
            _ => Ok(()),
        }
    }

    async fn set_hdfs_entry_metadata(&self, entry: &crate::HDFSEntry) -> Result<()> {
        match self {
            Self::Local(storage) => {
                storage
                    .set_metadata(
                        &entry.relative_path,
                        None,
                        Some(entry.mtime),
                        None,
                        None,
                        Some(entry.mode & 0o7777),
                    )
                    .await
            }
            Self::NFS(storage) => {
                storage
                    .update_metadata(
                        &entry.relative_path,
                        None,
                        Some(entry.mtime),
                        None,
                        None,
                        Some(entry.mode & 0o7777),
                    )
                    .await
            }
            Self::CIFS(storage) => {
                storage
                    .update_metadata(
                        &entry.relative_path,
                        None,
                        Some(entry.mtime),
                        None,
                        None,
                        Some(entry.mode & 0o7777),
                    )
                    .await
            }
            Self::HDFS(storage) => {
                storage
                    .set_permission(&entry.relative_path, entry.mode)
                    .await?;
                storage.set_mtime(&entry.relative_path, entry.mtime).await?;
                storage
                    .set_owner_group(&entry.relative_path, Some(&entry.owner), Some(&entry.group))
                    .await
            }
            Self::S3(_) => Ok(()),
        }
    }

    pub(crate) async fn apply_copied_metadata(to: &Self, entry: &EntryEnum) -> Result<()> {
        match to {
            Self::S3(_) => Ok(()),
            _ => to.set_entry_metadata(entry).await,
        }
    }

    pub(crate) async fn complete_copied_entry(
        from: &Self,
        to: &Self,
        entry: &EntryEnum,
        is_source_reserved: bool,
    ) -> Result<()> {
        Self::apply_copied_metadata(to, entry).await?;
        if !is_source_reserved {
            from.delete_file(entry).await?;
        }
        Ok(())
    }
}
