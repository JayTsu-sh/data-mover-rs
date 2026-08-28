//! Cross-backend metadata mapping, copied-entry completion, and ACL/xattr wire orchestration.

use std::io::{self, Read as _};
use std::path::Path;

use bytes::Bytes;
use tracing::warn;

#[cfg(windows)]
use crate::acl;
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

    /// 从源端复制 ACL（非继承的显式 ACE + 继承保护状态）到目标端
    ///
    /// 支持组合：
    /// - Local → Local（仅 Windows，Win32 API）
    /// - CIFS → CIFS（跨平台，smb-rs 直通）
    /// - NFS → NFS（仅当双方都支持 ACL，即 `NFSv4+`）
    /// - 跨类型或不支持的组合静默跳过
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn copy_acl(
        from: &StorageEnum,
        to: &StorageEnum,
        relative_path: &Path,
    ) -> Result<()> {
        match (from, to) {
            // CIFS → CIFS：smb-rs SecurityDescriptor 直通（跨平台）
            (StorageEnum::CIFS(src), StorageEnum::CIFS(dst)) => {
                let sd = src.get_security_descriptor(relative_path).await?;
                dst.set_security_descriptor(relative_path, &sd).await
            }

            // NFS → NFS：NFSv4 ACL 直通（仅当双方都支持 ACL）
            (StorageEnum::NFS(src), StorageEnum::NFS(dst)) => {
                if src.supports_acl() && dst.supports_acl() {
                    let acl = src.get_acl(relative_path).await?;
                    dst.set_acl(relative_path, &acl).await?;
                }
                Ok(())
            }

            // Local → Local：Win32 API（仅 Windows）
            #[cfg(windows)]
            (StorageEnum::Local(src), StorageEnum::Local(dst)) => {
                let source_abs = src.root_path.join(relative_path);
                let target_abs = dst.root_path.join(relative_path);
                acl::copy_acl(&source_abs, &target_abs)
            }

            // 其他组合不支持 ACL，静默跳过
            _ => Ok(()),
        }
    }

    /// 从源端复制所有 extended attributes (xattr) 到目标端
    ///
    /// 支持组合：
    /// - NFS → NFS（仅当双方都支持 xattr，即 `NFSv4+`）
    /// - 其他组合静默跳过
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn copy_xattr(
        from: &StorageEnum,
        to: &StorageEnum,
        relative_path: &Path,
    ) -> Result<()> {
        match (from, to) {
            (StorageEnum::NFS(src), StorageEnum::NFS(dst)) => {
                if !src.supports_xattr() || !dst.supports_xattr() {
                    return Ok(());
                }
                let names = match src.list_xattr(relative_path).await {
                    Ok(names) => names,
                    Err(e) => {
                        // Unsupported 错误（v3 server）静默跳过；其他错误记录 warn
                        if !e.to_string().contains("Unsupported") {
                            warn!(
                                "Failed to list xattr for {:?}, skipping: {}",
                                relative_path, e
                            );
                        }
                        return Ok(());
                    }
                };
                for name in names {
                    let value = src.get_xattr(relative_path, &name).await?;
                    dst.set_xattr(relative_path, &name, value).await?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// 读取 ACL 数据为二进制字节（用于跨进程传输）
    ///
    /// 返回 `Some(bytes)` 表示有 ACL 数据，`None` 表示不支持或无 ACL。
    /// NFS ACL 使用自定义二进制格式（见 `serialize_nfs_acl`/`deserialize_nfs_acl`）。
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn get_acl_bytes(&self, relative_path: &Path) -> Result<Option<Vec<u8>>> {
        match self {
            StorageEnum::CIFS(s) => {
                use binrw::BinWrite;
                let sd = s.get_security_descriptor(relative_path).await?;
                // 用 binrw 序列化 SecurityDescriptor 为字节
                let mut buf = std::io::Cursor::new(Vec::new());
                sd.write_le(&mut buf)
                    .map_err(|e| StorageError::OperationError(format!("serialize SD: {e}")))?;
                Ok(Some(buf.into_inner()))
            }
            StorageEnum::NFS(s) if s.supports_acl() => {
                match s.get_acl(relative_path).await {
                    Ok(acl) if !acl.aces.is_empty() => Ok(Some(serialize_nfs_acl(&acl)?)),
                    _ => Ok(None), // 空 ACL 或不支持时静默跳过
                }
            }
            #[cfg(windows)]
            StorageEnum::Local(s) => {
                let abs_path = s.root_path.join(relative_path);
                match acl::get_acl_bytes(&abs_path) {
                    Ok(bytes) if bytes.is_empty() => Ok(None),
                    Ok(bytes) => Ok(Some(bytes)),
                    Err(e) => Err(e),
                }
            }
            _ => Ok(None),
        }
    }

    /// 从二进制字节设置 ACL（用于跨进程传输）
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn set_acl_bytes(&self, relative_path: &Path, acl_data: &[u8]) -> Result<()> {
        match self {
            StorageEnum::CIFS(s) => {
                use binrw::BinRead;
                // 用 binrw 反序列化字节为 SecurityDescriptor
                let mut cursor = std::io::Cursor::new(acl_data);
                let sd = smb::SecurityDescriptor::read_le(&mut cursor)
                    .map_err(|e| StorageError::OperationError(format!("deserialize SD: {e}")))?;
                s.set_security_descriptor(relative_path, &sd).await
            }
            StorageEnum::NFS(s) if s.supports_acl() => {
                let acl = deserialize_nfs_acl(acl_data)?;
                s.set_acl(relative_path, &acl).await
            }
            #[cfg(windows)]
            StorageEnum::Local(s) => {
                let abs_path = s.root_path.join(relative_path);
                acl::set_acl_bytes(&abs_path, acl_data)
            }
            _ => Ok(()),
        }
    }

    /// 读取所有 xattr 为 key-value 对（用于跨进程传输）
    ///
    /// 返回 `Some(bytes)` 表示有 xattr 数据，`None` 表示不支持或无 xattr。
    /// 二进制格式：`[u32 count] [u32 name_len] [name] [u32 value_len] [value] ...`
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn get_xattr_bytes(&self, relative_path: &Path) -> Result<Option<Vec<u8>>> {
        match self {
            StorageEnum::NFS(s) if s.supports_xattr() => {
                let names = match s.list_xattr(relative_path).await {
                    Ok(names) if !names.is_empty() => names,
                    _ => return Ok(None),
                };
                let mut buf = Vec::new();
                let count = u32::try_from(names.len()).map_err(|_| {
                    StorageError::OperationError("too many xattrs to serialize".to_string())
                })?;
                buf.extend_from_slice(&count.to_le_bytes());
                for name in &names {
                    let name_bytes = name.as_bytes();
                    let name_len = u32::try_from(name_bytes.len()).map_err(|_| {
                        StorageError::OperationError(
                            "xattr name is too long to serialize".to_string(),
                        )
                    })?;
                    buf.extend_from_slice(&name_len.to_le_bytes());
                    buf.extend_from_slice(name_bytes);
                    let value = s.get_xattr(relative_path, name).await?;
                    let value_len = u32::try_from(value.len()).map_err(|_| {
                        StorageError::OperationError(
                            "xattr value is too large to serialize".to_string(),
                        )
                    })?;
                    buf.extend_from_slice(&value_len.to_le_bytes());
                    buf.extend_from_slice(&value);
                }
                Ok(Some(buf))
            }
            _ => Ok(None),
        }
    }

    /// 从二进制字节设置所有 xattr（用于跨进程传输）
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn set_xattr_bytes(&self, relative_path: &Path, xattr_data: &[u8]) -> Result<()> {
        match self {
            StorageEnum::NFS(s) if s.supports_xattr() => {
                let pairs = deserialize_xattr(xattr_data)?;
                for (name, value) in pairs {
                    s.set_xattr(relative_path, &name, Bytes::from(value))
                        .await?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

// ============================================================
// NFS ACL / xattr 二进制序列化（跨进程传输用）
// ============================================================

/// 将 `NFSv4` ACL 序列化为二进制字节。
///
/// 格式：`[u32 ace_count] [ace...]`
/// 每个 ace：`[u32 type] [u32 flags] [u32 mask] [u32 who_len] [who_bytes]`
fn serialize_nfs_acl(acl: &nfs_rs::Acl) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let ace_count = u32::try_from(acl.aces.len()).map_err(|_| {
        StorageError::OperationError("too many NFS ACL entries to serialize".to_string())
    })?;
    buf.extend_from_slice(&ace_count.to_le_bytes());
    for ace in &acl.aces {
        buf.extend_from_slice(&(ace.ace_type as u32).to_le_bytes());
        buf.extend_from_slice(&ace.flags.0.to_le_bytes());
        buf.extend_from_slice(&ace.access_mask.0.to_le_bytes());
        let who_bytes = ace.who.as_bytes();
        let who_len = u32::try_from(who_bytes.len()).map_err(|_| {
            StorageError::OperationError("NFS ACL principal is too long to serialize".to_string())
        })?;
        buf.extend_from_slice(&who_len.to_le_bytes());
        buf.extend_from_slice(who_bytes);
    }
    Ok(buf)
}

/// 反序列化长度上限常量（防止恶意/损坏数据导致 OOM）
const MAX_ACE_COUNT: usize = 4096;
const MAX_ACE_WHO_LEN: usize = 1024;
const MAX_XATTR_COUNT: usize = 1024;
const MAX_XATTR_NAME_LEN: usize = 256;
const MAX_XATTR_VALUE_LEN: usize = 64 * 1024; // 64 KiB

/// 从 cursor 读取一个 little-endian u32
fn read_u32_le(cursor: &mut io::Cursor<&[u8]>, context: &str) -> Result<u32> {
    let mut buf = [0u8; 4];
    cursor
        .read_exact(&mut buf)
        .map_err(|e| StorageError::OperationError(format!("deserialize {context}: {e}")))?;
    Ok(u32::from_le_bytes(buf))
}

/// 从 cursor 读取指定长度的字节，带上限检查防止 OOM
fn read_bytes_checked(
    cursor: &mut io::Cursor<&[u8]>,
    len: usize,
    max: usize,
    context: &str,
) -> Result<Vec<u8>> {
    if len > max {
        return Err(StorageError::OperationError(format!(
            "{context} length {len} exceeds maximum {max}"
        )));
    }
    let mut buf = vec![0u8; len];
    cursor
        .read_exact(&mut buf)
        .map_err(|e| StorageError::OperationError(format!("deserialize {context}: {e}")))?;
    Ok(buf)
}

/// 从二进制字节反序列化 `NFSv4` ACL。
fn deserialize_nfs_acl(data: &[u8]) -> Result<nfs_rs::Acl> {
    let mut cursor = io::Cursor::new(data);

    let count = read_u32_le(&mut cursor, "ACL count")? as usize;
    if count > MAX_ACE_COUNT {
        return Err(StorageError::OperationError(format!(
            "ACE count {count} exceeds maximum {MAX_ACE_COUNT}"
        )));
    }

    let mut aces = Vec::with_capacity(count);
    for _ in 0..count {
        let ace_type = match read_u32_le(&mut cursor, "ACE type")? {
            0 => nfs_rs::AceType::AccessAllowed,
            1 => nfs_rs::AceType::AccessDenied,
            2 => nfs_rs::AceType::SystemAudit,
            3 => nfs_rs::AceType::SystemAlarm,
            v => {
                return Err(StorageError::OperationError(format!(
                    "unknown ACE type: {v}"
                )));
            }
        };

        let flags = nfs_rs::AceFlags(read_u32_le(&mut cursor, "ACE flags")?);
        let access_mask = nfs_rs::AceMask(read_u32_le(&mut cursor, "ACE mask")?);

        let who_len = read_u32_le(&mut cursor, "ACE who len")? as usize;
        let who_buf = read_bytes_checked(&mut cursor, who_len, MAX_ACE_WHO_LEN, "ACE 'who'")?;
        let who = String::from_utf8(who_buf)
            .map_err(|e| StorageError::OperationError(format!("invalid ACE who UTF-8: {e}")))?;

        aces.push(nfs_rs::NfsAce {
            ace_type,
            flags,
            access_mask,
            who,
        });
    }

    Ok(nfs_rs::Acl { aces })
}

/// 从二进制字节反序列化 xattr key-value 对。
///
/// 格式：`[u32 count] [u32 name_len] [name] [u32 value_len] [value] ...`
fn deserialize_xattr(data: &[u8]) -> Result<Vec<(String, Vec<u8>)>> {
    let mut cursor = io::Cursor::new(data);

    let count = read_u32_le(&mut cursor, "xattr count")? as usize;
    if count > MAX_XATTR_COUNT {
        return Err(StorageError::OperationError(format!(
            "xattr count {count} exceeds maximum {MAX_XATTR_COUNT}"
        )));
    }

    let mut pairs = Vec::with_capacity(count);
    for _ in 0..count {
        let name_len = read_u32_le(&mut cursor, "xattr name len")? as usize;
        let name_buf = read_bytes_checked(&mut cursor, name_len, MAX_XATTR_NAME_LEN, "xattr name")?;
        let name = String::from_utf8(name_buf)
            .map_err(|e| StorageError::OperationError(format!("invalid xattr name UTF-8: {e}")))?;

        let value_len = read_u32_le(&mut cursor, "xattr value len")? as usize;
        let value_buf =
            read_bytes_checked(&mut cursor, value_len, MAX_XATTR_VALUE_LEN, "xattr value")?;

        pairs.push((name, value_buf));
    }

    Ok(pairs)
}
