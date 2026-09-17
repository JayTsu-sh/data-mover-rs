//! Storage location recognition and concrete backend construction.

use std::borrow::Cow;
use std::path::Path;

use tracing::debug;

use crate::Result;
use crate::error::StorageError;
use crate::hdfs::create_hdfs_storage;
use crate::local::create_local_storage;
use crate::nfs::create_nfs_storage;
use crate::s3::create_s3_storage;
use crate::storage_enum::{StorageEnum, StorageType};
use crate::storage_options::{BackendConfig, CreateStorageOptions};
use crate::url_redact::redact_storage_url;

/// 将 Path 转为 S3 兼容的字符串（正斜杠分隔）。
/// Linux 上零开销（直接返回 `Cow::Borrowed`），Windows 上仅在含 `\` 时分配新 `String`。
#[inline]
pub(crate) fn path_to_s3_key(path: &Path) -> Cow<'_, str> {
    let s = path.to_string_lossy();
    #[cfg(windows)]
    {
        if s.contains('\\') {
            return Cow::Owned(s.replace('\\', "/"));
        }
    }
    s
}

/// Detects the storage type from a path by checking its prefix.
/// This handles NFS and S3 paths specially by checking for their respective prefixes.
#[must_use]
pub fn detect_storage_type(path: &str) -> StorageType {
    match path {
        p if p.starts_with("hdfs://") => StorageType::Hdfs,
        p if p.starts_with("smb://") => StorageType::Cifs,
        p if p.starts_with("nfs://") => StorageType::Nfs,
        p if p.starts_with("s3://")
            || p.starts_with("s3+http://")
            || p.starts_with("s3+https://")
            || p.starts_with("s3+sg://")
            || p.starts_with("s3+sg+https://")
            || p.starts_with("s3+dxn://")
            || p.starts_with("s3+dxn+https://")
            || p.starts_with("s3+hcp://") =>
        {
            StorageType::S3
        }
        _ => StorageType::Local,
    }
}

/// 根据路径前缀创建对应的存储实例
///
/// `ensure_dir = true` 用于目标端：prefix 目录不存在时自动创建；
/// `ensure_dir = false` 用于源端：prefix 不存在时报错。
/// S3 无目录概念，该参数对其无效果。
///
/// # Errors
///
/// Returns an error when the requested storage operation cannot be completed.
pub async fn create_storage(path: &str, options: CreateStorageOptions) -> Result<StorageEnum> {
    let CreateStorageOptions {
        block_size,
        ensure_dir,
        backend,
    } = options;
    let storage_type = detect_storage_type(path);
    let hdfs_config = match (&storage_type, backend) {
        (StorageType::Hdfs, BackendConfig::Hdfs(config)) => Some(config),
        (StorageType::Hdfs, BackendConfig::Default) => {
            return Err(StorageError::ConfigError(
                "HDFS location requires BackendConfig::Hdfs".to_string(),
            ));
        }
        (_, BackendConfig::Hdfs(_)) => {
            return Err(StorageError::ConfigError(
                "BackendConfig::Hdfs requires an hdfs:// location".to_string(),
            ));
        }
        (_, BackendConfig::Default) => None,
    };
    debug!(
        "Creating {:?} storage for path: {} (ensure_dir={})",
        storage_type,
        redact_storage_url(path),
        ensure_dir
    );
    match storage_type {
        StorageType::Cifs => Err(StorageError::UnsupportedType(
            "smb:// locations are not available through StorageEnum; \
             connect CIFS with storage::connect_backend(BackendConfig::Cifs)"
                .to_string(),
        )),
        StorageType::Nfs => create_nfs_storage(path, block_size, ensure_dir).await,
        StorageType::S3 => create_s3_storage(path, block_size).await,
        StorageType::Hdfs => Ok(StorageEnum::HDFS(
            create_hdfs_storage(
                path,
                hdfs_config
                    .as_ref()
                    .ok_or_else(|| StorageError::ConfigError("missing HDFS config".to_string()))?,
                block_size,
                ensure_dir,
            )
            .await?,
        )),
        StorageType::Local => create_local_storage(path, block_size, ensure_dir),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn smb_locations_are_refused_by_the_legacy_factory() {
        let error = create_storage(
            "smb://user:secret@nas01/share",
            CreateStorageOptions::default(),
        )
        .await
        .err();
        assert!(
            matches!(error, Some(StorageError::UnsupportedType(_))),
            "smb:// must point callers at storage::connect_backend, got {error:?}"
        );
    }
}
