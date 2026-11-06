//! 真实 NFS 根目录连接探测；只读，调用者提供已批准的测试根。
#![cfg(all(feature = "backend-nfs", feature = "transfer"))]
use std::error::Error;

use data_mover::model::{FailureClass, StoragePath};
use data_mover::probes::check_connectivity;
use data_mover::storage::{BackendConfig, NfsBackendConfig, StorageRoleFailure, connect_backend};

#[tokio::test]
#[ignore = "requires NFS_REAL_URL pointing at an approved readable NFS root"]
async fn nfs_probe_reads_root_without_using_unsupported_root_stat() -> Result<(), Box<dyn Error>> {
    let storage = connect_backend(BackendConfig::Nfs(NfsBackendConfig {
        url: std::env::var("NFS_REAL_URL")?,
        block_size: None,
        ensure_dir: false,
        transfer_concurrency: None,
    }))
    .await?;
    assert!(matches!(
        storage.namespace()?.stat(&StoragePath::root()).await,
        Err(StorageRoleFailure::Entry(failure)) if failure.class() == FailureClass::InvalidInput
    ));
    check_connectivity(&storage).await?;
    Ok(())
}
