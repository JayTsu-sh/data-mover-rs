//! Workspace integration-test assembly; never a production dependency.
use data_mover_storage_core::storage::{BackendConfig, BackendConnectError, Storage};
/// Connect a built-in adapter for cross-engine contracts.
/// # Errors
/// Returns the adapter's configuration or connection error.
pub async fn connect_backend(config: BackendConfig) -> Result<Storage, BackendConnectError> {
    match config {
        BackendConfig::Local(c) => data_mover_backend_local::connect_backend(c),
        BackendConfig::Nfs(c) => data_mover_backend_nfs::connect_backend(c).await,
        BackendConfig::Cifs(c) => data_mover_backend_cifs::connect_backend(c).await,
        BackendConfig::S3(c) => data_mover_backend_s3::connect_backend(c).await,
        BackendConfig::Hdfs(c) => data_mover_backend_hdfs::connect_backend(c).await,
    }
}
