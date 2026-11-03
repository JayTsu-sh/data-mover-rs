#[allow(unused_imports)]
use super::backends;
use super::{BackendConfig, BackendConnectError, Storage};
#[allow(unused_imports)]
use crate::model::BackendKind;
/// Connects exactly one explicitly selected backend without path-based type inference.
///
/// The returned storage's [`Storage::identity`] is [`super::endpoint_identity`] of `config`.
///
/// # Errors
/// Returns a backend-attributed connection error when configuration, authentication, root
/// validation, or role construction fails.
pub async fn connect_backend(config: BackendConfig) -> Result<Storage, BackendConnectError> {
    match config {
        #[cfg(feature = "backend-local")]
        BackendConfig::Local(config) => backends::local::connect_backend(config),
        #[cfg(not(feature = "backend-local"))]
        BackendConfig::Local(_) => Err(BackendConnectError::not_compiled(BackendKind::Local)),
        #[cfg(feature = "backend-nfs")]
        BackendConfig::Nfs(config) => backends::nfs::connect_backend(config).await,
        #[cfg(not(feature = "backend-nfs"))]
        BackendConfig::Nfs(_) => Err(BackendConnectError::not_compiled(BackendKind::Nfs)),
        #[cfg(feature = "backend-cifs")]
        BackendConfig::Cifs(config) => backends::cifs::connect_backend(config).await,
        #[cfg(not(feature = "backend-cifs"))]
        BackendConfig::Cifs(_) => Err(BackendConnectError::not_compiled(BackendKind::Cifs)),
        #[cfg(feature = "backend-s3")]
        BackendConfig::S3(config) => backends::s3::connect_backend(config).await,
        #[cfg(not(feature = "backend-s3"))]
        BackendConfig::S3(_) => Err(BackendConnectError::not_compiled(BackendKind::S3)),
        #[cfg(feature = "backend-hdfs")]
        BackendConfig::Hdfs(config) => backends::hdfs::connect_backend(config).await,
        #[cfg(not(feature = "backend-hdfs"))]
        BackendConfig::Hdfs(_) => Err(BackendConnectError::not_compiled(BackendKind::Hdfs)),
    }
}
