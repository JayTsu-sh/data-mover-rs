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

/// Connects a source whose caller owns scan restart and checkpoint recovery.
/// Definite transport loss reaches traversal before RPC/page replay delays.
/// S3 uses one SDK attempt; HDFS allows one standby failover unless explicitly configured.
pub async fn connect_scan_backend(
    mut config: BackendConfig,
) -> Result<Storage, BackendConnectError> {
    if let BackendConfig::Nfs(nfs) = &mut config {
        let mut url = url::Url::parse(&nfs.url)
            .map_err(|error| BackendConnectError::new(BackendKind::Nfs, error))?;
        let pairs: Vec<(String, String)> = url
            .query_pairs()
            .filter(|(key, _)| key != "fail_fast")
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        url.set_query(None);
        url.query_pairs_mut()
            .extend_pairs(pairs)
            .append_pair("fail_fast", "true");
        nfs.url = url.to_string();
    }
    if let BackendConfig::Hdfs(hdfs) = &mut config {
        hdfs.client
            .overrides
            .entry("dfs.client.failover.max.attempts".into())
            .or_insert_with(|| "1".into());
    }
    #[cfg(feature = "backend-s3")]
    if let BackendConfig::S3(s3) = config {
        return backends::s3::connect_scan_backend(s3).await;
    }
    connect_backend(config).await
}
