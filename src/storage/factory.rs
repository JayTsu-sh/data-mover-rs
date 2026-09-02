use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use crate::hdfs::HdfsConfig;
use crate::model::{BackendIdentity, BackendKind};
use crate::storage::Storage;

#[derive(Clone, Debug)]
pub struct LocalBackendConfig {
    pub root: PathBuf,
    pub identity: BackendIdentity,
    pub read_concurrency: NonZeroUsize,
    pub write_concurrency: NonZeroUsize,
}

#[derive(Clone, Debug)]
pub struct NfsBackendConfig {
    pub url: String,
    pub identity: BackendIdentity,
    pub block_size: Option<u64>,
    pub ensure_dir: bool,
}

#[derive(Clone)]
pub struct CifsBackendConfig {
    pub server: String,
    pub share: String,
    pub root: Option<String>,
    pub username: String,
    pub password: String,
    pub identity: BackendIdentity,
}

impl fmt::Debug for CifsBackendConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CifsBackendConfig")
            .field("server", &self.server)
            .field("share", &self.share)
            .field("root", &self.root)
            .field("username", &"<redacted>")
            .field("password", &"<redacted>")
            .field("identity", &self.identity)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct S3BackendConfig {
    pub url: String,
    pub identity: BackendIdentity,
    pub block_size: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct HdfsBackendConfig {
    pub location: String,
    pub identity: BackendIdentity,
    pub client: HdfsConfig,
    pub block_size: Option<u64>,
    pub ensure_dir: bool,
}

#[derive(Clone, Debug)]
pub enum BackendConfig {
    Local(LocalBackendConfig),
    Nfs(NfsBackendConfig),
    Cifs(CifsBackendConfig),
    S3(S3BackendConfig),
    Hdfs(HdfsBackendConfig),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendConnectError {
    kind: BackendKind,
    message: String,
}

impl BackendConnectError {
    fn new(kind: BackendKind, error: impl fmt::Display) -> Self {
        Self {
            kind,
            message: error.to_string(),
        }
    }

    #[must_use]
    pub const fn kind(&self) -> BackendKind {
        self.kind
    }
}

impl fmt::Display for BackendConnectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "failed to connect {:?} backend: {}",
            self.kind, self.message
        )
    }
}

impl std::error::Error for BackendConnectError {}

/// Connects exactly one explicitly selected backend without path-based type inference.
///
/// # Errors
/// Returns a backend-attributed connection error when configuration, authentication, root
/// validation, or role construction fails.
pub async fn connect_backend(config: BackendConfig) -> Result<Storage, BackendConnectError> {
    match config {
        BackendConfig::Local(config) => crate::storage::backends::local::connect_transfer(
            config.root,
            config.identity,
            config.read_concurrency,
            config.write_concurrency,
        )
        .map_err(|error| BackendConnectError::new(BackendKind::Local, error)),
        BackendConfig::Nfs(config) => crate::nfs::create_nfs_role_storage(
            &config.url,
            config.block_size,
            config.ensure_dir,
            config.identity,
        )
        .await
        .map_err(|error| BackendConnectError::new(BackendKind::Nfs, error)),
        BackendConfig::Cifs(config) => {
            let client = smb_domain::Client::new();
            let target = smb_domain::ShareTarget::new(&config.server, &config.share)
                .map_err(|error| BackendConnectError::new(BackendKind::Cifs, error))?;
            let share = client
                .connect_share(
                    &target,
                    smb_domain::Credentials::ntlm(config.username, config.password),
                )
                .await
                .map_err(|error| BackendConnectError::new(BackendKind::Cifs, error))?;
            crate::cifs::create_cifs_role_storage(share, config.root, config.identity)
                .map_err(|error| BackendConnectError::new(BackendKind::Cifs, error))
        }
        BackendConfig::S3(config) => {
            let storage = crate::s3::S3Storage::new(&config.url, config.block_size)
                .await
                .map_err(|error| BackendConnectError::new(BackendKind::S3, error))?;
            storage
                .architecture_storage(config.identity)
                .map_err(|error| BackendConnectError::new(BackendKind::S3, error))
        }
        BackendConfig::Hdfs(config) => {
            let storage = crate::hdfs::create_hdfs_storage(
                &config.location,
                &config.client,
                config.block_size,
                config.ensure_dir,
            )
            .await
            .map_err(|error| BackendConnectError::new(BackendKind::Hdfs, error))?;
            storage
                .architecture_storage(config.identity)
                .map_err(|error| BackendConnectError::new(BackendKind::Hdfs, error))
        }
    }
}
