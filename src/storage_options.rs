//! Public option and backend-configuration types used by storage orchestration.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use tokio_util::sync::CancellationToken;

use crate::hdfs::HdfsConfig;
use crate::qos::QosManager;

#[derive(Clone, Default)]
pub struct CopyOptions {
    pub qos: Option<QosManager>,
    pub enable_integrity_check: bool,
    pub is_source_reserved: bool,
    pub bytes_counter: Option<Arc<AtomicU64>>,
    pub cancel: Option<CancellationToken>,
}

#[derive(Clone, Default)]
pub struct TarPackOptions {
    pub qos: Option<QosManager>,
    pub bytes_counter: Option<Arc<AtomicU64>>,
}

/// Backend-specific creation configuration.
///
/// Concrete variants are added only when a backend has explicit configuration
/// that does not belong in the storage location or common creation options.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum BackendConfig {
    #[default]
    Default,
    Hdfs(HdfsConfig),
}

/// Typed options used by the unified storage factory.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CreateStorageOptions {
    pub block_size: Option<u64>,
    pub ensure_dir: bool,
    pub backend: BackendConfig,
}

impl CreateStorageOptions {
    #[must_use]
    pub const fn new(block_size: Option<u64>, ensure_dir: bool) -> Self {
        Self {
            block_size,
            ensure_dir,
            backend: BackendConfig::Default,
        }
    }
}
