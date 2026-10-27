//! Private backend facades.

/// Shared Local/NFS/HDFS checkpoint spacing. Recovery requires a file larger than this interval.
pub(crate) const DEFAULT_CHECKPOINT_INTERVAL_BYTES: u64 = 64 * 1024 * 1024;

/// Creation permissions for data entries before any source metadata is applied.
pub(super) const DEFAULT_FILE_MODE: u32 = 0o644;
pub(super) const DEFAULT_DIRECTORY_MODE: u32 = 0o755;

pub(crate) mod cifs;
pub(crate) mod hdfs;
pub(crate) mod local;
pub(crate) mod nfs;
pub(crate) mod s3;
