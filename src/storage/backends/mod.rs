//! Private backend facades.

/// Shared Local/NFS checkpoint spacing. Recovery requires a file larger than this interval.
pub(crate) const DEFAULT_CHECKPOINT_INTERVAL_BYTES: u64 = 64 * 1024 * 1024;

pub(crate) mod cifs;
pub(crate) mod hdfs;
pub(crate) mod local;
pub(crate) mod nfs;
pub(crate) mod s3;
