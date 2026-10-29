//! Shared creation and checkpoint defaults.

/// Shared Local/NFS/HDFS checkpoint spacing. Recovery requires a file larger than this interval.
pub const DEFAULT_CHECKPOINT_INTERVAL_BYTES: u64 = 64 * 1024 * 1024;

/// Creation permissions for data entries before any source metadata is applied.
pub const DEFAULT_FILE_MODE: u32 = 0o644;
pub const DEFAULT_DIRECTORY_MODE: u32 = 0o755;
