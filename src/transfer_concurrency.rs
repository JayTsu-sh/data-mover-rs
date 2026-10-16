use crate::Result;
use crate::error::StorageError;

/// Per-file data-transfer concurrency.
///
/// Read and write depths are deliberately independent: sources and destinations
/// have different latency and protocol limits. Values are always in `1..=24`.
///
/// Factory-created storage resolves each direction independently in this order:
/// backend-specific direction, global direction, shared `DATA_MOVER_INFLIGHT`, then the
/// protocol default. A value set explicitly with an adapter's
/// `with_transfer_concurrency` builder replaces the resolved configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransferConcurrency {
    read: usize,
    write: usize,
}

impl TransferConcurrency {
    /// Highest supported queue depth.
    ///
    /// Supports explicit 8/16/24-depth experiments without changing protocol defaults.
    pub const MAX: usize = 24;

    /// Creates validated read/write queue depths.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when either value is outside `1..=24`.
    pub fn new(read: usize, write: usize) -> Result<Self> {
        validate("read", read)?;
        validate("write", write)?;
        Ok(Self { read, write })
    }

    /// Resolves queue depths for a role-based caller using the factory environment precedence.
    /// Explicit backend configuration and transfer limits remain caller-controlled.
    ///
    /// # Errors
    /// Returns a configuration error for an invalid selected environment value.
    pub fn from_env(backend: crate::model::BackendKind, defaults: Self) -> Result<Self> {
        let backend = match backend {
            crate::model::BackendKind::Local => TransferBackend::Local,
            crate::model::BackendKind::Nfs => TransferBackend::Nfs,
            crate::model::BackendKind::Cifs => TransferBackend::Cifs,
            crate::model::BackendKind::S3 => TransferBackend::S3,
            crate::model::BackendKind::Hdfs => TransferBackend::Hdfs,
        };
        resolve_transfer_concurrency(backend, defaults, None)
    }

    /// Returns the maximum number of concurrent reads for one file transfer.
    #[must_use]
    pub const fn read(self) -> usize {
        self.read
    }

    /// Returns the maximum number of concurrent writes for one file transfer.
    #[must_use]
    pub const fn write(self) -> usize {
        self.write
    }

    pub(crate) const fn defaults(read: usize, write: usize) -> Self {
        debug_assert!(read > 0 && read <= Self::MAX);
        debug_assert!(write > 0 && write <= Self::MAX);
        Self { read, write }
    }
}

fn validate(direction: &str, value: usize) -> Result<()> {
    if (1..=TransferConcurrency::MAX).contains(&value) {
        Ok(())
    } else {
        Err(StorageError::ConfigError(format!(
            "{direction} inflight must be between 1 and {}, got {value}",
            TransferConcurrency::MAX
        )))
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum TransferBackend {
    Local,
    Nfs,
    Cifs,
    S3,
    Hdfs,
}

impl TransferBackend {
    const fn env_prefix(self) -> &'static str {
        match self {
            Self::Local => "DATA_MOVER_LOCAL",
            Self::Nfs => "DATA_MOVER_NFS",
            Self::Cifs => "DATA_MOVER_CIFS",
            Self::S3 => "DATA_MOVER_S3",
            Self::Hdfs => "DATA_MOVER_HDFS",
        }
    }
}

pub(crate) fn resolve_transfer_concurrency(
    backend: TransferBackend,
    defaults: TransferConcurrency,
    explicit: Option<TransferConcurrency>,
) -> Result<TransferConcurrency> {
    resolve_with(backend, defaults, explicit, |name| std::env::var(name).ok())
}

fn resolve_with<F>(
    backend: TransferBackend,
    defaults: TransferConcurrency,
    explicit: Option<TransferConcurrency>,
    lookup: F,
) -> Result<TransferConcurrency>
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(explicit) = explicit {
        return Ok(explicit);
    }

    let backend_read = format!("{}_READ_INFLIGHT", backend.env_prefix());
    let backend_write = format!("{}_WRITE_INFLIGHT", backend.env_prefix());
    let shared = lookup("DATA_MOVER_INFLIGHT");
    let read = parse_env(
        &backend_read,
        lookup(&backend_read),
        "DATA_MOVER_READ_INFLIGHT",
        lookup("DATA_MOVER_READ_INFLIGHT"),
        shared.clone(),
        defaults.read(),
    )?;
    let write = parse_env(
        &backend_write,
        lookup(&backend_write),
        "DATA_MOVER_WRITE_INFLIGHT",
        lookup("DATA_MOVER_WRITE_INFLIGHT"),
        shared,
        defaults.write(),
    )?;
    TransferConcurrency::new(read, write)
}

fn parse_env(
    backend_name: &str,
    backend_value: Option<String>,
    global_name: &str,
    global_value: Option<String>,
    shared_value: Option<String>,
    default: usize,
) -> Result<usize> {
    let (name, value) = match (backend_value, global_value, shared_value) {
        (Some(value), _, _) => (backend_name, value),
        (None, Some(value), _) => (global_name, value),
        (None, None, Some(value)) => ("DATA_MOVER_INFLIGHT", value),
        (None, None, None) => return Ok(default),
    };
    let parsed = value.parse::<usize>().map_err(|_| {
        StorageError::ConfigError(format!(
            "{name} must be an integer between 1 and {}, got {value:?}",
            TransferConcurrency::MAX
        ))
    })?;
    validate(name, parsed)?;
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::{AssertTestError, AssertTestValue};

    fn resolve(values: &[(&str, &str)]) -> Result<TransferConcurrency> {
        let values: HashMap<&str, &str> = values.iter().copied().collect();
        resolve_with(
            TransferBackend::Nfs,
            TransferConcurrency::defaults(4, 8),
            None,
            |name| values.get(name).map(|value| (*value).to_string()),
        )
    }

    #[test]
    fn defaults_are_used_without_environment() {
        assert_eq!(
            resolve(&[]).assert_value("default concurrency"),
            TransferConcurrency::defaults(4, 8)
        );
    }

    #[test]
    fn backend_environment_overrides_global_per_direction() {
        let concurrency = resolve(&[
            ("DATA_MOVER_READ_INFLIGHT", "2"),
            ("DATA_MOVER_WRITE_INFLIGHT", "3"),
            ("DATA_MOVER_NFS_READ_INFLIGHT", "6"),
        ])
        .assert_value("resolved concurrency");
        assert_eq!(concurrency.read(), 6);
        assert_eq!(concurrency.write(), 3);
    }

    #[test]
    fn explicit_configuration_overrides_environment() {
        let explicit = TransferConcurrency::new(7, 9).assert_value("explicit concurrency");
        let resolved = resolve_with(
            TransferBackend::Nfs,
            TransferConcurrency::defaults(4, 8),
            Some(explicit),
            |_| Some("invalid-but-ignored".to_string()),
        )
        .assert_value("explicit precedence");
        assert_eq!(resolved, explicit);
    }

    #[test]
    fn invalid_environment_is_a_configuration_error() {
        let error =
            resolve(&[("DATA_MOVER_NFS_WRITE_INFLIGHT", "0")]).assert_error("zero must fail");
        assert!(matches!(error, StorageError::ConfigError(_)));

        let error = resolve(&[("DATA_MOVER_NFS_READ_INFLIGHT", "many")])
            .assert_error("non-number must fail");
        assert!(matches!(error, StorageError::ConfigError(_)));

        let error = resolve(&[("DATA_MOVER_NFS_READ_INFLIGHT", "25")])
            .assert_error("value above the tested safe limit must fail");
        assert!(matches!(error, StorageError::ConfigError(_)));
    }

    #[test]
    fn shared_environment_sets_both_depths_and_preserves_direction_overrides() {
        for depth in [8, 16, 24] {
            let value = depth.to_string();
            assert_eq!(
                resolve(&[("DATA_MOVER_INFLIGHT", &value)]).assert_value("shared depth"),
                TransferConcurrency::new(depth, depth).assert_value("valid depth")
            );
        }
        assert_eq!(
            resolve(&[
                ("DATA_MOVER_INFLIGHT", "24"),
                ("DATA_MOVER_READ_INFLIGHT", "16"),
                ("DATA_MOVER_NFS_WRITE_INFLIGHT", "8")
            ])
            .assert_value("direction overrides"),
            TransferConcurrency::new(16, 8).assert_value("expected depths")
        );
        for value in ["0", "25", "many"] {
            assert!(resolve(&[("DATA_MOVER_INFLIGHT", value)]).is_err());
        }
        assert!(
            resolve(&[
                ("DATA_MOVER_INFLIGHT", "invalid"),
                ("DATA_MOVER_READ_INFLIGHT", "8"),
                ("DATA_MOVER_WRITE_INFLIGHT", "16")
            ])
            .is_ok()
        );
    }

    #[test]
    fn hdfs_backend_uses_backend_specific_inflight_values() {
        let values = HashMap::from([
            ("DATA_MOVER_HDFS_READ_INFLIGHT", "8"),
            ("DATA_MOVER_HDFS_WRITE_INFLIGHT", "1"),
        ]);
        let resolved = resolve_with(
            TransferBackend::Hdfs,
            TransferConcurrency::defaults(4, 1),
            None,
            |name| values.get(name).map(|value| (*value).to_string()),
        )
        .assert_value("HDFS-specific concurrency");
        assert_eq!(resolved.read(), 8);
        assert_eq!(resolved.write(), 1);
    }
}
