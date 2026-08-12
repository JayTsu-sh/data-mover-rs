use crate::Result;
use crate::error::StorageError;

/// Per-file data-transfer concurrency.
///
/// Read and write depths are deliberately independent: sources and destinations
/// have different latency and protocol limits. Values are always in `1..=16`.
///
/// Factory-created storage resolves each direction independently in this order:
/// backend-specific environment variable, global environment variable, then the
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
    /// The real-protocol lab shows sharply rising CPU and memory consumption
    /// above the throughput knee at eight, with little aggregate benefit at
    /// sixteen. Keeping the hard limit at the highest validated depth prevents
    /// accidental resource exhaustion from a mistyped environment variable.
    pub const MAX: usize = 16;

    /// Creates validated read/write queue depths.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when either value is outside `1..=16`.
    pub fn new(read: usize, write: usize) -> Result<Self> {
        validate("read", read)?;
        validate("write", write)?;
        Ok(Self { read, write })
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
}

impl TransferBackend {
    const fn env_prefix(self) -> &'static str {
        match self {
            Self::Local => "DATA_MOVER_LOCAL",
            Self::Nfs => "DATA_MOVER_NFS",
            Self::Cifs => "DATA_MOVER_CIFS",
            Self::S3 => "DATA_MOVER_S3",
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
    let read = parse_env(
        &backend_read,
        lookup(&backend_read),
        "DATA_MOVER_READ_INFLIGHT",
        lookup("DATA_MOVER_READ_INFLIGHT"),
        defaults.read(),
    )?;
    let write = parse_env(
        &backend_write,
        lookup(&backend_write),
        "DATA_MOVER_WRITE_INFLIGHT",
        lookup("DATA_MOVER_WRITE_INFLIGHT"),
        defaults.write(),
    )?;
    TransferConcurrency::new(read, write)
}

fn parse_env(
    backend_name: &str,
    backend_value: Option<String>,
    global_name: &str,
    global_value: Option<String>,
    default: usize,
) -> Result<usize> {
    let (name, value) = match (backend_value, global_value) {
        (Some(value), _) => (backend_name, value),
        (None, Some(value)) => (global_name, value),
        (None, None) => return Ok(default),
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

        let error = resolve(&[("DATA_MOVER_NFS_READ_INFLIGHT", "17")])
            .assert_error("value above the tested safe limit must fail");
        assert!(matches!(error, StorageError::ConfigError(_)));
    }
}
