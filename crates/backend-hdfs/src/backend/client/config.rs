use std::future::Future;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use hdfs_native::file::FileWriter;
use hdfs_native::{Client, ClientBuilder, HdfsError};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::error::{HdfsErrorKind, StorageError};

const DEFAULT_BLOCK_SIZE: u64 = 128 * crate::MB;

const HDFS_ADAPTER_RETRY_DELAYS: [Duration; 4] = [
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(2),
];
const HDFS_ADAPTER_MAX_ATTEMPTS: usize = HDFS_ADAPTER_RETRY_DELAYS.len() + 1;
const HDFS_ADAPTER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

async fn retry_hdfs_read<T, F, Fut>(
    operation: &'static str,
    relative_path: Option<&Path>,
    cancel: Option<&CancellationToken>,
    mut action: F,
) -> Result<T, StorageError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, hdfs_native::HdfsError>>,
{
    retry_hdfs_read_indexed(operation, relative_path, cancel, |_| action()).await
}

async fn retry_hdfs_read_indexed<T, F, Fut>(
    operation: &'static str,
    relative_path: Option<&Path>,
    cancel: Option<&CancellationToken>,
    mut action: F,
) -> Result<T, StorageError>
where
    F: FnMut(usize) -> Fut,
    Fut: Future<Output = Result<T, hdfs_native::HdfsError>>,
{
    for attempt in 0..HDFS_ADAPTER_MAX_ATTEMPTS {
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            return Err(StorageError::Cancelled);
        }
        let future = tokio::time::timeout(HDFS_ADAPTER_ATTEMPT_TIMEOUT, action(attempt));
        let result = if let Some(token) = cancel {
            tokio::select! {
                biased;
                () = token.cancelled() => return Err(StorageError::Cancelled),
                result = future => result,
            }
        } else {
            future.await
        };
        let result = match result {
            Ok(result) => result,
            Err(_) => Err(adapter_deadline_error()),
        };
        match result {
            Ok(value) => return Ok(value),
            Err(error) => {
                let mapped = hdfs_operation_error(operation, relative_path, &error);
                let retryable = matches!(
                    &mapped,
                    StorageError::HdfsOperation(details) if details.retryable
                );
                if !retryable {
                    return Err(mapped);
                }
                let Some(delay) = HDFS_ADAPTER_RETRY_DELAYS.get(attempt).copied() else {
                    return Err(mapped);
                };
                if let Some(token) = cancel {
                    tokio::select! {
                        biased;
                        () = token.cancelled() => return Err(StorageError::Cancelled),
                        () = tokio::time::sleep(delay) => {}
                    }
                } else {
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
    unreachable!("HDFS adapter retry loop always returns")
}

/// The failure of one adapter attempt that outlived [`HDFS_ADAPTER_ATTEMPT_TIMEOUT`]: a
/// retryable I/O failure.
fn adapter_deadline_error() -> HdfsError {
    HdfsError::IOError(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "HDFS adapter attempt deadline exceeded",
    ))
}

/// Cloneable positional-read handle. `FileReader::read_range` uses `&self` and
/// does not mutate its cursor, so cloned handles can read independent chunks.
#[derive(Clone)]
struct HdfsReadHandle {
    reader: Arc<hdfs_native::file::FileReader>,
    length: u64,
    relative_path: PathBuf,
}

pub use data_mover_storage_core::storage::config::hdfs::{HdfsConfig, HdfsLocation, HdfsEndpointKind, HdfsKerberosCredentials};
#[allow(clippy::wildcard_imports)]
use data_mover_storage_core::storage::config::hdfs::*;

/// Parse an explicit HDFS location and construct its async client.
///
/// # Errors
///
/// Returns an error when the explicit location/configuration is invalid or the
/// upstream client cannot be constructed.
pub fn build_hdfs_client(
    location: &str,
    config: &HdfsConfig,
) -> Result<(Client, HdfsLocation), StorageError> {
    let parsed = HdfsLocation::parse_configured(location, config)?;
    let config_dir = match &config.config_dir {
        Some(directory) => directory.clone(),
        None => std::env::current_exe().unwrap_or_else(|_| {
            std::env::temp_dir().join(format!(
                "data-mover-no-hadoop-config-{}",
                std::process::id()
            ))
        }),
    };
    let mut overrides = config.overrides.clone();
    let authentication = if config.kerberos_credentials.is_some() {
        "kerberos"
    } else {
        "simple"
    };
    overrides
        .entry("hadoop.security.authentication".to_string())
        .or_insert_with(|| authentication.to_string());
    let mut builder = ClientBuilder::new()
        .with_url(parsed.endpoint())
        .with_config_dir(config_dir.to_string_lossy())
        .with_config(overrides);
    if let Some(credentials) = &config.kerberos_credentials {
        let principal = credentials
            .principal
            .clone()
            .unwrap_or_else(|| parsed.user().to_string());
        if credentials.keytab.is_none() && credentials.cache.is_none() {
            return Err(config_error(
                "HDFS Kerberos credentials require a keytab or credential cache",
            ));
        }
        let keytab = credentials
            .keytab
            .as_deref()
            .map(|keytab| {
                keytab
                    .to_str()
                    .ok_or_else(|| config_error("HDFS keytab path must be valid UTF-8"))
                    .map(str::to_string)
            })
            .transpose()?;
        builder = builder.with_kerberos_principal(principal);
        if let Some(keytab) = keytab {
            builder = builder.with_kerberos_keytab(keytab);
        }
        if let Some(cache) = &credentials.cache {
            builder = builder.with_kerberos_cache(cache);
        }
    } else {
        builder = builder.with_user(parsed.user());
    }
    let client = builder.build().map_err(|error| {
        StorageError::ConfigError(format!("failed to construct HDFS client: {error}"))
    })?;
    Ok((client, parsed))
}
