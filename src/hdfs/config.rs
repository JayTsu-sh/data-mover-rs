use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt as _;
use futures::stream::FuturesOrdered;
use percent_encoding::percent_decode_str;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use url::{Host, Url};

use crate::HDFSEntry;
use crate::checksum::{ConsistencyCheck, HashCalculator, create_hash_calculator};
use crate::error::StorageError;
use crate::filter::{FilterInput, should_skip};
use hdfs_native::{Client, ClientBuilder};

const DEFAULT_BLOCK_SIZE: u64 = 8 * crate::MB;
const MAX_TRANSFER_CHUNK_SIZE: u64 = 2 * crate::MB;

fn transfer_chunk_size(block_size: u64) -> u64 {
    block_size.clamp(1, MAX_TRANSFER_CHUNK_SIZE)
}
const SCAN_CHANNEL_CAPACITY: usize = 256;
const HDFS_ADAPTER_RETRY_DELAYS: [Duration; 4] = [
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(2),
];
const HDFS_ADAPTER_MAX_ATTEMPTS: usize = HDFS_ADAPTER_RETRY_DELAYS.len() + 1;
const HDFS_ADAPTER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
struct SequentialWriteContext<'a> {
    relative_path: &'a Path,
    start_offset: u64,
    expected_size: u64,
    require_final_size: bool,
    bytes_counter: Option<&'a Arc<AtomicU64>>,
    on_committed: Option<&'a crate::CommitCallback>,
}
pub(crate) enum AppendCompletion {
    Complete(u64),
    PartialUpTo(u64),
}

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
            Err(_) => Err(hdfs_native::HdfsError::IOError(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "HDFS adapter attempt deadline exceeded",
            ))),
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

/// Private HDFS scan event consumed by the later common-walk adapter.
#[derive(Debug)]
pub enum HdfsScanEvent {
    Entry(HDFSEntry),
    Error { path: PathBuf, error: StorageError },
}

/// Cloneable positional-read handle. `FileReader::read_range` uses `&self` and
/// does not mutate its cursor, so cloned handles can read independent chunks.
#[derive(Clone)]
pub struct HDFSFileHandle {
    reader: Arc<hdfs_native::file::FileReader>,
    length: u64,
    relative_path: PathBuf,
}

/// Explicit HDFS location for a direct `NameNode` or logical `NameService`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HdfsLocation {
    endpoint: String,
    kind: HdfsEndpointKind,
    user: String,
    root: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HdfsEndpointKind {
    /// A single explicit `NameNode` RPC endpoint.
    Direct,
    /// A logical Hadoop HA `NameService`.
    NameService,
}

/// Explicit Hadoop configuration used for HDFS location resolution.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct HdfsConfig {
    pub config_dir: Option<PathBuf>,
    pub overrides: HashMap<String, String>,
    pub kerberos_credentials: Option<HdfsKerberosCredentials>,
}

/// Kerberos credential material scoped to one HDFS backend instance.
///
/// The principal is parsed from the HDFS location URL so source and destination
/// backends in the same process can use independent identities.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct HdfsKerberosCredentials {
    /// Kerberos principal to acquire. When omitted, the user in the HDFS URL is used.
    pub principal: Option<String>,
    /// Keytab from which initiator credentials are acquired.
    pub keytab: Option<PathBuf>,
    /// Kerberos credential cache to read from or populate.
    pub cache: Option<String>,
}

impl fmt::Debug for HdfsKerberosCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HdfsKerberosCredentials")
            .field("principal", &self.principal)
            .field("keytab", &self.keytab.as_ref().map(|_| "<redacted>"))
            .field("cache", &self.cache.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl fmt::Debug for HdfsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let overrides = self
            .overrides
            .iter()
            .map(|(key, value)| {
                let safe_value = if is_sensitive_key(key) {
                    "<redacted>"
                } else {
                    value
                };
                (key, safe_value)
            })
            .collect::<HashMap<_, _>>();
        formatter
            .debug_struct("HdfsConfig")
            .field("config_dir", &self.config_dir)
            .field("overrides", &overrides)
            .field("kerberos_credentials", &self.kerberos_credentials)
            .finish()
    }
}

impl HdfsLocation {
    /// Parse `hdfs://user@host:port/root` without consulting ambient Hadoop
    /// configuration or authentication state.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the URL is not an explicit,
    /// password-free Simple-auth direct `NameNode` location.
    pub fn parse(location: &str) -> Result<Self, StorageError> {
        let parsed = Self::parse_configured(location, &HdfsConfig::default())?;
        if parsed.kind != HdfsEndpointKind::Direct {
            return Err(config_error("direct HDFS location requires an RPC port"));
        }
        Ok(parsed)
    }

    /// Parse either a direct `NameNode` or explicitly configured logical
    /// `NameService` location.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for incomplete topology, non-Simple
    /// authentication, or an invalid location.
    pub fn parse_configured(location: &str, config: &HdfsConfig) -> Result<Self, StorageError> {
        validate_percent_encoding(location)?;
        reject_raw_traversal(location)?;
        let url =
            Url::parse(location).map_err(|error| StorageError::UrlParseError(error.to_string()))?;

        if url.scheme() != "hdfs" {
            return Err(config_error("HDFS location must use the hdfs scheme"));
        }
        if url.password().is_some() {
            return Err(config_error("HDFS location must not contain a password"));
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(config_error(
                "HDFS location must not contain a query or fragment",
            ));
        }

        let user = decode_component(url.username(), "username")?;
        if user.is_empty() {
            return Err(config_error("HDFS location requires an explicit user"));
        }
        let host = url
            .host()
            .ok_or_else(|| config_error("HDFS location requires a NameNode host"))?;
        let settings = config.settings()?;
        validate_authentication(&settings, config.kerberos_credentials.as_ref())?;
        let (endpoint, kind) = match url.port() {
            Some(port) => (format_endpoint(&host, port), HdfsEndpointKind::Direct),
            None => (
                format_nameservice_endpoint(&host, &settings)?,
                HdfsEndpointKind::NameService,
            ),
        };
        let root = normalize_root(url.path())?;

        Ok(Self {
            endpoint,
            kind,
            user,
            root,
        })
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[must_use]
    pub const fn kind(&self) -> HdfsEndpointKind {
        self.kind
    }

    #[must_use]
    pub fn user(&self) -> &str {
        &self.user
    }

    #[must_use]
    pub fn root(&self) -> &str {
        &self.root
    }
}

impl HdfsConfig {
    fn settings(&self) -> Result<HashMap<String, String>, StorageError> {
        let mut settings = HashMap::new();
        if let Some(directory) = &self.config_dir {
            for filename in ["core-site.xml", "hdfs-site.xml"] {
                let path = directory.join(filename);
                if path.exists() {
                    let contents = fs::read_to_string(&path).map_err(StorageError::IoError)?;
                    let document: HadoopConfiguration = quick_xml::de::from_str(&contents)
                        .map_err(|error| StorageError::ConfigError(error.to_string()))?;
                    for property in document.properties {
                        settings.insert(
                            property.name.trim().to_string(),
                            property.value.trim().to_string(),
                        );
                    }
                }
            }
        }
        settings.extend(self.overrides.clone());
        Ok(settings)
    }
}

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
