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
use hdfs_native::{Client, ClientBuilder, KerberosCredentials};

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
#[derive(Clone, Eq, PartialEq)]
pub enum HdfsKerberosCredentials {
    CredentialCache { cache: String },
    Keytab { keytab: PathBuf },
}

impl fmt::Debug for HdfsKerberosCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CredentialCache { .. } => formatter
                .debug_struct("CredentialCache")
                .field("cache", &"<redacted>")
                .finish(),
            Self::Keytab { .. } => formatter
                .debug_struct("Keytab")
                .field("keytab", &"<redacted>")
                .finish(),
        }
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
        let principal = parsed.user().to_string();
        let credentials = match credentials {
            HdfsKerberosCredentials::CredentialCache { cache } => {
                KerberosCredentials::CredentialCache {
                    principal,
                    cache: cache.clone(),
                }
            }
            HdfsKerberosCredentials::Keytab { keytab } => {
                let keytab = keytab
                    .to_str()
                    .ok_or_else(|| config_error("HDFS keytab path must be valid UTF-8"))?
                    .to_string();
                KerberosCredentials::Keytab { principal, keytab }
            }
        };
        builder = builder.with_kerberos_credentials(credentials);
    } else {
        builder = builder.with_user(parsed.user());
    }
    let client = builder.build().map_err(|error| {
        StorageError::ConfigError(format!("failed to construct HDFS client: {error}"))
    })?;
    Ok((client, parsed))
}

/// Concrete HDFS client ownership and root-isolation shell.
#[derive(Clone, Debug)]
pub struct HDFSStorage {
    client: Client,
    location: HdfsLocation,
    block_size: u64,
    transfer_concurrency: crate::TransferConcurrency,
}

impl HDFSStorage {
    #[must_use]
    pub const fn client(&self) -> &Client {
        &self.client
    }

    #[must_use]
    pub const fn location(&self) -> &HdfsLocation {
        &self.location
    }

    #[must_use]
    pub const fn block_size(&self) -> u64 {
        self.block_size
    }

    #[must_use]
    pub const fn transfer_concurrency(&self) -> crate::TransferConcurrency {
        self.transfer_concurrency
    }

    #[must_use]
    pub const fn with_transfer_concurrency(
        mut self,
        concurrency: crate::TransferConcurrency,
    ) -> Self {
        self.transfer_concurrency = concurrency;
        self
    }

    /// Resolve a relative adapter path below the configured HDFS root.
    ///
    /// # Errors
    ///
    /// Returns an error for absolute paths, traversal, or non-UTF-8 components.
    pub fn resolve_path(&self, relative: &std::path::Path) -> Result<String, StorageError> {
        let mut parts = Vec::new();
        for component in relative.components() {
            match component {
                std::path::Component::Normal(value) => parts.push(
                    value
                        .to_str()
                        .ok_or_else(|| config_error("HDFS paths must be valid UTF-8"))?,
                ),
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir
                | std::path::Component::RootDir
                | std::path::Component::Prefix(_) => {
                    return Err(config_error(
                        "HDFS adapter paths must remain relative to the configured root",
                    ));
                }
            }
        }
        if parts.is_empty() {
            Ok(self.location.root.clone())
        } else if self.location.root == "/" {
            Ok(format!("/{}", parts.join("/")))
        } else {
            Ok(format!("{}/{}", self.location.root, parts.join("/")))
        }
    }

    /// Convert an upstream status into metadata relative to this storage root.
    ///
    /// # Errors
    ///
    /// Returns an error if the status is outside the configured root or its
    /// millisecond timestamps cannot be represented as nanoseconds.
    pub fn entry_from_status(
        &self,
        status: hdfs_native::client::FileStatus,
    ) -> Result<HDFSEntry, StorageError> {
        let relative = strip_root(&status.path, self.location.root())?;
        let relative_path = PathBuf::from(relative);
        let name = relative_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_string();
        let extension = relative_path
            .extension()
            .and_then(|extension| extension.to_str())
            .map(str::to_string);
        Ok(HDFSEntry {
            name,
            relative_path,
            is_dir: status.isdir,
            size: u64::try_from(status.length)
                .map_err(|_| config_error("HDFS file size does not fit u64"))?,
            mtime: millis_to_nanos(status.modification_time)?,
            atime: millis_to_nanos(status.access_time)?,
            mode: u32::from(status.permission),
            owner: status.owner,
            group: status.group,
            replication: status.replication,
            block_size: status.blocksize,
            extension,
        })
    }

    /// Look up metadata for a path relative to the configured HDFS root.
    ///
    /// # Errors
    ///
    /// Returns an error when the path escapes the configured root, does not
    /// exist, or the upstream status cannot be represented by `HDFSEntry`.
    pub async fn get_metadata(
        &self,
        relative_path: &std::path::Path,
    ) -> Result<HDFSEntry, StorageError> {
        let path = self.resolve_path(relative_path)?;
        let status = retry_hdfs_read("get metadata", Some(relative_path), None, || {
            self.client.get_file_info(&path)
        })
        .await?;
        self.entry_from_status(status)
    }

    /// Set Unix permission bits on a file or directory below this storage root.
    ///
    /// # Errors
    ///
    /// Returns an error for the storage root, escaping paths, permission bits
    /// outside HDFS' `0o7777` mask, or an upstream mutation failure.
    pub async fn set_permission(
        &self,
        relative_path: &std::path::Path,
        mode: u32,
    ) -> Result<(), StorageError> {
        if mode > 0o7777 {
            return Err(config_error("HDFS permission must fit in 0o7777"));
        }
        let path = self.resolve_mutation_path(relative_path)?;
        self.client
            .set_permission(&path, mode)
            .await
            .map_err(|error| hdfs_operation_error("set permission", Some(relative_path), &error))
    }

    /// Set modification time while preserving the current HDFS access time.
    ///
    /// The public timestamp uses the crate's nanosecond convention. HDFS stores
    /// millisecond timestamps, so sub-millisecond precision is truncated.
    ///
    /// # Errors
    ///
    /// Returns an error for a negative timestamp, the storage root, an escaping
    /// path, metadata lookup failure, or an upstream mutation failure.
    pub async fn set_mtime(
        &self,
        relative_path: &std::path::Path,
        mtime: i64,
    ) -> Result<(), StorageError> {
        let mtime = nanos_to_millis(mtime)?;
        let path = self.resolve_mutation_path(relative_path)?;
        let current = self.get_metadata(relative_path).await?;
        let atime = nanos_to_millis(current.atime)?;
        self.client
            .set_times(&path, mtime, atime)
            .await
            .map_err(|error| hdfs_operation_error("set times", Some(relative_path), &error))
    }

    /// Set HDFS string owner and/or group, leaving omitted values unchanged.
    ///
    /// Empty identity strings are treated as omitted. No numeric or `NFSv4`
    /// identity mapping is performed.
    ///
    /// # Errors
    ///
    /// Returns an error for the storage root, an escaping path, or an upstream
    /// mutation failure.
    pub async fn set_owner_group(
        &self,
        relative_path: &std::path::Path,
        owner: Option<&str>,
        group: Option<&str>,
    ) -> Result<(), StorageError> {
        let path = self.resolve_mutation_path(relative_path)?;
        let owner = owner.filter(|value| !value.is_empty());
        let group = group.filter(|value| !value.is_empty());
        if owner.is_none() && group.is_none() {
            return Ok(());
        }
        self.client
            .set_owner(&path, owner, group)
            .await
            .map_err(|error| hdfs_operation_error("set owner", Some(relative_path), &error))
    }

    fn resolve_mutation_path(
        &self,
        relative_path: &std::path::Path,
    ) -> Result<String, StorageError> {
        if relative_path.as_os_str().is_empty() {
            return Err(StorageError::InvalidPath(
                "refusing to mutate the configured HDFS root".to_string(),
            ));
        }
        self.resolve_path(relative_path)
    }

    /// List exactly the immediate children of one directory below this root.
    ///
    /// # Errors
    ///
    /// Returns an error when the path is missing, is not a directory, escapes
    /// the configured root, or upstream returns a non-immediate child.
    pub async fn list_directory(
        &self,
        relative_path: &std::path::Path,
    ) -> Result<Vec<HDFSEntry>, StorageError> {
        let directory = self.get_metadata(relative_path).await?;
        if !directory.is_dir {
            return Err(StorageError::InvalidPath(format!(
                "HDFS listing target is not a directory: {}",
                relative_path.display()
            )));
        }
        let path = self.resolve_path(relative_path)?;
        let statuses = retry_hdfs_read("list directory", Some(relative_path), None, || {
            self.client.list_status(&path, false)
        })
        .await?;
        self.entries_from_listing(statuses, &directory.relative_path)
    }

    fn entries_from_listing(
        &self,
        statuses: Vec<hdfs_native::client::FileStatus>,
        directory: &std::path::Path,
    ) -> Result<Vec<HDFSEntry>, StorageError> {
        statuses
            .into_iter()
            .map(|status| {
                let entry = self.entry_from_status(status)?;
                if entry.relative_path.parent() != Some(directory) {
                    return Err(config_error(
                        "HDFS directory listing returned a non-immediate child",
                    ));
                }
                Ok(entry)
            })
            .collect()
    }

    /// Start a bounded recursive HDFS scan without applying common walk
    /// filtering or message semantics.
    ///
    /// # Errors
    ///
    /// Returns an error when the starting path escapes the configured root or
    /// concurrency is outside the validated `1..=16` range. Runtime listing
    /// failures are emitted as [`HdfsScanEvent::Error`].
    pub fn scan_recursive(
        &self,
        sub_path: Option<&std::path::Path>,
        max_depth: Option<usize>,
        concurrency: usize,
    ) -> Result<crate::AsyncReceiver<HdfsScanEvent>, StorageError> {
        if !(1..=crate::TransferConcurrency::MAX).contains(&concurrency) {
            return Err(config_error(&format!(
                "HDFS scan concurrency must be between 1 and {}, got {concurrency}",
                crate::TransferConcurrency::MAX
            )));
        }
        let root = sub_path.unwrap_or_else(|| std::path::Path::new(""));
        self.resolve_path(root)?;
        let (sender, receiver) = async_channel::bounded(SCAN_CHANNEL_CAPACITY);
        let storage = self.clone();
        let root = root.to_path_buf();
        tokio::spawn(async move {
            storage
                .run_recursive_scan(root, max_depth, concurrency, sender)
                .await;
        });
        Ok(crate::AsyncReceiver::new(receiver))
    }

    /// Adapt the HDFS scanner to the common walk stream.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid scan options, an escaping subpath, or HDFS
    /// packaging, which is not implemented.
    pub fn walkdir(
        &self,
        sub_path: Option<&std::path::Path>,
        options: crate::storage_enum::WalkOptions,
    ) -> Result<crate::WalkDirAsyncIterator, StorageError> {
        if options.packaged {
            return Err(StorageError::UnsupportedType(
                "HDFS packaged walkdir is not implemented".to_string(),
            ));
        }
        let max_depth = options.depth.filter(|depth| *depth != 0);
        let raw = self.scan_recursive(sub_path, max_depth, options.concurrency)?;
        let (sender, receiver) = async_channel::bounded(SCAN_CHANNEL_CAPACITY);
        tokio::spawn(async move {
            while let Some(event) = raw.next().await {
                let message = match event {
                    HdfsScanEvent::Entry(entry) => {
                        if hdfs_entry_is_filtered(
                            &entry,
                            options.match_expressions.as_ref(),
                            options.exclude_expressions.as_ref(),
                        ) {
                            continue;
                        }
                        crate::StorageEntryMessage::Scanned(Arc::new(crate::EntryEnum::HDFS(entry)))
                    }
                    HdfsScanEvent::Error { path, error } => crate::StorageEntryMessage::Error {
                        event: crate::ErrorEvent::Scan,
                        path,
                        entry: None,
                        reason: error.to_string(),
                    },
                };
                if sender.send(message).await.is_err() {
                    break;
                }
            }
        });
        Ok(crate::AsyncReceiver::new(receiver))
    }

    /// Read and sort one HDFS directory for the shared `walkdir_2` DFS driver.
    ///
    /// # Errors
    ///
    /// Returns an error for a non-HDFS handle or when the directory cannot be
    /// listed safely inside the configured root.
    pub async fn read_dir_sorted(
        &self,
        dir_path: &str,
        handle: &crate::dir_tree::DirHandle,
        ctx: &crate::dir_tree::ReadContext,
    ) -> Result<crate::dir_tree::ReadResult, StorageError> {
        let crate::dir_tree::DirHandle::Hdfs(relative_path) = handle else {
            return Err(StorageError::MismatchedType);
        };
        let entries = self.list_directory(relative_path).await?;
        Ok(hdfs_read_result(dir_path, entries, ctx))
    }

    /// Run HDFS directory paging through the shared deterministic DFS driver.
    ///
    /// # Errors
    ///
    /// Returns an error when the subpath escapes the configured root or reader
    /// concurrency is outside the validated `1..=16` range.
    pub fn walkdir_2(
        &self,
        sub_path: Option<&std::path::Path>,
        depth: Option<usize>,
        match_expressions: Option<crate::FilterExpression>,
        exclude_expressions: Option<crate::FilterExpression>,
        concurrency: usize,
    ) -> Result<crate::WalkDirAsyncIterator2, StorageError> {
        use crate::dir_tree::{DirHandle, ReadContext, ReadRequest, run_dfs_driver};

        if !(1..=crate::TransferConcurrency::MAX).contains(&concurrency) {
            return Err(config_error(&format!(
                "HDFS walkdir_2 concurrency must be between 1 and {}, got {concurrency}",
                crate::TransferConcurrency::MAX
            )));
        }
        let start_path = sub_path.unwrap_or_else(|| std::path::Path::new(""));
        self.resolve_path(start_path)?;
        let start_path = start_path.to_path_buf();
        let (request_sender, request_receiver) =
            async_channel::bounded::<ReadRequest>(concurrency * 2);
        let (output_sender, output_receiver) = async_channel::bounded(64);

        for _ in 0..concurrency {
            let storage = self.clone();
            let receiver = request_receiver.clone();
            tokio::spawn(async move {
                while let Ok(request) = receiver.recv().await {
                    let result = storage
                        .read_dir_sorted(&request.dir_path, &request.handle, &request.ctx)
                        .await;
                    let _ = request.reply.send(result);
                }
            });
        }

        let context = ReadContext {
            match_expr: Arc::new(match_expressions),
            exclude_expr: Arc::new(exclude_expressions),
            current_depth: 0,
            max_depth: depth.unwrap_or(0),
            apply_filter: true,
            include_tags: false,
            is_versioned: false,
        };
        tokio::spawn(run_dfs_driver(
            request_sender,
            output_sender,
            PathBuf::new(),
            DirHandle::Hdfs(start_path),
            context,
        ));
        Ok(crate::AsyncReceiver::new(output_receiver))
    }

    /// Recursively create a directory below the configured HDFS root.
    ///
    /// # Errors
    ///
    /// Returns an error when the path escapes the root, an existing component
    /// is not a directory, or HDFS rejects the requested operation.
    pub async fn create_dir_all(
        &self,
        relative_path: &std::path::Path,
        mode: u16,
    ) -> Result<(), StorageError> {
        let path = self.resolve_path(relative_path)?;
        if relative_path.as_os_str().is_empty() {
            let status = retry_hdfs_read("get root metadata", None, None, || {
                self.client.get_file_info(&path)
            })
            .await?;
            if !status.isdir {
                return Err(StorageError::InvalidPath(
                    "configured HDFS root is not a directory".to_string(),
                ));
            }
            return Ok(());
        }
        self.client
            .mkdirs(&path, u32::from(mode), true)
            .await
            .map_err(|error| hdfs_operation_error("create directory", Some(relative_path), &error))
    }

    /// Open a regular HDFS file for independent positional reads.
    ///
    /// # Errors
    ///
    /// Returns an error for an escaping path, missing file, directory, or
    /// upstream open failure.
    pub async fn open_file(
        &self,
        relative_path: &std::path::Path,
    ) -> Result<HDFSFileHandle, StorageError> {
        let metadata = self.get_metadata(relative_path).await?;
        if metadata.is_dir {
            return Err(StorageError::InvalidPath(format!(
                "HDFS read target is a directory: {}",
                relative_path.display()
            )));
        }
        let path = self.resolve_path(relative_path)?;
        let reader = retry_hdfs_read("open file", Some(relative_path), None, || {
            self.client.read(&path)
        })
        .await?;
        let length = u64::try_from(reader.file_length())
            .map_err(|_| config_error("HDFS file length does not fit u64"))?;
        Ok(HDFSFileHandle {
            reader: Arc::new(reader),
            length,
            relative_path: relative_path.to_path_buf(),
        })
    }

    /// Read at most `count` bytes at `offset` without changing a shared cursor.
    ///
    /// # Errors
    ///
    /// Returns an error when the platform cannot represent the planned range or
    /// the upstream positional read fails.
    pub async fn read_at(
        &self,
        file: &HDFSFileHandle,
        offset: u64,
        count: u64,
    ) -> Result<bytes::Bytes, StorageError> {
        let Some((offset, length)) = plan_read_range(file.length, offset, count)? else {
            return Ok(bytes::Bytes::new());
        };
        let path = self.resolve_path(&file.relative_path)?;
        retry_hdfs_read_indexed("read range", Some(&file.relative_path), None, |attempt| {
            let reader = file.reader.clone();
            let client = self.client.clone();
            let path = path.clone();
            async move {
                if attempt == 0 {
                    reader.read_range(offset, length).await
                } else {
                    client.read(&path).await?.read_range(offset, length).await
                }
            }
        })
        .await
    }

    pub(crate) async fn read_file(
        &self,
        relative_path: &std::path::Path,
        size: u64,
    ) -> Result<bytes::Bytes, StorageError> {
        let file = self.open_file(relative_path).await?;
        self.read_at(&file, 0, size).await
    }

    pub(crate) async fn read_data(
        &self,
        sender: tokio::sync::mpsc::Sender<crate::DataChunk>,
        relative_path: &std::path::Path,
        size: u64,
        enable_integrity_check: bool,
        qos: Option<crate::QosManager>,
    ) -> Result<Option<HashCalculator>, StorageError> {
        if size == 0 {
            return Ok(None);
        }
        let file = self.open_file(relative_path).await?;
        let end = size.min(file.length);
        let chunk_size = transfer_chunk_size(self.block_size);
        let mut next_offset = 0_u64;
        let mut inflight = FuturesOrdered::new();
        let mut hasher = create_hash_calculator(enable_integrity_check);
        loop {
            while inflight.len() < self.transfer_concurrency.read() && next_offset < end {
                let requested = chunk_size.min(end - next_offset);
                let count = if let Some(qos) = qos.as_ref() {
                    let granted = qos.acquire_bandwidth_grant(requested).await;
                    qos.acquire_iops().await;
                    granted
                } else {
                    requested
                };
                let storage = self.clone();
                let file = file.clone();
                let offset = next_offset;
                inflight.push_back(
                    async move { (offset, storage.read_at(&file, offset, count).await) },
                );
                next_offset += count;
            }
            let Some((offset, data)) = inflight.next().await else {
                break;
            };
            let data = data?;
            if let Some(hasher) = hasher.as_mut() {
                hasher.update(&data);
            }
            if sender
                .send(crate::DataChunk { offset, data })
                .await
                .is_err()
            {
                break;
            }
        }
        Ok(hasher)
    }

    pub(crate) async fn read_data_intervals(
        &self,
        sender: tokio::sync::mpsc::Sender<crate::DataChunk>,
        relative_path: &std::path::Path,
        intervals: &[(u64, u64)],
        qos: Option<crate::QosManager>,
    ) -> Result<(), StorageError> {
        let file = self.open_file(relative_path).await?;
        let chunk_size = transfer_chunk_size(self.block_size);
        for &(start, end) in intervals {
            if start > end {
                return Err(config_error("HDFS read interval start exceeds end"));
            }
            let mut offset = start;
            while offset < end.min(file.length) {
                let requested = chunk_size.min(end - offset);
                let count = if let Some(qos) = qos.as_ref() {
                    let granted = qos.acquire_bandwidth_grant(requested).await;
                    qos.acquire_iops().await;
                    granted
                } else {
                    requested
                };
                let data = self.read_at(&file, offset, count).await?;
                if data.is_empty() {
                    break;
                }
                let read = u64::try_from(data.len())
                    .map_err(|_| config_error("HDFS chunk length does not fit u64"))?;
                if sender
                    .send(crate::DataChunk { offset, data })
                    .await
                    .is_err()
                {
                    return Ok(());
                }
                offset = offset
                    .checked_add(read)
                    .ok_or_else(|| config_error("HDFS read offset overflow"))?;
            }
        }
        Ok(())
    }

    /// Write a fresh file through one strictly sequential upstream writer.
    pub(crate) async fn write_data(
        &self,
        mut receiver: tokio::sync::mpsc::Receiver<crate::DataChunk>,
        relative_path: &std::path::Path,
        expected_size: u64,
        mode: u32,
        replication: Option<u32>,
        bytes_counter: Option<Arc<AtomicU64>>,
    ) -> Result<u64, StorageError> {
        if let Some(parent) = relative_path.parent()
            && !parent.as_os_str().is_empty()
        {
            self.create_dir_all(parent, 0o755).await?;
        }
        let path = self.resolve_path(relative_path)?;
        let options = hdfs_native::WriteOptions::default()
            .block_size(self.block_size)
            .permission(mode & 0o7777)
            .overwrite(true);
        let options = replication.map_or(options.clone(), |value| options.replication(value));
        let mut writer =
            self.client.create(&path, options).await.map_err(|error| {
                hdfs_operation_error("create file", Some(relative_path), &error)
            })?;
        let result = self
            .consume_sequential_chunks(
                &mut writer,
                &mut receiver,
                SequentialWriteContext {
                    relative_path,
                    start_offset: 0,
                    expected_size,
                    require_final_size: true,
                    bytes_counter: bytes_counter.as_ref(),
                    on_committed: None,
                },
            )
            .await;
        if result.is_err() {
            receiver.close();
            let _ = self.client.delete(&path, false).await;
        }
        result
    }

    /// Append a contiguous streamed tail to an existing regular file.
    ///
    /// `start_offset` must equal the current file length and every received
    /// chunk must form one contiguous tail ending at `expected_final_size`.
    /// This is sequential append, not positional write support.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid paths, missing files, directories, a stale
    /// prefix length, malformed chunks, append/write failures, or close failure.
    pub async fn append_stream(
        &self,
        receiver: tokio::sync::mpsc::Receiver<crate::DataChunk>,
        relative_path: &std::path::Path,
        start_offset: u64,
        expected_final_size: u64,
    ) -> Result<u64, StorageError> {
        self.append_stream_with_progress(
            receiver,
            relative_path,
            start_offset,
            AppendCompletion::Complete(expected_final_size),
            None,
            None,
        )
        .await
    }

    pub(crate) async fn append_stream_with_progress(
        &self,
        mut receiver: tokio::sync::mpsc::Receiver<crate::DataChunk>,
        relative_path: &std::path::Path,
        start_offset: u64,
        completion: AppendCompletion,
        bytes_counter: Option<&Arc<AtomicU64>>,
        on_committed: Option<&crate::CommitCallback>,
    ) -> Result<u64, StorageError> {
        let (expected_final_size, require_final_size) = match completion {
            AppendCompletion::Complete(size) => (size, true),
            AppendCompletion::PartialUpTo(size) => (size, false),
        };
        if expected_final_size < start_offset {
            return Err(config_error(
                "HDFS append final size cannot precede its starting offset",
            ));
        }
        let metadata = self.get_metadata(relative_path).await?;
        if metadata.is_dir {
            return Err(StorageError::InvalidPath(format!(
                "HDFS append target is a directory: {}",
                relative_path.display()
            )));
        }
        if metadata.size != start_offset {
            return Err(StorageError::OperationError(format!(
                "stale HDFS append offset {start_offset}, current length is {}: {}",
                metadata.size,
                relative_path.display()
            )));
        }
        if start_offset == expected_final_size {
            receiver.close();
            return Ok(start_offset);
        }
        let path = self.resolve_path(relative_path)?;
        let mut writer =
            self.client.append(&path).await.map_err(|error| {
                hdfs_operation_error("append file", Some(relative_path), &error)
            })?;
        self.consume_sequential_chunks(
            &mut writer,
            &mut receiver,
            SequentialWriteContext {
                relative_path,
                start_offset,
                expected_size: expected_final_size,
                require_final_size,
                bytes_counter,
                on_committed,
            },
        )
        .await
    }

    /// Prepare a trusted HDFS temporary file for tail-only resume.
    ///
    /// Returns the validated contiguous prefix length. Missing and explicitly
    /// fresh state is created as an empty file; overlong state is rebuilt.
    ///
    /// # Errors
    ///
    /// Returns an error for a root/escaping path, directory state, inaccessible
    /// metadata, or failure to rebuild the temporary file.
    pub async fn prepare_tail_resume(
        &self,
        part_path: &std::path::Path,
        expected_size: u64,
        resume: bool,
    ) -> Result<u64, StorageError> {
        if part_path.file_name().is_none() {
            return Err(StorageError::InvalidPath(
                "HDFS resume temporary path must name a file".to_string(),
            ));
        }
        self.resolve_path(part_path)?;
        if !resume {
            self.rebuild_resume_file(part_path).await?;
            return Ok(0);
        }
        match self.get_metadata(part_path).await {
            Ok(metadata) if metadata.is_dir => Err(StorageError::InvalidPath(format!(
                "HDFS resume temporary path is a directory: {}",
                part_path.display()
            ))),
            Ok(metadata) if metadata.size <= expected_size => Ok(metadata.size),
            Ok(_) | Err(StorageError::FileNotFound(_)) => {
                self.rebuild_resume_file(part_path).await?;
                Ok(0)
            }
            Err(error) => Err(error),
        }
    }

    /// Atomically publish one completed HDFS resume temporary file.
    ///
    /// # Errors
    ///
    /// Returns an error unless the temporary path is a regular file with the
    /// exact expected length, or when native same-root rename fails.
    pub async fn commit_tail_resume(
        &self,
        part_path: &std::path::Path,
        final_path: &std::path::Path,
        expected_size: u64,
    ) -> Result<(), StorageError> {
        let metadata = self.get_metadata(part_path).await?;
        if metadata.is_dir || metadata.size != expected_size {
            return Err(StorageError::OperationError(format!(
                "HDFS resume temporary file is not commit-ready: size={}, expected={expected_size}: {}",
                metadata.size,
                part_path.display()
            )));
        }
        self.rename(part_path, final_path).await
    }

    async fn rebuild_resume_file(&self, part_path: &std::path::Path) -> Result<(), StorageError> {
        if let Ok(metadata) = self.get_metadata(part_path).await
            && metadata.is_dir
        {
            return Err(StorageError::InvalidPath(format!(
                "HDFS resume temporary path is a directory: {}",
                part_path.display()
            )));
        }
        self.write_file(part_path, bytes::Bytes::new(), 0o644, None)
            .await
    }

    /// Stream a fresh HDFS file from bounded chunks using one sequential writer.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid ranges, gaps beyond the configured reorder
    /// window, size mismatch, path escape, or upstream create/write/close failure.
    pub async fn write_stream(
        &self,
        receiver: tokio::sync::mpsc::Receiver<crate::DataChunk>,
        relative_path: &std::path::Path,
        expected_size: u64,
        mode: u32,
        replication: Option<u32>,
    ) -> Result<u64, StorageError> {
        self.write_data(
            receiver,
            relative_path,
            expected_size,
            mode,
            replication,
            None,
        )
        .await
    }

    pub(crate) async fn write_file(
        &self,
        relative_path: &std::path::Path,
        data: bytes::Bytes,
        mode: u32,
        replication: Option<u32>,
    ) -> Result<(), StorageError> {
        let size = u64::try_from(data.len())
            .map_err(|_| config_error("HDFS write data length does not fit u64"))?;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        sender
            .send(crate::DataChunk { offset: 0, data })
            .await
            .map_err(|_| StorageError::OperationError("HDFS write channel closed".to_string()))?;
        drop(sender);
        self.write_data(receiver, relative_path, size, mode, replication, None)
            .await
            .map(|_| ())
    }

    /// Delete one regular file below the configured root.
    ///
    /// # Errors
    ///
    /// Returns an error for traversal, missing paths, directories, upstream
    /// failures, or an upstream `false` result.
    pub async fn delete_file(&self, relative_path: &std::path::Path) -> Result<(), StorageError> {
        let metadata = self.get_metadata(relative_path).await?;
        if metadata.is_dir {
            return Err(StorageError::InvalidPath(format!(
                "HDFS file delete target is a directory: {}",
                relative_path.display()
            )));
        }
        self.delete_resolved(relative_path, false).await
    }

    /// Recursively delete a directory while protecting the configured root.
    ///
    /// # Errors
    ///
    /// Returns an error for the root, traversal, missing paths, files, upstream
    /// failures, or an upstream `false` result.
    pub async fn delete_dir_all(
        &self,
        relative_path: &std::path::Path,
    ) -> Result<(), StorageError> {
        if relative_path.as_os_str().is_empty() {
            return Err(StorageError::InvalidPath(
                "refusing to delete the configured HDFS root".to_string(),
            ));
        }
        let metadata = self.get_metadata(relative_path).await?;
        if !metadata.is_dir {
            return Err(StorageError::InvalidPath(format!(
                "HDFS directory delete target is a file: {}",
                relative_path.display()
            )));
        }
        self.delete_resolved(relative_path, true).await
    }

    /// Explicit HDFS-specific storage-root cleanup for isolated lifecycle code.
    ///
    /// # Errors
    ///
    /// Returns an error when the root is missing or HDFS rejects deletion.
    pub async fn delete_storage_root(&self) -> Result<(), StorageError> {
        self.delete_resolved(std::path::Path::new(""), true).await
    }

    /// Atomically rename a file or directory below the configured root.
    ///
    /// Missing destination parents are created with mode `0o755`. Existing
    /// destinations are replaced using native HDFS overwrite semantics.
    ///
    /// # Errors
    ///
    /// Returns an error for either root, path escape, a missing source, a
    /// directory moved below itself, or an upstream rename failure.
    pub async fn rename(
        &self,
        from: &std::path::Path,
        to: &std::path::Path,
    ) -> Result<(), StorageError> {
        if from.as_os_str().is_empty() || to.as_os_str().is_empty() {
            return Err(StorageError::InvalidPath(
                "HDFS rename cannot use the configured storage root".to_string(),
            ));
        }
        let from_path = self.resolve_path(from)?;
        let to_path = self.resolve_path(to)?;
        let source = self.get_metadata(from).await?;
        if source.is_dir && to.starts_with(from) {
            return Err(StorageError::InvalidPath(format!(
                "HDFS cannot rename a directory into its own subtree: {} -> {}",
                from.display(),
                to.display()
            )));
        }
        if from == to {
            return Ok(());
        }
        if let Some(parent) = to.parent()
            && !parent.as_os_str().is_empty()
        {
            self.create_dir_all(parent, 0o755).await?;
        }
        self.client
            .rename(&from_path, &to_path, true)
            .await
            .map_err(|error| hdfs_operation_error("rename", Some(from), &error))
    }

    async fn delete_resolved(
        &self,
        relative_path: &std::path::Path,
        recursive: bool,
    ) -> Result<(), StorageError> {
        let path = self.resolve_path(relative_path)?;
        let deleted = self
            .client
            .delete(&path, recursive)
            .await
            .map_err(|error| hdfs_operation_error("delete", Some(relative_path), &error))?;
        deleted
            .then_some(())
            .ok_or_else(|| StorageError::FileNotFound(relative_path.display().to_string()))
    }

    async fn consume_sequential_chunks(
        &self,
        writer: &mut hdfs_native::file::FileWriter,
        receiver: &mut tokio::sync::mpsc::Receiver<crate::DataChunk>,
        context: SequentialWriteContext<'_>,
    ) -> Result<u64, StorageError> {
        let result = self
            .consume_sequential_chunk_data(writer, receiver, context)
            .await;
        if result.is_err() {
            receiver.close();
        }
        let close_result = Box::pin(writer.close()).await.map_err(|error| {
            hdfs_operation_error("close writer", Some(context.relative_path), &error)
        });
        match (result, close_result) {
            (Ok(offset), Ok(())) => Ok(offset),
            (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
            (Err(operation), Err(close)) => Err(StorageError::OperationError(format!(
                "HDFS write failed: {operation}; additionally failed to close writer: {close}"
            ))),
        }
    }

    async fn consume_sequential_chunk_data(
        &self,
        writer: &mut hdfs_native::file::FileWriter,
        receiver: &mut tokio::sync::mpsc::Receiver<crate::DataChunk>,
        context: SequentialWriteContext<'_>,
    ) -> Result<u64, StorageError> {
        let window = self.transfer_concurrency.write();
        let mut pending = BTreeMap::<u64, bytes::Bytes>::new();
        let mut next_offset = context.start_offset;
        while let Some(chunk) = receiver.recv().await {
            validate_write_chunk(&pending, next_offset, context.expected_size, &chunk)?;
            if !chunk.data.is_empty() {
                pending.insert(chunk.offset, chunk.data);
            }
            while let Some(data) = pending.remove(&next_offset) {
                let length = u64::try_from(data.len())
                    .map_err(|_| config_error("HDFS write chunk length does not fit u64"))?;
                let written = Box::pin(writer.write_bytes(data)).await.map_err(|error| {
                    hdfs_operation_error("write data", Some(context.relative_path), &error)
                })?;
                if u64::try_from(written).ok() != Some(length) {
                    return Err(StorageError::OperationError(format!(
                        "HDFS short write: expected {length} bytes, wrote {written}"
                    )));
                }
                next_offset = next_offset
                    .checked_add(length)
                    .ok_or_else(|| config_error("HDFS write offset overflow"))?;
                if let Some(counter) = context.bytes_counter {
                    counter.fetch_add(length, Ordering::Relaxed);
                }
                if let Some(callback) = context.on_committed {
                    callback(next_offset - length, length);
                }
            }
            if pending.len() > window {
                return Err(StorageError::OperationError(format!(
                    "HDFS write gap at offset {next_offset} exceeded reorder window {window}"
                )));
            }
        }
        validate_sequential_end(
            next_offset,
            context.expected_size,
            !pending.is_empty(),
            context.require_final_size,
        )?;
        Ok(next_offset)
    }

    async fn run_recursive_scan(
        self,
        root: PathBuf,
        max_depth: Option<usize>,
        concurrency: usize,
        sender: async_channel::Sender<HdfsScanEvent>,
    ) {
        let mut pending = VecDeque::from([(root, 0_usize)]);
        let mut active = futures::stream::FuturesUnordered::new();
        loop {
            while active.len() < concurrency {
                let Some((directory, depth)) = pending.pop_front() else {
                    break;
                };
                let storage = self.clone();
                active.push(async move {
                    let result = storage.list_directory(&directory).await;
                    (directory, depth, result)
                });
            }
            let Some((directory, depth, result)) = active.next().await else {
                break;
            };
            match result {
                Ok(entries) => {
                    for entry in entries {
                        let descend = entry.is_dir
                            && max_depth.is_none_or(|limit| depth.saturating_add(1) < limit);
                        let child_path = entry.relative_path.clone();
                        if sender.send(HdfsScanEvent::Entry(entry)).await.is_err() {
                            return;
                        }
                        if descend {
                            pending.push_back((child_path, depth.saturating_add(1)));
                        }
                    }
                }
                Err(error) => {
                    if sender
                        .send(HdfsScanEvent::Error {
                            path: directory,
                            error,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
    }
}

fn hdfs_entry_is_filtered(
    entry: &HDFSEntry,
    match_expression: Option<&crate::FilterExpression>,
    exclude_expression: Option<&crate::FilterExpression>,
) -> bool {
    hdfs_filter_decision(entry, match_expression, exclude_expression).0
}

fn plan_read_range(
    file_length: u64,
    offset: u64,
    count: u64,
) -> Result<Option<(usize, usize)>, StorageError> {
    if count == 0 || offset >= file_length {
        return Ok(None);
    }
    let length = count.min(file_length - offset);
    let offset = usize::try_from(offset)
        .map_err(|_| config_error("HDFS read offset exceeds platform capacity"))?;
    let length = usize::try_from(length)
        .map_err(|_| config_error("HDFS read length exceeds platform capacity"))?;
    Ok(Some((offset, length)))
}

fn validate_write_chunk(
    pending: &BTreeMap<u64, bytes::Bytes>,
    next_offset: u64,
    expected_size: u64,
    chunk: &crate::DataChunk,
) -> Result<(), StorageError> {
    let length = u64::try_from(chunk.data.len())
        .map_err(|_| config_error("HDFS write chunk length does not fit u64"))?;
    let end = chunk
        .offset
        .checked_add(length)
        .ok_or_else(|| config_error("HDFS write chunk range overflow"))?;
    if chunk.offset < next_offset || end > expected_size {
        return Err(StorageError::OperationError(format!(
            "invalid HDFS write range [{}, {end}) at committed offset {next_offset}",
            chunk.offset
        )));
    }
    if let Some((&previous_offset, previous)) = pending.range(..=chunk.offset).next_back() {
        let previous_end =
            previous_offset.saturating_add(u64::try_from(previous.len()).unwrap_or(u64::MAX));
        if previous_end > chunk.offset {
            return Err(StorageError::OperationError(
                "overlapping or duplicate HDFS write chunk".to_string(),
            ));
        }
    }
    if let Some((&following_offset, _)) = pending.range(chunk.offset..).next()
        && end > following_offset
    {
        return Err(StorageError::OperationError(
            "overlapping HDFS write chunk".to_string(),
        ));
    }
    Ok(())
}

fn validate_sequential_end(
    next_offset: u64,
    expected_size: u64,
    has_pending: bool,
    require_final_size: bool,
) -> Result<(), StorageError> {
    if has_pending || (require_final_size && next_offset != expected_size) {
        return Err(StorageError::OperationError(format!(
            "HDFS write ended at {next_offset} bytes, expected {expected_size}"
        )));
    }
    Ok(())
}

fn hdfs_filter_decision(
    entry: &HDFSEntry,
    match_expression: Option<&crate::FilterExpression>,
    exclude_expression: Option<&crate::FilterExpression>,
) -> (bool, bool, bool) {
    let path = entry.relative_path.to_string_lossy();
    let file_type = if entry.is_dir { "dir" } else { "file" };
    should_skip(
        match_expression,
        exclude_expression,
        FilterInput {
            file_name: Some(&entry.name),
            file_path: Some(&path),
            file_type: Some(file_type),
            modified_epoch: Some(entry.mtime / 1_000_000_000),
            size: Some(entry.size),
            extension: entry.extension.as_deref().or(Some("")),
        },
    )
}

fn hdfs_read_result(
    dir_path: &str,
    entries: Vec<HDFSEntry>,
    ctx: &crate::dir_tree::ReadContext,
) -> crate::dir_tree::ReadResult {
    let mut files = Vec::new();
    let mut subdirs = Vec::new();
    for entry in entries {
        let (skip, continue_scan, need_filter) = if ctx.apply_filter {
            hdfs_filter_decision(
                &entry,
                ctx.match_expr.as_ref().as_ref(),
                ctx.exclude_expr.as_ref().as_ref(),
            )
        } else {
            (false, true, false)
        };
        let can_descend = entry.is_dir
            && (ctx.max_depth == 0 || ctx.current_depth.saturating_add(1) < ctx.max_depth);
        let entry = Arc::new(crate::EntryEnum::HDFS(entry));
        if skip {
            if can_descend && continue_scan {
                subdirs.push(crate::dir_tree::SubdirEntry {
                    entry,
                    visible: false,
                    need_filter,
                });
            }
        } else if can_descend {
            subdirs.push(crate::dir_tree::SubdirEntry {
                entry,
                visible: true,
                need_filter,
            });
        } else {
            files.push(entry);
        }
    }
    files.sort_by(|left, right| left.get_name().cmp(right.get_name()));
    subdirs.sort_by(|left, right| left.entry.get_name().cmp(right.entry.get_name()));
    crate::dir_tree::ReadResult {
        dir_path: dir_path.to_string(),
        files,
        subdirs,
        errors: Vec::new(),
    }
}

fn strip_root<'a>(path: &'a str, root: &str) -> Result<&'a str, StorageError> {
    if root == "/" {
        return path
            .strip_prefix('/')
            .ok_or_else(|| config_error("HDFS status path must be absolute"));
    }
    if path == root {
        return Ok("");
    }
    path.strip_prefix(root)
        .and_then(|suffix| suffix.strip_prefix('/'))
        .ok_or_else(|| config_error("HDFS status path is outside the configured root"))
}

fn millis_to_nanos(value: u64) -> Result<i64, StorageError> {
    let nanos = value
        .checked_mul(1_000_000)
        .ok_or_else(|| config_error("HDFS timestamp overflows nanoseconds"))?;
    i64::try_from(nanos).map_err(|_| config_error("HDFS timestamp does not fit i64"))
}

fn nanos_to_millis(value: i64) -> Result<u64, StorageError> {
    let value = u64::try_from(value)
        .map_err(|_| config_error("HDFS timestamp cannot precede the Unix epoch"))?;
    Ok(value / 1_000_000)
}

/// Construct and validate an HDFS storage root.
///
/// # Errors
///
/// Returns an error when client construction, root lookup, or root creation
/// fails, or when the configured root is an existing file.
pub async fn create_hdfs_storage(
    location: &str,
    config: &HdfsConfig,
    block_size: Option<u64>,
    ensure_dir: bool,
) -> Result<HDFSStorage, StorageError> {
    let transfer_concurrency = crate::transfer_concurrency::resolve_transfer_concurrency(
        crate::transfer_concurrency::TransferBackend::Hdfs,
        crate::TransferConcurrency::defaults(4, 1),
        None,
    )?;
    let (client, location) = build_hdfs_client(location, config)?;
    match retry_hdfs_read("get storage root", None, None, || {
        client.get_file_info(location.root())
    })
    .await
    {
        Ok(status) if !status.isdir => {
            return Err(StorageError::InvalidPath(
                "configured HDFS root is not a directory".to_string(),
            ));
        }
        Ok(_) => {}
        Err(StorageError::FileNotFound(_)) if ensure_dir => client
            .mkdirs(location.root(), 0o755, true)
            .await
            .map_err(|error| hdfs_operation_error("create storage root", None, &error))?,
        Err(StorageError::FileNotFound(_)) => {
            return Err(StorageError::DirectoryNotFound(
                "<configured-root>".to_string(),
            ));
        }
        Err(error) => return Err(error),
    }
    Ok(HDFSStorage {
        client,
        location,
        block_size: block_size.unwrap_or(DEFAULT_BLOCK_SIZE),
        transfer_concurrency,
    })
}

fn safe_error_path(relative_path: Option<&Path>) -> String {
    relative_path.map_or_else(|| "<root>".to_string(), |path| path.display().to_string())
}

fn safe_hadoop_class(class: &str) -> Option<String> {
    (!class.is_empty()
        && class.len() <= 160
        && class
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'$')))
    .then(|| class.to_string())
}

fn class_has_suffix(class: &str, suffixes: &[&str]) -> bool {
    suffixes
        .iter()
        .any(|suffix| class == *suffix || class.ends_with(&format!(".{suffix}")))
}

fn structured_hdfs_error(
    operation: &'static str,
    relative_path: Option<&Path>,
    kind: crate::HdfsErrorKind,
    class: Option<&str>,
    diagnostic: &'static str,
    retryable: bool,
) -> StorageError {
    StorageError::HdfsOperation(crate::HdfsOperationError {
        operation,
        relative_path: relative_path.map(Path::to_path_buf),
        kind,
        hadoop_class: class.and_then(safe_hadoop_class),
        diagnostic,
        retryable,
    })
}

fn hdfs_rpc_error(
    operation: &'static str,
    relative_path: Option<&Path>,
    class: &str,
) -> StorageError {
    let safe_path = safe_error_path(relative_path);
    if class_has_suffix(class, &["FileNotFoundException", "PathNotFoundException"]) {
        StorageError::FileNotFound(safe_path)
    } else if class_has_suffix(class, &["AccessControlException"]) {
        StorageError::PermissionDenied(safe_path)
    } else if class_has_suffix(class, &["ChecksumException"]) {
        StorageError::ChecksumError(safe_path)
    } else if class_has_suffix(
        class,
        &[
            "DiskOutOfSpaceException",
            "DiskChecker$DiskOutOfSpaceException",
            "NSQuotaExceededException",
        ],
    ) {
        StorageError::InsufficientSpace(safe_path)
    } else {
        let kind = if class_has_suffix(
            class,
            &["FileAlreadyExistsException", "AlreadyBeingCreatedException"],
        ) {
            crate::HdfsErrorKind::AlreadyExists
        } else if class_has_suffix(class, &["UnsupportedOperationException"]) {
            crate::HdfsErrorKind::Unsupported
        } else {
            crate::HdfsErrorKind::Rpc
        };
        structured_hdfs_error(
            operation,
            relative_path,
            kind,
            Some(class),
            "upstream RPC failure",
            false,
        )
    }
}

fn hdfs_operation_error(
    operation: &'static str,
    relative_path: Option<&Path>,
    error: &hdfs_native::HdfsError,
) -> StorageError {
    use hdfs_native::HdfsError;
    let safe_path = safe_error_path(relative_path);
    match error {
        HdfsError::FileNotFound(_) => StorageError::FileNotFound(safe_path),
        HdfsError::InvalidPath(_) | HdfsError::InvalidArgument(_) => {
            StorageError::InvalidPath(safe_path)
        }
        HdfsError::ChecksumError => StorageError::ChecksumError(safe_path),
        HdfsError::IOError(error) if error.kind() == std::io::ErrorKind::NotFound => {
            StorageError::FileNotFound(safe_path)
        }
        HdfsError::IOError(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            StorageError::PermissionDenied(safe_path)
        }
        HdfsError::IOError(error) if error.kind() == std::io::ErrorKind::StorageFull => {
            StorageError::InsufficientSpace(safe_path)
        }
        HdfsError::RPCError(class, _) | HdfsError::FatalRPCError(class, _) => {
            hdfs_rpc_error(operation, relative_path, class)
        }
        HdfsError::AlreadyExists(_)
        | HdfsError::BlocksNotFound(_)
        | HdfsError::DataTransferError(_)
        | HdfsError::IsADirectoryError(_)
        | HdfsError::UnsupportedFeature(_)
        | HdfsError::UnsupportedErasureCodingPolicy(_)
        | HdfsError::TrashNotEnabled
        | HdfsError::ErasureCodingError(_)
        | HdfsError::SASLError(_)
        | HdfsError::GSSAPIError(_, _, _)
        | HdfsError::NoSASLMechanism
        | HdfsError::IOError(_)
        | HdfsError::OperationFailed(_)
        | HdfsError::InternalError(_)
        | HdfsError::InvalidRPCResponse(_)
        | HdfsError::UrlParseError(_)
        | HdfsError::XmlParseError(_) => {
            let (kind, diagnostic, retryable) = hdfs_structured_attributes(error);
            structured_hdfs_error(operation, relative_path, kind, None, diagnostic, retryable)
        }
    }
}

fn hdfs_structured_attributes(
    error: &hdfs_native::HdfsError,
) -> (crate::HdfsErrorKind, &'static str, bool) {
    use hdfs_native::HdfsError;
    match error {
        HdfsError::AlreadyExists(_) => (
            crate::HdfsErrorKind::AlreadyExists,
            "destination already exists",
            false,
        ),
        HdfsError::BlocksNotFound(_) => (
            crate::HdfsErrorKind::BlocksMissing,
            "file blocks are unavailable",
            true,
        ),
        HdfsError::DataTransferError(_) => (
            crate::HdfsErrorKind::DataTransfer,
            "data transfer failed",
            true,
        ),
        HdfsError::IsADirectoryError(_) => (
            crate::HdfsErrorKind::Directory,
            "target is a directory",
            false,
        ),
        HdfsError::UnsupportedFeature(_)
        | HdfsError::UnsupportedErasureCodingPolicy(_)
        | HdfsError::TrashNotEnabled => (
            crate::HdfsErrorKind::Unsupported,
            "operation is unsupported",
            false,
        ),
        HdfsError::ErasureCodingError(_) => (
            crate::HdfsErrorKind::ErasureCoding,
            "erasure coding operation failed",
            false,
        ),
        HdfsError::SASLError(_) | HdfsError::GSSAPIError(_, _, _) | HdfsError::NoSASLMechanism => (
            crate::HdfsErrorKind::Authentication,
            "authentication failed",
            false,
        ),
        HdfsError::IOError(error) => (
            crate::HdfsErrorKind::Io,
            "I/O operation failed",
            is_retryable_io(error.kind()),
        ),
        HdfsError::OperationFailed(_)
        | HdfsError::InternalError(_)
        | HdfsError::InvalidRPCResponse(_)
        | HdfsError::UrlParseError(_)
        | HdfsError::XmlParseError(_)
        | HdfsError::InvalidPath(_)
        | HdfsError::InvalidArgument(_)
        | HdfsError::ChecksumError
        | HdfsError::FileNotFound(_)
        | HdfsError::RPCError(_, _)
        | HdfsError::FatalRPCError(_, _) => (
            crate::HdfsErrorKind::Internal,
            "internal upstream operation failed",
            false,
        ),
    }
}

fn is_retryable_io(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::NotConnected
            | std::io::ErrorKind::UnexpectedEof
    )
}

#[derive(Deserialize)]
struct HadoopConfiguration {
    #[serde(rename = "property", default)]
    properties: Vec<HadoopProperty>,
}

#[derive(Deserialize)]
struct HadoopProperty {
    name: String,
    value: String,
}

fn validate_authentication(
    settings: &HashMap<String, String>,
    credentials: Option<&HdfsKerberosCredentials>,
) -> Result<(), StorageError> {
    let mode = settings.get("hadoop.security.authentication").map_or_else(
        || {
            if credentials.is_some() {
                "kerberos"
            } else {
                "simple"
            }
        },
        |mode| mode.trim(),
    );
    if mode.eq_ignore_ascii_case("simple") {
        if credentials.is_some() {
            return Err(config_error(
                "HDFS Kerberos credentials require kerberos authentication",
            ));
        }
    } else if mode.eq_ignore_ascii_case("kerberos") {
        if credentials.is_none() {
            return Err(config_error(
                "Kerberos HDFS authentication requires client-scoped credentials",
            ));
        }
    } else {
        return Err(config_error("unsupported HDFS authentication mode"));
    }
    Ok(())
}

fn format_nameservice_endpoint(
    host: &Host<&str>,
    settings: &HashMap<String, String>,
) -> Result<String, StorageError> {
    let Host::Domain(service) = host else {
        return Err(config_error("direct HDFS IP locations require an RPC port"));
    };
    let membership_key = format!("dfs.ha.namenodes.{service}");
    let members = settings
        .get(&membership_key)
        .ok_or_else(|| config_error("HDFS NameService requires explicit HA membership"))?;
    let members = members
        .split(',')
        .map(str::trim)
        .filter(|member| !member.is_empty())
        .collect::<Vec<_>>();
    if members.is_empty() {
        return Err(config_error(
            "HDFS NameService membership must not be empty",
        ));
    }
    for member in members {
        let address_key = format!("dfs.namenode.rpc-address.{service}.{member}");
        if settings
            .get(&address_key)
            .is_none_or(|address| address.trim().is_empty())
        {
            return Err(config_error(
                "every HDFS NameService member requires an RPC address",
            ));
        }
    }
    Ok(format!("hdfs://{service}"))
}

fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    ["password", "secret", "token", "credential", "keytab"]
        .iter()
        .any(|marker| key.contains(marker))
}

fn reject_raw_traversal(location: &str) -> Result<(), StorageError> {
    let Some(authority_start) = location.find("://").map(|index| index + 3) else {
        return Ok(());
    };
    let Some(path_offset) = location[authority_start..].find('/') else {
        return Ok(());
    };
    let raw_path = &location[authority_start + path_offset..];
    let raw_path = raw_path
        .split(['?', '#'])
        .next()
        .ok_or_else(|| config_error("HDFS location contains an invalid path"))?;
    let decoded = decode_component(raw_path, "root path")?;
    if decoded.split('/').any(|component| component == "..") {
        return Err(config_error("HDFS root must not contain traversal"));
    }
    Ok(())
}

fn config_error(message: &str) -> StorageError {
    StorageError::ConfigError(message.to_string())
}

fn validate_percent_encoding(value: &str) -> Result<(), StorageError> {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && (index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit())
        {
            return Err(config_error(
                "HDFS location contains malformed percent encoding",
            ));
        }
        index += if bytes[index] == b'%' { 3 } else { 1 };
    }
    Ok(())
}

fn decode_component(value: &str, label: &str) -> Result<String, StorageError> {
    percent_decode_str(value)
        .decode_utf8()
        .map(String::from)
        .map_err(|_| config_error(&format!("HDFS {label} is not valid UTF-8")))
}

fn format_endpoint(host: &Host<&str>, port: u16) -> String {
    match host {
        Host::Domain(domain) => format!("hdfs://{domain}:{port}"),
        Host::Ipv4(address) => format!("hdfs://{address}:{port}"),
        Host::Ipv6(address) => format!("hdfs://[{address}]:{port}"),
    }
}

fn normalize_root(encoded_path: &str) -> Result<String, StorageError> {
    let decoded = decode_component(encoded_path, "root path")?;
    if !decoded.starts_with('/') {
        return Err(config_error("HDFS root must be absolute"));
    }

    let mut components = Vec::new();
    for component in decoded.split('/') {
        match component {
            "" | "." => {}
            ".." => return Err(config_error("HDFS root must not contain traversal")),
            value => components.push(value),
        }
    }

    if components.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", components.join("/")))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;
    use std::process::Command;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio_util::sync::CancellationToken;

    use super::{
        HdfsConfig, HdfsEndpointKind, HdfsKerberosCredentials, HdfsLocation, build_hdfs_client,
    };
    use crate::error::StorageError;

    #[test]
    fn transfer_chunks_are_positive_and_do_not_change_hdfs_block_size() {
        assert_eq!(super::transfer_chunk_size(0), 1);
        assert_eq!(super::transfer_chunk_size(crate::MB), crate::MB);
        assert_eq!(
            super::transfer_chunk_size(super::DEFAULT_BLOCK_SIZE),
            super::MAX_TRANSFER_CHUNK_SIZE
        );
        assert_eq!(super::DEFAULT_BLOCK_SIZE, 8 * crate::MB);
    }

    #[tokio::test]
    async fn adapter_retry_is_bounded_and_eventually_succeeds() {
        assert_eq!(super::HDFS_ADAPTER_MAX_ATTEMPTS, 5);
        assert_eq!(
            super::HDFS_ADAPTER_ATTEMPT_TIMEOUT,
            std::time::Duration::from_secs(5)
        );
        assert_eq!(
            super::HDFS_ADAPTER_RETRY_DELAYS
                .iter()
                .sum::<std::time::Duration>(),
            std::time::Duration::from_millis(3_750)
        );
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let started = std::time::Instant::now();
        let result =
            super::retry_hdfs_read("test read", Some(Path::new("item")), None, move || {
                let attempt = observed.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt < 2 {
                        Err(hdfs_native::HdfsError::IOError(std::io::Error::from(
                            std::io::ErrorKind::TimedOut,
                        )))
                    } else {
                        Ok(7_u8)
                    }
                }
            })
            .await;
        assert_eq!(result.ok(), Some(7));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert!(started.elapsed() >= std::time::Duration::from_millis(750));
    }

    #[tokio::test]
    async fn adapter_retry_short_circuits_permanent_errors() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let result = super::retry_hdfs_read::<(), _, _>(
            "test read",
            Some(Path::new("missing")),
            None,
            move || {
                observed.fetch_add(1, Ordering::SeqCst);
                async {
                    Err(hdfs_native::HdfsError::FileNotFound(
                        "/absolute/missing".to_string(),
                    ))
                }
            },
        )
        .await;
        assert!(matches!(result, Err(StorageError::FileNotFound(path)) if path == "missing"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn adapter_retry_exhaustion_preserves_structured_error() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let result = super::retry_hdfs_read::<(), _, _>(
            "open file",
            Some(Path::new("item")),
            None,
            move || {
                observed.fetch_add(1, Ordering::SeqCst);
                async {
                    Err(hdfs_native::HdfsError::DataTransferError(
                        "ignored".to_string(),
                    ))
                }
            },
        )
        .await;
        assert!(matches!(
            result,
            Err(StorageError::HdfsOperation(crate::HdfsOperationError {
                operation: "open file",
                relative_path: Some(path),
                retryable: true,
                ..
            })) if path == Path::new("item")
        ));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            super::HDFS_ADAPTER_MAX_ATTEMPTS
        );
    }

    #[tokio::test]
    async fn adapter_retry_cancellation_stops_wait_and_inflight_attempt() {
        let token = CancellationToken::new();
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let cancel = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            cancel.cancel();
        });
        let result =
            super::retry_hdfs_read::<(), _, _>("test read", None, Some(&token), move || {
                observed.fetch_add(1, Ordering::SeqCst);
                async {
                    Err(hdfs_native::HdfsError::DataTransferError(
                        "ignored".to_string(),
                    ))
                }
            })
            .await;
        assert!(matches!(result, Err(StorageError::Cancelled)));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        let token = CancellationToken::new();
        let cancel = token.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            cancel.cancel();
        });
        let result =
            super::retry_hdfs_read::<(), _, _>("test read", None, Some(&token), || async {
                std::future::pending::<Result<(), hdfs_native::HdfsError>>().await
            })
            .await;
        assert!(matches!(result, Err(StorageError::Cancelled)));
    }

    #[test]
    fn parses_and_normalizes_direct_locations() {
        let cases = [
            (
                "hdfs://alice@namenode.example:9000/data//./incoming/",
                "hdfs://namenode.example:9000",
                "alice",
                "/data/incoming",
            ),
            (
                "hdfs://data%20mover@10.131.9.30:9000/%E6%95%B0%E6%8D%AE",
                "hdfs://10.131.9.30:9000",
                "data mover",
                "/数据",
            ),
            (
                "hdfs://alice@[2001:db8::1]:8020/",
                "hdfs://[2001:db8::1]:8020",
                "alice",
                "/",
            ),
        ];

        for (input, endpoint, user, root) in cases {
            let Ok(parsed) = HdfsLocation::parse(input) else {
                panic!("valid direct HDFS URL was rejected: {input}");
            };
            assert_eq!(parsed.endpoint(), endpoint);
            assert_eq!(parsed.user(), user);
            assert_eq!(parsed.root(), root);
        }
    }

    #[test]
    fn rejects_unsupported_or_ambiguous_locations() {
        let invalid = [
            "viewfs://alice@namenode:9000/",
            "hdfs://namenode:9000/",
            "hdfs://@namenode:9000/",
            "hdfs://alice:secret@namenode:9000/",
            "hdfs://alice@namenode/",
            "hdfs://alice@:9000/",
            "hdfs://alice@namenode:9000/root?x=1",
            "hdfs://alice@namenode:9000/root#fragment",
            "hdfs://alice@namenode:9000/root/%2e%2e/outside",
            "hdfs://alice@namenode:9000/root/%ZZ",
            "hdfs://%FF@namenode:9000/",
        ];

        for input in invalid {
            assert!(HdfsLocation::parse(input).is_err(), "accepted {input}");
        }
    }

    #[test]
    fn errors_do_not_echo_rejected_userinfo() {
        let input = "hdfs://alice:do-not-log@namenode:9000/root";
        let Err(error) = HdfsLocation::parse(input) else {
            panic!("password was accepted");
        };
        assert!(!error.to_string().contains("do-not-log"));
    }

    #[test]
    fn storage_root_resolution_cannot_escape() {
        let Ok((client, location)) = build_hdfs_client(
            "hdfs://user@127.0.0.1:9000/isolated/root",
            &HdfsConfig::default(),
        ) else {
            panic!("explicit client construction failed");
        };
        let storage = super::HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::TransferConcurrency::defaults(4, 1),
        };
        let Ok(resolved) = storage.resolve_path(Path::new("a/./b")) else {
            panic!("valid relative path was rejected");
        };
        assert_eq!(resolved, "/isolated/root/a/b");
        assert!(storage.resolve_path(Path::new("../outside")).is_err());
        assert!(storage.resolve_path(Path::new("/outside")).is_err());
    }

    #[test]
    fn status_conversion_preserves_hdfs_metadata_and_common_accessors() {
        let Ok((client, location)) = build_hdfs_client(
            "hdfs://user@127.0.0.1:9000/isolated/root",
            &HdfsConfig::default(),
        ) else {
            panic!("explicit client construction failed");
        };
        let storage = super::HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::TransferConcurrency::defaults(4, 1),
        };
        let status = hdfs_native::client::FileStatus {
            path: "/isolated/root/目录/report.txt".to_string(),
            length: 42,
            isdir: false,
            permission: 0o640,
            owner: "alice".to_string(),
            group: "analytics".to_string(),
            modification_time: 1_700_000_000_123,
            access_time: 1_700_000_001_456,
            replication: Some(3),
            blocksize: Some(128 * crate::MB),
        };
        let Ok(entry) = storage.entry_from_status(status) else {
            panic!("valid status conversion failed");
        };
        assert_eq!(entry.relative_path, Path::new("目录/report.txt"));
        assert_eq!(entry.name, "report.txt");
        assert_eq!(entry.extension.as_deref(), Some("txt"));
        assert_eq!(entry.owner, "alice");
        assert_eq!(entry.group, "analytics");
        assert_eq!(entry.replication, Some(3));
        assert_eq!(entry.block_size, Some(128 * crate::MB));
        assert_eq!(entry.mtime, 1_700_000_000_123_000_000);
        assert_eq!(entry.atime, 1_700_000_001_456_000_000);

        let common = crate::EntryEnum::HDFS(entry);
        assert_eq!(common.get_name(), "report.txt");
        assert_eq!(common.get_relative_path(), Path::new("目录/report.txt"));
        assert_eq!(common.get_size(), 42);
        assert!(!common.get_is_dir());
        assert!(common.get_is_regular_file());
        assert!(!common.get_is_symlink());
        assert_eq!(common.get_mode(), Some(0o640));
        assert_eq!(common.get_uid(), None);
        assert_eq!(common.get_gid(), None);
    }

    #[test]
    fn status_conversion_enforces_root_component_boundary() {
        let Ok((client, location)) =
            build_hdfs_client("hdfs://user@127.0.0.1:9000/root", &HdfsConfig::default())
        else {
            panic!("explicit client construction failed");
        };
        let storage = super::HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::TransferConcurrency::defaults(4, 1),
        };
        let status = hdfs_native::client::FileStatus {
            path: "/rooted/file".to_string(),
            length: 0,
            isdir: true,
            permission: 0o755,
            owner: String::new(),
            group: String::new(),
            modification_time: 0,
            access_time: 0,
            replication: None,
            blocksize: None,
        };
        assert!(storage.entry_from_status(status).is_err());
    }

    #[test]
    fn missing_upstream_path_keeps_not_found_classification() {
        let error = super::hdfs_operation_error(
            "get metadata",
            Some(Path::new("missing")),
            &hdfs_native::HdfsError::FileNotFound("/isolated/root/missing".to_string()),
        );
        assert!(matches!(
            error,
            crate::error::StorageError::FileNotFound(path)
                if path == "missing"
        ));
    }

    #[test]
    fn rpc_classes_map_without_inspecting_messages() {
        for (class, expected) in [
            ("org.apache.hadoop.fs.FileNotFoundException", "not-found"),
            (
                "org.apache.hadoop.security.AccessControlException",
                "permission",
            ),
            ("org.apache.hadoop.fs.ChecksumException", "checksum"),
            (
                "org.apache.hadoop.util.DiskChecker$DiskOutOfSpaceException",
                "space",
            ),
        ] {
            let error = super::hdfs_operation_error(
                "open file",
                Some(Path::new("safe.bin")),
                &hdfs_native::HdfsError::RPCError(
                    class.to_string(),
                    "secret message with hdfs://user:password@host/absolute".to_string(),
                ),
            );
            assert!(
                match expected {
                    "not-found" => matches!(error, StorageError::FileNotFound(_)),
                    "permission" => matches!(error, StorageError::PermissionDenied(_)),
                    "checksum" => matches!(error, StorageError::ChecksumError(_)),
                    "space" => matches!(error, StorageError::InsufficientSpace(_)),
                    _ => false,
                },
                "class {class} mapped to {error:?}"
            );
            let displayed = error.to_string();
            assert!(!displayed.contains("password"));
            assert!(!displayed.contains("/absolute"));
        }
    }

    #[test]
    fn unknown_rpc_error_is_structured_conservative_and_redacted() {
        let error = super::hdfs_operation_error(
            "rename",
            Some(Path::new("relative/source")),
            &hdfs_native::HdfsError::RPCError(
                "vendor.CustomFailure".to_string(),
                "token=secret /outside/root".to_string(),
            ),
        );
        let StorageError::HdfsOperation(details) = &error else {
            panic!("unknown RPC error lost structured HDFS context");
        };
        assert_eq!(details.operation, "rename");
        assert_eq!(
            details.relative_path.as_deref(),
            Some(Path::new("relative/source"))
        );
        assert_eq!(details.kind, crate::HdfsErrorKind::Rpc);
        assert_eq!(
            details.hadoop_class.as_deref(),
            Some("vendor.CustomFailure")
        );
        assert!(!details.retryable);
        let displayed = error.to_string();
        assert!(displayed.contains("relative/source"));
        assert!(!displayed.contains("secret"));
        assert!(!displayed.contains("/outside/root"));
    }

    #[test]
    fn malformed_rpc_class_is_not_reflected_in_diagnostics() {
        let error = super::hdfs_operation_error(
            "list directory",
            None,
            &hdfs_native::HdfsError::FatalRPCError(
                "bad\nclass hdfs://user:secret@host".to_string(),
                "ignored".to_string(),
            ),
        );
        let StorageError::HdfsOperation(details) = error else {
            panic!("fatal RPC error lost structured HDFS context");
        };
        assert!(details.hadoop_class.is_none());
        assert!(!details.retryable);
    }

    #[test]
    fn transient_io_and_data_transfer_errors_are_the_only_retryable_samples() {
        let io = super::hdfs_operation_error(
            "read range",
            Some(Path::new("file")),
            &hdfs_native::HdfsError::IOError(std::io::Error::from(std::io::ErrorKind::TimedOut)),
        );
        let transfer = super::hdfs_operation_error(
            "write data",
            Some(Path::new("file")),
            &hdfs_native::HdfsError::DataTransferError("secret".to_string()),
        );
        for error in [io, transfer] {
            assert!(matches!(
                &error,
                StorageError::HdfsOperation(crate::HdfsOperationError {
                    retryable: true,
                    ..
                })
            ));
            assert!(!error.to_string().contains("secret"));
        }
    }

    #[test]
    fn single_directory_listing_conversion_is_bounded_and_root_relative() {
        let Ok((client, location)) =
            build_hdfs_client("hdfs://user@127.0.0.1:9000/root", &HdfsConfig::default())
        else {
            panic!("explicit client construction failed");
        };
        let storage = super::HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::TransferConcurrency::defaults(4, 1),
        };
        assert!(
            storage
                .entries_from_listing(Vec::new(), Path::new("目录"))
                .is_ok_and(|entries| entries.is_empty())
        );

        let status = |path: &str, isdir| hdfs_native::client::FileStatus {
            path: path.to_string(),
            length: 0,
            isdir,
            permission: 0o755,
            owner: "user".to_string(),
            group: "group".to_string(),
            modification_time: 0,
            access_time: 0,
            replication: None,
            blocksize: None,
        };
        let Ok(entries) = storage.entries_from_listing(
            vec![
                status("/root/目录/file.txt", false),
                status("/root/目录/subdir", true),
            ],
            Path::new("目录"),
        ) else {
            panic!("valid immediate children were rejected");
        };
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].relative_path, Path::new("目录/file.txt"));
        assert_eq!(entries[1].relative_path, Path::new("目录/subdir"));
        assert!(entries[1].is_dir);

        assert!(
            storage
                .entries_from_listing(
                    vec![status("/root/目录/subdir/nested", false)],
                    Path::new("目录"),
                )
                .is_err()
        );
        assert!(
            storage
                .entries_from_listing(vec![status("/rooted/outside", false)], Path::new("目录"),)
                .is_err()
        );
    }

    #[test]
    fn recursive_scanner_validates_concurrency_and_root() {
        let Ok((client, location)) =
            build_hdfs_client("hdfs://user@127.0.0.1:9000/root", &HdfsConfig::default())
        else {
            panic!("explicit client construction failed");
        };
        let storage = super::HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::TransferConcurrency::defaults(4, 1),
        };
        assert!(storage.scan_recursive(None, None, 0).is_err());
        assert!(
            storage
                .scan_recursive(None, None, crate::TransferConcurrency::MAX + 1)
                .is_err()
        );
        assert!(
            storage
                .scan_recursive(Some(Path::new("../outside")), None, 1)
                .is_err()
        );
        assert!(
            storage
                .walkdir(
                    None,
                    crate::storage_enum::WalkOptions {
                        packaged: true,
                        ..Default::default()
                    },
                )
                .is_err()
        );
    }

    #[test]
    fn hdfs_filter_adapter_uses_truthful_entry_fields() {
        let entry = crate::HDFSEntry {
            name: "报告.txt".to_string(),
            relative_path: std::path::PathBuf::from("目录/报告.txt"),
            is_dir: false,
            size: 42,
            mtime: 1_700_000_000_000_000_000,
            atime: 0,
            mode: 0o640,
            owner: "alice".to_string(),
            group: "users".to_string(),
            replication: Some(3),
            block_size: Some(128 * crate::MB),
            extension: Some("txt".to_string()),
        };
        let Ok(include) = crate::filter::parse_filter_expression("extension == \"txt\"") else {
            panic!("valid include filter was rejected");
        };
        let Ok(exclude) = crate::filter::parse_filter_expression("path == \"目录/**\"") else {
            panic!("valid exclude filter was rejected");
        };
        assert!(!super::hdfs_entry_is_filtered(&entry, Some(&include), None));
        assert!(super::hdfs_entry_is_filtered(
            &entry,
            Some(&include),
            Some(&exclude)
        ));
    }

    #[test]
    fn positional_read_planning_truncates_at_eof_without_overflow() {
        assert_eq!(super::plan_read_range(10, 0, 0).ok(), Some(None));
        assert_eq!(super::plan_read_range(10, 10, 4).ok(), Some(None));
        assert_eq!(super::plan_read_range(10, 12, u64::MAX).ok(), Some(None));
        assert_eq!(super::plan_read_range(10, 2, 4).ok(), Some(Some((2, 4))));
        assert_eq!(
            super::plan_read_range(10, 8, u64::MAX).ok(),
            Some(Some((8, 2)))
        );
    }

    #[test]
    fn sequential_writer_rejects_overlap_duplicate_and_oversize_chunks() {
        let mut pending = std::collections::BTreeMap::new();
        pending.insert(4, bytes::Bytes::from_static(b"efgh"));
        let chunk = |offset, data| crate::DataChunk {
            offset,
            data: bytes::Bytes::from_static(data),
        };
        assert!(super::validate_write_chunk(&pending, 0, 12, &chunk(0, b"abcd")).is_ok());
        assert!(super::validate_write_chunk(&pending, 0, 12, &chunk(4, b"efgh")).is_err());
        assert!(super::validate_write_chunk(&pending, 0, 12, &chunk(2, b"cdef")).is_err());
        assert!(super::validate_write_chunk(&pending, 0, 12, &chunk(7, b"hijk")).is_err());
        assert!(super::validate_write_chunk(&pending, 4, 12, &chunk(0, b"abcd")).is_err());
        assert!(super::validate_write_chunk(&pending, 0, 12, &chunk(10, b"wxyz")).is_err());
    }

    #[test]
    fn resumable_append_may_stop_at_a_durable_contiguous_prefix() {
        assert!(super::validate_sequential_end(8, 16, false, false).is_ok());
        assert!(super::validate_sequential_end(16, 16, false, true).is_ok());
        assert!(super::validate_sequential_end(8, 16, false, true).is_err());
        assert!(super::validate_sequential_end(8, 16, true, false).is_err());
    }

    #[tokio::test]
    async fn delete_primitives_reject_escape_and_protect_root_before_io() {
        let Ok((client, location)) =
            build_hdfs_client("hdfs://user@127.0.0.1:9000/root", &HdfsConfig::default())
        else {
            panic!("explicit client construction failed");
        };
        let storage = super::HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::TransferConcurrency::defaults(4, 1),
        };
        assert!(storage.delete_dir_all(Path::new("")).await.is_err());
        assert!(storage.delete_file(Path::new("../outside")).await.is_err());
        assert!(storage.delete_dir_all(Path::new("/outside")).await.is_err());
    }

    #[tokio::test]
    async fn metadata_primitives_reject_root_escape_and_invalid_values_before_io() {
        let Ok((client, location)) =
            build_hdfs_client("hdfs://user@127.0.0.1:9000/root", &HdfsConfig::default())
        else {
            panic!("explicit client construction failed");
        };
        let storage = super::HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::TransferConcurrency::defaults(4, 1),
        };

        assert!(storage.set_permission(Path::new(""), 0o640).await.is_err());
        assert!(
            storage
                .set_permission(Path::new("../outside"), 0o640)
                .await
                .is_err()
        );
        assert!(
            storage
                .set_permission(Path::new("file"), 0o10_000)
                .await
                .is_err()
        );
        assert!(storage.set_mtime(Path::new("file"), -1).await.is_err());
        assert!(
            storage
                .set_owner_group(Path::new("/outside"), Some("alice"), Some("users"))
                .await
                .is_err()
        );
        assert!(
            storage
                .set_owner_group(Path::new(""), None, None)
                .await
                .is_err()
        );
        assert!(
            storage
                .set_owner_group(Path::new("missing"), Some(""), None)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn rename_rejects_roots_and_escape_before_io() {
        let Ok((client, location)) =
            build_hdfs_client("hdfs://user@127.0.0.1:9000/root", &HdfsConfig::default())
        else {
            panic!("explicit client construction failed");
        };
        let storage = super::HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::TransferConcurrency::defaults(4, 1),
        };
        assert!(
            storage
                .rename(Path::new(""), Path::new("to"))
                .await
                .is_err()
        );
        assert!(
            storage
                .rename(Path::new("from"), Path::new(""))
                .await
                .is_err()
        );
        assert!(
            storage
                .rename(Path::new("../from"), Path::new("to"))
                .await
                .is_err()
        );
        assert!(
            storage
                .rename(Path::new("from"), Path::new("/to"))
                .await
                .is_err()
        );
    }

    #[test]
    fn walkdir2_reader_result_is_sorted_and_preserves_hidden_descent() {
        let entry = |path: &str, is_dir: bool| crate::HDFSEntry {
            name: Path::new(path)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .to_string(),
            relative_path: std::path::PathBuf::from(path),
            is_dir,
            size: 0,
            mtime: 0,
            atime: 0,
            mode: 0o755,
            owner: String::new(),
            group: String::new(),
            replication: None,
            block_size: None,
            extension: Path::new(path)
                .extension()
                .and_then(|value| value.to_str())
                .map(str::to_string),
        };
        let Ok(include) = crate::filter::parse_filter_expression("name == \"*.txt\"") else {
            panic!("valid filter rejected");
        };
        let ctx = crate::dir_tree::ReadContext {
            match_expr: std::sync::Arc::new(Some(include)),
            exclude_expr: std::sync::Arc::new(None),
            current_depth: 0,
            max_depth: 3,
            apply_filter: true,
            include_tags: false,
            is_versioned: false,
        };
        let result = super::hdfs_read_result(
            "",
            vec![
                entry("z.txt", false),
                entry("keep/乙", true),
                entry("keep/甲", true),
                entry("a.txt", false),
            ],
            &ctx,
        );
        assert_eq!(result.files.len(), 2);
        assert_eq!(result.files[0].get_name(), "a.txt");
        assert_eq!(result.files[1].get_name(), "z.txt");
        assert_eq!(result.subdirs.len(), 2);
        assert_eq!(result.subdirs[0].entry.get_name(), "乙");
        assert_eq!(result.subdirs[1].entry.get_name(), "甲");
        assert!(result.subdirs.iter().all(|subdir| !subdir.visible));
        assert!(result.subdirs.iter().all(|subdir| subdir.need_filter));

        let boundary = crate::dir_tree::ReadContext {
            current_depth: 2,
            ..ctx
        };
        let result = super::hdfs_read_result("keep", vec![entry("keep/子目录", true)], &boundary);
        assert!(result.subdirs.is_empty());
    }

    #[tokio::test]
    async fn walkdir2_reader_rejects_non_hdfs_handle_before_io() {
        let Ok((client, location)) =
            build_hdfs_client("hdfs://user@127.0.0.1:9000/root", &HdfsConfig::default())
        else {
            panic!("explicit client construction failed");
        };
        let storage = super::HDFSStorage {
            client,
            location,
            block_size: super::DEFAULT_BLOCK_SIZE,
            transfer_concurrency: crate::TransferConcurrency::defaults(4, 1),
        };
        let ctx = crate::dir_tree::ReadContext {
            match_expr: std::sync::Arc::new(None),
            exclude_expr: std::sync::Arc::new(None),
            current_depth: 0,
            max_depth: 0,
            apply_filter: true,
            include_tags: false,
            is_versioned: false,
        };
        let result = storage
            .read_dir_sorted(
                "",
                &crate::dir_tree::DirHandle::Local(std::path::PathBuf::from("/")),
                &ctx,
            )
            .await;
        assert!(matches!(
            result,
            Err(crate::error::StorageError::MismatchedType)
        ));
        assert!(storage.walkdir_2(None, None, None, None, 0).is_err());
        assert!(
            storage
                .walkdir_2(Some(Path::new("../outside")), None, None, None, 1,)
                .is_err()
        );
        assert!(
            storage
                .create_dir_all(Path::new("../outside"), 0o755)
                .await
                .is_err()
        );
    }

    #[test]
    fn parses_nameservice_from_explicit_complete_overrides() {
        let config = HdfsConfig {
            config_dir: None,
            overrides: HashMap::from([
                (
                    "dfs.ha.namenodes.analytics".to_string(),
                    " nn1, nn2 ".to_string(),
                ),
                (
                    "dfs.namenode.rpc-address.analytics.nn1".to_string(),
                    "10.0.0.1:8020".to_string(),
                ),
                (
                    "dfs.namenode.rpc-address.analytics.nn2".to_string(),
                    "10.0.0.2:8020".to_string(),
                ),
                (
                    "hadoop.security.authentication".to_string(),
                    "simple".to_string(),
                ),
            ]),
            ..Default::default()
        };
        let Ok(location) =
            HdfsLocation::parse_configured("hdfs://migration@analytics/warehouse", &config)
        else {
            panic!("complete explicit NameService configuration was rejected");
        };
        assert_eq!(location.endpoint(), "hdfs://analytics");
        assert_eq!(location.kind(), HdfsEndpointKind::NameService);
        assert_eq!(location.user(), "migration");
        assert_eq!(location.root(), "/warehouse");
    }

    #[test]
    fn rejects_incomplete_nameservice_and_non_simple_authentication() {
        let incomplete = HdfsConfig {
            config_dir: None,
            overrides: HashMap::from([(
                "dfs.ha.namenodes.analytics".to_string(),
                "nn1,nn2".to_string(),
            )]),
            ..Default::default()
        };
        assert!(HdfsLocation::parse_configured("hdfs://user@analytics/", &incomplete).is_err());

        let kerberos = HdfsConfig {
            config_dir: None,
            overrides: HashMap::from([(
                "hadoop.security.authentication".to_string(),
                "kerberos".to_string(),
            )]),
            ..Default::default()
        };
        assert!(HdfsLocation::parse_configured("hdfs://user@namenode:9000/", &kerberos).is_err());
    }

    #[test]
    fn overrides_take_precedence_over_explicit_xml() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());
        let directory = std::env::temp_dir().join(format!("data-mover-hdfs-config-{suffix}"));
        let Ok(()) = fs::create_dir(&directory) else {
            panic!("failed to create test configuration directory");
        };
        let xml = r"<configuration>
          <property><name>dfs.ha.namenodes.analytics</name><value>old</value></property>
          <property><name>dfs.namenode.rpc-address.analytics.old</name><value>old:8020</value></property>
        </configuration>";
        let Ok(()) = fs::write(directory.join("hdfs-site.xml"), xml) else {
            panic!("failed to write test configuration");
        };
        let config = HdfsConfig {
            config_dir: Some(directory.clone()),
            overrides: HashMap::from([
                ("dfs.ha.namenodes.analytics".to_string(), "new".to_string()),
                (
                    "dfs.namenode.rpc-address.analytics.new".to_string(),
                    "new:8020".to_string(),
                ),
            ]),
            ..Default::default()
        };
        assert!(HdfsLocation::parse_configured("hdfs://user@analytics/", &config).is_ok());
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn debug_redacts_sensitive_override_values() {
        let config = HdfsConfig {
            config_dir: None,
            overrides: HashMap::from([
                ("service.token".to_string(), "do-not-log".to_string()),
                ("safe.setting".to_string(), "visible".to_string()),
            ]),
            ..Default::default()
        };
        let debug = format!("{config:?}");
        assert!(!debug.contains("do-not-log"));
        assert!(debug.contains("service.token"));
        assert!(debug.contains("visible"));
    }

    #[test]
    fn kerberos_keytab_uses_url_principal_and_redacts_secret_path() {
        let config = HdfsConfig {
            kerberos_credentials: Some(HdfsKerberosCredentials::Keytab {
                keytab: "/run/secrets/source.keytab".into(),
            }),
            ..Default::default()
        };
        let location = "hdfs://source%2Fclient%40SOURCE.EXAMPLE@namenode:9000/root";
        let Ok((client, parsed)) = build_hdfs_client(location, &config) else {
            panic!("client-scoped keytab configuration was rejected");
        };
        drop(client);
        assert_eq!(parsed.user(), "source/client@SOURCE.EXAMPLE");
        let debug = format!("{config:?}");
        assert!(debug.contains("Keytab"));
        assert!(!debug.contains("/run/secrets/source.keytab"));
    }

    #[test]
    fn kerberos_cache_and_simple_authentication_cannot_be_mixed() {
        let config = HdfsConfig {
            overrides: HashMap::from([(
                "hadoop.security.authentication".to_string(),
                "simple".to_string(),
            )]),
            kerberos_credentials: Some(HdfsKerberosCredentials::CredentialCache {
                cache: "FILE:/run/krb5/source.ccache".to_string(),
            }),
            ..Default::default()
        };
        assert!(
            HdfsLocation::parse_configured("hdfs://source@namenode:9000/root", &config).is_err()
        );
        assert!(!format!("{config:?}").contains("/run/krb5/source.ccache"));
    }

    #[test]
    fn explicit_configuration_ignores_poisoned_hadoop_environment() {
        const CHILD_MARKER: &str = "DATA_MOVER_HDFS_ENV_TEST_CHILD";
        if std::env::var_os(CHILD_MARKER).is_some() {
            let result = build_hdfs_client(
                "hdfs://explicit@127.0.0.1:9000/isolated",
                &HdfsConfig::default(),
            );
            assert!(result.is_ok());
            return;
        }

        let Ok(executable) = std::env::current_exe() else {
            panic!("failed to locate the unit-test executable");
        };
        let Ok(status) = Command::new(executable)
            .args([
                "--exact",
                "hdfs::tests::explicit_configuration_ignores_poisoned_hadoop_environment",
            ])
            .env(CHILD_MARKER, "1")
            .env("HADOOP_CONF_DIR", "/poisoned/hadoop/conf")
            .env("HADOOP_HOME", "/poisoned/hadoop/home")
            .env("HADOOP_USER_NAME", "wrong-user")
            .env("HADOOP_PROXY_USER", "wrong-proxy")
            .env("KRB5CCNAME", "/poisoned/krb5cc")
            .env("HADOOP_TOKEN_FILE_LOCATION", "/poisoned/token")
            .status()
        else {
            panic!("failed to start the isolated environment test");
        };
        assert!(status.success());
    }
}
