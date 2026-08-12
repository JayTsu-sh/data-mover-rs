//! Local file data-plane seam.
//!
//! Callers depend on positional read/write and durable sync semantics, while
//! the adapter owns how those operations are executed. The initial adapter
//! preserves the existing Tokio blocking-pool implementation.

use std::fmt;
use std::sync::Arc;

#[cfg(unix)]
use std::os::unix::fs::FileExt as _;
#[cfg(windows)]
use std::os::windows::fs::FileExt as _;

use bytes::Bytes;

use crate::Result;
use crate::error::StorageError;

const ENGINE_ENV: &str = "DATA_MOVER_LOCAL_IO_ENGINE";
const READ_RINGS_ENV: &str = "DATA_MOVER_LOCAL_IO_READ_RINGS";
const WRITE_RINGS_ENV: &str = "DATA_MOVER_LOCAL_IO_WRITE_RINGS";
const RING_ENTRIES_ENV: &str = "DATA_MOVER_LOCAL_IO_RING_ENTRIES";
const READ_REQUESTS_ENV: &str = "DATA_MOVER_LOCAL_IO_READ_REQUESTS";
const WRITE_REQUESTS_ENV: &str = "DATA_MOVER_LOCAL_IO_WRITE_REQUESTS";
const BUFFER_MIB_ENV: &str = "DATA_MOVER_LOCAL_IO_BUFFER_MIB";

#[derive(Clone, Debug)]
pub(crate) struct LocalDataIo {
    adapter: LocalDataIoAdapter,
    selection: Arc<EngineSelection>,
}

impl LocalDataIo {
    pub(crate) fn from_config(config: &LocalIoConfig) -> Self {
        let selection = select_engine(&config.engine, &SystemCapabilityDetector);
        let adapter = LocalDataIoAdapter::from_selection(&selection, config.pool);
        Self {
            adapter,
            selection: Arc::new(selection),
        }
    }
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn attach(&self, file: tokio::fs::File) -> Result<LocalIoFile> {
        self.attach_for(file, LocalIoDirection::Both).await
    }

    pub(crate) async fn attach_for(
        &self,
        file: tokio::fs::File,
        direction: LocalIoDirection,
    ) -> Result<LocalIoFile> {
        if let Some(error) = &self.selection.error {
            return Err(StorageError::ConfigError(error.clone()));
        }
        self.adapter.attach(file, direction).await
    }

    pub(crate) async fn read_at(
        &self,
        file: &LocalIoFile,
        offset: u64,
        count: u64,
    ) -> Result<Bytes> {
        self.adapter.read_at(file, offset, count).await
    }

    pub(crate) async fn write_at(
        &self,
        file: &LocalIoFile,
        offset: u64,
        data: Bytes,
    ) -> Result<usize> {
        self.adapter.write_at(file, offset, data).await
    }

    pub(crate) async fn sync_all(&self, file: &LocalIoFile) -> Result<()> {
        self.adapter.sync_all(file).await
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LocalIoDirection {
    Read,
    Write,
    #[cfg_attr(not(test), allow(dead_code))]
    Both,
}

impl Default for LocalDataIo {
    fn default() -> Self {
        let config = LocalIoConfigBuilder::new().build();
        let (selection, pool) = match config {
            Ok(config) => (
                select_engine(&config.engine, &SystemCapabilityDetector),
                config.pool,
            ),
            Err(error) => (
                EngineSelection::error(
                    LocalIoEngine::Invalid(error.to_string()),
                    error.to_string(),
                ),
                LocalFsPoolConfig::default(),
            ),
        };
        tracing::debug!(
            mode = ?selection.mode,
            engine = ?selection.engine,
            kernel_release = ?selection.kernel_release,
            kernel_version = ?selection.kernel_version,
            recommended_kernel = selection.recommended_kernel,
            reason = ?selection.reason,
            error = ?selection.error,
            "selected local data I/O capability"
        );
        let adapter = LocalDataIoAdapter::from_selection(&selection, pool);
        Self {
            adapter,
            selection: Arc::new(selection),
        }
    }
}

#[derive(Clone, Debug)]
enum LocalDataIoAdapter {
    Blocking(BlockingLocalDataIo),
    #[cfg(target_os = "linux")]
    Uring(UringLocalDataIo),
    Failed(Arc<String>),
}

impl LocalDataIoAdapter {
    fn from_selection(selection: &EngineSelection, pool: LocalFsPoolConfig) -> Self {
        if let Some(error) = &selection.error {
            return Self::Failed(Arc::new(error.clone()));
        }
        #[cfg(target_os = "linux")]
        if selection.engine == SelectedEngine::Uring {
            return Self::Uring(UringLocalDataIo::new(selection.mode.clone(), pool));
        }
        Self::Blocking(BlockingLocalDataIo)
    }

    async fn attach(
        &self,
        file: tokio::fs::File,
        direction: LocalIoDirection,
    ) -> Result<LocalIoFile> {
        match self {
            Self::Blocking(adapter) => {
                let _ = direction;
                Ok(adapter.attach(file).await)
            }
            #[cfg(target_os = "linux")]
            Self::Uring(adapter) => adapter.attach(file, direction).await,
            Self::Failed(error) => Err(StorageError::ConfigError(error.to_string())),
        }
    }

    async fn read_at(&self, file: &LocalIoFile, offset: u64, count: u64) -> Result<Bytes> {
        match self {
            Self::Blocking(adapter) => adapter.read_at(file, offset, count).await,
            #[cfg(target_os = "linux")]
            Self::Uring(adapter) => adapter.read_at(file, offset, count).await,
            Self::Failed(error) => Err(StorageError::ConfigError(error.to_string())),
        }
    }

    async fn write_at(&self, file: &LocalIoFile, offset: u64, data: Bytes) -> Result<usize> {
        match self {
            Self::Blocking(adapter) => adapter.write_at(file, offset, data).await,
            #[cfg(target_os = "linux")]
            Self::Uring(adapter) => adapter.write_at(file, offset, data).await,
            Self::Failed(error) => Err(StorageError::ConfigError(error.to_string())),
        }
    }

    async fn sync_all(&self, file: &LocalIoFile) -> Result<()> {
        match self {
            Self::Blocking(adapter) => adapter.sync_all(file).await,
            #[cfg(target_os = "linux")]
            Self::Uring(adapter) => adapter.sync_all(file).await,
            Self::Failed(error) => Err(StorageError::ConfigError(error.to_string())),
        }
    }
}

#[cfg(test)]
fn mode_from_env_value(value: Option<&str>) -> std::result::Result<LocalIoEngineMode, String> {
    value.map_or(Ok(LocalIoEngineMode::Blocking), LocalIoEngineMode::parse)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalIoEngine {
    Auto,
    Uring,
    Blocking,
    Invalid(String),
}

impl LocalIoEngine {
    fn parse(value: &str) -> std::result::Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "uring" => Ok(Self::Uring),
            "blocking" => Ok(Self::Blocking),
            _ => Err(format!(
                "{ENGINE_ENV} must be one of auto, uring, or blocking; got {value:?}"
            )),
        }
    }
}

type LocalIoEngineMode = LocalIoEngine;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalIoConfig {
    engine: LocalIoEngine,
    pool: LocalFsPoolConfig,
}

impl LocalIoConfig {
    #[must_use]
    pub fn builder() -> LocalIoConfigBuilder {
        LocalIoConfigBuilder::new()
    }
    #[must_use]
    pub fn engine(&self) -> &LocalIoEngine {
        &self.engine
    }
    #[must_use]
    pub const fn read_rings(&self) -> usize {
        self.pool.read_rings
    }
    #[must_use]
    pub const fn write_rings(&self) -> usize {
        self.pool.write_rings
    }
    #[must_use]
    pub const fn ring_entries(&self) -> u32 {
        self.pool.ring_entries
    }
    #[must_use]
    pub const fn read_requests(&self) -> usize {
        self.pool.read_requests
    }
    #[must_use]
    pub const fn write_requests(&self) -> usize {
        self.pool.write_requests
    }
    #[must_use]
    pub const fn buffered_bytes(&self) -> usize {
        self.pool.buffered_bytes
    }
}

#[derive(Clone, Debug, Default)]
pub struct LocalIoConfigBuilder {
    engine: Option<LocalIoEngine>,
    read_rings: Option<usize>,
    write_rings: Option<usize>,
    ring_entries: Option<u32>,
    read_requests: Option<usize>,
    write_requests: Option<usize>,
    buffer_mib: Option<usize>,
}

impl LocalIoConfigBuilder {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            engine: None,
            read_rings: None,
            write_rings: None,
            ring_entries: None,
            read_requests: None,
            write_requests: None,
            buffer_mib: None,
        }
    }
    #[must_use]
    pub fn engine(mut self, value: LocalIoEngine) -> Self {
        self.engine = Some(value);
        self
    }
    #[must_use]
    pub fn read_rings(mut self, value: usize) -> Self {
        self.read_rings = Some(value);
        self
    }
    #[must_use]
    pub fn write_rings(mut self, value: usize) -> Self {
        self.write_rings = Some(value);
        self
    }
    #[must_use]
    pub fn ring_entries(mut self, value: u32) -> Self {
        self.ring_entries = Some(value);
        self
    }
    #[must_use]
    pub fn read_requests(mut self, value: usize) -> Self {
        self.read_requests = Some(value);
        self
    }
    #[must_use]
    pub fn write_requests(mut self, value: usize) -> Self {
        self.write_requests = Some(value);
        self
    }
    #[must_use]
    pub fn buffer_mib(mut self, value: usize) -> Self {
        self.buffer_mib = Some(value);
        self
    }
    /// Resolves explicit values over environment values and validates the result.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for malformed or out-of-range values.
    pub fn build(self) -> Result<LocalIoConfig> {
        self.build_with(|name| std::env::var(name).ok())
    }
    fn build_with(self, lookup: impl Fn(&str) -> Option<String>) -> Result<LocalIoConfig> {
        let defaults = LocalFsPoolConfig::default();
        let engine = match self.engine {
            Some(value) => value,
            None => lookup(ENGINE_ENV)
                .as_deref()
                .map_or(Ok(LocalIoEngine::Blocking), LocalIoEngine::parse)
                .map_err(StorageError::ConfigError)?,
        };
        let pool = LocalFsPoolConfig {
            read_rings: resolve_usize(
                self.read_rings,
                READ_RINGS_ENV,
                defaults.read_rings,
                &lookup,
                "1..=4",
            )?,
            write_rings: resolve_usize(
                self.write_rings,
                WRITE_RINGS_ENV,
                defaults.write_rings,
                &lookup,
                "1..=4",
            )?,
            ring_entries: resolve_u32(
                self.ring_entries,
                RING_ENTRIES_ENV,
                defaults.ring_entries,
                &lookup,
            )?,
            read_requests: resolve_usize(
                self.read_requests,
                READ_REQUESTS_ENV,
                defaults.read_requests,
                &lookup,
                "> 0",
            )?,
            write_requests: resolve_usize(
                self.write_requests,
                WRITE_REQUESTS_ENV,
                defaults.write_requests,
                &lookup,
                "> 0",
            )?,
            buffered_bytes: resolve_usize(
                self.buffer_mib,
                BUFFER_MIB_ENV,
                defaults.buffered_bytes / 1024 / 1024,
                &lookup,
                "1..=512",
            )?
            .checked_mul(1024 * 1024)
            .ok_or_else(|| {
                StorageError::ConfigError(format!("{BUFFER_MIB_ENV} overflows bytes"))
            })?,
        }
        .validate()
        .map_err(StorageError::ConfigError)?;
        Ok(LocalIoConfig { engine, pool })
    }
}

fn resolve_usize(
    explicit: Option<usize>,
    name: &str,
    default: usize,
    lookup: &impl Fn(&str) -> Option<String>,
    range: &str,
) -> Result<usize> {
    if let Some(value) = explicit {
        return Ok(value);
    }
    lookup(name).map_or(Ok(default), |raw| {
        raw.parse().map_err(|_| {
            StorageError::ConfigError(format!("{name} must be an integer in {range}, got {raw:?}"))
        })
    })
}

fn resolve_u32(
    explicit: Option<u32>,
    name: &str,
    default: u32,
    lookup: &impl Fn(&str) -> Option<String>,
) -> Result<u32> {
    if let Some(value) = explicit {
        return Ok(value);
    }
    lookup(name).map_or(Ok(default), |raw| {
        raw.parse().map_err(|_| {
            StorageError::ConfigError(format!(
                "{name} must be a power-of-two integer, got {raw:?}"
            ))
        })
    })
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct KernelVersion {
    major: u64,
    minor: u64,
    patch: u64,
}

impl KernelVersion {
    const AUTO_MINIMUM: Self = Self::new(5, 10, 0);
    const RECOMMENDED: Self = Self::new(5, 15, 0);

    const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    fn parse(release: &str) -> Option<Self> {
        let mut fields = release.trim().split('.');
        let major = parse_leading_number(fields.next()?)?;
        let minor = parse_leading_number(fields.next()?)?;
        let patch = fields.next().map_or(Some(0), parse_leading_number)?;
        Some(Self::new(major, minor, patch))
    }
}

fn parse_leading_number(field: &str) -> Option<u64> {
    let digits = field
        .as_bytes()
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    (digits > 0)
        .then(|| field[..digits].parse::<u64>().ok())
        .flatten()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SelectedEngine {
    Blocking,
    Uring,
}

#[derive(Clone, Debug)]
struct EngineSelection {
    mode: LocalIoEngineMode,
    engine: SelectedEngine,
    kernel_release: Option<String>,
    kernel_version: Option<KernelVersion>,
    recommended_kernel: bool,
    reason: Option<String>,
    error: Option<String>,
}

impl EngineSelection {
    fn blocking(
        mode: LocalIoEngineMode,
        reason: impl Into<String>,
        release: Option<String>,
    ) -> Self {
        let kernel_version = release.as_deref().and_then(KernelVersion::parse);
        Self {
            mode,
            engine: SelectedEngine::Blocking,
            kernel_release: release,
            kernel_version,
            recommended_kernel: kernel_version.is_some_and(|v| v >= KernelVersion::RECOMMENDED),
            reason: Some(reason.into()),
            error: None,
        }
    }

    fn uring(mode: LocalIoEngineMode, release: Option<String>) -> Self {
        let kernel_version = release.as_deref().and_then(KernelVersion::parse);
        Self {
            mode,
            engine: SelectedEngine::Uring,
            kernel_release: release,
            kernel_version,
            recommended_kernel: kernel_version.is_some_and(|v| v >= KernelVersion::RECOMMENDED),
            reason: None,
            error: None,
        }
    }

    fn error(mode: LocalIoEngineMode, error: String) -> Self {
        Self {
            mode,
            engine: SelectedEngine::Blocking,
            kernel_release: None,
            kernel_version: None,
            recommended_kernel: false,
            reason: None,
            error: Some(error),
        }
    }
}

trait CapabilityDetector {
    fn kernel_release(&self) -> std::io::Result<String>;
    fn probe(&self) -> std::result::Result<(), ProbeError>;
}

#[derive(Debug)]
enum ProbeError {
    Setup(std::io::Error),
    MissingOpcodes(Vec<&'static str>),
}

impl fmt::Display for ProbeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Setup(error) => write!(formatter, "io_uring setup/probe failed: {error}"),
            Self::MissingOpcodes(opcodes) => {
                write!(
                    formatter,
                    "io_uring is missing required opcodes: {}",
                    opcodes.join(", ")
                )
            }
        }
    }
}

struct SystemCapabilityDetector;

impl CapabilityDetector for SystemCapabilityDetector {
    fn kernel_release(&self) -> std::io::Result<String> {
        #[cfg(target_os = "linux")]
        {
            std::fs::read_to_string("/proc/sys/kernel/osrelease")
                .map(|release| release.trim().to_string())
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "kernel release probe is only available on Linux",
            ))
        }
    }

    fn probe(&self) -> std::result::Result<(), ProbeError> {
        probe_io_uring()
    }
}

fn select_engine(mode: &LocalIoEngineMode, detector: &impl CapabilityDetector) -> EngineSelection {
    if *mode == LocalIoEngineMode::Blocking {
        return EngineSelection::blocking(mode.clone(), "explicit blocking mode", None);
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = detector;
        return if *mode == LocalIoEngineMode::Uring {
            EngineSelection::error(
                mode.clone(),
                "io_uring mode is only supported on Linux".to_string(),
            )
        } else {
            EngineSelection::blocking(mode.clone(), "io_uring is only supported on Linux", None)
        };
    }

    #[cfg(target_os = "linux")]
    {
        let release = detector.kernel_release().ok();
        let version = release.as_deref().and_then(KernelVersion::parse);
        if *mode == LocalIoEngineMode::Auto
            && version.is_some_and(|value| value < KernelVersion::AUTO_MINIMUM)
        {
            return EngineSelection::blocking(
                mode.clone(),
                "kernel is below the auto-mode minimum 5.10",
                release,
            );
        }
        match detector.probe() {
            Ok(()) => EngineSelection::uring(mode.clone(), release),
            Err(error) if *mode == LocalIoEngineMode::Auto => {
                EngineSelection::blocking(mode.clone(), error.to_string(), release)
            }
            Err(error) => EngineSelection::error(
                mode.clone(),
                format!("forced io_uring mode failed: {error}"),
            ),
        }
    }
}

#[cfg(target_os = "linux")]
fn probe_io_uring() -> std::result::Result<(), ProbeError> {
    use io_uring::{IoUring, Probe, opcode};

    let ring = IoUring::new(2).map_err(ProbeError::Setup)?;
    let mut probe = Probe::new();
    ring.submitter()
        .register_probe(&mut probe)
        .map_err(ProbeError::Setup)?;
    let mut missing = Vec::new();
    if !probe.is_supported(opcode::Read::CODE) {
        missing.push("READ");
    }
    if !probe.is_supported(opcode::Write::CODE) {
        missing.push("WRITE");
    }
    if !probe.is_supported(opcode::Fsync::CODE) {
        missing.push("FSYNC");
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(ProbeError::MissingOpcodes(missing))
    }
}

#[cfg(not(target_os = "linux"))]
fn probe_io_uring() -> std::result::Result<(), ProbeError> {
    Err(ProbeError::Setup(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "io_uring is only supported on Linux",
    )))
}

#[derive(Debug)]
pub(crate) struct LocalIoFile {
    inner: Arc<std::fs::File>,
    #[cfg(target_os = "linux")]
    uring: Option<UringFileTokens>,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
struct AttachedUringFile {
    worker: UringWorker,
    token: u64,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
struct UringFileTokens {
    read: Option<AttachedUringFile>,
    write: Option<AttachedUringFile>,
    pool: Arc<LocalFsIoPool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LocalFsPoolConfig {
    read_rings: usize,
    write_rings: usize,
    ring_entries: u32,
    read_requests: usize,
    write_requests: usize,
    buffered_bytes: usize,
}

impl Default for LocalFsPoolConfig {
    fn default() -> Self {
        Self {
            read_rings: 2,
            write_rings: 2,
            ring_entries: 64,
            read_requests: 32,
            write_requests: 64,
            buffered_bytes: 256 * 1024 * 1024,
        }
    }
}

impl LocalFsPoolConfig {
    fn validate(self) -> std::result::Result<Self, String> {
        if !(1..=4).contains(&self.read_rings) || !(1..=4).contains(&self.write_rings) {
            return Err("io_uring worker count must be between 1 and 4 per direction".to_string());
        }
        if !(8..=4096).contains(&self.ring_entries) || !self.ring_entries.is_power_of_two() {
            return Err(
                "io_uring ring entries must be a power of two between 8 and 4096".to_string(),
            );
        }
        if self.read_requests == 0 || self.write_requests == 0 || self.buffered_bytes == 0 {
            return Err(
                "io_uring request and buffered-byte budgets must be greater than zero".to_string(),
            );
        }
        if self.buffered_bytes > 512 * 1024 * 1024 {
            return Err("io_uring buffered-byte budget must not exceed 512 MiB".to_string());
        }
        Ok(self)
    }
}

#[cfg(target_os = "linux")]
mod uring;

#[cfg(target_os = "linux")]
use uring::{LocalFsIoPool, UringLocalDataIo, UringWorker};

#[cfg(all(test, target_os = "linux"))]
use uring::{
    LocalFsRegistry, PoolDirection, PoolOperation, PoolState, is_explicitly_unsupported,
    is_uncertain_worker_error, with_byte_budget,
};
#[derive(Clone, Copy, Debug, Default)]
struct BlockingLocalDataIo;

impl BlockingLocalDataIo {
    async fn attach(self, file: tokio::fs::File) -> LocalIoFile {
        LocalIoFile {
            inner: Arc::new(file.into_std().await),
            #[cfg(target_os = "linux")]
            uring: None,
        }
    }

    async fn read_at(self, file: &LocalIoFile, offset: u64, count: u64) -> Result<Bytes> {
        let capacity = usize::try_from(count).map_err(|_| {
            StorageError::OperationError(format!("read size {count} exceeds platform capacity"))
        })?;
        let file = Arc::clone(&file.inner);
        let buffer = tokio::task::spawn_blocking(move || {
            let mut buffer = vec![0_u8; capacity];
            let filled = read_fully_at(&mut buffer, offset, |remaining, position| {
                #[cfg(unix)]
                let read = file.read_at(remaining, position)?;
                #[cfg(windows)]
                let read = file.seek_read(remaining, position)?;
                Ok(read)
            })?;
            buffer.truncate(filled);
            Ok::<_, std::io::Error>(buffer)
        })
        .await??;
        Ok(Bytes::from(buffer))
    }

    async fn write_at(self, file: &LocalIoFile, offset: u64, data: Bytes) -> Result<usize> {
        let length = data.len();
        let file = Arc::clone(&file.inner);
        tokio::task::spawn_blocking(move || {
            write_fully_at(&data, offset, |remaining, position| {
                #[cfg(unix)]
                let count = file.write_at(remaining, position)?;
                #[cfg(windows)]
                let count = file.seek_write(remaining, position)?;
                Ok(count)
            })
        })
        .await??;
        Ok(length)
    }

    async fn sync_all(self, file: &LocalIoFile) -> Result<()> {
        let file = Arc::clone(&file.inner);
        tokio::task::spawn_blocking(move || file.sync_all())
            .await?
            .map_err(StorageError::IoError)
    }
}

fn read_fully_at(
    buffer: &mut [u8],
    offset: u64,
    mut read: impl FnMut(&mut [u8], u64) -> std::io::Result<usize>,
) -> std::io::Result<usize> {
    let mut filled = 0usize;
    while filled < buffer.len() {
        let position = offset.checked_add(filled as u64).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "read offset overflow")
        })?;
        let count = read(&mut buffer[filled..], position)?;
        if count == 0 {
            break;
        }
        filled += count;
    }
    Ok(filled)
}

fn write_fully_at(
    data: &[u8],
    offset: u64,
    mut write: impl FnMut(&[u8], u64) -> std::io::Result<usize>,
) -> std::io::Result<usize> {
    let mut written = 0usize;
    while written < data.len() {
        let position = offset.checked_add(written as u64).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "write offset overflow")
        })?;
        let count = write(&data[written..], position)?;
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write the complete local file chunk",
            ));
        }
        written += count;
    }
    Ok(written)
}

#[cfg(test)]
mod tests;
