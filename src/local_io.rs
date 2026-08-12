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

#[derive(Clone, Debug)]
pub(crate) struct LocalDataIo {
    adapter: LocalDataIoAdapter,
    selection: Arc<EngineSelection>,
}

impl LocalDataIo {
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
        let mode = mode_from_env_value(std::env::var(ENGINE_ENV).ok().as_deref())
            .unwrap_or_else(LocalIoEngineMode::Invalid);
        let selection = match mode {
            LocalIoEngineMode::Invalid(error) => {
                EngineSelection::error(LocalIoEngineMode::Invalid(error.clone()), error)
            }
            mode => select_engine(&mode, &SystemCapabilityDetector),
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
        let adapter = LocalDataIoAdapter::from_selection(&selection);
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
    fn from_selection(selection: &EngineSelection) -> Self {
        if let Some(error) = &selection.error {
            return Self::Failed(Arc::new(error.clone()));
        }
        #[cfg(target_os = "linux")]
        if selection.engine == SelectedEngine::Uring {
            return Self::Uring(UringLocalDataIo::new(selection.mode.clone()));
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

fn mode_from_env_value(value: Option<&str>) -> std::result::Result<LocalIoEngineMode, String> {
    value.map_or(Ok(LocalIoEngineMode::Auto), LocalIoEngineMode::parse)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LocalIoEngineMode {
    Auto,
    Uring,
    Blocking,
    Invalid(String),
}

impl LocalIoEngineMode {
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
    _pool: Arc<LocalFsIoPool>,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LocalFsPoolConfig {
    read_rings: usize,
    write_rings: usize,
}

#[cfg(target_os = "linux")]
impl Default for LocalFsPoolConfig {
    fn default() -> Self {
        Self {
            read_rings: 2,
            write_rings: 2,
        }
    }
}

#[cfg(target_os = "linux")]
impl LocalFsPoolConfig {
    fn validate(self) -> std::io::Result<Self> {
        if !(1..=4).contains(&self.read_rings) || !(1..=4).contains(&self.write_rings) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "io_uring worker count must be between 1 and 4 per direction",
            ));
        }
        Ok(self)
    }
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
enum PoolDirection {
    Read,
    Write,
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct LocalFsIoPool {
    device: u64,
    config: LocalFsPoolConfig,
    read: std::sync::OnceLock<std::result::Result<Vec<UringWorker>, String>>,
    write: std::sync::OnceLock<std::result::Result<Vec<UringWorker>, String>>,
}

#[cfg(target_os = "linux")]
impl LocalFsIoPool {
    fn new(device: u64, config: LocalFsPoolConfig) -> Self {
        debug_assert!(config.validate().is_ok());
        Self {
            device,
            config,
            read: std::sync::OnceLock::new(),
            write: std::sync::OnceLock::new(),
        }
    }

    fn worker(
        &self,
        direction: PoolDirection,
        device: u64,
        inode: u64,
    ) -> std::io::Result<UringWorker> {
        let (workers, count, label) = match direction {
            PoolDirection::Read => (&self.read, self.config.read_rings, "read"),
            PoolDirection::Write => (&self.write, self.config.write_rings, "write"),
        };
        let workers = workers.get_or_init(|| spawn_worker_group(self.device, label, count));
        let workers = workers
            .as_ref()
            .map_err(|error| std::io::Error::other(error.clone()))?;
        let index = affinity_index(device, inode, workers.len());
        Ok(workers[index].clone())
    }
}

#[cfg(target_os = "linux")]
fn spawn_worker_group(
    device: u64,
    direction: &str,
    count: usize,
) -> std::result::Result<Vec<UringWorker>, String> {
    (0..count)
        .map(|index| {
            UringWorker::spawn(&format!("data-mover-uring-{device}-{direction}-{index}"))
                .map_err(|error| error.to_string())
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn affinity_index(device: u64, inode: u64, worker_count: usize) -> usize {
    let mixed = device.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ inode;
    usize::try_from(mixed % worker_count as u64).unwrap_or(0)
}

#[cfg(target_os = "linux")]
#[derive(Default)]
struct LocalFsRegistry {
    pools: parking_lot::Mutex<std::collections::HashMap<u64, std::sync::Weak<LocalFsIoPool>>>,
}

#[cfg(target_os = "linux")]
impl LocalFsRegistry {
    fn resolve(&self, device: u64, config: LocalFsPoolConfig) -> Arc<LocalFsIoPool> {
        let mut pools = self.pools.lock();
        if let Some(pool) = pools.get(&device).and_then(std::sync::Weak::upgrade) {
            return pool;
        }
        let pool = Arc::new(LocalFsIoPool::new(device, config));
        pools.insert(device, Arc::downgrade(&pool));
        pool
    }
}

#[cfg(target_os = "linux")]
fn global_fs_registry() -> &'static LocalFsRegistry {
    static REGISTRY: std::sync::OnceLock<LocalFsRegistry> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(LocalFsRegistry::default)
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
struct UringLocalDataIo {
    mode: LocalIoEngineMode,
    pools: Arc<parking_lot::Mutex<std::collections::HashMap<u64, Arc<LocalFsIoPool>>>>,
}

#[cfg(target_os = "linux")]
impl UringLocalDataIo {
    fn new(mode: LocalIoEngineMode) -> Self {
        Self {
            mode,
            pools: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        }
    }

    async fn attach(
        &self,
        file: tokio::fs::File,
        direction: LocalIoDirection,
    ) -> Result<LocalIoFile> {
        use std::os::unix::fs::MetadataExt as _;

        let standard = file.into_std().await;
        let metadata = standard.metadata()?;
        let device = metadata.dev();
        let inode = metadata.ino();
        let pool = if let Some(pool) = self.pools.lock().get(&device).cloned() {
            pool
        } else {
            let pool = global_fs_registry().resolve(device, LocalFsPoolConfig::default());
            self.pools.lock().insert(device, Arc::clone(&pool));
            pool
        };
        let attached = self
            .attach_to_pool(&standard, &pool, device, inode, direction)
            .await;
        let uring = match attached {
            Ok(tokens) => Some(tokens),
            Err(error) if self.mode == LocalIoEngineMode::Auto => {
                tracing::debug!(%error, device, "io_uring pool initialization failed; using blocking I/O for file");
                None
            }
            Err(error) => {
                return Err(StorageError::ConfigError(format!(
                    "forced io_uring pool initialization failed: {error}"
                )));
            }
        };
        Ok(LocalIoFile {
            inner: Arc::new(standard),
            uring,
        })
    }

    async fn attach_to_pool(
        &self,
        file: &std::fs::File,
        pool: &Arc<LocalFsIoPool>,
        device: u64,
        inode: u64,
        direction: LocalIoDirection,
    ) -> std::io::Result<UringFileTokens> {
        let read_worker = matches!(direction, LocalIoDirection::Read | LocalIoDirection::Both)
            .then(|| pool.worker(PoolDirection::Read, device, inode))
            .transpose()?;
        let write_worker = matches!(direction, LocalIoDirection::Write | LocalIoDirection::Both)
            .then(|| pool.worker(PoolDirection::Write, device, inode))
            .transpose()?;
        let read = if let Some(worker) = read_worker {
            let token = worker.attach(file.try_clone()?).await?;
            Some(AttachedUringFile { worker, token })
        } else {
            None
        };
        let write = if let Some(worker) = write_worker {
            let token = worker.attach(file.try_clone()?).await?;
            Some(AttachedUringFile { worker, token })
        } else {
            None
        };
        Ok(UringFileTokens {
            read,
            write,
            _pool: Arc::clone(pool),
        })
    }

    async fn read_at(&self, file: &LocalIoFile, offset: u64, count: u64) -> Result<Bytes> {
        if let Some(attached) = file.uring.as_ref().and_then(|tokens| tokens.read.as_ref()) {
            attached
                .worker
                .read(attached.token, offset, count)
                .await
                .map_err(Into::into)
        } else {
            BlockingLocalDataIo.read_at(file, offset, count).await
        }
    }

    async fn write_at(&self, file: &LocalIoFile, offset: u64, data: Bytes) -> Result<usize> {
        if let Some(attached) = file.uring.as_ref().and_then(|tokens| tokens.write.as_ref()) {
            attached
                .worker
                .write(attached.token, offset, data)
                .await
                .map_err(Into::into)
        } else {
            BlockingLocalDataIo.write_at(file, offset, data).await
        }
    }

    async fn sync_all(&self, file: &LocalIoFile) -> Result<()> {
        if let Some(attached) = file.uring.as_ref().and_then(|tokens| tokens.write.as_ref()) {
            attached
                .worker
                .sync(attached.token)
                .await
                .map_err(Into::into)
        } else {
            BlockingLocalDataIo.sync_all(file).await
        }
    }
}

#[cfg(target_os = "linux")]
enum UringCommand {
    Attach {
        file: std::fs::File,
        complete: tokio::sync::oneshot::Sender<std::io::Result<u64>>,
    },
    Read {
        token: u64,
        offset: u64,
        count: u64,
        complete: tokio::sync::oneshot::Sender<std::io::Result<Bytes>>,
    },
    Write {
        token: u64,
        offset: u64,
        data: Bytes,
        complete: tokio::sync::oneshot::Sender<std::io::Result<usize>>,
    },
    Sync {
        token: u64,
        complete: tokio::sync::oneshot::Sender<std::io::Result<()>>,
    },
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
struct UringWorker {
    sender: async_channel::Sender<UringCommand>,
    #[cfg_attr(not(test), allow(dead_code))]
    id: u64,
}

#[cfg(target_os = "linux")]
impl UringWorker {
    const CHANNEL_CAPACITY: usize = 64;
    const RING_ENTRIES: u32 = 64;

    fn spawn(name: &str) -> std::io::Result<Self> {
        static NEXT_WORKER_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let (sender, receiver) = async_channel::bounded(Self::CHANNEL_CAPACITY);
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    tokio_uring::builder()
                        .entries(Self::RING_ENTRIES)
                        .start(async move {
                            let _ = started_tx.send(());
                            run_uring_worker(receiver).await;
                        });
                }));
                if result.is_err() {
                    tracing::debug!("io_uring worker runtime failed during startup or execution");
                }
            })?;
        started_rx.recv().map_err(|_| {
            std::io::Error::other("io_uring worker failed before runtime initialization")
        })?;
        Ok(Self {
            sender,
            id: NEXT_WORKER_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        })
    }

    async fn send(&self, command: UringCommand) -> std::io::Result<()> {
        self.sender.send(command).await.map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "io_uring worker stopped")
        })
    }

    async fn attach(&self, file: std::fs::File) -> std::io::Result<u64> {
        let (complete, receive) = tokio::sync::oneshot::channel();
        self.send(UringCommand::Attach { file, complete }).await?;
        receive.await.map_err(worker_completion_lost)?
    }

    async fn read(&self, token: u64, offset: u64, count: u64) -> std::io::Result<Bytes> {
        let (complete, receive) = tokio::sync::oneshot::channel();
        self.send(UringCommand::Read {
            token,
            offset,
            count,
            complete,
        })
        .await?;
        receive.await.map_err(worker_completion_lost)?
    }

    async fn write(&self, token: u64, offset: u64, data: Bytes) -> std::io::Result<usize> {
        let (complete, receive) = tokio::sync::oneshot::channel();
        self.send(UringCommand::Write {
            token,
            offset,
            data,
            complete,
        })
        .await?;
        receive.await.map_err(worker_completion_lost)?
    }

    async fn sync(&self, token: u64) -> std::io::Result<()> {
        let (complete, receive) = tokio::sync::oneshot::channel();
        self.send(UringCommand::Sync { token, complete }).await?;
        receive.await.map_err(worker_completion_lost)?
    }
}

#[cfg(target_os = "linux")]
fn worker_completion_lost(_: tokio::sync::oneshot::error::RecvError) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "io_uring worker stopped before completing request",
    )
}

#[cfg(target_os = "linux")]
async fn run_uring_worker(receiver: async_channel::Receiver<UringCommand>) {
    use std::collections::HashMap;
    use std::rc::Rc;

    let mut files = HashMap::<u64, Rc<tokio_uring::fs::File>>::new();
    let mut next_token = 1_u64;
    while let Ok(command) = receiver.recv().await {
        match command {
            UringCommand::Attach { file, complete } => {
                let token = next_token;
                next_token = next_token.wrapping_add(1).max(1);
                files.insert(token, Rc::new(tokio_uring::fs::File::from_std(file)));
                let _ = complete.send(Ok(token));
            }
            UringCommand::Read {
                token,
                offset,
                count,
                complete,
            } => {
                if let Some(file) = files.get(&token).cloned() {
                    tokio_uring::spawn(async move {
                        let _ = complete.send(uring_read_fully(&file, offset, count).await);
                    });
                } else {
                    let _ = complete.send(Err(unknown_file_token(token)));
                }
            }
            UringCommand::Write {
                token,
                offset,
                data,
                complete,
            } => {
                let length = data.len();
                if let Some(file) = files.get(&token).cloned() {
                    tokio_uring::spawn(async move {
                        let (result, _) = file.write_all_at(data, offset).await;
                        let _ = complete.send(result.map(|()| length));
                    });
                } else {
                    let _ = complete.send(Err(unknown_file_token(token)));
                }
            }
            UringCommand::Sync { token, complete } => {
                if let Some(file) = files.get(&token).cloned() {
                    tokio_uring::spawn(async move {
                        let _ = complete.send(file.sync_all().await);
                    });
                } else {
                    let _ = complete.send(Err(unknown_file_token(token)));
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
async fn uring_read_fully(
    file: &tokio_uring::fs::File,
    offset: u64,
    count: u64,
) -> std::io::Result<Bytes> {
    use tokio_uring::buf::BoundedBuf as _;

    let capacity = usize::try_from(count).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "read size exceeds platform capacity",
        )
    })?;
    let mut buffer = vec![0_u8; capacity];
    let mut filled = 0_usize;
    while filled < capacity {
        let position = offset.checked_add(filled as u64).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "read offset overflow")
        })?;
        let (result, slice) = file.read_at(buffer.slice(filled..), position).await;
        buffer = slice.into_inner();
        match result? {
            0 => break,
            count => filled += count,
        }
    }
    buffer.truncate(filled);
    Ok(Bytes::from(buffer))
}

#[cfg(target_os = "linux")]
fn unknown_file_token(token: u64) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("unknown io_uring file token {token}"),
    )
}

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
mod tests {
    use std::cell::Cell;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use bytes::Bytes;

    use super::{
        CapabilityDetector, EngineSelection, KernelVersion, LocalDataIo, LocalIoEngineMode,
        ProbeError, SelectedEngine, mode_from_env_value, read_fully_at, select_engine,
        write_fully_at,
    };

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    fn blocking_io() -> LocalDataIo {
        LocalDataIo {
            adapter: super::LocalDataIoAdapter::Blocking(super::BlockingLocalDataIo),
            selection: Arc::new(EngineSelection::blocking(
                LocalIoEngineMode::Blocking,
                "test",
                None,
            )),
        }
    }

    #[cfg(target_os = "linux")]
    fn uring_io() -> LocalDataIo {
        LocalDataIo {
            adapter: super::LocalDataIoAdapter::Uring(super::UringLocalDataIo::new(
                LocalIoEngineMode::Uring,
            )),
            selection: Arc::new(EngineSelection::uring(
                LocalIoEngineMode::Uring,
                Some("test".to_string()),
            )),
        }
    }

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> std::io::Result<Self> {
            let serial = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "data-mover-local-io-{label}-{}-{serial}",
                std::process::id()
            ));
            std::fs::create_dir(&path)?;
            Ok(Self(path))
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_positional_reads_preserve_offsets_and_eof_length()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = TempDir::new("reads")?;
        let path = temp.path().join("source.bin");
        std::fs::write(&path, b"0123456789abcdef")?;
        let io = LocalDataIo::default();
        let file = io.attach(tokio::fs::File::open(path).await?).await?;

        let (first, middle, tail) = tokio::join!(
            io.read_at(&file, 0, 4),
            io.read_at(&file, 6, 5),
            io.read_at(&file, 14, 8),
        );
        assert_eq!(first?.as_ref(), b"0123");
        assert_eq!(middle?.as_ref(), b"6789a");
        assert_eq!(tail?.as_ref(), b"ef");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn out_of_order_writes_sync_and_reopen_with_expected_digest()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = TempDir::new("writes")?;
        let path = temp.path().join("destination.bin");
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .await?;
        let io = LocalDataIo::default();
        let file = io.attach(file).await?;

        let (tail, head, middle) = tokio::join!(
            io.write_at(&file, 8, Bytes::from_static(b"ijkl")),
            io.write_at(&file, 0, Bytes::from_static(b"abcd")),
            io.write_at(&file, 4, Bytes::from_static(b"efgh")),
        );
        assert_eq!(tail?, 4);
        assert_eq!(head?, 4);
        assert_eq!(middle?, 4);
        io.sync_all(&file).await?;
        drop(file);

        let actual = std::fs::read(path)?;
        let expected = b"abcdefghijkl";
        assert_eq!(actual, expected);
        assert_eq!(blake3::hash(&actual), blake3::hash(expected));
        Ok(())
    }

    async fn run_data_io_contract(
        label: &str,
        io: LocalDataIo,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp = TempDir::new(label)?;
        let path = temp.path().join("contract.bin");
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .await?;
        let file = io.attach(file).await?;
        assert!(io.read_at(&file, 0, 0).await?.is_empty());

        let chunks = [
            (8, Bytes::from_static(b"ijklmnop")),
            (0, Bytes::from_static(b"abcdefgh")),
            (16, Bytes::from_static(b"qrstuvwxyz")),
        ];
        let (middle, head, tail) = tokio::join!(
            io.write_at(&file, chunks[0].0, chunks[0].1.clone()),
            io.write_at(&file, chunks[1].0, chunks[1].1.clone()),
            io.write_at(&file, chunks[2].0, chunks[2].1.clone()),
        );
        assert_eq!((middle?, head?, tail?), (8, 8, 10));
        io.sync_all(&file).await?;

        let (head, middle, eof) = tokio::join!(
            io.read_at(&file, 0, 8),
            io.read_at(&file, 8, 8),
            io.read_at(&file, 24, 16),
        );
        assert_eq!(head?.as_ref(), b"abcdefgh");
        assert_eq!(middle?.as_ref(), b"ijklmnop");
        assert_eq!(eof?.as_ref(), b"yz");
        drop(file);

        let actual = std::fs::read(path)?;
        assert_eq!(actual, b"abcdefghijklmnopqrstuvwxyz");
        assert_eq!(
            blake3::hash(&actual),
            blake3::hash(b"abcdefghijklmnopqrstuvwxyz")
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn blocking_adapter_satisfies_data_io_contract() -> Result<(), Box<dyn std::error::Error>>
    {
        run_data_io_contract("blocking-contract", blocking_io()).await
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn real_uring_adapter_satisfies_data_io_contract_when_available()
    -> Result<(), Box<dyn std::error::Error>> {
        if super::probe_io_uring().is_err() {
            return Ok(());
        }
        run_data_io_contract("uring-contract", uring_io()).await
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn closed_uring_worker_returns_broken_pipe_without_waiting() {
        let (sender, receiver) = async_channel::bounded(1);
        receiver.close();
        let worker = super::UringWorker { sender, id: 0 };
        let error = worker
            .read(1, 0, 1)
            .await
            .err()
            .unwrap_or_else(|| std::io::Error::other("closed worker unexpectedly succeeded"));
        assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn pool_config_accepts_one_through_four_per_direction() {
        use super::LocalFsPoolConfig;

        for count in 1..=4 {
            assert!(
                LocalFsPoolConfig {
                    read_rings: count,
                    write_rings: count
                }
                .validate()
                .is_ok()
            );
        }
        for count in [0, 5] {
            assert!(
                LocalFsPoolConfig {
                    read_rings: count,
                    write_rings: 2
                }
                .validate()
                .is_err()
            );
            assert!(
                LocalFsPoolConfig {
                    read_rings: 2,
                    write_rings: count
                }
                .validate()
                .is_err()
            );
        }
        assert_eq!(LocalFsPoolConfig::default().read_rings, 2);
        assert_eq!(LocalFsPoolConfig::default().write_rings, 2);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn registry_is_single_flight_per_device_and_weakly_reclaims_pools() {
        use super::{LocalFsPoolConfig, LocalFsRegistry};

        let registry = Arc::new(LocalFsRegistry::default());
        let threads = (0..8)
            .map(|_| {
                let registry = Arc::clone(&registry);
                std::thread::spawn(move || registry.resolve(42, LocalFsPoolConfig::default()))
            })
            .collect::<Vec<_>>();
        let pools = threads
            .into_iter()
            .map(|thread| {
                thread
                    .join()
                    .unwrap_or_else(|_| panic!("registry test thread panicked"))
            })
            .collect::<Vec<_>>();
        assert!(pools.iter().all(|pool| Arc::ptr_eq(&pools[0], pool)));
        let different = registry.resolve(43, LocalFsPoolConfig::default());
        assert!(!Arc::ptr_eq(&pools[0], &different));

        let weak = Arc::downgrade(&pools[0]);
        drop(pools);
        assert!(weak.upgrade().is_none());
        let rebuilt = registry.resolve(42, LocalFsPoolConfig::default());
        assert_eq!(rebuilt.device, 42);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn pool_initializes_directions_lazily_and_affinity_is_stable() -> std::io::Result<()> {
        use super::{LocalFsIoPool, LocalFsPoolConfig, PoolDirection};

        if super::probe_io_uring().is_err() {
            return Ok(());
        }
        let pool = LocalFsIoPool::new(7, LocalFsPoolConfig::default());
        assert!(pool.read.get().is_none());
        assert!(pool.write.get().is_none());
        let first = pool.worker(PoolDirection::Read, 7, 100)?;
        let repeated = pool.worker(PoolDirection::Read, 7, 100)?;
        assert_eq!(first.id, repeated.id);
        assert_eq!(
            pool.read
                .get()
                .and_then(|result| result.as_ref().ok())
                .map(Vec::len),
            Some(2)
        );
        assert!(pool.write.get().is_none());

        let ids = (100..132)
            .map(|inode| {
                pool.worker(PoolDirection::Read, 7, inode)
                    .map(|worker| worker.id)
            })
            .collect::<std::io::Result<std::collections::HashSet<_>>>()?;
        assert_eq!(ids.len(), 2);
        let write = pool.worker(PoolDirection::Write, 7, 100)?;
        assert_ne!(write.id, first.id);
        assert_eq!(
            pool.write
                .get()
                .and_then(|result| result.as_ref().ok())
                .map(Vec::len),
            Some(2)
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pooled_uring_copies_eight_files_with_matching_digests()
    -> Result<(), Box<dyn std::error::Error>> {
        if super::probe_io_uring().is_err() {
            return Ok(());
        }
        let temp = TempDir::new("eight-files")?;
        let io = uring_io();
        let tasks = (0_u8..8)
            .map(|index| {
                let io = io.clone();
                let path = temp.path().join(format!("file-{index}.bin"));
                tokio::spawn(async move {
                    let expected = (0..257_usize)
                        .map(|position| index.wrapping_add(position.to_le_bytes()[0]))
                        .collect::<Vec<_>>();
                    let file = tokio::fs::OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .read(true)
                        .write(true)
                        .open(&path)
                        .await?;
                    let file = io.attach(file).await?;
                    for (chunk, data) in expected.chunks(37).enumerate() {
                        io.write_at(&file, (chunk * 37) as u64, Bytes::copy_from_slice(data))
                            .await?;
                    }
                    io.sync_all(&file).await?;
                    let actual = io.read_at(&file, 0, 512).await?;
                    if blake3::hash(&actual) != blake3::hash(&expected) {
                        return Err(crate::error::StorageError::OperationError(
                            "pooled copy digest mismatch".into(),
                        ));
                    }
                    Ok::<_, crate::error::StorageError>(())
                })
            })
            .collect::<Vec<_>>();
        for task in tasks {
            task.await??;
        }
        Ok(())
    }

    #[test]
    fn parses_engine_modes_and_defaults_to_auto() {
        assert_eq!(mode_from_env_value(None), Ok(LocalIoEngineMode::Auto));
        assert_eq!(
            LocalIoEngineMode::parse(" AuTo "),
            Ok(LocalIoEngineMode::Auto)
        );
        assert_eq!(
            LocalIoEngineMode::parse("URING"),
            Ok(LocalIoEngineMode::Uring)
        );
        assert_eq!(
            LocalIoEngineMode::parse("blocking"),
            Ok(LocalIoEngineMode::Blocking)
        );
        let Err(error) = LocalIoEngineMode::parse("threads") else {
            panic!("invalid engine mode unexpectedly parsed");
        };
        assert!(error.contains("DATA_MOVER_LOCAL_IO_ENGINE"));
        assert!(error.contains("auto, uring, or blocking"));
    }

    #[test]
    fn parses_kernel_releases_numerically_without_panicking() {
        assert_eq!(
            KernelVersion::parse("5.10"),
            Some(KernelVersion::new(5, 10, 0))
        );
        assert_eq!(
            KernelVersion::parse("6.8.0-101-generic"),
            Some(KernelVersion::new(6, 8, 0))
        );
        assert_eq!(
            KernelVersion::parse("5.9.99"),
            Some(KernelVersion::new(5, 9, 99))
        );
        assert!(KernelVersion::new(5, 10, 0) > KernelVersion::new(5, 9, 99));
        assert_eq!(KernelVersion::parse("linux-6.8"), None);
        assert_eq!(KernelVersion::parse("6"), None);
        assert_eq!(KernelVersion::parse("18446744073709551616.1.0"), None);
    }

    #[derive(Clone, Copy)]
    enum FakeProbe {
        Success,
        SetupDenied,
        MissingWrite,
    }

    struct FakeDetector {
        release: Option<&'static str>,
        probe: FakeProbe,
        release_calls: Cell<usize>,
        probe_calls: Cell<usize>,
    }

    impl FakeDetector {
        fn new(release: Option<&'static str>, probe: FakeProbe) -> Self {
            Self {
                release,
                probe,
                release_calls: Cell::new(0),
                probe_calls: Cell::new(0),
            }
        }
    }

    impl CapabilityDetector for FakeDetector {
        fn kernel_release(&self) -> std::io::Result<String> {
            self.release_calls.set(self.release_calls.get() + 1);
            self.release.map(str::to_string).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "release unavailable")
            })
        }

        fn probe(&self) -> std::result::Result<(), ProbeError> {
            self.probe_calls.set(self.probe_calls.get() + 1);
            match self.probe {
                FakeProbe::Success => Ok(()),
                FakeProbe::SetupDenied => Err(ProbeError::Setup(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "blocked by seccomp",
                ))),
                FakeProbe::MissingWrite => Err(ProbeError::MissingOpcodes(vec!["WRITE"])),
            }
        }
    }

    fn assert_blocking(selection: &EngineSelection, reason: &str) {
        assert_eq!(selection.engine, SelectedEngine::Blocking);
        assert!(selection.error.is_none());
        assert!(
            selection
                .reason
                .as_deref()
                .is_some_and(|value| value.contains(reason))
        );
    }

    #[test]
    fn blocking_and_old_auto_modes_short_circuit_probe() {
        let blocking = FakeDetector::new(Some("6.8.0"), FakeProbe::Success);
        assert_blocking(
            &select_engine(&LocalIoEngineMode::Blocking, &blocking),
            "explicit",
        );
        assert_eq!(blocking.release_calls.get(), 0);
        assert_eq!(blocking.probe_calls.get(), 0);

        let old = FakeDetector::new(Some("5.9.16"), FakeProbe::Success);
        assert_blocking(&select_engine(&LocalIoEngineMode::Auto, &old), "below");
        assert_eq!(old.release_calls.get(), 1);
        assert_eq!(old.probe_calls.get(), 0);
    }

    #[test]
    fn auto_uses_real_probe_when_release_is_unparseable() {
        let detector = FakeDetector::new(Some("vendor-kernel"), FakeProbe::Success);
        let selection = select_engine(&LocalIoEngineMode::Auto, &detector);
        assert_eq!(selection.mode, LocalIoEngineMode::Auto);
        assert_eq!(selection.engine, SelectedEngine::Uring);
        assert_eq!(selection.kernel_version, None);
        assert_eq!(detector.probe_calls.get(), 1);
    }

    #[test]
    fn auto_falls_back_but_forced_uring_reports_probe_failures() {
        for probe in [FakeProbe::SetupDenied, FakeProbe::MissingWrite] {
            let auto = select_engine(
                &LocalIoEngineMode::Auto,
                &FakeDetector::new(Some("6.8.0"), probe),
            );
            assert_blocking(
                &auto,
                if matches!(probe, FakeProbe::SetupDenied) {
                    "seccomp"
                } else {
                    "WRITE"
                },
            );

            let forced = select_engine(
                &LocalIoEngineMode::Uring,
                &FakeDetector::new(Some("4.19.0-backport"), probe),
            );
            assert_eq!(forced.engine, SelectedEngine::Blocking);
            assert!(
                forced
                    .error
                    .as_deref()
                    .is_some_and(|error| error.contains("forced io_uring"))
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn real_linux_probe_reports_capability_without_failing_restricted_ci() {
        let detector = super::SystemCapabilityDetector;
        match detector.probe() {
            Ok(()) => {}
            Err(ProbeError::Setup(error)) => eprintln!("io_uring capability unavailable: {error}"),
            Err(ProbeError::MissingOpcodes(opcodes)) => {
                eprintln!("io_uring required opcodes unavailable: {opcodes:?}");
            }
        }
    }

    #[test]
    fn blocking_loops_continue_after_short_io() -> Result<(), Box<dyn std::error::Error>> {
        let source = b"abcdefgh";
        let mut destination = [0_u8; 8];
        let mut read_calls = 0usize;
        let filled = read_fully_at(&mut destination, 9, |remaining, position| {
            let source_offset = usize::try_from(position - 9)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
            let count = remaining.len().min(3);
            remaining[..count].copy_from_slice(&source[source_offset..source_offset + count]);
            read_calls += 1;
            Ok(count)
        })?;
        assert_eq!(filled, source.len());
        assert_eq!(destination, *source);
        assert_eq!(read_calls, 3);

        let mut output = Vec::new();
        let mut write_calls = 0usize;
        let written = write_fully_at(source, 20, |remaining, position| {
            assert_eq!(position, 20 + output.len() as u64);
            let count = remaining.len().min(3);
            output.extend_from_slice(&remaining[..count]);
            write_calls += 1;
            Ok(count)
        })?;
        assert_eq!(written, source.len());
        assert_eq!(output, source);
        assert_eq!(write_calls, 3);
        Ok(())
    }

    #[test]
    fn blocking_write_zero_and_offset_overflow_keep_io_error_kinds() {
        let Err(write_zero) = write_fully_at(b"x", 0, |_, _| Ok(0)) else {
            panic!("zero write must fail");
        };
        assert_eq!(write_zero.kind(), std::io::ErrorKind::WriteZero);

        let mut read_buffer = [0_u8; 2];
        let mut first_read = true;
        let Err(read_overflow) = read_fully_at(&mut read_buffer, u64::MAX, |remaining, _| {
            if first_read {
                remaining[0] = b'x';
                first_read = false;
                Ok(1)
            } else {
                Ok(0)
            }
        }) else {
            panic!("second read position must overflow");
        };
        assert_eq!(read_overflow.kind(), std::io::ErrorKind::InvalidInput);

        let mut first_write = true;
        let Err(write_overflow) = write_fully_at(b"xy", u64::MAX, |_, _| {
            if first_write {
                first_write = false;
                Ok(1)
            } else {
                Ok(0)
            }
        }) else {
            panic!("second write position must overflow");
        };
        assert_eq!(write_overflow.kind(), std::io::ErrorKind::InvalidInput);
    }
}
