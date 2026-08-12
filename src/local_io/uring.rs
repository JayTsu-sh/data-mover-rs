use std::sync::Arc;

use bytes::Bytes;

use super::{
    AttachedUringFile, BlockingLocalDataIo, LocalFsPoolConfig, LocalIoDirection, LocalIoEngineMode,
    LocalIoFile, UringFileTokens,
};
use crate::Result;
use crate::error::StorageError;

#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
pub(super) enum PoolDirection {
    Read,
    Write,
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(super) struct LocalFsIoPool {
    pub(super) device: u64,
    config: LocalFsPoolConfig,
    pub(super) read: std::sync::OnceLock<std::result::Result<Vec<UringWorker>, String>>,
    pub(super) write: std::sync::OnceLock<std::result::Result<Vec<UringWorker>, String>>,
    state: std::sync::atomic::AtomicU8,
    inflight: std::sync::atomic::AtomicUsize,
    changed: tokio::sync::Notify,
    pub(super) transitions: std::sync::atomic::AtomicUsize,
    pub(super) read_requests: Arc<tokio::sync::Semaphore>,
    pub(super) write_requests: Arc<tokio::sync::Semaphore>,
    pub(super) buffered_bytes: Arc<tokio::sync::Semaphore>,
    metrics: PoolMetrics,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum PoolState {
    Uring = 0,
    FallingBack = 1,
    Blocking = 2,
    Failed = 3,
}

#[cfg(target_os = "linux")]
impl LocalFsIoPool {
    pub(super) fn new(device: u64, config: LocalFsPoolConfig) -> Self {
        debug_assert!(config.validate().is_ok());
        Self {
            device,
            config,
            read: std::sync::OnceLock::new(),
            write: std::sync::OnceLock::new(),
            state: std::sync::atomic::AtomicU8::new(PoolState::Uring as u8),
            inflight: std::sync::atomic::AtomicUsize::new(0),
            changed: tokio::sync::Notify::new(),
            transitions: std::sync::atomic::AtomicUsize::new(0),
            read_requests: Arc::new(tokio::sync::Semaphore::new(config.read_requests)),
            write_requests: Arc::new(tokio::sync::Semaphore::new(config.write_requests)),
            buffered_bytes: Arc::new(tokio::sync::Semaphore::new(config.buffered_bytes)),
            metrics: PoolMetrics::default(),
        }
    }

    pub(super) fn record(
        &self,
        operation: PoolOperation,
        bytes: usize,
        failed: bool,
        started: std::time::Instant,
    ) {
        let index = operation as usize;
        self.metrics.requests[index].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics.bytes[index].fetch_add(bytes as u64, std::sync::atomic::Ordering::Relaxed);
        if failed {
            self.metrics.errors[index].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.metrics.latency_ns[index].fetch_add(
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn snapshot(&self) -> PoolMetricsSnapshot {
        PoolMetricsSnapshot {
            requests: std::array::from_fn(|i| {
                self.metrics.requests[i].load(std::sync::atomic::Ordering::Relaxed)
            }),
            bytes: std::array::from_fn(|i| {
                self.metrics.bytes[i].load(std::sync::atomic::Ordering::Relaxed)
            }),
            errors: std::array::from_fn(|i| {
                self.metrics.errors[i].load(std::sync::atomic::Ordering::Relaxed)
            }),
            latency_ns: std::array::from_fn(|i| {
                self.metrics.latency_ns[i].load(std::sync::atomic::Ordering::Relaxed)
            }),
            current_inflight: self.inflight.load(std::sync::atomic::Ordering::Acquire),
            peak_inflight: self
                .metrics
                .peak_inflight
                .load(std::sync::atomic::Ordering::Relaxed),
            transitions: self.transitions.load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    pub(super) async fn acquire_read_budget(
        &self,
        count: u64,
    ) -> std::io::Result<(
        tokio::sync::OwnedSemaphorePermit,
        Option<tokio::sync::OwnedSemaphorePermit>,
    )> {
        let bytes = usize::try_from(count).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "read size exceeds platform capacity",
            )
        })?;
        if bytes > self.config.buffered_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "read size {bytes} exceeds filesystem buffered-byte budget {}",
                    self.config.buffered_bytes
                ),
            ));
        }
        let request = Arc::clone(&self.read_requests)
            .acquire_owned()
            .await
            .map_err(|_| std::io::Error::other("read request budget closed"))?;
        let buffer = if bytes == 0 {
            None
        } else {
            let permits = u32::try_from(bytes).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "read size exceeds byte permit capacity",
                )
            })?;
            Some(
                Arc::clone(&self.buffered_bytes)
                    .acquire_many_owned(permits)
                    .await
                    .map_err(|_| std::io::Error::other("buffered-byte budget closed"))?,
            )
        };
        Ok((request, buffer))
    }

    pub(super) async fn acquire_write_budget(
        &self,
    ) -> std::io::Result<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.write_requests)
            .acquire_owned()
            .await
            .map_err(|_| std::io::Error::other("write request budget closed"))
    }

    pub(super) fn state(&self) -> PoolState {
        match self.state.load(std::sync::atomic::Ordering::Acquire) {
            0 => PoolState::Uring,
            1 => PoolState::FallingBack,
            2 => PoolState::Blocking,
            _ => PoolState::Failed,
        }
    }

    pub(super) async fn begin(self: &Arc<Self>) -> std::io::Result<Option<PoolRequestGuard>> {
        loop {
            match self.state() {
                PoolState::Uring => {
                    self.inflight
                        .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                    let current = self.inflight.load(std::sync::atomic::Ordering::Acquire);
                    self.metrics
                        .peak_inflight
                        .fetch_max(current, std::sync::atomic::Ordering::Relaxed);
                    if self.state() == PoolState::Uring {
                        return Ok(Some(PoolRequestGuard(Arc::clone(self))));
                    }
                    self.finish_request();
                }
                PoolState::FallingBack => self.changed.notified().await,
                PoolState::Blocking => return Ok(None),
                PoolState::Failed => {
                    return Err(std::io::Error::other("io_uring filesystem pool failed"));
                }
            }
        }
    }

    fn finish_request(&self) {
        if self
            .inflight
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel)
            == 1
        {
            self.changed.notify_waiters();
        }
    }

    pub(super) async fn fallback(&self) {
        if self
            .state
            .compare_exchange(
                PoolState::Uring as u8,
                PoolState::FallingBack as u8,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
        {
            self.transitions
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::warn!(
                device = self.device,
                state = "falling_back",
                "local io_uring pool fallback started"
            );
            while self.inflight.load(std::sync::atomic::Ordering::Acquire) != 0 {
                self.changed.notified().await;
            }
            self.state.store(
                PoolState::Blocking as u8,
                std::sync::atomic::Ordering::Release,
            );
            self.changed.notify_waiters();
            tracing::warn!(
                device = self.device,
                state = "blocking",
                "local io_uring pool fallback completed"
            );
        } else {
            while self.state() == PoolState::FallingBack {
                self.changed.notified().await;
            }
        }
    }

    pub(super) fn fail(&self) {
        let previous = self
            .state
            .swap(PoolState::Failed as u8, std::sync::atomic::Ordering::AcqRel);
        if previous != PoolState::Failed as u8 {
            tracing::error!(
                device = self.device,
                state = "failed",
                replay_uncertain_write = false,
                "local io_uring pool entered failed state"
            );
            self.changed.notify_waiters();
        }
    }

    pub(super) fn worker(
        &self,
        direction: PoolDirection,
        device: u64,
        inode: u64,
    ) -> std::io::Result<UringWorker> {
        let (workers, count, label) = match direction {
            PoolDirection::Read => (&self.read, self.config.read_rings, "read"),
            PoolDirection::Write => (&self.write, self.config.write_rings, "write"),
        };
        let workers = workers.get_or_init(|| {
            spawn_worker_group(self.device, label, count, self.config.ring_entries)
        });
        let workers = workers
            .as_ref()
            .map_err(|error| std::io::Error::other(error.clone()))?;
        let index = affinity_index(device, inode, workers.len());
        Ok(workers[index].clone())
    }
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
#[repr(usize)]
pub(super) enum PoolOperation {
    Read = 0,
    Write = 1,
    Fsync = 2,
}

#[cfg(target_os = "linux")]
#[derive(Default, Debug)]
struct PoolMetrics {
    requests: [std::sync::atomic::AtomicU64; 3],
    bytes: [std::sync::atomic::AtomicU64; 3],
    errors: [std::sync::atomic::AtomicU64; 3],
    latency_ns: [std::sync::atomic::AtomicU64; 3],
    peak_inflight: std::sync::atomic::AtomicUsize,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(super) struct PoolMetricsSnapshot {
    pub(super) requests: [u64; 3],
    pub(super) bytes: [u64; 3],
    pub(super) errors: [u64; 3],
    pub(super) latency_ns: [u64; 3],
    pub(super) current_inflight: usize,
    pub(super) peak_inflight: usize,
    pub(super) transitions: usize,
}

#[cfg(target_os = "linux")]
pub(super) struct PoolRequestGuard(Arc<LocalFsIoPool>);

#[cfg(target_os = "linux")]
impl Drop for PoolRequestGuard {
    fn drop(&mut self) {
        self.0.finish_request();
    }
}

#[cfg(target_os = "linux")]
fn spawn_worker_group(
    device: u64,
    direction: &str,
    count: usize,
    ring_entries: u32,
) -> std::result::Result<Vec<UringWorker>, String> {
    (0..count)
        .map(|index| {
            UringWorker::spawn(
                &format!("data-mover-uring-{device}-{direction}-{index}"),
                ring_entries,
            )
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
pub(super) struct LocalFsRegistry {
    pools: parking_lot::Mutex<std::collections::HashMap<u64, std::sync::Weak<LocalFsIoPool>>>,
}

#[cfg(target_os = "linux")]
impl LocalFsRegistry {
    pub(super) fn resolve(
        &self,
        device: u64,
        config: LocalFsPoolConfig,
    ) -> std::io::Result<Arc<LocalFsIoPool>> {
        let mut pools = self.pools.lock();
        if let Some(pool) = pools.get(&device).and_then(std::sync::Weak::upgrade) {
            if pool.config != config {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("conflicting local I/O configuration for filesystem device {device}"),
                ));
            }
            return Ok(pool);
        }
        let pool = Arc::new(LocalFsIoPool::new(device, config));
        pools.insert(device, Arc::downgrade(&pool));
        tracing::info!(
            device,
            read_rings = config.read_rings,
            write_rings = config.write_rings,
            ring_entries = config.ring_entries,
            read_requests = config.read_requests,
            write_requests = config.write_requests,
            buffered_bytes = config.buffered_bytes,
            "created local io_uring filesystem pool"
        );
        Ok(pool)
    }
}

#[cfg(target_os = "linux")]
fn global_fs_registry() -> &'static LocalFsRegistry {
    static REGISTRY: std::sync::OnceLock<LocalFsRegistry> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(LocalFsRegistry::default)
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
pub(super) struct UringLocalDataIo {
    mode: LocalIoEngineMode,
    config: LocalFsPoolConfig,
    pools: Arc<parking_lot::Mutex<std::collections::HashMap<u64, Arc<LocalFsIoPool>>>>,
}

#[cfg(target_os = "linux")]
impl UringLocalDataIo {
    pub(super) fn new(mode: LocalIoEngineMode, config: LocalFsPoolConfig) -> Self {
        Self {
            mode,
            pools: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
            config,
        }
    }

    pub(super) async fn attach(
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
            let pool = global_fs_registry().resolve(device, self.config)?;
            self.pools.lock().insert(device, Arc::clone(&pool));
            pool
        };
        loop {
            match pool.state() {
                PoolState::Blocking => {
                    return Ok(LocalIoFile {
                        inner: Arc::new(standard),
                        uring: None,
                    });
                }
                PoolState::Failed => {
                    return Err(StorageError::OperationError(
                        "io_uring filesystem pool is in failed state".to_string(),
                    ));
                }
                PoolState::FallingBack => pool.changed.notified().await,
                PoolState::Uring => break,
            }
        }
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
            pool: Arc::clone(pool),
        })
    }

    pub(super) async fn read_at(
        &self,
        file: &LocalIoFile,
        offset: u64,
        count: u64,
    ) -> Result<Bytes> {
        let Some(tokens) = file.uring.as_ref() else {
            return BlockingLocalDataIo.read_at(file, offset, count).await;
        };
        let Some(attached) = tokens.read.as_ref() else {
            return BlockingLocalDataIo.read_at(file, offset, count).await;
        };
        let (_request_permit, buffer_permit) = tokens.pool.acquire_read_budget(count).await?;
        let Some(guard) = tokens.pool.begin().await? else {
            let bytes = BlockingLocalDataIo.read_at(file, offset, count).await?;
            return Ok(with_byte_budget(bytes, buffer_permit));
        };
        let started = std::time::Instant::now();
        let result = attached.worker.read(attached.token, offset, count).await;
        drop(guard);
        tokens.pool.record(
            PoolOperation::Read,
            result.as_ref().map_or(0, Bytes::len),
            result.is_err(),
            started,
        );
        match result {
            Ok(bytes) => Ok(with_byte_budget(bytes, buffer_permit)),
            Err(error) if is_explicitly_unsupported(&error) => {
                tokens.pool.fallback().await;
                let bytes = BlockingLocalDataIo.read_at(file, offset, count).await?;
                Ok(with_byte_budget(bytes, buffer_permit))
            }
            Err(error) if is_uncertain_worker_error(&error) => {
                tokens.pool.fail();
                Err(StorageError::OperationError(format!(
                    "io_uring read completion is uncertain: {error}"
                )))
            }
            Err(error) => Err(error.into()),
        }
    }

    pub(super) async fn write_at(
        &self,
        file: &LocalIoFile,
        offset: u64,
        data: Bytes,
    ) -> Result<usize> {
        let Some(tokens) = file.uring.as_ref() else {
            return BlockingLocalDataIo.write_at(file, offset, data).await;
        };
        let Some(attached) = tokens.write.as_ref() else {
            return BlockingLocalDataIo.write_at(file, offset, data).await;
        };
        let _request_permit = tokens.pool.acquire_write_budget().await?;
        let Some(guard) = tokens.pool.begin().await? else {
            return BlockingLocalDataIo.write_at(file, offset, data).await;
        };
        let retry_data = data.clone();
        let started = std::time::Instant::now();
        let result = attached.worker.write(attached.token, offset, data).await;
        drop(guard);
        tokens.pool.record(
            PoolOperation::Write,
            result.as_ref().copied().unwrap_or(0),
            result.is_err(),
            started,
        );
        match result {
            Ok(written) => Ok(written),
            Err(error) if is_explicitly_unsupported(&error) => {
                tokens.pool.fallback().await;
                BlockingLocalDataIo.write_at(file, offset, retry_data).await
            }
            Err(error) if is_uncertain_worker_error(&error) => {
                tokens.pool.fail();
                Err(StorageError::OperationError(format!(
                    "io_uring write completion is uncertain; write was not replayed: {error}"
                )))
            }
            Err(error) => Err(error.into()),
        }
    }

    pub(super) async fn sync_all(&self, file: &LocalIoFile) -> Result<()> {
        let Some(tokens) = file.uring.as_ref() else {
            return BlockingLocalDataIo.sync_all(file).await;
        };
        let Some(attached) = tokens.write.as_ref() else {
            return BlockingLocalDataIo.sync_all(file).await;
        };
        let Some(guard) = tokens.pool.begin().await? else {
            return BlockingLocalDataIo.sync_all(file).await;
        };
        let started = std::time::Instant::now();
        let result = attached.worker.sync(attached.token).await;
        drop(guard);
        tokens
            .pool
            .record(PoolOperation::Fsync, 0, result.is_err(), started);
        match result {
            Ok(()) => Ok(()),
            Err(error) if is_explicitly_unsupported(&error) => {
                tokens.pool.fallback().await;
                BlockingLocalDataIo.sync_all(file).await
            }
            Err(error) if is_uncertain_worker_error(&error) => {
                tokens.pool.fail();
                Err(StorageError::OperationError(format!(
                    "io_uring fsync completion is uncertain: {error}"
                )))
            }
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(target_os = "linux")]
struct BudgetedBytes {
    data: Bytes,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

#[cfg(target_os = "linux")]
impl AsRef<[u8]> for BudgetedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

#[cfg(target_os = "linux")]
pub(super) fn with_byte_budget(
    bytes: Bytes,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> Bytes {
    match permit {
        Some(permit) => Bytes::from_owner(BudgetedBytes {
            data: bytes,
            _permit: permit,
        }),
        None => bytes,
    }
}

#[cfg(target_os = "linux")]
pub(super) fn is_explicitly_unsupported(error: &std::io::Error) -> bool {
    matches!(error.raw_os_error(), Some(38 | 95))
}

#[cfg(target_os = "linux")]
pub(super) fn is_uncertain_worker_error(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::BrokenPipe
}

#[cfg(target_os = "linux")]
pub(super) enum UringCommand {
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
pub(super) struct UringWorker {
    pub(super) sender: async_channel::Sender<UringCommand>,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) id: u64,
}

#[cfg(target_os = "linux")]
impl UringWorker {
    const CHANNEL_CAPACITY: usize = 64;

    fn spawn(name: &str, ring_entries: u32) -> std::io::Result<Self> {
        static NEXT_WORKER_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let (sender, receiver) = async_channel::bounded(Self::CHANNEL_CAPACITY);
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    tokio_uring::builder()
                        .entries(ring_entries)
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

    pub(super) async fn read(&self, token: u64, offset: u64, count: u64) -> std::io::Result<Bytes> {
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
