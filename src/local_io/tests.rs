use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use super::{
    BUFFER_MIB_ENV, CapabilityDetector, EngineSelection, KernelVersion, LocalDataIo,
    LocalIoConfigBuilder, LocalIoEngineMode, ProbeError, READ_REQUESTS_ENV, READ_RINGS_ENV,
    RING_ENTRIES_ENV, SelectedEngine, WRITE_REQUESTS_ENV, WRITE_RINGS_ENV, mode_from_env_value,
    read_fully_at, select_engine, write_fully_at,
};
use crate::error::StorageError;

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
            super::LocalFsPoolConfig::default(),
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
async fn blocking_adapter_satisfies_data_io_contract() -> Result<(), Box<dyn std::error::Error>> {
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
                write_rings: count,
                ..LocalFsPoolConfig::default()
            }
            .validate()
            .is_ok()
        );
    }
    for count in [0, 5] {
        assert!(
            LocalFsPoolConfig {
                read_rings: count,
                write_rings: 2,
                ..LocalFsPoolConfig::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            LocalFsPoolConfig {
                read_rings: 2,
                write_rings: count,
                ..LocalFsPoolConfig::default()
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
                .unwrap_or_else(|error| panic!("registry resolve failed: {error}"))
        })
        .collect::<Vec<_>>();
    assert!(pools.iter().all(|pool| Arc::ptr_eq(&pools[0], pool)));
    let different = registry
        .resolve(43, LocalFsPoolConfig::default())
        .unwrap_or_else(|error| panic!("registry resolve failed: {error}"));
    assert!(!Arc::ptr_eq(&pools[0], &different));

    let weak = Arc::downgrade(&pools[0]);
    drop(pools);
    assert!(weak.upgrade().is_none());
    let rebuilt = registry
        .resolve(42, LocalFsPoolConfig::default())
        .unwrap_or_else(|error| panic!("registry resolve failed: {error}"));
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

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_fallback_drains_inflight_and_transitions_once() -> std::io::Result<()> {
    use super::{LocalFsIoPool, LocalFsPoolConfig, PoolState};

    let pool = Arc::new(LocalFsIoPool::new(99, LocalFsPoolConfig::default()));
    let first = pool
        .begin()
        .await?
        .unwrap_or_else(|| panic!("pool unexpectedly blocking"));
    let second = pool
        .begin()
        .await?
        .unwrap_or_else(|| panic!("pool unexpectedly blocking"));
    let leader_pool = Arc::clone(&pool);
    let leader = tokio::spawn(async move { leader_pool.fallback().await });
    while pool.state() != PoolState::FallingBack {
        tokio::task::yield_now().await;
    }
    let follower_pool = Arc::clone(&pool);
    let follower = tokio::spawn(async move { follower_pool.fallback().await });
    let waiter_pool = Arc::clone(&pool);
    let waiter = tokio::spawn(async move { waiter_pool.begin().await });
    tokio::task::yield_now().await;
    assert!(!leader.is_finished());
    assert!(!waiter.is_finished());

    drop(first);
    drop(second);
    leader.await.map_err(std::io::Error::other)?;
    follower.await.map_err(std::io::Error::other)?;
    assert!(waiter.await.map_err(std::io::Error::other)??.is_none());
    assert_eq!(pool.state(), PoolState::Blocking);
    assert_eq!(pool.transitions.load(Ordering::Relaxed), 1);
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn failed_pool_rejects_new_requests() {
    use super::{LocalFsIoPool, LocalFsPoolConfig, PoolState};

    let pool = Arc::new(LocalFsIoPool::new(100, LocalFsPoolConfig::default()));
    pool.fail();
    assert_eq!(pool.state(), PoolState::Failed);
    let error =
        pool.begin().await.err().unwrap_or_else(|| {
            std::io::Error::other("failed pool unexpectedly accepted a request")
        });
    assert_eq!(error.kind(), std::io::ErrorKind::Other);
}

#[cfg(target_os = "linux")]
#[test]
fn fallback_error_classification_is_narrow() {
    for errno in [38, 95] {
        assert!(super::is_explicitly_unsupported(
            &std::io::Error::from_raw_os_error(errno)
        ));
    }
    for errno in [2, 5, 13, 22, 28] {
        assert!(!super::is_explicitly_unsupported(
            &std::io::Error::from_raw_os_error(errno)
        ));
    }
    assert!(super::is_uncertain_worker_error(&std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "worker stopped",
    )));
    assert!(!super::is_uncertain_worker_error(
        &std::io::Error::from_raw_os_error(95)
    ));
}

#[cfg(target_os = "linux")]
fn small_budget_config() -> super::LocalFsPoolConfig {
    super::LocalFsPoolConfig {
        read_requests: 2,
        write_requests: 3,
        buffered_bytes: 8,
        ..super::LocalFsPoolConfig::default()
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn byte_permit_follows_bytes_clones_until_last_drop() -> std::io::Result<()> {
    let pool = Arc::new(super::LocalFsIoPool::new(200, small_budget_config()));
    let (request, permit) = pool.acquire_read_budget(6).await?;
    assert_eq!(pool.read_requests.available_permits(), 1);
    assert_eq!(pool.buffered_bytes.available_permits(), 2);
    drop(request);
    let bytes = super::with_byte_budget(Bytes::from_static(b"abcdef"), permit);
    let clone = bytes.clone();
    drop(bytes);
    assert_eq!(pool.buffered_bytes.available_permits(), 2);
    drop(clone);
    assert_eq!(pool.buffered_bytes.available_permits(), 8);
    assert_eq!(pool.read_requests.available_permits(), 2);
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn shared_request_budgets_bound_concurrency_and_return_to_zero_usage() -> std::io::Result<()>
{
    let pool = Arc::new(super::LocalFsIoPool::new(201, small_budget_config()));
    let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..12 {
        let pool = Arc::clone(&pool);
        let active = Arc::clone(&active);
        let peak = Arc::clone(&peak);
        tasks.push(tokio::spawn(async move {
            let (request, buffer) = pool.acquire_read_budget(4).await?;
            let now = active.fetch_add(1, Ordering::AcqRel) + 1;
            peak.fetch_max(now, Ordering::AcqRel);
            tokio::task::yield_now().await;
            active.fetch_sub(1, Ordering::AcqRel);
            drop(buffer);
            drop(request);
            Ok::<_, std::io::Error>(())
        }));
    }
    for task in tasks {
        task.await.map_err(std::io::Error::other)??;
    }
    assert!(peak.load(Ordering::Acquire) <= 2);
    assert_eq!(pool.read_requests.available_permits(), 2);
    assert_eq!(pool.buffered_bytes.available_permits(), 8);

    let permits = futures::future::join_all((0..3).map(|_| pool.acquire_write_budget()))
        .await
        .into_iter()
        .collect::<std::io::Result<Vec<_>>>()?;
    assert_eq!(pool.write_requests.available_permits(), 0);
    drop(permits);
    assert_eq!(pool.write_requests.available_permits(), 3);
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn zero_and_oversized_reads_handle_byte_budget_without_waiting() -> std::io::Result<()> {
    let pool = Arc::new(super::LocalFsIoPool::new(202, small_budget_config()));
    let (request, bytes) = pool.acquire_read_budget(0).await?;
    assert!(bytes.is_none());
    assert_eq!(pool.buffered_bytes.available_permits(), 8);
    drop(request);

    let error =
        pool.acquire_read_budget(9).await.err().unwrap_or_else(|| {
            std::io::Error::other("oversized read unexpectedly acquired budget")
        });
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(pool.read_requests.available_permits(), 2);
    assert_eq!(pool.buffered_bytes.available_permits(), 8);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn budget_config_rejects_zero_and_excessive_values() {
    for config in [
        super::LocalFsPoolConfig {
            read_requests: 0,
            ..small_budget_config()
        },
        super::LocalFsPoolConfig {
            write_requests: 0,
            ..small_budget_config()
        },
        super::LocalFsPoolConfig {
            buffered_bytes: 0,
            ..small_budget_config()
        },
        super::LocalFsPoolConfig {
            buffered_bytes: 512 * 1024 * 1024 + 1,
            ..small_budget_config()
        },
    ] {
        assert!(config.validate().is_err());
    }
}

#[test]
fn public_config_defaults_and_partial_explicit_precedence() {
    let values = std::collections::HashMap::from([
        (READ_RINGS_ENV, "4".to_string()),
        (WRITE_RINGS_ENV, "3".to_string()),
        (RING_ENTRIES_ENV, "128".to_string()),
        (READ_REQUESTS_ENV, "40".to_string()),
        (WRITE_REQUESTS_ENV, "70".to_string()),
        (BUFFER_MIB_ENV, "512".to_string()),
    ]);
    let config = LocalIoConfigBuilder::new()
        .read_rings(1)
        .build_with(|name| values.get(name).cloned())
        .unwrap_or_else(|error| panic!("config failed: {error}"));
    assert_eq!(config.read_rings(), 1);
    assert_eq!(config.write_rings(), 3);
    assert_eq!(config.ring_entries(), 128);
    assert_eq!(config.read_requests(), 40);
    assert_eq!(config.write_requests(), 70);
    assert_eq!(config.buffered_bytes(), 512 * 1024 * 1024);
}

#[test]
fn public_config_rejects_invalid_env_and_ring_entries() {
    let error = LocalIoConfigBuilder::new()
        .build_with(|name| (name == READ_RINGS_ENV).then(|| "many".to_string()))
        .err()
        .unwrap_or_else(|| StorageError::ConfigError("unexpected success".into()));
    assert!(error.to_string().contains(READ_RINGS_ENV));
    for entries in [0, 7, 12, 8192] {
        assert!(
            LocalIoConfigBuilder::new()
                .ring_entries(entries)
                .build_with(|_| None)
                .is_err()
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn registry_rejects_conflicting_live_pool_configuration() {
    let registry = super::LocalFsRegistry::default();
    let first = registry
        .resolve(300, super::LocalFsPoolConfig::default())
        .unwrap_or_else(|error| panic!("first resolve failed: {error}"));
    let conflict = super::LocalFsPoolConfig {
        read_rings: 4,
        ..super::LocalFsPoolConfig::default()
    };
    let error = registry
        .resolve(300, conflict)
        .err()
        .unwrap_or_else(|| std::io::Error::other("conflict unexpectedly succeeded"));
    assert!(error.to_string().contains("device 300"));
    drop(first);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn metrics_snapshot_counts_operations_without_double_counting() -> std::io::Result<()> {
    use super::{LocalFsIoPool, LocalFsPoolConfig, PoolOperation};

    let pool = Arc::new(LocalFsIoPool::new(301, LocalFsPoolConfig::default()));
    let first = pool
        .begin()
        .await?
        .unwrap_or_else(|| panic!("unexpected blocking pool"));
    let second = pool
        .begin()
        .await?
        .unwrap_or_else(|| panic!("unexpected blocking pool"));
    assert_eq!(pool.snapshot().peak_inflight, 2);
    drop(first);
    drop(second);
    pool.record(PoolOperation::Read, 11, false, std::time::Instant::now());
    pool.record(PoolOperation::Write, 7, true, std::time::Instant::now());
    pool.record(PoolOperation::Fsync, 0, false, std::time::Instant::now());
    let snapshot = pool.snapshot();
    assert_eq!(snapshot.requests, [1, 1, 1]);
    assert_eq!(snapshot.bytes, [11, 7, 0]);
    assert_eq!(snapshot.errors, [0, 1, 0]);
    assert_eq!(snapshot.current_inflight, 0);
    assert!(snapshot.latency_ns.iter().all(|value| *value > 0));
    assert_eq!(snapshot.transitions, 0);
    Ok(())
}

#[test]
fn parses_engine_modes_and_defaults_to_blocking() {
    assert_eq!(mode_from_env_value(None), Ok(LocalIoEngineMode::Blocking));
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
        self.release
            .map(str::to_string)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "release unavailable"))
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
