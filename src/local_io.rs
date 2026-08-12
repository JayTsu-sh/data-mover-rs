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
    adapter: BlockingLocalDataIo,
    selection: Arc<EngineSelection>,
}

impl LocalDataIo {
    pub(crate) async fn attach(&self, file: tokio::fs::File) -> Result<LocalIoFile> {
        if let Some(error) = &self.selection.error {
            return Err(StorageError::ConfigError(error.clone()));
        }
        Ok(self.adapter.attach(file).await)
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
        Self {
            adapter: BlockingLocalDataIo,
            selection: Arc::new(selection),
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
}

#[derive(Clone, Copy, Debug, Default)]
struct BlockingLocalDataIo;

impl BlockingLocalDataIo {
    async fn attach(self, file: tokio::fs::File) -> LocalIoFile {
        LocalIoFile {
            inner: Arc::new(file.into_std().await),
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
    use std::sync::atomic::{AtomicU64, Ordering};

    use bytes::Bytes;

    use super::{
        CapabilityDetector, EngineSelection, KernelVersion, LocalDataIo, LocalIoEngineMode,
        ProbeError, SelectedEngine, mode_from_env_value, read_fully_at, select_engine,
        write_fully_at,
    };

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

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
