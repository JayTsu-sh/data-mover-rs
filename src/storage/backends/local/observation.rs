use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(test)]
use std::{collections::HashMap, time::Duration};

use cap_std::ambient_authority;
use cap_std::fs::{Dir, Metadata};

use crate::model::{
    BackendIdentity, BackendSessionFailure, EntryKind, EntryOperationFailure, FailureClass,
    IdentityStrength, ObservedEntry, Operation, SourceIdentity, SpecialFileKind, StoragePath,
    StorageTimestamp, SymlinkTarget, SymlinkTargetEncoding, TimePrecision, Transience,
};
use crate::storage::StorageRoleFailure;

pub(crate) struct LocalObservationAdapter {
    root: Arc<Dir>,
    identity: BackendIdentity,
    #[cfg(test)]
    probe: Arc<ObservationProbe>,
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct ObservationProbe {
    first_delayed: std::sync::atomic::AtomicBool,
    started: std::sync::Mutex<Vec<String>>,
    completed: std::sync::Mutex<Vec<String>>,
    delays: std::sync::Mutex<HashMap<String, Duration>>,
}

#[cfg(test)]
impl ObservationProbe {
    async fn enter(self: &Arc<Self>, path: &StoragePath) -> ObservationProbeGuard {
        let path = path.as_str().to_owned();
        self.started
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(path.clone());
        let configured = self
            .delays
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&path)
            .copied();
        let first = self
            .first_delayed
            .swap(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(delay) = configured.or((!first).then_some(Duration::from_millis(60))) {
            tokio::time::sleep(delay).await;
        }
        ObservationProbeGuard {
            probe: Arc::clone(self),
            path,
        }
    }

    pub(super) fn orders(&self) -> (Vec<String>, Vec<String>) {
        let started = self
            .started
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let completed = self
            .completed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        (started, completed)
    }
}

#[cfg(test)]
struct ObservationProbeGuard {
    probe: Arc<ObservationProbe>,
    path: String,
}

#[cfg(test)]
impl Drop for ObservationProbeGuard {
    fn drop(&mut self) {
        self.probe
            .completed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(self.path.clone());
    }
}

impl LocalObservationAdapter {
    pub(crate) fn new(
        root: impl AsRef<Path>,
        identity: BackendIdentity,
    ) -> Result<Self, StorageRoleFailure> {
        let root = Dir::open_ambient_dir(root, ambient_authority())
            .map_err(|error| session_io_failure(&error))?;
        Ok(Self {
            root: Arc::new(root),
            identity,
            #[cfg(test)]
            probe: Arc::new(ObservationProbe::default()),
        })
    }

    pub(crate) fn from_root(root: Arc<Dir>, identity: BackendIdentity) -> Self {
        Self {
            root,
            identity,
            #[cfg(test)]
            probe: Arc::new(ObservationProbe::default()),
        }
    }

    #[cfg(test)]
    pub(crate) fn probe_orders(&self) -> (Vec<String>, Vec<String>) {
        self.probe.orders()
    }

    pub(crate) async fn observe(
        &self,
        path: StoragePath,
    ) -> Result<ObservedEntry, StorageRoleFailure> {
        #[cfg(test)]
        let _probe = self.probe.enter(&path).await;
        let relative = checked_relative(&path)?;
        let root = Arc::clone(&self.root);
        let metadata = tokio::task::spawn_blocking(move || root.symlink_metadata(relative))
            .await
            .map_err(|_| failure(&path, FailureClass::Internal))?
            .map_err(|error| observation_io_failure(&path, &error))?;
        let kind =
            entry_kind(&metadata).ok_or_else(|| failure(&path, FailureClass::Unsupported))?;
        let size = (kind == EntryKind::File).then_some(metadata.len());
        let modified = metadata
            .modified()
            .ok()
            .and_then(|time| system_time_to_timestamp(time.into_std()));
        let source_identity = source_identity(&self.identity, &path, &metadata)?;
        let facts = backend_facts(&metadata);
        let target = self.symlink_target(&path, kind).await?;
        let observed = match target {
            Some(target) => ObservedEntry::new_symlink(path, modified, source_identity, target),
            None => ObservedEntry::new(path, kind, size, modified, source_identity),
        }
        .map_err(|_| failure(&StoragePath::root(), FailureClass::Internal))?;
        observed
            .with_backend_fact_bytes(facts)
            .map_err(|_| failure(&StoragePath::root(), FailureClass::Internal))
    }

    async fn symlink_target(
        &self,
        path: &StoragePath,
        kind: EntryKind,
    ) -> Result<Option<SymlinkTarget>, StorageRoleFailure> {
        if kind != EntryKind::Symlink {
            return Ok(None);
        }
        let relative = checked_relative(path)?;
        let root = Arc::clone(&self.root);
        let target = tokio::task::spawn_blocking(move || root.read_link(relative))
            .await
            .map_err(|_| failure(path, FailureClass::Internal))?
            .map_err(|error| observation_io_failure(path, &error))?;
        let (encoding, bytes) = path_bytes(&target)?;
        SymlinkTarget::new(encoding, bytes)
            .map(Some)
            .map_err(|_| failure(path, FailureClass::Protocol))
    }
}

#[cfg(unix)]
#[allow(clippy::unnecessary_wraps)]
fn path_bytes(path: &Path) -> Result<(SymlinkTargetEncoding, Vec<u8>), StorageRoleFailure> {
    use std::os::unix::ffi::OsStrExt as _;
    Ok((
        SymlinkTargetEncoding::UnixBytes,
        path.as_os_str().as_bytes().to_vec(),
    ))
}

#[cfg(windows)]
#[allow(clippy::unnecessary_wraps)]
fn path_bytes(path: &Path) -> Result<(SymlinkTargetEncoding, Vec<u8>), StorageRoleFailure> {
    use std::os::windows::ffi::OsStrExt as _;
    let bytes = path
        .as_os_str()
        .encode_wide()
        .flat_map(u16::to_le_bytes)
        .collect();
    Ok((SymlinkTargetEncoding::WindowsWide, bytes))
}

#[cfg(all(not(unix), not(windows)))]
fn path_bytes(_path: &Path) -> Result<(SymlinkTargetEncoding, Vec<u8>), StorageRoleFailure> {
    Err(failure(&StoragePath::root(), FailureClass::Unsupported))
}

fn checked_relative(path: &StoragePath) -> Result<PathBuf, StorageRoleFailure> {
    let relative = PathBuf::from(path.as_str());
    let invalid = relative.as_os_str().is_empty()
        || relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        });
    if invalid {
        Err(failure(path, FailureClass::InvalidInput))
    } else {
        Ok(relative)
    }
}

fn system_time_to_timestamp(time: SystemTime) -> Option<StorageTimestamp> {
    let nanos = match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i128::try_from(duration.as_nanos()).ok()?,
        Err(error) => -i128::try_from(error.duration().as_nanos()).ok()?,
    };
    StorageTimestamp::new(nanos, TimePrecision::Nanoseconds).ok()
}

#[cfg(unix)]
fn entry_kind(metadata: &Metadata) -> Option<EntryKind> {
    use cap_std::fs::FileTypeExt as _;
    let kind = metadata.file_type();
    if kind.is_file() {
        Some(EntryKind::File)
    } else if kind.is_dir() {
        Some(EntryKind::Directory)
    } else if kind.is_symlink() {
        Some(EntryKind::Symlink)
    } else if kind.is_block_device() {
        Some(EntryKind::Special(SpecialFileKind::BlockDevice))
    } else if kind.is_char_device() {
        Some(EntryKind::Special(SpecialFileKind::CharacterDevice))
    } else if kind.is_fifo() {
        Some(EntryKind::Special(SpecialFileKind::Fifo))
    } else if kind.is_socket() {
        Some(EntryKind::Special(SpecialFileKind::Socket))
    } else {
        None
    }
}

#[cfg(not(unix))]
fn entry_kind(metadata: &Metadata) -> Option<EntryKind> {
    let kind = metadata.file_type();
    if kind.is_file() {
        Some(EntryKind::File)
    } else if kind.is_dir() {
        Some(EntryKind::Directory)
    } else if kind.is_symlink() {
        Some(EntryKind::Symlink)
    } else {
        None
    }
}

#[cfg(unix)]
fn source_identity(
    backend: &BackendIdentity,
    _path: &StoragePath,
    metadata: &Metadata,
) -> Result<SourceIdentity, StorageRoleFailure> {
    use cap_std::fs::MetadataExt as _;
    let mut stable = Vec::with_capacity(16);
    stable.extend_from_slice(&metadata.dev().to_le_bytes());
    stable.extend_from_slice(&metadata.ino().to_le_bytes());
    SourceIdentity::new(
        backend.clone(),
        IdentityStrength::StableWithinBackend,
        stable,
    )
    .map_err(|_| failure(&StoragePath::root(), FailureClass::Internal))
}

#[cfg(not(unix))]
fn source_identity(
    backend: &BackendIdentity,
    path: &StoragePath,
    _metadata: &Metadata,
) -> Result<SourceIdentity, StorageRoleFailure> {
    SourceIdentity::new(
        backend.clone(),
        IdentityStrength::PathScoped,
        path.as_str().as_bytes(),
    )
    .map_err(|_| failure(path, FailureClass::Internal))
}

#[cfg(unix)]
fn backend_facts(metadata: &Metadata) -> Vec<u8> {
    use cap_std::fs::MetadataExt as _;
    let mut facts = Vec::with_capacity(57);
    facts.push(1);
    for value in [
        metadata.dev(),
        metadata.ino(),
        metadata.mode().into(),
        metadata.uid().into(),
        metadata.gid().into(),
        metadata.nlink(),
        metadata.rdev(),
    ] {
        facts.extend_from_slice(&value.to_le_bytes());
    }
    facts
}

#[cfg(not(unix))]
fn backend_facts(metadata: &Metadata) -> Vec<u8> {
    let mut facts = Vec::with_capacity(17);
    facts.push(1);
    facts.extend_from_slice(&metadata.len().to_le_bytes());
    facts.extend_from_slice(&u64::from(metadata.permissions().readonly()).to_le_bytes());
    facts
}

fn failure(path: &StoragePath, class: FailureClass) -> StorageRoleFailure {
    entry_failure(path, class, Transience::Permanent)
}

fn entry_failure(
    path: &StoragePath,
    class: FailureClass,
    transience: Transience,
) -> StorageRoleFailure {
    let error = EntryOperationFailure::new(
        path.clone(),
        Operation::Observe,
        class,
        transience,
        "local observation failed",
    )
    .unwrap_or_else(|_| unreachable!("static diagnostic is valid"));
    StorageRoleFailure::Entry(error)
}

fn io_failure(path: &StoragePath, error: &io::Error) -> StorageRoleFailure {
    let (class, transience) = classify_io(error.kind());
    entry_failure(path, class, transience)
}

fn session_io_failure(error: &io::Error) -> StorageRoleFailure {
    session_failure(Operation::Connect, error)
}

fn observation_io_failure(path: &StoragePath, error: &io::Error) -> StorageRoleFailure {
    match error.kind() {
        io::ErrorKind::NotConnected | io::ErrorKind::BrokenPipe => {
            session_failure(Operation::Observe, error)
        }
        _ => io_failure(path, error),
    }
}

fn session_failure(operation: Operation, error: &io::Error) -> StorageRoleFailure {
    let (class, transience) = classify_io(error.kind());
    let failure = BackendSessionFailure::new(
        operation,
        class,
        transience,
        "local backend connection failed",
    )
    .unwrap_or_else(|_| unreachable!("static diagnostic is valid"));
    StorageRoleFailure::Session(failure)
}

pub(crate) fn classify_io(kind: io::ErrorKind) -> (FailureClass, Transience) {
    match kind {
        io::ErrorKind::NotFound => (FailureClass::NotFound, Transience::Permanent),
        io::ErrorKind::PermissionDenied => (FailureClass::PermissionDenied, Transience::Permanent),
        io::ErrorKind::InvalidInput => (FailureClass::InvalidInput, Transience::Permanent),
        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => {
            (FailureClass::Protocol, Transience::Transient)
        }
        _ => (FailureClass::Protocol, Transience::Unknown),
    }
}

#[cfg(test)]
mod tests;
