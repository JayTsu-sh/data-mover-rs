use std::io;
#[cfg(unix)]
use std::os::unix::fs::FileExt as _;
#[cfg(windows)]
use std::os::windows::fs::FileExt as _;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use cap_std::ambient_authority;
use cap_std::fs::Dir;

use super::observation::{LocalObservationAdapter, classify_io, source_identity};
use crate::model::{
    BackendIdentity, EntryOperationFailure, FailureClass, Operation, SourceIdentity, StoragePath,
    Transience,
};
use crate::storage::{
    ByteStream, ReadRequest, ReadSource, SourceDescriptor, SourceQosBudget, StorageRoleFailure,
};

const MAX_READ_CHUNK: u64 = 1024 * 1024;

pub(crate) struct LocalReadSource {
    root: Arc<Dir>,
    identity: BackendIdentity,
    observer: LocalObservationAdapter,
    #[cfg(test)]
    probe: Arc<ReadProbe>,
}

#[cfg(test)]
#[derive(Default)]
struct ReadProbe {
    calls: std::sync::atomic::AtomicUsize,
    delay: std::sync::Mutex<Option<std::time::Duration>>,
}

impl LocalReadSource {
    pub(crate) fn new(
        root: impl AsRef<Path>,
        identity: BackendIdentity,
    ) -> Result<Self, StorageRoleFailure> {
        let root_path = std::fs::canonicalize(root).map_err(|error| session_failure(&error))?;
        let root = Arc::new(
            Dir::open_ambient_dir(root_path, ambient_authority())
                .map_err(|error| session_failure(&error))?,
        );
        Ok(Self {
            observer: LocalObservationAdapter::from_root(Arc::clone(&root), identity.clone()),
            root,
            identity,
            #[cfg(test)]
            probe: Arc::new(ReadProbe::default()),
        })
    }

    #[cfg(test)]
    pub(crate) fn delay_reads(&self, delay: std::time::Duration) {
        *self
            .probe
            .delay
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(delay);
    }

    #[cfg(test)]
    pub(crate) fn read_call_count(&self) -> usize {
        self.probe.calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn delay_description(&self, path: &str, delay: std::time::Duration) {
        self.observer.delay_observation(path, delay);
    }

    #[cfg(test)]
    pub(crate) fn description_started(&self) -> bool {
        !self.observer.probe_orders().0.is_empty()
    }

    async fn resolve_range(
        &self,
        request: &ReadRequest,
    ) -> Result<std::ops::Range<u64>, StorageRoleFailure> {
        if let Some(range) = &request.range {
            return Ok(range.clone());
        }
        let size = self.describe(&request.path).await?.size.ok_or_else(|| {
            failure(
                &request.path,
                FailureClass::Unsupported,
                Transience::Permanent,
            )
        })?;
        Ok(0..size)
    }
}

#[async_trait]
impl ReadSource for LocalReadSource {
    async fn describe(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        let observed = self.observer.observe(path.clone()).await?;
        Ok(SourceDescriptor {
            path: path.clone(),
            kind: observed.kind(),
            size: observed.size(),
            source_identity: observed.source_identity().clone(),
        })
    }

    async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        if request.cancel.is_cancelled() {
            return Err(failure(
                &request.path,
                FailureClass::Cancelled,
                Transience::Transient,
            ));
        }
        let range = self.resolve_range(&request).await?;
        if range.end < range.start {
            return Err(failure(
                &request.path,
                FailureClass::InvalidInput,
                Transience::Permanent,
            ));
        }
        let relative = checked_relative(&request.path)?;
        let file = self
            .open_bound_file(&request.path, relative, request.expected_source.as_ref())
            .await?;
        let state = LocalReadState {
            file,
            path: request.path,
            next: range.start,
            end: range.end,
            cancel: request.cancel,
            source_qos: request.source_qos,
            #[cfg(test)]
            probe: Arc::clone(&self.probe),
        };
        Ok(Box::pin(futures::stream::try_unfold(
            state,
            read_next_chunk,
        )))
    }
}

impl LocalReadSource {
    async fn open_bound_file(
        &self,
        path: &StoragePath,
        relative: PathBuf,
        expected: Option<&SourceIdentity>,
    ) -> Result<Arc<std::fs::File>, StorageRoleFailure> {
        let root = Arc::clone(&self.root);
        let path = path.clone();
        let error_path = path.clone();
        let identity = self.identity.clone();
        let expected = expected.cloned();
        tokio::task::spawn_blocking(move || {
            let file = root.open(relative)?;
            let observed =
                source_identity(&identity, &path, &file.metadata()?).map_err(role_to_io)?;
            if expected.as_ref().is_some_and(|value| value != &observed) {
                return Err(io::Error::from(io::ErrorKind::AlreadyExists));
            }
            Ok(Arc::new(file.into_std()))
        })
        .await
        .map_err(|_| failure(&error_path, FailureClass::Internal, Transience::Unknown))?
        .map_err(|error| io_failure(&error_path, &error))
    }
}

struct LocalReadState {
    file: Arc<std::fs::File>,
    path: StoragePath,
    next: u64,
    end: u64,
    cancel: tokio_util::sync::CancellationToken,
    source_qos: Option<SourceQosBudget>,
    #[cfg(test)]
    probe: Arc<ReadProbe>,
}

async fn read_next_chunk(
    mut state: LocalReadState,
) -> Result<Option<(Bytes, LocalReadState)>, StorageRoleFailure> {
    if state.next == state.end {
        return Ok(None);
    }
    let start = state.next;
    let requested = (state.end - start).min(MAX_READ_CHUNK);
    let granted = if let Some(qos) = &state.source_qos {
        qos.admit_read(requested, &state.cancel)
            .await
            .map_err(|_| failure(&state.path, FailureClass::Cancelled, Transience::Transient))?
    } else {
        requested
    };
    let end = start + granted;
    #[cfg(test)]
    {
        state
            .probe
            .calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let delay = *state
            .probe
            .delay
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(delay) = delay {
            tokio::select! {
                biased;
                () = state.cancel.cancelled() => {
                    return Err(failure(&state.path, FailureClass::Cancelled, Transience::Transient));
                }
                () = tokio::time::sleep(delay) => {}
            }
        }
    }
    let file = Arc::clone(&state.file);
    let path = state.path.clone();
    let read = tokio::task::spawn_blocking(move || read_file_range(&file, start..end));
    let bytes = tokio::select! {
        biased;
        () = state.cancel.cancelled() => {
            return Err(failure(&state.path, FailureClass::Cancelled, Transience::Transient));
        }
        result = read => result
            .map_err(|_| failure(&path, FailureClass::Internal, Transience::Unknown))?
            .map_err(|error| io_failure(&path, &error))?,
    };
    if let Some(qos) = &state.source_qos {
        qos.record_read_bytes(bytes.len() as u64);
    }
    state.next = end;
    Ok(Some((bytes, state)))
}

fn checked_relative(path: &StoragePath) -> Result<PathBuf, StorageRoleFailure> {
    let relative = PathBuf::from(path.as_str());
    let invalid = relative.as_os_str().is_empty()
        || relative.is_absolute()
        || relative.components().any(|part| {
            matches!(
                part,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        });
    if invalid {
        Err(failure(
            path,
            FailureClass::InvalidInput,
            Transience::Permanent,
        ))
    } else {
        Ok(relative)
    }
}

fn read_file_range(file: &std::fs::File, range: std::ops::Range<u64>) -> io::Result<Bytes> {
    let length = range
        .end
        .checked_sub(range.start)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
    let mut output = vec![0_u8; length];
    let mut read = 0;
    while read < length {
        let count = file.read_at(&mut output[read..], range.start + read as u64)?;
        if count == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        read += count;
    }
    Ok(Bytes::from(output))
}

fn role_to_io(error: StorageRoleFailure) -> io::Error {
    io::Error::other(error)
}

fn io_failure(path: &StoragePath, error: &io::Error) -> StorageRoleFailure {
    let (class, transience) = classify_io(error.kind());
    failure(path, class, transience)
}

fn failure(path: &StoragePath, class: FailureClass, transience: Transience) -> StorageRoleFailure {
    let error = EntryOperationFailure::new(
        path.clone(),
        Operation::Read,
        class,
        transience,
        "local source read failed",
    )
    .unwrap_or_else(|_| unreachable!("static diagnostic is valid"));
    StorageRoleFailure::Entry(error)
}

fn session_failure(error: &io::Error) -> StorageRoleFailure {
    let (class, transience) = classify_io(error.kind());
    let error = crate::model::BackendSessionFailure::new(
        Operation::Connect,
        class,
        transience,
        "local source connection failed",
    )
    .unwrap_or_else(|_| unreachable!("static diagnostic is valid"));
    StorageRoleFailure::Session(error)
}

#[cfg(test)]
mod tests;
