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
use futures::StreamExt as _;
use futures::stream::{FuturesOrdered, FuturesUnordered};

use super::observation::{LocalObservationAdapter, classify_io, source_identity};
use crate::model::{
    BackendIdentity, EntryOperationFailure, FailureClass, Operation, SourceIdentity, StoragePath,
    Transience,
};
use crate::storage::{
    ByteStream, PositionedByteStream, PositionedChunk, ReadRequest, ReadSource, SourceDescriptor,
    SourceQosBudget, StorageRoleFailure,
};

/// Backend capability ceiling for one Local positional read.
///
/// The runtime may negotiate a smaller value from its byte budget or `QoS`, but Local does not
/// internally subdivide a granted range below this ceiling. A short `read_at` is completed by
/// reading only the missing suffix.
const LOCAL_MAX_READ_CHUNK_BYTES: usize = 2 * 1024 * 1024;

type LocalReadResult = (
    u64,
    u64,
    io::Result<Bytes>,
    Option<crate::storage::ReadAdmission>,
);
type LocalReadFuture = tokio::task::JoinHandle<LocalReadResult>;

/// One-open local source supporting ordered streams and completion-ordered positioned reads.
pub(crate) struct LocalReadSource {
    root: Arc<Dir>,
    identity: BackendIdentity,
    observer: LocalObservationAdapter,
    read_concurrency: usize,
    #[cfg(test)]
    probe: Arc<ReadProbe>,
}

#[cfg(test)]
#[derive(Default)]
struct ReadProbe {
    streams: std::sync::atomic::AtomicUsize,
    calls: std::sync::atomic::AtomicUsize,
    delay: std::sync::Mutex<Option<std::time::Duration>>,
    gates: std::sync::Mutex<std::collections::HashMap<u64, Arc<ReadGate>>>,
    active: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
    completion_order: std::sync::Mutex<Vec<u64>>,
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct ReadGate {
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
impl ReadGate {
    pub(crate) async fn wait_started(&self) {
        self.started.notified().await;
    }

    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

impl LocalReadSource {
    pub(crate) fn new(
        root: impl AsRef<Path>,
        identity: BackendIdentity,
        read_concurrency: usize,
    ) -> Result<Self, StorageRoleFailure> {
        if read_concurrency == 0 {
            return Err(failure(
                &StoragePath::root(),
                FailureClass::InvalidInput,
                Transience::Permanent,
            ));
        }
        let root_path = std::fs::canonicalize(root).map_err(|error| session_failure(&error))?;
        let root = Arc::new(
            Dir::open_ambient_dir(root_path, ambient_authority())
                .map_err(|error| session_failure(&error))?,
        );
        Ok(Self {
            observer: LocalObservationAdapter::from_root(Arc::clone(&root), identity.clone()),
            root,
            identity,
            read_concurrency,
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
    pub(crate) fn read_stream_count(&self) -> usize {
        self.probe.streams.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn gate_read_at(&self, offset: u64) -> Arc<ReadGate> {
        let gate = Arc::new(ReadGate::default());
        self.probe
            .gates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(offset, Arc::clone(&gate));
        gate
    }

    #[cfg(test)]
    pub(crate) fn peak_read_concurrency(&self) -> usize {
        self.probe.peak.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn read_completion_order(&self) -> Vec<u64> {
        self.probe
            .completion_order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
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
    fn supports_read_budget(&self) -> bool {
        true
    }
    fn maximum_read_chunk_bytes(&self) -> usize {
        LOCAL_MAX_READ_CHUNK_BYTES
    }

    async fn describe(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        let (observed, version) = self
            .observer
            .observe_versioned(path.clone(), crate::model::ObservationPlan::default())
            .await?;
        Ok(SourceDescriptor {
            path: path.clone(),
            kind: observed.kind(),
            size: observed.size(),
            source_identity: observed.source_identity().clone(),
            backend_fact: None,
            content_version: Some(version),
            inline_timestamps: None,
            inline_mode: None,
        })
    }

    fn supports_positioned_read(&self) -> bool {
        true
    }

    async fn read_positioned(
        &self,
        request: ReadRequest,
    ) -> Result<PositionedByteStream, StorageRoleFailure> {
        let state = self.read_state(request, false).await?;
        Ok(Box::pin(futures::stream::try_unfold(
            state,
            read_next_chunk,
        )))
    }

    async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        let state = self.read_state(request, true).await?;
        Ok(Box::pin(
            futures::stream::try_unfold(state, read_next_chunk)
                .map(|item| item.map(|chunk| chunk.data)),
        ))
    }
}

impl LocalReadSource {
    async fn read_state(
        &self,
        request: ReadRequest,
        ordered: bool,
    ) -> Result<LocalReadState, StorageRoleFailure> {
        if request.cancel.is_cancelled() {
            return Err(failure(
                &request.path,
                FailureClass::Cancelled,
                Transience::Transient,
            ));
        }
        if request.maximum_chunk_bytes == 0 || request.read_inflight == 0 {
            return Err(failure(
                &request.path,
                FailureClass::InvalidInput,
                Transience::Permanent,
            ));
        }
        #[cfg(test)]
        self.probe
            .streams
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
            next_issue: range.start,
            next_emit: range.start,
            end: range.end,
            maximum_chunk_bytes: request.maximum_chunk_bytes.min(LOCAL_MAX_READ_CHUNK_BYTES),
            read_concurrency: self.read_concurrency.min(request.read_inflight),
            inflight: if ordered {
                ReadQueue::Ordered(FuturesOrdered::new())
            } else {
                ReadQueue::Positioned(FuturesUnordered::new())
            },
            cancel: request.cancel,
            source_qos: request.source_qos,
            budget: request.read_budget,
            #[cfg(test)]
            probe: Arc::clone(&self.probe),
        };
        Ok(state)
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

enum ReadQueue {
    Ordered(FuturesOrdered<LocalReadFuture>),
    Positioned(FuturesUnordered<LocalReadFuture>),
}

impl ReadQueue {
    fn len(&self) -> usize {
        match self {
            Self::Ordered(q) => q.len(),
            Self::Positioned(q) => q.len(),
        }
    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn push_back(&mut self, value: LocalReadFuture) {
        match self {
            Self::Ordered(q) => q.push_back(value),
            Self::Positioned(q) => q.push(value),
        }
    }
    async fn next(&mut self) -> Option<Result<LocalReadResult, tokio::task::JoinError>> {
        match self {
            Self::Ordered(q) => q.next().await,
            Self::Positioned(q) => q.next().await,
        }
    }
}

struct LocalReadState {
    file: Arc<std::fs::File>,
    path: StoragePath,
    next_issue: u64,
    next_emit: u64,
    end: u64,
    maximum_chunk_bytes: usize,
    read_concurrency: usize,
    inflight: ReadQueue,
    cancel: tokio_util::sync::CancellationToken,
    source_qos: Option<SourceQosBudget>,
    budget: Option<crate::storage::ReadBudget>,
    #[cfg(test)]
    probe: Arc<ReadProbe>,
}

async fn read_next_chunk(
    mut state: LocalReadState,
) -> Result<Option<(PositionedChunk, LocalReadState)>, StorageRoleFailure> {
    fill_read_pipeline(&mut state).await?;
    if state.inflight.is_empty() {
        return Ok(None);
    }
    let next = tokio::select! {
        biased;
        () = state.cancel.cancelled() => {
            return Err(failure(&state.path, FailureClass::Cancelled, Transience::Transient));
        }
        result = state.inflight.next() => result,
    };
    let joined = next
        .ok_or_else(|| failure(&state.path, FailureClass::Internal, Transience::Unknown))?
        .map_err(|_| failure(&state.path, FailureClass::Internal, Transience::Unknown))?;
    let (start, requested, result, admission) = joined;
    if matches!(state.inflight, ReadQueue::Ordered(_)) && start != state.next_emit {
        return Err(failure(
            &state.path,
            FailureClass::Internal,
            Transience::Unknown,
        ));
    }
    let bytes = result.map_err(|error| io_failure(&state.path, &error))?;
    if bytes.len() as u64 != requested {
        return Err(failure(
            &state.path,
            FailureClass::Corruption,
            Transience::Unknown,
        ));
    }
    if let Some(qos) = &state.source_qos {
        qos.record_read_bytes(bytes.len() as u64);
    }
    state.next_emit = state
        .next_emit
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| {
            failure(
                &state.path,
                FailureClass::InvalidInput,
                Transience::Permanent,
            )
        })?;
    if let (Some(budget), Some(admission)) = (&state.budget, admission) {
        budget.ready(start, admission);
    }
    Ok(Some((
        PositionedChunk {
            offset: start,
            data: bytes,
        },
        state,
    )))
}

async fn fill_read_pipeline(state: &mut LocalReadState) -> Result<(), StorageRoleFailure> {
    while state.inflight.len() < state.read_concurrency && state.next_issue < state.end {
        if state.cancel.is_cancelled() {
            return Err(failure(
                &state.path,
                FailureClass::Cancelled,
                Transience::Transient,
            ));
        }
        let requested = (state.end - state.next_issue).min(state.maximum_chunk_bytes as u64);
        let admission = if let Some(budget) = &state.budget {
            let Some(admission) = budget
                .reserve(
                    usize::try_from(requested).map_err(|_| {
                        failure(
                            &state.path,
                            FailureClass::InvalidInput,
                            Transience::Permanent,
                        )
                    })?,
                    state.inflight.is_empty(),
                )
                .await
                .map_err(|_| {
                    failure(&state.path, FailureClass::Cancelled, Transience::Transient)
                })?
            else {
                break;
            };
            Some(admission)
        } else {
            None
        };
        let granted = if let Some(qos) = &state.source_qos {
            qos.admit_read(requested, &state.cancel)
                .await
                .map_err(|_| failure(&state.path, FailureClass::Cancelled, Transience::Transient))?
        } else {
            requested
        };
        if granted == 0 {
            return Err(failure(
                &state.path,
                FailureClass::Internal,
                Transience::Unknown,
            ));
        }
        let start = state.next_issue;
        let end = start.checked_add(granted).ok_or_else(|| {
            failure(
                &state.path,
                FailureClass::InvalidInput,
                Transience::Permanent,
            )
        })?;
        let file = Arc::clone(&state.file);
        let cancel = state.cancel.clone();
        #[cfg(test)]
        let probe = Arc::clone(&state.probe);
        #[cfg(test)]
        probe.on_submit();
        // Submit the blocking read immediately, including while later QoS admissions await.
        // A second asynchronous task per chunk only adds scheduling and wakeups.
        #[cfg(test)]
        let runtime = tokio::runtime::Handle::current();
        state
            .inflight
            .push_back(tokio::task::spawn_blocking(move || {
                #[cfg(test)]
                runtime.block_on(probe.before_read(start));
                let result = if cancel.is_cancelled() {
                    Err(io::Error::from(io::ErrorKind::Interrupted))
                } else {
                    read_file_range(&file, start..end)
                };
                #[cfg(test)]
                probe.after_read(start);
                (start, granted, result, admission)
            }));
        state.next_issue = end;
    }
    Ok(())
}

#[cfg(test)]
impl ReadProbe {
    /// Records one read entering the in-flight window, at submission rather than at execution.
    ///
    /// "In flight" is what the pipeline actually controls: `fill_read_pipeline` submits up to
    /// `read_concurrency` reads before awaiting any completion. Counting from the moment the
    /// blocking thread starts running instead measured how the blocking pool happened to
    /// interleave, so a short read could finish before the next one was scheduled and the peak
    /// would land below the window size under load — a race in the observation, not in the
    /// pipeline.
    fn on_submit(&self) {
        use std::sync::atomic::Ordering;

        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
    }

    async fn before_read(&self, offset: u64) {
        use std::sync::atomic::Ordering;

        self.calls.fetch_add(1, Ordering::SeqCst);
        let gate = self
            .gates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&offset)
            .cloned();
        if let Some(gate) = gate {
            gate.started.notify_one();
            gate.release.notified().await;
        }
        let common = *self
            .delay
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(delay) = common {
            tokio::time::sleep(delay).await;
        }
    }

    fn after_read(&self, offset: u64) {
        use std::sync::atomic::Ordering;

        self.completion_order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(offset);
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
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
        #[cfg(unix)]
        let count = file.read_at(&mut output[read..], range.start + read as u64)?;
        #[cfg(windows)]
        let count = file.seek_read(&mut output[read..], range.start + read as u64)?;
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
    if error.kind() == io::ErrorKind::UnexpectedEof {
        return failure(path, FailureClass::Corruption, Transience::Unknown);
    }
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
