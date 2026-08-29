use std::collections::BTreeMap;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use cap_std::ambient_authority;
use cap_std::fs::Dir;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::{
    TraversalCompletion, TraversalItem, TraversalOutcome, TraversalRequest, TraversalSession,
    TraversalSource, TraversalTerminalFailure,
};
use crate::model::{
    BackendIdentity, BackendSessionFailure, EntryFailureIdentity, EntryOperationFailure,
    FailureClass, Operation, StoragePath, Transience,
};
use crate::storage::StorageRoleFailure;
use crate::storage::backends::local::observation::{LocalObservationAdapter, classify_io};

enum Candidate {
    Path {
        sequence: u64,
        path: StoragePath,
    },
    EntryFailure {
        sequence: u64,
        error: EntryOperationFailure,
    },
    SessionFailure(BackendSessionFailure),
    EnumerationComplete,
    InternalFailure,
}

enum ListFailureDisposition {
    EntrySent,
    SessionSent,
    ReceiverClosed,
}

enum DirectoryRead {
    Entries(cap_std::fs::ReadDir),
    Skip,
    Stop,
}

pub(crate) struct LocalTraversalSource {
    root: Arc<Dir>,
    observer: Arc<LocalObservationAdapter>,
    identity: BackendIdentity,
    probe: Arc<EnumerationProbe>,
}

#[derive(Default)]
struct EnumerationProbe {
    #[cfg(test)]
    read_count: AtomicUsize,
    #[cfg(test)]
    fail_on_read: AtomicUsize,
    #[cfg(test)]
    failed_path: std::sync::Mutex<Option<StoragePath>>,
}

impl EnumerationProbe {
    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn read_dir(&self, root: &Dir, path: &Path) -> io::Result<cap_std::fs::ReadDir> {
        #[cfg(test)]
        {
            let current = self.read_count.fetch_add(1, Ordering::SeqCst) + 1;
            if current == self.fail_on_read.load(Ordering::SeqCst) {
                *self
                    .failed_path
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(storage_path(path));
                return Err(io::Error::from(io::ErrorKind::PermissionDenied));
            }
        }
        root.read_dir(path)
    }
}

impl LocalTraversalSource {
    pub(crate) fn new(
        root: impl AsRef<Path>,
        identity: BackendIdentity,
    ) -> Result<Self, BackendSessionFailure> {
        let root_path = std::fs::canonicalize(root.as_ref())
            .map_err(|error| session_failure(Operation::Connect, &error))?;
        let root = Arc::new(
            Dir::open_ambient_dir(&root_path, ambient_authority())
                .map_err(|error| session_failure(Operation::Connect, &error))?,
        );
        let observer = Arc::new(LocalObservationAdapter::from_root(
            Arc::clone(&root),
            identity.clone(),
        ));
        Ok(Self {
            root,
            observer,
            identity,
            probe: Arc::new(EnumerationProbe::default()),
        })
    }

    #[cfg(test)]
    fn observation_orders(&self) -> (Vec<String>, Vec<String>) {
        self.observer.probe_orders()
    }

    #[cfg(test)]
    fn fail_enumeration_read(&self, read_number: usize) {
        self.probe.fail_on_read.store(read_number, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn failed_enumeration_path(&self) -> Option<StoragePath> {
        self.probe
            .failed_path
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl TraversalSource for LocalTraversalSource {
    fn traverse(&self, request: TraversalRequest) -> TraversalSession {
        let (item_tx, item_rx) = mpsc::channel(request.max_buffered_items.get());
        let (candidate_tx, candidate_rx) = mpsc::channel(request.max_buffered_items.get());
        let (completion_tx, completion_rx) = oneshot::channel();
        let cancel = request.cancel.clone();
        spawn_enumerator(
            Arc::clone(&self.root),
            self.identity.clone(),
            Arc::clone(&self.probe),
            &request,
            candidate_tx,
        );
        tokio::spawn(run_observers(
            Arc::clone(&self.observer),
            request,
            candidate_rx,
            item_tx,
            completion_tx,
        ));
        TraversalSession::new(item_rx, completion_rx, cancel)
    }
}

fn spawn_enumerator(
    root: Arc<Dir>,
    identity: BackendIdentity,
    probe: Arc<EnumerationProbe>,
    request: &TraversalRequest,
    sender: mpsc::Sender<Candidate>,
) {
    let root_path = request.root.clone();
    let cancel = request.cancel.clone();
    let completion_sender = sender.clone();
    let task = tokio::task::spawn_blocking(move || {
        enumerate(&root, &identity, &probe, &root_path, &cancel, &sender);
    });
    tokio::spawn(async move {
        let candidate = match task.await {
            Ok(()) => Candidate::EnumerationComplete,
            Err(_) => Candidate::InternalFailure,
        };
        let _ = completion_sender.send(candidate).await;
    });
}

fn enumerate(
    root: &Dir,
    identity: &BackendIdentity,
    probe: &EnumerationProbe,
    scan_root: &StoragePath,
    cancel: &CancellationToken,
    sender: &mpsc::Sender<Candidate>,
) {
    let Some(relative_root) = initial_directory(scan_root, sender) else {
        return;
    };
    let mut sequence = 0_u64;
    let mut directories = vec![relative_root];
    while let Some(directory) = directories.pop() {
        if cancel.is_cancelled() {
            return;
        }
        let entries = match read_directory(probe, root, &directory, sender, &mut sequence) {
            DirectoryRead::Entries(entries) => entries,
            DirectoryRead::Skip => continue,
            DirectoryRead::Stop => return,
        };
        for entry in entries {
            if cancel.is_cancelled() {
                return;
            }
            let Some(next) = enumerate_entry(
                root,
                identity,
                sender,
                &directory,
                entry,
                sequence,
                &mut directories,
            ) else {
                return;
            };
            sequence = next;
        }
    }
}

fn read_directory(
    probe: &EnumerationProbe,
    root: &Dir,
    directory: &Path,
    sender: &mpsc::Sender<Candidate>,
    sequence: &mut u64,
) -> DirectoryRead {
    let error = match probe.read_dir(root, directory) {
        Ok(entries) => return DirectoryRead::Entries(entries),
        Err(error) => error,
    };
    let path = storage_path(directory);
    match send_list_failure(sender, &path, *sequence, &error) {
        ListFailureDisposition::EntrySent => match sequence.checked_add(1) {
            Some(next) => {
                *sequence = next;
                DirectoryRead::Skip
            }
            None => DirectoryRead::Stop,
        },
        ListFailureDisposition::SessionSent | ListFailureDisposition::ReceiverClosed => {
            DirectoryRead::Stop
        }
    }
}

fn initial_directory(scan_root: &StoragePath, sender: &mpsc::Sender<Candidate>) -> Option<PathBuf> {
    if let Ok(path) = checked_relative_root(scan_root) {
        return Some(path);
    }
    let candidate = Candidate::EntryFailure {
        sequence: 0,
        error: entry_failure(scan_root, FailureClass::InvalidInput),
    };
    sender.blocking_send(candidate).ok()?;
    None
}

fn enumerate_entry(
    root: &Dir,
    identity: &BackendIdentity,
    sender: &mpsc::Sender<Candidate>,
    directory: &Path,
    entry: io::Result<cap_std::fs::DirEntry>,
    sequence: u64,
    directories: &mut Vec<PathBuf>,
) -> Option<u64> {
    let entry = match entry {
        Ok(entry) => entry,
        Err(source) => {
            let path = storage_path(directory);
            let error = entry_io_failure(&path, &source);
            sender
                .blocking_send(Candidate::EntryFailure { sequence, error })
                .ok()?;
            return sequence.checked_add(1);
        }
    };
    let child = directory.join(entry.file_name());
    let path = match candidate_path(identity, directory, &child) {
        Ok(path) => path,
        Err(error) => {
            sender
                .blocking_send(Candidate::EntryFailure { sequence, error })
                .ok()?;
            return sequence.checked_add(1);
        }
    };
    sender
        .blocking_send(Candidate::Path {
            sequence,
            path: path.clone(),
        })
        .ok()?;
    if root
        .symlink_metadata(&child)
        .is_ok_and(|metadata| metadata.is_dir())
    {
        directories.push(child);
    }
    sequence.checked_add(1)
}

fn candidate_path(
    identity: &BackendIdentity,
    directory: &Path,
    child: &Path,
) -> Result<StoragePath, EntryOperationFailure> {
    let Some(encoded) = child.to_str() else {
        return Err(unrepresentable_failure(identity, child));
    };
    StoragePath::new(encoded.replace('\\', "/"))
        .map_err(|_| entry_failure(&storage_path(directory), FailureClass::Unsupported))
}

fn send_list_failure(
    sender: &mpsc::Sender<Candidate>,
    path: &StoragePath,
    sequence: u64,
    error: &io::Error,
) -> ListFailureDisposition {
    let session = matches!(
        error.kind(),
        io::ErrorKind::NotConnected | io::ErrorKind::BrokenPipe
    );
    let candidate = if session {
        Candidate::SessionFailure(session_failure(Operation::Traverse, error))
    } else {
        Candidate::EntryFailure {
            sequence,
            error: entry_io_failure(path, error),
        }
    };
    if sender.blocking_send(candidate).is_err() {
        ListFailureDisposition::ReceiverClosed
    } else if session {
        ListFailureDisposition::SessionSent
    } else {
        ListFailureDisposition::EntrySent
    }
}

async fn run_observers(
    observer: Arc<LocalObservationAdapter>,
    request: TraversalRequest,
    mut candidates: mpsc::Receiver<Candidate>,
    items: mpsc::Sender<TraversalItem>,
    completion: oneshot::Sender<Result<TraversalOutcome, TraversalTerminalFailure>>,
) {
    let mut tasks = JoinSet::new();
    let mut state = drive_observers(&observer, &request, &mut candidates, &items, &mut tasks).await;
    drain_observers(&request, &items, &mut tasks, &mut state).await;
    tasks.abort_all();
    drop(items);
    let result = terminal_result(state, request.cancel.is_cancelled());
    let _ = completion.send(result);
}

async fn drive_observers(
    observer: &Arc<LocalObservationAdapter>,
    request: &TraversalRequest,
    candidates: &mut mpsc::Receiver<Candidate>,
    items: &mpsc::Sender<TraversalItem>,
    tasks: &mut ObserverTasks,
) -> ObserverState {
    let mut state = ObserverState::new(request.observation_plan);
    loop {
        if state.terminal.is_some() || request.cancel.is_cancelled() {
            break;
        }
        if tasks.len() + state.pending.len() >= request.max_inflight_operations.get() {
            if !settle_one(tasks, &mut state, &request.cancel).await
                || state.terminal.is_some()
                || !flush_ready(items, &mut state, &request.cancel).await
            {
                break;
            }
            continue;
        }
        let candidate = tokio::select! {
            biased;
            () = request.cancel.cancelled() => None,
            candidate = candidates.recv() => candidate,
        };
        if !handle_candidate(candidate, observer, tasks, &mut state) {
            break;
        }
        if state.terminal.is_none() && !flush_ready(items, &mut state, &request.cancel).await {
            break;
        }
    }
    state
}

fn handle_candidate(
    candidate: Option<Candidate>,
    observer: &Arc<LocalObservationAdapter>,
    tasks: &mut ObserverTasks,
    state: &mut ObserverState,
) -> bool {
    match candidate {
        Some(Candidate::Path { sequence, path }) => {
            let observer = Arc::clone(observer);
            let plan = state.observation_plan;
            tasks.spawn(async move { (sequence, observer.observe_with_plan(path, plan).await) });
            true
        }
        Some(Candidate::EntryFailure { sequence, error }) => {
            state
                .pending
                .insert(sequence, TraversalItem::EntryFailure(error));
            true
        }
        Some(Candidate::SessionFailure(error)) => {
            state.terminal = Some(TraversalTerminalFailure::Session(error));
            false
        }
        Some(Candidate::EnumerationComplete) => {
            state.enumeration_complete = true;
            false
        }
        Some(Candidate::InternalFailure) | None => {
            state.terminal = Some(TraversalTerminalFailure::Internal);
            false
        }
    }
}

async fn drain_observers(
    request: &TraversalRequest,
    items: &mpsc::Sender<TraversalItem>,
    tasks: &mut ObserverTasks,
    state: &mut ObserverState,
) {
    while state.enumeration_complete
        && state.terminal.is_none()
        && !tasks.is_empty()
        && !request.cancel.is_cancelled()
    {
        if !settle_one(tasks, state, &request.cancel).await || state.terminal.is_some() {
            break;
        }
        if !flush_ready(items, state, &request.cancel).await {
            break;
        }
    }
}

type ObserverTasks = JoinSet<(u64, Result<crate::model::ObservedEntry, StorageRoleFailure>)>;

struct ObserverState {
    next: u64,
    observed: u64,
    failed: u64,
    pending: BTreeMap<u64, TraversalItem>,
    terminal: Option<TraversalTerminalFailure>,
    enumeration_complete: bool,
    observation_plan: crate::model::ObservationPlan,
}

impl ObserverState {
    fn new(observation_plan: crate::model::ObservationPlan) -> Self {
        Self {
            next: 0,
            observed: 0,
            failed: 0,
            pending: BTreeMap::new(),
            terminal: None,
            enumeration_complete: false,
            observation_plan,
        }
    }
}

async fn settle_one(
    tasks: &mut ObserverTasks,
    state: &mut ObserverState,
    cancel: &CancellationToken,
) -> bool {
    let result = tokio::select! {
        biased;
        () = cancel.cancelled() => return false,
        result = tasks.join_next() => result,
    };
    match result {
        Some(Ok((sequence, Ok(entry)))) => {
            state
                .pending
                .insert(sequence, TraversalItem::Entry(Box::new(entry)));
        }
        Some(Ok((sequence, Err(StorageRoleFailure::Entry(error))))) => {
            state
                .pending
                .insert(sequence, TraversalItem::EntryFailure(error));
        }
        Some(Ok((_, Err(StorageRoleFailure::Session(error))))) => {
            state.terminal = Some(TraversalTerminalFailure::Session(error));
        }
        Some(Err(_)) | None => state.terminal = Some(TraversalTerminalFailure::Internal),
    }
    true
}

async fn flush_ready(
    sender: &mpsc::Sender<TraversalItem>,
    state: &mut ObserverState,
    cancel: &CancellationToken,
) -> bool {
    while let Some(item) = state.pending.remove(&state.next) {
        match &item {
            TraversalItem::Entry(_) => state.observed += 1,
            TraversalItem::EntryFailure(_) => state.failed += 1,
        }
        let sent = tokio::select! {
            biased;
            () = cancel.cancelled() => false,
            result = sender.send(item) => result.is_ok(),
        };
        if !sent {
            return false;
        }
        state.next += 1;
    }
    true
}

fn terminal_result(
    state: ObserverState,
    cancelled: bool,
) -> Result<TraversalOutcome, TraversalTerminalFailure> {
    if cancelled {
        Ok(TraversalOutcome::Cancelled)
    } else if let Some(error) = state.terminal {
        Err(error)
    } else {
        Ok(TraversalOutcome::Completed(TraversalCompletion {
            observed_entries: state.observed,
            entry_failures: state.failed,
        }))
    }
}

fn checked_relative_root(path: &StoragePath) -> Result<PathBuf, ()> {
    let relative = PathBuf::from(path.as_str());
    if relative.is_absolute()
        || relative.components().any(|part| {
            matches!(
                part,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        Err(())
    } else if relative.as_os_str().is_empty() {
        Ok(PathBuf::from("."))
    } else {
        Ok(relative)
    }
}

fn storage_path(path: &Path) -> StoragePath {
    path.to_str()
        .and_then(|value| StoragePath::new(value.replace('\\', "/")).ok())
        .unwrap_or_else(StoragePath::root)
}

fn unrepresentable_failure(backend: &BackendIdentity, path: &Path) -> EntryOperationFailure {
    let display_path = unrepresentable_path(path);
    let identity = EntryFailureIdentity::derive(backend, display_path.as_str().as_bytes());
    entry_failure(&display_path, FailureClass::Unsupported).with_identity(identity)
}

#[cfg(unix)]
fn unrepresentable_path(path: &Path) -> StoragePath {
    use std::fmt::Write as _;
    use std::os::unix::ffi::OsStrExt as _;
    let mut encoded = String::from("@local-unix-hex:");
    for byte in path.as_os_str().as_bytes() {
        let _ = write!(encoded, "{byte:02x}");
    }
    StoragePath::new(encoded).unwrap_or_else(|_| StoragePath::root())
}

#[cfg(windows)]
fn unrepresentable_path(path: &Path) -> StoragePath {
    use std::fmt::Write as _;
    use std::os::windows::ffi::OsStrExt as _;
    let mut encoded = String::from("@local-windows-wide:");
    for unit in path.as_os_str().encode_wide() {
        let _ = write!(encoded, "{unit:04x}");
    }
    StoragePath::new(encoded).unwrap_or_else(|_| StoragePath::root())
}

#[cfg(all(not(unix), not(windows)))]
fn unrepresentable_path(_path: &Path) -> StoragePath {
    StoragePath::root()
}

fn entry_failure(path: &StoragePath, class: FailureClass) -> EntryOperationFailure {
    entry_failure_with_transience(path, class, Transience::Permanent)
}

fn entry_io_failure(path: &StoragePath, error: &io::Error) -> EntryOperationFailure {
    let (class, transience) = classify_io(error.kind());
    entry_failure_with_transience(path, class, transience)
}

fn entry_failure_with_transience(
    path: &StoragePath,
    class: FailureClass,
    transience: Transience,
) -> EntryOperationFailure {
    EntryOperationFailure::new(
        path.clone(),
        Operation::Traverse,
        class,
        transience,
        "local traversal entry failed",
    )
    .unwrap_or_else(|_| unreachable!("static diagnostic is valid"))
}

fn session_failure(operation: Operation, error: &io::Error) -> BackendSessionFailure {
    let (class, transience) = classify_io(error.kind());
    BackendSessionFailure::new(
        operation,
        class,
        transience,
        "local traversal session failed",
    )
    .unwrap_or_else(|_| unreachable!("static diagnostic is valid"))
}

#[cfg(test)]
mod tests;
