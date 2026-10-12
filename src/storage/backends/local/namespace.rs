//! Local namespace role over the same `cap-std` sandbox the other Local roles use.
//!
//! No verb ever follows a symlink, in any path component. Every path is resolved one component
//! at a time from the root capability with `open_dir_nofollow`, so a symlink anywhere along the
//! way (including one swapped in after an earlier `List` saw a directory) makes the verb fail
//! instead of acting on the link's target, and nothing can escape the configured root. The
//! final component is then operated on inside that parent handle: `List` and `Stat` describe a
//! link itself, and `Delete` removes the link, never its target.
//!
//! Directory changes that create or move entries are made durable by synchronizing the parent
//! directories; `Delete` is not, because a recursive delete would otherwise pay one directory
//! sync per entry.
//!
//! Transfer artifacts (`.data-mover-*`, ADR-0006) are invisible here: `List` hides them and no
//! verb addresses them. `Delete` of a directory that only artifacts keep non-empty — empty as
//! seen through `List` — removes them with it.

use std::ffi::OsStr;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use cap_std::fs::{Dir, Metadata};
#[cfg(windows)]
use windows_sys::Win32::Foundation::ERROR_STOPPED_ON_SYMLINK;

use super::observation::{
    backend_facts, classify_io, content_version, entry_kind, path_bytes, source_identity,
    timestamp_metadata,
};
#[cfg(unix)]
use crate::model::OwnershipMode;
use crate::model::{
    BackendIdentity, BackendSessionFailure, EntryKind, EntryOperationFailure, FailureClass,
    Operation, StoragePath, SymlinkTarget, Transience,
};
#[cfg(unix)]
use crate::storage::InlineOwnership;
use crate::storage::artifacts::is_artifact_name;
use crate::storage::durability::sync_directory;
use crate::storage::{
    CaseSensitivity, ListRequest, ListingStream, NameNormalization, NameSemantics, Namespace,
    NamespaceCapabilities, SourceDescriptor, StorageRoleFailure,
};
use hooks::Hooks;
#[cfg(test)]
pub(crate) use hooks::{Hook, ReadFailure};

/// Namespace role for one Local root.
#[derive(Clone)]
pub(crate) struct LocalNamespace {
    root: Arc<Dir>,
    identity: BackendIdentity,
    #[cfg(test)]
    probe: Arc<ListProbe>,
}

/// Test seam that fails one chosen `List` call, standing in for an unreadable directory.
#[cfg(test)]
#[derive(Default)]
struct ListProbe {
    calls: AtomicUsize,
    fail_on_call: AtomicUsize,
    fail_path: std::sync::Mutex<Option<StoragePath>>,
    failed_path: std::sync::Mutex<Option<StoragePath>>,
    hooks: std::sync::Mutex<Hooks>,
}

impl LocalNamespace {
    pub(crate) fn from_root(root: Arc<Dir>, identity: BackendIdentity) -> Self {
        Self {
            root,
            identity,
            #[cfg(test)]
            probe: Arc::new(ListProbe::default()),
        }
    }

    /// Makes the `call_number`-th `List` (1-based) fail with `PermissionDenied`.
    #[cfg(test)]
    pub(crate) fn fail_list_call(&self, call_number: usize) {
        self.probe.fail_on_call.store(call_number, Ordering::SeqCst);
    }

    /// Makes every `List` of `path` fail with `PermissionDenied`, whatever order listings are
    /// issued in.
    #[cfg(test)]
    pub(crate) fn fail_list_path(&self, path: StoragePath) {
        *self
            .probe
            .fail_path
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(path);
    }

    /// Runs `hook` on a directory before it is opened for listing.
    #[cfg(test)]
    pub(crate) fn before_list(&self, hook: Hook) {
        self.lock_hooks().listing = Some(hook);
    }

    /// Runs `hook` on each listed child between reading its directory entry and describing it.
    #[cfg(test)]
    pub(crate) fn before_describe(&self, hook: Hook) {
        self.lock_hooks().describing = Some(hook);
    }

    /// Runs `hook` on a link before its target is read.
    #[cfg(test)]
    pub(crate) fn before_read_link(&self, hook: Hook) {
        self.lock_hooks().reading_link = Some(hook);
    }

    /// Asks `hook` before each directory read whether that read fails instead ([`ReadFailure`]).
    #[cfg(test)]
    pub(crate) fn failing_directory_reads(&self, hook: ReadFailure) {
        self.lock_hooks().reading_directory = Some(hook);
    }

    #[cfg(test)]
    fn lock_hooks(&self) -> std::sync::MutexGuard<'_, Hooks> {
        self.probe
            .hooks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(test)]
    fn hooks(&self) -> Hooks {
        self.lock_hooks().clone()
    }

    #[cfg(not(test))]
    #[allow(clippy::unused_self)]
    fn hooks(&self) -> Hooks {
        Hooks::default()
    }

    /// The directory whose listing the probe failed, once it has.
    #[cfg(test)]
    pub(crate) fn failed_list_path(&self) -> Option<StoragePath> {
        self.probe
            .failed_path
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[cfg(test)]
    fn injected_list_failure(&self, directory: &StoragePath) -> Option<StorageRoleFailure> {
        let call = self.probe.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let by_path = self
            .probe
            .fail_path
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            == Some(directory);
        if !by_path && call != self.probe.fail_on_call.load(Ordering::SeqCst) {
            return None;
        }
        *self
            .probe
            .failed_path
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(directory.clone());
        let error = io::Error::from(io::ErrorKind::PermissionDenied);
        Some(io_failure(directory, Operation::Traverse, &error))
    }

    /// Runs one blocking filesystem operation against the root capability.
    async fn blocking<T, F>(
        &self,
        path: &StoragePath,
        operation: Operation,
        work: F,
    ) -> Result<T, StorageRoleFailure>
    where
        T: Send + 'static,
        F: FnOnce(&Dir, &BackendIdentity) -> Result<T, StorageRoleFailure> + Send + 'static,
    {
        let root = Arc::clone(&self.root);
        let identity = self.identity.clone();
        tokio::task::spawn_blocking(move || work(&root, &identity))
            .await
            .map_err(|_| entry_failure(path, operation, FailureClass::Internal))?
    }

    async fn stat_entry(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        let native = checked(path, Operation::Observe, true)?;
        let target = path.clone();
        self.blocking(path, Operation::Observe, move |root, identity| {
            let metadata = if native.as_os_str().is_empty() {
                root.dir_metadata()
            } else {
                in_parent(root, &native, |parent, leaf| parent.symlink_metadata(leaf))
            }
            .map_err(|error| io_failure(&target, Operation::Observe, &error))?;
            describe(identity, target, &metadata, Operation::Observe)
        })
        .await
    }

    async fn read_link_entry(
        &self,
        path: &StoragePath,
    ) -> Result<SymlinkTarget, StorageRoleFailure> {
        let native = checked(path, Operation::Observe, false)?;
        let target = path.clone();
        let hooks = self.hooks();
        self.blocking(path, Operation::Observe, move |root, _| {
            hooks.before_read_link(&native);
            // `read_link_contents` returns the link text as stored; `read_link` would refuse
            // any absolute target as an escape, although nothing here resolves it.
            let link = in_parent(root, &native, |parent, leaf| {
                parent.read_link_contents(leaf)
            })
            .map_err(|error| io_failure(&target, Operation::Observe, &error))?;
            let (encoding, bytes) = path_bytes(&link)?;
            SymlinkTarget::new(encoding, bytes)
                .map_err(|_| entry_failure(&target, Operation::Observe, FailureClass::Protocol))
        })
        .await
    }

    async fn create_directory_entry(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        let native = checked(path, Operation::Namespace, false)?;
        let target = path.clone();
        self.blocking(path, Operation::Namespace, move |root, _| {
            in_parent(root, &native, |parent, leaf| {
                parent.create_dir(leaf)?;
                sync_directory(parent)
            })
            .map_err(|error| io_failure(&target, Operation::Namespace, &error))
        })
        .await
    }

    async fn delete_one(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        let native = checked(path, Operation::Namespace, false)?;
        let target = path.clone();
        self.blocking(path, Operation::Namespace, move |root, _| {
            in_parent(root, &native, delete_entry)
                .map_err(|error| io_failure(&target, Operation::Namespace, &error))
        })
        .await
    }

    async fn rename_one(
        &self,
        from: &StoragePath,
        to: &StoragePath,
    ) -> Result<(), StorageRoleFailure> {
        let source = checked(from, Operation::Namespace, false)?;
        let destination = checked(to, Operation::Namespace, false)?;
        let target = from.clone();
        self.blocking(from, Operation::Namespace, move |root, _| {
            rename_entry(root, &source, &destination)
                .map_err(|error| io_failure(&target, Operation::Namespace, &error))
        })
        .await
    }
}

#[async_trait]
impl Namespace for LocalNamespace {
    async fn stat(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.stat_entry(path).await
    }

    /// Streams the directory page by page, one blocking read per page ([`listing`], P7 C5).
    fn list(&self, request: ListRequest) -> ListingStream {
        let native = match checked(&request.path, Operation::Traverse, true) {
            Ok(native) => native,
            Err(failure) => return ListingStream::failed(request.path, failure),
        };
        let namespace = self.clone();
        ListingStream::from_producer(request.path.clone(), move |sink| {
            namespace.produce_listing(request, native, sink)
        })
    }

    async fn read_link(&self, path: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        self.read_link_entry(path).await
    }

    async fn create_symlink(
        &self,
        request: crate::storage::CreateSymlinkRequest,
    ) -> Result<(), StorageRoleFailure> {
        symlink::create(self, request).await
    }

    async fn create_directory(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        self.create_directory_entry(path).await
    }

    async fn cleanup_transfer_artifacts(
        &self,
        path: &StoragePath,
    ) -> Result<(), StorageRoleFailure> {
        let native = checked(path, Operation::Namespace, true)?;
        let target = path.clone();
        self.blocking(path, Operation::Namespace, move |root, _| {
            let directory = open_directory(root, &native)
                .map_err(|e| io_failure(&target, Operation::Namespace, &e))?;
            let work = || -> io::Result<()> {
                let mut artifacts = Vec::new();
                for entry in directory.entries()? {
                    let entry = entry?;
                    if is_artifact_name(&entry.file_name()) {
                        artifacts.push((entry.file_name(), entry.file_type()?));
                    }
                }
                for (name, kind) in artifacts {
                    let result = if kind.is_dir() {
                        directory.remove_dir_all(&name)
                    } else {
                        remove_link_or_file(&directory, &name)
                    };
                    if let Err(e) = result
                        && e.kind() != io::ErrorKind::NotFound
                    {
                        return Err(e);
                    }
                }
                sync_directory(&directory)
            };
            work().map_err(|e| io_failure(&target, Operation::Namespace, &e))
        })
        .await
    }

    async fn delete(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        self.delete_one(path).await
    }

    async fn rename(&self, from: &StoragePath, to: &StoragePath) -> Result<(), StorageRoleFailure> {
        self.rename_one(from, to).await
    }

    fn capabilities(&self) -> NamespaceCapabilities {
        NamespaceCapabilities {
            histories: false,
            create_directory: Ok(()),
            delete: Ok(()),
            rename: Ok(()),
            symlinks: true,
            names: platform_names(),
            directory_objects: false,
        }
    }
}

/// How the local filesystem compares names, by platform default: Linux filesystems are
/// byte-exact, Windows and macOS default to case-insensitive volumes.
const fn platform_names() -> NameSemantics {
    if cfg!(target_os = "linux") {
        NameSemantics::EXACT
    } else if cfg!(windows) {
        NameSemantics {
            case: CaseSensitivity::Insensitive,
            normalization: NameNormalization::None,
        }
    } else if cfg!(target_os = "macos") {
        NameSemantics {
            case: CaseSensitivity::Insensitive,
            normalization: NameNormalization::Unknown,
        }
    } else {
        NameSemantics::UNKNOWN
    }
}

/// Builds the descriptor from the same facts [`super::observation`] reports, so a listed entry
/// and an observed one agree on identity, backend fact, content version and timestamps.
fn describe(
    identity: &BackendIdentity,
    path: StoragePath,
    metadata: &Metadata,
    operation: Operation,
) -> Result<SourceDescriptor, StorageRoleFailure> {
    let Some(kind) = entry_kind(metadata) else {
        return Err(entry_failure(&path, operation, FailureClass::Unsupported));
    };
    let source_identity = source_identity(identity, &path, metadata)?;
    let size = (kind == EntryKind::File).then_some(metadata.len());
    let mut descriptor = SourceDescriptor::new(path, kind, size, source_identity)
        .with_backend_fact(backend_facts(metadata).into())
        .with_inline_timestamps(timestamp_metadata(metadata));
    descriptor.content_version = Some(content_version(metadata));
    #[cfg(unix)]
    {
        use cap_std::fs::MetadataExt as _;
        // Ownership exactly as the observation role reads it from this same lstat, so a plan
        // that asks for ownership needs no second one per entry.
        descriptor = descriptor
            .with_inline_mode(metadata.mode() & 0o7777)
            .with_inline_ownership(InlineOwnership::Known(OwnershipMode {
                uid: metadata.uid(),
                gid: metadata.gid(),
                mode: metadata.mode(),
            }));
    }
    Ok(descriptor)
}

/// Opens the directory at `relative` below `root`, one component at a time, refusing a
/// symlink at every step. An empty path is the root itself.
fn open_directory(root: &Dir, relative: &Path) -> io::Result<Dir> {
    let mut current = root.try_clone()?;
    for component in relative.components() {
        let Component::Normal(name) = component else {
            return Err(io::ErrorKind::InvalidInput.into());
        };
        let parent = current.into_std_file();
        let child = cap_primitives::fs::open_dir_nofollow(&parent, Path::new(name))?;
        current = Dir::from_std_file(child);
    }
    Ok(current)
}

/// Runs `work` on the final component of `native` inside its parent, opened without
/// following any symlink.
fn in_parent<T>(
    root: &Dir,
    native: &Path,
    work: impl FnOnce(&Dir, &OsStr) -> io::Result<T>,
) -> io::Result<T> {
    let (Some(parent), Some(leaf)) = (native.parent(), native.file_name()) else {
        return Err(io::ErrorKind::InvalidInput.into());
    };
    work(&open_directory(root, parent)?, leaf)
}

/// Removes one entry of `parent` without following a symlink.
///
/// A directory must be empty as seen through `List`: transfer artifacts left in it are removed
/// with it, but a visible entry keeps the directory and the error, and no artifact beside it is
/// touched. Nothing else is deleted recursively.
fn delete_entry(parent: &Dir, leaf: &OsStr) -> io::Result<()> {
    if parent.symlink_metadata(leaf)?.is_dir() {
        return match parent.remove_dir(leaf) {
            Err(error) if error.kind() == io::ErrorKind::DirectoryNotEmpty => {
                remove_artifact_children(parent, leaf, error)?;
                parent.remove_dir(leaf)
            }
            result => result,
        };
    }
    match parent.remove_file(leaf) {
        // A directory symlink on Windows is removed as a directory; the link, not its target.
        #[cfg(windows)]
        Err(file_error) => parent.remove_dir(leaf).map_err(|_| file_error),
        result => result,
    }
}

/// Removes every child of `parent/leaf` when all of them are transfer artifacts (artifact
/// directories with their contents, never following a symlink); otherwise returns `refused` and
/// removes nothing. An artifact that vanishes meanwhile (a writer published it) is not an error:
/// the caller's final `remove_dir` decides.
fn remove_artifact_children(parent: &Dir, leaf: &OsStr, refused: io::Error) -> io::Result<()> {
    let directory = open_directory(parent, Path::new(leaf))?;
    let mut children = Vec::new();
    for entry in directory.entries()? {
        let entry = entry?;
        if !is_artifact_name(&entry.file_name()) {
            return Err(refused);
        }
        children.push((entry.file_name(), entry.file_type()?));
    }
    for (name, kind) in children {
        let removed = if kind.is_dir() {
            directory.remove_dir_all(&name)
        } else {
            remove_link_or_file(&directory, &name)
        };
        match removed {
            Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
            _ => {}
        }
    }
    Ok(())
}

/// Removes a file or a symlink itself; on Windows a directory symlink or junction is removed as a
/// directory, like `delete_entry` does.
fn remove_link_or_file(directory: &Dir, name: &OsStr) -> io::Result<()> {
    match directory.remove_file(name) {
        #[cfg(windows)]
        Err(file_error) => directory.remove_dir(name).map_err(|_| file_error),
        result => result,
    }
}

/// Moves `source` to `destination`, both resolved without following symlinks, and syncs the
/// parent directories that changed.
fn rename_entry(root: &Dir, source: &Path, destination: &Path) -> io::Result<()> {
    let (Some(from_parent), Some(from_leaf), Some(to_parent), Some(to_leaf)) = (
        source.parent(),
        source.file_name(),
        destination.parent(),
        destination.file_name(),
    ) else {
        return Err(io::ErrorKind::InvalidInput.into());
    };
    let from = open_directory(root, from_parent)?;
    let to = open_directory(root, to_parent)?;
    from.rename(from_leaf, &to, to_leaf)?;
    sync_directory(&to)?;
    if from_parent != to_parent {
        sync_directory(&from)?;
    }
    Ok(())
}

/// Confines `path` to the root and normalizes it to plain components; the root is the empty
/// path. `allow_root` admits the root itself, which only the non-mutating verbs may name.
fn checked(
    path: &StoragePath,
    operation: Operation,
    allow_root: bool,
) -> Result<PathBuf, StorageRoleFailure> {
    let invalid = || entry_failure(path, operation, FailureClass::InvalidInput);
    let mut native = PathBuf::new();
    for component in Path::new(path.as_str()).components() {
        match component {
            Component::Normal(name) if !is_artifact_name(name) => native.push(name),
            Component::CurDir => {}
            _ => return Err(invalid()),
        }
    }
    // A path of only `.` components names the root too.
    if native.as_os_str().is_empty() && !allow_root {
        return Err(invalid());
    }
    Ok(native)
}

/// Classifies an I/O error for a namespace verb. A lost connection (a network mount going
/// away) ends the session; everything else is scoped to the entry.
fn io_failure(path: &StoragePath, operation: Operation, error: &io::Error) -> StorageRoleFailure {
    if is_symlink_refusal(error) {
        return StorageRoleFailure::Entry(entry_failure_value(
            path,
            operation,
            FailureClass::InvalidInput,
            Transience::Permanent,
        ));
    }
    match error.kind() {
        io::ErrorKind::NotConnected | io::ErrorKind::BrokenPipe => {
            let (class, transience) = classify_io(error.kind());
            let failure = BackendSessionFailure::new(
                operation,
                class,
                transience,
                "local namespace session failed",
            )
            .unwrap_or_else(|_| unreachable!("static diagnostic is valid"));
            StorageRoleFailure::Session(failure)
        }
        kind => {
            let (class, transience) = classify(kind);
            StorageRoleFailure::Entry(entry_failure_value(path, operation, class, transience))
        }
    }
}

/// Whether `open_dir_nofollow` refused a symlink path component. Windows reports that as
/// `ERROR_STOPPED_ON_SYMLINK`, which has no stable `io::ErrorKind`; unix reports `ENOTDIR`,
/// which [`classify`] maps.
#[cfg(windows)]
fn is_symlink_refusal(error: &io::Error) -> bool {
    error
        .raw_os_error()
        .and_then(|code| u32::try_from(code).ok())
        == Some(ERROR_STOPPED_ON_SYMLINK)
}

#[cfg(not(windows))]
const fn is_symlink_refusal(_error: &io::Error) -> bool {
    false
}

/// `classify_io`, plus the kinds only namespace verbs meet.
fn classify(kind: io::ErrorKind) -> (FailureClass, Transience) {
    match kind {
        io::ErrorKind::DirectoryNotEmpty => (FailureClass::Conflict, Transience::Permanent),
        // A path component that is not a directory, or a symlink where a directory was
        // required: the request names the wrong kind of entry, and retrying will not help.
        io::ErrorKind::NotADirectory => (FailureClass::InvalidInput, Transience::Permanent),
        _ => classify_io(kind),
    }
}

fn entry_failure(
    path: &StoragePath,
    operation: Operation,
    class: FailureClass,
) -> StorageRoleFailure {
    StorageRoleFailure::Entry(entry_failure_value(
        path,
        operation,
        class,
        Transience::Permanent,
    ))
}

fn entry_failure_value(
    path: &StoragePath,
    operation: Operation,
    class: FailureClass,
    transience: Transience,
) -> EntryOperationFailure {
    EntryOperationFailure::new(
        path.clone(),
        operation,
        class,
        transience,
        "local namespace operation failed",
    )
    .unwrap_or_else(|_| unreachable!("static diagnostic is valid"))
}

mod hooks;
mod listing;
#[cfg(test)]
mod streaming_tests;
mod symlink;
#[cfg(test)]
mod tests;
