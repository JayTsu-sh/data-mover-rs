//! An in-memory NFS server for the role listing's tests: a tree of directories served as
//! READDIRPLUS replies of a fixed number of entries, with every request counted.
//!
//! Handles are `fh:<path>` plus a generation. [`FakeNfs::replace`] gives a directory a new handle,
//! so the one listings handed out before goes stale, as it does on a server when the directory is
//! removed and made again. Not a protocol mock: it serves the protocol trait the NFS client
//! implements, the seam the listing's producer is written against.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Semaphore;

use super::namespace::{
    NfsListingPages, NfsNamespaceAdapter, NfsNamespaceObservation, NfsNamespaceProtocol,
    NfsReadEnd, NfsReadFailure, NfsRestart,
};
use super::source::NfsProtocolFailure;
use crate::model::{
    BackendIdentity, BackendKind, EntryKind, FailureClass, StoragePath, TimestampMetadata,
    Transience,
};

/// A READDIRPLUS read of `path` that fails once `after` of its children have been read.
#[derive(Clone)]
pub(crate) struct Fault {
    pub(crate) path: String,
    pub(crate) after: usize,
    pub(crate) failure: NfsReadFailure,
    /// How many reads fail this way; later ones read the directory normally.
    pub(crate) times: usize,
}

/// See the [module docs](self).
#[derive(Default)]
pub(crate) struct FakeNfs {
    per_reply: usize,
    directories: HashMap<String, Vec<(String, EntryKind)>>,
    handles: Mutex<HashMap<String, Bytes>>,
    generation: AtomicUsize,
    faults: Mutex<Vec<Fault>>,
    gate: Option<Arc<Semaphore>>,
    /// Directories whose parent's listing hands out a handle that no longer names them.
    listed_stale: Vec<String>,
    /// Names the read hides instead of handing over, as the `NFSv3` reader hides ONTAP's
    /// snapshot directory (P7 C7b).
    hidden_names: Vec<String>,
    /// `directory_handle` calls: one per path resolution, whatever LOOKUPs it would cost.
    pub(crate) lookups: AtomicUsize,
    /// READDIRPLUS requests sent.
    pub(crate) replies: AtomicUsize,
    /// Children read out of replies and pushed.
    pub(crate) read: AtomicUsize,
    /// `forget_directory` calls.
    pub(crate) forgets: AtomicUsize,
    /// Reads started, and reads that have returned (or were dropped).
    pub(crate) reads_started: AtomicUsize,
    pub(crate) reads_ended: AtomicUsize,
    /// The handle each read was given, by path.
    pub(crate) read_with: Mutex<Vec<(String, Bytes)>>,
}

pub(crate) fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl FakeNfs {
    /// A server whose READDIRPLUS replies hold `per_reply` entries each.
    pub(crate) fn new(per_reply: usize) -> Self {
        Self {
            per_reply,
            ..Self::default()
        }
    }

    /// Adds directory `path` (and nothing above it) with these children; a child that is itself
    /// a directory must be added too to be listed.
    pub(crate) fn directory(mut self, path: &str, children: Vec<(String, EntryKind)>) -> Self {
        self.directories.insert(path.to_owned(), children);
        self
    }

    /// Adds a flat directory of `files` files named `f00000`….
    pub(crate) fn flat(self, path: &str, files: usize) -> Self {
        let children = (0..files)
            .map(|index| (format!("f{index:05}"), EntryKind::File))
            .collect();
        self.directory(path, children)
    }

    /// Makes every reply wait for one permit of `gate`.
    pub(crate) fn gated(mut self, gate: Arc<Semaphore>) -> Self {
        self.gate = Some(gate);
        self
    }

    /// Makes every read hide a child named `name` (`NfsListingPages::hide`) instead of handing
    /// it over.
    pub(crate) fn hiding(mut self, name: &str) -> Self {
        self.hidden_names.push(name.to_owned());
        self
    }

    /// Makes the listing of `path`'s parent hand out a stale handle for it.
    pub(crate) fn listed_stale(mut self, path: &str) -> Self {
        self.listed_stale.push(path.to_owned());
        self
    }

    pub(crate) fn fault(&self, fault: Fault) {
        locked(&self.faults).push(fault);
    }

    /// Gives `path` a new handle: the one handed out before no longer names it.
    pub(crate) fn replace(&self, path: &str) {
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        locked(&self.handles).insert(
            path.to_owned(),
            Bytes::from(format!("fh:{path}#{generation}")),
        );
    }

    /// The handle that names `path` now.
    pub(crate) fn handle(&self, path: &str) -> Bytes {
        locked(&self.handles)
            .entry(path.to_owned())
            .or_insert_with(|| Bytes::from(format!("fh:{path}")))
            .clone()
    }

    pub(crate) fn count(counter: &AtomicUsize) -> usize {
        counter.load(Ordering::SeqCst)
    }

    pub(crate) fn namespace(self: &Arc<Self>) -> NfsNamespaceAdapter {
        self.namespace_named("nfs-listing-fake")
    }

    /// A namespace over this server under the endpoint identity `name`.
    pub(crate) fn namespace_named(self: &Arc<Self>, name: &str) -> NfsNamespaceAdapter {
        let identity =
            BackendIdentity::new(BackendKind::Nfs, name).unwrap_or_else(|error| panic!("{error}"));
        NfsNamespaceAdapter::new(Arc::clone(self) as Arc<dyn NfsNamespaceProtocol>, identity)
    }

    fn named_by(&self, handle: &Bytes) -> Option<String> {
        self.directories
            .keys()
            .find(|path| self.handle(path) == *handle)
            .cloned()
    }

    fn take_fault(&self, path: &str) -> Option<Fault> {
        let mut faults = locked(&self.faults);
        let index = faults
            .iter()
            .position(|fault| fault.path == path && fault.times > 0)?;
        faults[index].times -= 1;
        Some(faults[index].clone())
    }

    fn child(&self, directory: &str, name: &str, kind: EntryKind) -> NfsNamespaceObservation {
        let path = if directory.is_empty() {
            name.to_owned()
        } else {
            format!("{directory}/{name}")
        };
        let file_handle = if self.listed_stale.contains(&path) {
            Bytes::from(format!("fh:{path}#stale"))
        } else {
            self.handle(&path)
        };
        NfsNamespaceObservation {
            file_handle,
            path: StoragePath::new(path).unwrap_or_else(|error| panic!("{error}")),
            kind,
            size: (kind == EntryKind::File).then_some(1),
            timestamps: Some(TimestampMetadata {
                accessed: None,
                modified: None,
                created: None,
            }),
            mode: None,
            ownership: None,
        }
    }
}

/// The failure of a read given a handle that no longer names a directory.
pub(crate) fn stale() -> NfsReadFailure {
    NfsReadFailure {
        failure: NfsProtocolFailure::new(FailureClass::Protocol, Transience::Unknown),
        restart: NfsRestart::Relookup,
    }
}

/// Counts a read as ended however it ends, dropped included.
struct Ended<'a>(&'a AtomicUsize);

impl Drop for Ended<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl NfsNamespaceProtocol for FakeNfs {
    async fn stat(
        &self,
        _path: &StoragePath,
    ) -> Result<NfsNamespaceObservation, NfsProtocolFailure> {
        Err(NfsProtocolFailure::protocol())
    }

    async fn directory_handle(&self, path: &StoragePath) -> Result<Bytes, NfsProtocolFailure> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        if self.directories.contains_key(path.as_str()) {
            Ok(self.handle(path.as_str()))
        } else {
            Err(NfsProtocolFailure::new(
                FailureClass::NotFound,
                Transience::Permanent,
            ))
        }
    }

    async fn read_directory(
        &self,
        path: &StoragePath,
        handle: Bytes,
        pages: &mut dyn NfsListingPages,
    ) -> Result<NfsReadEnd, NfsReadFailure> {
        self.reads_started.fetch_add(1, Ordering::SeqCst);
        let _ended = Ended(&self.reads_ended);
        locked(&self.read_with).push((path.as_str().to_owned(), handle.clone()));
        let directory = self.named_by(&handle).ok_or_else(stale)?;
        assert_eq!(
            directory,
            path.as_str(),
            "the handle names another directory"
        );
        let children = &self.directories[&directory];
        let fault = self.take_fault(&directory);
        for (reply, chunk) in children.chunks(self.per_reply.max(1)).enumerate() {
            if pages.stopping() {
                return Ok(NfsReadEnd::Stopped);
            }
            if let Some(gate) = &self.gate
                && let Ok(permit) = gate.acquire().await
            {
                permit.forget();
            }
            self.replies.fetch_add(1, Ordering::SeqCst);
            for (offset, (name, kind)) in chunk.iter().enumerate() {
                if let Some(fault) = &fault
                    && fault.after == reply * self.per_reply + offset
                {
                    return Err(fault.failure.clone());
                }
                if pages.stopping() {
                    return Ok(NfsReadEnd::Stopped);
                }
                self.read.fetch_add(1, Ordering::SeqCst);
                if self.hidden_names.contains(name) {
                    pages.hide();
                    continue;
                }
                if pages.push(Ok(self.child(&directory, name, *kind))) && !pages.hand_over().await {
                    return Ok(NfsReadEnd::Stopped);
                }
            }
        }
        match fault {
            Some(fault) if fault.after >= children.len() => Err(fault.failure),
            _ => Ok(NfsReadEnd::Complete),
        }
    }

    async fn forget_directory(&self, _path: &StoragePath) -> Result<(), NfsProtocolFailure> {
        self.forgets.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn read_link(&self, _path: &StoragePath) -> Result<Bytes, NfsProtocolFailure> {
        Err(NfsProtocolFailure::protocol())
    }

    async fn create_directory(&self, _path: &StoragePath) -> Result<(), NfsProtocolFailure> {
        Err(NfsProtocolFailure::protocol())
    }

    async fn delete(
        &self,
        _path: &StoragePath,
        _kind: EntryKind,
    ) -> Result<(), NfsProtocolFailure> {
        Err(NfsProtocolFailure::protocol())
    }

    async fn rename(
        &self,
        _from: &StoragePath,
        _to: &StoragePath,
    ) -> Result<(), NfsProtocolFailure> {
        Err(NfsProtocolFailure::protocol())
    }
}
