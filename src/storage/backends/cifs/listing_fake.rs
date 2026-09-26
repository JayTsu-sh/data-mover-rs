//! In-memory CIFS directories for the listing's tests: a tree served record by record through the
//! [`CifsDirectory`] seam, with every open, close and record counted.
//!
//! Not a protocol mock: it serves the protocol trait the smb-rs client implements, the seam the
//! listing's producer is written against. Like smb-rs's `QueryDirectoryStream`, a directory's
//! stream goes on after an error, so a reader that does not stop at the first one reads on.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt as _;
use futures::stream::{self, BoxStream};
use tokio::sync::Semaphore;

use super::metadata::CifsInlineMetadata;
use super::namespace::{
    CifsDirectory, CifsListedChild, CifsNamespace, CifsNamespaceProtocol, CifsUnspelled,
};
use super::source::CifsSourceFacts;
use crate::model::{BackendIdentity, BackendKind, EntryKind, StoragePath};
use crate::storage::Namespace;

/// Makes the error a fault injects (smb-rs errors are not `Clone`).
pub(crate) type MakeError = fn() -> smb_domain::Error;

/// A read of `path` whose record number `after` (counting from 0) is an error instead, for the
/// first `times` reads; later reads go through. Each read takes at most one fault, the first
/// registered that has reads left. As an open fault, `after` is unused.
#[derive(Clone)]
pub(crate) struct Fault {
    pub(crate) path: String,
    pub(crate) after: usize,
    pub(crate) error: MakeError,
    pub(crate) times: usize,
}

/// See the [module docs](self).
#[derive(Default)]
pub(crate) struct FakeCifs {
    directories: HashMap<String, Vec<(String, EntryKind)>>,
    /// When set, every record taken needs one permit.
    gate: Option<Arc<Semaphore>>,
    faults: Mutex<Vec<Fault>>,
    /// Opens of a path that fail, for the first `times` opens.
    open_faults: Mutex<Vec<Fault>>,
    /// Whether every close fails (the handle is gone all the same).
    failing_close: AtomicBool,
    pub(crate) counts: Arc<Counts>,
}

/// What the fake counts; shared with the directories it opens.
#[derive(Default)]
pub(crate) struct Counts {
    /// Opens that succeeded, and closes.
    pub(crate) opens: AtomicUsize,
    pub(crate) closes: AtomicUsize,
    /// Opens attempted, failed ones included.
    pub(crate) open_attempts: AtomicUsize,
    /// Handles open now, and the most ever open at once.
    pub(crate) open_now: AtomicUsize,
    pub(crate) peak_open: AtomicUsize,
    /// Records taken out of directory streams, errors included.
    pub(crate) read: AtomicUsize,
}

pub(crate) fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl FakeCifs {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Adds a directory with these children, in this order.
    #[must_use]
    pub(crate) fn directory(mut self, path: &str, children: Vec<(String, EntryKind)>) -> Self {
        self.directories.insert(path.to_owned(), children);
        self
    }

    /// Adds a directory of `files` files named `f00000`, `f00001`, ...
    #[must_use]
    pub(crate) fn flat(self, path: &str, files: usize) -> Self {
        let children = (0..files)
            .map(|index| (format!("f{index:05}"), EntryKind::File))
            .collect();
        self.directory(path, children)
    }

    #[must_use]
    pub(crate) fn gated(mut self, gate: Arc<Semaphore>) -> Self {
        self.gate = Some(gate);
        self
    }

    pub(crate) fn fault(&self, fault: Fault) {
        locked(&self.faults).push(fault);
    }

    pub(crate) fn open_fault(&self, fault: Fault) {
        locked(&self.open_faults).push(fault);
    }

    pub(crate) fn fail_closes(&self) {
        self.failing_close.store(true, Ordering::SeqCst);
    }

    pub(crate) fn count(counter: &AtomicUsize) -> usize {
        counter.load(Ordering::SeqCst)
    }

    pub(super) fn namespace(self: &Arc<Self>) -> CifsNamespace {
        let identity = BackendIdentity::new(BackendKind::Cifs, "smb://fake/share")
            .unwrap_or_else(|error| panic!("{error}"));
        CifsNamespace::new(Arc::clone(self), identity)
    }

    /// The namespace role over this fake, for traversal tests.
    pub(crate) fn role(self: &Arc<Self>) -> Arc<dyn Namespace> {
        Arc::new(self.namespace())
    }

    /// The error an open of `path` fails with, used up once its opens are spent.
    fn open_fault_for(&self, path: &str) -> Option<MakeError> {
        let mut faults = locked(&self.open_faults);
        let fault = faults
            .iter_mut()
            .find(|fault| fault.path == path && fault.times > 0)?;
        fault.times -= 1;
        Some(fault.error)
    }
}

/// A listed child as the smb-rs wrapper makes it, with fixed timestamps and no file id.
pub(super) fn listed_child(directory: &str, name: &str, kind: EntryKind) -> CifsListedChild {
    let parent = StoragePath::new(directory).unwrap_or_else(|error| panic!("{error}"));
    let child = CifsUnspelled::spell(&parent, name)?;
    let stamp = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
    Ok((
        child,
        CifsInlineMetadata {
            facts: CifsSourceFacts {
                kind,
                size: 3,
                identity: Bytes::from_static(b"listed"),
                file_id: None,
                maximum_read_chunk: u32::MAX,
            },
            accessed: stamp,
            modified: stamp,
            created: stamp,
            readonly: Some(false),
            reparse_point: false,
        },
    ))
}

#[async_trait]
impl CifsNamespaceProtocol for FakeCifs {
    async fn stat(&self, _path: &StoragePath) -> smb_domain::Result<CifsSourceFacts> {
        Err(smb_domain::Error::InvalidArgument(
            "the listing fake has no stat".into(),
        ))
    }

    async fn open_directory(
        &self,
        path: &StoragePath,
    ) -> smb_domain::Result<Box<dyn CifsDirectory>> {
        let counts = &self.counts;
        counts.open_attempts.fetch_add(1, Ordering::SeqCst);
        if let Some(error) = self.open_fault_for(path.as_str()) {
            return Err(error());
        }
        let children = self
            .directories
            .get(path.as_str())
            .cloned()
            .ok_or_else(|| smb_domain::Error::UnexpectedMessageStatus(0xC000_0034))?;
        counts.opens.fetch_add(1, Ordering::SeqCst);
        let now = counts.open_now.fetch_add(1, Ordering::SeqCst) + 1;
        counts.peak_open.fetch_max(now, Ordering::SeqCst);
        Ok(Box::new(
            FakeDirectory {
                path: path.as_str().to_owned(),
                children,
                fault: None,
                gate: self.gate.clone(),
                counts: Arc::clone(counts),
                failing_close: self.failing_close.load(Ordering::SeqCst),
            }
            .with_faults(self),
        ))
    }

    async fn create_directory(&self, _path: &StoragePath) -> smb_domain::Result<()> {
        Ok(())
    }

    async fn remove(&self, _path: &StoragePath) -> smb_domain::Result<()> {
        Ok(())
    }

    async fn rename_entry(&self, _from: &StoragePath, _to: &StoragePath) -> smb_domain::Result<()> {
        Ok(())
    }
}

/// One open directory of the fake.
struct FakeDirectory {
    path: String,
    children: Vec<(String, EntryKind)>,
    /// The record of this read that is an error instead, and the error.
    fault: Option<(usize, MakeError)>,
    gate: Option<Arc<Semaphore>>,
    counts: Arc<Counts>,
    failing_close: bool,
}

impl FakeDirectory {
    /// Takes the first fault registered for this directory that has reads left: one per read.
    fn with_faults(mut self, fake: &FakeCifs) -> Self {
        let mut faults = locked(&fake.faults);
        if let Some(fault) = faults
            .iter_mut()
            .find(|fault| fault.path == self.path && fault.times > 0)
        {
            fault.times -= 1;
            self.fault = Some((fault.after, fault.error));
        }
        drop(faults);
        self
    }
}

#[async_trait]
impl CifsDirectory for FakeDirectory {
    fn entries(&self) -> BoxStream<'_, smb_domain::Result<CifsListedChild>> {
        stream::iter(0..self.children.len())
            .then(move |index| async move {
                if let Some(gate) = &self.gate
                    && let Ok(permit) = gate.acquire().await
                {
                    permit.forget();
                }
                self.counts.read.fetch_add(1, Ordering::SeqCst);
                if let Some((_, error)) = self.fault.filter(|(at, _)| *at == index) {
                    return Err(error());
                }
                let (name, kind) = &self.children[index];
                Ok(listed_child(&self.path, name, *kind))
            })
            .boxed()
    }

    async fn close(self: Box<Self>) -> smb_domain::Result<()> {
        self.counts.closes.fetch_add(1, Ordering::SeqCst);
        self.counts.open_now.fetch_sub(1, Ordering::SeqCst);
        if self.failing_close {
            return Err(smb_domain::Error::OutcomeUnknown);
        }
        Ok(())
    }
}

/// A directory handing out children read beforehand, for fakes that list whole directories.
pub(super) struct MemoryDirectory {
    children: Mutex<Option<Vec<CifsListedChild>>>,
}

impl MemoryDirectory {
    pub(super) fn boxed(children: Vec<CifsListedChild>) -> Box<dyn CifsDirectory> {
        Box::new(Self {
            children: Mutex::new(Some(children)),
        })
    }
}

#[async_trait]
impl CifsDirectory for MemoryDirectory {
    fn entries(&self) -> BoxStream<'_, smb_domain::Result<CifsListedChild>> {
        let children = locked(&self.children).take().unwrap_or_default();
        stream::iter(children.into_iter().map(Ok)).boxed()
    }

    async fn close(self: Box<Self>) -> smb_domain::Result<()> {
        Ok(())
    }
}
