//! Recursive deletion over the [`Namespace`] role with bounded progress items.
//!
//! Replaces the legacy per-backend `delete_dir_all_with_progress`: the tree is listed
//! breadth-first, non-directory entries are deleted concurrently as they are discovered, and
//! directories are removed deepest-first once their contents are gone. Every backend that lends
//! a namespace role gets this behaviour without protocol-specific code.
//!
//! Semantics worth knowing before relying on it:
//!
//! - `Delete` answering `NotFound` counts as success: SMB removes an entry at CLOSE, so a lost
//!   close response can leave the entry already gone.
//! - A directory whose descendants could not all be deleted is not sent to the backend (it would
//!   only answer `Conflict`); it is reported once as an entry failure instead.
//! - Cancellation stops new work but awaits the deletes already in flight, so no backend handle
//!   is abandoned mid-operation.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::{
    CapabilityUnavailable, Namespace, NamespaceRequest, NamespaceResult, PreflightPolicy, Storage,
    StorageRoleFailure,
};
use crate::model::{
    BackendSessionFailure, EntryKind, EntryOperationFailure, FailureClass, Operation, StoragePath,
    Transience,
};

/// One bounded recursive-delete request.
#[derive(Clone, Debug)]
pub struct DeleteTreeRequest {
    /// Directory whose contents are deleted. Must be a directory.
    pub root: StoragePath,
    /// Also remove `root` itself once it is empty. Ignored for the configured backend root,
    /// which is never deleted.
    pub delete_root: bool,
    pub max_inflight_operations: NonZeroUsize,
    pub max_buffered_items: NonZeroUsize,
    pub cancel: CancellationToken,
}

/// One progress item. Entry failures do not terminate the session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteTreeItem {
    Deleted { path: StoragePath, kind: EntryKind },
    EntryFailure(EntryOperationFailure),
}

/// Positive evidence that the delete reached its normal terminal boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteTreeCompletion {
    pub deleted_entries: u64,
    pub deleted_directories: u64,
    pub entry_failures: u64,
}

/// Normal terminal outcomes, distinct from backend/runtime failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteTreeOutcome {
    Completed(DeleteTreeCompletion),
    Cancelled,
}

/// A terminal outcome that cannot be represented as an item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteTreeTerminalFailure {
    Session(BackendSessionFailure),
    Internal,
}

impl fmt::Display for DeleteTreeTerminalFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session(error) => error.fmt(formatter),
            Self::Internal => formatter.write_str("recursive delete runtime failed"),
        }
    }
}

impl std::error::Error for DeleteTreeTerminalFailure {}

/// Bounded item receiver paired with mandatory completion evidence.
pub struct DeleteTreeSession {
    items: mpsc::Receiver<DeleteTreeItem>,
    completion: oneshot::Receiver<Result<DeleteTreeOutcome, DeleteTreeTerminalFailure>>,
    cancel: CancellationToken,
    exhausted: bool,
}

impl DeleteTreeSession {
    /// Receives the next progress item with consumer-driven backpressure.
    pub async fn next_item(&mut self) -> Option<DeleteTreeItem> {
        let item = tokio::select! {
            biased;
            () = self.cancel.cancelled() => {
                self.items.close();
                while self.items.try_recv().is_ok() {}
                None
            }
            item = self.items.recv() => item,
        };
        self.exhausted |= item.is_none();
        item
    }

    /// Returns completion evidence or the unique terminal failure.
    ///
    /// # Errors
    /// Returns `Internal` if called before EOF or if the driver disappears without evidence.
    pub async fn finish(self) -> Result<DeleteTreeOutcome, DeleteTreeTerminalFailure> {
        if !self.exhausted {
            return Err(DeleteTreeTerminalFailure::Internal);
        }
        self.completion
            .await
            .unwrap_or(Err(DeleteTreeTerminalFailure::Internal))
    }
}

/// Starts deleting `request.root`'s subtree through `storage`'s namespace role.
///
/// # Errors
/// Returns the capability failure when the storage lends no namespace role under production
/// preflight policy, before any backend I/O.
pub fn delete_tree(
    storage: &Storage,
    request: DeleteTreeRequest,
) -> Result<DeleteTreeSession, CapabilityUnavailable> {
    let namespace = storage.namespace(&PreflightPolicy::production())?;
    Ok(delete_tree_with_namespace(namespace, request))
}

pub(crate) fn delete_tree_with_namespace(
    namespace: Arc<dyn Namespace>,
    request: DeleteTreeRequest,
) -> DeleteTreeSession {
    let (item_tx, item_rx) = mpsc::channel(request.max_buffered_items.get());
    let (completion_tx, completion_rx) = oneshot::channel();
    let cancel = request.cancel.clone();
    tokio::spawn(run(namespace, request, item_tx, completion_tx));
    DeleteTreeSession {
        items: item_rx,
        completion: completion_rx,
        cancel,
        exhausted: false,
    }
}

type DeleteTasks = JoinSet<(
    StoragePath,
    EntryKind,
    Result<NamespaceResult, StorageRoleFailure>,
)>;

struct Driver {
    namespace: Arc<dyn Namespace>,
    request: DeleteTreeRequest,
    items: mpsc::Sender<DeleteTreeItem>,
    tasks: DeleteTasks,
    /// Directories (other than the root) discovered so far, keyed by depth.
    directories: BTreeMap<usize, Vec<StoragePath>>,
    /// Directories with at least one descendant that could not be deleted.
    incomplete: HashSet<String>,
    /// A backend reported `Cancelled`, which is a signal rather than a failure: the run stops
    /// and reports `Cancelled` instead of counting entry failures.
    cancelled: bool,
    completion: DeleteTreeCompletion,
}

async fn run(
    namespace: Arc<dyn Namespace>,
    request: DeleteTreeRequest,
    items: mpsc::Sender<DeleteTreeItem>,
    completion: oneshot::Sender<Result<DeleteTreeOutcome, DeleteTreeTerminalFailure>>,
) {
    let mut driver = Driver {
        namespace,
        request,
        items,
        tasks: JoinSet::new(),
        directories: BTreeMap::new(),
        incomplete: HashSet::new(),
        cancelled: false,
        completion: DeleteTreeCompletion {
            deleted_entries: 0,
            deleted_directories: 0,
            entry_failures: 0,
        },
    };
    let result = driver.run().await;
    let terminal = if driver.request.cancel.is_cancelled() || driver.cancelled {
        Ok(DeleteTreeOutcome::Cancelled)
    } else {
        result.map(|()| DeleteTreeOutcome::Completed(driver.completion))
    };
    drop(driver.items);
    let _ = completion.send(terminal);
}

impl Driver {
    async fn run(&mut self) -> Result<(), DeleteTreeTerminalFailure> {
        let listing = self.list_and_delete_entries().await;
        // Always drain in-flight deletes, even after cancellation or a listing failure, so no
        // backend operation is abandoned between open and close.
        let drained = self.drain().await;
        listing?;
        drained?;
        if self.request.cancel.is_cancelled() || self.cancelled {
            return Ok(());
        }
        self.delete_directories().await?;
        if self.request.cancel.is_cancelled() || self.cancelled {
            return Ok(());
        }
        if self.request.delete_root && self.request.root != StoragePath::root() {
            let root = self.request.root.clone();
            self.delete_directory(root).await?;
        }
        Ok(())
    }

    async fn list_and_delete_entries(&mut self) -> Result<(), DeleteTreeTerminalFailure> {
        let mut queue = VecDeque::from([(self.request.root.clone(), 0_usize)]);
        while let Some((directory, depth)) = queue.pop_front() {
            if self.request.cancel.is_cancelled() || self.cancelled {
                return Ok(());
            }
            let entries = match self
                .namespace
                .execute(NamespaceRequest::List(directory.clone()))
                .await
            {
                Ok(NamespaceResult::Entries(entries)) => entries,
                Ok(_) => {
                    self.report_failure(entry_failure(
                        &directory,
                        Operation::Traverse,
                        FailureClass::Protocol,
                    ))
                    .await?;
                    continue;
                }
                Err(StorageRoleFailure::Entry(error)) => {
                    self.report_failure(error).await?;
                    continue;
                }
                Err(StorageRoleFailure::Session(error)) => {
                    return Err(DeleteTreeTerminalFailure::Session(error));
                }
            };
            for entry in entries {
                if entry.kind == EntryKind::Directory {
                    self.directories
                        .entry(depth + 1)
                        .or_default()
                        .push(entry.path.clone());
                    queue.push_back((entry.path, depth + 1));
                } else {
                    self.spawn_delete(entry.path, entry.kind).await?;
                }
            }
        }
        Ok(())
    }

    async fn spawn_delete(
        &mut self,
        path: StoragePath,
        kind: EntryKind,
    ) -> Result<(), DeleteTreeTerminalFailure> {
        while self.tasks.len() >= self.request.max_inflight_operations.get() {
            self.settle_one().await?;
        }
        let namespace = Arc::clone(&self.namespace);
        self.tasks.spawn(async move {
            let result = namespace
                .execute(NamespaceRequest::Delete(path.clone()))
                .await;
            (path, kind, result)
        });
        Ok(())
    }

    async fn drain(&mut self) -> Result<(), DeleteTreeTerminalFailure> {
        while !self.tasks.is_empty() {
            self.settle_one().await?;
        }
        Ok(())
    }

    async fn settle_one(&mut self) -> Result<(), DeleteTreeTerminalFailure> {
        match self.tasks.join_next().await {
            Some(Ok((path, kind, result))) => self.report_delete(path, kind, result).await,
            Some(Err(_)) | None => Err(DeleteTreeTerminalFailure::Internal),
        }
    }

    /// Removes discovered directories deepest-first; each level runs with the configured
    /// concurrency because siblings never depend on each other.
    async fn delete_directories(&mut self) -> Result<(), DeleteTreeTerminalFailure> {
        let levels: Vec<Vec<StoragePath>> = std::mem::take(&mut self.directories)
            .into_iter()
            .rev()
            .map(|(_, paths)| paths)
            .collect();
        for paths in levels {
            for path in paths {
                if self.request.cancel.is_cancelled() || self.cancelled {
                    return self.drain().await;
                }
                if self.incomplete.contains(path.as_str()) {
                    self.report_incomplete(&path).await?;
                } else {
                    self.spawn_delete(path, EntryKind::Directory).await?;
                }
            }
            self.drain().await?;
        }
        Ok(())
    }

    async fn delete_directory(
        &mut self,
        path: StoragePath,
    ) -> Result<(), DeleteTreeTerminalFailure> {
        if self.incomplete.contains(path.as_str()) {
            return self.report_incomplete(&path).await;
        }
        let result = self
            .namespace
            .execute(NamespaceRequest::Delete(path.clone()))
            .await;
        self.report_delete(path, EntryKind::Directory, result).await
    }

    async fn report_delete(
        &mut self,
        path: StoragePath,
        kind: EntryKind,
        result: Result<NamespaceResult, StorageRoleFailure>,
    ) -> Result<(), DeleteTreeTerminalFailure> {
        match result {
            Ok(_) => self.report_deleted(path, kind).await,
            Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => {
                self.report_deleted(path, kind).await
            }
            // Cancellation is an upstream signal, not a failed delete. Stop, keep the ancestors,
            // and let the session report `Cancelled` rather than a fabricated conflict chain.
            Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Cancelled => {
                self.cancelled = true;
                self.mark_incomplete(error.path());
                Ok(())
            }
            Err(StorageRoleFailure::Entry(error)) => self.report_failure(error).await,
            Err(StorageRoleFailure::Session(error)) => {
                Err(DeleteTreeTerminalFailure::Session(error))
            }
        }
    }

    async fn report_deleted(
        &mut self,
        path: StoragePath,
        kind: EntryKind,
    ) -> Result<(), DeleteTreeTerminalFailure> {
        if kind == EntryKind::Directory {
            self.completion.deleted_directories += 1;
        } else {
            self.completion.deleted_entries += 1;
        }
        self.send(DeleteTreeItem::Deleted { path, kind }).await
    }

    async fn report_incomplete(
        &mut self,
        path: &StoragePath,
    ) -> Result<(), DeleteTreeTerminalFailure> {
        self.report_failure(
            EntryOperationFailure::new(
                path.clone(),
                Operation::Namespace,
                FailureClass::Conflict,
                Transience::Permanent,
                "directory kept because descendants were not deleted",
            )
            .unwrap_or_else(|_| unreachable!("static diagnostic is valid")),
        )
        .await
    }

    async fn report_failure(
        &mut self,
        error: EntryOperationFailure,
    ) -> Result<(), DeleteTreeTerminalFailure> {
        self.mark_incomplete(error.path());
        self.completion.entry_failures += 1;
        self.send(DeleteTreeItem::EntryFailure(error)).await
    }

    /// A failed entry keeps every ancestor up to the request root from being deleted.
    fn mark_incomplete(&mut self, path: &StoragePath) {
        let mut current = path.as_str();
        // A directory that failed to list or delete is itself incomplete.
        self.incomplete.insert(current.to_owned());
        while let Some((parent, _)) = current.rsplit_once('/') {
            self.incomplete.insert(parent.to_owned());
            current = parent;
        }
        self.incomplete.insert(String::new());
    }

    async fn send(&self, item: DeleteTreeItem) -> Result<(), DeleteTreeTerminalFailure> {
        tokio::select! {
            biased;
            () = self.request.cancel.cancelled() => Ok(()),
            result = self.items.send(item) => result.map_err(|_| DeleteTreeTerminalFailure::Internal),
        }
    }
}

fn entry_failure(
    path: &StoragePath,
    operation: Operation,
    class: FailureClass,
) -> EntryOperationFailure {
    EntryOperationFailure::new(
        path.clone(),
        operation,
        class,
        Transience::Permanent,
        "recursive delete entry failed",
    )
    .unwrap_or_else(|_| unreachable!("static diagnostic is valid"))
}

#[cfg(test)]
mod tests;
