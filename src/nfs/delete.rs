//! Raw recursive deletion. Unlike public traversal, this includes transfer artifacts.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_channel::Sender;
use bytes::Bytes;
use nfs_rs::{DirectoryCookie, ReaddirplusEntry, ReaddirplusPage};
use tokio::task::JoinSet;

use super::{
    FType3, MAX_STALE_RETRIES, NFSStorage, is_retryable_with_invalidation, is_server_busy,
};
use crate::error::StorageError;
use crate::model::StoragePath;
use crate::{DeleteDirIterator, DeleteEvent, Result};

impl NFSStorage {
    /// Deletes the subtree, returning the first listing or deletion failure.
    ///
    /// # Errors
    /// Returns an error if any directory cannot be listed or any child cannot be removed.
    pub async fn delete_dir_all(&self, relative_path: Option<&Path>) -> Result<()> {
        let events = self.delete_dir_all_with_progress(relative_path, 4)?;
        while let Some(event) = events.next().await {
            if let Some(error) = event.error {
                return Err(StorageError::OperationError(error));
            }
        }
        Ok(())
    }

    /// Deletes raw namespace children, including `.data-mover-*` artifacts.
    ///
    /// # Errors
    /// Background failures are reported through the progress iterator's `error` field.
    pub fn delete_dir_all_with_progress(
        &self,
        relative_path: Option<&Path>,
        concurrency: usize,
    ) -> Result<DeleteDirIterator> {
        let (tx, rx) = async_channel::bounded(1000);
        let storage = self.clone();
        let root = relative_path.map(Path::to_path_buf);
        tokio::spawn(async move {
            if let Err(error) = storage
                .delete_raw_tree(root.as_deref(), concurrency.clamp(1, 64), &tx)
                .await
            {
                let _ = tx
                    .send(DeleteEvent::failure(
                        root.unwrap_or_default(),
                        error.to_string(),
                    ))
                    .await;
            }
        });
        Ok(DeleteDirIterator::new(rx))
    }

    async fn delete_raw_tree(
        &self,
        root: Option<&Path>,
        concurrency: usize,
        tx: &Sender<DeleteEvent>,
    ) -> Result<()> {
        let path = root.unwrap_or_else(|| Path::new(""));
        let handle = self.lookup_fh(path).await?.fh;
        let mut pending = vec![(path.to_path_buf(), handle)];
        let mut directories = Vec::new();
        let mut files = JoinSet::new();
        let scanned = async {
            while let Some((path, handle)) = pending.pop() {
                self.delete_raw_children(&path, handle, concurrency, tx, &mut pending, &mut files)
                    .await?;
                directories.push(path);
            }
            Ok(())
        }
        .await;
        // NFSv4.1 RPC futures must finish even after another operation failed. Dropping a
        // JoinSet here would abort REMOVE requests and could fence their session slots.
        drain_files(&mut files, scanned).await?;
        // Parents are recorded before their children, so reversing is post-order.
        for path in directories.into_iter().rev() {
            if path.as_os_str().is_empty() {
                continue;
            }
            self.delete_dir(&path).await?;
            send_progress(tx, path, true).await?;
        }
        Ok(())
    }

    async fn delete_raw_children(
        &self,
        path: &Path,
        mut handle: Bytes,
        concurrency: usize,
        tx: &Sender<DeleteEvent>,
        pending: &mut Vec<(PathBuf, Bytes)>,
        files: &mut JoinSet<Result<()>>,
    ) -> Result<()> {
        let mut position = DirectoryCookie::default();
        let mut reached = HashSet::new();
        reached.insert(position.cookie);
        loop {
            let page = self
                .delete_directory_page(path, &mut handle, position)
                .await?;
            for entry in page.entries {
                if entry.file_name == "." || entry.file_name == ".." {
                    continue;
                }
                self.delete_raw_child(path, entry, concurrency, tx, pending, files)
                    .await?;
            }
            if page.eof {
                return Ok(());
            }
            if !reached.insert(page.next.cookie) {
                return Err(StorageError::NfsError(format!(
                    "Repeated deletion listing cookie for {}",
                    path.display()
                )));
            }
            position = page.next;
        }
    }

    async fn delete_directory_page(
        &self,
        path: &Path,
        handle: &mut Bytes,
        position: DirectoryCookie,
    ) -> Result<ReaddirplusPage> {
        for attempt in 0..=MAX_STALE_RETRIES {
            match self.mount.readdirplus_page(handle.clone(), position).await {
                Ok(page) => return Ok(page),
                Err(error)
                    if position == DirectoryCookie::default()
                        && attempt < MAX_STALE_RETRIES
                        && (is_retryable_with_invalidation(&error) || is_server_busy(&error)) =>
                {
                    if is_retryable_with_invalidation(&error) {
                        let relative = StoragePath::new(path.to_string_lossy().replace('\\', "/"))
                            .map_err(|error| StorageError::InvalidPath(error.to_string()))?;
                        self.forget_role_directory(&relative).await?;
                        *handle = self.lookup_fh(path).await?.fh;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(error) => {
                    return Err(StorageError::NfsError(format!(
                        "Failed to list {} for deletion: {error}",
                        path.display(),
                    )));
                }
            }
        }
        unreachable!("bounded retry loop always returns")
    }

    async fn delete_raw_child(
        &self,
        parent: &Path,
        entry: ReaddirplusEntry,
        concurrency: usize,
        tx: &Sender<DeleteEvent>,
        pending: &mut Vec<(PathBuf, Bytes)>,
        files: &mut JoinSet<Result<()>>,
    ) -> Result<()> {
        let path = parent.join(&entry.file_name);
        let handle = if entry.handle.is_empty() {
            self.lookup_fh(&path).await?.fh
        } else {
            entry.handle
        };
        let attrs = match entry.attr {
            Some(attrs) => attrs,
            None => self.mount.getattr(handle.clone()).await.map_err(|error| {
                StorageError::NfsError(format!(
                    "Failed to stat {} for deletion: {error}",
                    path.display()
                ))
            })?,
        };
        if attrs.type_ == FType3::NF3DIR as u32 {
            pending.push((path, handle));
        } else {
            if files.len() >= concurrency {
                finish_file(files).await?;
            }
            let storage = self.clone();
            let tx = tx.clone();
            files.spawn(async move {
                storage.delete_file(&path).await?;
                send_progress(&tx, path, false).await
            });
        }
        Ok(())
    }
}

async fn drain_files(files: &mut JoinSet<Result<()>>, mut result: Result<()>) -> Result<()> {
    while !files.is_empty() {
        let completed = finish_file(files).await;
        if result.is_ok() {
            result = completed;
        }
    }
    result
}

async fn finish_file(files: &mut JoinSet<Result<()>>) -> Result<()> {
    if let Some(result) = files.join_next().await {
        result.map_err(|error| {
            StorageError::OperationError(format!("NFS deletion task failed: {error}"))
        })??;
    }
    Ok(())
}

async fn send_progress(tx: &Sender<DeleteEvent>, path: PathBuf, is_dir: bool) -> Result<()> {
    tx.send(DeleteEvent::success(path, is_dir))
        .await
        .map_err(|_| StorageError::OperationError("Deletion progress receiver closed".to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[tokio::test]
    async fn failed_listing_waits_for_every_started_remove_and_keeps_first_error() {
        let finished = Arc::new(AtomicBool::new(false));
        let signal = Arc::clone(&finished);
        let mut files = JoinSet::new();
        files.spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            signal.store(true, Ordering::SeqCst);
            Err(StorageError::OperationError(
                "later remove failure".to_owned(),
            ))
        });
        let result = drain_files(
            &mut files,
            Err(StorageError::OperationError("listing failure".to_owned())),
        )
        .await;
        assert!(finished.load(Ordering::SeqCst));
        assert!(files.is_empty());
        assert!(result.is_err_and(|error| error.to_string().contains("listing failure")));
    }

    #[tokio::test]
    async fn closed_progress_receiver_does_not_abort_other_removes() {
        let (tx, rx) = async_channel::bounded(1);
        drop(rx);
        let finished = Arc::new(AtomicBool::new(false));
        let signal = Arc::clone(&finished);
        let mut files = JoinSet::new();
        files.spawn(async move { send_progress(&tx, PathBuf::from("gone"), false).await });
        files.spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            signal.store(true, Ordering::SeqCst);
            Ok(())
        });
        assert!(drain_files(&mut files, Ok(())).await.is_err());
        assert!(finished.load(Ordering::SeqCst));
        assert!(files.is_empty());
    }
}
