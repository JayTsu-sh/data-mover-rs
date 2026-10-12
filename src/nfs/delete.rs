//! Raw recursive deletion. Unlike public traversal, this includes transfer artifacts.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use async_channel::Sender;
use bytes::Bytes;
use nfs_rs::{DirectoryCookie, ReaddirplusEntry};
use tokio::task::JoinSet;

use super::{FType3, NFSStorage};
use crate::error::StorageError;
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
        while let Some((path, handle)) = pending.pop() {
            self.delete_raw_children(&path, handle, concurrency, tx, &mut pending, &mut files)
                .await?;
            directories.push(path);
        }
        while !files.is_empty() {
            finish_file(&mut files).await?;
        }
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
        handle: Bytes,
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
                .mount
                .readdirplus_page(handle.clone(), position)
                .await
                .map_err(|error| {
                    StorageError::NfsError(format!(
                        "Failed to list {} for deletion: {error}",
                        path.display()
                    ))
                })?;
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
