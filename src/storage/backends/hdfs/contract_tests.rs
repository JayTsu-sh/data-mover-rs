use std::collections::HashMap;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::StreamExt as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;

use super::protocol::{HdfsEntryFacts, HdfsProtocol, entry_failure};
use crate::model::{EntryKind, FailureClass, Operation, StoragePath, Transience};
use crate::storage::{ByteStream, StorageRoleFailure};

#[derive(Default)]
pub(crate) struct MemoryHdfs {
    objects: Mutex<HashMap<String, Bytes>>,
    metadata_calls: Mutex<Vec<String>>,
    stat_calls: AtomicUsize,
    fail_write: std::sync::atomic::AtomicBool,
    fail_rename_after_commit: std::sync::atomic::AtomicBool,
}

impl MemoryHdfs {
    pub(crate) async fn insert(&self, path: &str, value: Bytes) {
        self.objects.lock().await.insert(path.into(), value);
    }

    pub(crate) async fn get(&self, path: &str) -> Option<Bytes> {
        self.objects.lock().await.get(path).cloned()
    }

    pub(crate) async fn len(&self) -> usize {
        self.objects.lock().await.len()
    }

    pub(crate) fn fail_writes(&self) {
        self.fail_write
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn fail_rename_after_commit(&self) {
        self.fail_rename_after_commit.store(true, Ordering::SeqCst);
    }

    pub(crate) async fn metadata_calls(&self) -> Vec<String> {
        self.metadata_calls.lock().await.clone()
    }

    pub(crate) fn stat_calls(&self) -> usize {
        self.stat_calls.load(Ordering::SeqCst)
    }

    fn facts(path: &StoragePath, size: u64) -> HdfsEntryFacts {
        HdfsEntryFacts {
            path: path.clone(),
            kind: EntryKind::File,
            size: Some(size),
            atime: 0,
            mtime: 0,
            mode: 0o640,
            owner: "alice".into(),
            group: "users".into(),
            replication: Some(3),
            block_size: Some(128 * 1024 * 1024),
        }
    }

    fn missing(path: &StoragePath, operation: Operation) -> StorageRoleFailure {
        entry_failure(
            path,
            operation,
            FailureClass::NotFound,
            Transience::Permanent,
        )
    }
}

#[async_trait]
impl HdfsProtocol for MemoryHdfs {
    async fn stat(&self, path: &StoragePath) -> Result<HdfsEntryFacts, StorageRoleFailure> {
        self.stat_calls.fetch_add(1, Ordering::SeqCst);
        let objects = self.objects.lock().await;
        let value = objects
            .get(path.as_str())
            .ok_or_else(|| Self::missing(path, Operation::Observe))?;
        Ok(Self::facts(path, value.len() as u64))
    }

    async fn list(&self, path: &StoragePath) -> Result<Vec<HdfsEntryFacts>, StorageRoleFailure> {
        let prefix = if path.as_str().is_empty() {
            String::new()
        } else {
            format!("{}/", path.as_str())
        };
        let objects = self.objects.lock().await;
        Ok(objects
            .iter()
            .filter_map(|(name, value)| {
                let relative = name.strip_prefix(&prefix)?;
                (!relative.contains('/')).then(|| {
                    let path = StoragePath::new(name).unwrap_or_else(|error| panic!("{error}"));
                    Self::facts(&path, value.len() as u64)
                })
            })
            .collect())
    }

    async fn read_range(
        &self,
        path: &StoragePath,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes, StorageRoleFailure> {
        let objects = self.objects.lock().await;
        let value = objects
            .get(path.as_str())
            .ok_or_else(|| Self::missing(path, Operation::Read))?;
        let start = usize::try_from(range.start).map_err(|_| {
            entry_failure(
                path,
                Operation::Read,
                FailureClass::InvalidInput,
                Transience::Permanent,
            )
        })?;
        let end = usize::try_from(range.end).map_err(|_| {
            entry_failure(
                path,
                Operation::Read,
                FailureClass::InvalidInput,
                Transience::Permanent,
            )
        })?;
        Ok(value.slice(start..end))
    }

    async fn create_directory(&self, _path: &StoragePath) -> Result<(), StorageRoleFailure> {
        Ok(())
    }

    async fn delete(&self, path: &StoragePath, _kind: EntryKind) -> Result<(), StorageRoleFailure> {
        self.objects.lock().await.remove(path.as_str());
        Ok(())
    }

    async fn rename(
        &self,
        from: &StoragePath,
        to: &StoragePath,
        overwrite: bool,
    ) -> Result<(), StorageRoleFailure> {
        let mut objects = self.objects.lock().await;
        if !overwrite && objects.contains_key(to.as_str()) {
            return Err(entry_failure(
                to,
                Operation::Publish,
                FailureClass::Conflict,
                Transience::Permanent,
            ));
        }
        let value = objects
            .remove(from.as_str())
            .ok_or_else(|| Self::missing(from, Operation::Publish))?;
        objects.insert(to.as_str().into(), value);
        if self.fail_rename_after_commit.load(Ordering::SeqCst) {
            return Err(entry_failure(
                to,
                Operation::Publish,
                FailureClass::Connectivity,
                Transience::Transient,
            ));
        }
        Ok(())
    }

    async fn prepare_stage(
        &self,
        path: &StoragePath,
        _expected_size: u64,
    ) -> Result<(), StorageRoleFailure> {
        self.objects
            .lock()
            .await
            .insert(path.as_str().into(), Bytes::new());
        Ok(())
    }

    async fn write_stage(
        &self,
        path: &StoragePath,
        expected_size: u64,
        mut input: ByteStream,
    ) -> Result<u64, StorageRoleFailure> {
        if self.fail_write.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(entry_failure(
                path,
                Operation::Write,
                FailureClass::Protocol,
                Transience::Transient,
            ));
        }
        let mut value = BytesMut::new();
        while let Some(chunk) = input.next().await {
            value.extend_from_slice(&chunk?);
        }
        if value.len() as u64 != expected_size {
            return Err(entry_failure(
                path,
                Operation::Write,
                FailureClass::Corruption,
                Transience::Permanent,
            ));
        }
        self.objects
            .lock()
            .await
            .insert(path.as_str().into(), value.freeze());
        Ok(expected_size)
    }

    async fn set_mapped_ownership(
        &self,
        path: &StoragePath,
        owner: &str,
        group: &str,
        mode: u32,
    ) -> Result<(), StorageRoleFailure> {
        self.metadata_calls.lock().await.push(format!(
            "ownership:{}:{owner}:{group}:{mode:o}",
            path.as_str()
        ));
        Ok(())
    }

    async fn set_timestamps(
        &self,
        path: &StoragePath,
        atime: Option<i64>,
        mtime: Option<i64>,
    ) -> Result<(), StorageRoleFailure> {
        self.metadata_calls
            .lock()
            .await
            .push(format!("timestamps:{}:{atime:?}:{mtime:?}", path.as_str()));
        Ok(())
    }
}
