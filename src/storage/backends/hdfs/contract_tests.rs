use std::collections::HashMap;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;

use super::protocol::{HdfsEntryFacts, HdfsProtocol, HdfsWriteSession, entry_failure};
use crate::model::{EntryKind, FailureClass, Operation, StoragePath, Transience};
use crate::storage::StorageRoleFailure;
use crate::storage::artifacts::{ArtifactKind, parse_artifact_name};

#[derive(Default)]
pub(crate) struct MemoryHdfs {
    objects: Mutex<HashMap<String, Bytes>>,
    metadata_calls: Mutex<Vec<String>>,
    stat_calls: AtomicUsize,
    read_active: AtomicUsize,
    read_peak: AtomicUsize,
    read_limit: AtomicUsize,
    write_limit: AtomicUsize,
    write_peak: AtomicUsize,
    append_calls: AtomicUsize,
    stabilize_calls: AtomicUsize,
    recovered_tail: Mutex<Option<Bytes>>,
    hsync_calls: AtomicUsize,
    fail_after_hsync: std::sync::atomic::AtomicBool,
    fail_pointer_write: std::sync::atomic::AtomicBool,
    hsync_completed: std::sync::atomic::AtomicBool,
    delayed_reads: std::sync::atomic::AtomicBool,
    fail_write: std::sync::atomic::AtomicBool,
    fail_rename_after_commit: std::sync::atomic::AtomicBool,
}

/// Whether `path` names a pointer artifact or its temporary, exactly (not a data file whose name
/// merely contains "pointer").
fn is_pointer(path: &StoragePath) -> bool {
    let name = path.as_str().rsplit('/').next().unwrap_or_default();
    parse_artifact_name(name).is_some_and(|parsed| parsed.kind == ArtifactKind::Pointer)
}

impl MemoryHdfs {
    /// Fails the first data write after an hsync: the pointer an at-destination checkpoint
    /// writes right after its hsync is not data and goes through.
    pub(crate) fn fail_once_after_hsync(&self) {
        self.fail_after_hsync.store(true, Ordering::SeqCst);
    }
    /// Fails the next write of a pointer (or its temporary).
    pub(crate) fn fail_once_on_pointer_write(&self) {
        self.fail_pointer_write.store(true, Ordering::SeqCst);
    }
    pub(crate) fn configure_io(&self, read: usize, write: usize) {
        self.read_limit.store(read, Ordering::SeqCst);
        self.write_limit.store(write, Ordering::SeqCst);
        self.delayed_reads.store(true, Ordering::SeqCst);
    }
    pub(crate) fn io_peaks(&self) -> (usize, usize, usize) {
        (
            self.read_peak.load(Ordering::SeqCst),
            self.write_peak.load(Ordering::SeqCst),
            self.append_calls.load(Ordering::SeqCst),
        )
    }
    pub(crate) fn hsync_calls(&self) -> usize {
        self.hsync_calls.load(Ordering::SeqCst)
    }
    pub(crate) async fn reveal_tail_during_lease_recovery(&self, tail: Bytes) {
        *self.recovered_tail.lock().await = Some(tail);
    }
    pub(crate) fn stabilize_calls(&self) -> usize {
        self.stabilize_calls.load(Ordering::SeqCst)
    }
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

    pub(crate) fn allow_writes(&self) {
        self.fail_write.store(false, Ordering::SeqCst);
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
    fn maximum_read_chunk_bytes(&self) -> usize {
        match self.read_limit.load(Ordering::SeqCst) {
            0 => 2 * 1024 * 1024,
            value => value,
        }
    }
    fn maximum_write_chunk_bytes(&self) -> usize {
        match self.write_limit.load(Ordering::SeqCst) {
            0 => 2 * 1024 * 1024,
            value => value,
        }
    }
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
        let _active = ActiveRead(self);
        let active = self.read_active.fetch_add(1, Ordering::SeqCst) + 1;
        self.read_peak.fetch_max(active, Ordering::SeqCst);
        if self.delayed_reads.load(Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
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

    async fn claim_stage(
        &self,
        from: &StoragePath,
        claimed: &StoragePath,
    ) -> Result<(), StorageRoleFailure> {
        let mut objects = self.objects.lock().await;
        if objects.contains_key(claimed.as_str()) {
            return Err(entry_failure(
                claimed,
                Operation::Prepare,
                FailureClass::Conflict,
                Transience::Permanent,
            ));
        }
        let value = objects
            .remove(from.as_str())
            .ok_or_else(|| Self::missing(from, Operation::Prepare))?;
        objects.insert(claimed.as_str().into(), value);
        Ok(())
    }

    async fn create_empty_stage_exclusive(
        &self,
        path: &StoragePath,
    ) -> Result<(), StorageRoleFailure> {
        let mut objects = self.objects.lock().await;
        if objects.contains_key(path.as_str()) {
            return Err(entry_failure(
                path,
                Operation::Prepare,
                FailureClass::Conflict,
                Transience::Permanent,
            ));
        }
        objects.insert(path.as_str().into(), Bytes::new());
        Ok(())
    }

    async fn open_stage_writer(
        &self,
        path: &StoragePath,
        start_offset: u64,
        direct: bool,
    ) -> Result<Box<dyn HdfsWriteSession + '_>, StorageRoleFailure> {
        // Data writers only: the pointer an at-destination stage writes is not an append of it.
        if !is_pointer(path) {
            self.append_calls.fetch_add(1, Ordering::SeqCst);
        }
        if self.fail_write.load(Ordering::SeqCst) {
            return Err(entry_failure(
                path,
                Operation::Write,
                FailureClass::Protocol,
                Transience::Transient,
            ));
        }
        let mut objects = self.objects.lock().await;
        if direct {
            if start_offset != 0 {
                return Err(entry_failure(
                    path,
                    Operation::Write,
                    FailureClass::InvalidInput,
                    Transience::Permanent,
                ));
            }
            objects.insert(path.as_str().into(), Bytes::new());
        } else {
            let current = objects
                .get(path.as_str())
                .ok_or_else(|| Self::missing(path, Operation::Write))?;
            if current.len() as u64 != start_offset {
                return Err(entry_failure(
                    path,
                    Operation::Write,
                    FailureClass::Conflict,
                    Transience::Permanent,
                ));
            }
        }
        drop(objects);
        Ok(Box::new(MemoryWriteSession {
            storage: self,
            path: path.clone(),
            offset: start_offset,
        }))
    }

    async fn stabilize_recovered_stage(
        &self,
        path: &StoragePath,
    ) -> Result<u64, StorageRoleFailure> {
        self.stabilize_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(tail) = self.recovered_tail.lock().await.take() {
            self.objects.lock().await.insert(path.as_str().into(), tail);
        }
        let objects = self.objects.lock().await;
        let value = objects
            .get(path.as_str())
            .ok_or_else(|| Self::missing(path, Operation::Prepare))?;
        Ok(value.len() as u64)
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

    async fn set_mode(&self, path: &StoragePath, mode: u32) -> Result<(), StorageRoleFailure> {
        self.metadata_calls
            .lock()
            .await
            .push(format!("mode:{}:{mode:o}", path.as_str()));
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

struct MemoryWriteSession<'a> {
    storage: &'a MemoryHdfs,
    path: StoragePath,
    offset: u64,
}

#[async_trait]
impl HdfsWriteSession for MemoryWriteSession<'_> {
    async fn write(&mut self, data: Bytes) -> Result<usize, StorageRoleFailure> {
        let pointer = is_pointer(&self.path);
        if (pointer
            && self
                .storage
                .fail_pointer_write
                .swap(false, Ordering::SeqCst))
            || (self.storage.hsync_completed.load(Ordering::SeqCst)
                && !pointer
                && self.storage.fail_after_hsync.swap(false, Ordering::SeqCst))
        {
            return Err(entry_failure(
                &self.path,
                Operation::Write,
                FailureClass::Protocol,
                Transience::Transient,
            ));
        }
        self.storage
            .write_peak
            .fetch_max(data.len(), Ordering::SeqCst);
        let mut objects = self.storage.objects.lock().await;
        let value = objects
            .get_mut(self.path.as_str())
            .ok_or_else(|| MemoryHdfs::missing(&self.path, Operation::Write))?;
        if value.len() as u64 != self.offset {
            return Err(entry_failure(
                &self.path,
                Operation::Write,
                FailureClass::Conflict,
                Transience::Permanent,
            ));
        }
        let count = data.len();
        let mut appended = BytesMut::from(value.as_ref());
        appended.extend_from_slice(&data);
        *value = appended.freeze();
        self.offset = self
            .offset
            .checked_add(count as u64)
            .ok_or_else(|| MemoryHdfs::missing(&self.path, Operation::Write))?;
        Ok(count)
    }

    async fn hsync(&mut self) -> Result<(), StorageRoleFailure> {
        self.storage.hsync_calls.fetch_add(1, Ordering::SeqCst);
        self.storage.hsync_completed.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn close(self: Box<Self>) -> Result<(), StorageRoleFailure> {
        Ok(())
    }
}

#[tokio::test]
async fn exclusive_stage_creation_preserves_existing_partial() {
    let storage = MemoryHdfs::default();
    let path = StoragePath::new("partial").unwrap_or_else(|error| panic!("{error}"));

    storage
        .create_empty_stage_exclusive(&path)
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    let conflict = storage.create_empty_stage_exclusive(&path).await;

    assert!(conflict.is_err());
    assert_eq!(storage.get("partial").await, Some(Bytes::new()));
}

#[tokio::test]
async fn claim_stage_failures_are_prepare_operations() {
    let storage = MemoryHdfs::default();
    let base = StoragePath::new("base").unwrap_or_else(|error| panic!("{error}"));
    let claimed = StoragePath::new("claimed").unwrap_or_else(|error| panic!("{error}"));
    storage.insert("claimed", Bytes::new()).await;

    let conflict = storage.claim_stage(&base, &claimed).await;
    assert!(matches!(conflict, Err(StorageRoleFailure::Entry(error))
        if error.operation() == Operation::Prepare && error.class() == FailureClass::Conflict));

    storage
        .delete(&claimed, EntryKind::File)
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    let missing = storage.claim_stage(&base, &claimed).await;
    assert!(matches!(missing, Err(StorageRoleFailure::Entry(error))
        if error.operation() == Operation::Prepare && error.class() == FailureClass::NotFound));
}

struct ActiveRead<'a>(&'a MemoryHdfs);
impl Drop for ActiveRead<'_> {
    fn drop(&mut self) {
        self.0.read_active.fetch_sub(1, Ordering::SeqCst);
    }
}
