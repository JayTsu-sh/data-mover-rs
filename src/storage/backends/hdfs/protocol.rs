use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;

use crate::model::{
    BackendSessionFailure, EntryKind, EntryOperationFailure, FailureClass, Operation, StoragePath,
    Transience,
};
use crate::storage::{ByteStream, StorageRoleFailure};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HdfsEntryFacts {
    pub path: StoragePath,
    pub kind: EntryKind,
    pub size: Option<u64>,
    pub atime: i64,
    pub mtime: i64,
    pub mode: u32,
    pub owner: String,
    pub group: String,
    pub replication: Option<u32>,
    pub block_size: Option<u64>,
}

#[async_trait]
pub(crate) trait HdfsProtocol: Send + Sync {
    async fn stat(&self, path: &StoragePath) -> Result<HdfsEntryFacts, StorageRoleFailure>;
    async fn list(&self, path: &StoragePath) -> Result<Vec<HdfsEntryFacts>, StorageRoleFailure>;
    async fn read_range(
        &self,
        path: &StoragePath,
        range: Range<u64>,
    ) -> Result<Bytes, StorageRoleFailure>;
    async fn create_directory(&self, path: &StoragePath) -> Result<(), StorageRoleFailure>;
    async fn delete(&self, path: &StoragePath, kind: EntryKind) -> Result<(), StorageRoleFailure>;
    async fn rename(
        &self,
        from: &StoragePath,
        to: &StoragePath,
        overwrite: bool,
    ) -> Result<(), StorageRoleFailure>;
    async fn create_empty_stage_exclusive(
        &self,
        path: &StoragePath,
    ) -> Result<(), StorageRoleFailure>;
    async fn append_stage(
        &self,
        path: &StoragePath,
        start_offset: u64,
        expected_size: u64,
        input: ByteStream,
    ) -> Result<u64, StorageRoleFailure>;
    async fn set_mapped_ownership(
        &self,
        path: &StoragePath,
        owner: &str,
        group: &str,
        mode: u32,
    ) -> Result<(), StorageRoleFailure>;
    async fn set_timestamps(
        &self,
        path: &StoragePath,
        atime: Option<i64>,
        mtime: Option<i64>,
    ) -> Result<(), StorageRoleFailure>;
}

pub(crate) fn entry_failure(
    path: &StoragePath,
    operation: Operation,
    class: FailureClass,
    transience: Transience,
) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            operation,
            class,
            transience,
            "HDFS role operation failed",
        )
        .unwrap_or_else(|_| unreachable!("static HDFS diagnostic is valid")),
    )
}

pub(crate) fn session_failure(
    operation: Operation,
    class: FailureClass,
    transience: Transience,
) -> StorageRoleFailure {
    StorageRoleFailure::Session(
        BackendSessionFailure::new(
            operation,
            class,
            transience,
            "HDFS session operation failed",
        )
        .unwrap_or_else(|_| unreachable!("static HDFS diagnostic is valid")),
    )
}

pub(crate) fn cancelled(path: &StoragePath, operation: Operation) -> StorageRoleFailure {
    entry_failure(
        path,
        operation,
        FailureClass::Cancelled,
        Transience::Transient,
    )
}
