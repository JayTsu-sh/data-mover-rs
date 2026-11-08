//! 精确 key 的永久清理；历史分批删除，当前记录最后删除，不递归处理同前缀子项。

use std::collections::HashSet;
use std::future::Future;

use tokio_util::sync::CancellationToken;

use super::super::histories::record;
use super::super::listing::file_descriptor;
use super::super::paging::Paging;
use super::super::source::role_failure;
use super::super::{S3Protocol, S3Result};
use super::{S3Namespace, failure, refuse_artifact};
use crate::model::{
    EntryKind, FailureClass, ObservedEntry, Operation, RecordKind, StoragePath, StorageTimestamp,
    VersionRecord,
};
use crate::storage::{PurgeOutcome, StorageRoleFailure};

const BATCH_RECORDS: usize = 128;

struct Batch {
    latest: Option<VersionRecord>,
    older: Vec<String>,
    found: bool,
}

impl Batch {
    fn remember(
        &mut self,
        key: &StoragePath,
        version: VersionRecord,
    ) -> Result<(), StorageRoleFailure> {
        self.found = true;
        if version.is_latest() {
            if self
                .latest
                .as_ref()
                .is_some_and(|latest| !same_anchor(latest, &version))
            {
                return Err(failure(
                    key,
                    FailureClass::Protocol,
                    "versions listing has multiple current records; retained",
                ));
            }
            self.latest = Some(version);
        } else if self.older.len() < BATCH_RECORDS {
            let id = version.version_id().unwrap_or("null");
            if !self.older.iter().any(|seen| seen == id) {
                self.older.push(id.to_owned());
            }
        }
        Ok(())
    }
}

impl<P: S3Protocol + 'static> S3Namespace<P> {
    pub(super) async fn purge_entry(
        &self,
        entry: &ObservedEntry,
        cancel: &CancellationToken,
    ) -> Result<PurgeOutcome, StorageRoleFailure> {
        check_cancel(entry.path(), cancel)?;
        if entry.path().as_str().is_empty() {
            return Err(failure(
                entry.path(),
                FailureClass::InvalidInput,
                "cannot purge the storage root",
            ));
        }
        let entry = entry.directory_marker_object().unwrap_or(entry);
        if entry.kind() == EntryKind::Directory && entry.history().is_none() {
            return Ok(PurgeOutcome::VirtualDirectory);
        }
        let key = entry
            .history_source_path()
            .map_err(|_| {
                failure(
                    entry.path(),
                    FailureClass::InvalidInput,
                    "invalid history source path",
                )
            })?
            .unwrap_or_else(|| entry.path().clone());
        refuse_artifact(&key)?;
        if !self.histories {
            return self.purge_current_object(entry, &key, cancel).await;
        }
        let expected = entry
            .history()
            .and_then(|history| history.records().iter().find(|record| record.is_latest()))
            .ok_or_else(|| {
                failure(
                    &key,
                    FailureClass::Unsupported,
                    "purging history requires the observed latest version or delete marker",
                )
            })?;
        self.purge_history(&key, expected, cancel).await
    }

    async fn purge_current_object(
        &self,
        entry: &ObservedEntry,
        key: &StoragePath,
        cancel: &CancellationToken,
    ) -> Result<PurgeOutcome, StorageRoleFailure> {
        let facts = match request(key, cancel, self.protocol.head(key.as_str())).await {
            Ok(facts) => facts,
            Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => {
                return Ok(PurgeOutcome::AlreadyAbsent);
            }
            Err(error) => return Err(error),
        };
        let current = file_descriptor(&self.identity, key.clone(), &facts)
            .map_err(StorageRoleFailure::Entry)?;
        if current.source_identity != *entry.source_identity()
            || current.size != entry.size()
            || !same_modified(facts.last_modified, entry.modified())
        {
            return Err(failure(
                key,
                FailureClass::Conflict,
                "source object changed after observation; retained",
            ));
        }
        check_cancel(key, cancel)?;
        if self.protocol.bucket_capabilities().versions {
            // 从未启用版本的对象也用精确 null 版本删除，避免期间启用版本后留下删除标记。
            self.purge_version(key, facts.version_id.as_deref().unwrap_or("null"), cancel)
                .await?;
        } else {
            request(key, cancel, self.protocol.delete_object(key.as_str())).await?;
        }
        Ok(PurgeOutcome::Deleted { records: 1 })
    }

    async fn purge_history(
        &self,
        key: &StoragePath,
        expected: &VersionRecord,
        cancel: &CancellationToken,
    ) -> Result<PurgeOutcome, StorageRoleFailure> {
        let mut deleted = 0_u64;
        let mut previous: Option<HashSet<String>> = None;
        loop {
            let batch = self.purge_batch(key, cancel).await?;
            if !batch.found {
                return Ok(if deleted == 0 {
                    PurgeOutcome::AlreadyAbsent
                } else {
                    PurgeOutcome::Deleted { records: deleted }
                });
            }
            let latest = batch.latest.as_ref().ok_or_else(|| {
                failure(
                    key,
                    FailureClass::Protocol,
                    "versions listing has no current record; retained",
                )
            })?;
            if !same_anchor(latest, expected) {
                return Err(failure(
                    key,
                    FailureClass::Conflict,
                    "source history changed after observation; current record retained",
                ));
            }
            if batch.older.is_empty() {
                self.purge_version(key, latest.version_id().unwrap_or("null"), cancel)
                    .await?;
                return Ok(PurgeOutcome::Deleted {
                    records: deleted + 1,
                });
            }
            if previous.as_ref().is_some_and(|previous| {
                previous.len() == batch.older.len()
                    && batch.older.iter().all(|id| previous.contains(id))
            }) {
                return Err(failure(
                    key,
                    FailureClass::Protocol,
                    "purge listing did not advance after confirmed deletes; current record retained",
                ));
            }
            for version in &batch.older {
                self.purge_version(key, version, cancel).await?;
                deleted += 1;
            }
            previous = Some(batch.older.into_iter().collect());
            // 删除会改变分页位置；下一批从头列举，不复用已删除记录的游标。
        }
    }

    async fn purge_version(
        &self,
        key: &StoragePath,
        version: &str,
        cancel: &CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        check_cancel(key, cancel)?;
        request(
            key,
            cancel,
            self.protocol.delete_version(key.as_str(), version),
        )
        .await
    }

    async fn purge_batch(
        &self,
        key: &StoragePath,
        cancel: &CancellationToken,
    ) -> Result<Batch, StorageRoleFailure> {
        let mut batch = Batch {
            latest: None,
            older: Vec::with_capacity(BATCH_RECORDS),
            found: false,
        };
        let mut paging = Paging::new(self.limits);
        let mut marker = None;
        loop {
            check_cancel(key, cancel)?;
            let page = request(
                key,
                cancel,
                self.protocol
                    .list_versions_page(key.as_str(), marker.as_ref()),
            )
            .await?;
            let listed = page.versions.len()
                + page.markers.len()
                + page.prefixes.len()
                + page.rejected.len();
            if !page.rejected.is_empty() {
                return Err(failure(
                    key,
                    FailureClass::Protocol,
                    "undecodable version in purge listing; retained",
                ));
            }
            for listed in page
                .versions
                .into_iter()
                .chain(page.markers)
                .filter(|listed| listed.key == key.as_str())
            {
                let version =
                    record(&self.identity, key, listed).map_err(StorageRoleFailure::Entry)?;
                batch.remember(key, version)?;
            }
            if batch.latest.is_some() && batch.older.len() == BATCH_RECORDS {
                return Ok(batch);
            }
            marker = paging.advance_holding(
                key,
                listed,
                batch.older.len() + usize::from(batch.latest.is_some()),
                page.next,
            )?;
            if marker.is_none() {
                return Ok(batch);
            }
        }
    }
}

/// LIST 的毫秒和 HEAD 的 HTTP 秒按实际较粗精度比较；缺失证据不能与已有时间相等。
fn same_modified(actual: Option<StorageTimestamp>, expected: Option<StorageTimestamp>) -> bool {
    match (actual, expected) {
        (Some(actual), Some(expected)) => {
            let quantum = actual
                .precision()
                .quantum_nanos()
                .max(expected.precision().quantum_nanos());
            actual.unix_nanos().div_euclid(quantum) == expected.unix_nanos().div_euclid(quantum)
        }
        (None, None) => true,
        _ => false,
    }
}

fn same_anchor(actual: &VersionRecord, expected: &VersionRecord) -> bool {
    let same_payload = match (actual.kind(), expected.kind()) {
        (
            RecordKind::Object {
                size: a, etag: ea, ..
            },
            RecordKind::Object {
                size: b, etag: eb, ..
            },
        ) => a == b && ea == eb,
        (RecordKind::DeleteMarker { .. }, RecordKind::DeleteMarker { .. }) => true,
        _ => false,
    };
    actual.version_id() == expected.version_id()
        && actual.identity() == expected.identity()
        && same_payload
        // null 记录可以原地替换，不能只靠版本号或 marker 的路径身份保护。
        && (actual.version_id() != Some("null") || actual.modified() == expected.modified())
}

fn check_cancel(key: &StoragePath, cancel: &CancellationToken) -> Result<(), StorageRoleFailure> {
    if cancel.is_cancelled() {
        Err(failure(
            key,
            FailureClass::Cancelled,
            "source purge cancelled",
        ))
    } else {
        Ok(())
    }
}

/// S3 请求不持有 NAS 会话句柄，可以在取消时丢弃等待；已发出的删除结果仍可能未知。
async fn request<T>(
    key: &StoragePath,
    cancel: &CancellationToken,
    operation: impl Future<Output = S3Result<T>>,
) -> Result<T, StorageRoleFailure> {
    tokio::select! {
        biased;
        () = cancel.cancelled() => Err(failure(key, FailureClass::Cancelled, "purge request cancelled; sent mutation outcome may be unknown")),
        result = operation => result.map_err(|error| role_failure(key, Operation::Namespace, error)),
    }
}
