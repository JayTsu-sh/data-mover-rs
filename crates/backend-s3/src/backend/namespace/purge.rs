//! 精确 key 的永久清理；历史分批删除，当前记录最后删除，不递归处理同前缀子项。

use std::collections::HashSet;
use std::future::Future;

use tokio_util::sync::CancellationToken;

use super::super::histories::record;
use super::super::listing::file_descriptor;
use super::super::paging::Paging;
use super::super::source::role_failure;
use super::super::{S3Protocol, S3ProtocolFailure, S3Result};
use super::{S3Namespace, failure, refuse_artifact};
use crate::model::{
    EntryKind, FailureClass, ObservedEntry, Operation, RecordKind, SourceVersion, StoragePath,
    StorageTimestamp, Transience, VersionRecord,
};
use crate::storage::{PurgeFailure, PurgeOutcome, StorageRoleFailure};

const BATCH_RECORDS: usize = 100;

struct Batch {
    latest: Option<VersionRecord>,
    older: Vec<VersionRecord>,
    found: bool,
}

impl Batch {
    fn remember(
        &mut self,
        key: &StoragePath,
        version: VersionRecord,
    ) -> Result<(), StorageRoleFailure> {
        self.found = true;
        if self.current_and_history_conflict(&version) {
            return Err(failure(
                key,
                FailureClass::Protocol,
                "the same version is listed as current and historical; current record retained",
            ));
        }
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
            if !self
                .older
                .iter()
                .any(|seen| seen.version_id().unwrap_or("null") == id)
            {
                self.older.push(version);
            }
        }
        Ok(())
    }

    fn current_and_history_conflict(&self, version: &VersionRecord) -> bool {
        let id = version.version_id().unwrap_or("null");
        if version.is_latest() {
            self.older
                .iter()
                .any(|older| older.version_id().unwrap_or("null") == id)
        } else {
            self.latest
                .as_ref()
                .is_some_and(|latest| latest.version_id().unwrap_or("null") == id)
        }
    }
}

impl<P: S3Protocol + 'static> S3Namespace<P> {
    pub(super) async fn purge_entry(
        &self,
        entry: &ObservedEntry,
        cancel: &CancellationToken,
    ) -> Result<PurgeOutcome, PurgeFailure> {
        check_cancel(entry.path(), cancel)?;
        if entry.path().as_str().is_empty() {
            return Err(failure(
                entry.path(),
                FailureClass::InvalidInput,
                "cannot purge the storage root",
            )
            .into());
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
    ) -> Result<PurgeOutcome, PurgeFailure> {
        let facts = match request(key, cancel, self.protocol.head(key.as_str())).await {
            Ok(facts) => facts,
            Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => {
                return Ok(PurgeOutcome::AlreadyAbsent);
            }
            Err(error) => return Err(error.into()),
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
            )
            .into());
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
    ) -> Result<PurgeOutcome, PurgeFailure> {
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
                )
                .into());
            }
            if batch.older.is_empty() {
                self.purge_version(key, latest.version_id().unwrap_or("null"), cancel)
                    .await?;
                return Ok(PurgeOutcome::Deleted {
                    records: deleted + 1,
                });
            }
            if previous.as_ref().is_some_and(|previous| {
                batch
                    .older
                    .iter()
                    .any(|record| previous.contains(record.version_id().unwrap_or("null")))
            }) {
                return Err(failure(
                    key,
                    FailureClass::Protocol,
                    "purge listing did not advance after confirmed deletes; current record retained",
                ).into());
            }
            self.purge_older(key, &batch.older, cancel).await?;
            deleted += batch.older.len() as u64;
            previous = Some(
                batch
                    .older
                    .iter()
                    .map(|record| record.version_id().unwrap_or("null").to_owned())
                    .collect(),
            );
            // 删除会改变分页位置；下一批从头列举，不复用已删除记录的游标。
        }
    }

    async fn purge_version(
        &self,
        key: &StoragePath,
        version: &str,
        cancel: &CancellationToken,
    ) -> Result<(), PurgeFailure> {
        check_cancel(key, cancel).map_err(|source| version_failure(version, source))?;
        request(
            key,
            cancel,
            self.protocol.delete_version(key.as_str(), version),
        )
        .await
        .map_err(|source| version_failure(version, source))
    }

    async fn purge_older(
        &self,
        key: &StoragePath,
        records: &[VersionRecord],
        cancel: &CancellationToken,
    ) -> Result<(), PurgeFailure> {
        let versions: Vec<_> = records
            .iter()
            .filter(|record| !record.is_delete_marker())
            .map(|record| record.version_id().unwrap_or("null").to_owned())
            .collect();
        if !versions.is_empty() {
            self.purge_older_data(key, &versions, cancel).await?;
        }
        // Some stores acknowledge every marker in DeleteObjects but remove only one.
        // Keep same-key marker mutations separate; the current anchor remains last.
        for marker in records.iter().filter(|record| record.is_delete_marker()) {
            self.purge_version(key, marker.version_id().unwrap_or("null"), cancel)
                .await?;
        }
        Ok(())
    }

    async fn purge_older_data(
        &self,
        key: &StoragePath,
        versions: &[String],
        cancel: &CancellationToken,
    ) -> Result<(), PurgeFailure> {
        check_cancel(key, cancel)?;
        let selected: Vec<_> = versions
            .iter()
            .map(|id| (key.as_str().to_owned(), SourceVersion::Id(id.clone())))
            .collect();
        let results = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                let cause = cancelled(key, true);
                selected.iter().map(|_| Err(cause.clone())).collect()
            }
            results = self.protocol.delete_objects(&selected) => results.into_iter()
                .map(|result| result.map_err(|error| role_failure(key, Operation::Namespace, error)))
                .collect(),
        };
        batch_result(key, versions, results)
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

fn version_failure(version: &str, source: StorageRoleFailure) -> PurgeFailure {
    PurgeFailure {
        version: Some(SourceVersion::Id(version.to_owned())),
        source: Box::new(source),
        additional: Vec::new(),
    }
}

fn batch_result(
    key: &StoragePath,
    versions: &[String],
    results: Vec<Result<(), StorageRoleFailure>>,
) -> Result<(), PurgeFailure> {
    let mismatch = (results.len() != versions.len()).then(|| {
        role_failure(
            key,
            Operation::Namespace,
            S3ProtocolFailure::protocol(
                "purge batch result count does not match requested versions; outcomes unknown",
            ),
        )
    });
    let mut results = results.into_iter();
    let mut failures = versions.iter().filter_map(|id| {
        let cause = match results.next() {
            Some(Err(cause)) => Some(cause),
            Some(Ok(())) | None => mismatch.clone(),
        };
        cause.map(|cause| (SourceVersion::Id(id.clone()), cause))
    });
    if let Some(source) = &mismatch {
        return Err(PurgeFailure {
            version: None,
            source: Box::new(source.clone()),
            additional: failures.collect(),
        });
    }
    let Some((version, source)) = failures.next() else {
        return Ok(());
    };
    Err(PurgeFailure {
        version: Some(version),
        source: Box::new(source),
        additional: failures.collect(),
    })
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

fn cancelled(key: &StoragePath, submitted: bool) -> StorageRoleFailure {
    role_failure(
        key,
        Operation::Namespace,
        S3ProtocolFailure::entry(
            FailureClass::Cancelled,
            if submitted {
                Transience::Unknown
            } else {
                Transience::Transient
            },
            if submitted {
                "purge cancelled; submitted deletion outcomes may be unknown"
            } else {
                "source purge cancelled before request submission"
            },
        ),
    )
}

fn check_cancel(key: &StoragePath, cancel: &CancellationToken) -> Result<(), StorageRoleFailure> {
    if cancel.is_cancelled() {
        Err(cancelled(key, false))
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
        () = cancel.cancelled() => Err(cancelled(key, true)),
        result = operation => result.map_err(|error| role_failure(key, Operation::Namespace, error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AssertTestError;
    use crate::model::{BackendIdentity, BackendKind, IdentityStrength, SourceIdentity};

    type Result = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn record(
        latest: bool,
        marker: bool,
        version: Option<&str>,
    ) -> std::result::Result<VersionRecord, Box<dyn std::error::Error + Send + Sync>> {
        let identity = BackendIdentity::new(BackendKind::S3, "purge-contradiction")?;
        let kind = if marker {
            RecordKind::DeleteMarker { modified: None }
        } else {
            RecordKind::Object {
                size: 7,
                etag: "etag".into(),
                modified: None,
            }
        };
        let strength = if version.is_some() {
            IdentityStrength::VersionScoped
        } else {
            IdentityStrength::PathScoped
        };
        Ok(VersionRecord::new(
            version.map(str::to_owned),
            latest,
            kind,
            SourceIdentity::new(identity, strength, b"same-version")?,
            None,
        )?)
    }

    fn reject_conflicting_current_flag(marker: bool) -> Result {
        let key = StoragePath::new("payload")?;
        for (version, current_first) in [Some("same-version"), None]
            .into_iter()
            .flat_map(|version| [false, true].map(|first| (version, first)))
        {
            let mut batch = Batch {
                latest: None,
                older: Vec::new(),
                found: false,
            };
            batch.remember(&key, record(current_first, marker, version)?)?;
            let error = batch
                .remember(&key, record(!current_first, marker, version)?)
                .err()
                .ok_or("contradictory current/historical version was accepted")?;
            assert!(
                matches!(error, StorageRoleFailure::Entry(ref cause)
                if cause.class() == FailureClass::Protocol
                    && cause.diagnostic().contains("current and historical")),
                "{error:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn data_version_cannot_be_both_current_and_historical() -> Result {
        reject_conflicting_current_flag(false)
    }

    #[test]
    fn delete_marker_cannot_be_both_current_and_historical() -> Result {
        reject_conflicting_current_flag(true)
    }

    #[test]
    fn all_batch_causes_keep_their_versions_and_typed_scope() -> Result {
        let key = StoragePath::new("payload")?;
        let versions: Vec<_> = (0..100)
            .map(|index| {
                if index == 0 {
                    "null".into()
                } else {
                    format!("v{index}")
                }
            })
            .collect();
        let results: Vec<_> = (0..100)
            .map(|index| {
                Err(role_failure(
                    &key,
                    Operation::Namespace,
                    if index % 2 == 0 {
                        S3ProtocolFailure::entry(
                            FailureClass::PermissionDenied,
                            Transience::Permanent,
                            "AccessDenied",
                        )
                    } else {
                        S3ProtocolFailure::session(
                            FailureClass::Timeout,
                            Transience::Unknown,
                            "lost batch receipt",
                        )
                    },
                ))
            })
            .collect();
        let failure = batch_result(&key, &versions, results).assert_error("batch failed");
        assert_eq!(failure.version, Some(SourceVersion::Id("null".into())));
        assert!(
            matches!(failure.source.as_ref(), StorageRoleFailure::Entry(cause)
            if cause.class() == FailureClass::PermissionDenied && cause.diagnostic() == "AccessDenied")
        );
        assert_eq!(failure.additional.len(), 99);
        for (index, (version, cause)) in failure.additional.iter().enumerate() {
            assert_eq!(*version, SourceVersion::Id(format!("v{}", index + 1)));
            assert_eq!(
                matches!(cause, StorageRoleFailure::Session(cause)
                if cause.class() == FailureClass::Timeout && cause.transience() == Transience::Unknown),
                index % 2 == 0
            );
        }
        Ok(())
    }

    #[test]
    fn partial_batch_success_does_not_create_extra_failures() -> Result {
        let key = StoragePath::new("payload")?;
        let versions = vec!["old".into(), "held".into(), "marker".into()];
        let denied = failure(&key, FailureClass::PermissionDenied, "held history");
        let result = batch_result(&key, &versions, vec![Ok(()), Err(denied.clone()), Ok(())]);
        let error = result.assert_error("held version failed");
        assert_eq!(error.version, Some(SourceVersion::Id("held".into())));
        assert_eq!(*error.source, denied);
        assert!(error.additional.is_empty());
        assert!(batch_result(&key, &versions, vec![Ok(()), Ok(()), Ok(())]).is_ok());
        Ok(())
    }

    #[test]
    fn short_batch_results_report_unknown_and_preserve_a_known_denial() -> Result {
        let key = StoragePath::new("payload")?;
        let versions = vec!["old".into(), "held".into(), "marker".into()];
        let error = batch_result(
            &key,
            &versions,
            vec![
                Ok(()),
                Err(failure(
                    &key,
                    FailureClass::PermissionDenied,
                    "known denial",
                )),
            ],
        )
        .assert_error("missing result is not success");
        assert!(error.version.is_none());
        assert_eq!(error.additional.len(), 3);
        assert!(
            matches!(error.source.as_ref(), StorageRoleFailure::Session(cause)
            if cause.class() == FailureClass::Protocol && cause.transience() == Transience::Unknown)
        );
        assert!(
            matches!(&error.additional[1].1, StorageRoleFailure::Entry(cause)
            if cause.class() == FailureClass::PermissionDenied && cause.diagnostic() == "known denial")
        );
        assert_eq!(error.additional[2].0, SourceVersion::Id("marker".into()));
        Ok(())
    }

    #[test]
    fn cancellation_distinguishes_before_submission_and_unknown_inflight_outcome() -> Result {
        let key = StoragePath::new("payload")?;
        for (submitted, transience) in [(false, Transience::Transient), (true, Transience::Unknown)]
        {
            assert!(
                matches!(cancelled(&key, submitted), StorageRoleFailure::Entry(cause)
                if cause.class() == FailureClass::Cancelled && cause.transience() == transience)
            );
        }
        Ok(())
    }
}
