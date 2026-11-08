//! 永久清理跨多页、多批版本，保持当前锚点，并保留同前缀子项。

use super::*;
use crate::model::{ObservedEntry, StorageTimestamp, TimePrecision};
use crate::storage::PurgeOutcome;
use std::sync::atomic::Ordering;
use tokio_util::sync::CancellationToken;

async fn observed(namespace: &dyn Namespace, key: &str) -> TestResult<ObservedEntry> {
    let (entries, failures) = list_whole(namespace, &StoragePath::root()).await?;
    assert!(failures.is_empty());
    let entry = entries
        .into_iter()
        .find(|entry| entry.path.as_str() == key)
        .ok_or("entry missing")?;
    Ok(ObservedEntry::new(
        entry.path,
        entry.kind,
        entry.size,
        entry.inline_timestamps.and_then(|times| times.modified),
        entry.source_identity,
    )?
    .with_history(entry.listing.history)?)
}

#[tokio::test]
async fn unchanged_listing_after_acknowledged_deletion_stops_before_current_removal() -> TestResult
{
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "key", b"old").await?;
    put(&protocol, "key", b"current").await?;
    let namespace = namespace(&protocol)?;
    let entry = observed(namespace.as_ref(), "key").await?;
    protocol
        .ignore_version_deletes
        .store(true, Ordering::Relaxed);
    assert!(
        matches!(namespace.purge_observed(&entry, CancellationToken::new()).await,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Protocol)
    );
    assert!(protocol.version_deletes().is_empty());
    assert!(protocol.objects.lock().await.contains_key("key"));
    Ok(())
}

#[tokio::test]
async fn permanent_purge_removes_all_versions_current_last_without_touching_prefix_siblings()
-> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    for _ in 0..300 {
        put(&protocol, "key", b"payload").await?;
    }
    put(&protocol, "key-other", b"sibling").await?;
    put(&protocol, "key/child", b"child").await?;
    let namespace = namespace(&protocol)?;
    let entry = observed(namespace.as_ref(), "key").await?;
    let latest = entry
        .history()
        .and_then(|history| history.latest())
        .and_then(VersionRecord::version_id)
        .ok_or("latest missing")?;
    {
        let mut knobs = protocol.listing();
        knobs.page_size = 7;
        knobs.minio_markers = true;
    }
    assert_eq!(
        namespace
            .purge_observed(&entry, CancellationToken::new())
            .await?,
        PurgeOutcome::Deleted { records: 300 }
    );
    let deletes = protocol.version_deletes();
    assert_eq!(deletes.len(), 300);
    assert_eq!(deletes.last(), Some(&("key".into(), latest.into())));
    assert!(protocol.keys_with_versions().contains(&"key-other".into()));
    assert!(protocol.keys_with_versions().contains(&"key/child".into()));
    assert_eq!(
        namespace
            .purge_observed(&entry, CancellationToken::new())
            .await?,
        PurgeOutcome::AlreadyAbsent
    );
    Ok(())
}

#[tokio::test]
async fn partial_historical_failure_keeps_the_current_anchor() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    for _ in 0..4 {
        put(&protocol, "key", b"payload").await?;
    }
    let namespace = namespace(&protocol)?;
    let entry = observed(namespace.as_ref(), "key").await?;
    protocol.lock_version("key", "v0001");
    assert!(
        matches!(namespace.purge_observed(&entry, CancellationToken::new()).await,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::PermissionDenied)
    );
    assert_eq!(protocol.version_deletes().len(), 2);
    assert!(
        !protocol
            .version_deletes()
            .iter()
            .any(|(_, id)| id == "v0004")
    );
    assert!(protocol.objects.lock().await.contains_key("key"));
    assert!(protocol.plain_deletes_of_final_keys().is_empty());
    Ok(())
}

#[tokio::test]
async fn suspended_and_never_versioned_buckets_delete_exact_null_record() -> TestResult {
    for suspended in [false, true] {
        let protocol = Arc::new(MemoryS3::default());
        if suspended {
            protocol.set_versioning(Versioning::Suspended);
        }
        protocol.listing().same_instant = true;
        put(&protocol, "key", b"payload").await?;
        let namespace = namespace(&protocol)?;
        let entry = if suspended {
            observed(namespace.as_ref(), "key").await?
        } else {
            let storage = connect(protocol.clone(), identity(), None)?;
            observe_descriptor(
                &storage,
                namespace.stat(&path("key")).await?,
                ObservationPlan::default(),
            )
            .await?
        };
        assert_eq!(
            namespace
                .purge_observed(&entry, CancellationToken::new())
                .await?,
            PurgeOutcome::Deleted { records: 1 }
        );
        assert_eq!(protocol.version_deletes(), [("key".into(), "null".into())]);
        assert!(protocol.plain_deletes_of_final_keys().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn current_purge_compares_list_and_head_times_at_observed_precision() -> TestResult {
    let listed = StorageTimestamp::new(1_700_000_000_123_000_000, TimePrecision::Milliseconds)?;
    let coarse = StorageTimestamp::new(1_700_000_000_000_000_000, TimePrecision::Seconds)?;
    let changed_second = StorageTimestamp::new(1_700_000_001_000_000_000, TimePrecision::Seconds)?;
    let changed_millis =
        StorageTimestamp::new(1_700_000_000_124_000_000, TimePrecision::Milliseconds)?;
    for (head, changed_body, allowed) in [
        (Some(coarse), false, true),
        (Some(listed), false, true),
        (Some(changed_second), false, false),
        (Some(changed_millis), false, false),
        (None, false, false),
        (Some(coarse), true, false),
    ] {
        let protocol = Arc::new(MemoryS3::default());
        put(&protocol, "key", b"payload").await?;
        *protocol.last_modified.lock().await = Some(listed);
        let namespace = namespace(&protocol)?;
        let entry = observed(namespace.as_ref(), "key").await?;
        *protocol.last_modified.lock().await = head;
        if changed_body {
            put(&protocol, "key", b"changed").await?;
        }
        let result = namespace
            .purge_observed(&entry, CancellationToken::new())
            .await;
        if allowed {
            assert_eq!(result?, PurgeOutcome::Deleted { records: 1 });
            assert_eq!(protocol.version_deletes(), [("key".into(), "null".into())]);
        } else {
            assert!(
                matches!(result, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Conflict)
            );
            assert!(protocol.version_deletes().is_empty());
            assert!(protocol.objects.lock().await.contains_key("key"));
        }
    }
    Ok(())
}

#[tokio::test]
async fn deleted_key_purges_every_data_version_and_the_final_marker() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "gone", b"first").await?;
    put(&protocol, "gone", b"second").await?;
    protocol
        .delete_object("gone")
        .await
        .map_err(|error| format!("{error:?}"))?;
    let namespace = namespace(&protocol)?;
    let entry = observed(namespace.as_ref(), "gone").await?;
    assert_eq!(
        namespace
            .purge_observed(&entry, CancellationToken::new())
            .await?,
        PurgeOutcome::Deleted { records: 3 }
    );
    assert!(protocol.keys_with_versions().is_empty());
    assert_eq!(
        protocol.version_deletes().last(),
        Some(&("gone".into(), "v0003".into()))
    );
    Ok(())
}

#[tokio::test]
async fn changed_history_and_cancellation_never_authorize_purge() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "key", b"copied").await?;
    let namespace = namespace(&protocol)?;
    let entry = observed(namespace.as_ref(), "key").await?;
    put(&protocol, "key", b"uncopied").await?;
    assert!(
        matches!(namespace.purge_observed(&entry, CancellationToken::new()).await,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Conflict)
    );
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(namespace.purge_observed(&entry, cancel).await,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Cancelled));
    assert!(protocol.version_deletes().is_empty());
    Ok(())
}

#[tokio::test]
async fn readonly_history_retains_current_record_and_reports_permission_denied() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "key", b"old").await?;
    put(&protocol, "key", b"current").await?;
    let namespace = namespace(&protocol)?;
    let entry = observed(namespace.as_ref(), "key").await?;
    protocol.lock_versions("key");
    assert!(
        matches!(namespace.purge_observed(&entry, CancellationToken::new()).await,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::PermissionDenied)
    );
    assert!(protocol.version_deletes().is_empty());
    assert_eq!(
        protocol.objects.lock().await.get("key").map(AsRef::as_ref),
        Some(b"current".as_slice())
    );
    Ok(())
}
