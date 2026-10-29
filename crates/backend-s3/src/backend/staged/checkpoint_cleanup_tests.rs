//! Strict cleanup ownership, pagination and Object Lock regressions over `MemoryS3`.

use super::*;
use crate::storage::backends::s3::tests::Versioning;

struct History {
    protocol: Arc<MemoryS3>,
    adapter: S3StagedDestination<MemoryS3>,
    stage: PreparedStage,
    key: StoragePath,
}

fn hold() -> S3ProtocolFailure {
    S3ProtocolFailure::entry(
        FailureClass::PermissionDenied,
        Transience::Permanent,
        "legal hold",
    )
}

#[tokio::test]
async fn three_held_takeovers_remove_all_incarnations_of_the_same_upload() -> TestResult {
    let History {
        protocol,
        adapter,
        stage: first,
        key,
    } = history(3).await?;
    *protocol.delete_failure.lock().await = Some((key.as_str().into(), hold()));
    let data = Bytes::from(vec![5; PART + 1]);
    drop(takeover(&adapter, &data).await?);
    drop(takeover(&adapter, &data).await?);
    let latest = takeover(&adapter, &data).await?;
    let oldest = final_upload::of(&first).ok_or("first upload")?;
    let current = final_upload::of(&latest).ok_or("latest upload")?;
    assert_eq!(oldest.upload_id, current.upload_id);
    assert_ne!(oldest.nonce, current.nonce);
    assert!(
        current.lock().stale_pointers.iter().all(|bytes| {
            DestinationPointer::decode(bytes)
                .ok()
                .and_then(|pointer| accepted(&pointer))
                .is_none_or(|record| record.nonce != oldest.nonce)
        }),
        "the oldest incarnation is no longer in the in-memory ownership history"
    );
    *protocol.delete_failure.lock().await = None;
    adapter.discard(latest).await?;
    assert!(
        protocol
            .list_versions(key.as_str())
            .await
            .map_err(|error| format!("{error:?}"))?
            .is_empty()
    );
    assert_eq!(*protocol.multipart_begins.lock().await, 1);
    Ok(())
}

async fn takeover(
    adapter: &S3StagedDestination<MemoryS3>,
    data: &Bytes,
) -> TestResult<PreparedStage> {
    let stage = prepare(adapter, data.len()).await?;
    arm(&stage, data)?;
    adapter.persist_hash_checkpoint(&stage).await?;
    Ok(stage)
}

#[tokio::test]
async fn a_replaced_nonce_cannot_be_reused_as_the_current_fence() -> TestResult {
    let History {
        protocol,
        adapter,
        stage: first,
        key,
    } = history(3).await?;
    let old = pointer(&protocol).await?;
    *protocol.delete_failure.lock().await = Some((key.as_str().into(), hold()));
    let current = takeover(&adapter, &Bytes::from(vec![5; PART + 1])).await?;
    *protocol.delete_failure.lock().await = None;
    // Re-expose the immediately replaced incarnation: its nonce is in stale_pointers,
    // but it must not fence cleanup by the newer stage.
    upload_pointer::put(&*protocol, &key, old.encode().map_err(|_| "encode")?).await?;
    let deletes = protocol.version_deletes();
    let failure = adapter
        .discard(current)
        .await
        .err()
        .ok_or("replaced nonce is not our fence")?;
    assert!(
        matches!(failure, StorageRoleFailure::Entry(ref error) if error.class() == FailureClass::Conflict)
    );
    assert_eq!(pointer(&protocol).await?, old);
    assert_eq!(protocol.version_deletes(), deletes);
    assert_eq!(*protocol.aborts.lock().await, 0);
    adapter.discard(first).await?;
    Ok(())
}

#[tokio::test]
async fn a_replaced_owner_cannot_cleanup_the_new_current_nonce() -> TestResult {
    let History {
        protocol,
        adapter,
        stage: first,
        key,
    } = history(3).await?;
    *protocol.delete_failure.lock().await = Some((key.as_str().into(), hold()));
    let data = Bytes::from(vec![5; PART + 1]);
    let second = prepare(&adapter, data.len()).await?;
    arm(&second, &data)?;
    adapter.persist_hash_checkpoint(&second).await?;
    *protocol.delete_failure.lock().await = None;
    let before = pointer(&protocol).await?;
    let deletes = protocol.version_deletes();
    let failure = adapter
        .discard(first)
        .await
        .err()
        .ok_or("old owner must fail closed")?;
    assert!(
        matches!(failure, StorageRoleFailure::Entry(ref error) if error.class() == FailureClass::Conflict)
    );
    assert_eq!(pointer(&protocol).await?, before);
    assert_eq!(protocol.version_deletes(), deletes);
    assert_eq!(*protocol.aborts.lock().await, 0);
    adapter.discard(second).await?;
    Ok(())
}

#[tokio::test]
async fn a_repeated_cursor_fails_without_deleting_the_current_root() -> TestResult {
    let History {
        protocol,
        adapter,
        stage,
        key,
    } = history(24).await?;
    let before = pointer(&protocol).await?;
    protocol.listing().restart_prefix = Some(key.as_str().into());
    let failure = adapter
        .discard(stage)
        .await
        .err()
        .ok_or("repeated cursor must fail")?;
    assert!(
        matches!(failure, StorageRoleFailure::Entry(ref error) if error.class() == FailureClass::Protocol)
    );
    assert_eq!(pointer(&protocol).await?, before);
    assert!(protocol.version_deletes().is_empty());
    assert_eq!(*protocol.aborts.lock().await, 0);
    Ok(())
}

async fn history(generations: usize) -> TestResult<History> {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    let key = upload_pointer::pointer_path(&StoragePath::new(KEY)?)?;
    *protocol.delete_failure.lock().await = Some((key.as_str().into(), hold()));
    let adapter = destination(&protocol);
    let data = Bytes::from(vec![5; PART + 1]);
    let stage = prepare(&adapter, data.len()).await?;
    arm(&stage, &data)?;
    for _ in 0..generations {
        adapter.persist_hash_checkpoint(&stage).await?;
    }
    *protocol.delete_failure.lock().await = None;
    protocol.listing().page_size = 2;
    protocol.listing().minio_markers = true;
    Ok(History {
        protocol,
        adapter,
        stage,
        key,
    })
}

#[tokio::test]
async fn an_unknown_latest_marker_causes_no_deletions() -> TestResult {
    let History {
        protocol,
        adapter,
        stage,
        key,
    } = history(12).await?;
    protocol
        .delete_object(key.as_str())
        .await
        .map_err(|error| format!("{error:?}"))?;
    let before = protocol
        .list_versions(key.as_str())
        .await
        .map_err(|error| format!("{error:?}"))?;
    let deletes = protocol.version_deletes();
    let failure = adapter
        .discard(stage)
        .await
        .err()
        .ok_or("unknown marker must fail closed")?;
    assert!(
        matches!(failure, StorageRoleFailure::Entry(ref error) if error.class() == FailureClass::Conflict)
    );
    assert_eq!(protocol.version_deletes(), deletes);
    assert_eq!(
        protocol
            .list_versions(key.as_str())
            .await
            .map_err(|error| format!("{error:?}"))?,
        before
    );
    assert_eq!(*protocol.aborts.lock().await, 0);
    Ok(())
}

#[tokio::test]
async fn an_unknown_historical_marker_is_preserved() -> TestResult {
    let History {
        protocol,
        adapter,
        stage,
        key,
    } = history(12).await?;
    protocol
        .delete_object(key.as_str())
        .await
        .map_err(|error| format!("{error:?}"))?;
    let hidden = protocol
        .list_versions(key.as_str())
        .await
        .map_err(|error| format!("{error:?}"))?;
    let marker = hidden.first().ok_or("marker")?.version_id.clone();
    let body = final_upload::of(&stage)
        .ok_or("upload")?
        .lock()
        .stale_pointers
        .last()
        .cloned()
        .ok_or("snapshot")?;
    // A later owned root is current again; the intervening marker has no ownership proof.
    let version = upload_pointer::put(&*protocol, &key, body.clone()).await?;
    final_upload::of(&stage)
        .ok_or("upload")?
        .record_pointer(version, body);
    adapter.discard(stage).await?;
    let remaining = protocol
        .list_versions(key.as_str())
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert_eq!(remaining.len(), 1);
    assert!(remaining[0].delete_marker);
    assert_eq!(remaining[0].version_id, marker);
    Ok(())
}

#[tokio::test]
async fn a_multipage_sweep_removes_every_owned_generation_and_keeps_final() -> TestResult {
    let History {
        protocol,
        adapter,
        stage,
        key,
    } = history(24).await?;
    let final_bytes = Bytes::from_static(b"unrelated final content");
    protocol
        .objects
        .lock()
        .await
        .insert(KEY.into(), final_bytes.clone());
    let neighbor = format!("{}-another-owner", key.as_str());
    protocol
        .objects
        .lock()
        .await
        .insert(neighbor.clone(), Bytes::from_static(b"foreign"));
    let before = protocol
        .list_versions(key.as_str())
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert!(before.len() > 4);
    let anchor = before.first().ok_or("current root")?.version_id.clone();
    adapter.discard(stage).await?;
    assert!(
        protocol
            .list_versions(key.as_str())
            .await
            .map_err(|error| format!("{error:?}"))?
            .is_empty()
    );
    assert_eq!(
        protocol
            .version_deletes()
            .last()
            .map(|(_, version)| version.as_str()),
        Some(anchor.as_str())
    );
    assert!(protocol.listing().version_pages > 4);
    assert_eq!(protocol.objects.lock().await.get(KEY), Some(&final_bytes));
    assert!(protocol.objects.lock().await.contains_key(&neighbor));
    assert!(protocol.plain_deletes_of_final_keys().is_empty());
    Ok(())
}

#[tokio::test]
async fn a_refused_history_delete_stops_after_two_pages_and_retains_the_current_root() -> TestResult
{
    let History {
        protocol,
        adapter,
        stage,
        key,
    } = history(24).await?;
    let before = pointer(&protocol).await?;
    let versions = protocol
        .list_versions(key.as_str())
        .await
        .map_err(|error| format!("{error:?}"))?;
    *protocol.delete_failure.lock().await = Some((key.as_str().into(), hold()));
    let failure = adapter
        .discard(stage)
        .await
        .err()
        .ok_or("hold must fail cleanup")?;
    assert!(
        matches!(failure, StorageRoleFailure::Entry(ref error) if error.class() == FailureClass::PermissionDenied)
    );
    assert_eq!(pointer(&protocol).await?, before);
    assert_eq!(
        protocol
            .list_versions(key.as_str())
            .await
            .map_err(|error| format!("{error:?}"))?,
        versions
    );
    assert!(protocol.listing().version_pages <= 2);
    assert!(protocol.version_deletes().is_empty());
    assert_eq!(*protocol.aborts.lock().await, 0);
    Ok(())
}
