//! The upload on the final key in a versioned bucket (ADR-0006 C17): pointers are deleted by the
//! version their write or read reported, and a lock on one is a warning.

use super::*;
use crate::storage::backends::s3::tests::Versioning;

fn versioned() -> Arc<MemoryS3> {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    protocol
}

/// The version ids stored under `key`, newest first, and whether each is a delete marker.
async fn listed(protocol: &MemoryS3, key: &str) -> TestResult<Vec<(String, bool)>> {
    Ok(protocol
        .list_versions(key)
        .await
        .map_err(|failure| format!("{failure:?}"))?
        .into_iter()
        .map(|entry| (entry.version_id, entry.delete_marker))
        .collect())
}

/// Writes a pointer of `binding` through the fake's `PutObject`, so it gets a version.
async fn put_pointer(protocol: &MemoryS3, binding: [u8; 32], extension: Bytes) -> TestResult {
    let bytes = Bytes::from(
        DestinationPointer {
            binding,
            transfer_identity: IDENTITY,
            durable_prefix: None,
            extension,
        }
        .encode()
        .map_err(|_| "encode")?,
    );
    let md5 = content_md5(&bytes);
    protocol
        .put_object(&pointer_key(), bytes, &md5)
        .await
        .map_err(|failure| format!("{failure:?}"))?;
    Ok(())
}

/// A pointer whose `PutObject` stored it but lost the reply learns its version from the read-back
/// and is still deleted by it at publication; the final key gets one version.
#[tokio::test]
async fn a_lost_pointer_reply_still_deletes_the_pointer_by_version() -> TestResult {
    let protocol = versioned();
    let destination = destination(&protocol);
    let data = payload(3 * PART);
    *protocol.put_commits_then_fails.lock().await = true;
    let request = request(data.len(), BINDING, ResumeMode::Discover, true)?;
    let stage = destination.prepare_at_destination(request).await?;
    let written = listed(&protocol, &pointer_key()).await?;
    assert_eq!(written.len(), 1);
    destination.write(&stage, chunks(&data)).await?;
    let evidence = publish_and_verify(&destination, &stage, &data).await?;
    assert!(listed(&protocol, &pointer_key()).await?.is_empty());
    assert_eq!(
        protocol.version_deletes(),
        [(pointer_key(), written[0].0.clone())]
    );
    let finals = listed(&protocol, FINAL).await?;
    assert_eq!(evidence.version, Some(finals[0].0.clone()));
    assert_eq!(finals.len(), 1);
    Ok(())
}

/// A discard deletes the pointer by its version and aborts the upload: nothing is left.
#[tokio::test]
async fn a_discard_deletes_the_pointer_by_version() -> TestResult {
    let protocol = versioned();
    let destination = destination(&protocol);
    let data = payload(3 * PART);
    let request = request(data.len(), BINDING, ResumeMode::Discover, true)?;
    let stage = destination.prepare_at_destination(request).await?;
    destination.write(&stage, chunks(&data)).await?;
    destination.discard(stage).await?;
    assert!(protocol.keys_with_versions().is_empty());
    assert!(uploads(&protocol).await?.is_empty());
    assert!(protocol.plain_deletes_of_final_keys().is_empty());
    Ok(())
}

/// A leftover pointer — beside a large object, where discovery cleans it, and beside a small one,
/// where only the pointer is looked at — is deleted by the version discovery read: no delete
/// marker, and only the new upload's pointer (then nothing) is left.
#[tokio::test]
async fn a_leftover_pointer_is_deleted_by_the_version_discovery_read() -> TestResult {
    for size in [3 * PART, MIB] {
        let protocol = versioned();
        let destination = destination(&protocol);
        let old = seed_upload(&protocol, &[(1, &payload(PART))]).await?;
        put_pointer(&protocol, [9; 32], record(&old)?).await?;
        let leftover = listed(&protocol, &pointer_key()).await?;
        let data = payload(size);
        let request = request(data.len(), BINDING, ResumeMode::Discover, true)?;
        let stage = destination.prepare_at_destination(request).await?;
        assert_eq!(
            stage.prepare_fact(),
            PrepareFact::Restarted {
                reason: RestartReason::BindingChanged
            }
        );
        assert_eq!(
            protocol.version_deletes().first(),
            Some(&(pointer_key(), leftover[0].0.clone())),
            "{size}"
        );
        destination.write(&stage, chunks(&data)).await?;
        publish_and_verify(&destination, &stage, &data).await?;
        assert!(
            listed(&protocol, &pointer_key()).await?.is_empty(),
            "{size}"
        );
        assert_eq!(listed(&protocol, FINAL).await?.len(), 1, "{size}");
    }
    Ok(())
}

/// A resume writes its own pointer over the killed writer's, then deletes the replaced version:
/// once its own is deleted at publication, the old one cannot come back.
#[tokio::test]
async fn a_resume_deletes_the_pointer_version_it_replaced() -> TestResult {
    let protocol = versioned();
    let destination = destination(&protocol);
    let data = payload(4 * PART);
    let request = || request(data.len(), BINDING, ResumeMode::Discover, true);
    let stage = destination.prepare_at_destination(request()?).await?;
    let cut = destination
        .write(&stage, failing_after(&data[..2 * PART])?)
        .await;
    assert!(cut.is_err());
    drop(stage);
    let killed = listed(&protocol, &pointer_key()).await?;
    let stage = destination.prepare_at_destination(request()?).await?;
    assert!(matches!(stage.prepare_fact(), PrepareFact::Resumed { .. }));
    let taken = listed(&protocol, &pointer_key()).await?;
    assert_eq!(taken.len(), 1, "only the resume's pointer");
    assert_ne!(taken[0].0, killed[0].0);
    destination.write(&stage, chunks(&data[2 * PART..])).await?;
    publish_and_verify(&destination, &stage, &data).await?;
    assert!(listed(&protocol, &pointer_key()).await?.is_empty());
    assert_eq!(listed(&protocol, FINAL).await?.len(), 1);
    Ok(())
}

/// A resume that cannot delete the pointer version it replaced (a legal hold on that one version)
/// must not delete its own by version at publication — the held one would become current again —
/// but hide both behind a delete marker.
#[tokio::test]
async fn a_held_replaced_pointer_stays_hidden_after_a_resume() -> TestResult {
    let protocol = versioned();
    let destination = destination(&protocol);
    let data = payload(4 * PART);
    let request = || request(data.len(), BINDING, ResumeMode::Discover, true);
    let stage = destination.prepare_at_destination(request()?).await?;
    let cut = destination
        .write(&stage, failing_after(&data[..2 * PART])?)
        .await;
    assert!(cut.is_err());
    drop(stage);
    let killed = listed(&protocol, &pointer_key()).await?;
    protocol.lock_version(&pointer_key(), &killed[0].0);
    let stage = destination.prepare_at_destination(request()?).await?;
    destination.write(&stage, chunks(&data[2 * PART..])).await?;
    publish_and_verify(&destination, &stage, &data).await?;
    assert!(
        pointer(&protocol).await.is_none(),
        "the held pointer came back"
    );
    let kept: Vec<bool> = listed(&protocol, &pointer_key())
        .await?
        .into_iter()
        .map(|entry| entry.1)
        .collect();
    assert_eq!(
        kept,
        [true, false, false],
        "a marker over ours and the held one"
    );
    Ok(())
}

/// The SDK re-sent a pointer `PutObject` whose first attempt committed but lost its reply: two
/// byte-identical versions, of which the write knows only the second. Deleting it makes the first
/// current, so the deletion sweeps it too — at publication, and when the next prepare cleans up a
/// duplicated leftover (beside a large object and beside a small one).
#[tokio::test]
async fn a_duplicated_pointer_version_does_not_come_back() -> TestResult {
    let protocol = versioned();
    let destination = destination(&protocol);
    let data = payload(3 * PART);
    *protocol.put_stored_twice.lock().await = true;
    let request = |size: usize| request(size, BINDING, ResumeMode::Discover, true);
    let stage = destination
        .prepare_at_destination(request(data.len())?)
        .await?;
    assert_eq!(listed(&protocol, &pointer_key()).await?.len(), 2);
    destination.write(&stage, chunks(&data)).await?;
    publish_and_verify(&destination, &stage, &data).await?;
    assert!(listed(&protocol, &pointer_key()).await?.is_empty());
    for size in [3 * PART, MIB] {
        *protocol.put_stored_twice.lock().await = true;
        put_pointer(&protocol, [9; 32], record("gone")?).await?;
        let stage = destination.prepare_at_destination(request(size)?).await?;
        assert!(matches!(
            stage.prepare_fact(),
            PrepareFact::Restarted { .. }
        ));
        let left = listed(&protocol, &pointer_key()).await?.len();
        assert_eq!(
            left,
            usize::from(size > MIB),
            "only the new upload's pointer ({size})"
        );
        destination.discard(stage).await?;
        assert!(
            listed(&protocol, &pointer_key()).await?.is_empty(),
            "{size}"
        );
    }
    Ok(())
}

/// A killed writer whose pointer PUT was stored twice: the resume deletes the version it read and
/// replaced, and its publication sweeps the duplicate below — no pointer comes back.
#[tokio::test]
async fn a_resume_over_a_duplicated_pointer_leaves_none() -> TestResult {
    let protocol = versioned();
    let destination = destination(&protocol);
    let data = payload(4 * PART);
    let request = || request(data.len(), BINDING, ResumeMode::Discover, true);
    *protocol.put_stored_twice.lock().await = true;
    let stage = destination.prepare_at_destination(request()?).await?;
    let cut = destination
        .write(&stage, failing_after(&data[..2 * PART])?)
        .await;
    assert!(cut.is_err());
    drop(stage);
    let stage = destination.prepare_at_destination(request()?).await?;
    assert!(matches!(stage.prepare_fact(), PrepareFact::Resumed { .. }));
    destination.write(&stage, chunks(&data[2 * PART..])).await?;
    publish_and_verify(&destination, &stage, &data).await?;
    assert!(
        pointer(&protocol).await.is_none(),
        "a stale pointer came back"
    );
    assert!(listed(&protocol, &pointer_key()).await?.is_empty());
    Ok(())
}

/// With versioning suspended, on a store whose `PutObject` reply names no version: discovery read
/// the killed writer's pointer as `"null"`, and the resume's own write replaced that one `"null"`
/// version — the resume must not delete `"null"` (its own pointer) as the replaced version, or its
/// publication finds the pointer gone and refuses as taken over.
#[tokio::test]
async fn a_resume_does_not_delete_its_own_null_pointer() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Suspended);
    *protocol.put_omits_version.lock().await = true;
    let destination = destination(&protocol);
    let data = payload(4 * PART);
    let request = || request(data.len(), BINDING, ResumeMode::Discover, true);
    let stage = destination.prepare_at_destination(request()?).await?;
    let cut = destination
        .write(&stage, failing_after(&data[..2 * PART])?)
        .await;
    assert!(cut.is_err());
    drop(stage);
    let stage = destination.prepare_at_destination(request()?).await?;
    assert!(matches!(stage.prepare_fact(), PrepareFact::Resumed { .. }));
    assert!(pointer(&protocol).await.is_some(), "the resume's pointer");
    destination.write(&stage, chunks(&data[2 * PART..])).await?;
    publish_and_verify(&destination, &stage, &data).await?;
    assert!(pointer(&protocol).await.is_none());
    Ok(())
}

/// A pointer version Object Lock holds is hidden behind a delete marker instead of failing — at
/// discovery, which then starts afresh, and at publication.
#[tokio::test]
async fn a_locked_pointer_is_hidden_behind_a_delete_marker() -> TestResult {
    let protocol = versioned();
    let destination = destination(&protocol);
    protocol.lock_versions(&pointer_key());
    put_pointer(&protocol, [9; 32], record("gone")?).await?;
    let data = payload(3 * PART);
    let request = request(data.len(), BINDING, ResumeMode::Discover, true)?;
    let stage = destination.prepare_at_destination(request).await?;
    assert!(matches!(
        stage.prepare_fact(),
        PrepareFact::Restarted { .. }
    ));
    destination.write(&stage, chunks(&data)).await?;
    let evidence = publish_and_verify(&destination, &stage, &data).await?;
    assert!(evidence.version.is_some());
    assert!(pointer(&protocol).await.is_none());
    let kept: Vec<bool> = listed(&protocol, &pointer_key())
        .await?
        .into_iter()
        .map(|entry| entry.1)
        .collect();
    // Discovery hid the leftover, prepare wrote ours over the marker, publication hid ours.
    assert_eq!(kept, [true, false, true, false]);
    assert!(protocol.plain_deletes_of_final_keys().is_empty());
    Ok(())
}

/// A completion whose reply was lost while a later write replaced our object (a writer the caller
/// contract excludes): the latest version is not ours, so nothing is claimed — a `Conflict` with
/// the final key changed.
#[tokio::test]
async fn an_ambiguous_completion_replaced_since_is_a_changed_conflict() -> TestResult {
    let protocol = versioned();
    let data = payload(3 * PART);
    let (destination, stage) = written_stage(&protocol, &data).await?;
    *protocol.complete_commits_then_fails.lock().await = true;
    *protocol.written_after_lost_completion.lock().await = Some(Bytes::from_static(b"later"));
    let failure = publish(&destination, &stage, &data)
        .await
        .err()
        .ok_or("the settlement must refuse a replaced object")?;
    assert_eq!(class(&failure.error), Some(FailureClass::Conflict));
    assert!(failure.final_destination_changed);
    assert_eq!(listed(&protocol, FINAL).await?.len(), 2);
    Ok(())
}

/// The fake records an unversioned `DeleteObject` of a final key, which the tests above assert
/// never happens; a transfer artifact may be deleted so.
#[tokio::test]
async fn the_fake_records_plain_deletes_of_final_keys() -> TestResult {
    let protocol = versioned();
    protocol
        .delete_object(&pointer_key())
        .await
        .map_err(|failure| format!("{failure:?}"))?;
    assert!(protocol.plain_deletes_of_final_keys().is_empty());
    protocol
        .delete_object(FINAL)
        .await
        .map_err(|failure| format!("{failure:?}"))?;
    assert_eq!(protocol.plain_deletes_of_final_keys(), [FINAL.to_string()]);
    assert_eq!(listed(&protocol, FINAL).await?.len(), 1, "a delete marker");
    Ok(())
}
