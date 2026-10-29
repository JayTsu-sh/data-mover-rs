//! S3 transfers into a versioned bucket through the engine (ADR-0006 C17), over the in-memory S3
//! in its versioned mode: every write path reports the version it made, a resumed copy adds
//! exactly one version, pointers leave no version or delete marker behind, and no unversioned
//! `DeleteObject` ever reaches a final key.

use std::error::Error;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::s3_at_destination_tests::{
    assert_uploaded_parts, completed_parts, expert_run, hash_batches, stored_pointer,
};

use crate::model::{FailureClass, StoragePath, Transience};
use crate::storage::artifacts::{ArtifactKind, is_artifact_path, sibling_artifact};
use crate::storage::backends::s3::tests::{MemoryS3, Versioning, endpoint_of, source_storage};
use crate::storage::backends::s3::{
    S3Protocol as _, S3ProtocolFailure, S3VersionFacts, connect_at_destination,
    connect_native_at_destination,
};
use crate::storage::{PrepareFact, StorageRoleFailure};
use crate::transfer::{
    SourceVersion, TransferOutcome, TransferPolicy, TransferRequest, TransferRoute, transfer,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const MIB: usize = 1024 * 1024;
const PART: usize = 8 * MIB;
/// The automatic interval of these destinations: an object over it writes a pointer.
const INTERVAL: usize = PART;

fn payload(size: usize, salt: u8) -> Bytes {
    Bytes::from(
        (0..=250_u8)
            .cycle()
            .skip(usize::from(salt))
            .take(size)
            .collect::<Vec<_>>(),
    )
}

/// A bucket with `mode` versioning whose `source` holds `versions` (id, bytes), oldest first.
async fn bucket(mode: Versioning, versions: &[(&str, &Bytes)], paths: &[&str]) -> Arc<MemoryS3> {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(mode);
    for (id, bytes) in versions {
        for path in paths {
            protocol
                .put_version(&format!("src/{path}"), id, (*bytes).clone())
                .await;
        }
    }
    protocol
}

fn request(
    protocol: &Arc<MemoryS3>,
    final_key: &str,
    policy: TransferPolicy,
) -> TestResult<TransferRequest> {
    let storage =
        || connect_at_destination(protocol.clone(), endpoint_of(protocol), INTERVAL as u64);
    Ok(TransferRequest::new(
        source_storage(protocol, "src")?,
        StoragePath::new(final_key)?,
        storage()?,
        CancellationToken::new(),
    )
    .with_transfer_policy(policy))
}

async fn entries(protocol: &MemoryS3, key: &str) -> TestResult<Vec<S3VersionFacts>> {
    Ok(protocol
        .list_versions(key)
        .await
        .map_err(|failure| format!("{failure:?}"))?)
}

async fn assert_legacy_pointer_versions(protocol: &MemoryS3, key: &str) -> TestResult {
    for version in entries(protocol, key).await? {
        if version.delete_marker {
            continue;
        }
        let bytes = stored(protocol, key, &version.version_id)
            .await
            .ok_or("pointer version")?;
        let pointer = crate::storage::pointer::DestinationPointer::decode(&bytes)
            .map_err(|_| "pointer decode")?;
        assert!(
            pointer.checkpoint.is_none(),
            "legacy success cannot retain a v2 root"
        );
    }
    Ok(())
}

fn pointer_of(final_key: &str) -> TestResult<String> {
    Ok(
        sibling_artifact(&StoragePath::new(final_key)?, ArtifactKind::Upload)
            .ok_or("pointer path")?
            .as_str()
            .to_string(),
    )
}

/// The bucket holds exactly `final_count` versions of `final_key` (the newest being
/// `outcome_version`, when given), no delete marker and no version of a transfer artifact
/// anywhere, no upload is open on the key, and no unversioned delete reached a final key.
async fn clean_versions(
    protocol: &MemoryS3,
    final_key: &str,
    final_count: usize,
    outcome_version: Option<&str>,
) -> TestResult {
    for key in protocol.keys_with_versions() {
        let listed = entries(protocol, &key).await?;
        assert!(!is_artifact_path(&key), "{key} kept {listed:?}");
        assert!(
            listed.iter().all(|entry| !entry.delete_marker),
            "{key}: {listed:?}"
        );
    }
    let finals = entries(protocol, final_key).await?;
    assert_eq!(finals.len(), final_count, "{finals:?}");
    if let Some(version) = outcome_version {
        assert_eq!(finals[0].version_id, version);
    }
    let open = protocol.list_uploads(final_key).await;
    assert_eq!(
        open.map_err(|failure| format!("{failure:?}"))?,
        Vec::<String>::new()
    );
    assert_eq!(protocol.plain_deletes_of_final_keys(), Vec::<String>::new());
    Ok(())
}

fn version(outcome: &TransferOutcome) -> TestResult<&str> {
    Ok(outcome
        .destination_version
        .as_deref()
        .ok_or("the outcome names no destination version")?)
}

/// Every S3 write path into a versioned bucket — a single `PutObject`, a checkpointed multipart
/// upload on the final key (with its pointer), `Direct` small and large — reports the version it
/// made, which is the key's only version; nothing else is left.
#[tokio::test]
async fn every_write_path_reports_the_version_it_made() -> TestResult {
    for (final_key, size, policy) in [
        ("single", 1024, TransferPolicy::Checkpointed),
        ("multipart", 3 * PART, TransferPolicy::Checkpointed),
        ("direct-small", 1024, TransferPolicy::Direct),
        ("direct-large", 3 * PART, TransferPolicy::Direct),
    ] {
        let data = payload(size, 1);
        let protocol = bucket(Versioning::Enabled, &[("s1", &data)], &[final_key]).await;
        let request = request(&protocol, final_key, policy)?;
        let puts = if final_key == "multipart" {
            hash_batches(&request, size)? + 5
        } else {
            u32::from(size <= PART)
        };
        let outcome = transfer(request).await?;
        assert_eq!(*protocol.puts.lock().await, puts, "{final_key}");
        assert_eq!(outcome.blake3, Some(*blake3::hash(&data).as_bytes()));
        clean_versions(&protocol, final_key, 1, Some(version(&outcome)?)).await?;
        let pointer = pointer_of(final_key)?;
        let pointer_deleted = protocol
            .version_deletes()
            .iter()
            .any(|(key, _)| *key == pointer);
        assert_eq!(pointer_deleted, final_key == "multipart", "{final_key}");
    }
    Ok(())
}

/// A native S3→S3 request to `final_key` (ADR-0006 C18) whose destination copies up to one part
/// with `CopyObject` and anything larger with `UploadPartCopy` parts of `PART`.
fn native(protocol: &Arc<MemoryS3>, final_key: &str) -> TestResult<TransferRequest> {
    let storage = || {
        connect_native_at_destination(
            protocol.clone(),
            endpoint_of(protocol),
            INTERVAL as u64,
            (PART as u64, PART as u64),
        )
    };
    Ok(TransferRequest::new(
        source_storage(protocol, "src")?,
        StoragePath::new(final_key)?,
        storage()?,
        CancellationToken::new(),
    ))
}

/// A native S3→S3 copy goes to the final key (ADR-0006 C18) — one `CopyObject`, or an upload
/// filled with `UploadPartCopy` — and reports the version it made, the key's only one: no temp
/// key, no pointer version, no delete marker.
#[tokio::test]
async fn a_native_copy_reports_its_version_and_leaves_no_temp_version() -> TestResult {
    for size in [1024, 3 * PART] {
        let data = payload(size, 11);
        let protocol = bucket(Versioning::Enabled, &[("s1", &data)], &["native"]).await;
        let outcome = transfer(native(&protocol, "native")?).await?;
        assert_eq!(outcome.route, TransferRoute::Native, "{size}");
        let (copies, parts) = if size > PART { (0, 3) } else { (1, 0) };
        assert_eq!(*protocol.native_copies.lock().await, copies, "{size}");
        assert_eq!(*protocol.part_copies.lock().await, parts, "{size}");
        clean_versions(&protocol, "native", 1, Some(version(&outcome)?)).await?;
    }
    Ok(())
}

/// Stored versions copied natively by id, oldest first — the large one part by part, the small
/// one in one copy — arrive as two versions in that order, each holding its source version; an
/// interrupted native copy by id resumes into one version.
#[tokio::test]
async fn native_copies_by_id_arrive_in_order_and_resume_into_one_version() -> TestResult {
    let (v1, v2) = (payload(3 * PART, 12), payload(PART / 2, 13));
    let protocol = bucket(
        Versioning::Enabled,
        &[("s1", &v1), ("s2", &v2)],
        &["history", "cut", "history-cut"],
    )
    .await;
    let by_id = |key: &str, id: &str| {
        native(&protocol, key)
            .map(|request| request.with_source_version(SourceVersion::Id(id.into())))
    };
    let mut made = Vec::new();
    for id in ["s1", "s2"] {
        let outcome = transfer(by_id("history", id)?).await?;
        assert_eq!(outcome.route, TransferRoute::Native);
        made.push(version(&outcome)?.to_string());
    }
    clean_versions(&protocol, "history", 2, Some(&made[1])).await?;
    assert_eq!(entries(&protocol, "history").await?[1].version_id, made[0]);
    assert_eq!(
        stored(&protocol, "history", &made[0]).await,
        Some(v1.clone())
    );
    assert_eq!(
        stored(&protocol, "history", &made[1]).await,
        Some(v2.clone())
    );

    let outcome = Box::pin(cut_then_resume(
        &protocol,
        by_id("cut", "s1")?,
        by_id("cut", "s1")?,
        v1.len(),
    ))
    .await?;
    let resumed = version(&outcome)?.to_string();
    clean_versions(&protocol, "cut", 1, Some(&resumed)).await?;
    assert_eq!(stored(&protocol, "cut", &resumed).await, Some(v1));
    Ok(())
}

/// The bytes stored as version `id` of `key`.
async fn stored(protocol: &MemoryS3, key: &str, id: &str) -> Option<Bytes> {
    let versions = protocol.versions.lock().await;
    versions.get(&(key.to_string(), id.to_string())).cloned()?
}

/// A native multipart copy whose completion reply is lost claims the version it made, as a
/// streamed one does.
#[tokio::test]
async fn an_ambiguous_native_completion_claims_the_version_it_made() -> TestResult {
    let data = payload(3 * PART, 14);
    let protocol = bucket(Versioning::Enabled, &[("s1", &data)], &["ambiguous-native"]).await;
    *protocol.complete_commits_then_fails.lock().await = true;
    let outcome = transfer(native(&protocol, "ambiguous-native")?).await?;
    assert_eq!(outcome.route, TransferRoute::Native);
    assert_eq!(*protocol.completes.lock().await, 1);
    clean_versions(&protocol, "ambiguous-native", 1, Some(version(&outcome)?)).await?;
    Ok(())
}

fn reset() -> S3ProtocolFailure {
    S3ProtocolFailure::session(FailureClass::Connectivity, Transience::Transient, "reset")
}

/// Runs `request` with part 3 failing once parts 1 and 2 are stored, then drops the failure (a
/// writer that dies), and resumes with `retry`: exactly one version of the key is added.
async fn cut_then_resume(
    protocol: &Arc<MemoryS3>,
    request: TransferRequest,
    retry: TransferRequest,
    size: usize,
) -> TestResult<TransferOutcome> {
    *protocol.part_failure.lock().await = Some((3, reset()));
    *protocol.part_failure_waits.lock().await = true;
    let failure = transfer(request)
        .await
        .err()
        .ok_or("the failed part must cut the transfer")?;
    assert!(failure.has_recoverable_stage());
    drop(failure);
    *protocol.part_failure.lock().await = None;
    let completed = completed_parts(protocol, retry.final_path().as_str()).await?;
    completed.assert_third_failed();
    let missing = completed.missing(size, PART);
    *protocol.body_fragments.lock().await = Some(Vec::new());
    let uploaded = *protocol.part_uploads.lock().await;
    let copied = *protocol.part_copies.lock().await;
    let outcome = transfer(retry).await?;
    assert_eq!(
        outcome.prepare,
        PrepareFact::Resumed {
            bytes: completed.bytes()
        }
    );
    assert_eq!(outcome.reused_bytes, completed.bytes());
    let count = u32::try_from(missing.len())?;
    assert_eq!(
        *protocol.part_uploads.lock().await - uploaded,
        if outcome.route == TransferRoute::Streaming {
            count
        } else {
            0
        },
        "no completed part may be reuploaded"
    );
    assert_eq!(
        *protocol.part_copies.lock().await - copied,
        if outcome.route == TransferRoute::Native {
            count
        } else {
            0
        },
        "no completed part may be recopied"
    );
    assert_uploaded_parts(
        protocol,
        if outcome.route == TransferRoute::Streaming {
            &missing
        } else {
            &[]
        },
    )
    .await?;
    Ok(outcome)
}

/// An interrupted copy leaves its upload and hash intent; takeover and final phases remove
/// their owned pointer versions. The final key gains one version and no artifact remains.
#[tokio::test]
async fn an_interrupted_and_resumed_copy_adds_one_version() -> TestResult {
    let data = payload(3 * PART, 2);
    let protocol = bucket(Versioning::Enabled, &[("s1", &data)], &["resumed"]).await;
    let policy = TransferPolicy::Checkpointed;
    let outcome = Box::pin(cut_then_resume(
        &protocol,
        request(&protocol, "resumed", policy)?,
        request(&protocol, "resumed", policy)?,
        data.len(),
    ))
    .await?;
    assert_eq!(outcome.blake3, Some(*blake3::hash(&data).as_bytes()));
    clean_versions(&protocol, "resumed", 1, Some(version(&outcome)?)).await?;
    Ok(())
}

/// A failure that is discarded (before its first checkpoint, the pointer already written) deletes
/// the pointer by its version: no pointer version and no delete marker are left, and the final
/// key's earlier version is untouched.
#[tokio::test]
async fn a_discarded_failure_leaves_the_bucket_as_it_was() -> TestResult {
    let data = payload(3 * PART, 3);
    let protocol = bucket(Versioning::Enabled, &[("s1", &data)], &["kept"]).await;
    protocol
        .put_version("kept", "old", Bytes::from_static(b"previous"))
        .await;
    let denied =
        S3ProtocolFailure::entry(FailureClass::PermissionDenied, Transience::Permanent, "x");
    *protocol.part_failure.lock().await = Some((1, denied));
    let failure = transfer(request(&protocol, "kept", TransferPolicy::Checkpointed)?)
        .await
        .err()
        .ok_or("a failed part must fail the transfer")?;
    assert!(!failure.final_destination_changed());
    failure.discard_stage().await?;
    clean_versions(&protocol, "kept", 1, Some("old")).await?;
    Ok(())
}

/// Two stored versions copied by id, oldest first, arrive as two versions of the key in that
/// order, each outcome naming its own.
#[tokio::test]
async fn versions_copied_by_id_arrive_in_order() -> TestResult {
    let (v1, v2) = (payload(3 * PART, 4), payload(PART / 2, 5));
    let protocol = bucket(
        Versioning::Enabled,
        &[("s1", &v1), ("s2", &v2)],
        &["history", "cut", "history-cut"],
    )
    .await;
    let mut made = Vec::new();
    for id in ["s1", "s2"] {
        let request = request(&protocol, "history", TransferPolicy::Checkpointed)?
            .with_source_version(SourceVersion::Id(id.into()));
        made.push(
            transfer(request)
                .await?
                .destination_version
                .ok_or("version")?,
        );
    }
    clean_versions(&protocol, "history", 2, Some(&made[1])).await?;
    let listed = entries(&protocol, "history").await?;
    assert_eq!(listed[1].version_id, made[0]);
    let stored = protocol.versions.lock().await;
    let content = |id: &String| stored.get(&("history".to_string(), id.clone())).cloned();
    assert_eq!(content(&made[0]), Some(Some(v1.clone())));
    assert_eq!(content(&made[1]), Some(Some(v2.clone())));
    Ok(())
}

/// An interrupted copy of a stored version resumes from the parts it left and adds one version
/// holding that version's bytes.
#[tokio::test]
async fn an_interrupted_id_copy_resumes_into_one_version() -> TestResult {
    let (v1, v2) = (payload(3 * PART, 6), payload(3 * PART, 7));
    let protocol = bucket(
        Versioning::Enabled,
        &[("s1", &v1), ("s2", &v2)],
        &["history", "cut", "history-cut"],
    )
    .await;
    let by_id = || {
        request(&protocol, "history-cut", TransferPolicy::Checkpointed)
            .map(|request| request.with_source_version(SourceVersion::Id("s1".into())))
    };
    let outcome = Box::pin(cut_then_resume(&protocol, by_id()?, by_id()?, v1.len())).await?;
    assert!(outcome.reused_bytes > 0);
    let made = version(&outcome)?.to_string();
    clean_versions(&protocol, "history-cut", 1, Some(&made)).await?;
    let stored = protocol
        .versions
        .lock()
        .await
        .get(&("history-cut".into(), made))
        .cloned();
    assert_eq!(stored, Some(Some(v1)));
    Ok(())
}

/// With versioning suspended every write reports version `"null"`: the outcome names no version,
/// and the pointer is deleted as `"null"`, leaving no delete marker.
#[tokio::test]
async fn suspended_versioning_reports_no_version() -> TestResult {
    for (final_key, size, policy) in [
        ("single", 1024, TransferPolicy::Checkpointed),
        ("multipart", 3 * PART, TransferPolicy::Checkpointed),
        ("direct-large", 3 * PART, TransferPolicy::Direct),
    ] {
        let data = payload(size, 8);
        let protocol = bucket(Versioning::Suspended, &[("null", &data)], &[final_key]).await;
        let outcome = transfer(request(&protocol, final_key, policy)?).await?;
        assert_eq!(outcome.destination_version, None, "{final_key}");
        clean_versions(&protocol, final_key, 1, Some("null")).await?;
    }
    Ok(())
}

/// The explicit expert v1 route retains its Object Lock warning contract, with one pointer PUT.
#[tokio::test]
async fn a_locked_pointer_does_not_fail_the_transfer() -> TestResult {
    let data = payload(3 * PART, 9);
    let protocol = bucket(Versioning::Enabled, &[("s1", &data)], &["source"]).await;
    let pointer = pointer_of("locked")?;
    protocol.lock_versions(&pointer);
    let outcome = expert_run(&protocol, INTERVAL as u64, "locked").await?;
    assert_eq!(*protocol.puts.lock().await, 1, "legacy v1 pointer only");
    let finals = entries(&protocol, "locked").await?;
    assert_eq!(finals.len(), 1);
    assert_eq!(finals[0].version_id, version(&outcome)?);
    assert!(
        protocol.head(&pointer).await.is_err(),
        "the pointer is hidden"
    );
    let kept: Vec<bool> = entries(&protocol, &pointer)
        .await?
        .iter()
        .map(|entry| entry.delete_marker)
        .collect();
    assert_eq!(kept, [true, false], "a marker over the locked version");
    assert_legacy_pointer_versions(&protocol, &pointer).await?;
    assert_eq!(protocol.plain_deletes_of_final_keys(), Vec::<String>::new());
    Ok(())
}

/// Ceph RGW (DXN) refuses a delete marker over a held pointer as well as its deletion by version:
/// still a warning — the transfer succeeds and names its version, the pointer stays — and the
/// key's next transfer is not stopped by it.
#[tokio::test]
async fn a_pointer_held_against_markers_does_not_fail_the_transfer() -> TestResult {
    let data = payload(3 * PART, 16);
    let protocol = bucket(Versioning::Enabled, &[("s1", &data)], &["source"]).await;
    protocol.lock_refuses_delete_markers();
    let pointer = pointer_of("held")?;
    protocol.lock_versions(&pointer);
    for made in 1..=2 {
        let outcome = expert_run(&protocol, INTERVAL as u64, "held").await?;
        assert_eq!(
            *protocol.puts.lock().await,
            u32::try_from(made)?,
            "one legacy pointer per transfer"
        );
        let finals = entries(&protocol, "held").await?;
        assert_eq!(finals.len(), made);
        assert_eq!(finals[0].version_id, version(&outcome)?);
        let kept = entries(&protocol, &pointer).await?;
        assert_eq!(kept.len(), made, "one held pointer version per transfer");
        assert!(kept.iter().all(|entry| !entry.delete_marker), "{kept:?}");
        assert_legacy_pointer_versions(&protocol, &pointer).await?;
    }
    assert_eq!(
        protocol
            .list_uploads("held")
            .await
            .map_err(|f| format!("{f:?}"))?
            .len(),
        0
    );
    assert_eq!(protocol.plain_deletes_of_final_keys(), Vec::<String>::new());
    Ok(())
}

/// A v2 hold is a committed failure with cleanup authority, not success retaining raw hash
/// fragments indefinitely. The reversible refusal models a legal hold until explicitly lifted.
#[tokio::test]
async fn a_held_hash_checkpoint_requires_cleanup_after_the_hold_is_removed() -> TestResult {
    let data = payload(3 * PART, 17);
    let protocol = bucket(Versioning::Enabled, &[("s1", &data)], &["hash-held"]).await;
    let pointer = pointer_of("hash-held")?;
    *protocol.delete_failure.lock().await = Some((
        pointer.clone(),
        S3ProtocolFailure::entry(
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "Object Lock legal hold",
        ),
    ));
    let request = request(&protocol, "hash-held", TransferPolicy::Checkpointed)?;
    let puts = hash_batches(&request, data.len())? + 5;
    let failure = transfer(request)
        .await
        .err()
        .ok_or("a held v2 hash root must not report success")?;
    assert!(failure.final_destination_changed());
    assert!(failure.has_pending_cleanup());
    assert!(!failure.has_unpublished_stage());
    assert!(
        matches!(failure.source().and_then(|error| error.downcast_ref::<StorageRoleFailure>()), Some(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::PermissionDenied)
    );
    assert_eq!(
        *protocol.puts.lock().await,
        puts,
        "bounded intent and lifecycle versions"
    );
    assert_eq!(protocol.objects.lock().await.get("hash-held"), Some(&data));
    assert!(
        stored_pointer(&protocol, "hash-held")
            .await?
            .checkpoint
            .is_some()
    );
    let roots = entries(&protocol, &pointer).await?;
    assert_eq!(roots.len(), usize::try_from(puts)?);
    assert!(roots.iter().all(|root| !root.delete_marker));
    assert_eq!(entries(&protocol, "hash-held").await?.len(), 1);
    *protocol.delete_failure.lock().await = None;
    failure.cleanup_published_stage().await?;
    clean_versions(&protocol, "hash-held", 1, None).await?;
    assert_eq!(protocol.objects.lock().await.get("hash-held"), Some(&data));
    Ok(())
}

/// A `CopyObject` reply without a version in a versioned bucket (DXN) claims the key's latest
/// version, which holds the copy — one listing; a reply that names its version, and a bucket
/// without versioning, list nothing.
#[tokio::test]
async fn a_copy_reply_without_a_version_claims_the_latest_version() -> TestResult {
    for (mode, omitted, lists) in [
        (Versioning::Enabled, true, 1),
        (Versioning::Enabled, false, 0),
        (Versioning::Off, true, 0),
    ] {
        let data = payload(1024, 17);
        let protocol = bucket(mode, &[("s1", &data)], &["copied"]).await;
        *protocol.copy_omits_version.lock().await = omitted;
        let outcome = transfer(native(&protocol, "copied")?).await?;
        assert_eq!(outcome.route, TransferRoute::Native);
        assert_eq!(*protocol.native_copies.lock().await, 1);
        let label = format!("{mode:?} omitted={omitted}");
        assert_eq!(*protocol.version_lists.lock().await, lists, "{label}");
        if mode == Versioning::Enabled {
            clean_versions(&protocol, "copied", 1, Some(version(&outcome)?)).await?;
        } else {
            assert_eq!(outcome.destination_version, None, "{label}");
        }
    }
    Ok(())
}

/// A completion whose reply is lost — on the checkpointed upload and on a `Direct` one — is
/// settled through the key's versions: the latest has our size and composite `ETag`, so its
/// version is claimed.
#[tokio::test]
async fn an_ambiguous_completion_claims_the_version_it_made() -> TestResult {
    for (final_key, policy) in [
        ("ambiguous", TransferPolicy::Checkpointed),
        ("ambiguous-direct", TransferPolicy::Direct),
    ] {
        let data = payload(3 * PART, 10);
        let protocol = bucket(Versioning::Enabled, &[("s1", &data)], &[final_key]).await;
        *protocol.complete_commits_then_fails.lock().await = true;
        let outcome = transfer(request(&protocol, final_key, policy)?).await?;
        assert_eq!(*protocol.completes.lock().await, 1, "{final_key}");
        clean_versions(&protocol, final_key, 1, Some(version(&outcome)?)).await?;
    }
    Ok(())
}
