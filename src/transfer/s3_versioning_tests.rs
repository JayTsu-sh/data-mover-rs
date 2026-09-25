//! S3 transfers into a versioned bucket through the engine (ADR-0006 C17), over the in-memory S3
//! in its versioned mode: every write path reports the version it made, a resumed copy adds
//! exactly one version, pointers leave no version or delete marker behind, and no unversioned
//! `DeleteObject` ever reaches a final key.

use std::error::Error;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::model::{FailureClass, StoragePath, Transience};
use crate::storage::PrepareFact;
use crate::storage::artifacts::{ArtifactKind, is_artifact_path, sibling_artifact};
use crate::storage::backends::s3::tests::{MemoryS3, Versioning, endpoint_of, native_context};
use crate::storage::backends::s3::{
    S3Protocol as _, S3ProtocolFailure, S3VersionFacts, connect, connect_at_destination,
};
use crate::transfer::{
    InflightLimits, SourceVersion, TransferOutcome, TransferPolicy, TransferRequest, TransferRoute,
    transfer,
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
async fn bucket(mode: Versioning, versions: &[(&str, &Bytes)]) -> Arc<MemoryS3> {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(mode);
    for (id, bytes) in versions {
        protocol.put_version("source", id, (*bytes).clone()).await;
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
        storage()?,
        StoragePath::new("source")?,
        storage()?,
        StoragePath::new(final_key)?,
        InflightLimits::new(4, 4 * MIB, 4)?,
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
        let protocol = bucket(Versioning::Enabled, &[("s1", &data)]).await;
        let outcome = transfer(request(&protocol, final_key, policy)?).await?;
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

/// A native S3→S3 copy (until ADR-0006 C18 through the temp key, copied to the final key at
/// publication) reports the version the copy made, and deletes every version of its temp key by
/// id: no full-size temp version or delete marker is left in a versioned bucket.
#[tokio::test]
async fn a_native_copy_reports_its_version_and_leaves_no_temp_version() -> TestResult {
    for size in [1024, 3 * PART] {
        let data = payload(size, 11);
        let protocol = bucket(Versioning::Enabled, &[("s1", &data)]).await;
        let storage = || {
            connect(
                protocol.clone(),
                endpoint_of(&protocol),
                Some(native_context()),
            )
        };
        let request = TransferRequest::new(
            storage()?,
            StoragePath::new("source")?,
            storage()?,
            StoragePath::new("native")?,
            InflightLimits::new(4, 4 * MIB, 4)?,
            CancellationToken::new(),
        );
        let outcome = transfer(request).await?;
        assert_eq!(outcome.route, TransferRoute::Native, "{size}");
        assert_eq!(*protocol.native_copies.lock().await, 1, "{size}");
        clean_versions(&protocol, "native", 1, Some(version(&outcome)?)).await?;
    }
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
    let outcome = transfer(retry).await?;
    assert_eq!(
        outcome.prepare,
        PrepareFact::Resumed {
            bytes: 2 * PART as u64
        }
    );
    assert_eq!(outcome.reused_bytes, 2 * PART as u64);
    Ok(outcome)
}

/// An interrupted copy leaves its upload and one pointer version; the resume takes the upload
/// over (replacing the pointer and deleting the replaced version) and completes it once: the key
/// gains one version, and neither pointer version nor delete marker is left.
#[tokio::test]
async fn an_interrupted_and_resumed_copy_adds_one_version() -> TestResult {
    let data = payload(3 * PART, 2);
    let protocol = bucket(Versioning::Enabled, &[("s1", &data)]).await;
    let policy = TransferPolicy::Checkpointed;
    let outcome = cut_then_resume(
        &protocol,
        request(&protocol, "resumed", policy)?,
        request(&protocol, "resumed", policy)?,
    )
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
    let protocol = bucket(Versioning::Enabled, &[("s1", &data)]).await;
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
    let protocol = bucket(Versioning::Enabled, &[("s1", &v1), ("s2", &v2)]).await;
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
    let protocol = bucket(Versioning::Enabled, &[("s1", &v1), ("s2", &v2)]).await;
    let by_id = || {
        request(&protocol, "history-cut", TransferPolicy::Checkpointed)
            .map(|request| request.with_source_version(SourceVersion::Id("s1".into())))
    };
    let outcome = cut_then_resume(&protocol, by_id()?, by_id()?).await?;
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
        let protocol = bucket(Versioning::Suspended, &[("null", &data)]).await;
        let outcome = transfer(request(&protocol, final_key, policy)?).await?;
        assert_eq!(outcome.destination_version, None, "{final_key}");
        clean_versions(&protocol, final_key, 1, Some("null")).await?;
    }
    Ok(())
}

/// Object Lock refusing to delete the pointer's version is a warning, not a failure: the transfer
/// succeeds and names its version, and the pointer is hidden behind a delete marker.
#[tokio::test]
async fn a_locked_pointer_does_not_fail_the_transfer() -> TestResult {
    let data = payload(3 * PART, 9);
    let protocol = bucket(Versioning::Enabled, &[("s1", &data)]).await;
    let pointer = pointer_of("locked")?;
    protocol.lock_versions(&pointer);
    let outcome = transfer(request(&protocol, "locked", TransferPolicy::Checkpointed)?).await?;
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
    assert_eq!(protocol.plain_deletes_of_final_keys(), Vec::<String>::new());
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
        let protocol = bucket(Versioning::Enabled, &[("s1", &data)]).await;
        *protocol.complete_commits_then_fails.lock().await = true;
        let outcome = transfer(request(&protocol, final_key, policy)?).await?;
        assert_eq!(*protocol.completes.lock().await, 1, "{final_key}");
        clean_versions(&protocol, final_key, 1, Some(version(&outcome)?)).await?;
    }
    Ok(())
}
