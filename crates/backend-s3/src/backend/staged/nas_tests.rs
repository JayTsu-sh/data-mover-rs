//! An ONTAP NAS bucket as a destination (P7 §14.2), over the in-memory bucket in NAS mode: it
//! keeps no progress, and every prepare refuses what it cannot write before sending anything.

use std::time::Duration;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::{EntryKind, IdentityStrength, ObjectTag, SourceIdentity};
use crate::storage::backends::s3::tests::{MemoryS3, content_md5, identity};
use crate::storage::backends::s3::{S3NativeCopySource, S3Protocol as _, S3ProtocolFailure};
use crate::storage::{
    FinalDestination, ResumeMode, SourceDescriptor, StagedDestination, VerifyRequest,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const OVER_ONE_REQUEST: u64 = 5 * 1024 * 1024 * 1024 + 1;

fn prepare_request(path: &str, size: Option<u64>) -> TestResult<PrepareRequest> {
    Ok(PrepareRequest {
        source: SourceDescriptor::new(
            StoragePath::new("source")?,
            EntryKind::File,
            size,
            SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"source")?,
        ),
        final_destination: FinalDestination::new(StoragePath::new(path)?),
        recovery_binding: [5; 32],
    })
}

fn at_destination(path: &str, size: Option<u64>) -> TestResult<DestinationPrepareRequest> {
    Ok(
        DestinationPrepareRequest::new(prepare_request(path, size)?, [6; 32])
            .with_resume(ResumeMode::Discover),
    )
}

fn unsupported_prepare<T: std::fmt::Debug>(result: &Result<T, StorageRoleFailure>) -> bool {
    matches!(
        result,
        Err(StorageRoleFailure::Entry(failure))
            if failure.class() == FailureClass::Unsupported
                && failure.transience() == Transience::Permanent
                && failure.operation() == Operation::Prepare
    )
}

/// A NAS bucket keeps no progress (no automatic interval either) and refuses objects over 5 GiB
/// and keys it would rewrite; an object bucket keeps progress and refuses neither.
#[test]
fn a_nas_destination_declares_no_resume_and_its_limits() -> TestResult {
    let nas = S3StagedDestination::new(Arc::new(MemoryS3::nas_bucket()), identity());
    assert!(!nas.resumable_at_destination());
    assert_eq!(nas.automatic_checkpoint_interval_bytes(), None);
    let path = StoragePath::new("a/b")?;
    assert!(nas.final_refusal(&path, Some(OVER_ONE_REQUEST)).is_some());
    assert!(
        nas.final_refusal(&path, Some(OVER_ONE_REQUEST - 1))
            .is_none()
    );
    assert!(
        nas.final_refusal(&StoragePath::new("a/../b")?, Some(1))
            .is_some()
    );
    let object = S3StagedDestination::new(Arc::new(MemoryS3::default()), identity());
    assert!(object.resumable_at_destination());
    assert!(object.automatic_checkpoint_interval_bytes().is_some());
    assert!(
        object
            .final_refusal(&path, Some(OVER_ONE_REQUEST))
            .is_none()
    );
    Ok(())
}

/// Over the single-PUT threshold a NAS bucket is sent no multipart request: at the destination
/// and `Direct` both prepare one streamed `PutObject` (fresh, not resumable) — after one `HEAD` of
/// the final key at the destination (to settle a lost reply), none for `Direct`, and nothing
/// else; the multipart path itself refuses before any request.
#[tokio::test]
async fn a_nas_bucket_is_never_asked_for_a_multipart_upload() -> TestResult {
    let protocol = Arc::new(MemoryS3::nas_bucket());
    let destination = S3StagedDestination::new(protocol.clone(), identity());
    let size = Some(9 * 1024 * 1024);
    let stage = destination
        .prepare_at_destination(at_destination("big", size)?)
        .await?;
    assert!(streaming::of(&stage).is_some());
    assert!(!stage.recovery_enabled());
    assert_eq!(stage.prepare_fact(), crate::storage::PrepareFact::Fresh);
    let stage = destination
        .prepare_direct(prepare_request("big", size)?, CancellationToken::new())
        .await?;
    assert!(streaming::of(&stage).is_some() && stage.direct());
    assert_eq!(
        protocol.nas().heads,
        1,
        "one HEAD of the final key at the destination, none for Direct"
    );
    let pointer = StoragePath::new(".data-mover-x.upload")?;
    let refused = destination
        .prepare_final_upload(at_destination("big", size)?, PART_SIZE, pointer)
        .await;
    assert!(unsupported_prepare(&refused), "{refused:?}");
    assert_eq!(protocol.nas().heads, 1);
    assert_eq!(*protocol.multipart_begins.lock().await, 0);
    Ok(())
}

/// Every prepare — at the destination, `Direct`, native — refuses an oversized object or a key
/// the bucket would rewrite as that entry's `Unsupported`, before any request.
#[tokio::test]
async fn every_nas_prepare_refuses_before_any_request() -> TestResult {
    for (path, size) in [
        ("big", Some(OVER_ONE_REQUEST)),
        ("a/../x", Some(1)),
        ("u", None),
    ] {
        let protocol = Arc::new(MemoryS3::nas_bucket());
        let destination = S3StagedDestination::new(protocol.clone(), identity());
        let prepared = destination
            .prepare_at_destination(at_destination(path, size)?)
            .await;
        assert!(unsupported_prepare(&prepared), "{path}: {prepared:?}");
        let prepared = destination
            .prepare_direct(prepare_request(path, size)?, CancellationToken::new())
            .await;
        assert!(unsupported_prepare(&prepared), "{path}: {prepared:?}");
        let prepared = destination
            .prepare_native_stage(at_destination(path, size)?)
            .await;
        assert!(unsupported_prepare(&prepared), "{path}: {prepared:?}");
        assert_eq!(protocol.nas().heads, 0, "{path}");
        assert_eq!(*protocol.puts.lock().await, 0, "{path}");
    }
    Ok(())
}

/// A NAS single `PutObject` whose reply is lost is settled against the `HEAD` prepare made: a
/// commit changed the final key (its opaque `ETag` is new), no commit left it as it was — and a
/// file / directory clash is refused before anything changes.
#[tokio::test]
async fn a_lost_nas_single_put_is_settled_by_the_prepare_head() -> TestResult {
    for committed in [true, false] {
        let protocol = Arc::new(MemoryS3::nas_bucket());
        let old = Bytes::from_static(b"old");
        protocol
            .put_object("small", old.clone().into(), &content_md5(&old))
            .await
            .map_err(|failure| format!("{failure:?}"))?;
        let destination = S3StagedDestination::new(protocol.clone(), identity())
            .with_hold_limit(Duration::from_secs(1), Duration::from_millis(10));
        let stage = destination
            .prepare_at_destination(at_destination("small", Some(3))?)
            .await?;
        destination
            .write_single(&stage, Bytes::from_static(b"new"))
            .await?;
        if committed {
            *protocol.put_commits_then_fails.lock().await = true;
        } else {
            *protocol.put_fails_without_commit.lock().await = true;
        }
        let Err(failure) = destination
            .publish(
                &stage,
                PublishRequest {
                    expected_size: 3,
                    expected_blake3: None,
                    cancel: CancellationToken::new(),
                },
            )
            .await
        else {
            return Err("a lost reply was published".into());
        };
        assert_eq!(failure.final_destination_changed, committed);
    }
    Ok(())
}

/// A NAS bucket holding `src` (and, when `existing`, an older `dst`) and a native stage copying
/// `src` to `dst`, not yet published.
async fn copy_stage(
    existing: bool,
) -> TestResult<(Arc<MemoryS3>, S3StagedDestination<MemoryS3>, PreparedStage)> {
    let protocol = Arc::new(MemoryS3::nas_bucket());
    for (key, body) in [("src", COPIED), ("dst", b"old")] {
        if key == "dst" && !existing {
            continue;
        }
        let body = Bytes::from_static(body);
        protocol
            .put_object(key, body.clone().into(), &content_md5(&body))
            .await
            .map_err(|failure| format!("{failure:?}"))?;
    }
    let source_etag = protocol
        .head("src")
        .await
        .map_err(|failure| format!("{failure:?}"))?
        .etag;
    let destination = S3StagedDestination::new(protocol.clone(), identity())
        .with_hold_limit(Duration::from_secs(1), Duration::from_millis(10));
    let stage = destination
        .prepare_native_stage(at_destination("dst", Some(7))?)
        .await?;
    single::of(&stage)
        .ok_or("not a single stage")?
        .set_native_source(S3NativeCopySource {
            bucket: "memory".into(),
            key: "src".into(),
            etag: source_etag,
            version_id: None,
            size: 7,
        });
    Ok((protocol, destination, stage))
}

const COPIED: &[u8] = b"copy me";

fn publish_request() -> PublishRequest {
    PublishRequest {
        expected_size: 7,
        expected_blake3: None,
        cancel: CancellationToken::new(),
    }
}

/// A NAS `CopyObject` (N5) whose reply is lost is settled against the `HEAD` prepare made: it
/// changed the key when the copy committed (the copy has an `ETag` of its own), not when it did
/// not — whether or not an older object was there.
#[tokio::test]
async fn a_lost_nas_copy_reply_is_settled_by_the_prepare_head() -> TestResult {
    for existing in [false, true] {
        for committed in [true, false] {
            let (protocol, destination, stage) = copy_stage(existing).await?;
            if committed {
                *protocol.copy_commits_then_fails.lock().await = true;
            } else {
                *protocol.native_failure.lock().await = Some(S3ProtocolFailure::session(
                    FailureClass::Connectivity,
                    Transience::Transient,
                    "copy response lost",
                ));
            }
            let Err(failure) = destination.publish(&stage, publish_request()).await else {
                return Err("a lost copy reply was published".into());
            };
            assert_eq!(
                failure.final_destination_changed, committed,
                "existing {existing}, committed {committed}"
            );
        }
    }
    Ok(())
}

/// Read back, a NAS copy must still carry the `ETag` its reply named — tags set after it do not
/// move it (measured 2026-09-27), so publishing sends no `HEAD`, tagged or not: replaced after the
/// copy it is a `Conflict`; rewritten keeping the mtime (same `ETag`) it is a transient
/// `Corruption`; untouched, or tagged, it verifies.
#[tokio::test]
async fn a_nas_copy_is_read_back_against_its_reply() -> TestResult {
    for case in ["intact", "tagged", "replaced", "rewritten"] {
        let (protocol, destination, stage) = copy_stage(false).await?;
        if case == "tagged" {
            let single = single::of(&stage).ok_or("not a single stage")?;
            let tags = vec![ObjectTag::new("k", "v")?];
            single::apply_metadata(
                &stage,
                single,
                true,
                MetadataMutation::Tags(tags),
                &CancellationToken::new(),
            )?;
        }
        let heads_before = protocol.nas().heads;
        let published = destination
            .publish(&stage, publish_request())
            .await
            .map_err(|failure| failure.error)?;
        assert_eq!(
            protocol.nas().heads,
            heads_before,
            "{case}: no HEAD at publish"
        );
        let other = Bytes::from_static(b"another");
        match case {
            "replaced" => {
                protocol
                    .put_object("dst", other.clone().into(), &content_md5(&other))
                    .await
                    .map_err(|failure| format!("{failure:?}"))?;
            }
            "rewritten" => protocol.nas_rewrite_keeping_mtime("dst", other).await,
            _ => {}
        }
        let verified = destination
            .verify(
                &stage,
                VerifyRequest {
                    expected_size: 7,
                    expected_blake3: *blake3::hash(COPIED).as_bytes(),
                    cancel: CancellationToken::new(),
                    published: Some(published),
                },
            )
            .await;
        let class = match &verified {
            Ok(_) => None,
            Err(StorageRoleFailure::Entry(failure)) => Some(failure.class()),
            Err(other) => return Err(format!("{case}: {other:?}").into()),
        };
        let expected = match case {
            "replaced" => Some(FailureClass::Conflict),
            "rewritten" => Some(FailureClass::Corruption),
            _ => None,
        };
        assert_eq!(class, expected, "{case}: {verified:?}");
    }
    Ok(())
}
