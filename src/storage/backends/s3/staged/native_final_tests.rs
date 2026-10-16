//! Native S3→S3 copies to the final key (ADR-0006 C18), over the in-memory S3: a small source is
//! one `CopyObject` at publication, a large one an upload on the final key filled with
//! `UploadPartCopy`.

use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::final_upload::FinalUpload;
use super::*;
use crate::model::{EntryKind, IdentityStrength, ObjectTag, SourceIdentity, StoragePath};
use crate::storage::backends::s3::metadata::S3Metadata;
use crate::storage::backends::s3::tests::{MemoryS3, etag_of, identity};
use crate::storage::backends::s3::{S3NativeCopySource, S3TagSupport};
use crate::storage::{
    FinalDestination, PrepareFact, ResumeMode, SourceDescriptor, StagedDestination,
    VerificationPoint,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const MIB: u64 = 1024 * 1024;
const PART: u64 = 5 * MIB;
const FINAL: &str = "dir/final";

fn destination(protocol: &Arc<MemoryS3>) -> S3StagedDestination<MemoryS3> {
    let metadata = S3Metadata::new(protocol.clone(), identity(), S3TagSupport::Supported);
    S3StagedDestination::new(protocol.clone(), identity())
        .with_checkpoint_interval(2 * PART)
        .with_native_sizing(PART, PART)
        .with_metadata(Arc::new(metadata))
}

/// `source` holding `size` bytes, and its binding for a native copy.
async fn seeded(protocol: &MemoryS3, size: u64) -> TestResult<(Bytes, S3NativeCopySource)> {
    let length = usize::try_from(size)?;
    let bytes = Bytes::from((0..=250_u8).cycle().take(length).collect::<Vec<_>>());
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), bytes.clone());
    let source = S3NativeCopySource {
        bucket: "memory".into(),
        key: "source".into(),
        etag: etag_of(&bytes),
        version_id: None,
        size,
    };
    Ok((bytes, source))
}

fn request(size: u64, resume: ResumeMode) -> TestResult<DestinationPrepareRequest> {
    let prepare = PrepareRequest {
        final_destination: FinalDestination::new(StoragePath::new(FINAL)?),
        source: SourceDescriptor::new(
            StoragePath::new("source")?,
            EntryKind::File,
            Some(size),
            SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"source")?,
        ),
        recovery_binding: [7; 32],
    };
    Ok(DestinationPrepareRequest::new(prepare, [4; 32])
        .with_resume(resume)
        .with_recoverable(false))
}

fn publication(bytes: &Bytes) -> PublishRequest {
    PublishRequest {
        expected_size: bytes.len() as u64,
        expected_blake3: Some(*blake3::hash(bytes).as_bytes()),
        cancel: CancellationToken::new(),
    }
}

async fn verify(
    destination: &S3StagedDestination<MemoryS3>,
    stage: &PreparedStage,
    bytes: &Bytes,
    evidence: PublicationEvidence,
) -> TestResult {
    let verified = destination
        .verify(
            stage,
            VerifyRequest {
                expected_size: bytes.len() as u64,
                expected_blake3: *blake3::hash(bytes).as_bytes(),
                cancel: CancellationToken::new(),
                published: Some(evidence),
            },
        )
        .await?;
    assert_eq!(verified.verified_bytes, bytes.len() as u64);
    Ok(())
}

fn class(error: &StorageRoleFailure) -> Option<FailureClass> {
    match error {
        StorageRoleFailure::Entry(entry) => Some(entry.class()),
        StorageRoleFailure::Session(_) => None,
    }
}

/// Parts after the resumed bytes, numbered on; a source too large for 10 000 parts of the minimum
/// gets larger parts, and one that needs parts over 5 GiB is refused.
#[test]
fn native_parts_cover_the_rest_of_the_source() -> TestResult {
    let ranges = native_final::part_ranges(2 * PART, 4 * PART + 3, PART, 3);
    let expected = [
        (3, 2 * PART..3 * PART),
        (4, 3 * PART..4 * PART),
        (5, 4 * PART..4 * PART + 3),
    ];
    assert_eq!(ranges, expected);
    let path = StoragePath::new(FINAL)?;
    let huge = 10_000 * 64 * MIB + 1;
    assert_eq!(
        native_final::native_part_size(huge, 64 * MIB, &path)?,
        usize::try_from(64 * MIB + 1)?
    );
    assert!(native_final::native_part_size(10_000 * 5 * 1024 * MIB + 1, PART, &path).is_err());
    Ok(())
}

/// Up to the single-copy limit: nothing is sent before publication, which is one `CopyObject`
/// with the pending tags set after it; the object is read back after publication, pinned to what
/// the copy reported.
#[tokio::test]
async fn a_small_native_copy_is_sent_at_publication() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let (bytes, source) = seeded(&protocol, PART).await?;
    let stage = destination
        .prepare_native_stage(request(PART, ResumeMode::Discover)?)
        .await?;
    assert!(stage.at_destination && !stage.recovery_enabled());
    let after = destination.verification_point(&stage) == VerificationPoint::AfterPublish;
    assert!(after);
    let filled = destination
        .fill_native(&stage, source, CancellationToken::new(), 6)
        .await
        .map_err(|failure| format!("{:?}", failure.error))?;
    assert_eq!(
        (filled.write.persisted_bytes, filled.native_requests),
        (PART, 1)
    );
    assert_eq!(
        *protocol.native_copies.lock().await,
        0,
        "nothing before publication"
    );
    assert_eq!(
        destination.observe_checkpoint(&stage).await?.durable_prefix,
        PART
    );
    let tag = ObjectTag::new("class", "gold")?;
    destination
        .apply_metadata(
            &stage,
            MetadataMutation::Tags(vec![tag.clone()]),
            CancellationToken::new(),
        )
        .await?;
    let evidence = destination
        .publish(&stage, publication(&bytes))
        .await
        .map_err(|failure| format!("{:?}", failure.error))?;
    assert_eq!(*protocol.native_copies.lock().await, 1);
    assert_eq!(protocol.objects.lock().await.get(FINAL), Some(&bytes));
    assert_eq!(
        protocol
            .get_tags(FINAL, None)
            .await
            .map_err(|e| format!("{e:?}"))?,
        vec![tag]
    );
    verify(&destination, &stage, &bytes, evidence).await
}

/// A source replaced after it was bound is refused before anything is copied: the final key is
/// unchanged. A copy whose reply is lost but whose object is at the final key with the source's
/// size and `ETag` counts as published.
#[tokio::test]
async fn a_small_native_copy_settles_refusals_and_lost_replies() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let (bytes, source) = seeded(&protocol, MIB).await?;
    let mut replaced = source.clone();
    replaced.etag = "\"replaced\"".into();
    for (bound, lost_reply) in [(replaced, false), (source, true)] {
        *protocol.copy_commits_then_fails.lock().await = lost_reply;
        let stage = destination
            .prepare_native_stage(request(MIB, ResumeMode::Discover)?)
            .await?;
        destination
            .fill_native(&stage, bound, CancellationToken::new(), 6)
            .await
            .map_err(|failure| format!("{:?}", failure.error))?;
        let published = destination.publish(&stage, publication(&bytes)).await;
        match published {
            Err(failure) if !lost_reply => {
                assert!(!failure.final_destination_changed);
                assert_eq!(class(&failure.error), Some(FailureClass::Conflict));
                assert!(!protocol.objects.lock().await.contains_key(FINAL));
            }
            Ok(evidence) if lost_reply => verify(&destination, &stage, &bytes, evidence).await?,
            other => return Err(format!("unexpected publication: {:?}", other.err()).into()),
        }
    }
    Ok(())
}

/// Above it: a resumable prepare writes the pointer when the upload begins, the fill copies every
/// part with `UploadPartCopy` and turns recovery on once the parts reach the interval, and
/// publication completes the upload and deletes the pointer. An atomic copy writes no pointer
/// and never becomes recoverable.
#[tokio::test]
async fn a_large_native_copy_fills_and_completes_an_upload_on_the_final_key() -> TestResult {
    for (resume, pointers) in [(ResumeMode::Discover, 1), (ResumeMode::Restart, 0)] {
        let protocol = Arc::new(MemoryS3::default());
        let destination = destination(&protocol);
        let size = 4 * PART + 3;
        let (bytes, source) = seeded(&protocol, size).await?;
        let stage = destination
            .prepare_native_stage(request(size, resume)?)
            .await?;
        assert_eq!(stage.prepare_fact(), PrepareFact::Fresh);
        assert!(!stage.recovery_enabled());
        assert_eq!(*protocol.puts.lock().await, pointers, "{resume:?}");
        let filled = destination
            .fill_native(&stage, source, CancellationToken::new(), 6)
            .await
            .map_err(|failure| format!("{:?}", failure.error))?;
        assert_eq!((filled.native_bytes, filled.native_requests), (size, 5));
        assert_eq!(stage.recovery_enabled(), pointers == 1, "{resume:?}");
        assert!(!protocol.objects.lock().await.contains_key(FINAL));
        let evidence = destination
            .publish(&stage, publication(&bytes))
            .await
            .map_err(|failure| format!("{:?}", failure.error))?;
        verify(&destination, &stage, &bytes, evidence).await?;
        let objects = protocol.objects.lock().await;
        assert_eq!(objects.get(FINAL), Some(&bytes));
        assert!(!objects.keys().any(|key| key.contains(".data-mover-")));
        assert!(protocol.uploads.lock().await.is_empty());
    }
    Ok(())
}

/// The parts' `ETag`s are the store's own — nothing proves they are MD5s (SSE-KMS) — so a native
/// completion is not held to their composite: a completion reporting another `ETag` publishes,
/// and read-back verification is what checks the content.
#[tokio::test]
async fn a_native_completion_is_not_held_to_the_part_composite() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let size = 2 * PART + 1;
    let (bytes, source) = seeded(&protocol, size).await?;
    let stage = destination
        .prepare_native_stage(request(size, ResumeMode::Discover)?)
        .await?;
    destination
        .fill_native(&stage, source, CancellationToken::new(), 6)
        .await
        .map_err(|failure| format!("{:?}", failure.error))?;
    *protocol.complete_etag.lock().await = Some("\"00000000000000000000000000000000-3\"".into());
    destination
        .publish(&stage, publication(&bytes))
        .await
        .map_err(|failure| format!("{:?}", failure.error))?;
    assert_eq!(protocol.objects.lock().await.get(FINAL), Some(&bytes));
    Ok(())
}

/// A streamed `write` keeps four parts in flight at the part size it plans, but resuming a native
/// upload's larger parts only as many as fit in those four parts' bytes, at least one.
#[test]
fn a_streamed_resume_of_larger_parts_keeps_fewer_in_flight() -> TestResult {
    let upload = |size: u64, part: u64| -> TestResult<usize> {
        let pointer = StoragePath::new("dir/.pointer")?;
        let request = request(size, ResumeMode::Discover)?;
        Ok(FinalUpload::new(&request, "id".into(), part, pointer)?.streamed_inflight())
    };
    assert_eq!(upload(200 * MIB, 8 * MIB)?, 4);
    assert_eq!(upload(200 * MIB, 5 * MIB)?, 4);
    assert_eq!(upload(200 * MIB, 16 * MIB)?, 2);
    assert_eq!(upload(200 * MIB, 64 * MIB)?, 1);
    // A source whose streamed parts are already 64 MiB (over 625 GiB) keeps four.
    assert_eq!(upload(10_000 * 64 * MIB, 64 * MIB)?, 4);
    Ok(())
}
