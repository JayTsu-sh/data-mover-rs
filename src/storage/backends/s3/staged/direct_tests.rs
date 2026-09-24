//! `Direct` writes to S3 (ADR-0006 C14c): the object is written at its final key inside `write`
//! — one `PutObject` up to T, a multipart upload above — never through a temp key, and a failed
//! write leaves no upload.

use bytes::Bytes;
use futures::stream;
use tokio_util::sync::CancellationToken;

use super::super::DEFAULT_SINGLE_PUT_THRESHOLD;
use super::*;
use crate::model::{EntryKind, IdentityStrength, SourceIdentity};
use crate::storage::backends::s3::tests::{MemoryS3, identity};
use crate::storage::{
    FinalDestination, PublishRequest, SourceDescriptor, StagedDestination, VerificationPoint,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const MIB: usize = 1024 * 1024;

fn prepare_request(size: Option<u64>) -> TestResult<PrepareRequest> {
    Ok(PrepareRequest {
        source: SourceDescriptor::new(
            StoragePath::new("source")?,
            EntryKind::File,
            size,
            SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"source")?,
        ),
        final_destination: FinalDestination::new(StoragePath::new("dir/final")?),
        recovery_binding: [83; 32],
    })
}

fn destination(protocol: &Arc<MemoryS3>) -> S3StagedDestination<MemoryS3> {
    S3StagedDestination::new(protocol.clone(), identity())
}

fn chunks(payload: &Bytes) -> ByteStream {
    let pieces: Vec<_> = payload
        .chunks(MIB)
        .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
        .collect();
    Box::pin(stream::iter(pieces))
}

async fn prepared(
    destination: &S3StagedDestination<MemoryS3>,
    size: Option<u64>,
) -> TestResult<PreparedStage> {
    Ok(destination
        .prepare_direct(prepare_request(size)?, CancellationToken::new())
        .await?)
}

/// Publishes the written stage, then reads it back.
async fn publish_and_verify(
    destination: &S3StagedDestination<MemoryS3>,
    stage: &PreparedStage,
    payload: &Bytes,
) -> TestResult<PublicationEvidence> {
    let evidence = destination
        .publish(
            stage,
            PublishRequest {
                expected_size: payload.len() as u64,
                expected_blake3: Some(*blake3::hash(payload).as_bytes()),
                cancel: CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| format!("{:?}", failure.error))?;
    let verified = destination
        .verify(
            stage,
            VerifyRequest {
                expected_size: payload.len() as u64,
                expected_blake3: *blake3::hash(payload).as_bytes(),
                cancel: CancellationToken::new(),
                published: Some(evidence.clone()),
            },
        )
        .await?;
    assert_eq!(verified.verified_bytes, payload.len() as u64);
    Ok(evidence)
}

async fn no_artifacts(protocol: &MemoryS3) -> bool {
    protocol
        .objects
        .lock()
        .await
        .keys()
        .all(|key| !key.contains(".data-mover-"))
}

/// Up to T: one `PutObject` inside `write` makes the object visible; publication sends nothing.
#[tokio::test]
async fn a_small_direct_object_is_one_put_inside_write() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let payload = Bytes::from(vec![5; 1024]);
    let stage = prepared(&destination, Some(1024)).await?;
    assert!(stage.direct && !stage.recovery_enabled() && !stage.durable_publication);
    assert_eq!(
        destination.verification_point(&stage),
        VerificationPoint::AfterPublish
    );
    assert!(protocol.objects.lock().await.is_empty());
    destination.write_single(&stage, payload.clone()).await?;
    assert_eq!(*protocol.puts.lock().await, 1);
    assert_eq!(
        protocol.objects.lock().await.get("dir/final"),
        Some(&payload)
    );
    publish_and_verify(&destination, &stage, &payload).await?;
    assert_eq!(*protocol.puts.lock().await, 1);
    assert_eq!(*protocol.multipart_begins.lock().await, 0);
    assert!(no_artifacts(&protocol).await);
    Ok(())
}

/// Above T: parts and the completion happen inside `write`, on the final key; the upload is only
/// begun there, and publication sends nothing.
#[tokio::test]
async fn a_large_direct_object_completes_inside_write() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let payload = Bytes::from((0..=250_u8).cycle().take(20 * MIB).collect::<Vec<_>>());
    let stage = prepared(&destination, Some(payload.len() as u64)).await?;
    assert_eq!(*protocol.multipart_begins.lock().await, 0);
    let written = destination.write(&stage, chunks(&payload)).await?;
    assert_eq!(written.persisted_bytes, payload.len() as u64);
    assert_eq!(*protocol.multipart_begins.lock().await, 1);
    assert_eq!(*protocol.part_uploads.lock().await, 3);
    assert_eq!(*protocol.completes.lock().await, 1);
    assert_eq!(
        protocol.objects.lock().await.get("dir/final"),
        Some(&payload)
    );
    assert!(protocol.uploads.lock().await.is_empty());
    publish_and_verify(&destination, &stage, &payload).await?;
    assert_eq!(*protocol.completes.lock().await, 1);
    assert_eq!(*protocol.puts.lock().await, 0);
    assert!(no_artifacts(&protocol).await);
    // A discard never deletes the final key.
    destination.discard(stage).await?;
    assert!(protocol.objects.lock().await.contains_key("dir/final"));
    Ok(())
}

/// An unknown size goes through a multipart upload, however small; on a versioned bucket the
/// publication names the version the completion created.
#[tokio::test]
async fn an_unknown_size_is_a_multipart_upload_reporting_its_version() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .put_version("dir/final", "v1", Bytes::from_static(b"old"))
        .await;
    let destination = destination(&protocol);
    let payload = Bytes::from_static(b"small but unknown");
    let stage = prepared(&destination, None).await?;
    destination.write(&stage, chunks(&payload)).await?;
    assert_eq!(*protocol.completes.lock().await, 1);
    let evidence = publish_and_verify(&destination, &stage, &payload).await?;
    let version = evidence.version.ok_or("no version")?;
    assert_ne!(version, "v1");
    assert_eq!(
        protocol.version.lock().await.as_deref(),
        Some(version.as_str())
    );
    Ok(())
}

/// A failed part aborts the upload inside `write`: no upload, no object, no temp key.
#[tokio::test]
async fn a_failed_part_aborts_the_upload() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    *protocol.part_failure.lock().await = Some((
        2,
        S3ProtocolFailure::session(
            FailureClass::Connectivity,
            Transience::Transient,
            "part lost",
        ),
    ));
    let destination = destination(&protocol);
    let payload = Bytes::from(vec![6; 20 * MIB]);
    let stage = prepared(&destination, Some(payload.len() as u64)).await?;
    assert!(destination.write(&stage, chunks(&payload)).await.is_err());
    assert!(protocol.uploads.lock().await.is_empty());
    assert_eq!(*protocol.aborts.lock().await, 1);
    assert_eq!(*protocol.completes.lock().await, 0);
    assert!(protocol.objects.lock().await.is_empty());
    destination.discard(stage).await?;
    assert_eq!(*protocol.aborts.lock().await, 1, "nothing left to abort");
    Ok(())
}

/// A failed input aborts the upload too.
#[tokio::test]
async fn a_failed_input_aborts_the_upload() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let stage = prepared(&destination, Some(20 * MIB as u64)).await?;
    let path = stage.final_destination.path().clone();
    let input: ByteStream = Box::pin(stream::iter([
        Ok(Bytes::from(vec![1; 9 * MIB])),
        Err(entry(&path, Operation::Read, "source failed")),
    ]));
    assert!(destination.write(&stage, input).await.is_err());
    assert!(protocol.uploads.lock().await.is_empty());
    assert_eq!(*protocol.aborts.lock().await, 1);
    assert!(protocol.objects.lock().await.is_empty());
    Ok(())
}

/// A completion whose reply is lost counts as done when the final object has our size and the
/// composite `ETag` of our parts. The upload is aborted anyway — the match may be an identical
/// earlier object while ours never completed (a completed one answers not-found) — and no version
/// is claimed.
#[tokio::test]
async fn a_lost_completion_reply_is_reconciled() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    *protocol.complete_commits_then_fails.lock().await = true;
    let destination = destination(&protocol);
    let payload = Bytes::from(vec![7; 9 * MIB]);
    let stage = prepared(&destination, Some(payload.len() as u64)).await?;
    destination.write(&stage, chunks(&payload)).await?;
    assert_eq!(*protocol.aborts.lock().await, 1);
    assert!(protocol.uploads.lock().await.is_empty());
    publish_and_verify(&destination, &stage, &payload).await?;
    Ok(())
}

/// Uploads an earlier writer left on the final key are aborted before this one begins; uploads
/// on other keys are left alone.
#[tokio::test]
async fn orphan_uploads_on_the_final_key_are_aborted() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .begin_multipart("dir/final")
        .await
        .map_err(|e| format!("{e:?}"))?;
    let other = protocol
        .begin_multipart("dir/final.other")
        .await
        .map_err(|e| format!("{e:?}"))?;
    let destination = destination(&protocol);
    let payload = Bytes::from(vec![8; 9 * MIB]);
    let stage = prepared(&destination, Some(payload.len() as u64)).await?;
    destination.write(&stage, chunks(&payload)).await?;
    assert!(
        protocol
            .list_uploads("dir/final")
            .await
            .map_err(|e| format!("{e:?}"))?
            .is_empty()
    );
    assert_eq!(
        protocol
            .list_uploads("dir/final.other")
            .await
            .map_err(|e| format!("{e:?}"))?,
        [other]
    );
    assert_eq!(*protocol.aborts.lock().await, 1);
    Ok(())
}

/// A discard aborts an upload still open, and never touches the final key.
#[tokio::test]
async fn a_discard_aborts_an_open_upload_only() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .objects
        .lock()
        .await
        .insert("dir/final".into(), Bytes::from_static(b"existing"));
    let destination = destination(&protocol);
    let stage = prepared(&destination, None).await?;
    let upload = upload_of(&stage).ok_or("no direct upload")?;
    destination
        .begin_direct_upload(stage.final_destination.path(), upload)
        .await?;
    assert_eq!(protocol.uploads.lock().await.len(), 1);
    destination.discard(stage).await?;
    assert!(protocol.uploads.lock().await.is_empty());
    assert!(protocol.objects.lock().await.contains_key("dir/final"));
    Ok(())
}

/// A `Direct` write cannot replace the object it reads.
#[tokio::test]
async fn a_direct_write_over_its_own_source_is_refused() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let mut request = prepare_request(Some(4))?;
    request.final_destination = FinalDestination::new(StoragePath::new("source")?);
    let refused = destination(&protocol)
        .prepare_direct(request, CancellationToken::new())
        .await;
    assert!(matches!(
        refused,
        Err(StorageRoleFailure::Entry(failure)) if failure.class() == FailureClass::Conflict
    ));
    Ok(())
}

/// Read-back after publication finds the object replaced by someone else: `Conflict`.
#[tokio::test]
async fn a_replaced_direct_object_fails_verification() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let payload = Bytes::from(vec![9; 9 * MIB]);
    let stage = prepared(&destination, Some(payload.len() as u64)).await?;
    destination.write(&stage, chunks(&payload)).await?;
    protocol
        .objects
        .lock()
        .await
        .insert("dir/final".into(), Bytes::from(vec![0; 9 * MIB]));
    let evidence = destination
        .publish(
            &stage,
            PublishRequest {
                expected_size: payload.len() as u64,
                expected_blake3: Some(*blake3::hash(&payload).as_bytes()),
                cancel: CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| format!("{:?}", failure.error))?;
    let refused = destination
        .verify(
            &stage,
            VerifyRequest {
                expected_size: payload.len() as u64,
                expected_blake3: *blake3::hash(&payload).as_bytes(),
                cancel: CancellationToken::new(),
                published: Some(evidence),
            },
        )
        .await;
    assert!(matches!(
        refused,
        Err(StorageRoleFailure::Entry(ref failure)) if failure.class() == FailureClass::Conflict
    ));
    Ok(())
}

/// A `Direct` completion replaces the final object irreversibly, so an input shorter than the
/// source's known size is never completed: the upload is aborted and the final key untouched.
#[tokio::test]
async fn a_short_direct_input_is_never_completed() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let payload = Bytes::from(vec![7; 9 * MIB]);
    let stage = prepared(&destination, Some(payload.len() as u64 + 1)).await?;
    let refused = destination.write(&stage, chunks(&payload)).await;
    assert!(matches!(
        refused,
        Err(StorageRoleFailure::Entry(ref failure)) if failure.class() == FailureClass::InvalidInput
    ));
    assert!(!protocol.objects.lock().await.contains_key("dir/final"));
    assert!(protocol.uploads.lock().await.is_empty());
    Ok(())
}

#[test]
fn only_the_multipart_etag_form_is_compared() {
    assert!(is_composite("\"68daf77c9985b60757239608a9f5e69e-3\""));
    assert!(is_composite("68daf77c9985b60757239608a9f5e69e-10000"));
    assert!(!is_composite("\"68daf77c9985b60757239608a9f5e69e\""));
    assert!(!is_composite("\"68daf77c9985b60757239608a9f5e69e-\""));
    assert!(!is_composite("\"abc-x1\""));
}

#[test]
fn the_default_threshold_is_where_direct_switches_to_multipart() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    for (size, single) in [
        (Some(DEFAULT_SINGLE_PUT_THRESHOLD), true),
        (Some(DEFAULT_SINGLE_PUT_THRESHOLD + 1), false),
        (None, false),
    ] {
        let stage =
            destination.prepare_direct_stage(prepare_request(size)?, &CancellationToken::new())?;
        assert_eq!(single::of(&stage).is_some(), single, "{size:?}");
        assert_eq!(upload_of(&stage).is_some(), !single, "{size:?}");
    }
    Ok(())
}
