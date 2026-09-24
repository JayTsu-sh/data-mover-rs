//! Objects up to the single-PUT threshold (ADR-0006 C14b): one `PutObject` to the final key, no
//! upload and no temp key, read back after publication pinned to what the PUT created.

use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::{EntryKind, IdentityStrength, SourceIdentity, StoragePath};
use crate::storage::backends::s3::tests::{MemoryS3, etag_of, identity};
use crate::storage::{FinalDestination, SourceDescriptor};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const T: u64 = DEFAULT_SINGLE_PUT_THRESHOLD;

fn prepare_request(size: u64) -> TestResult<PrepareRequest> {
    Ok(PrepareRequest {
        source: SourceDescriptor::new(
            StoragePath::new("source")?,
            EntryKind::File,
            Some(size),
            SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"source")?,
        ),
        final_destination: FinalDestination::new(StoragePath::new("final")?),
        recovery_binding: [61; 32],
    })
}

fn destination(protocol: &Arc<MemoryS3>) -> S3StagedDestination<MemoryS3> {
    S3StagedDestination::new(protocol.clone(), identity())
}

fn publish_request(payload: &Bytes) -> PublishRequest {
    PublishRequest {
        expected_size: payload.len() as u64,
        expected_blake3: Some(*blake3::hash(payload).as_bytes()),
        cancel: CancellationToken::new(),
    }
}

fn verify_request(payload: &Bytes, published: &PublicationEvidence) -> VerifyRequest {
    VerifyRequest {
        expected_size: payload.len() as u64,
        expected_blake3: *blake3::hash(payload).as_bytes(),
        cancel: CancellationToken::new(),
        published: Some(published.clone()),
    }
}

/// Prepares and writes `payload` as one stage of `destination`.
async fn written(
    destination: &S3StagedDestination<MemoryS3>,
    payload: &Bytes,
) -> TestResult<PreparedStage> {
    let stage = destination
        .prepare(prepare_request(payload.len() as u64)?)
        .await?;
    let input = Box::pin(futures::stream::iter([Ok(payload.clone())]));
    assert_eq!(
        destination.write(&stage, input).await?.persisted_bytes,
        payload.len() as u64
    );
    Ok(stage)
}

async fn assert_no_stage_objects(protocol: &MemoryS3) {
    assert!(
        protocol
            .objects
            .lock()
            .await
            .keys()
            .all(|key| !key.starts_with(ARTIFACT_PREFIX)),
        "a single PUT leaves no temp key"
    );
    assert!(protocol.uploads.lock().await.is_empty());
    assert_eq!(*protocol.multipart_begins.lock().await, 0);
}

fn class(error: &StorageRoleFailure) -> (FailureClass, Transience) {
    match error {
        StorageRoleFailure::Entry(failure) => (failure.class(), failure.transience()),
        StorageRoleFailure::Session(failure) => (failure.class(), failure.transience()),
    }
}

#[test]
fn the_threshold_is_eight_mib_and_configurable_within_five_mib_and_five_gib() {
    assert_eq!(single_put_threshold(None).ok(), Some(8 * 1024 * 1024));
    for valid in [MIN_SINGLE_PUT_THRESHOLD, MAX_SINGLE_PUT_THRESHOLD] {
        assert_eq!(single_put_threshold(Some(valid)).ok(), Some(valid));
    }
    for invalid in [
        0,
        MIN_SINGLE_PUT_THRESHOLD - 1,
        MAX_SINGLE_PUT_THRESHOLD + 1,
    ] {
        let error = single_put_threshold(Some(invalid))
            .err()
            .map(|error| error.to_string());
        assert!(
            error.is_some_and(|message| message.contains("single_put_threshold")),
            "{invalid} accepted"
        );
    }
}

/// An object of exactly T bytes is one `PutObject` to the final key: no upload, no temp key.
#[tokio::test]
async fn an_object_of_exactly_the_threshold_is_one_put() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let payload = Bytes::from(vec![5; usize::try_from(T)?]);
    let stage = written(&destination, &payload).await?;
    assert!(!stage.recovery_enabled());
    assert_eq!(
        destination.verification_point(&stage),
        VerificationPoint::AfterPublish
    );
    assert_eq!(
        *protocol.puts.lock().await,
        0,
        "nothing is sent before publish"
    );
    let published = destination
        .publish(&stage, publish_request(&payload))
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(published.version, None);
    assert_eq!(*protocol.puts.lock().await, 1);
    assert_eq!(protocol.objects.lock().await.get("final"), Some(&payload));
    assert_no_stage_objects(&protocol).await;
    Ok(())
}

/// One byte over T keeps today's multipart upload on the temp key, verified before publication.
#[tokio::test]
async fn one_byte_over_the_threshold_is_still_multipart() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let stage = destination.prepare(prepare_request(T + 1)?).await?;
    assert_eq!(*protocol.multipart_begins.lock().await, 1);
    assert!(
        destination
            .stage_state(&stage, Operation::Write)
            .await
            .is_ok()
    );
    assert_eq!(
        destination.verification_point(&stage),
        VerificationPoint::BeforePublish
    );
    destination.discard(stage).await?;
    Ok(())
}

/// An empty object is one PUT as well.
#[tokio::test]
async fn an_empty_object_is_one_put() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let stage = destination.prepare(prepare_request(0)?).await?;
    let evidence = destination
        .write(&stage, Box::pin(futures::stream::empty()))
        .await?;
    assert_eq!(evidence.persisted_bytes, 0);
    let payload = Bytes::new();
    let published = destination
        .publish(&stage, publish_request(&payload))
        .await
        .map_err(|failure| failure.error)?;
    destination
        .verify(&stage, verify_request(&payload, &published))
        .await?;
    assert_eq!(*protocol.puts.lock().await, 1);
    assert_eq!(protocol.objects.lock().await.get("final"), Some(&payload));
    assert_no_stage_objects(&protocol).await;
    Ok(())
}

/// More bytes than the source size are refused as the caller's error, and nothing is sent.
#[tokio::test]
async fn more_bytes_than_the_source_size_are_invalid_input() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let stage = destination.prepare(prepare_request(3)?).await?;
    let input = Box::pin(futures::stream::iter([
        Ok(Bytes::from_static(b"ab")),
        Ok(Bytes::from_static(b"cd")),
    ]));
    let error = destination
        .write(&stage, input)
        .await
        .err()
        .ok_or("oversized write accepted")?;
    assert_eq!(class(&error).0, FailureClass::InvalidInput);
    let single = destination
        .write_single(&stage, Bytes::from_static(b"abcd"))
        .await
        .err()
        .ok_or("oversized single write accepted")?;
    assert_eq!(class(&single).0, FailureClass::InvalidInput);
    assert_eq!(*protocol.puts.lock().await, 0);
    Ok(())
}

/// `BadDigest` on the PUT is a transient `Corruption`: nothing published, final unchanged.
#[tokio::test]
async fn a_bad_digest_publishes_nothing_and_leaves_the_final_unchanged() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let payload = Bytes::from_static(b"corrupted in flight");
    let stage = written(&destination, &payload).await?;
    *protocol.bad_digest_next_put.lock().await = true;
    let failure = destination
        .publish(&stage, publish_request(&payload))
        .await
        .err()
        .ok_or("a bad digest published")?;
    assert!(!failure.final_destination_changed);
    assert_eq!(
        class(&failure.error),
        (FailureClass::Corruption, Transience::Transient)
    );
    assert!(protocol.objects.lock().await.get("final").is_none());
    // The stage is kept: publishing it again succeeds.
    destination
        .publish(&stage, publish_request(&payload))
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(protocol.objects.lock().await.get("final"), Some(&payload));
    Ok(())
}

/// A cancelled publication sends nothing and does not change the final object.
#[tokio::test]
async fn cancellation_before_the_put_leaves_the_final_unchanged() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let payload = Bytes::from_static(b"cancelled");
    let stage = written(&destination, &payload).await?;
    let request = publish_request(&payload);
    request.cancel.cancel();
    let failure = destination
        .publish(&stage, request)
        .await
        .err()
        .ok_or("a cancelled publication published")?;
    assert!(!failure.final_destination_changed);
    assert_eq!(class(&failure.error).0, FailureClass::Cancelled);
    assert_eq!(*protocol.puts.lock().await, 0);
    Ok(())
}

/// A PUT whose reply was lost is settled by HEAD: our size and our MD5 `ETag` count as published.
#[tokio::test]
async fn a_lost_put_reply_is_settled_by_head() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    *protocol.version.lock().await = Some("v0".into());
    let destination = destination(&protocol);
    let payload = Bytes::from_static(b"stored, reply lost");
    let stage = written(&destination, &payload).await?;
    *protocol.put_commits_then_fails.lock().await = true;
    let published = destination
        .publish(&stage, publish_request(&payload))
        .await
        .map_err(|failure| failure.error)?;
    let current = protocol.version.lock().await.clone();
    assert!(published.version.is_some());
    assert_eq!(published.version, current);
    destination
        .verify(&stage, verify_request(&payload, &published))
        .await?;
    Ok(())
}

/// A lost reply that HEAD cannot settle may have written the final object: changed.
#[tokio::test]
async fn an_unsettled_lost_put_reply_reports_the_final_changed() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let payload = Bytes::from_static(b"stored, reply lost, head fails");
    let stage = written(&destination, &payload).await?;
    *protocol.put_commits_then_fails.lock().await = true;
    *protocol.head_failure.lock().await = Some((
        "final".into(),
        S3ProtocolFailure::session(
            FailureClass::Connectivity,
            Transience::Transient,
            "endpoint unavailable",
        ),
    ));
    let failure = destination
        .publish(&stage, publish_request(&payload))
        .await
        .err()
        .ok_or("an unsettled PUT counted as published")?;
    assert!(failure.final_destination_changed);
    assert_eq!(class(&failure.error).0, FailureClass::Connectivity);
    Ok(())
}

/// In a versioned bucket the read-back is pinned to our version; a later write is `Conflict`.
#[tokio::test]
async fn verification_reads_our_version_and_detects_a_later_overwrite() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .put_version("final", "v0", Bytes::from_static(b"previous"))
        .await;
    let destination = destination(&protocol);
    let payload = Bytes::from_static(b"versioned payload");
    let stage = written(&destination, &payload).await?;
    let published = destination
        .publish(&stage, publish_request(&payload))
        .await
        .map_err(|failure| failure.error)?;
    let version = published.version.clone().ok_or("no version reported")?;
    assert_ne!(version, "v0");
    let evidence = destination
        .verify(&stage, verify_request(&payload, &published))
        .await?;
    assert_eq!(evidence.blake3, *blake3::hash(&payload).as_bytes());
    let reads = protocol.range_observations.lock().await.clone();
    assert!(!reads.is_empty());
    for read in reads {
        assert_eq!(read.version_id.as_deref(), Some(version.as_str()));
        assert_eq!(read.etag, etag_of(&payload));
    }
    let later = Bytes::from_static(b"someone else");
    protocol
        .put_object(
            "final",
            later.clone(),
            &super::super::tests::content_md5(&later),
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
    let error = destination
        .verify(&stage, verify_request(&payload, &published))
        .await
        .err()
        .ok_or("a replaced object verified")?;
    assert_eq!(class(&error).0, FailureClass::Conflict);
    Ok(())
}

/// Without versions the read-back carries `If-Match` on our `ETag`; a later write is `Conflict`.
#[tokio::test]
async fn unversioned_verification_reads_with_our_etag_and_detects_a_later_overwrite() -> TestResult
{
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let payload = Bytes::from_static(b"unversioned payload");
    let stage = written(&destination, &payload).await?;
    let published = destination
        .publish(&stage, publish_request(&payload))
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(published.version, None);
    destination
        .verify(&stage, verify_request(&payload, &published))
        .await?;
    let reads = protocol.range_observations.lock().await.clone();
    assert!(!reads.is_empty());
    for read in reads {
        assert_eq!(read.version_id, None);
        assert_eq!(read.etag, etag_of(&payload));
    }
    protocol
        .objects
        .lock()
        .await
        .insert("final".into(), Bytes::from_static(b"overwritten"));
    let error = destination
        .verify(&stage, verify_request(&payload, &published))
        .await
        .err()
        .ok_or("an overwritten object verified")?;
    assert_eq!(class(&error).0, FailureClass::Conflict);
    Ok(())
}

/// Verifying before publication is refused: there is nothing at the final key yet.
#[tokio::test]
async fn verification_before_publication_is_refused() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let payload = Bytes::from_static(b"not yet");
    let stage = written(&destination, &payload).await?;
    let request = VerifyRequest {
        published: None,
        ..verify_request(&payload, &evidence_for("final")?)
    };
    assert!(destination.verify(&stage, request).await.is_err());
    Ok(())
}

fn evidence_for(path: &str) -> TestResult<PublicationEvidence> {
    Ok(PublicationEvidence {
        final_destination: StoragePath::new(path)?,
        disposition: crate::storage::PublicationDisposition::Published,
        version: None,
    })
}

/// Discarding an unpublished single stage touches nothing on the server.
#[tokio::test]
async fn discarding_an_unpublished_single_stage_sends_nothing() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .objects
        .lock()
        .await
        .insert("final".into(), Bytes::from_static(b"kept"));
    let destination = destination(&protocol);
    let stage = written(&destination, &Bytes::from_static(b"dropped")).await?;
    destination.discard(stage).await?;
    assert_eq!(*protocol.puts.lock().await, 0);
    assert_eq!(
        protocol.objects.lock().await.get("final"),
        Some(&Bytes::from_static(b"kept"))
    );
    Ok(())
}

/// Asked to recover a single stage anyway, the destination starts it again from nothing.
#[tokio::test]
async fn recovering_a_single_stage_restarts_it_fresh() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let payload = Bytes::from_static(b"restart me");
    let stage = written(&destination, &payload).await?;
    let identity = destination.recovery_identity(&stage).await?;
    let request = prepare_request(payload.len() as u64)?;
    let recovered = destination
        .recover(RecoverRequest {
            identity,
            final_destination: request.final_destination,
            source: request.source,
            recovery_binding: request.recovery_binding,
            claim_token: [62; 32],
        })
        .await?;
    assert_eq!(recovered.write_offset, 0);
    assert!(!recovered.recovery_enabled());
    assert_eq!(
        destination.observe_checkpoint(&recovered).await?,
        CheckpointObservation { durable_prefix: 0 }
    );
    assert_eq!(
        destination.verification_point(&recovered),
        VerificationPoint::AfterPublish
    );
    Ok(())
}

/// A configured threshold of `None` sends every object through a multipart upload.
#[tokio::test]
async fn without_a_threshold_every_object_is_multipart() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol).with_single_put_threshold(None);
    let stage = destination.prepare(prepare_request(1)?).await?;
    assert_eq!(*protocol.multipart_begins.lock().await, 1);
    destination.discard(stage).await?;
    Ok(())
}

/// Tags applied to the stage wait for the object: the PUT creates it, then they are set on it.
#[tokio::test]
async fn staged_tags_are_set_on_the_published_object() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let storage = crate::storage::backends::s3::connect(protocol.clone(), identity(), None)?;
    let destination = storage.staged_destination(&crate::storage::PreflightPolicy::production())?;
    let payload = Bytes::from_static(b"tagged");
    let stage = destination
        .prepare(prepare_request(payload.len() as u64)?)
        .await?;
    destination.write_single(&stage, payload.clone()).await?;
    let tag = crate::model::ObjectTag::new("class", "gold")?;
    destination
        .apply_metadata(
            &stage,
            MetadataMutation::Tags(vec![tag.clone()]),
            CancellationToken::new(),
        )
        .await?;
    assert!(
        protocol
            .get_tags("final", None)
            .await
            .map_err(|error| format!("{error:?}"))?
            .is_empty()
    );
    destination
        .publish(&stage, publish_request(&payload))
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(
        protocol
            .get_tags("final", None)
            .await
            .map_err(|error| format!("{error:?}"))?,
        vec![tag]
    );
    Ok(())
}
