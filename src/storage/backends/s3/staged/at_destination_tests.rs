//! The multipart upload on the final key, resumable through its `.upload` pointer (ADR-0006
//! C15b), over the in-memory S3.

use bytes::Bytes;
use futures::stream;
use tokio_util::sync::CancellationToken;

use super::super::upload_discovery::contiguous_prefix;
use super::*;
use crate::model::{EntryKind, IdentityStrength, SourceIdentity};
use crate::storage::artifacts::{ArtifactKind, artifact_name};
use crate::storage::backends::s3::S3PartFacts;
use crate::storage::backends::s3::S3TagSupport;
use crate::storage::backends::s3::metadata::S3Metadata;
use crate::storage::backends::s3::tests::{MemoryS3, content_md5, identity};
use crate::storage::{
    CheckpointRegistration, DeferredCheckpoint, FinalDestination, PrepareRequest, RecoveryIdentity,
    SourceDescriptor, StagedDestination, VerificationPoint,
};
use crate::storage::{RestartReason, ResumeMode};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const MIB: usize = 1024 * 1024;
const PART: usize = 8 * MIB;
const IDENTITY: [u8; 32] = [4; 32];
const BINDING: [u8; 32] = [7; 32];
const FINAL: &str = "dir/final";

fn pointer_key() -> String {
    format!("dir/{}", artifact_name("final", ArtifactKind::Upload))
}

fn payload(size: usize) -> Bytes {
    Bytes::from((0..=250_u8).cycle().take(size).collect::<Vec<_>>())
}

fn request(
    size: usize,
    binding: [u8; 32],
    resume: ResumeMode,
    recoverable: bool,
) -> TestResult<DestinationPrepareRequest> {
    let prepare = PrepareRequest {
        final_destination: FinalDestination::new(StoragePath::new(FINAL)?),
        source: SourceDescriptor::new(
            StoragePath::new("source")?,
            EntryKind::File,
            Some(size as u64),
            SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"source")?,
        ),
        recovery_binding: binding,
    };
    Ok(DestinationPrepareRequest::new(prepare, IDENTITY)
        .with_resume(resume)
        .with_recoverable(recoverable))
}

/// The automatic interval of [`destination`]: a recoverable upload over it writes its pointer at
/// prepare.
const INTERVAL: usize = 2 * PART;

fn destination(protocol: &Arc<MemoryS3>) -> S3StagedDestination<MemoryS3> {
    S3StagedDestination::new(protocol.clone(), identity()).with_checkpoint_interval(INTERVAL as u64)
}

fn chunks(bytes: &[u8]) -> ByteStream {
    let pieces: Vec<_> = bytes
        .chunks(MIB)
        .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
        .collect();
    Box::pin(stream::iter(pieces))
}

/// `bytes`, then a failed read (the source connection dropped).
fn failing_after(bytes: &[u8]) -> TestResult<ByteStream> {
    let mut pieces: Vec<_> = bytes
        .chunks(MIB)
        .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
        .collect();
    pieces.push(Err(classified_entry(
        &StoragePath::new("source")?,
        Operation::Read,
        FailureClass::Connectivity,
        Transience::Transient,
        "cut",
    )));
    Ok(Box::pin(stream::iter(pieces)))
}

struct NeverRegistered;

#[async_trait]
impl CheckpointRegistration for NeverRegistered {
    async fn register(
        &self,
        _stage: &PreparedStage,
        _identity: RecoveryIdentity,
    ) -> Result<(), StorageRoleFailure> {
        Err(entry(
            &StoragePath::root(),
            Operation::Prepare,
            "a stage kept at the destination never registers",
        ))
    }
}

fn with_checkpoint(stage: &mut PreparedStage, interval: usize, size: usize) {
    stage.deferred_checkpoint = Some(DeferredCheckpoint {
        interval_bytes: interval as u64,
        source_size: size as u64,
        registration: Arc::new(NeverRegistered),
    });
}

async fn publish(
    destination: &S3StagedDestination<MemoryS3>,
    stage: &PreparedStage,
    data: &Bytes,
) -> Result<PublicationEvidence, PublicationFailure> {
    destination
        .publish(
            stage,
            PublishRequest {
                expected_size: data.len() as u64,
                expected_blake3: Some(*blake3::hash(data).as_bytes()),
                cancel: CancellationToken::new(),
            },
        )
        .await
}

async fn publish_and_verify(
    destination: &S3StagedDestination<MemoryS3>,
    stage: &PreparedStage,
    data: &Bytes,
) -> TestResult<PublicationEvidence> {
    let evidence = publish(destination, stage, data)
        .await
        .map_err(|failure| format!("{:?}", failure.error))?;
    let verified = destination
        .verify(
            stage,
            VerifyRequest {
                expected_size: data.len() as u64,
                expected_blake3: *blake3::hash(data).as_bytes(),
                cancel: CancellationToken::new(),
                published: Some(evidence.clone()),
            },
        )
        .await?;
    assert_eq!(verified.verified_bytes, data.len() as u64);
    Ok(evidence)
}

async fn pointer(protocol: &MemoryS3) -> Option<DestinationPointer> {
    let bytes = protocol.objects.lock().await.get(&pointer_key()).cloned()?;
    DestinationPointer::decode(&bytes).ok()
}

async fn uploads(protocol: &MemoryS3) -> TestResult<Vec<String>> {
    Ok(protocol
        .list_uploads(FINAL)
        .await
        .map_err(|failure| format!("{failure:?}"))?)
}

/// An upload on the final key holding `parts` (number, bytes), as a killed writer leaves it.
async fn seed_upload(protocol: &MemoryS3, parts: &[(i32, &[u8])]) -> TestResult<String> {
    let id = protocol
        .begin_multipart(FINAL)
        .await
        .map_err(|failure| format!("{failure:?}"))?;
    for (number, bytes) in parts {
        let bytes = Bytes::copy_from_slice(bytes);
        let md5 = content_md5(&bytes);
        protocol
            .upload_part(FINAL, &id, *number, bytes, &md5)
            .await
            .map_err(|failure| format!("{failure:?}"))?;
    }
    Ok(id)
}

async fn seed_pointer(protocol: &MemoryS3, binding: [u8; 32], extension: Bytes) -> TestResult {
    let bytes = DestinationPointer {
        binding,
        transfer_identity: IDENTITY,
        durable_prefix: None,
        extension,
    }
    .encode()
    .map_err(|_| "encode")?;
    protocol
        .objects
        .lock()
        .await
        .insert(pointer_key(), Bytes::from(bytes));
    Ok(())
}

fn record(upload_id: &str) -> TestResult<Bytes> {
    Ok(UploadRecord {
        nonce: [1; 16],
        part_size: PART as u64,
        upload_id: upload_id.to_string(),
    }
    .encode()
    .ok_or("record")?)
}

fn class(error: &StorageRoleFailure) -> Option<FailureClass> {
    match error {
        StorageRoleFailure::Entry(entry) => Some(entry.class()),
        StorageRoleFailure::Session(_) => None,
    }
}

/// A checkpointed upload that is not recoverable from the start (the engine's automatic interval)
/// begins its upload on the final key and writes its pointer at once (ADR-0006 C16), but turns its
/// recovery on only at its first deferred checkpoint, without writing the pointer again;
/// publication completes it and removes the pointer.
#[tokio::test]
async fn a_resumable_upload_writes_its_pointer_at_once_and_recovers_from_its_checkpoint()
-> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let data = payload(40 * MIB);
    let request = request(data.len(), BINDING, ResumeMode::Discover, false)?;
    let mut stage = destination.prepare_at_destination(request).await?;
    assert_eq!(stage.prepare_fact(), PrepareFact::Fresh);
    assert!(stage.at_destination && !stage.recovery_enabled());
    assert_eq!(
        destination.verification_point(&stage),
        VerificationPoint::AfterPublish
    );
    let open = uploads(&protocol).await?;
    assert_eq!(open.len(), 1);
    assert!(pointer(&protocol).await.is_some());
    with_checkpoint(&mut stage, 16 * MIB, data.len());
    destination.write(&stage, chunks(&data)).await?;
    assert!(stage.recovery_enabled());
    let recorded = pointer(&protocol).await.ok_or("the prepare's pointer")?;
    assert_eq!(
        (
            recorded.binding,
            recorded.transfer_identity,
            recorded.durable_prefix
        ),
        (BINDING, IDENTITY, None)
    );
    let upload = UploadRecord::decode(&recorded.extension).ok_or("upload record")?;
    assert_eq!(
        (upload.upload_id, upload.part_size),
        (open[0].clone(), PART as u64)
    );
    assert!(!protocol.objects.lock().await.contains_key(FINAL));
    publish_and_verify(&destination, &stage, &data).await?;
    assert_eq!(protocol.objects.lock().await.get(FINAL), Some(&data));
    assert!(pointer(&protocol).await.is_none());
    assert!(uploads(&protocol).await?.is_empty());
    assert_eq!(*protocol.puts.lock().await, 1);
    Ok(())
}

/// D3: a checkpointed transfer up to the interval, or one that may not resume (`Restart`: an
/// atomic replace), writes no pointer, whatever its size.
#[tokio::test]
async fn a_small_or_unresumable_upload_writes_no_pointer() -> TestResult {
    for (size, resume) in [
        (INTERVAL, ResumeMode::Discover),
        (40 * MIB, ResumeMode::Restart),
    ] {
        let protocol = Arc::new(MemoryS3::default());
        let destination = destination(&protocol);
        let data = payload(size);
        let request = request(data.len(), BINDING, resume, false)?;
        let stage = destination.prepare_at_destination(request).await?;
        destination.write(&stage, chunks(&data)).await?;
        assert!(!stage.recovery_enabled());
        publish_and_verify(&destination, &stage, &data).await?;
        assert_eq!(*protocol.puts.lock().await, 0, "{size} {resume:?}");
        assert!(uploads(&protocol).await?.is_empty());
    }
    Ok(())
}

/// ADR-0006 C16: a writer killed before its first deferred checkpoint (a container restart) left
/// its pointer from prepare, so the next prepare resumes from the parts the service lists instead
/// of aborting them as an upload without a pointer.
#[tokio::test]
async fn a_crash_before_the_first_checkpoint_still_resumes() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let data = payload(7 * PART);
    let request = || request(data.len(), BINDING, ResumeMode::Discover, false);
    let mut stage = destination.prepare_at_destination(request()?).await?;
    with_checkpoint(&mut stage, 6 * PART, data.len());
    let cut = destination
        .write(&stage, failing_after(&data[..3 * PART])?)
        .await;
    assert!(cut.is_err());
    assert!(!stage.recovery_enabled(), "the checkpoint was not reached");
    drop(stage);

    let stage = destination.prepare_at_destination(request()?).await?;
    let resumed = 3 * PART as u64;
    assert_eq!(
        stage.prepare_fact(),
        PrepareFact::Resumed { bytes: resumed }
    );
    assert_eq!(uploads(&protocol).await?.len(), 1);
    destination.write(&stage, chunks(&data[3 * PART..])).await?;
    publish_and_verify(&destination, &stage, &data).await?;
    assert_eq!(protocol.objects.lock().await.get(FINAL), Some(&data));
    assert!(pointer(&protocol).await.is_none());
    Ok(())
}

/// ADR-0006 C16: the pointer written at prepare failing (a `BadDigest`: `Corruption`, transient)
/// fails the prepare with that error and aborts the upload it began, leaving nothing on the key.
#[tokio::test]
async fn a_failed_pointer_at_prepare_aborts_the_new_upload() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    *protocol.bad_digest_next_put.lock().await = true;
    let request = request(3 * PART, BINDING, ResumeMode::Discover, false)?;
    let error = destination
        .prepare_at_destination(request)
        .await
        .err()
        .ok_or("the pointer PUT must fail the prepare")?;
    let StorageRoleFailure::Entry(entry) = &error else {
        return Err("an entry failure".into());
    };
    assert_eq!(
        (entry.class(), entry.transience()),
        (FailureClass::Corruption, Transience::Transient)
    );
    assert_eq!(*protocol.multipart_begins.lock().await, 1);
    assert!(uploads(&protocol).await?.is_empty());
    assert!(pointer(&protocol).await.is_none());
    Ok(())
}

/// A prepare that wrote no pointer (`Restart`) but is given a deferred checkpoint writes it at
/// that checkpoint and turns recovery on — also when the pointer's `PutObject` stored it but lost
/// the reply (it reads back byte for byte, so it counts as written).
#[tokio::test]
async fn a_checkpoint_writes_the_pointer_prepare_did_not() -> TestResult {
    for lost_reply in [false, true] {
        let protocol = Arc::new(MemoryS3::default());
        let destination = destination(&protocol);
        let data = payload(5 * PART);
        let request = request(data.len(), BINDING, ResumeMode::Restart, false)?;
        let mut stage = destination.prepare_at_destination(request).await?;
        assert!(pointer(&protocol).await.is_none());
        with_checkpoint(&mut stage, 2 * PART, data.len());
        *protocol.put_commits_then_fails.lock().await = lost_reply;
        destination.write(&stage, chunks(&data)).await?;
        assert!(stage.recovery_enabled(), "lost_reply={lost_reply}");
        let recorded = pointer(&protocol).await.ok_or("the checkpoint's pointer")?;
        assert_eq!(recorded.binding, BINDING);
        assert_eq!(*protocol.puts.lock().await, 1);
        publish_and_verify(&destination, &stage, &data).await?;
        assert!(pointer(&protocol).await.is_none());
    }
    Ok(())
}

/// A write cut after its checkpoint leaves the upload and its pointer; the next prepare resumes
/// from the parts the service lists and uploads only the rest — also on a store that lists the
/// upload under another spelling of its id than it issued (`MinIO`), where the resume must not take
/// its own upload for another writer's and abort it.
#[tokio::test]
async fn an_interrupted_upload_resumes_from_its_listed_parts() -> TestResult {
    for minio in [false, true] {
        let protocol = Arc::new(MemoryS3::default());
        *protocol.minio_upload_ids.lock().await = minio;
        let destination = destination(&protocol);
        let data = payload(7 * PART);
        let request = || request(data.len(), BINDING, ResumeMode::Discover, false);
        let mut stage = destination.prepare_at_destination(request()?).await?;
        with_checkpoint(&mut stage, 2 * PART, data.len());
        let cut = destination
            .write(&stage, failing_after(&data[..6 * PART])?)
            .await;
        assert!(cut.is_err());
        assert!(stage.recovery_enabled());
        let first = pointer(&protocol).await.ok_or("pointer")?;
        drop(stage);

        let stage = destination.prepare_at_destination(request()?).await?;
        let resumed = 6 * PART as u64;
        assert_eq!(
            stage.prepare_fact(),
            PrepareFact::Resumed { bytes: resumed },
            "minio={minio}"
        );
        assert_eq!(stage.write_offset, resumed);
        assert_eq!(uploads(&protocol).await?.len(), 1, "minio={minio}");
        let second = pointer(&protocol).await.ok_or("pointer")?;
        assert_ne!(first.extension, second.extension, "a resume takes over");
        let sent = *protocol.part_uploads.lock().await;
        destination.write(&stage, chunks(&data[6 * PART..])).await?;
        assert_eq!(*protocol.part_uploads.lock().await, sent + 1);
        publish_and_verify(&destination, &stage, &data).await?;
        assert_eq!(protocol.objects.lock().await.get(FINAL), Some(&data));
        assert!(pointer(&protocol).await.is_none());
    }
    Ok(())
}

/// Parts 1, 2 and 4 (a gap, as a killed writer leaves it): the resume continues from the
/// contiguous prefix — two parts — and uploads 3, 4 and 5; part 4 is replaced, not an error.
#[tokio::test]
async fn a_gap_resumes_from_the_contiguous_prefix() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let data = payload(5 * PART);
    let part = |number: usize| &data[(number - 1) * PART..number * PART];
    let id = seed_upload(&protocol, &[(1, part(1)), (2, part(2)), (4, part(4))]).await?;
    seed_pointer(&protocol, BINDING, record(&id)?).await?;
    let request = request(data.len(), BINDING, ResumeMode::Discover, false)?;
    let stage = destination.prepare_at_destination(request).await?;
    let prefix = 2 * PART as u64;
    assert_eq!(stage.prepare_fact(), PrepareFact::Resumed { bytes: prefix });
    assert_eq!(stage.write_offset, prefix);
    assert_eq!(uploads(&protocol).await?, vec![id.clone()]);
    let taken = pointer(&protocol).await.ok_or("pointer")?;
    let taken = UploadRecord::decode(&taken.extension).ok_or("record")?;
    assert_eq!(taken.upload_id, id);
    assert_ne!(taken.nonce, [1; 16]);
    destination.write(&stage, chunks(&data[2 * PART..])).await?;
    assert_eq!(*protocol.part_uploads.lock().await, 3 + 3);
    publish_and_verify(&destination, &stage, &data).await?;
    assert_eq!(protocol.objects.lock().await.get(FINAL), Some(&data));
    Ok(())
}

/// What discovery finds and cleans up: a pointer whose upload is gone, an upload without a
/// pointer, another binding's pointer, and a pointer another backend wrote. The old upload is
/// aborted, the pointer removed, and a new upload begun.
#[tokio::test]
async fn leftovers_are_cleaned_up_and_the_upload_starts_over() -> TestResult {
    let data = payload(3 * PART);
    let mut hdfs = b"DMHSTG01".to_vec();
    hdfs.extend_from_slice(&[0; 16]);
    for (case, reason) in [
        ("pointer without upload", RestartReason::PointerWithoutStage),
        ("upload without pointer", RestartReason::StageWithoutPointer),
        ("other binding", RestartReason::BindingChanged),
        ("other backend's pointer", RestartReason::PointerCorrupt),
    ] {
        let protocol = Arc::new(MemoryS3::default());
        let destination = destination(&protocol);
        let old = if case == "pointer without upload" {
            "gone".to_string()
        } else {
            seed_upload(&protocol, &[(1, &data[..PART])]).await?
        };
        match case {
            "pointer without upload" => seed_pointer(&protocol, BINDING, record(&old)?).await?,
            "other binding" => seed_pointer(&protocol, [9; 32], record(&old)?).await?,
            "other backend's pointer" => {
                seed_pointer(&protocol, BINDING, Bytes::from(hdfs.clone())).await?;
            }
            _ => {}
        }
        let request = request(data.len(), BINDING, ResumeMode::Discover, false)?;
        let stage = destination.prepare_at_destination(request).await?;
        assert_eq!(
            stage.prepare_fact(),
            PrepareFact::Restarted { reason },
            "{case}"
        );
        let open = uploads(&protocol).await?;
        assert_eq!(open.len(), 1, "{case}");
        assert_ne!(open[0], old, "{case}");
        // The old pointer is gone; the new upload's own (ADR-0006 C16) names it.
        let fresh = pointer(&protocol).await.ok_or("the new upload's pointer")?;
        let fresh = UploadRecord::decode(&fresh.extension).ok_or("upload record")?;
        assert_eq!(fresh.upload_id, open[0], "{case}");
        destination.write(&stage, chunks(&data)).await?;
        publish_and_verify(&destination, &stage, &data).await?;
    }
    Ok(())
}

/// A second prepare takes the upload over (a new nonce in the pointer); the first writer's
/// publication is then refused with `Conflict`, the final key unchanged, and its discard leaves
/// everything to the second, which completes the upload.
#[tokio::test]
async fn a_take_over_refuses_the_first_writers_publication() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let data = payload(3 * PART);
    let request = || request(data.len(), BINDING, ResumeMode::Discover, true);
    let first = destination.prepare_at_destination(request()?).await?;
    destination.write(&first, chunks(&data)).await?;
    let second = destination.prepare_at_destination(request()?).await?;
    assert_eq!(
        second.prepare_fact(),
        PrepareFact::Resumed {
            bytes: data.len() as u64
        }
    );
    let refused = publish(&destination, &first, &data)
        .await
        .err()
        .ok_or("the first writer's publication must be refused")?;
    assert_eq!(class(&refused.error), Some(FailureClass::Conflict));
    assert!(!refused.final_destination_changed);
    assert!(!protocol.objects.lock().await.contains_key(FINAL));
    let taken = pointer(&protocol).await.ok_or("the second's pointer")?;
    destination.discard(first).await?;
    assert_eq!(pointer(&protocol).await, Some(taken));
    assert_eq!(uploads(&protocol).await?.len(), 1);
    destination.write(&second, chunks(&[])).await?;
    publish_and_verify(&destination, &second, &data).await?;
    assert_eq!(protocol.objects.lock().await.get(FINAL), Some(&data));
    assert!(pointer(&protocol).await.is_none());
    Ok(())
}

async fn written_stage(
    protocol: &Arc<MemoryS3>,
    data: &Bytes,
) -> TestResult<(S3StagedDestination<MemoryS3>, PreparedStage)> {
    let destination = destination(protocol);
    let request = request(data.len(), BINDING, ResumeMode::Discover, true)?;
    let stage = destination.prepare_at_destination(request).await?;
    destination.write(&stage, chunks(data)).await?;
    Ok((destination, stage))
}

/// A completion whose reply is lost, with the upload gone afterwards: our size and composite
/// `ETag` at the final key count as published and the pointer is removed. The key's versions list
/// the object as `"null"` here, so no version is claimed (a versioned bucket's is: ADR-0006 C17,
/// `transfer::s3_versioning_tests`).
#[tokio::test]
async fn a_lost_completion_reply_of_a_gone_upload_counts_as_published() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .put_version("elsewhere", "v1", Bytes::from_static(b"x"))
        .await;
    let data = payload(3 * PART);
    let (destination, stage) = written_stage(&protocol, &data).await?;
    *protocol.complete_commits_then_fails.lock().await = true;
    let evidence = publish_and_verify(&destination, &stage, &data).await?;
    assert_eq!(evidence.version, None);
    assert_eq!(protocol.objects.lock().await.get(FINAL), Some(&data));
    assert!(pointer(&protocol).await.is_none());
    Ok(())
}

/// A completion the service refused before it could commit (`InvalidPart`, a permanent
/// `Conflict`) left the final key unchanged: the stage (upload and pointer) is kept, and
/// publishing again succeeds.
#[tokio::test]
async fn a_refused_completion_keeps_the_stage() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let data = payload(3 * PART);
    let (destination, stage) = written_stage(&protocol, &data).await?;
    *protocol.complete_failure.lock().await = Some(S3ProtocolFailure::entry(
        FailureClass::Conflict,
        Transience::Permanent,
        "InvalidPart",
    ));
    let failed = publish(&destination, &stage, &data)
        .await
        .err()
        .ok_or("the completion failed")?;
    assert!(!failed.final_destination_changed);
    assert!(!protocol.objects.lock().await.contains_key(FINAL));
    assert_eq!(uploads(&protocol).await?.len(), 1);
    assert!(pointer(&protocol).await.is_some());
    publish_and_verify(&destination, &stage, &data).await?;
    assert!(pointer(&protocol).await.is_none());
    Ok(())
}

/// A completion that failed without a definite refusal (a reset, a timeout, a server error) may
/// still complete on the server even while `ListParts` lists the upload: the final key is
/// reported changed. Cleaning up afterwards removes the pointer and aborts the upload — or finds
/// it completed — and never touches the final key.
#[tokio::test]
async fn an_ambiguous_completion_failure_reports_the_final_key_changed() -> TestResult {
    let ambiguous = [
        (FailureClass::Connectivity, Transience::Transient),
        (FailureClass::Internal, Transience::Transient),
        (FailureClass::Protocol, Transience::Unknown),
    ];
    for (failure_class, transience) in ambiguous {
        let protocol = Arc::new(MemoryS3::default());
        let data = payload(3 * PART);
        let (destination, stage) = written_stage(&protocol, &data).await?;
        *protocol.complete_failure.lock().await = Some(S3ProtocolFailure::session(
            failure_class,
            transience,
            "reset",
        ));
        let failed = publish(&destination, &stage, &data)
            .await
            .err()
            .ok_or("the completion failed")?;
        assert!(failed.final_destination_changed, "{failure_class:?}");
        assert_eq!(uploads(&protocol).await?.len(), 1, "still listed");
        destination.discard(stage).await?;
        assert!(pointer(&protocol).await.is_none());
        assert!(uploads(&protocol).await?.is_empty());
    }
    Ok(())
}

/// An upload that vanished (aborted by someone else) with no object of ours at the final key is
/// a `Conflict` that may have changed the final key.
#[tokio::test]
async fn a_vanished_upload_without_our_object_is_a_changed_conflict() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let data = payload(3 * PART);
    let (destination, stage) = written_stage(&protocol, &data).await?;
    for id in uploads(&protocol).await? {
        protocol
            .abort_multipart(FINAL, &id)
            .await
            .map_err(|failure| format!("{failure:?}"))?;
    }
    let failed = publish(&destination, &stage, &data)
        .await
        .err()
        .ok_or("nothing was completed")?;
    assert_eq!(class(&failed.error), Some(FailureClass::Conflict));
    assert!(failed.final_destination_changed);
    Ok(())
}

/// A completed object whose `ETag` is not the composite of our parts is a permanent
/// `Corruption`, with the final key changed.
#[tokio::test]
async fn a_composite_mismatch_is_corruption_of_the_final_key() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let data = payload(3 * PART);
    let (destination, stage) = written_stage(&protocol, &data).await?;
    *protocol.complete_etag.lock().await = Some("\"0123456789abcdef0123456789abcdef-3\"".into());
    let failed = publish(&destination, &stage, &data)
        .await
        .err()
        .ok_or("the mismatch must fail")?;
    assert_eq!(class(&failed.error), Some(FailureClass::Corruption));
    assert!(failed.final_destination_changed);
    Ok(())
}

/// A discard removes the pointer before it aborts the upload: when the abort fails, the upload
/// without a pointer is what the next prepare cleans up. The final key is never touched.
#[tokio::test]
async fn a_discard_removes_the_pointer_before_the_abort() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let data = payload(3 * PART);
    protocol
        .objects
        .lock()
        .await
        .insert(FINAL.into(), Bytes::from_static(b"previous"));
    let (destination, stage) = written_stage(&protocol, &data).await?;
    *protocol.abort_failure.lock().await = Some(S3ProtocolFailure::session(
        FailureClass::Connectivity,
        Transience::Transient,
        "reset",
    ));
    assert!(destination.discard(stage).await.is_err());
    assert!(pointer(&protocol).await.is_none());
    assert_eq!(uploads(&protocol).await?.len(), 1);
    *protocol.abort_failure.lock().await = None;
    let (destination, stage) = written_stage(&protocol, &data).await?;
    assert_eq!(
        stage.prepare_fact(),
        PrepareFact::Restarted {
            reason: RestartReason::StageWithoutPointer
        }
    );
    destination.discard(stage).await?;
    assert!(pointer(&protocol).await.is_none());
    assert!(uploads(&protocol).await?.is_empty());
    assert_eq!(
        protocol.objects.lock().await.get(FINAL),
        Some(&Bytes::from_static(b"previous"))
    );
    Ok(())
}

/// A small object looks only at the pointer: a leftover one is removed with the uploads on the
/// key and reported, and the object is still one `PutObject`.
#[tokio::test]
async fn a_small_object_cleans_up_a_leftover_pointer() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let id = seed_upload(&protocol, &[(1, &payload(PART)[..])]).await?;
    seed_pointer(&protocol, [9; 32], record(&id)?).await?;
    let data = payload(MIB);
    let request = request(data.len(), BINDING, ResumeMode::Discover, true)?;
    let stage = destination.prepare_at_destination(request).await?;
    assert!(single::of(&stage).is_some() && stage.at_destination);
    assert_eq!(
        stage.prepare_fact(),
        PrepareFact::Restarted {
            reason: RestartReason::BindingChanged
        }
    );
    assert!(pointer(&protocol).await.is_none());
    assert!(uploads(&protocol).await?.is_empty());
    destination.write(&stage, chunks(&data)).await?;
    publish_and_verify(&destination, &stage, &data).await?;
    assert_eq!(*protocol.multipart_begins.lock().await, 1);
    Ok(())
}

/// Tags applied before publication are set once the upload completed.
#[tokio::test]
async fn tags_are_set_on_the_completed_object() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let data = payload(3 * PART);
    let metadata = Arc::new(S3Metadata::new(
        protocol.clone(),
        identity(),
        S3TagSupport::Supported,
    ));
    let destination = destination(&protocol).with_metadata(metadata);
    let request = request(data.len(), BINDING, ResumeMode::Discover, false)?;
    let stage = destination.prepare_at_destination(request).await?;
    destination.write(&stage, chunks(&data)).await?;
    let tag = ObjectTag::new("class", "gold")?;
    destination
        .apply_metadata(
            &stage,
            MetadataMutation::Tags(vec![tag.clone()]),
            CancellationToken::new(),
        )
        .await?;
    assert!(!protocol.objects.lock().await.contains_key(FINAL));
    publish_and_verify(&destination, &stage, &data).await?;
    let tags = protocol
        .get_tags(FINAL, None)
        .await
        .map_err(|failure| format!("{failure:?}"))?;
    assert_eq!(tags, vec![tag]);
    Ok(())
}

/// S3 keeps its recovery state at the destination (ADR-0006 C15c) with the 64 MiB automatic
/// interval; the test hook that puts the old temp-key path back plans no automatic checkpoints
/// (the store path registers every checkpointed upload from the start).
#[test]
fn s3_keeps_recovery_at_the_destination() {
    let protocol = Arc::new(MemoryS3::default());
    let on = S3StagedDestination::new(protocol.clone(), identity());
    assert!(on.recovery_at_destination());
    assert_eq!(
        on.automatic_checkpoint_interval_bytes(),
        Some(64 * MIB as u64)
    );
    let off = S3StagedDestination::new(protocol, identity()).with_recovery_at_destination(false);
    assert!(!off.recovery_at_destination());
    assert_eq!(off.automatic_checkpoint_interval_bytes(), None);
}

/// D3 for the expert destination half, which asks for a recoverable prepare of every
/// checkpointed object over one chunk: only an object over the interval writes its pointer at
/// prepare; a smaller one writes none and is not resumable.
#[tokio::test]
async fn a_recoverable_prepare_writes_a_pointer_only_over_the_interval() -> TestResult {
    for (size, pointer_written) in [(INTERVAL, false), (3 * PART, true)] {
        let protocol = Arc::new(MemoryS3::default());
        let destination = destination(&protocol);
        let data = payload(size);
        let request = request(size, BINDING, ResumeMode::Discover, true)?;
        let stage = destination.prepare_at_destination(request).await?;
        assert_eq!(stage.recovery_enabled(), pointer_written, "{size}");
        assert_eq!(
            pointer(&protocol).await.is_some(),
            pointer_written,
            "{size}"
        );
        destination.write(&stage, chunks(&data)).await?;
        publish_and_verify(&destination, &stage, &data).await?;
        assert_eq!(*protocol.puts.lock().await, u32::from(pointer_written));
        assert!(pointer(&protocol).await.is_none());
    }
    Ok(())
}

fn part(number: i32, size: usize) -> S3PartFacts {
    S3PartFacts {
        number,
        size: size as u64,
        etag: format!("e{number}"),
    }
}

fn numbers(prefix: &[(i32, String)]) -> Vec<i32> {
    prefix.iter().map(|part| part.0).collect()
}

#[test]
fn the_contiguous_prefix_stops_at_a_gap_or_a_wrong_size() {
    let size = Some(5 * PART as u64);
    let (bytes, prefix) = contiguous_prefix(
        vec![part(4, PART), part(1, PART), part(2, PART)],
        PART as u64,
        size,
    );
    assert_eq!((bytes, numbers(&prefix)), (2 * PART as u64, vec![1, 2]));
    let (bytes, prefix) = contiguous_prefix(
        vec![part(1, PART), part(2, PART - 1), part(3, PART)],
        PART as u64,
        size,
    );
    assert_eq!((bytes, numbers(&prefix)), (PART as u64, vec![1]));
    let (bytes, _) = contiguous_prefix(vec![part(2, PART)], PART as u64, size);
    assert_eq!(bytes, 0);
}

#[test]
fn only_a_last_part_that_ends_at_the_source_size_may_be_short() {
    let short = |size| contiguous_prefix(vec![part(1, PART), part(2, MIB)], PART as u64, size).0;
    assert_eq!(short(Some((PART + MIB) as u64)), (PART + MIB) as u64);
    assert_eq!(short(Some((PART + 2 * MIB) as u64)), PART as u64);
    assert_eq!(short(None), PART as u64);
    // A full part that would pass the source's end is not part of the prefix, and nothing past
    // the end is either.
    let (bytes, prefix) = contiguous_prefix(
        vec![part(1, PART), part(2, PART), part(3, PART)],
        PART as u64,
        Some((PART + MIB) as u64),
    );
    assert_eq!((bytes, numbers(&prefix)), (PART as u64, vec![1]));
    let (bytes, prefix) = contiguous_prefix(
        vec![part(1, PART), part(2, PART), part(3, PART)],
        PART as u64,
        Some(2 * PART as u64),
    );
    assert_eq!((bytes, numbers(&prefix)), (2 * PART as u64, vec![1, 2]));
}

#[path = "versioning_tests.rs"]
mod versioning;
