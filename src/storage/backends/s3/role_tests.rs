//! S3 role behaviour over the in-memory bucket: ranges, uploads on the final key, tags,
//! resumes, cancellation, publication, and the store-era entry points removed in ADR-0006 C19.

use super::*;
use crate::model::{
    EntryKind, FailureClass, IdentityStrength, SourceIdentity, SourceVersion, Transience,
};
use crate::storage::backends::s3::staged::S3StagedDestination;
use crate::storage::{
    ByteStream, DestinationPrepareRequest, PrepareFact, PreparedStage, RecoverRequest,
    RecoveryIdentity, ResumeMode, SourceDescriptor, StagedDestination, StorageRoleFailure,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// A source named `source` of `size` bytes (`None`: unknown).
fn described(size: Option<u64>) -> TestResult<SourceDescriptor> {
    Ok(SourceDescriptor::new(
        StoragePath::new("source")?,
        EntryKind::File,
        size,
        SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"source")?,
    ))
}

/// A prepare at the destination (ADR-0006: the only S3 prepare since C19) of `source` to `path`.
fn at_destination(
    path: &str,
    source: SourceDescriptor,
    binding: [u8; 32],
    resume: ResumeMode,
) -> TestResult<DestinationPrepareRequest> {
    let prepare = PrepareRequest {
        final_destination: FinalDestination::new(StoragePath::new(path)?),
        source,
        recovery_binding: binding,
    };
    Ok(DestinationPrepareRequest::new(prepare, [3; 32]).with_resume(resume))
}

/// A fresh upload on the final key `final` of `source`, holding `payload`: nothing is published.
async fn written_to_final(
    destination: &dyn StagedDestination,
    source: SourceDescriptor,
    payload: &Bytes,
) -> TestResult<PreparedStage> {
    let request = at_destination("final", source, [7; 32], ResumeMode::Restart)?;
    let stage = destination.prepare_at_destination(request).await?;
    let input = Box::pin(futures::stream::iter([Ok(payload.clone())]));
    let written = destination.write(&stage, input).await?;
    assert_eq!(written.persisted_bytes, payload.len() as u64);
    Ok(stage)
}

/// Publishes `stage`, then reads the object back pinned to what the publication reported (an S3
/// stage is verified after publication).
async fn publish_and_verify(
    destination: &dyn StagedDestination,
    stage: &PreparedStage,
    content: &[u8],
) -> TestResult {
    let digest = *blake3::hash(content).as_bytes();
    let size = content.len() as u64;
    let published = destination
        .publish(
            stage,
            PublishRequest {
                expected_size: size,
                expected_blake3: Some(digest),
                cancel: CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    destination
        .verify(
            stage,
            VerifyRequest {
                expected_size: size,
                expected_blake3: digest,
                cancel: CancellationToken::new(),
                published: Some(published),
            },
        )
        .await?;
    Ok(())
}

fn class<T>(result: &Result<T, StorageRoleFailure>) -> Option<FailureClass> {
    match result {
        Err(StorageRoleFailure::Entry(error)) => Some(error.class()),
        _ => None,
    }
}

#[test]
fn certified_standard_s3_roles_are_available_in_production()
-> Result<(), Box<dyn std::error::Error>> {
    let storage = connect(
        Arc::new(MemoryS3::default()),
        identity(),
        Some(native_context()),
    )?;
    assert!(storage.read_source(&validation_policy()).is_ok());
    assert!(storage.staged_destination(&validation_policy()).is_ok());
    assert!(storage.metadata(&validation_policy()).is_ok());
    assert!(matches!(
        storage
            .capabilities()
            .availability(crate::storage::Capability::ReadSource),
        crate::storage::CapabilityAvailability::Supported
    ));
    Ok(())
}

#[tokio::test]
async fn every_range_is_bound_and_overwrites_cannot_mix_content()
-> Result<(), Box<dyn std::error::Error>> {
    use futures::StreamExt as _;
    for version in [None, Some("null"), Some("version-1")] {
        let protocol = Arc::new(MemoryS3::default());
        *protocol.version.lock().await = version.map(str::to_owned);
        protocol
            .objects
            .lock()
            .await
            .insert("source".into(), Bytes::from_static(b"abcdef"));
        let storage = connect(protocol.clone(), identity(), None)?;
        let source = storage.read_source(&validation_policy())?;
        let path = StoragePath::new("source")?;
        let before = source.describe(&path).await?;
        let mut stream = source
            .read(ReadRequest {
                path: path.clone(),
                range: None,
                expected_source: Some(before.source_identity.clone()),
                maximum_chunk_bytes: 3,
                read_inflight: 1,
                read_budget: None,
                cancel: CancellationToken::new(),
                source_qos: None,
                version: SourceVersion::Current,
            })
            .await?;
        assert_eq!(
            stream.next().await.transpose()?,
            Some(Bytes::from_static(b"abc"))
        );
        protocol
            .objects
            .lock()
            .await
            .insert("source".into(), Bytes::from_static(b"UVWXYZ"));
        assert!(stream.next().await.transpose().is_err());
        let reads = protocol.range_observations.lock().await;
        assert_eq!(reads.len(), 2);
        assert_eq!(reads[0].version_id.as_deref(), version);
        assert_eq!(reads[0].etag, reads[1].etag);
        if version != Some("version-1") {
            assert_ne!(
                before.source_identity,
                source.describe(&path).await?.source_identity
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn range_stream_multipart_verify_publish_and_readback()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), Bytes::from_static(b"0123456789"));
    // The multipart path itself: an object this small would otherwise be one PUT.
    let storage = connect_multipart_only(protocol.clone(), identity(), Some(native_context()))?;
    let policy = validation_policy();
    let source = storage.read_source(&policy)?;
    let descriptor = source.describe(&StoragePath::new("source")?).await?;
    let mut read = source
        .read(ReadRequest {
            path: StoragePath::new("source")?,
            range: Some(2..8),
            expected_source: Some(descriptor.clone().source_identity),
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            read_budget: None,
            cancel: CancellationToken::new(),
            source_qos: None,
            version: SourceVersion::Current,
        })
        .await?;
    assert_eq!(
        futures::StreamExt::next(&mut read)
            .await
            .transpose()?
            .expect("one range"),
        Bytes::from_static(b"234567")
    );

    let destination = storage.staged_destination(&policy)?;
    let payload = Bytes::from_static(b"streamed multipart payload");
    // An upload on the final key completes only the source's size.
    let mut descriptor = descriptor;
    descriptor.size = Some(payload.len() as u64);
    let stage = written_to_final(&*destination, descriptor, &payload).await?;
    assert!(
        !protocol.objects.lock().await.contains_key("final"),
        "nothing is visible before publication"
    );
    publish_and_verify(&*destination, &stage, &payload).await?;
    assert_eq!(protocol.objects.lock().await.get("final"), Some(&payload));
    Ok(())
}

#[tokio::test]
async fn tags_are_lazy_and_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryS3::default());
    let storage = connect(protocol.clone(), identity(), Some(native_context()))?;
    let metadata = storage.metadata(&validation_policy())?;
    let path = StoragePath::new("tagged")?;
    let omitted = metadata.observe(&path, ObservationPlan::default()).await?;
    assert!(matches!(omitted.tags(), MetadataObservation::NotRequested));
    assert_eq!(*protocol.tag_reads.lock().await, 0);
    let tag = ObjectTag::new("class", "gold")?;
    metadata
        .apply(
            &path,
            MetadataMutation::Tags(vec![tag.clone()]),
            CancellationToken::new(),
        )
        .await?;
    let observed = metadata
        .observe(
            &path,
            ObservationPlan::default().with_tags(ObservationMode::Required),
        )
        .await?;
    assert_eq!(observed.tags().value(), Some(&vec![tag]));
    assert_eq!(*protocol.tag_reads.lock().await, 1);
    Ok(())
}

#[tokio::test]
async fn unsupported_profile_tags_never_call_the_service() -> Result<(), Box<dyn std::error::Error>>
{
    let protocol = Arc::new(MemoryS3::default());
    let storage = connect_with_tag_support(
        protocol.clone(),
        identity(),
        Some(native_context()),
        S3TagSupport::Unsupported,
    )?;
    let metadata = storage.metadata(&validation_policy())?;
    let path = StoragePath::new("untagged")?;
    let observed = metadata
        .observe(
            &path,
            ObservationPlan::default().with_tags(ObservationMode::Required),
        )
        .await?;
    assert!(matches!(observed.tags(), MetadataObservation::Unsupported));
    let result = metadata
        .apply(
            &path,
            MetadataMutation::Tags(vec![ObjectTag::new("class", "gold")?]),
            CancellationToken::new(),
        )
        .await;
    assert!(
        matches!(result, Err(crate::storage::StorageRoleFailure::Entry(ref failure))
        if failure.class() == crate::model::FailureClass::Unsupported)
    );
    assert_eq!(*protocol.tag_reads.lock().await, 0);
    assert!(protocol.tags.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn failed_input_preserves_checkpoint_until_explicit_discard()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryS3::default());
    let storage = connect(protocol.clone(), identity(), Some(native_context()))?;
    let destination = storage.staged_destination(&validation_policy())?;
    let stage = destination
        .prepare_at_destination(at_destination(
            "final",
            described(None)?,
            [9; 32],
            ResumeMode::Restart,
        )?)
        .await?;
    let failure = super::source::entry(
        &StoragePath::new("source")?,
        crate::model::Operation::Read,
        "injected",
    );
    assert!(
        destination
            .write(&stage, Box::pin(futures::stream::iter([Err(failure)])))
            .await
            .is_err()
    );
    assert_eq!(*protocol.aborts.lock().await, 0);
    assert!(!protocol.objects.lock().await.contains_key("final"));
    destination.discard(stage).await?;
    assert_eq!(*protocol.aborts.lock().await, 1);
    assert!(protocol.uploads.lock().await.is_empty());
    assert!(protocol.objects.lock().await.is_empty(), "nor a pointer");
    Ok(())
}

#[tokio::test]
async fn cancellation_remains_a_typed_entry_outcome() -> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .objects
        .lock()
        .await
        .insert("source".into(), Bytes::from_static(b"payload"));
    let storage = connect(protocol, identity(), Some(native_context()))?;
    let source = storage.read_source(&validation_policy())?;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = source
        .read(ReadRequest {
            path: StoragePath::new("source")?,
            range: None,
            expected_source: None,
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            read_budget: None,
            cancel,
            source_qos: None,
            version: SourceVersion::Current,
        })
        .await;
    let Err(failure) = result else {
        panic!("pre-cancel returned a stream")
    };
    match failure {
        crate::storage::StorageRoleFailure::Entry(error) => {
            assert_eq!(error.class(), crate::model::FailureClass::Cancelled);
            assert_eq!(error.transience(), crate::model::Transience::Permanent);
        }
        crate::storage::StorageRoleFailure::Session(_) => {
            panic!("cancellation is not a session outage")
        }
    }
    Ok(())
}

#[tokio::test]
async fn overwrite_publication_does_not_head_the_existing_destination()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryS3::default());
    // The completion of an upload: an object this small would otherwise be one PUT.
    let storage = connect_multipart_only(protocol.clone(), identity(), Some(native_context()))?;
    let destination = storage.staged_destination(&validation_policy())?;
    let payload = Bytes::from_static(b"payload");
    let stage = written_to_final(&*destination, described(Some(7))?, &payload).await?;
    let previous = Bytes::from_static(b"previous");
    protocol
        .objects
        .lock()
        .await
        .insert("final".into(), previous);
    let unavailable = S3ProtocolFailure::session(
        FailureClass::Connectivity,
        Transience::Transient,
        "endpoint unavailable",
    );
    *protocol.head_failure.lock().await = Some(("final".into(), unavailable));
    let result = destination
        .publish(
            &stage,
            PublishRequest {
                expected_size: payload.len() as u64,
                expected_blake3: Some(*blake3::hash(&payload).as_bytes()),
                cancel: CancellationToken::new(),
            },
        )
        .await;
    let publication = result.map_err(|failure| failure.error)?;
    assert_eq!(
        publication.disposition,
        crate::storage::PublicationDisposition::Published
    );
    assert_eq!(
        protocol.objects.lock().await.get("final").cloned(),
        Some(payload)
    );
    Ok(())
}

/// `parts` parts of 8 MiB of `5`, then a failed read.
fn interrupted_after(parts: usize) -> TestResult<ByteStream> {
    let part = Bytes::from(vec![5u8; 8 * 1024 * 1024]);
    let mut input: Vec<_> = (0..parts).map(|_| Ok(part.clone())).collect();
    input.push(Err(super::source::entry(
        &StoragePath::new("source")?,
        crate::model::Operation::Read,
        "interrupted",
    )));
    Ok(Box::pin(futures::stream::iter(input)))
}

/// An upload interrupted after four parts is picked up from the destination by a fresh
/// connection: the pointer names the upload, the listed parts are the durable prefix, and only
/// the rest is sent.
#[tokio::test]
async fn an_upload_is_resumed_from_the_destination_after_reconnect() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let request = || at_destination("resumed", described(None)?, [6; 32], ResumeMode::Discover);
    let connected = || {
        connect(protocol.clone(), identity(), Some(native_context()))?
            .staged_destination(&validation_policy())
            .map_err(Into::<Box<dyn std::error::Error>>::into)
    };
    let destination = connected()?;
    let stage = destination.prepare_at_destination(request()?).await?;
    assert!(
        stage.recovery_enabled(),
        "an unknown size writes its pointer at once"
    );
    assert!(
        destination
            .write(&stage, interrupted_after(4)?)
            .await
            .is_err()
    );
    drop((stage, destination));

    let destination = connected()?;
    let resumed = destination.prepare_at_destination(request()?).await?;
    let checkpoint = 4 * 8 * 1024 * 1024;
    assert_eq!(
        resumed.prepare_fact(),
        PrepareFact::Resumed { bytes: checkpoint }
    );
    assert_eq!(resumed.write_offset, checkpoint);
    let observed = destination.observe_checkpoint(&resumed).await?;
    assert_eq!(observed.durable_prefix, checkpoint);
    let full = vec![5u8; 4 * 8 * 1024 * 1024 + 17];
    let rest = Bytes::copy_from_slice(&full[usize::try_from(checkpoint)?..]);
    let input = Box::pin(futures::stream::iter([Ok(rest)]));
    destination.write(&resumed, input).await?;
    publish_and_verify(&*destination, &resumed, &full).await?;
    let objects = protocol.objects.lock().await;
    assert_eq!(
        objects.get("resumed").map(Bytes::as_ref),
        Some(full.as_slice())
    );
    assert_eq!(objects.len(), 1, "the pointer is gone");
    Ok(())
}

/// The store era's entry points are gone (ADR-0006 C19): S3 is prepared only at the
/// destination, nothing is recorded where data-mover runs, and none of them touches the bucket.
#[tokio::test]
async fn store_era_entry_points_are_unsupported() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = connect(protocol.clone(), identity(), Some(native_context()))?
        .staged_destination(&validation_policy())?;
    let request = at_destination("final", described(None)?, [8; 32], ResumeMode::Discover)?;
    let prepare = request.prepare.clone();
    assert_eq!(
        class(&destination.prepare(prepare.clone()).await),
        Some(FailureClass::Unsupported)
    );
    assert_eq!(
        class(&destination.prepare_ephemeral(prepare.clone()).await),
        Some(FailureClass::Unsupported)
    );
    assert_eq!(*protocol.multipart_begins.lock().await, 0);
    assert!(protocol.objects.lock().await.is_empty());
    let stage = destination.prepare_at_destination(request).await?;
    assert_eq!(
        class(&destination.recovery_identity(&stage).await),
        Some(FailureClass::Unsupported)
    );
    assert_eq!(
        class(&destination.handoff_recovery(&stage).await),
        Some(FailureClass::Unsupported)
    );
    let recovered = destination
        .recover(RecoverRequest {
            identity: RecoveryIdentity::from_bytes(Bytes::from_static(b"old\0upload"))?,
            final_destination: prepare.final_destination,
            source: prepare.source,
            recovery_binding: prepare.recovery_binding,
            claim_token: [9; 32],
        })
        .await;
    assert_eq!(class(&recovered), Some(FailureClass::Unsupported));
    destination.discard(stage).await?;
    assert!(protocol.uploads.lock().await.is_empty());
    assert!(protocol.objects.lock().await.is_empty());
    Ok(())
}

/// The store era's temp-key stage: `.data-mover-stage/…` and an upload id, no backend state.
fn temp_key_stage() -> TestResult<PreparedStage> {
    Ok(PreparedStage::new(
        identity(),
        FinalDestination::new(StoragePath::new("final")?),
        Bytes::from_static(b".data-mover-stage/binding/final\0upload-1"),
        [8; 32],
        0,
        None,
    ))
}

/// A stage this destination did not prepare at the destination, nor as `Direct` — the store
/// era's temp-key stage — is refused by every role method with a permanent `Conflict`, before
/// anything is touched.
#[tokio::test]
async fn a_temp_key_stage_is_refused() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = connect(protocol.clone(), identity(), Some(native_context()))?
        .staged_destination(&validation_policy())?;
    let conflict = Some(FailureClass::Conflict);
    let stage = temp_key_stage()?;
    let input = Box::pin(futures::stream::iter([Ok(Bytes::from_static(b"data"))]));
    assert_eq!(class(&destination.write(&stage, input).await), conflict);
    assert_eq!(
        class(&destination.observe_checkpoint(&stage).await),
        conflict
    );
    let verify = VerifyRequest {
        expected_size: 4,
        expected_blake3: [0; 32],
        cancel: CancellationToken::new(),
        published: None,
    };
    assert_eq!(class(&destination.verify(&stage, verify).await), conflict);
    let tags = MetadataMutation::Tags(Vec::new());
    let applied = destination.apply_metadata(&stage, tags, CancellationToken::new());
    assert_eq!(class(&applied.await), conflict);
    let publish = PublishRequest {
        expected_size: 4,
        expected_blake3: None,
        cancel: CancellationToken::new(),
    };
    let refused = destination.publish(&stage, publish).await.err();
    let refused = refused.ok_or("a temp-key stage was published")?;
    assert_eq!(class(&Err::<(), _>(refused.error)), conflict);
    assert!(!refused.final_destination_changed);
    let source = S3NativeCopySource {
        bucket: "memory".into(),
        key: "source".into(),
        etag: "\"etag\"".into(),
        version_id: None,
        size: 4,
    };
    let adapter = S3StagedDestination::new(protocol.clone(), identity());
    let filled = adapter.fill_native(&stage, source, CancellationToken::new(), 1);
    let refused = filled.await.err().ok_or("a temp-key stage was filled")?;
    assert_eq!(class(&Err::<(), _>(refused.error)), conflict);
    assert_eq!(class(&destination.discard(stage).await), conflict);
    assert_eq!(*protocol.aborts.lock().await, 0);
    assert_eq!(*protocol.puts.lock().await, 0);
    assert!(protocol.objects.lock().await.is_empty());
    Ok(())
}
