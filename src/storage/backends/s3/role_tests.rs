//! S3 role behaviour over the in-memory bucket: ranges, multipart staging, tags, checkpoints,
//! cancellation, publication.

use super::*;
use crate::model::SourceVersion;

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
    let storage = connect(protocol.clone(), identity(), Some(native_context()))?;
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
    let stage = destination
        .prepare(PrepareRequest {
            final_destination: FinalDestination::new(StoragePath::new("final")?),
            source: descriptor,
            recovery_binding: [7; 32],
        })
        .await?;
    let input = Box::pin(futures::stream::iter([Ok(payload.clone())]));
    assert_eq!(
        destination.write(&stage, input).await?.persisted_bytes,
        payload.len() as u64
    );
    let digest = *blake3::hash(&payload).as_bytes();
    destination
        .verify(
            &stage,
            VerifyRequest {
                expected_size: payload.len() as u64,
                expected_blake3: digest,
                cancel: CancellationToken::new(),
            },
        )
        .await?;
    destination
        .publish(
            &stage,
            PublishRequest {
                expected_size: payload.len() as u64,
                expected_blake3: Some(digest),
                cancel: CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(protocol.objects.lock().await.get("final"), Some(&payload));
    assert!(destination.observe_checkpoint(&stage).await.is_err());
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
    let path = StoragePath::new("final")?;
    let source_identity = crate::model::SourceIdentity::new(
        identity(),
        crate::model::IdentityStrength::PathScoped,
        b"source",
    )?;
    let stage = destination
        .prepare(PrepareRequest {
            final_destination: FinalDestination::new(path.clone()),
            source: crate::storage::SourceDescriptor {
                path,
                kind: crate::model::EntryKind::File,
                size: None,
                source_identity,
                backend_fact: None,
                content_version: None,
                inline_timestamps: None,
                inline_mode: None,
                version: SourceVersion::Current,
            },
            recovery_binding: [9; 32],
        })
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
    let storage = connect(protocol.clone(), identity(), Some(native_context()))?;
    let destination = storage.staged_destination(&validation_policy())?;
    let path = StoragePath::new("final")?;
    let source_identity = crate::model::SourceIdentity::new(
        identity(),
        crate::model::IdentityStrength::PathScoped,
        b"source",
    )?;
    let stage = destination
        .prepare(PrepareRequest {
            final_destination: FinalDestination::new(path),
            source: crate::storage::SourceDescriptor {
                path: StoragePath::new("source")?,
                kind: crate::model::EntryKind::File,
                size: Some(7),
                source_identity,
                backend_fact: None,
                content_version: None,
                inline_timestamps: None,
                inline_mode: None,
                version: SourceVersion::Current,
            },
            recovery_binding: [4; 32],
        })
        .await?;
    let payload = Bytes::from_static(b"payload");
    destination
        .write(
            &stage,
            Box::pin(futures::stream::iter([Ok(payload.clone())])),
        )
        .await?;
    *protocol.head_failure.lock().await = Some((
        "final".into(),
        S3ProtocolFailure::session(
            crate::model::FailureClass::Connectivity,
            crate::model::Transience::Transient,
            "endpoint unavailable",
        ),
    ));
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

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn multipart_checkpoint_is_reobserved_and_resumed_after_reconnect()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryS3::default());
    let first_storage = connect(protocol.clone(), identity(), Some(native_context()))?;
    let policy = validation_policy();
    let destination = first_storage.staged_destination(&policy)?;
    let final_path = StoragePath::new("resumed-final")?;
    let source = crate::storage::SourceDescriptor {
        path: StoragePath::new("source")?,
        kind: crate::model::EntryKind::File,
        size: None,
        source_identity: crate::model::SourceIdentity::new(
            identity(),
            crate::model::IdentityStrength::PathScoped,
            b"stable-source",
        )?,
        backend_fact: None,
        content_version: None,
        inline_timestamps: None,
        inline_mode: None,
        version: SourceVersion::Current,
    };
    let prepare = PrepareRequest {
        final_destination: FinalDestination::new(final_path.clone()),
        source: source.clone(),
        recovery_binding: [6; 32],
    };
    let stage = destination.prepare(prepare.clone()).await?;
    let recovery = destination.recovery_identity(&stage).await?;
    let part = Bytes::from(vec![5u8; 8 * 1024 * 1024]);
    let injected = super::source::entry(&source.path, crate::model::Operation::Read, "interrupted");
    let interrupted = futures::stream::iter([
        Ok(part.clone()),
        Ok(part.clone()),
        Ok(part.clone()),
        Ok(part.clone()),
        Err(injected),
    ]);
    assert!(
        destination
            .write(&stage, Box::pin(interrupted))
            .await
            .is_err()
    );
    let checkpoint = destination.observe_checkpoint(&stage).await?.durable_prefix;
    assert_eq!(checkpoint, (part.len() * 4) as u64);

    let second_storage = connect(protocol.clone(), identity(), Some(native_context()))?;
    let resumed_destination = second_storage.staged_destination(&policy)?;
    let resumed = resumed_destination
        .recover(crate::storage::RecoverRequest {
            identity: recovery,
            final_destination: prepare.final_destination.clone(),
            source: source.clone(),
            recovery_binding: prepare.recovery_binding,
            claim_token: [8; 32],
        })
        .await?;
    assert_eq!(
        resumed_destination
            .observe_checkpoint(&resumed)
            .await?
            .durable_prefix,
        checkpoint
    );
    let full = vec![5u8; 4 * 8 * 1024 * 1024 + 17];
    let checkpoint = usize::try_from(checkpoint)?;
    resumed_destination
        .write(
            &resumed,
            Box::pin(futures::stream::iter([Ok(Bytes::copy_from_slice(
                &full[checkpoint..],
            ))])),
        )
        .await?;
    let digest = *blake3::hash(&full).as_bytes();
    resumed_destination
        .verify(
            &resumed,
            VerifyRequest {
                expected_size: full.len() as u64,
                expected_blake3: digest,
                cancel: CancellationToken::new(),
            },
        )
        .await?;
    resumed_destination
        .publish(
            &resumed,
            PublishRequest {
                expected_size: full.len() as u64,
                expected_blake3: Some(digest),
                cancel: CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(
        protocol
            .objects
            .lock()
            .await
            .get(final_path.as_str())
            .map(Bytes::as_ref),
        Some(full.as_slice())
    );
    Ok(())
}

#[tokio::test]
async fn publication_reconciles_a_committed_copy_with_a_lost_response()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryS3::default());
    let storage = connect(protocol.clone(), identity(), Some(native_context()))?;
    let policy = validation_policy();
    let destination = storage.staged_destination(&policy)?;
    let payload = Bytes::from_static(b"ambiguous publication payload");
    let path = StoragePath::new("ambiguous-final")?;
    let stage = destination
        .prepare(PrepareRequest {
            final_destination: FinalDestination::new(path.clone()),
            source: crate::storage::SourceDescriptor {
                path: StoragePath::new("source")?,
                kind: crate::model::EntryKind::File,
                size: Some(payload.len() as u64),
                source_identity: crate::model::SourceIdentity::new(
                    identity(),
                    crate::model::IdentityStrength::PathScoped,
                    b"source",
                )?,
                backend_fact: None,
                content_version: None,
                inline_timestamps: None,
                inline_mode: None,
                version: SourceVersion::Current,
            },
            recovery_binding: [2; 32],
        })
        .await?;
    destination
        .write(
            &stage,
            Box::pin(futures::stream::iter([Ok(payload.clone())])),
        )
        .await?;
    *protocol.copy_commits_then_fails.lock().await = true;
    let digest = *blake3::hash(&payload).as_bytes();
    let published = destination
        .publish(
            &stage,
            PublishRequest {
                expected_size: payload.len() as u64,
                expected_blake3: Some(digest),
                cancel: CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(published.final_destination, path);
    assert_eq!(
        protocol.objects.lock().await.get("ambiguous-final"),
        Some(&payload)
    );
    Ok(())
}
