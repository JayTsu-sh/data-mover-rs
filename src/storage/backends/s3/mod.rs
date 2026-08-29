//! Standard S3 roles for the `ArchitectureReady` storage seam.

mod metadata;
mod protocol;
mod source;
mod staged;

use std::sync::Arc;

use crate::model::{BackendIdentity, BackendKind};
use crate::storage::{BackendCapabilities, CapabilityAvailability, Storage};

pub(crate) use protocol::{
    S3ClaimOutcome, S3ObjectFacts, S3PartFacts, S3Protocol, S3ProtocolFailure, S3Result,
};

pub(crate) fn connect<P>(
    protocol: Arc<P>,
    identity: BackendIdentity,
) -> Result<Storage, Box<dyn std::error::Error>>
where
    P: S3Protocol + 'static,
{
    if identity.kind() != BackendKind::S3 {
        return Err("S3 roles require an S3 backend identity".into());
    }
    let source = Arc::new(source::S3ReadSource::new(
        protocol.clone(),
        identity.clone(),
    ));
    let staged = Arc::new(staged::S3StagedDestination::new(
        protocol.clone(),
        identity.clone(),
    ));
    let metadata = Arc::new(metadata::S3Metadata::new(protocol));
    Storage::connected(
        identity,
        BackendCapabilities::new(
            CapabilityAvailability::Supported,
            CapabilityAvailability::Supported,
            CapabilityAvailability::Unsupported(crate::storage::UnsupportedReason::new(
                "namespace role is delivered by traversal",
            )?),
            CapabilityAvailability::Supported,
        ),
        Some(source),
        Some(staged),
        None,
        Some(metadata),
    )
    .map_err(Into::into)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Range;
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use tokio::sync::Mutex;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::model::{
        BackendKind, MetadataObservation, ObjectTag, ObservationMode, ObservationPlan, StoragePath,
    };
    use crate::storage::{
        FinalDestination, MetadataMutation, PrepareRequest, PublishRequest, ReadRequest,
        VerifyRequest,
    };

    type UploadParts = HashMap<String, (String, Vec<(i32, Bytes)>)>;

    #[derive(Default)]
    struct MemoryS3 {
        objects: Mutex<HashMap<String, Bytes>>,
        uploads: Mutex<UploadParts>,
        tags: Mutex<HashMap<String, Vec<ObjectTag>>>,
        tag_reads: Mutex<u32>,
        aborts: Mutex<u32>,
        head_failure: Mutex<Option<(String, S3ProtocolFailure)>>,
        claims: Mutex<HashMap<String, [u8; 32]>>,
        copy_commits_then_fails: Mutex<bool>,
    }

    #[async_trait]
    impl S3Protocol for MemoryS3 {
        async fn head(&self, key: &str) -> S3Result<S3ObjectFacts> {
            if let Some((failed_key, failure)) = &*self.head_failure.lock().await
                && failed_key == key
            {
                return Err(failure.clone());
            }
            let objects = self.objects.lock().await;
            let bytes = objects.get(key).ok_or_else(|| {
                S3ProtocolFailure::entry(
                    crate::model::FailureClass::NotFound,
                    crate::model::Transience::Permanent,
                    "not found",
                )
            })?;
            Ok(S3ObjectFacts {
                size: bytes.len() as u64,
                etag: blake3::hash(bytes).to_hex().to_string(),
                version_id: None,
            })
        }
        async fn get_range(&self, key: &str, range: Range<u64>) -> S3Result<Bytes> {
            let objects = self.objects.lock().await;
            let bytes = objects.get(key).ok_or_else(|| {
                S3ProtocolFailure::entry(
                    crate::model::FailureClass::NotFound,
                    crate::model::Transience::Permanent,
                    "not found",
                )
            })?;
            let start = usize::try_from(range.start).map_err(|_| {
                S3ProtocolFailure::entry(
                    crate::model::FailureClass::InvalidInput,
                    crate::model::Transience::Permanent,
                    "range too large",
                )
            })?;
            let end = usize::try_from(range.end).map_err(|_| {
                S3ProtocolFailure::entry(
                    crate::model::FailureClass::InvalidInput,
                    crate::model::Transience::Permanent,
                    "range too large",
                )
            })?;
            Ok(bytes.slice(start..end))
        }
        async fn begin_multipart(&self, key: &str) -> S3Result<String> {
            let id = format!("upload-{key}");
            self.uploads
                .lock()
                .await
                .insert(id.clone(), (key.to_string(), Vec::new()));
            Ok(id)
        }
        async fn upload_part(
            &self,
            _key: &str,
            id: &str,
            number: i32,
            bytes: Bytes,
        ) -> S3Result<String> {
            self.uploads
                .lock()
                .await
                .get_mut(id)
                .ok_or_else(|| S3ProtocolFailure::protocol("missing upload"))?
                .1
                .push((number, bytes));
            Ok(format!("etag-{number}"))
        }
        async fn complete_multipart(
            &self,
            _key: &str,
            id: &str,
            _parts: &[(i32, String)],
        ) -> S3Result<()> {
            let (key, mut parts) = self
                .uploads
                .lock()
                .await
                .remove(id)
                .ok_or_else(|| S3ProtocolFailure::protocol("missing upload"))?;
            parts.sort_by_key(|part| part.0);
            let bytes: Vec<u8> = parts.into_iter().flat_map(|part| part.1).collect();
            self.objects.lock().await.insert(key, Bytes::from(bytes));
            Ok(())
        }
        async fn abort_multipart(&self, _key: &str, id: &str) -> S3Result<()> {
            self.uploads.lock().await.remove(id);
            *self.aborts.lock().await += 1;
            Ok(())
        }
        async fn list_parts(&self, _key: &str, id: &str) -> S3Result<Vec<S3PartFacts>> {
            Ok(self
                .uploads
                .lock()
                .await
                .get(id)
                .ok_or_else(|| S3ProtocolFailure::protocol("missing upload"))?
                .1
                .iter()
                .map(|(number, bytes)| S3PartFacts {
                    number: *number,
                    size: bytes.len() as u64,
                    etag: format!("etag-{number}"),
                })
                .collect())
        }
        async fn copy_object(&self, from: &str, to: &str) -> S3Result<()> {
            let bytes = self
                .objects
                .lock()
                .await
                .get(from)
                .cloned()
                .ok_or_else(|| S3ProtocolFailure::protocol("not found"))?;
            self.objects.lock().await.insert(to.to_string(), bytes);
            if *self.copy_commits_then_fails.lock().await {
                Err(S3ProtocolFailure::session(
                    crate::model::FailureClass::Connectivity,
                    crate::model::Transience::Transient,
                    "copy response lost",
                ))
            } else {
                Ok(())
            }
        }
        async fn delete_object(&self, key: &str) -> S3Result<()> {
            self.objects.lock().await.remove(key);
            Ok(())
        }
        async fn get_tags(&self, key: &str) -> S3Result<Vec<ObjectTag>> {
            *self.tag_reads.lock().await += 1;
            Ok(self.tags.lock().await.get(key).cloned().unwrap_or_default())
        }
        async fn put_tags(&self, key: &str, tags: &[ObjectTag]) -> S3Result<()> {
            self.tags
                .lock()
                .await
                .insert(key.to_string(), tags.to_vec());
            Ok(())
        }
        async fn claim(&self, key: &str, token: [u8; 32]) -> S3Result<S3ClaimOutcome> {
            let mut claims = self.claims.lock().await;
            match claims.get(key) {
                None => {
                    claims.insert(key.to_string(), token);
                    Ok(S3ClaimOutcome::Acquired)
                }
                Some(existing) if existing == &token => Ok(S3ClaimOutcome::AlreadyOwned),
                Some(_) => Ok(S3ClaimOutcome::Conflict),
            }
        }
        async fn release_claim(&self, key: &str) -> S3Result<()> {
            self.claims.lock().await.remove(key);
            Ok(())
        }
    }

    fn identity() -> BackendIdentity {
        BackendIdentity::new(BackendKind::S3, "memory-bucket").expect("valid identity")
    }

    fn validation_policy() -> crate::storage::PreflightPolicy {
        crate::storage::PreflightPolicy::production()
    }

    #[test]
    fn certified_standard_s3_roles_are_available_in_production()
    -> Result<(), Box<dyn std::error::Error>> {
        let storage = connect(Arc::new(MemoryS3::default()), identity())?;
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
    async fn range_stream_multipart_verify_publish_and_readback()
    -> Result<(), Box<dyn std::error::Error>> {
        let protocol = Arc::new(MemoryS3::default());
        protocol
            .objects
            .lock()
            .await
            .insert("source".into(), Bytes::from_static(b"0123456789"));
        let storage = connect(protocol.clone(), identity())?;
        let policy = validation_policy();
        let source = storage.read_source(&policy)?;
        let descriptor = source.describe(&StoragePath::new("source")?).await?;
        let mut read = source
            .read(ReadRequest {
                path: StoragePath::new("source")?,
                range: Some(2..8),
                expected_source: Some(descriptor.clone().source_identity),
                cancel: CancellationToken::new(),
                source_qos: None,
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
                    policy: crate::storage::ExistingDestinationPolicy::Overwrite,
                    expected_size: payload.len() as u64,
                    expected_blake3: digest,
                    cancel: CancellationToken::new(),
                },
            )
            .await
            .map_err(|failure| failure.error)?;
        assert_eq!(protocol.objects.lock().await.get("final"), Some(&payload));
        Ok(())
    }

    #[tokio::test]
    async fn tags_are_lazy_and_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let protocol = Arc::new(MemoryS3::default());
        let storage = connect(protocol.clone(), identity())?;
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
    async fn failed_input_preserves_checkpoint_until_explicit_discard()
    -> Result<(), Box<dyn std::error::Error>> {
        let protocol = Arc::new(MemoryS3::default());
        let storage = connect(protocol.clone(), identity())?;
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
    async fn cancellation_remains_a_typed_entry_outcome() -> Result<(), Box<dyn std::error::Error>>
    {
        let protocol = Arc::new(MemoryS3::default());
        protocol
            .objects
            .lock()
            .await
            .insert("source".into(), Bytes::from_static(b"payload"));
        let storage = connect(protocol, identity())?;
        let source = storage.read_source(&validation_policy())?;
        let cancel = CancellationToken::new();
        cancel.cancel();
        let result = source
            .read(ReadRequest {
                path: StoragePath::new("source")?,
                range: None,
                expected_source: None,
                cancel,
                source_qos: None,
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
    async fn destination_head_session_failure_never_means_absent()
    -> Result<(), Box<dyn std::error::Error>> {
        let protocol = Arc::new(MemoryS3::default());
        let storage = connect(protocol.clone(), identity())?;
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
                    policy: crate::storage::ExistingDestinationPolicy::Overwrite,
                    expected_size: payload.len() as u64,
                    expected_blake3: *blake3::hash(&payload).as_bytes(),
                    cancel: CancellationToken::new(),
                },
            )
            .await;
        let Err(failure) = result else {
            panic!("session failure allowed publication")
        };
        assert!(matches!(
            failure.error,
            crate::storage::StorageRoleFailure::Session(_)
        ));
        assert!(!failure.final_destination_changed);
        assert!(!protocol.objects.lock().await.contains_key("final"));
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn multipart_checkpoint_is_reobserved_and_resumed_after_reconnect()
    -> Result<(), Box<dyn std::error::Error>> {
        let protocol = Arc::new(MemoryS3::default());
        let first_storage = connect(protocol.clone(), identity())?;
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
        };
        let prepare = PrepareRequest {
            final_destination: FinalDestination::new(final_path.clone()),
            source: source.clone(),
            recovery_binding: [6; 32],
        };
        let stage = destination.prepare(prepare.clone()).await?;
        let recovery = destination.recovery_identity(&stage).await?;
        let part = Bytes::from(vec![5u8; 8 * 1024 * 1024]);
        let injected =
            super::source::entry(&source.path, crate::model::Operation::Read, "interrupted");
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
        assert_eq!(checkpoint, part.len() as u64);

        let second_storage = connect(protocol.clone(), identity())?;
        let resumed_destination = second_storage.staged_destination(&policy)?;
        let persisted_recovery = recovery.as_bytes().clone();
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
        let competing = connect(protocol.clone(), identity())?.staged_destination(&policy)?;
        let conflict = competing
            .recover(crate::storage::RecoverRequest {
                identity: crate::storage::RecoveryIdentity::from_bytes(persisted_recovery)?,
                final_destination: prepare.final_destination,
                source: source.clone(),
                recovery_binding: prepare.recovery_binding,
                claim_token: [9; 32],
            })
            .await;
        assert!(matches!(
            conflict,
            Err(crate::storage::StorageRoleFailure::Entry(_))
        ));
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
                    policy: crate::storage::ExistingDestinationPolicy::Overwrite,
                    expected_size: full.len() as u64,
                    expected_blake3: digest,
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
        let storage = connect(protocol.clone(), identity())?;
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
                    policy: crate::storage::ExistingDestinationPolicy::Overwrite,
                    expected_size: payload.len() as u64,
                    expected_blake3: digest,
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
}
