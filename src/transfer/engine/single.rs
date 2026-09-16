use super::*;

/// A complete source chunk needs no producer task or inflight channel. Fragments remain shared.
pub(super) async fn transfer(
    request: &TransferRequest,
    source: Arc<dyn ReadSource>,
    destination: &Arc<dyn StagedDestination>,
    descriptor: &SourceDescriptor,
    stage: &PreparedStage,
    source_qos: Option<SourceQosBudget>,
) -> Result<TransferEvidence, TransferFailure> {
    let size = descriptor.size.ok_or_else(|| {
        TransferFailure::orchestration(TransferPhase::Transfer, "source size is unavailable")
    })?;
    let source_error =
        |error| TransferFailure::role(TransferPhase::Transfer, TransferSide::Source, error);
    let mut input = source
        .read(ReadRequest {
            path: descriptor.path.clone(),
            range: Some(0..size),
            expected_source: Some(descriptor.source_identity.clone()),
            maximum_chunk_bytes: request.inflight.negotiated_chunk_ceiling(),
            read_inflight: 1,
            read_budget: None,
            cancel: request.cancel.clone(),
            source_qos,
        })
        .await
        .map_err(source_error)?;
    let mut parts = Vec::new();
    let mut total = 0_u64;
    // The bytes are already resident here; retain a commit digest so a remote
    // publication error can be reconciled without another source read.
    let mut hasher = blake3::Hasher::new();
    while let Some(item) = input.next().await {
        let bytes = item.map_err(source_error)?;
        total = total
            .checked_add(bytes.len() as u64)
            .filter(|total| *total <= size)
            .ok_or_else(|| {
                TransferFailure::orchestration(
                    TransferPhase::Transfer,
                    "source emitted more bytes than described",
                )
            })?;
        if bytes.is_empty() {
            return Err(TransferFailure::orchestration(
                TransferPhase::Transfer,
                "source emitted an empty chunk",
            ));
        }
        hasher.update(&bytes);
        parts.push(bytes);
    }
    validate_source_completion(request, descriptor, total, size)?;
    let write = if parts.len() == 1 {
        destination
            .write_single(
                stage,
                parts.pop().ok_or_else(|| {
                    TransferFailure::orchestration(
                        TransferPhase::Transfer,
                        "source chunk is unavailable",
                    )
                })?,
            )
            .await
    } else {
        destination
            .write(
                stage,
                Box::pin(futures::stream::iter(parts.into_iter().map(Ok))),
            )
            .await
    }
    .map_err(|error| {
        TransferFailure::role(TransferPhase::Transfer, TransferSide::Destination, error)
    })?;
    if write.persisted_bytes != size {
        return Err(TransferFailure::orchestration(
            TransferPhase::Transfer,
            "destination byte count differs from source",
        ));
    }
    let checkpoint = if stage.recovery_enabled() {
        destination
            .observe_checkpoint(stage)
            .await
            .map_err(|error| {
                TransferFailure::role(TransferPhase::Checkpoint, TransferSide::Destination, error)
            })?
    } else {
        CheckpointObservation {
            durable_prefix: write.persisted_bytes,
        }
    };
    Ok(TransferEvidence {
        write,
        checkpoint,
        source_blake3: Some(*hasher.finalize().as_bytes()),
    })
}

fn validate_source_completion(
    request: &TransferRequest,
    descriptor: &SourceDescriptor,
    total: u64,
    size: u64,
) -> Result<(), TransferFailure> {
    if request.cancel.is_cancelled() {
        return Err(TransferFailure::role(
            TransferPhase::Transfer,
            TransferSide::Source,
            source_failure(&descriptor.path, FailureClass::Cancelled),
        ));
    }
    if total != size {
        return Err(TransferFailure::orchestration(
            TransferPhase::Transfer,
            "source was incomplete",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::model::{IdentityStrength, SourceIdentity};
    use crate::storage::{ByteStream, FinalDestination, PrepareRequest};
    use crate::transfer::{InflightLimits, TransferIdentity};

    struct CancelsAtEof {
        descriptor: SourceDescriptor,
        payload: Bytes,
    }

    #[async_trait]
    impl ReadSource for CancelsAtEof {
        async fn describe(
            &self,
            _path: &StoragePath,
        ) -> Result<SourceDescriptor, StorageRoleFailure> {
            Ok(self.descriptor.clone())
        }

        async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
            let cancel = request.cancel;
            let payload = self.payload.clone();
            Ok(Box::pin(futures::stream::unfold(
                Some(payload),
                move |state| {
                    let cancel = cancel.clone();
                    async move {
                        if let Some(bytes) = state {
                            Some((Ok(bytes), None))
                        } else {
                            cancel.cancel();
                            None
                        }
                    }
                },
            )))
        }
    }

    #[tokio::test]
    async fn cancellation_after_eof_remains_a_structured_source_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let source_root = tempfile::tempdir()?;
        let destination_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("source"), b"payload")?;
        let (source_storage, _) = crate::storage::backends::local::test_source_storage(
            source_root.path(),
            "single-cancel-source",
        )?;
        let (destination_storage, destination_role) =
            crate::storage::backends::local::test_destination_storage_with_role(
                destination_root.path(),
                "single-cancel-destination",
            )?;
        let source_identity = SourceIdentity::new(
            crate::storage::backends::local::test_identity("single-cancel-source"),
            IdentityStrength::StableWithinBackend,
            b"single-cancel-source",
        )?;
        let descriptor = SourceDescriptor::new(
            StoragePath::new("source")?,
            crate::model::EntryKind::File,
            Some(7),
            source_identity,
        );
        let final_destination = FinalDestination::new(StoragePath::new("final")?);
        let stage = destination_role
            .prepare(PrepareRequest {
                final_destination: final_destination.clone(),
                source: descriptor.clone(),
                recovery_binding: [41; 32],
            })
            .await?;
        let cancel = tokio_util::sync::CancellationToken::new();
        let request = TransferRequest::new(
            TransferIdentity::new("single-cancel")?,
            source_storage,
            descriptor.path.clone(),
            destination_storage,
            final_destination.path().clone(),
            InflightLimits::new(1, 64 * 1024, 1)?,
            cancel,
        );
        let source: Arc<dyn ReadSource> = Arc::new(CancelsAtEof {
            descriptor: descriptor.clone(),
            payload: Bytes::from_static(b"payload"),
        });
        let destination: Arc<dyn StagedDestination> = destination_role;

        let failure = transfer(&request, source, &destination, &descriptor, &stage, None)
            .await
            .err()
            .ok_or("cancelled single-chunk transfer succeeded")?;

        assert_eq!(failure.side, TransferSide::Source);
        assert!(matches!(
            failure.role.as_deref(),
            Some(StorageRoleFailure::Entry(error))
                if error.class() == FailureClass::Cancelled
        ));
        destination.discard(stage).await?;
        Ok(())
    }
}
