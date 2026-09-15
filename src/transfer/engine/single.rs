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
    let mut hasher = request.needs_source_digest().then(blake3::Hasher::new);
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
        if let Some(hasher) = &mut hasher {
            hasher.update(&bytes);
        }
        parts.push(bytes);
    }
    if total != size || request.cancel.is_cancelled() {
        return Err(TransferFailure::orchestration(
            TransferPhase::Transfer,
            "source was incomplete or cancelled",
        ));
    }
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
        source_blake3: hasher.map_or([0; 32], |hasher| *hasher.finalize().as_bytes()),
    })
}
