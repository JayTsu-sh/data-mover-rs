//! Read-back verification and publication of a transferred stage.

use std::sync::Arc;

use super::{TransferFailure, TransferPhase, TransferSide, Transferred};
use crate::model::FailureClass;
use crate::storage::{
    PrepareFact, PreparedStage, PublicationEvidence, PublishRequest, SourceQosStats,
    StagedDestination, StorageRoleFailure, VerifyRequest,
};

/// Read-back verification of content already published at its final name (a destination whose
/// [`VerificationPoint`](crate::storage::VerificationPoint) is `AfterPublish`). Nothing can be rolled back any more: a failure —
/// including cancellation — is reported with `final_destination_changed` and keeps no stage.
pub(super) async fn verify_published(
    destination: &Arc<dyn StagedDestination>,
    stage: &PreparedStage,
    published: &PublicationEvidence,
    expected_size: u64,
    source_digest: Option<[u8; 32]>,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<(), TransferFailure> {
    let changed = |mut failure: TransferFailure| {
        failure.final_destination_changed = true;
        failure
    };
    let Some(expected_blake3) = source_digest else {
        return Err(changed(TransferFailure::orchestration(
            TransferPhase::Verify,
            "source digest is unavailable for verification",
        )));
    };
    let verification = destination
        .verify(
            stage,
            VerifyRequest {
                expected_size,
                expected_blake3,
                cancel,
                published: Some(published.clone()),
            },
        )
        .await
        .map_err(|error| {
            changed(TransferFailure::role(
                TransferPhase::Verify,
                TransferSide::Destination,
                error,
            ))
        })?;
    if verification.verified_bytes != expected_size || verification.blake3 != expected_blake3 {
        return Err(changed(TransferFailure::orchestration(
            TransferPhase::Verify,
            "published content differs from source evidence",
        )));
    }
    Ok(())
}

/// Publishes verified staged content.
pub(super) async fn publish_transferred(
    transferred: Transferred,
    expected_size: u64,
    source_digest: Option<[u8; 32]>,
    cancel: tokio_util::sync::CancellationToken,
    source_qos: SourceQosStats,
) -> Result<(PublicationEvidence, Transferred), TransferFailure> {
    let publication = transferred
        .destination
        .publish(
            &transferred.stage,
            PublishRequest {
                expected_size,
                expected_blake3: source_digest,
                cancel: cancel.clone(),
            },
        )
        .await;
    let evidence = match publication {
        Ok(evidence) => evidence,
        Err(publication) => {
            let mut failure = TransferFailure::role(
                TransferPhase::Publish,
                TransferSide::Destination,
                publication.error,
            );
            failure.final_destination_changed = publication.final_destination_changed;
            if publication.final_destination_changed {
                return Err(failure
                    .with_committed_cleanup(Arc::clone(&transferred.destination), transferred.stage)
                    .with_source_qos(source_qos));
            }
            return Err(failure
                .with_stage(Arc::clone(&transferred.destination), transferred.stage)
                .with_source_qos(source_qos));
        }
    };
    Ok((evidence, transferred))
}

pub(super) async fn verify_transferred(
    transferred: Transferred,
    cancel: tokio_util::sync::CancellationToken,
    source_qos: SourceQosStats,
) -> Result<(Transferred, crate::storage::VerificationEvidence), TransferFailure> {
    if cancel.is_cancelled() {
        return Err(TransferFailure::orchestration(
            TransferPhase::Verify,
            "transfer was cancelled before verification",
        )
        .with_stage(Arc::clone(&transferred.destination), transferred.stage)
        .with_source_qos(source_qos));
    }
    let Some(source_blake3) = transferred.source_blake3 else {
        return Err(TransferFailure::orchestration(
            TransferPhase::Verify,
            "source digest is unavailable for verification",
        )
        .with_stage(Arc::clone(&transferred.destination), transferred.stage)
        .with_source_qos(source_qos));
    };
    let verification = transferred
        .destination
        .verify(
            &transferred.stage,
            VerifyRequest {
                expected_size: transferred.checkpoint.durable_prefix,
                expected_blake3: source_blake3,
                cancel: cancel.clone(),
                published: None,
            },
        )
        .await;
    let verification = match verification {
        Ok(verification) => verification,
        Err(error) if content_mismatch(&error) => {
            let failure =
                TransferFailure::role(TransferPhase::Verify, TransferSide::Destination, error)
                    .with_source_qos(source_qos);
            return Err(discard_stale_resume(transferred, failure).await);
        }
        Err(error) => {
            return Err(TransferFailure::role(
                TransferPhase::Verify,
                TransferSide::Destination,
                error,
            )
            .with_stage(Arc::clone(&transferred.destination), transferred.stage)
            .with_source_qos(source_qos));
        }
    };
    if verification.verified_bytes != transferred.checkpoint.durable_prefix
        || verification.blake3 != source_blake3
    {
        let failure = TransferFailure::orchestration(
            TransferPhase::Verify,
            "destination verification evidence differs from source evidence",
        )
        .with_source_qos(source_qos);
        return Err(discard_stale_resume(transferred, failure).await);
    }
    if cancel.is_cancelled() {
        return Err(TransferFailure::orchestration(
            TransferPhase::Verify,
            "transfer was cancelled before publication",
        )
        .with_stage(Arc::clone(&transferred.destination), transferred.stage)
        .with_source_qos(source_qos));
    }
    Ok((transferred, verification))
}

/// Whether a failed verification judged the staged content — the backend read it and it differs —
/// rather than failing to look. Backends report a mismatch as `Corruption`, not as evidence.
pub(super) fn content_mismatch(error: &StorageRoleFailure) -> bool {
    matches!(error, StorageRoleFailure::Entry(entry) if entry.class() == FailureClass::Corruption)
}

/// A resumed stage kept at the destination that fails verification is stale — its pointer would
/// resume it again on every retry, surviving restarts. It is cleaned up in place, and the failure
/// keeps no stage; any other failing stage stays with the failure as before.
async fn discard_stale_resume(
    transferred: Transferred,
    failure: TransferFailure,
) -> TransferFailure {
    discard_stale_stage(transferred.destination, transferred.stage, failure).await
}

/// [`discard_stale_resume`] for a bare stage, shared with the expert destination half.
pub(super) async fn discard_stale_stage(
    destination: Arc<dyn StagedDestination>,
    stage: PreparedStage,
    failure: TransferFailure,
) -> TransferFailure {
    let resumed_here =
        stage.at_destination && matches!(stage.prepare_fact, PrepareFact::Resumed { .. });
    if !resumed_here {
        return failure.with_stage(destination, stage);
    }
    // `discard` removes the pointer before the stage (the backend contract), so even a clean-up
    // that fails halfway leaves no pointer to resume from: the next prepare restarts. The failure
    // to report is the verification either way.
    let _cleanup = destination.discard(stage).await;
    failure
}
