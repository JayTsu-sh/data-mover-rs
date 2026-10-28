//! Read-back verification and publication of a transferred stage.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::negotiation::apply_copied_metadata;
use super::{ReadBackVerification, TransferFailure, TransferPhase, TransferSide, Transferred};
use crate::metadata::MetadataApplicationReport;
use crate::model::{EntryOperationFailure, FailureClass, Operation, Transience};
use crate::storage::checkpoint::CheckpointPhase;
use crate::storage::{
    PrepareFact, PreparedStage, PublicationEvidence, PublicationFailure, PublishRequest,
    SourceQosStats, StagedDestination, StorageRoleFailure, VerificationPoint, VerifyRequest,
};

/// Recovered finals are checked before acceptance. Staged metadata finishes before publication;
/// deferred publication metadata is checked without mutating the recovered final. Fresh stages
/// retain the destination's verification point; armed checkpoints add publication metadata I/O.
pub(super) async fn complete_transfer(
    transferred: Transferred,
    read_back: ReadBackVerification,
    cancel: CancellationToken,
    source_qos: SourceQosStats,
) -> Result<CompletedTransfer, TransferFailure> {
    let recovered = transferred.stage.recovered_publication.is_some();
    let verify_after = !recovered
        && read_back == ReadBackVerification::Enabled
        && transferred
            .destination
            .verification_point(&transferred.stage)
            == VerificationPoint::AfterPublish;
    let transferred =
        verify_before_metadata(transferred, read_back, cancel.clone(), source_qos).await?;
    let (transferred, metadata) = apply_metadata(transferred, cancel.clone(), source_qos).await?;
    if verify_after && (cancel.is_cancelled() || transferred.source_blake3.is_none()) {
        return Err(retain_unpublished_failure(
            transferred,
            TransferFailure::orchestration(
                TransferPhase::Verify,
                "transfer was cancelled, or has no source digest, before publication",
            ),
            source_qos,
        ));
    }
    let expected_size = transferred.checkpoint.durable_prefix;
    let source_digest = transferred.source_blake3;
    let (evidence, transferred) = publish_transferred(
        transferred,
        expected_size,
        source_digest,
        cancel.clone(),
        source_qos,
    )
    .await?;
    let transferred = if verify_after {
        verify_published_transferred(transferred, &evidence, cancel, source_qos).await?
    } else {
        transferred
    };
    let transferred = release_transferred(transferred, source_qos).await?;
    Ok((evidence, transferred, metadata))
}

type CompletedTransfer = (
    PublicationEvidence,
    Transferred,
    Option<MetadataApplicationReport>,
);

pub(super) fn bind_prepared_checkpoint_metadata(
    stage: PreparedStage,
    destination: &Arc<dyn StagedDestination>,
    binding: [u8; 32],
) -> Result<PreparedStage, TransferFailure> {
    if stage.checkpoint_bind_metadata(binding).is_err() {
        let failure = TransferFailure::role(
            TransferPhase::Metadata,
            TransferSide::Destination,
            StorageRoleFailure::Entry(
                EntryOperationFailure::new(
                    stage.final_destination.path().clone(),
                    Operation::Publish,
                    FailureClass::Unsupported,
                    Transience::Permanent,
                    "publication checkpoint metadata plan differs from this request",
                )
                .unwrap_or_else(|_| unreachable!("static metadata binding diagnostic is valid")),
            ),
        );
        return Err(if stage.recovered_publication.is_some() {
            let mut failure = failure;
            failure.final_destination_changed = true;
            failure.with_committed_cleanup(Arc::clone(destination), stage)
        } else {
            failure.with_stage(Arc::clone(destination), stage)
        });
    }
    Ok(stage)
}

async fn verify_before_metadata(
    transferred: Transferred,
    read_back: ReadBackVerification,
    cancel: CancellationToken,
    source_qos: SourceQosStats,
) -> Result<Transferred, TransferFailure> {
    if let Some(published) = transferred.stage.recovered_publication.clone() {
        return verify_published_transferred(transferred, &published, cancel, source_qos).await;
    }
    if read_back == ReadBackVerification::Enabled
        && transferred
            .destination
            .verification_point(&transferred.stage)
            == VerificationPoint::BeforePublish
    {
        return verify_transferred(transferred, cancel, source_qos)
            .await
            .map(|(stage, _)| stage);
    }
    Ok(transferred)
}

async fn apply_metadata(
    transferred: Transferred,
    cancel: CancellationToken,
    source_qos: SourceQosStats,
) -> Result<(Transferred, Option<MetadataApplicationReport>), TransferFailure> {
    if transferred.stage.recovered_publication.is_some() {
        // Rename-based metadata was applied before ReadyToPublish. A backend with deferred
        // publication metadata must still prove it committed; never replay writes on the final.
        if let Some(copied) = &transferred.copied_metadata_plan
            && let Err(error) = copied.validate_recovered(&transferred, cancel).await
        {
            return Err(retain_published_failure(
                transferred,
                TransferFailure::role(TransferPhase::Metadata, TransferSide::Destination, error),
                source_qos,
            ));
        }
        // The bound plan is reconstructed from the current source and the published final has
        // been validated. Its ACL omissions remain useful, but outcomes must say recovery.
        let report = transferred
            .copied_metadata_plan
            .as_ref()
            .and_then(super::negotiation::CopiedMetadataPlan::recovered_acl_report);
        return Ok((transferred, report));
    }
    match apply_copied_metadata(
        &transferred,
        transferred.copied_metadata_plan.as_ref(),
        cancel,
    )
    .await
    {
        Ok(report) => Ok((transferred, report)),
        Err(error) => Err(retain_unpublished_failure(
            transferred,
            TransferFailure::metadata(error),
            source_qos,
        )),
    }
}

/// A recovered stage already names the published final and cannot be retained as unpublished.
fn retain_unpublished_failure(
    transferred: Transferred,
    failure: TransferFailure,
    source_qos: SourceQosStats,
) -> TransferFailure {
    if transferred.stage.recovered_publication.is_some() {
        return retain_published_failure(transferred, failure, source_qos);
    }
    failure
        .with_stage(transferred.destination, transferred.stage)
        .with_source_qos(source_qos)
}

fn retain_published_failure(
    transferred: Transferred,
    mut failure: TransferFailure,
    source_qos: SourceQosStats,
) -> TransferFailure {
    failure.final_destination_changed = true;
    if transferred.stage.checkpoint_enabled() || transferred.stage.recovered_publication.is_some() {
        failure = failure.with_committed_cleanup(transferred.destination, transferred.stage);
    }
    failure.with_source_qos(source_qos)
}

async fn verify_published_transferred(
    transferred: Transferred,
    published: &PublicationEvidence,
    cancel: CancellationToken,
    source_qos: SourceQosStats,
) -> Result<Transferred, TransferFailure> {
    let result = verify_published(
        &transferred.destination,
        &transferred.stage,
        published,
        transferred.checkpoint.durable_prefix,
        transferred.source_blake3,
        cancel,
    )
    .await;
    match result {
        Ok(()) => Ok(transferred),
        Err(failure) => Err(retain_published_failure(transferred, failure, source_qos)),
    }
}

/// Read-back verification of content already published at its final name (a destination whose
/// [`VerificationPoint`](crate::storage::VerificationPoint) is `AfterPublish`, or a recovered
/// final). The owning caller retains armed checkpoint cleanup authority on every failure.
/// Backends own identity pinning. Without a persisted final identity, matching content does not
/// prove which publication created it: attribution relies on one writer per final key across
/// recovery and on recovery artifacts not being independently removed or replaced.
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
    let publication = if let Some(published) = transferred.stage.recovered_publication.clone() {
        Ok(published)
    } else {
        publish_ready_stage(&transferred, expected_size, source_digest, cancel).await
    };
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
    let transferred = record_published(transferred, source_qos).await?;
    Ok((evidence, transferred))
}

async fn publish_ready_stage(
    transferred: &Transferred,
    expected_size: u64,
    source_digest: Option<[u8; 32]>,
    cancel: CancellationToken,
) -> Result<PublicationEvidence, PublicationFailure> {
    if transferred.stage.checkpoint_enabled() {
        if source_digest.is_none() || transferred.stage.checkpoint_metadata_binding().is_none() {
            return Err(PublicationFailure {
                error: StorageRoleFailure::Entry(
                    EntryOperationFailure::new(
                        transferred.stage.final_destination.path().clone(),
                        Operation::Publish,
                        FailureClass::Corruption,
                        Transience::Permanent,
                        "source digest or metadata seal is unavailable for checkpoint publication",
                    )
                    .unwrap_or_else(|_| unreachable!("static publication diagnostic is valid")),
                ),
                final_destination_changed: false,
            });
        }
        persist_phase(transferred, CheckpointPhase::ReadyToPublish)
            .await
            .map_err(|error| PublicationFailure {
                error,
                final_destination_changed: false,
            })?;
    }
    transferred
        .destination
        .publish(
            &transferred.stage,
            PublishRequest {
                expected_size,
                expected_blake3: source_digest,
                cancel,
            },
        )
        .await
}

async fn persist_phase(
    transferred: &Transferred,
    phase: CheckpointPhase,
) -> Result<(), StorageRoleFailure> {
    transferred.stage.checkpoint_set_phase(phase);
    transferred
        .destination
        .persist_hash_checkpoint(&transferred.stage)
        .await
}

async fn record_published(
    transferred: Transferred,
    source_qos: SourceQosStats,
) -> Result<Transferred, TransferFailure> {
    if transferred.stage.checkpoint_enabled()
        && transferred.stage.checkpoint_phase() != CheckpointPhase::Published
        && let Err(error) = persist_phase(&transferred, CheckpointPhase::Published).await
    {
        let failure =
            TransferFailure::role(TransferPhase::Publish, TransferSide::Destination, error);
        return Err(retain_published_failure(transferred, failure, source_qos));
    }
    Ok(transferred)
}

async fn release_transferred(
    transferred: Transferred,
    source_qos: SourceQosStats,
) -> Result<Transferred, TransferFailure> {
    if transferred.stage.checkpoint_enabled()
        && let Err(error) = transferred
            .destination
            .release_hash_checkpoint(&transferred.stage)
            .await
    {
        let failure =
            TransferFailure::role(TransferPhase::Publish, TransferSide::Destination, error);
        return Err(retain_published_failure(transferred, failure, source_qos));
    }
    Ok(transferred)
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
