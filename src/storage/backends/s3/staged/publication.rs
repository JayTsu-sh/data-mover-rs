use super::super::source::{cancelled, entry, role_failure};
use super::super::{S3Protocol, S3ProtocolFailure};
use super::S3StagedDestination;
use crate::model::Operation;
use crate::storage::{
    ExistingDestinationPolicy, PreparedStage, PublicationDisposition, PublicationEvidence,
    PublicationFailure, PublishRequest, StorageRoleFailure,
};

pub(super) async fn publish<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    stage: &PreparedStage,
    request: PublishRequest,
) -> Result<PublicationEvidence, PublicationFailure> {
    let key = validated_stage(adapter, stage, &request).await?;
    if destination_exists(adapter, stage).await?
        && let Some(evidence) = apply_existing_policy(adapter, stage, &key, &request).await?
    {
        return Ok(evidence);
    }
    copy_or_reconcile(adapter, stage, &key, &request).await?;
    cleanup(adapter, stage, &key, true).await?;
    Ok(evidence(stage, PublicationDisposition::Published))
}

async fn validated_stage<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    stage: &PreparedStage,
    request: &PublishRequest,
) -> Result<String, PublicationFailure> {
    let unchanged = |error| PublicationFailure {
        error,
        final_destination_changed: false,
    };
    let key = adapter.validate(stage).map_err(unchanged)?;
    if request.cancel.is_cancelled() {
        return Err(unchanged(cancelled(
            stage.final_destination.path(),
            Operation::Publish,
        )));
    }
    let staged = adapter.protocol.head(&key).await.map_err(|failure| {
        unchanged(role_failure(
            stage.final_destination.path(),
            Operation::Publish,
            failure,
        ))
    })?;
    if staged.size != request.expected_size {
        return Err(unchanged(entry(
            stage.final_destination.path(),
            Operation::Publish,
            "staged size changed after verification",
        )));
    }
    Ok(key)
}

async fn destination_exists<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    stage: &PreparedStage,
) -> Result<bool, PublicationFailure> {
    match adapter
        .protocol
        .head(stage.final_destination.path().as_str())
        .await
    {
        Ok(_) => Ok(true),
        Err(S3ProtocolFailure::Entry {
            class: crate::model::FailureClass::NotFound,
            ..
        }) => Ok(false),
        Err(failure) => Err(unchanged_failure(role_failure(
            stage.final_destination.path(),
            Operation::Publish,
            failure,
        ))),
    }
}

async fn apply_existing_policy<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    stage: &PreparedStage,
    key: &str,
    request: &PublishRequest,
) -> Result<Option<PublicationEvidence>, PublicationFailure> {
    match request.policy {
        ExistingDestinationPolicy::FailIfExists => Err(unchanged_failure(entry(
            stage.final_destination.path(),
            Operation::Publish,
            "destination exists",
        ))),
        ExistingDestinationPolicy::Overwrite => Ok(None),
        ExistingDestinationPolicy::VerifyOrSkip => {
            if !matches_expected(adapter, stage, request).await? {
                return Err(unchanged_failure(entry(
                    stage.final_destination.path(),
                    Operation::Publish,
                    "existing destination differs",
                )));
            }
            cleanup(adapter, stage, key, false).await?;
            Ok(Some(evidence(
                stage,
                PublicationDisposition::ExistingEquivalent,
            )))
        }
    }
}

async fn copy_or_reconcile<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    stage: &PreparedStage,
    key: &str,
    request: &PublishRequest,
) -> Result<(), PublicationFailure> {
    let Err(copy_failure) = adapter
        .protocol
        .copy_object(key, stage.final_destination.path().as_str())
        .await
    else {
        return Ok(());
    };
    match matches_expected(adapter, stage, request).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(changed_failure(role_failure(
            stage.final_destination.path(),
            Operation::Publish,
            copy_failure,
        ))),
        Err(failure) => Err(PublicationFailure {
            error: failure.error,
            final_destination_changed: true,
        }),
    }
}

async fn matches_expected<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    stage: &PreparedStage,
    request: &PublishRequest,
) -> Result<bool, PublicationFailure> {
    adapter
        .content_matches(
            stage.final_destination.path(),
            request.expected_size,
            &request.expected_blake3,
            &request.cancel,
            Operation::Publish,
        )
        .await
        .map_err(unchanged_failure)
}

async fn cleanup<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    stage: &PreparedStage,
    key: &str,
    changed: bool,
) -> Result<(), PublicationFailure> {
    adapter
        .protocol
        .delete_object(key)
        .await
        .map_err(|failure| PublicationFailure {
            error: role_failure(stage.final_destination.path(), Operation::Publish, failure),
            final_destination_changed: changed,
        })?;
    let claim = adapter
        .states
        .lock()
        .await
        .get(stage.token.as_ref())
        .and_then(|state| state.claim_key.clone());
    if let Some(claim_key) = claim {
        adapter
            .protocol
            .release_claim(&claim_key)
            .await
            .map_err(|failure| PublicationFailure {
                error: role_failure(stage.final_destination.path(), Operation::Publish, failure),
                final_destination_changed: changed,
            })?;
    }
    adapter.states.lock().await.remove(stage.token.as_ref());
    Ok(())
}

fn evidence(stage: &PreparedStage, disposition: PublicationDisposition) -> PublicationEvidence {
    PublicationEvidence {
        final_destination: stage.final_destination.path().clone(),
        disposition,
    }
}

fn unchanged_failure(error: StorageRoleFailure) -> PublicationFailure {
    PublicationFailure {
        error,
        final_destination_changed: false,
    }
}

fn changed_failure(error: StorageRoleFailure) -> PublicationFailure {
    PublicationFailure {
        error,
        final_destination_changed: true,
    }
}
