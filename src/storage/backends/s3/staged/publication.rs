use super::super::S3Protocol;
use super::super::source::{cancelled, entry, role_failure};
use super::S3StagedDestination;
use crate::model::Operation;
use crate::storage::{
    PreparedStage, PublicationDisposition, PublicationEvidence, PublicationFailure, PublishRequest,
    StorageRoleFailure,
};

pub(super) async fn publish<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    stage: &PreparedStage,
    request: PublishRequest,
) -> Result<PublicationEvidence, PublicationFailure> {
    let key = validated_stage(adapter, stage, &request).await?;
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
    match matches_expected(adapter, stage, key, request).await {
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
    stage_key: &str,
    request: &PublishRequest,
) -> Result<bool, PublicationFailure> {
    let expected_blake3 = if let Some(digest) = request.expected_blake3 {
        digest
    } else {
        let path = crate::model::StoragePath::new(stage_key).map_err(|_| {
            unchanged_failure(entry(
                stage.final_destination.path(),
                Operation::Publish,
                "invalid S3 stage path during publication reconciliation",
            ))
        })?;
        let Some(digest) = adapter
            .content_digest(
                &path,
                request.expected_size,
                &request.cancel,
                Operation::Publish,
            )
            .await
            .map_err(unchanged_failure)?
        else {
            return Ok(false);
        };
        digest
    };
    adapter
        .content_matches(
            stage.final_destination.path(),
            request.expected_size,
            &expected_blake3,
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
