use super::super::source::{cancelled, entry, role_failure};
use super::super::{S3Protocol, S3ProtocolFailure, S3Result, is_real_version_id};
use super::S3StagedDestination;
use super::upload_pointer::not_deletable_by_version;
use crate::model::{FailureClass, Operation};
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
    let copied = copy_or_reconcile(adapter, stage, &key, &request).await?;
    cleanup(adapter, stage, &key, true).await?;
    let version = if copied {
        current_version(adapter, stage).await
    } else {
        None
    };
    Ok(evidence(stage, PublicationDisposition::Published, version))
}

/// The version of the final object a `CopyObject` that succeeded just made (ADR-0006 C17): the
/// current one, since one key is never written by two transfers at once. `None` when it has no
/// real version or cannot be read.
async fn current_version<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    stage: &PreparedStage,
) -> Option<String> {
    let path = stage.final_destination.path();
    match adapter.protocol.head(path.as_str()).await {
        Ok(facts) => facts
            .version_id
            .filter(|version| is_real_version_id(version)),
        Err(error) => {
            tracing::debug!(path = %path.as_str(), ?error, "could not read the published S3 object's version");
            None
        }
    }
}

/// Deletes a temp key (ADR-0006 C17). It is this stage's alone — named from its binding and final
/// path — so every entry the key's version listing shows is deleted by id, delete markers and the
/// `"null"` version (versioning suspended, or an object from before versioning) included: a plain
/// delete would keep each full-size version behind a new delete marker. An empty listing means
/// nothing is there — no bare delete marker is added. A store that cannot list (logged) gets the
/// plain delete; so does a version it refuses to delete by id (Object Lock, no delete-by-version,
/// any failure to delete `"null"`; logged). A version already gone is fine, and any other failure is returned,
/// so the clean-up fails rather than leaving a version behind.
pub(super) async fn delete_temp_key<P: S3Protocol>(protocol: &P, key: &str) -> S3Result<()> {
    let versions = match protocol.list_versions(key).await {
        Ok(listed) => listed,
        Err(error) => {
            tracing::warn!(
                key,
                ?error,
                "could not list the S3 temp key's versions; deleting it plainly"
            );
            return protocol.delete_object(key).await;
        }
    };
    for version in versions {
        let id = version.version_id;
        match protocol.delete_version(key, &id).await {
            Ok(())
            | Err(S3ProtocolFailure::Entry {
                class: FailureClass::NotFound,
                ..
            }) => {}
            // `"null"` is also what an unversioned bucket lists: a store that cannot delete it by
            // id gets the plain delete it always had.
            Err(error) if id == "null" || not_deletable_by_version(&error, &id) => {
                tracing::warn!(
                    key,
                    version = id,
                    ?error,
                    "could not delete an S3 temp key version; hiding it behind a delete marker"
                );
                return protocol.delete_object(key).await;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
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

/// Copies the temp key to the final key; `true` when the `CopyObject` succeeded, `false` when a
/// failed one is settled by the final object's content (then it may be an identical earlier one).
async fn copy_or_reconcile<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    stage: &PreparedStage,
    key: &str,
    request: &PublishRequest,
) -> Result<bool, PublicationFailure> {
    let Err(copy_failure) = adapter
        .protocol
        .copy_object(key, stage.final_destination.path().as_str())
        .await
    else {
        return Ok(true);
    };
    match matches_expected(adapter, stage, key, request).await {
        Ok(true) => Ok(false),
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
    delete_temp_key(&*adapter.protocol, key)
        .await
        .map_err(|failure| PublicationFailure {
            error: role_failure(stage.final_destination.path(), Operation::Publish, failure),
            final_destination_changed: changed,
        })?;
    adapter.states.lock().await.remove(stage.token.as_ref());
    Ok(())
}

fn evidence(
    stage: &PreparedStage,
    disposition: PublicationDisposition,
    version: Option<String>,
) -> PublicationEvidence {
    PublicationEvidence {
        final_destination: stage.final_destination.path().clone(),
        disposition,
        version,
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::delete_temp_key;
    use crate::storage::backends::s3::S3Protocol as _;
    use crate::storage::backends::s3::tests::{MemoryS3, Versioning, content_md5};

    const TEMP: &str = ".data-mover-stage/binding/final";

    async fn put(protocol: &MemoryS3, body: &'static [u8]) -> Result<(), String> {
        let body = Bytes::from_static(body);
        let md5 = content_md5(&body);
        protocol
            .put_object(TEMP, body, &md5)
            .await
            .map(drop)
            .map_err(|failure| format!("{failure:?}"))
    }

    /// A temp key written while versioning was enabled and again once it was suspended lists
    /// `["null" (latest), v1]`: both go, and no delete marker is added.
    #[tokio::test]
    async fn every_temp_key_version_goes_the_null_one_included() -> Result<(), String> {
        let protocol = MemoryS3::default();
        protocol.set_versioning(Versioning::Enabled);
        put(&protocol, b"first").await?;
        protocol.set_versioning(Versioning::Suspended);
        put(&protocol, b"second").await?;
        let listed = protocol
            .list_versions(TEMP)
            .await
            .map_err(|e| format!("{e:?}"))?;
        assert_eq!(listed.len(), 2);
        delete_temp_key(&protocol, TEMP)
            .await
            .map_err(|e| format!("{e:?}"))?;
        let left = protocol
            .list_versions(TEMP)
            .await
            .map_err(|e| format!("{e:?}"))?;
        assert!(left.is_empty(), "{left:?}");
        assert!(protocol.head(TEMP).await.is_err());
        Ok(())
    }

    /// A temp key that holds nothing is left alone: no bare delete marker.
    #[tokio::test]
    async fn an_empty_temp_key_gets_no_delete_marker() -> Result<(), String> {
        let protocol = MemoryS3::default();
        protocol.set_versioning(Versioning::Enabled);
        delete_temp_key(&protocol, TEMP)
            .await
            .map_err(|e| format!("{e:?}"))?;
        assert!(protocol.keys_with_versions().is_empty());
        Ok(())
    }
}
