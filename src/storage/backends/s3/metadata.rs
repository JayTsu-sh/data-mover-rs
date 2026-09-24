use std::sync::Arc;

use async_trait::async_trait;

use crate::model::{
    BackendIdentity, FailureClass, MetadataObservation, MetadataObservations, MetadataProvenance,
    ObjectTag, ObservationMode, ObservationPlan, Operation, SourceIdentity, StoragePath,
    TimestampMetadata, Transience,
};
use crate::storage::{Metadata, MetadataMutation, StorageRoleFailure};

use super::source::{cancelled, classified_entry, entry, object_identity, role_failure};
use super::{S3ObjectFacts, S3Protocol};

pub(crate) struct S3Metadata<P> {
    protocol: Arc<P>,
    identity: BackendIdentity,
    tag_support: S3TagSupport,
}

#[derive(Clone, Copy)]
pub(crate) enum S3TagSupport {
    Supported,
    Unsupported,
}

impl<P> S3Metadata<P> {
    pub(crate) const fn new(
        protocol: Arc<P>,
        identity: BackendIdentity,
        tag_support: S3TagSupport,
    ) -> Self {
        Self {
            protocol,
            identity,
            tag_support,
        }
    }
}

/// Owner and mode: an object has none, so there is nothing to carry and nothing lost.
fn omitted<T>(mode: ObservationMode) -> MetadataObservation<T> {
    match mode {
        ObservationMode::Omit => MetadataObservation::NotRequested,
        ObservationMode::InlineOnly => MetadataObservation::Unsupported,
        ObservationMode::BestEffort | ObservationMode::Required => {
            MetadataObservation::NotApplicable
        }
    }
}

/// ACLs and extended attributes in the file sense: S3 cannot give them, so a copy that asked
/// for them reports them skipped because of the source.
fn cannot_read<T>(mode: ObservationMode) -> MetadataObservation<T> {
    if mode == ObservationMode::Omit {
        MetadataObservation::NotRequested
    } else {
        MetadataObservation::Unsupported
    }
}

/// The object's own `Last-Modified`, from the same response as its identity. An object carries no
/// access or creation time; the legacy `x-amz-meta-last-modified` header is not consulted.
fn timestamps(
    mode: ObservationMode,
    facts: Option<&S3ObjectFacts>,
) -> MetadataObservation<TimestampMetadata> {
    match (mode, facts) {
        (ObservationMode::Omit, _) => MetadataObservation::NotRequested,
        (_, None) => MetadataObservation::Unsupported,
        (_, Some(facts)) => MetadataObservation::Value {
            value: TimestampMetadata {
                accessed: None,
                modified: facts.last_modified,
                created: None,
            },
            provenance: MetadataProvenance::AdditionalCall,
        },
    }
}

impl<P: S3Protocol + 'static> S3Metadata<P> {
    async fn head(&self, path: &StoragePath) -> Result<S3ObjectFacts, StorageRoleFailure> {
        self.protocol
            .head(path.as_str())
            .await
            .map_err(|e| role_failure(path, Operation::Metadata, e))
    }

    async fn observations(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
        facts: Option<&S3ObjectFacts>,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        MetadataObservations::new(
            cannot_read(plan.acl()),
            cannot_read(plan.xattrs()),
            self.tags(path, plan.tags()).await?,
            omitted(plan.ownership_mode()),
            timestamps(plan.timestamps(), facts),
        )
        .map_err(|e| entry(path, Operation::Metadata, e.to_string()))
    }

    async fn tags(
        &self,
        path: &StoragePath,
        mode: ObservationMode,
    ) -> Result<MetadataObservation<Vec<ObjectTag>>, StorageRoleFailure> {
        Ok(if matches!(self.tag_support, S3TagSupport::Supported) {
            match mode {
                ObservationMode::Omit => MetadataObservation::NotRequested,
                ObservationMode::InlineOnly => MetadataObservation::Unsupported,
                ObservationMode::BestEffort => match self.protocol.get_tags(path.as_str()).await {
                    Ok(value) => MetadataObservation::Value {
                        value,
                        provenance: MetadataProvenance::AdditionalCall,
                    },
                    Err(_) => MetadataObservation::Failed {
                        class: FailureClass::Protocol,
                        transience: Transience::Unknown,
                    },
                },
                ObservationMode::Required => MetadataObservation::Value {
                    value: self
                        .protocol
                        .get_tags(path.as_str())
                        .await
                        .map_err(|e| role_failure(path, Operation::Metadata, e))?,
                    provenance: MetadataProvenance::AdditionalCall,
                },
            }
        } else {
            match mode {
                ObservationMode::Omit => MetadataObservation::NotRequested,
                _ => MetadataObservation::Unsupported,
            }
        })
    }
}

#[async_trait]
impl<P: S3Protocol + 'static> Metadata for S3Metadata<P> {
    /// Every copy from S3 carries the object's `Last-Modified` as the file's mtime; owner and mode
    /// are asked for only so the plan records that an object has none.
    fn copied_metadata_observation_plan(&self) -> Option<ObservationPlan> {
        Some(
            ObservationPlan::default()
                .with_ownership_mode(ObservationMode::Required)
                .with_timestamps(ObservationMode::Required),
        )
    }

    async fn observe(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let facts = match plan.timestamps() {
            ObservationMode::BestEffort | ObservationMode::Required => Some(self.head(path).await?),
            ObservationMode::Omit | ObservationMode::InlineOnly => None,
        };
        self.observations(path, plan, facts.as_ref()).await
    }

    /// Binds the time to the object the copy described: the `Last-Modified` comes from a HEAD whose
    /// `ETag` / version must still be the one described, so a replaced object is a conflict rather
    /// than the new object's time on the old object's bytes.
    async fn observe_bound(
        &self,
        path: &StoragePath,
        expected: &SourceIdentity,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let facts = self.head(path).await?;
        let observed = object_identity(&self.identity, &facts)
            .map_err(|e| entry(path, Operation::Metadata, e.to_string()))?;
        if observed != *expected {
            return Err(classified_entry(
                path,
                Operation::Metadata,
                FailureClass::Conflict,
                Transience::Permanent,
                "S3 source identity changed",
            ));
        }
        self.observations(path, plan, Some(&facts)).await
    }

    async fn apply(
        &self,
        path: &crate::model::StoragePath,
        mutation: MetadataMutation,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        if cancel.is_cancelled() {
            return Err(cancelled(path, Operation::Metadata));
        }
        if matches!(self.tag_support, S3TagSupport::Unsupported)
            && matches!(mutation, MetadataMutation::Tags(_))
        {
            return Err(super::source::classified_entry(
                path,
                Operation::Metadata,
                FailureClass::Unsupported,
                Transience::Permanent,
                "object tags are unsupported by this S3 compatibility profile",
            ));
        }
        match mutation {
            MetadataMutation::Tags(tags) => self
                .protocol
                .put_tags(path.as_str(), &tags)
                .await
                .map_err(|e| role_failure(path, Operation::Metadata, e)),
            _ => Err(entry(
                path,
                Operation::Metadata,
                "metadata kind is unsupported by S3",
            )),
        }
    }
}

#[cfg(test)]
#[path = "metadata_tests.rs"]
mod tests;
