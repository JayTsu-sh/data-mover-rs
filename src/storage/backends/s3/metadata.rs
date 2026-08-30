use std::sync::Arc;

use async_trait::async_trait;

use crate::model::{
    FailureClass, MetadataObservation, MetadataObservations, MetadataProvenance, ObservationMode,
    ObservationPlan, Operation, Transience,
};
use crate::storage::{Metadata, MetadataMutation, StorageRoleFailure};

use super::S3Protocol;
use super::source::{cancelled, entry, role_failure};

pub(crate) struct S3Metadata<P> {
    protocol: Arc<P>,
    tag_support: S3TagSupport,
}

#[derive(Clone, Copy)]
pub(crate) enum S3TagSupport {
    Supported,
    Unsupported,
}

impl<P> S3Metadata<P> {
    pub(crate) const fn new(protocol: Arc<P>, tag_support: S3TagSupport) -> Self {
        Self {
            protocol,
            tag_support,
        }
    }
}

fn omitted<T>(mode: ObservationMode) -> MetadataObservation<T> {
    match mode {
        ObservationMode::Omit => MetadataObservation::NotRequested,
        ObservationMode::InlineOnly => MetadataObservation::Unsupported,
        ObservationMode::BestEffort | ObservationMode::Required => {
            MetadataObservation::NotApplicable
        }
    }
}

#[async_trait]
impl<P: S3Protocol + 'static> Metadata for S3Metadata<P> {
    async fn observe(
        &self,
        path: &crate::model::StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let tags = if matches!(self.tag_support, S3TagSupport::Supported) {
            match plan.tags() {
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
            match plan.tags() {
                ObservationMode::Omit => MetadataObservation::NotRequested,
                _ => MetadataObservation::Unsupported,
            }
        };
        MetadataObservations::new(
            omitted(plan.acl()),
            omitted(plan.xattrs()),
            tags,
            omitted(plan.ownership_mode()),
            omitted(plan.timestamps()),
        )
        .map_err(|e| entry(path, Operation::Metadata, e.to_string()))
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
