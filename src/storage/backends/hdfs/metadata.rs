use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::protocol::{HdfsProtocol, cancelled, entry_failure};
use crate::model::{
    FailureClass, MetadataObservation, MetadataObservations, MetadataProvenance, ObservationMode,
    ObservationPlan, Operation, StoragePath, StorageTimestamp, TimePrecision, TimestampMetadata,
    Transience,
};
use crate::storage::{Metadata, MetadataMutation, StorageRoleFailure};

pub(super) struct HdfsMetadata {
    protocol: Arc<dyn HdfsProtocol>,
}

impl HdfsMetadata {
    pub(super) fn new<P: HdfsProtocol + 'static>(protocol: Arc<P>) -> Self {
        Self { protocol }
    }
}

#[async_trait]
impl Metadata for HdfsMetadata {
    async fn observe(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let timestamp_observation = if plan.timestamps() == ObservationMode::Omit {
            MetadataObservation::NotRequested
        } else {
            let facts = self.protocol.stat(path).await?;
            inline(plan.timestamps(), timestamps(facts.atime, facts.mtime))
        };
        MetadataObservations::new(
            unavailable(plan.acl()),
            unavailable(plan.xattrs()),
            not_applicable(plan.tags()),
            unavailable(plan.ownership_mode()),
            timestamp_observation,
        )
        .map_err(|_| failure(path, FailureClass::Protocol))
    }

    async fn apply(
        &self,
        path: &StoragePath,
        mutation: MetadataMutation,
        cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        if cancel.is_cancelled() {
            return Err(cancelled(path, Operation::Metadata));
        }
        match mutation {
            MetadataMutation::MappedOwnership(value) => {
                self.protocol
                    .set_mapped_ownership(path, value.owner(), value.group(), value.mode)
                    .await
            }
            MetadataMutation::Timestamps(value) => {
                self.protocol
                    .set_timestamps(
                        path,
                        timestamp_nanos(value.accessed),
                        timestamp_nanos(value.modified),
                    )
                    .await
            }
            MetadataMutation::Acl(_)
            | MetadataMutation::Xattrs(_)
            | MetadataMutation::Tags(_)
            | MetadataMutation::NumericOwnership(_) => {
                Err(failure(path, FailureClass::Unsupported))
            }
        }
    }
}

fn timestamps(atime: i64, mtime: i64) -> TimestampMetadata {
    TimestampMetadata {
        accessed: StorageTimestamp::new(i128::from(atime), TimePrecision::Milliseconds).ok(),
        modified: StorageTimestamp::new(i128::from(mtime), TimePrecision::Milliseconds).ok(),
        created: None,
    }
}

fn timestamp_nanos(value: Option<StorageTimestamp>) -> Option<i64> {
    value.and_then(|timestamp| i64::try_from(timestamp.unix_nanos()).ok())
}

fn unavailable<T>(mode: ObservationMode) -> MetadataObservation<T> {
    if mode == ObservationMode::Omit {
        MetadataObservation::NotRequested
    } else {
        MetadataObservation::Unsupported
    }
}

fn not_applicable<T>(mode: ObservationMode) -> MetadataObservation<T> {
    if mode == ObservationMode::Omit {
        MetadataObservation::NotRequested
    } else {
        MetadataObservation::NotApplicable
    }
}

fn inline<T>(mode: ObservationMode, value: T) -> MetadataObservation<T> {
    if mode == ObservationMode::Omit {
        MetadataObservation::NotRequested
    } else {
        MetadataObservation::Value {
            value,
            provenance: MetadataProvenance::Inline,
        }
    }
}

fn failure(path: &StoragePath, class: FailureClass) -> StorageRoleFailure {
    entry_failure(path, Operation::Metadata, class, Transience::Permanent)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_and_not_applicable_families_are_distinct() {
        assert_eq!(
            unavailable::<()>(ObservationMode::Required),
            MetadataObservation::Unsupported
        );
        assert_eq!(
            not_applicable::<()>(ObservationMode::Required),
            MetadataObservation::NotApplicable
        );
        assert_eq!(
            unavailable::<()>(ObservationMode::Omit),
            MetadataObservation::NotRequested
        );
    }
}
