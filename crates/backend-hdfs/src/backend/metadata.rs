use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::protocol::{HdfsProtocol, cancelled, entry_failure};
use crate::model::{
    FailureClass, MetadataCapabilities, MetadataFamily as F, MetadataObservation,
    MetadataObservations, MetadataProvenance, ObservationMode, ObservationPlan,
    ObservationSupport as S, Operation, SourceVersion, StoragePath, StorageTimestamp,
    TimePrecision, TimestampMetadata, Transience,
};
use crate::storage::{Metadata, MetadataMutation, StorageRoleFailure};

pub(super) struct HdfsMetadata {
    protocol: Arc<dyn HdfsProtocol>,
    identity: crate::model::BackendIdentity,
}

impl HdfsMetadata {
    pub(super) fn new<P: HdfsProtocol + 'static>(
        protocol: Arc<P>,
        identity: crate::model::BackendIdentity,
    ) -> Self {
        Self { protocol, identity }
    }
}

#[async_trait]
impl Metadata for HdfsMetadata {
    fn observation_capabilities(&self) -> MetadataCapabilities {
        MetadataCapabilities::default()
            .with(F::Acl, S::Unsupported)
            .with(F::Xattrs, S::Unsupported)
            .with(F::Tags, S::NotApplicable)
            .with(F::OwnershipMode, S::Unsupported)
            .with(F::Timestamps, S::Supported)
    }

    async fn permissions(
        &self,
        described: &crate::storage::SourceDescriptor,
    ) -> Result<crate::storage::PermissionFacts, StorageRoleFailure> {
        if !matches!(described.version(), SourceVersion::Current) {
            return Err(failure(&described.path, FailureClass::Unsupported));
        }
        super::source::permissions(described)
            .ok_or_else(|| failure(&described.path, FailureClass::Protocol))
    }
    fn copied_metadata_observation_plan(&self) -> Option<ObservationPlan> {
        Some(ObservationPlan::default().with_timestamps(ObservationMode::Required))
    }

    async fn observe_copy_bound(
        &self,
        path: &StoragePath,
        expected: &crate::model::SourceIdentity,
        plan: ObservationPlan,
    ) -> Result<crate::storage::CopiedMetadataObservation, StorageRoleFailure> {
        let facts = self.protocol.stat(path).await?;
        let descriptor = super::source::descriptor(&self.identity, path.clone(), &facts)?;
        if descriptor.source_identity != *expected {
            return Err(failure(path, FailureClass::Conflict));
        }
        let observations = MetadataObservations::new(
            unavailable(plan.acl()),
            unavailable(plan.xattrs()),
            not_applicable(plan.tags()),
            unavailable(plan.ownership_mode()),
            inline(plan.timestamps(), timestamps(facts.atime, facts.mtime)),
        )
        .map_err(|_| failure(path, FailureClass::Protocol))?;
        Ok(crate::storage::CopiedMetadataObservation {
            observations,
            mode_without_ownership: Some(facts.mode & 0o7777),
            owner_names_unmapped: false,
        })
    }

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
            MetadataMutation::Mode(mode) => self.protocol.set_mode(path, mode).await,
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

pub(super) fn timestamps(atime: i64, mtime: i64) -> TimestampMetadata {
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

    #[tokio::test]
    async fn named_permissions_survive_unsupported_ownership_but_reject_version_ids()
    -> Result<(), Box<dyn std::error::Error>> {
        use super::super::protocol::HdfsEntryFacts;
        use crate::model::{BackendIdentity, BackendKind, EntryKind, SourceVersion};
        use crate::storage::PrincipalIdentity;

        let identity = BackendIdentity::new(BackendKind::Hdfs, "permission-versions")?;
        let metadata = HdfsMetadata::new(
            Arc::new(super::super::contract_tests::MemoryHdfs::default()),
            identity.clone(),
        );
        let facts = HdfsEntryFacts {
            path: StoragePath::new("file")?,
            kind: EntryKind::File,
            size: Some(4),
            mtime: 10,
            atime: 0,
            mode: 0o640,
            owner: "alice".into(),
            group: "staff".into(),
            replication: None,
            block_size: None,
        };
        let mut descriptor =
            super::super::source::descriptor(&identity, StoragePath::new("file")?, &facts)?;
        assert_eq!(
            metadata
                .observation_capabilities()
                .support(F::OwnershipMode),
            S::Unsupported
        );
        assert_eq!(
            metadata.permissions(&descriptor).await?,
            crate::storage::PermissionFacts {
                owner: Some(PrincipalIdentity::Named("alice".into())),
                group: Some(PrincipalIdentity::Named("staff".into())),
                mode: Some(0o640),
            }
        );
        descriptor.version = SourceVersion::Id("old-version".into());
        let result = metadata.permissions(&descriptor).await;
        assert!(matches!(result, Err(StorageRoleFailure::Entry(ref entry))
            if entry.class() == FailureClass::Unsupported && entry.operation() == Operation::Metadata));
        Ok(())
    }
}
