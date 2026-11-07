//! 版本元数据计划只能写指定版本，不得退回当前对象或重新获取源元数据。
use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::{MetadataObservation, MetadataProvenance, ObjectTag, ObservationPlan};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Default)]
struct VersionTarget {
    writes: Mutex<Vec<(String, MetadataMutation)>>,
}

#[async_trait]
impl Metadata for VersionTarget {
    async fn observe(
        &self,
        _: &StoragePath,
        _: ObservationPlan,
    ) -> std::result::Result<MetadataObservations, StorageRoleFailure> {
        panic!("captured metadata must not be read again")
    }
    async fn apply(
        &self,
        _: &StoragePath,
        _: MetadataMutation,
        _: CancellationToken,
    ) -> std::result::Result<(), StorageRoleFailure> {
        panic!("a version mutation must not change the current object")
    }
    async fn apply_to_version(
        &self,
        _: &StoragePath,
        version_id: &str,
        mutation: MetadataMutation,
        _: CancellationToken,
    ) -> std::result::Result<(), StorageRoleFailure> {
        self.writes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((version_id.to_owned(), mutation));
        Ok(())
    }
}

struct CurrentOnly;
#[async_trait]
impl Metadata for CurrentOnly {
    async fn observe(
        &self,
        _: &StoragePath,
        _: ObservationPlan,
    ) -> std::result::Result<MetadataObservations, StorageRoleFailure> {
        panic!("unexpected observation")
    }
    async fn apply(
        &self,
        _: &StoragePath,
        _: MetadataMutation,
        _: CancellationToken,
    ) -> std::result::Result<(), StorageRoleFailure> {
        panic!("unsupported version mutation must not fall back to current")
    }
}

fn plan() -> Result<MetadataPlan> {
    let observed = MetadataObservations::new(
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        MetadataObservation::Value {
            value: vec![ObjectTag::new("raw", "changed")?],
            provenance: MetadataProvenance::Inline,
        },
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
    )?;
    Ok(compile_metadata_plan(&MetadataPlanRequest {
        observations: &observed,
        target: MetadataTarget {
            acl: AclTarget::NotApplicable,
            xattrs: ValueTarget::NotApplicable,
            tags: ValueTarget::Supported,
            ownership_mode: OwnershipTarget::NotApplicable,
            timestamps: TimestampTargetCapability::Unsupported,
        },
        policies: MetadataPolicies::default().with_tags(MetadataPolicy::RequireExact),
        principal_mapper: None,
    })?)
}

#[tokio::test]
async fn captured_plan_mutates_only_the_explicit_target_version() -> Result {
    let target = VersionTarget::default();
    let report = plan()?
        .apply_to_version(
            &target,
            &StoragePath::new("key")?,
            "target-v1",
            CancellationToken::new(),
        )
        .await?;
    let writes = target.writes.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(writes.len(), 1);
    assert_eq!(
        writes[0],
        (
            "target-v1".to_owned(),
            MetadataMutation::Tags(vec![ObjectTag::new("raw", "changed")?])
        )
    );
    assert!(
        report
            .outcomes()
            .iter()
            .any(|outcome| outcome.family == MetadataFamily::Tags
                && outcome.outcome == ApplicationOutcome::Applied)
    );
    Ok(())
}

#[tokio::test]
async fn unsupported_version_mutation_is_reported_without_current_writes() -> Result {
    let failure = plan()?
        .apply_to_version(
            &CurrentOnly,
            &StoragePath::new("key")?,
            "target-v1",
            CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("unsupported version update succeeded")?;
    assert!(failure.to_string().contains("Unsupported"));
    assert_eq!(failure.failures().len(), 1);
    Ok(())
}

#[tokio::test]
async fn cancelled_version_plan_does_not_mutate_any_version() -> Result {
    let target = VersionTarget::default();
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(
        plan()?
            .apply_to_version(&target, &StoragePath::new("key")?, "target-v1", cancel)
            .await
            .is_err()
    );
    assert!(
        target
            .writes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    );
    Ok(())
}
