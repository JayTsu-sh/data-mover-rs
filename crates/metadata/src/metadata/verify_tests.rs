//! 发布确认必须回读完整标签集合，并拒绝被另一版本替换的目标。

use std::sync::Arc;

use bytes::Bytes;
use data_mover_backend_s3::backend::{
    connect,
    tests::{MemoryS3, identity},
};
use tokio_util::sync::CancellationToken;

use super::{
    AclTarget, MetadataPlan, MetadataPlanRequest, MetadataPolicies, MetadataPolicy, MetadataTarget,
    MetadataVerificationError, OwnershipTarget, TimestampTargetCapability, ValueTarget,
    compile_metadata_plan,
};
use crate::model::{
    MetadataFamily, MetadataObservation, MetadataObservationParts, MetadataProvenance, ObjectTag,
    StoragePath,
};
use crate::storage::MetadataMutation;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn tags_plan(tags: Vec<ObjectTag>) -> Result<MetadataPlan, Box<dyn std::error::Error>> {
    let observations = MetadataObservationParts {
        tags: MetadataObservation::Value {
            value: tags,
            provenance: MetadataProvenance::Inline,
        },
        ..MetadataObservationParts::default()
    }
    .build()?;
    Ok(compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: MetadataTarget {
            acl: AclTarget::NotApplicable,
            xattrs: ValueTarget::NotApplicable,
            tags: ValueTarget::Supported,
            ownership_mode: OwnershipTarget::NotApplicable,
            timestamps: TimestampTargetCapability::NotApplicable,
        },
        policies: MetadataPolicies::default().with_tags(MetadataPolicy::RequireExact),
        principal_mapper: None,
    })?)
}

#[tokio::test]
async fn target_tags_are_read_back_as_a_complete_set() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .put_version("file", "v1", Bytes::from_static(b"payload"))
        .await;
    let storage = connect(Arc::clone(&protocol), identity(), None)?;
    let path = StoragePath::new("file")?;
    let plan = tags_plan(vec![ObjectTag::new("a", "1")?, ObjectTag::new("b", "2")?])?;
    plan.apply(
        storage.metadata()?.as_ref(),
        &path,
        CancellationToken::new(),
    )
    .await?;
    let initial_reads = *protocol.tag_reads.lock().await;
    let actual = plan
        .verify_published(&storage, &path, Some("v1"), CancellationToken::new())
        .await?;
    assert_eq!(actual.size, Some(7));
    assert!(*protocol.tag_reads.lock().await > initial_reads);
    storage
        .metadata()?
        .apply(
            &path,
            MetadataMutation::Tags(vec![ObjectTag::new("b", "2")?, ObjectTag::new("a", "1")?]),
            CancellationToken::new(),
        )
        .await?;
    plan.verify_published(&storage, &path, Some("v1"), CancellationToken::new())
        .await?;
    storage
        .metadata()?
        .apply(
            &path,
            MetadataMutation::Tags(vec![
                ObjectTag::new("a", "1")?,
                ObjectTag::new("b", "2")?,
                ObjectTag::new("extra", "3")?,
            ]),
            CancellationToken::new(),
        )
        .await?;
    assert!(matches!(
        plan.verify_published(&storage, &path, Some("v1"), CancellationToken::new()).await,
        Err(MetadataVerificationError::Mismatch { families, .. }) if families == [MetadataFamily::Tags]
    ));
    Ok(())
}

#[tokio::test]
async fn publication_version_and_cancellation_are_checked_before_accepting_metadata() -> TestResult
{
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .put_version("file", "v1", Bytes::from_static(b"payload"))
        .await;
    let storage = connect(Arc::clone(&protocol), identity(), None)?;
    let path = StoragePath::new("file")?;
    let plan = tags_plan(vec![])?;
    protocol
        .put_version("file", "v2", Bytes::from_static(b"replacement"))
        .await;
    assert!(matches!(
        plan.verify_published(&storage, &path, Some("v1"), CancellationToken::new())
            .await,
        Err(MetadataVerificationError::VersionChanged { .. })
    ));
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(
        plan.verify_published(&storage, &path, Some("v2"), cancel)
            .await,
        Err(MetadataVerificationError::Cancelled)
    ));
    Ok(())
}
