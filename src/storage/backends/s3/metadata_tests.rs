//! The S3 source's copy baseline: the object's own `Last-Modified`, bound to the object read.

use bytes::Bytes;

use super::*;
use crate::model::{BackendKind, StorageTimestamp, TimePrecision};
use crate::storage::backends::s3::tests::MemoryS3;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

const PATH: &str = "object";
const MODIFIED_SECS: i128 = 1_700_000_000;

async fn fixture(
    last_modified: Option<StorageTimestamp>,
) -> Result<(Arc<MemoryS3>, S3Metadata<MemoryS3>)> {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .objects
        .lock()
        .await
        .insert(PATH.to_owned(), Bytes::from_static(b"payload"));
    *protocol.last_modified.lock().await = last_modified;
    let identity = BackendIdentity::new(BackendKind::S3, "source")?;
    let metadata = S3Metadata::new(protocol.clone(), identity, S3TagSupport::Supported);
    Ok((protocol, metadata))
}

fn modified() -> Result<StorageTimestamp> {
    Ok(StorageTimestamp::new(
        MODIFIED_SECS * 1_000_000_000,
        TimePrecision::Seconds,
    )?)
}

async fn described(protocol: &MemoryS3, metadata: &S3Metadata<MemoryS3>) -> Result<SourceIdentity> {
    let facts = protocol.head(PATH).await.map_err(|_| "head failed")?;
    Ok(object_identity(&metadata.identity, &facts)?)
}

#[tokio::test]
async fn a_copy_carries_the_objects_last_modified_and_nothing_else() -> Result {
    let (protocol, metadata) = fixture(Some(modified()?)).await?;
    let plan = metadata
        .copied_metadata_observation_plan()
        .ok_or("S3 declares a copy baseline")?;
    let path = StoragePath::new(PATH)?;
    let observed = metadata
        .observe_bound(&path, &described(&protocol, &metadata).await?, plan)
        .await?;
    assert_eq!(
        observed.timestamps().value(),
        Some(&TimestampMetadata {
            accessed: None,
            modified: Some(modified()?),
            created: None,
        })
    );
    assert_eq!(
        observed.ownership_mode(),
        &MetadataObservation::NotApplicable,
        "an object has no owner or mode: nothing to lose"
    );
    Ok(())
}

/// A server that sends no `Last-Modified` leaves the time unknown — never the time of the copy.
#[tokio::test]
async fn a_missing_last_modified_is_unknown_not_now() -> Result {
    let (protocol, metadata) = fixture(None).await?;
    let path = StoragePath::new(PATH)?;
    let observed = metadata
        .observe_bound(
            &path,
            &described(&protocol, &metadata).await?,
            ObservationPlan::default().with_timestamps(ObservationMode::Required),
        )
        .await?;
    assert_eq!(
        observed
            .timestamps()
            .value()
            .and_then(|value| value.modified),
        None
    );
    Ok(())
}

/// The time comes from the object that was described: once it is replaced, its time is not put
/// on the bytes that were read.
#[tokio::test]
async fn a_replaced_object_is_a_conflict() -> Result {
    let (protocol, metadata) = fixture(Some(modified()?)).await?;
    let expected = described(&protocol, &metadata).await?;
    protocol
        .objects
        .lock()
        .await
        .insert(PATH.to_owned(), Bytes::from_static(b"replaced"));
    let path = StoragePath::new(PATH)?;
    let result = metadata
        .observe_bound(
            &path,
            &expected,
            ObservationPlan::default().with_timestamps(ObservationMode::Required),
        )
        .await;
    assert!(
        matches!(&result, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Conflict),
        "{:?}",
        result.err()
    );
    Ok(())
}

/// ACLs and extended attributes asked of S3 are unreadable, so the copy reports them skipped
/// because of the source rather than silently absent.
#[tokio::test]
async fn acls_and_xattrs_asked_for_are_unreadable() -> Result {
    let (_, metadata) = fixture(Some(modified()?)).await?;
    let observed = metadata
        .observe(
            &StoragePath::new(PATH)?,
            ObservationPlan::default()
                .with_acl(ObservationMode::BestEffort)
                .with_xattrs(ObservationMode::BestEffort),
        )
        .await?;
    assert_eq!(observed.acl(), &MetadataObservation::Unsupported);
    assert_eq!(observed.xattrs(), &MetadataObservation::Unsupported);
    assert_eq!(observed.timestamps(), &MetadataObservation::NotRequested);
    Ok(())
}

/// Only an explicit read of the times costs a HEAD; `InlineOnly` has nothing inline to give.
#[tokio::test]
async fn times_cost_a_head_only_when_asked_for() -> Result {
    let (_, metadata) = fixture(Some(modified()?)).await?;
    let path = StoragePath::new(PATH)?;
    let asked = metadata
        .observe(
            &path,
            ObservationPlan::default().with_timestamps(ObservationMode::BestEffort),
        )
        .await?;
    assert_eq!(
        asked.timestamps().value().and_then(|value| value.modified),
        Some(modified()?)
    );
    let inline = metadata
        .observe(
            &path,
            ObservationPlan::default().with_timestamps(ObservationMode::InlineOnly),
        )
        .await?;
    assert_eq!(inline.timestamps(), &MetadataObservation::Unsupported);
    Ok(())
}

/// An object deleted after it was described is not found — the copy fails for that, not for a
/// made-up time.
#[tokio::test]
async fn an_object_deleted_after_describe_is_not_found() -> Result {
    let (protocol, metadata) = fixture(Some(modified()?)).await?;
    let expected = described(&protocol, &metadata).await?;
    protocol.objects.lock().await.remove(PATH);
    let result = metadata
        .observe_bound(
            &StoragePath::new(PATH)?,
            &expected,
            ObservationPlan::default().with_timestamps(ObservationMode::Required),
        )
        .await;
    assert!(
        matches!(&result, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound),
        "{:?}",
        result.err()
    );
    Ok(())
}
