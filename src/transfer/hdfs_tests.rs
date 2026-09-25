use std::sync::Arc;

use bytes::Bytes;
use futures::stream;
use tokio_util::sync::CancellationToken;

use super::{InflightLimits, TransferIdentity, TransferPolicy, TransferRequest, transfer};
use crate::model::{
    MappedOwnership, ObservationMode, ObservationPlan, Operation, StoragePath, StorageTimestamp,
    TimePrecision, TimestampMetadata,
};
use crate::storage::backends::hdfs::contract_tests::MemoryHdfs;
use crate::storage::backends::hdfs::protocol::cancelled;
use crate::storage::backends::hdfs::{connect, test_identity};
use crate::storage::{
    DestinationPrepareRequest, FinalDestination, MetadataMutation, PreflightPolicy, PrepareFact,
    PrepareRequest, PublishRequest, SourceDescriptor, StagedDestination, Storage,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn recoverable_request(request: TransferRequest, _resume_marker: Option<()>) -> TransferRequest {
    request.with_transfer_policy(TransferPolicy::Checkpointed)
}

fn request(source: Storage, destination: Storage) -> TestResult<TransferRequest> {
    Ok(TransferRequest::new(
        source,
        StoragePath::new("source")?,
        destination,
        StoragePath::new("final")?,
        InflightLimits::new(2, 64 * 1024, 2)?,
        CancellationToken::new(),
    )
    .with_identity_override(TransferIdentity::from_label("hdfs-role-contract")?))
}

/// The transfer identity every at-destination prepare in these tests records in its pointer.
const RESUME_IDENTITY: [u8; 32] = [0x42; 32];

/// A fresh connection's staged role on `protocol` — one process's view of the destination — and
/// the description of its `source`.
async fn staged_roles(
    protocol: &Arc<MemoryHdfs>,
    label: &str,
) -> TestResult<(Arc<dyn StagedDestination>, SourceDescriptor)> {
    let storage = connect(Arc::clone(protocol), test_identity(label)?)?;
    let descriptor = storage
        .read_source(&PreflightPolicy::production())?
        .describe(&StoragePath::new("source")?)
        .await?;
    Ok((
        storage.staged_destination(&PreflightPolicy::production())?,
        descriptor,
    ))
}

/// A recoverable prepare of `final` that resumes what it finds there.
fn at_destination(
    descriptor: &SourceDescriptor,
    binding: [u8; 32],
) -> TestResult<DestinationPrepareRequest> {
    Ok(DestinationPrepareRequest::new(
        PrepareRequest {
            final_destination: FinalDestination::new(StoragePath::new("final")?),
            source: descriptor.clone(),
            recovery_binding: binding,
        },
        RESUME_IDENTITY,
    ))
}

fn publish_request(payload: &[u8]) -> PublishRequest {
    PublishRequest {
        expected_size: payload.len() as u64,
        expected_blake3: Some(*blake3::hash(payload).as_bytes()),
        cancel: CancellationToken::new(),
    }
}

#[tokio::test]
async fn architecture_roles_transfer_verify_and_publish() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol
        .insert("source", Bytes::from_static(b"hdfs payload"))
        .await;
    let source = connect(protocol.clone(), test_identity("source")?)?;
    let destination = connect(protocol.clone(), test_identity("destination")?)?;
    let outcome = transfer(request(source, destination)?).await?;
    assert_eq!(outcome.transferred_bytes, 12);
    assert_eq!(
        protocol.get("final").await.as_deref(),
        Some(b"hdfs payload".as_slice())
    );
    Ok(())
}

#[tokio::test]
async fn write_failure_retains_discard_authority_and_final_is_unchanged() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol
        .insert("source", Bytes::from_static(b"failure"))
        .await;
    protocol.fail_writes();
    let source = connect(protocol.clone(), test_identity("source-failure")?)?;
    let destination = connect(protocol.clone(), test_identity("destination-failure")?)?;
    let Err(failure) = transfer(request(source, destination)?).await else {
        return Err("injected HDFS write failure succeeded".into());
    };
    assert!(!failure.has_recoverable_stage());
    assert!(failure.has_unpublished_stage());
    assert!(!failure.final_destination_changed());
    failure.discard_stage().await?;
    assert!(protocol.get("final").await.is_none());
    assert_eq!(protocol.len().await, 1);
    Ok(())
}

#[tokio::test]
async fn cancelled_transfer_stops_before_hdfs_stage_creation() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol
        .insert("source", Bytes::from_static(b"cancelled"))
        .await;
    let source = connect(protocol.clone(), test_identity("source-cancel")?)?;
    let destination = connect(protocol.clone(), test_identity("destination-cancel")?)?;
    let request = request(source, destination)?;
    request.cancel.cancel();

    assert!(transfer(request).await.is_err());
    assert!(protocol.get("final").await.is_none());
    assert_eq!(protocol.len().await, 1);
    Ok(())
}

/// A stage prepared at the destination is found again by a new connection, which resumes from its
/// observed prefix; entering again resumes again.
#[tokio::test]
async fn hdfs_stage_resumes_an_observed_prefix_from_the_destination() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol
        .insert("source", Bytes::from_static(b"restart"))
        .await;
    let (staged, descriptor) = staged_roles(&protocol, "resume").await?;
    let stage = staged
        .prepare_at_destination(at_destination(&descriptor, [8; 32])?)
        .await?;
    assert_eq!(stage.prepare_fact(), PrepareFact::Fresh);
    drop(stage);
    for attempt in 0..2 {
        let (staged, descriptor) = staged_roles(&protocol, "resume").await?;
        let resumed = staged
            .prepare_at_destination(at_destination(&descriptor, [8; 32])?)
            .await?;
        assert_eq!(
            resumed.prepare_fact(),
            PrepareFact::Resumed { bytes: 0 },
            "attempt {attempt}"
        );
        assert_eq!(staged.observe_checkpoint(&resumed).await?.durable_prefix, 0);
        if attempt == 1 {
            staged.discard(resumed).await?;
        }
    }
    assert_eq!(protocol.len().await, 1);
    Ok(())
}

#[tokio::test]
async fn interrupted_hdfs_stage_resumes_only_the_durable_tail() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol
        .insert("source", Bytes::from_static(b"abcdef"))
        .await;
    let (staged, descriptor) = staged_roles(&protocol, "partial").await?;
    let stage = staged
        .prepare_at_destination(at_destination(&descriptor, [4; 32])?)
        .await?;
    let interrupted = Box::pin(stream::iter([
        Ok(Bytes::from_static(b"abc")),
        Err(cancelled(&StoragePath::new("source")?, Operation::Read)),
    ]));
    assert!(staged.write(&stage, interrupted).await.is_err());
    assert_eq!(staged.observe_checkpoint(&stage).await?.durable_prefix, 3);
    drop(stage);

    let (staged, descriptor) = staged_roles(&protocol, "partial").await?;
    let resumed = staged
        .prepare_at_destination(at_destination(&descriptor, [4; 32])?)
        .await?;
    assert_eq!(resumed.prepare_fact(), PrepareFact::Resumed { bytes: 3 });
    let evidence = staged
        .write(
            &resumed,
            Box::pin(stream::iter([Ok(Bytes::from_static(b"def"))])),
        )
        .await?;
    assert_eq!(evidence.persisted_bytes, 6);
    assert_eq!(staged.observe_checkpoint(&resumed).await?.durable_prefix, 6);
    staged
        .publish(&resumed, publish_request(b"abcdef"))
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(
        protocol.get("final").await.as_deref(),
        Some(b"abcdef".as_slice())
    );
    // Only the source and the final file are left: no stage, no pointer.
    assert_eq!(protocol.len().await, 2);
    Ok(())
}

#[tokio::test]
async fn hdfs_resume_stabilizes_the_lease_before_choosing_the_resume_offset() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol
        .insert("source", Bytes::from_static(b"abcdef"))
        .await;
    let (staged, descriptor) = staged_roles(&protocol, "lease").await?;
    drop(
        staged
            .prepare_at_destination(at_destination(&descriptor, [9; 32])?)
            .await?,
    );
    protocol
        .reveal_tail_during_lease_recovery(Bytes::from_static(b"abc"))
        .await;

    let (staged, descriptor) = staged_roles(&protocol, "lease").await?;
    let resumed = staged
        .prepare_at_destination(at_destination(&descriptor, [9; 32])?)
        .await?;
    assert_eq!(protocol.stabilize_calls(), 1);
    assert_eq!(resumed.prepare_fact(), PrepareFact::Resumed { bytes: 3 });
    let evidence = staged
        .write(
            &resumed,
            Box::pin(stream::iter([Ok(Bytes::from_static(b"def"))])),
        )
        .await?;

    assert_eq!(evidence.persisted_bytes, 6);
    staged.discard(resumed).await?;
    Ok(())
}

#[tokio::test]
async fn complete_resumed_hdfs_stage_does_not_poll_an_ended_stream_twice() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol
        .insert("source", Bytes::from_static(b"abcdef"))
        .await;
    let (staged, descriptor) = staged_roles(&protocol, "complete").await?;
    drop(
        staged
            .prepare_at_destination(at_destination(&descriptor, [11; 32])?)
            .await?,
    );
    protocol
        .reveal_tail_during_lease_recovery(Bytes::from_static(b"abcdef"))
        .await;
    let (staged, descriptor) = staged_roles(&protocol, "complete").await?;
    let resumed = staged
        .prepare_at_destination(at_destination(&descriptor, [11; 32])?)
        .await?;
    assert_eq!(resumed.prepare_fact(), PrepareFact::Resumed { bytes: 6 });

    let ended = stream::unfold((), |()| async {
        None::<(Result<Bytes, crate::storage::StorageRoleFailure>, ())>
    });
    let evidence = staged.write(&resumed, Box::pin(ended)).await?;

    assert_eq!(evidence.persisted_bytes, 6);
    staged.discard(resumed).await?;
    Ok(())
}

#[tokio::test]
async fn require_resume_reclaims_hdfs_stage_before_reupload() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let payload = Bytes::from(vec![0x72; 64 * 1024]);
    protocol.insert("source", payload.clone()).await;
    protocol.fail_writes();
    let source = connect(protocol.clone(), test_identity("require-source")?)?;
    let destination = connect(protocol.clone(), test_identity("require-destination")?)?;
    let failure = transfer(recoverable_request(request(source, destination)?, None))
        .await
        .err()
        .ok_or("injected HDFS failure succeeded")?;
    drop(failure);
    protocol.allow_writes();
    let source = connect(protocol.clone(), test_identity("require-source")?)?;
    let destination = connect(protocol.clone(), test_identity("require-destination")?)?;
    let outcome = transfer(recoverable_request(request(source, destination)?, None)).await?;
    assert_eq!(outcome.transferred_bytes, payload.len() as u64);
    assert_eq!(
        protocol.get("final").await.as_deref(),
        Some(payload.as_ref())
    );
    Ok(())
}

#[tokio::test]
async fn hdfs_metadata_observation_is_plan_scoped() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol
        .insert("source", Bytes::from_static(b"metadata"))
        .await;
    let storage = connect(protocol.clone(), test_identity("metadata")?)?;
    let metadata = storage.metadata(&PreflightPolicy::production())?;
    let omitted = metadata
        .observe(&StoragePath::new("source")?, ObservationPlan::default())
        .await?;
    assert!(matches!(
        omitted.timestamps(),
        crate::model::MetadataObservation::NotRequested
    ));
    assert_eq!(protocol.stat_calls(), 0);
    let requested = metadata
        .observe(
            &StoragePath::new("source")?,
            ObservationPlan::default().with_timestamps(ObservationMode::InlineOnly),
        )
        .await?;
    assert!(requested.timestamps().value().is_some());
    assert_eq!(protocol.stat_calls(), 1);
    assert!(matches!(
        requested.acl(),
        crate::model::MetadataObservation::NotRequested
    ));
    metadata
        .apply(
            &StoragePath::new("source")?,
            MetadataMutation::MappedOwnership(MappedOwnership::new("alice", "users", 0o640)?),
            CancellationToken::new(),
        )
        .await?;
    metadata
        .apply(
            &StoragePath::new("source")?,
            MetadataMutation::Timestamps(TimestampMetadata {
                accessed: Some(StorageTimestamp::new(
                    1_700_000_000_123_000_000,
                    TimePrecision::Milliseconds,
                )?),
                modified: None,
                created: None,
            }),
            CancellationToken::new(),
        )
        .await?;
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(
        metadata
            .apply(
                &StoragePath::new("source")?,
                MetadataMutation::MappedOwnership(MappedOwnership::new("nobody", "users", 0o600)?),
                cancel,
            )
            .await
            .is_err()
    );
    assert_eq!(protocol.metadata_calls().await.len(), 2);
    Ok(())
}

/// A rename over an existing file whose reply is lost after the `NameNode` committed it counts as
/// published: the stage is gone and the final file holds exactly the staged content. (Other
/// content there is not ours — `a_lost_publication_reply_with_other_content_is_not_published`.)
#[tokio::test]
async fn hdfs_publication_settles_a_lost_rename_reply_by_content() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let payload = Bytes::from_static(b"replacement");
    protocol.insert("source", payload.clone()).await;
    protocol
        .insert("final", Bytes::from_static(b"original"))
        .await;
    let (staged, descriptor) = staged_roles(&protocol, "policy").await?;
    let stage = staged
        .prepare_at_destination(at_destination(&descriptor, [3; 32])?)
        .await?;
    staged
        .write(&stage, Box::pin(stream::iter([Ok(payload.clone())])))
        .await?;
    protocol.fail_rename_after_commit();
    staged
        .publish(&stage, publish_request(&payload))
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(protocol.get("final").await, Some(payload));
    assert_eq!(protocol.len().await, 2);
    Ok(())
}
