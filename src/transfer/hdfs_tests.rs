use std::sync::Arc;

use bytes::Bytes;
use futures::stream;
use tokio_util::sync::CancellationToken;

use super::{InflightLimits, RecoveryPolicy, TransferIdentity, TransferRequest, transfer};
use crate::model::{
    FailureClass, MappedOwnership, ObservationMode, ObservationPlan, StoragePath, StorageTimestamp,
    TimePrecision, TimestampMetadata,
};
use crate::storage::backends::hdfs::contract_tests::MemoryHdfs;
use crate::storage::backends::hdfs::{connect, test_identity};
use crate::storage::{
    ExistingDestinationPolicy, FinalDestination, MetadataMutation, PreflightPolicy, PrepareRequest,
    PublicationDisposition, PublishRequest, RecoverRequest, RecoveryIdentity, Storage,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn request(source: Storage, destination: Storage) -> TestResult<TransferRequest> {
    Ok(TransferRequest::new(
        TransferIdentity::new("hdfs-role-contract")?,
        source,
        StoragePath::new("source")?,
        destination,
        StoragePath::new("final")?,
        InflightLimits::new(2, 64 * 1024, 2)?,
        CancellationToken::new(),
    ))
}

async fn prepared_stage(
    protocol: Arc<MemoryHdfs>,
    final_path: &str,
) -> TestResult<(
    Arc<dyn crate::storage::StagedDestination>,
    crate::storage::PreparedStage,
)> {
    let source = connect(protocol, test_identity("stage-source")?)?;
    let descriptor = source
        .read_source(&PreflightPolicy::production())?
        .describe(&StoragePath::new("source")?)
        .await?;
    let destination = connect(
        Arc::new(MemoryHdfs::default()),
        test_identity("stage-dest")?,
    )?;
    let staged = destination.staged_destination(&PreflightPolicy::production())?;
    let stage = staged
        .prepare(PrepareRequest {
            final_destination: FinalDestination::new(StoragePath::new(final_path)?),
            source: descriptor,
            recovery_binding: [8; 32],
        })
        .await?;
    Ok((staged, stage))
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
    assert!(failure.has_recoverable_stage());
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

#[tokio::test]
async fn ordinary_hdfs_stage_reports_restart_only_recovery_boundary() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol
        .insert("source", Bytes::from_static(b"restart"))
        .await;
    let (destination, stage) = prepared_stage(protocol, "final").await?;
    let error = destination
        .recovery_identity(&stage)
        .await
        .err()
        .ok_or("recovery succeeded")?;
    assert!(
        matches!(error, crate::storage::StorageRoleFailure::Entry(ref value)
        if value.class() == FailureClass::Unsupported)
    );
    let recovery = RecoveryIdentity::from_bytes(Bytes::from_static(b"hdfs-restart-only"))?;
    let source = crate::storage::SourceDescriptor::new(
        StoragePath::new("source")?,
        crate::model::EntryKind::File,
        Some(7),
        crate::model::SourceIdentity::new(
            test_identity("recovery-source")?,
            crate::model::IdentityStrength::PathScoped,
            b"source",
        )?,
    );
    assert!(
        destination
            .recover(RecoverRequest {
                identity: recovery,
                final_destination: FinalDestination::new(StoragePath::new("final")?),
                source,
                recovery_binding: [8; 32],
                claim_token: [1; 32],
            })
            .await
            .is_err()
    );
    destination.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn resume_or_restart_reuploads_when_hdfs_recovery_is_unsupported() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol
        .insert("source", Bytes::from_static(b"restart"))
        .await;
    let source = connect(protocol.clone(), test_identity("restart-source")?)?;
    let destination = connect(protocol.clone(), test_identity("restart-destination")?)?;
    let recovery = RecoveryIdentity::from_bytes(Bytes::from_static(b"unsupported-hdfs-state"))?;
    let outcome = transfer(
        request(source, destination)?
            .with_recovery(RecoveryPolicy::ResumeOrRestart, Some(recovery)),
    )
    .await?;
    assert_eq!(outcome.transferred_bytes, 7);
    assert_eq!(
        protocol.get("final").await.as_deref(),
        Some(b"restart".as_slice())
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

#[tokio::test]
async fn verify_or_skip_keeps_equivalent_hdfs_final() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let payload = Bytes::from_static(b"same");
    protocol.insert("source", payload.clone()).await;
    let source = connect(protocol.clone(), test_identity("publish-source")?)?;
    let destination = connect(protocol.clone(), test_identity("publish-dest")?)?;
    let descriptor = source
        .read_source(&PreflightPolicy::production())?
        .describe(&StoragePath::new("source")?)
        .await?;
    protocol.insert("final", payload.clone()).await;
    let staged = destination.staged_destination(&PreflightPolicy::production())?;
    let stage = staged
        .prepare(PrepareRequest {
            final_destination: FinalDestination::new(StoragePath::new("final")?),
            source: descriptor,
            recovery_binding: [9; 32],
        })
        .await?;
    staged
        .write(&stage, Box::pin(stream::iter([Ok(payload.clone())])))
        .await?;
    let evidence = staged
        .publish(
            &stage,
            PublishRequest {
                policy: ExistingDestinationPolicy::VerifyOrSkip,
                expected_size: payload.len() as u64,
                expected_blake3: *blake3::hash(&payload).as_bytes(),
                cancel: CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(
        evidence.disposition,
        PublicationDisposition::ExistingEquivalent
    );
    assert_eq!(protocol.get("final").await, Some(payload));
    Ok(())
}

#[tokio::test]
async fn hdfs_publication_reports_conflict_and_ambiguous_commit_truthfully() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let payload = Bytes::from_static(b"replacement");
    protocol.insert("source", payload.clone()).await;
    protocol
        .insert("final", Bytes::from_static(b"original"))
        .await;
    let source = connect(protocol.clone(), test_identity("policy-source")?)?;
    let destination = connect(protocol.clone(), test_identity("policy-destination")?)?;
    let descriptor = source
        .read_source(&PreflightPolicy::production())?
        .describe(&StoragePath::new("source")?)
        .await?;
    let staged = destination.staged_destination(&PreflightPolicy::production())?;
    let prepare = |binding| PrepareRequest {
        final_destination: FinalDestination::new(
            StoragePath::new("final").unwrap_or_else(|e| panic!("{e}")),
        ),
        source: descriptor.clone(),
        recovery_binding: binding,
    };
    let conflict_stage = staged.prepare(prepare([2; 32])).await?;
    staged
        .write(
            &conflict_stage,
            Box::pin(stream::iter([Ok(payload.clone())])),
        )
        .await?;
    let conflict = staged
        .publish(
            &conflict_stage,
            PublishRequest {
                policy: ExistingDestinationPolicy::FailIfExists,
                expected_size: payload.len() as u64,
                expected_blake3: *blake3::hash(&payload).as_bytes(),
                cancel: CancellationToken::new(),
            },
        )
        .await
        .err()
        .ok_or("FailIfExists unexpectedly published")?;
    assert!(!conflict.final_destination_changed);
    assert_eq!(
        protocol.get("final").await.as_deref(),
        Some(b"original".as_slice())
    );
    staged.discard(conflict_stage).await?;

    let ambiguous_stage = staged.prepare(prepare([3; 32])).await?;
    staged
        .write(
            &ambiguous_stage,
            Box::pin(stream::iter([Ok(payload.clone())])),
        )
        .await?;
    protocol.fail_rename_after_commit();
    let ambiguous = staged
        .publish(
            &ambiguous_stage,
            PublishRequest {
                policy: ExistingDestinationPolicy::Overwrite,
                expected_size: payload.len() as u64,
                expected_blake3: *blake3::hash(&payload).as_bytes(),
                cancel: CancellationToken::new(),
            },
        )
        .await
        .err()
        .ok_or("injected rename response failure succeeded")?;
    assert!(ambiguous.final_destination_changed);
    assert_eq!(protocol.get("final").await, Some(payload));
    Ok(())
}
