use std::sync::Arc;

use bytes::Bytes;
use futures::stream;
use tokio_util::sync::CancellationToken;

use super::{
    InflightLimits, RecoveryContext, RecoveryProvider, RecoveryRegistrar,
    RecoveryRegistrationFailure, Resumability, TransferIdentity, TransferRequest, transfer,
};
use crate::model::{
    FailureClass, MappedOwnership, ObservationMode, ObservationPlan, Operation, StoragePath,
    StorageTimestamp, TimePrecision, TimestampMetadata,
};
use crate::storage::backends::hdfs::contract_tests::MemoryHdfs;
use crate::storage::backends::hdfs::protocol::cancelled;
use crate::storage::backends::hdfs::{connect, test_identity};
use crate::storage::{
    ExistingDestinationPolicy, FinalDestination, MetadataMutation, PreflightPolicy, PrepareRequest,
    PublicationDisposition, PublishRequest, RecoverRequest, RecoveryIdentity, Storage,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct AcceptingRecoveryRegistrar;

#[async_trait::async_trait]
impl RecoveryRegistrar for AcceptingRecoveryRegistrar {
    async fn register(
        &self,
        _identity: RecoveryIdentity,
    ) -> Result<(), RecoveryRegistrationFailure> {
        Ok(())
    }
}

struct HdfsRecoveryProvider {
    identity: Option<RecoveryIdentity>,
}

#[async_trait::async_trait]
impl RecoveryProvider for HdfsRecoveryProvider {
    async fn open(&self) -> Result<RecoveryContext, RecoveryRegistrationFailure> {
        Ok(RecoveryContext::new(
            self.identity.clone(),
            [7; 32],
            Arc::new(AcceptingRecoveryRegistrar),
        ))
    }
}

fn recoverable_request(
    request: TransferRequest,
    identity: Option<RecoveryIdentity>,
) -> TransferRequest {
    request.with_recovery(
        Resumability::Enabled,
        Some(Arc::new(HdfsRecoveryProvider { identity })),
    )
}

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

#[tokio::test]
async fn hdfs_stage_exports_and_recovers_an_observed_prefix() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol
        .insert("source", Bytes::from_static(b"restart"))
        .await;
    let (destination, stage) = prepared_stage(protocol, "final").await?;
    let recovery = destination.recovery_identity(&stage).await?;
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
    let recover = || RecoverRequest {
        identity: recovery.clone(),
        final_destination: FinalDestination::new(
            StoragePath::new("final").unwrap_or_else(|error| panic!("{error}")),
        ),
        source: source.clone(),
        recovery_binding: [8; 32],
        claim_token: [1; 32],
    };
    let recovered = destination.recover(recover()).await?;
    assert_eq!(
        destination
            .observe_checkpoint(&recovered)
            .await?
            .durable_prefix,
        0
    );
    let reentered = destination.recover(recover()).await?;
    assert_eq!(
        destination
            .observe_checkpoint(&reentered)
            .await?
            .durable_prefix,
        0
    );
    destination.discard(recovered).await?;
    Ok(())
}

#[tokio::test]
async fn interrupted_hdfs_stage_recovers_only_the_durable_tail() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol
        .insert("source", Bytes::from_static(b"abcdef"))
        .await;
    let source = connect(protocol.clone(), test_identity("partial-source")?)?;
    let destination = connect(protocol.clone(), test_identity("partial-destination")?)?;
    let descriptor = source
        .read_source(&PreflightPolicy::production())?
        .describe(&StoragePath::new("source")?)
        .await?;
    let staged = destination.staged_destination(&PreflightPolicy::production())?;
    let stage = staged
        .prepare(PrepareRequest {
            final_destination: FinalDestination::new(StoragePath::new("final")?),
            source: descriptor.clone(),
            recovery_binding: [4; 32],
        })
        .await?;
    let interrupted = Box::pin(stream::iter([
        Ok(Bytes::from_static(b"abc")),
        Err(cancelled(&StoragePath::new("source")?, Operation::Read)),
    ]));
    assert!(staged.write(&stage, interrupted).await.is_err());
    assert_eq!(staged.observe_checkpoint(&stage).await?.durable_prefix, 3);
    let identity = staged.recovery_identity(&stage).await?;
    let recovered = staged
        .recover(RecoverRequest {
            identity,
            final_destination: FinalDestination::new(StoragePath::new("final")?),
            source: descriptor,
            recovery_binding: [4; 32],
            claim_token: [5; 32],
        })
        .await?;
    let evidence = staged
        .write(
            &recovered,
            Box::pin(stream::iter([Ok(Bytes::from_static(b"def"))])),
        )
        .await?;
    assert_eq!(evidence.persisted_bytes, 6);
    assert_eq!(
        staged.observe_checkpoint(&recovered).await?.durable_prefix,
        6
    );
    staged.discard(recovered).await?;
    Ok(())
}

#[tokio::test]
async fn hdfs_recovery_rejects_tampering_and_competing_claims_without_mutation() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol
        .insert("source", Bytes::from_static(b"claim"))
        .await;
    let source = connect(protocol.clone(), test_identity("claim-source")?)?;
    let destination = connect(protocol.clone(), test_identity("claim-destination")?)?;
    let descriptor = source
        .read_source(&PreflightPolicy::production())?
        .describe(&StoragePath::new("source")?)
        .await?;
    let staged = destination.staged_destination(&PreflightPolicy::production())?;
    let prepare = PrepareRequest {
        final_destination: FinalDestination::new(StoragePath::new("final")?),
        source: descriptor.clone(),
        recovery_binding: [6; 32],
    };
    let stage = staged.prepare(prepare.clone()).await?;
    let identity = staged.recovery_identity(&stage).await?;
    let mut bytes = identity.as_bytes().to_vec();
    let last = bytes
        .len()
        .checked_sub(1)
        .ok_or("empty recovery identity")?;
    bytes[last] ^= 1;
    let tampered = RecoveryIdentity::from_bytes(bytes)?;
    let recover = |identity, claim_token| RecoverRequest {
        identity,
        final_destination: prepare.final_destination.clone(),
        source: descriptor.clone(),
        recovery_binding: prepare.recovery_binding,
        claim_token,
    };
    assert!(staged.recover(recover(tampered, [1; 32])).await.is_err());
    assert_eq!(protocol.len().await, 2);
    let (first, second) = tokio::join!(
        staged.recover(recover(identity.clone(), [2; 32])),
        staged.recover(recover(identity, [3; 32]))
    );
    let ((Ok(winner), Err(loser)) | (Err(loser), Ok(winner))) = (first, second) else {
        return Err("claim race did not produce exactly one winner".into());
    };
    assert!(
        matches!(loser, crate::storage::StorageRoleFailure::Entry(error)
        if error.class() == FailureClass::Conflict)
    );
    assert_eq!(protocol.len().await, 2);
    staged.discard(winner).await?;
    Ok(())
}

#[tokio::test]
async fn invalid_identity_preserves_unknown_hdfs_stage_without_reupload() -> TestResult {
    let protocol = Arc::new(MemoryHdfs::default());
    let payload = Bytes::from(vec![0x72; 128 * 1024]);
    protocol.insert("source", payload).await;
    let source = connect(protocol.clone(), test_identity("restart-source")?)?;
    let destination = connect(protocol.clone(), test_identity("restart-destination")?)?;
    let descriptor = source
        .read_source(&PreflightPolicy::production())?
        .describe(&StoragePath::new("source")?)
        .await?;
    let staged = destination.staged_destination(&PreflightPolicy::production())?;
    let unknown = staged
        .prepare(PrepareRequest {
            final_destination: FinalDestination::new(StoragePath::new("final")?),
            source: descriptor,
            recovery_binding: [7; 32],
        })
        .await?;
    let recovery = staged.recovery_identity(&unknown).await?;
    let mut bytes = recovery.as_bytes().to_vec();
    let last = bytes
        .len()
        .checked_sub(1)
        .ok_or("empty recovery identity")?;
    bytes[last] ^= 1;
    let recovery = RecoveryIdentity::from_bytes(bytes)?;
    let result = transfer(recoverable_request(
        request(source, destination)?,
        Some(recovery),
    ))
    .await;
    assert!(result.is_err());
    assert!(protocol.get("final").await.is_none());
    assert_eq!(protocol.len().await, 2);
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
    let identity = failure
        .into_recovery_identity()
        .await
        .map_err(|(_, error)| error)?;
    protocol.allow_writes();
    let source = connect(protocol.clone(), test_identity("require-source")?)?;
    let destination = connect(protocol.clone(), test_identity("require-destination")?)?;
    let outcome = transfer(recoverable_request(
        request(source, destination)?,
        Some(identity),
    ))
    .await?;
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
