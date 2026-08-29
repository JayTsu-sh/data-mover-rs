use bytes::Bytes;
use data_mover::model::{
    BackendIdentity, BackendKind, EntryKind, IdentityStrength, SourceIdentity, StoragePath,
};
use data_mover::storage::{
    ExistingDestinationPolicy, FinalDestination, MetadataMutation, PreflightPolicy, PrepareRequest,
    PublishRequest, ReadRequest, RecoverRequest, SourceDescriptor, Storage, VerifyRequest,
};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const PART_SIZE: usize = 8 * 1024 * 1024;

async fn connected(url: &str, identity: BackendIdentity) -> TestResult<Storage> {
    data_mover::s3::S3Storage::new(url, None)
        .await?
        .architecture_storage(identity)
}

#[tokio::test]
#[ignore = "requires the shared standard S3 lab"]
async fn standard_s3_architecture_roles_stage_publish_and_read_back() -> TestResult {
    let url = std::env::var("LAB_S3_ARCHITECTURE_URL")?;
    let path = StoragePath::new(std::env::var("LAB_S3_ARCHITECTURE_KEY")?)?;
    let identity = BackendIdentity::new(BackendKind::S3, "standard-s3-contract")?;
    let payload = Bytes::from(vec![0x5a; PART_SIZE * 5 + 137]);
    let (storage, stage) = stage_with_reconnect(&url, &path, &identity, &payload).await?;
    verify_publish_and_tag(&storage, &stage, &path, &payload).await?;
    verify_range_and_cancellation(&storage, &path, &payload).await?;
    verify_stale_upload_restart(&url, &path, &identity, payload.len()).await
}

async fn verify_stale_upload_restart(
    url: &str,
    path: &StoragePath,
    identity: &BackendIdentity,
    size: usize,
) -> TestResult {
    let restart_path = StoragePath::new(format!("{}.restart", path.as_str()))?;
    let source = source_descriptor(identity, size)?;
    let prepare = PrepareRequest {
        final_destination: FinalDestination::new(restart_path),
        source: source.clone(),
        recovery_binding: [9; 32],
    };
    let storage = connected(url, identity.clone()).await?;
    let destination = storage.staged_destination(&PreflightPolicy::production())?;
    let stale = destination.prepare(prepare.clone()).await?;
    let recovery = destination.recovery_identity(&stale).await?;
    destination.discard(stale).await?;
    let storage = connected(url, identity.clone()).await?;
    let destination = storage.staged_destination(&PreflightPolicy::production())?;
    let result = destination
        .recover(RecoverRequest {
            identity: recovery,
            final_destination: prepare.final_destination.clone(),
            source,
            recovery_binding: prepare.recovery_binding,
            claim_token: [8; 32],
        })
        .await;
    assert!(
        matches!(result, Err(data_mover::storage::StorageRoleFailure::Entry(ref failure))
        if failure.class() == data_mover::model::FailureClass::NotFound)
    );
    let fresh = destination.prepare(prepare).await?;
    destination.discard(fresh).await?;
    Ok(())
}

async fn stage_with_reconnect(
    url: &str,
    path: &StoragePath,
    identity: &BackendIdentity,
    payload: &Bytes,
) -> TestResult<(Storage, data_mover::storage::PreparedStage)> {
    let storage = connected(url, identity.clone()).await?;
    let destination = storage.staged_destination(&PreflightPolicy::production())?;
    let source = source_descriptor(identity, payload.len())?;
    let prepare = PrepareRequest {
        final_destination: FinalDestination::new(path.clone()),
        source: source.clone(),
        recovery_binding: [3; 32],
    };
    let stage = destination.prepare(prepare.clone()).await?;
    let recovery = destination.recovery_identity(&stage).await?;
    assert!(
        destination
            .write(&stage, interrupted_input(payload, &source)?)
            .await
            .is_err()
    );
    let checkpoint = destination.observe_checkpoint(&stage).await?.durable_prefix;
    assert!((PART_SIZE as u64..=(PART_SIZE * 4) as u64).contains(&checkpoint));
    assert_eq!(checkpoint % PART_SIZE as u64, 0);
    let storage = connected(url, identity.clone()).await?;
    let destination = storage.staged_destination(&PreflightPolicy::production())?;
    let resumed = destination
        .recover(RecoverRequest {
            identity: recovery,
            final_destination: prepare.final_destination,
            source,
            recovery_binding: prepare.recovery_binding,
            claim_token: [4; 32],
        })
        .await?;
    destination
        .write(
            &resumed,
            Box::pin(futures::stream::iter([Ok(
                payload.slice(usize::try_from(checkpoint)?..)
            )])),
        )
        .await?;
    Ok((storage, resumed))
}

fn source_descriptor(identity: &BackendIdentity, size: usize) -> TestResult<SourceDescriptor> {
    Ok(SourceDescriptor {
        path: StoragePath::new("generated-source")?,
        kind: EntryKind::File,
        size: Some(size as u64),
        source_identity: SourceIdentity::new(
            identity.clone(),
            IdentityStrength::PathScoped,
            b"generated-source-v1",
        )?,
    })
}

fn interrupted_input(
    payload: &Bytes,
    source: &SourceDescriptor,
) -> TestResult<data_mover::storage::ByteStream> {
    let interrupted = data_mover::model::EntryOperationFailure::new(
        source.path.clone(),
        data_mover::model::Operation::Read,
        data_mover::model::FailureClass::Connectivity,
        data_mover::model::Transience::Transient,
        "injected interruption",
    )?;
    Ok(Box::pin(futures::stream::iter([
        Ok(payload.slice(..PART_SIZE)),
        Ok(payload.slice(PART_SIZE..PART_SIZE * 2)),
        Ok(payload.slice(PART_SIZE * 2..PART_SIZE * 3)),
        Ok(payload.slice(PART_SIZE * 3..PART_SIZE * 4)),
        Err(data_mover::storage::StorageRoleFailure::Entry(interrupted)),
    ])))
}

async fn verify_publish_and_tag(
    storage: &Storage,
    stage: &data_mover::storage::PreparedStage,
    path: &StoragePath,
    payload: &Bytes,
) -> TestResult {
    let policy = PreflightPolicy::production();
    let destination = storage.staged_destination(&policy)?;
    let digest = *blake3::hash(payload).as_bytes();
    destination
        .verify(
            stage,
            VerifyRequest {
                expected_size: payload.len() as u64,
                expected_blake3: digest,
                cancel: CancellationToken::new(),
            },
        )
        .await?;
    destination
        .publish(
            stage,
            PublishRequest {
                policy: ExistingDestinationPolicy::Overwrite,
                expected_size: payload.len() as u64,
                expected_blake3: digest,
                cancel: CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    let tag = data_mover::model::ObjectTag::new("contract", "standard-s3")?;
    let metadata = storage.metadata(&policy)?;
    metadata
        .apply(
            path,
            MetadataMutation::Tags(vec![tag.clone()]),
            CancellationToken::new(),
        )
        .await?;
    let observed = metadata
        .observe(
            path,
            data_mover::model::ObservationPlan::default()
                .with_tags(data_mover::model::ObservationMode::Required),
        )
        .await?;
    assert_eq!(observed.tags().value(), Some(&vec![tag]));
    Ok(())
}

async fn verify_range_and_cancellation(
    storage: &Storage,
    path: &StoragePath,
    payload: &Bytes,
) -> TestResult {
    let reader = storage.read_source(&PreflightPolicy::production())?;
    let observed = reader.describe(path).await?;
    let mut stream = reader
        .read(ReadRequest {
            path: path.clone(),
            range: Some(13..PART_SIZE as u64 + 21),
            expected_source: Some(observed.source_identity.clone()),
            cancel: CancellationToken::new(),
            source_qos: None,
        })
        .await?;
    let mut actual = Vec::new();
    while let Some(chunk) = futures::StreamExt::next(&mut stream).await {
        actual.extend_from_slice(&chunk?);
    }
    assert_eq!(actual, payload.slice(13..PART_SIZE + 21));
    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = reader
        .read(ReadRequest {
            path: path.clone(),
            range: None,
            expected_source: Some(observed.source_identity),
            cancel,
            source_qos: None,
        })
        .await;
    assert!(matches!(
        result,
        Err(data_mover::storage::StorageRoleFailure::Entry(_))
    ));
    Ok(())
}
