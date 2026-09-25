use bytes::Bytes;
use data_mover::model::{
    BackendIdentity, BackendKind, EntryKind, IdentityStrength, SourceIdentity, SourceVersion,
    StoragePath,
};
use data_mover::storage::{
    DestinationPrepareRequest, FinalDestination, MetadataMutation, PreflightPolicy, PrepareFact,
    PrepareRequest, PublishRequest, ReadRequest, RestartReason, ResumeMode, SourceDescriptor,
    Storage, VerifyRequest,
};
use data_mover::transfer::{
    InflightLimits, PayloadShapingPolicy, SourceQosGroup, SourceQosPolicy, TransferIdentity,
    TransferRequest, transfer,
};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const PART_SIZE: usize = 8 * 1024 * 1024;
/// Over the 64 MiB automatic interval: only such a checkpointed object keeps a resume pointer
/// (ADR-0006 D3).
const PAYLOAD_SIZE: usize = PART_SIZE * 9 + 137;
/// The transfer identity every prepare in this contract records.
const TRANSFER_IDENTITY: [u8; 32] = [8; 32];

async fn connected(url: &str) -> TestResult<Storage> {
    data_mover::s3::S3Storage::new(url, None)
        .await?
        .architecture_storage()
}

#[tokio::test]
#[ignore = "requires the shared standard S3 lab"]
async fn standard_s3_architecture_roles_stage_publish_and_read_back() -> TestResult {
    let url = std::env::var("LAB_S3_ARCHITECTURE_URL")?;
    let path = StoragePath::new(std::env::var("LAB_S3_ARCHITECTURE_KEY")?)?;
    let identity = BackendIdentity::new(BackendKind::S3, "standard-s3-contract")?;
    let payload = Bytes::from(vec![0x5a; PAYLOAD_SIZE]);
    let (storage, stage) = stage_with_reconnect(&url, &path, &identity, &payload).await?;
    verify_publish_and_metadata(&storage, &stage, &path, &payload, true).await?;
    verify_range_and_cancellation(&storage, &path, &payload).await?;
    verify_native_and_shaped_fallback(&storage, &path, &payload).await?;
    verify_stale_upload_restart(&url, &path, &identity, payload.len()).await
}

#[tokio::test]
#[ignore = "requires the shared DXN S3 lab"]
async fn dxn_s3_architecture_roles_and_known_limits() -> TestResult {
    let url = std::env::var("LAB_DXN_S3_ARCHITECTURE_URL")?;
    let path = StoragePath::new(std::env::var("LAB_DXN_S3_ARCHITECTURE_KEY")?)?;
    let identity = BackendIdentity::new(BackendKind::S3, "dxn-s3-contract")?;
    let payload = Bytes::from(vec![0x6b; PAYLOAD_SIZE]);
    let (storage, stage) = stage_with_reconnect(&url, &path, &identity, &payload).await?;
    verify_publish_and_metadata(&storage, &stage, &path, &payload, false).await?;
    verify_range_and_cancellation(&storage, &path, &payload).await?;
    verify_native_and_shaped_fallback(&storage, &path, &payload).await?;
    verify_stale_upload_restart(&url, &path, &identity, payload.len()).await
}

async fn verify_native_and_shaped_fallback(
    storage: &Storage,
    source: &StoragePath,
    payload: &Bytes,
) -> TestResult {
    let native_path = StoragePath::new(format!("{}.native", source.as_str()))?;
    let request = transfer_request(storage, source, native_path, "native")?;
    let qos = SourceQosGroup::new(SourceQosPolicy::new(None, u64::try_from(PART_SIZE)?, None)?);
    let native = transfer(request.with_source_qos(qos)).await?;
    assert_eq!(native.blake3, Some(*blake3::hash(payload).as_bytes()));
    assert_eq!(native.source_qos.native_bytes, payload.len() as u64);
    assert_eq!(native.source_qos.native_requests, 1);
    assert!(!native.source_qos.native_payload_shaped);

    let shaped_path = StoragePath::new(format!("{}.shaped", source.as_str()))?;
    let shaped = transfer(
        transfer_request(storage, source, shaped_path, "shaped")?
            .with_payload_shaping(PayloadShapingPolicy::RequireClientShaped),
    )
    .await?;
    assert_eq!(shaped.source_qos.native_requests, 0);
    Ok(())
}

fn transfer_request(
    storage: &Storage,
    source: &StoragePath,
    final_path: StoragePath,
    identity: &str,
) -> TestResult<TransferRequest> {
    Ok(TransferRequest::new(
        storage.clone(),
        source.clone(),
        storage.clone(),
        final_path,
        InflightLimits::new(4, PART_SIZE, 4)?,
        CancellationToken::new(),
    )
    .with_identity_override(TransferIdentity::from_label(format!(
        "s3-contract-{identity}"
    ))?))
}

/// A recoverable prepare kept at the destination (ADR-0006 C15c), resuming or cleaning up in place.
fn prepare_request(
    path: &StoragePath,
    source: &SourceDescriptor,
    binding: [u8; 32],
    resume: ResumeMode,
) -> DestinationPrepareRequest {
    DestinationPrepareRequest::new(
        PrepareRequest {
            final_destination: FinalDestination::new(path.clone()),
            source: source.clone(),
            recovery_binding: binding,
        },
        TRANSFER_IDENTITY,
    )
    .with_resume(resume)
    .with_recoverable(true)
}

/// What a later prepare finds at the destination, in fresh connections: another binding's
/// leftovers are cleaned up (`BindingChanged`), a requested restart cleans up its own
/// (`Requested`), and after a discard nothing is left (`Fresh`).
async fn verify_stale_upload_restart(
    url: &str,
    path: &StoragePath,
    identity: &BackendIdentity,
    size: usize,
) -> TestResult {
    let restart_path = StoragePath::new(format!("{}.restart", path.as_str()))?;
    let source = source_descriptor(identity, size)?;
    let request = |binding, resume| prepare_request(&restart_path, &source, binding, resume);
    let policy = PreflightPolicy::production();
    let destination = connected(url).await?.staged_destination(&policy)?;
    // Whatever a crashed earlier run left on the key goes first.
    let earlier = destination
        .prepare_at_destination(request([9; 32], ResumeMode::Restart))
        .await?;
    destination.discard(earlier).await?;
    let stale = destination
        .prepare_at_destination(request([9; 32], ResumeMode::Discover))
        .await?;
    assert_eq!(stale.prepare_fact(), PrepareFact::Fresh);
    drop(stale);
    let destination = connected(url).await?.staged_destination(&policy)?;
    let changed = destination
        .prepare_at_destination(request([10; 32], ResumeMode::Discover))
        .await?;
    assert_eq!(
        changed.prepare_fact(),
        PrepareFact::Restarted {
            reason: RestartReason::BindingChanged
        }
    );
    drop(changed);
    let restarted = destination
        .prepare_at_destination(request([10; 32], ResumeMode::Restart))
        .await?;
    assert_eq!(
        restarted.prepare_fact(),
        PrepareFact::Restarted {
            reason: RestartReason::Requested
        }
    );
    destination.discard(restarted).await?;
    let fresh = destination
        .prepare_at_destination(request([10; 32], ResumeMode::Discover))
        .await?;
    assert_eq!(fresh.prepare_fact(), PrepareFact::Fresh);
    destination.discard(fresh).await?;
    Ok(())
}

/// A multipart upload on the final key cut after four parts, resumed through a fresh connection
/// from the parts the service lists (its `.upload` pointer names the upload), then written to
/// the end — not yet published.
async fn stage_with_reconnect(
    url: &str,
    path: &StoragePath,
    identity: &BackendIdentity,
    payload: &Bytes,
) -> TestResult<(Storage, data_mover::storage::PreparedStage)> {
    eprintln!("S3 contract stage: prepare an upload on the final key");
    let policy = PreflightPolicy::production();
    let storage = connected(url).await?;
    let destination = storage.staged_destination(&policy)?;
    let source = source_descriptor(identity, payload.len())?;
    let request = || prepare_request(path, &source, [3; 32], ResumeMode::Discover);
    let stage = destination.prepare_at_destination(request()).await?;
    assert_eq!(stage.prepare_fact(), PrepareFact::Fresh);
    assert!(
        destination
            .write(&stage, interrupted_input(payload, &source)?)
            .await
            .is_err()
    );
    drop(stage);
    let storage = connected(url).await?;
    let destination = storage.staged_destination(&policy)?;
    eprintln!("S3 contract stage: resume the upload after reconnect");
    let resumed = destination.prepare_at_destination(request()).await?;
    // A failed input waits for the parts in flight: all four are listed.
    let prefix = PART_SIZE * 4;
    assert_eq!(
        resumed.prepare_fact(),
        PrepareFact::Resumed {
            bytes: prefix as u64
        }
    );
    destination
        .write(
            &resumed,
            Box::pin(futures::stream::iter([Ok(payload.slice(prefix..))])),
        )
        .await?;
    Ok((storage, resumed))
}

fn source_descriptor(identity: &BackendIdentity, size: usize) -> TestResult<SourceDescriptor> {
    Ok(SourceDescriptor::new(
        StoragePath::new("generated-source")?,
        EntryKind::File,
        Some(size as u64),
        SourceIdentity::new(
            identity.clone(),
            IdentityStrength::PathScoped,
            b"generated-source-v1",
        )?,
    ))
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

async fn verify_publish_and_metadata(
    storage: &Storage,
    stage: &data_mover::storage::PreparedStage,
    path: &StoragePath,
    payload: &Bytes,
    supports_tags: bool,
) -> TestResult {
    let policy = PreflightPolicy::production();
    let destination = storage.staged_destination(&policy)?;
    let digest = *blake3::hash(payload).as_bytes();
    // An upload on the final key is read back after publication (`VerificationPoint::AfterPublish`).
    let published = destination
        .publish(
            stage,
            PublishRequest {
                expected_size: payload.len() as u64,
                expected_blake3: Some(digest),
                cancel: CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    destination
        .verify(
            stage,
            VerifyRequest {
                expected_size: payload.len() as u64,
                expected_blake3: digest,
                cancel: CancellationToken::new(),
                published: Some(published),
            },
        )
        .await?;
    let tag = data_mover::model::ObjectTag::new("contract", "standard-s3")?;
    let metadata = storage.metadata(&policy)?;
    if !supports_tags {
        let observed = metadata
            .observe(
                path,
                data_mover::model::ObservationPlan::default()
                    .with_tags(data_mover::model::ObservationMode::Required),
            )
            .await?;
        assert!(matches!(
            observed.tags(),
            data_mover::model::MetadataObservation::Unsupported
        ));
        let result = metadata
            .apply(
                path,
                MetadataMutation::Tags(vec![tag]),
                CancellationToken::new(),
            )
            .await;
        assert!(
            matches!(result, Err(data_mover::storage::StorageRoleFailure::Entry(ref failure))
            if failure.class() == data_mover::model::FailureClass::Unsupported)
        );
        return Ok(());
    }
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
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            read_budget: None,
            cancel: CancellationToken::new(),
            source_qos: None,
            version: SourceVersion::Current,
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
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            read_budget: None,
            cancel,
            source_qos: None,
            version: SourceVersion::Current,
        })
        .await;
    assert!(matches!(
        result,
        Err(data_mover::storage::StorageRoleFailure::Entry(_))
    ));
    Ok(())
}
