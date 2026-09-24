use super::*;
use data_mover::model::StoragePath;
use data_mover::storage::{HdfsBackendConfig, Storage, connect_backend};
use data_mover::transfer::{
    InflightLimits, TransferIdentity, TransferPolicy, TransferRequest, transfer,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn fixture(case: &str) -> TestResult<(data_mover::HDFSStorage, Storage)> {
    let location = hdfs_lab_location(case)?;
    let legacy = create_storage(
        &location,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    let StorageEnum::HDFS(legacy) = legacy else {
        return Err("expected HDFS fixture".into());
    };
    let storage = connect_backend(data_mover::storage::BackendConfig::Hdfs(
        HdfsBackendConfig {
            location: location.clone(),
            client: hdfs_lab_config(),
            block_size: None,
            ensure_dir: true,
        },
    ))
    .await?;
    Ok((legacy, storage))
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_transfer_policies_and_chunk_boundaries() -> TestResult {
    let (legacy, storage) = fixture("policy-runtime").await?;
    for size in [4096, 2 * 1024 * 1024 + 1, 64 * 1024 * 1024 + 1] {
        let content = bytes::Bytes::from(vec![73; size]);
        create_hdfs_file(&legacy, "source", content.clone()).await?;
        for policy in [
            TransferPolicy::Checkpointed,
            TransferPolicy::AtomicReplace,
            TransferPolicy::Direct,
        ] {
            create_hdfs_file(&legacy, "final", bytes::Bytes::from_static(b"old target")).await?;
            let request = TransferRequest::new(
                TransferIdentity::new(format!("lab-hdfs-{policy:?}-{size}"))?,
                storage.clone(),
                StoragePath::new("source")?,
                storage.clone(),
                StoragePath::new("final")?,
                InflightLimits::new(8, 16 * 1024 * 1024, 8)?,
                tokio_util::sync::CancellationToken::new(),
            )
            .with_transfer_policy(policy);
            let result = transfer(request).await?;
            assert_eq!(result.transferred_bytes, size as u64);
            assert_eq!(result.blake3, Some(*blake3::hash(&content).as_bytes()));
            let file = legacy.open_file(std::path::Path::new("final")).await?;
            assert_eq!(legacy.read_at(&file, 0, size as u64).await?, content);
        }
    }
    legacy.delete_storage_root().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_recovers_closed_hdfs_prefix() -> TestResult {
    use data_mover::storage::{FinalDestination, PreflightPolicy, PrepareRequest, RecoverRequest};
    let (legacy, storage) = fixture("policy-runtime-recovery").await?;
    let content = bytes::Bytes::from(vec![91; 2 * 1024 * 1024 + 1]);
    create_hdfs_file(&legacy, "source", content.clone()).await?;
    let descriptor = storage
        .read_source(&PreflightPolicy::production())?
        .describe(&StoragePath::new("source")?)
        .await?;
    let destination = storage.staged_destination(&PreflightPolicy::production())?;
    let final_destination = FinalDestination::new(StoragePath::new("final")?);
    let stage = destination
        .prepare(PrepareRequest {
            final_destination: final_destination.clone(),
            source: descriptor.clone(),
            recovery_binding: [19; 32],
        })
        .await?;
    let error = data_mover::storage::StorageRoleFailure::Entry(
        data_mover::model::EntryOperationFailure::new(
            StoragePath::new("source")?,
            data_mover::model::Operation::Read,
            data_mover::model::FailureClass::Cancelled,
            data_mover::model::Transience::Transient,
            "test cancellation",
        )?,
    );
    let input = Box::pin(futures::stream::iter([
        Ok(content.slice(..2 * 1024 * 1024)),
        Err(error),
    ]));
    assert!(destination.write(&stage, input).await.is_err());
    assert_eq!(
        destination.observe_checkpoint(&stage).await?.durable_prefix,
        2 * 1024 * 1024
    );
    let identity = destination.recovery_identity(&stage).await?;
    drop(stage);
    let recovered = destination
        .recover(RecoverRequest {
            identity,
            final_destination,
            source: descriptor,
            recovery_binding: [19; 32],
            claim_token: [21; 32],
        })
        .await?;
    destination
        .write(
            &recovered,
            Box::pin(futures::stream::iter([
                Ok(content.slice(2 * 1024 * 1024..)),
            ])),
        )
        .await?;
    destination
        .publish(
            &recovered,
            data_mover::storage::PublishRequest {
                expected_size: content.len() as u64,
                expected_blake3: Some(*blake3::hash(&content).as_bytes()),
                cancel: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await
        .map_err(|error| error.error)?;
    let file = legacy.open_file(std::path::Path::new("final")).await?;
    assert_eq!(
        legacy.read_at(&file, 0, content.len() as u64).await?,
        content
    );
    destination.discard(recovered).await?;
    legacy.delete_storage_root().await?;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_local_source_metadata_to_hdfs_policies() -> TestResult {
    use std::num::NonZeroUsize;
    use std::os::unix::fs::PermissionsExt as _;

    use data_mover::storage::LocalBackendConfig;
    use data_mover::transfer::ReadBackVerification;

    let (legacy, destination) = fixture("local-source-metadata-policies").await?;
    let local_root = hdfs_lab_local_path("local-source-metadata-policies")?;
    tokio::fs::create_dir_all(&local_root).await?;
    let source_path = local_root.join("source");
    tokio::fs::write(&source_path, b"local metadata payload").await?;
    tokio::fs::set_permissions(&source_path, std::fs::Permissions::from_mode(0o640)).await?;
    filetime::set_file_mtime(
        &source_path,
        filetime::FileTime::from_unix_time(1_700_000_000, 123_000_000),
    )?;
    let source = connect_backend(data_mover::storage::BackendConfig::Local(
        LocalBackendConfig {
            root: local_root.clone(),
            read_concurrency: NonZeroUsize::new(2).ok_or("invalid read concurrency")?,
            write_concurrency: NonZeroUsize::new(2).ok_or("invalid write concurrency")?,
        },
    ))
    .await?;

    for policy in [
        TransferPolicy::Checkpointed,
        TransferPolicy::AtomicReplace,
        TransferPolicy::Direct,
    ] {
        let final_path = format!("final-{policy:?}").to_ascii_lowercase();
        let outcome = transfer(
            TransferRequest::new(
                TransferIdentity::new(format!("hdfs-lab-local-metadata-{policy:?}"))?,
                source.clone(),
                StoragePath::new("source")?,
                destination.clone(),
                StoragePath::new(final_path.clone())?,
                InflightLimits::new(2, 128, 2)?,
                tokio_util::sync::CancellationToken::new(),
            )
            .with_transfer_policy(policy)
            .with_read_back_verification(ReadBackVerification::Enabled),
        )
        .await?;
        assert_eq!(
            outcome.blake3,
            Some(*blake3::hash(b"local metadata payload").as_bytes())
        );
        let copied = legacy
            .get_metadata(std::path::Path::new(&final_path))
            .await?;
        assert_eq!(copied.mode, 0o640);
        assert_eq!(copied.mtime, 1_700_000_000_123_000_000);
        assert_eq!(copied.owner, hdfs_short_user(legacy.location().user()));
    }

    legacy.delete_storage_root().await?;
    tokio::fs::remove_dir_all(local_root).await?;
    Ok(())
}
