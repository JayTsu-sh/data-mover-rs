use super::*;

use data_mover::model::StoragePath;
use data_mover::storage::{
    DestinationPrepareRequest, FinalDestination, PrepareFact, PrepareRequest, ResumeMode,
    SourceDescriptor, Storage, connect_backend,
};
use data_mover::transfer::{TransferIdentity, TransferPolicy, TransferRequest, transfer};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_transfer_policies_and_chunk_boundaries() -> TestResult {
    let (raw, storage) = fixture("policy-runtime").await?;
    for size in [4096, 2 * 1024 * 1024 + 1, 64 * 1024 * 1024 + 1] {
        let content = bytes::Bytes::from(vec![73; size]);
        create_hdfs_file(&raw, "source", content.clone()).await?;
        for policy in [
            TransferPolicy::Checkpointed,
            TransferPolicy::AtomicReplace,
            TransferPolicy::Direct,
        ] {
            create_hdfs_file(&raw, "final", bytes::Bytes::from_static(b"old target")).await?;
            let request = TransferRequest::new(
                storage.clone(),
                StoragePath::new("source")?,
                storage.clone(),
                tokio_util::sync::CancellationToken::new(),
            )
            .with_final_path(StoragePath::new("final")?)
            .with_identity_override(TransferIdentity::from_label(format!(
                "lab-hdfs-{policy:?}-{size}"
            ))?)
            .with_transfer_policy(policy);
            let result = transfer(request).await?;
            assert_eq!(result.transferred_bytes, size as u64);
            assert_eq!(result.blake3, Some(*blake3::hash(&content).as_bytes()));
            assert_eq!(raw_read(&raw, "final").await?, content);
        }
    }
    raw.cleanup().await?;
    Ok(())
}

/// The at-destination prepare of `final` both connections make: same binding, same transfer.
fn resume_request(descriptor: &SourceDescriptor) -> TestResult<DestinationPrepareRequest> {
    Ok(DestinationPrepareRequest::new(
        PrepareRequest {
            final_destination: FinalDestination::new(StoragePath::new("final")?),
            source: descriptor.clone(),
            recovery_binding: [19; 32],
        },
        [21; 32],
    ))
}

/// Writes the first 2 MiB of a stage prepared at the destination, fails the input and drops the
/// stage, as a killed process would.
async fn interrupt_stage(
    storage: &Storage,
    content: &bytes::Bytes,
) -> TestResult<SourceDescriptor> {
    let descriptor = storage
        .read_source()?
        .describe(&StoragePath::new("source")?)
        .await?;
    let destination = storage.staged_destination()?;
    // A stage an earlier failed run left in this root is cleaned up, not resumed.
    let stage = destination
        .prepare_at_destination(resume_request(&descriptor)?.with_resume(ResumeMode::Restart))
        .await?;
    assert!(!matches!(stage.prepare_fact(), PrepareFact::Resumed { .. }));
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
    drop(stage);
    Ok(descriptor)
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_recovers_closed_hdfs_prefix() -> TestResult {
    let (raw, storage) = fixture("policy-runtime-recovery").await?;
    let content = bytes::Bytes::from(vec![91; 2 * 1024 * 1024 + 1]);
    create_hdfs_file(&raw, "source", content.clone()).await?;
    let descriptor = interrupt_stage(&storage, &content).await?;
    drop((raw, storage));

    // A fresh connection finds the stage beside the final file and resumes it.
    let (raw, storage) = fixture("policy-runtime-recovery").await?;
    let destination = storage.staged_destination()?;
    let resumed = destination
        .prepare_at_destination(resume_request(&descriptor)?)
        .await?;
    assert_eq!(
        resumed.prepare_fact(),
        PrepareFact::Resumed {
            bytes: 2 * 1024 * 1024
        }
    );
    destination
        .write(
            &resumed,
            Box::pin(futures::stream::iter([
                Ok(content.slice(2 * 1024 * 1024..)),
            ])),
        )
        .await?;
    destination
        .publish(
            &resumed,
            data_mover::storage::PublishRequest {
                expected_size: content.len() as u64,
                expected_blake3: Some(*blake3::hash(&content).as_bytes()),
                cancel: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await
        .map_err(|error| error.error)?;
    assert_eq!(raw_read(&raw, "final").await?, content);
    assert_no_artifacts(&raw).await?;
    raw.cleanup().await?;
    Ok(())
}

/// The raw listing of the run's storage root shows no transfer artifact.
async fn assert_no_artifacts(storage: &HdfsFixture) -> TestResult {
    let leftovers = storage
        .client
        .list_status(storage.location.root(), false)
        .await?
        .into_iter()
        .filter(|entry| {
            entry
                .path
                .rsplit('/')
                .next()
                .is_some_and(|name| name.starts_with(".data-mover-"))
        })
        .map(|entry| entry.path)
        .collect::<Vec<_>>();
    assert!(leftovers.is_empty(), "artifacts left: {leftovers:?}");
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

    let (raw, destination) = fixture("local-source-metadata-policies").await?;
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
            read_chunk_bytes: None,
            write_chunk_bytes: None,
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
                source.clone(),
                StoragePath::new("source")?,
                destination.clone(),
                tokio_util::sync::CancellationToken::new(),
            )
            .with_final_path(StoragePath::new(final_path.clone())?)
            .with_identity_override(TransferIdentity::from_label(format!(
                "hdfs-lab-local-metadata-{policy:?}"
            ))?)
            .with_transfer_policy(policy)
            .with_read_back_verification(ReadBackVerification::Enabled),
        )
        .await?;
        assert_eq!(
            outcome.blake3,
            Some(*blake3::hash(b"local metadata payload").as_bytes())
        );
        let copied = raw.client.get_file_info(&raw.path(&final_path)).await?;
        assert_eq!(copied.permission, 0o640);
        assert_eq!(copied.modification_time, 1_700_000_000_123);
        assert_eq!(copied.owner, hdfs_short_user(raw.location.user()));
    }

    raw.cleanup().await?;
    tokio::fs::remove_dir_all(local_root).await?;
    Ok(())
}
