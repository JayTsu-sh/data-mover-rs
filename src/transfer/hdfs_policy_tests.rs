use std::sync::Arc;
#[cfg(unix)]
use std::{num::NonZeroUsize, os::unix::fs::PermissionsExt as _};

use bytes::Bytes;
use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

use crate::model::StoragePath;
use crate::storage::backends::hdfs::{connect, contract_tests::MemoryHdfs, test_identity};
use crate::storage::{InflightConfig, InflightRuntime, PreflightPolicy, ReadBudget, ReadRequest};
use crate::transfer::{
    InflightLimits, ReadBackVerification, TransferIdentity, TransferPolicy, TransferRequest,
    transfer,
};

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

#[cfg(unix)]
#[tokio::test]
async fn local_metadata_copies_mode_and_mtime_to_hdfs_without_replacing_principals() -> Result {
    let root = tempfile::tempdir()?;
    let path = root.path().join("source");
    std::fs::write(&path, b"payload")?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640))?;
    filetime::set_file_mtime(
        &path,
        filetime::FileTime::from_unix_time(1_700_000_000, 123_000_000),
    )?;
    let source = crate::storage::connect_backend(crate::storage::BackendConfig::Local(
        crate::storage::LocalBackendConfig {
            root: root.path().into(),
            identity: crate::storage::backends::local::test_identity("hdfs-metadata-source"),
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
        let protocol = Arc::new(MemoryHdfs::default());
        let destination = connect(
            protocol.clone(),
            test_identity(&format!("hdfs-metadata-{policy:?}"))?,
        )?;
        transfer(
            TransferRequest::new(
                TransferIdentity::new(format!("hdfs-metadata-{policy:?}"))?,
                source.clone(),
                StoragePath::new("source")?,
                destination,
                StoragePath::new("final")?,
                InflightLimits::new(2, 128, 2)?,
                CancellationToken::new(),
            )
            .with_transfer_policy(policy)
            .with_read_back_verification(ReadBackVerification::Disabled),
        )
        .await?;
        let calls = protocol.metadata_calls().await;
        assert!(
            calls
                .iter()
                .any(|call| call.starts_with("mode:") && call.ends_with(":640")),
            "{policy:?} did not copy mode: {calls:?}"
        );
        assert!(
            calls.iter().any(|call| call.starts_with("timestamps:")
                && call.ends_with(":None:Some(1700000000123000000)")),
            "{policy:?} did not copy mtime: {calls:?}"
        );
        assert!(
            calls.iter().all(|call| !call.starts_with("ownership:")),
            "{policy:?} replaced HDFS principals: {calls:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn direct_failure_never_exposes_stage_cleanup() -> Result {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol.insert("source", Bytes::from_static(b"new")).await;
    protocol.insert("final", Bytes::from_static(b"old")).await;
    protocol.fail_writes();
    let storage = connect(protocol.clone(), test_identity("direct-failure")?)?;
    let request = TransferRequest::new(
        TransferIdentity::new("direct-hdfs-failure")?,
        storage.clone(),
        StoragePath::new("source")?,
        storage,
        StoragePath::new("final")?,
        InflightLimits::new(2, 128, 2)?,
        CancellationToken::new(),
    )
    .with_transfer_policy(TransferPolicy::Direct);
    let failure = transfer(request)
        .await
        .err()
        .ok_or("injected direct failure succeeded")?;
    assert!(failure.final_destination_changed());
    assert!(!failure.has_unpublished_stage());
    assert!(!failure.has_recoverable_stage());
    assert!(failure.discard_stage().await.is_err());
    assert!(protocol.get("final").await.is_some());
    assert_eq!(protocol.len().await, 2);
    Ok(())
}

#[tokio::test]
async fn checkpointed_registers_only_after_hsync_and_resumes_durable_prefix() -> Result {
    const INTERVAL: usize = 64 * 1024 * 1024;
    for size in [INTERVAL, INTERVAL + 1] {
        let protocol = Arc::new(MemoryHdfs::default());
        let content = Bytes::from(vec![31; size]);
        protocol.insert("source", content.clone()).await;
        protocol.insert("final", Bytes::from_static(b"old")).await;
        let source = connect(
            protocol.clone(),
            test_identity(&format!("checkpoint-source-{size}"))?,
        )?;
        let destination = connect(
            protocol.clone(),
            test_identity(&format!("checkpoint-target-{size}"))?,
        )?;
        let request = || -> std::result::Result<TransferRequest, Box<dyn std::error::Error>> {
            Ok(TransferRequest::new(
                TransferIdentity::new(format!("hdfs-checkpoint-{}-{size}", std::process::id()))?,
                source.clone(),
                StoragePath::new("source")?,
                destination.clone(),
                StoragePath::new("final")?,
                InflightLimits::new(4, 8 * 1024 * 1024, 4)?,
                CancellationToken::new(),
            )
            .with_transfer_policy(TransferPolicy::Checkpointed)
            .with_read_back_verification(ReadBackVerification::Disabled))
        };
        if size > INTERVAL {
            protocol.fail_once_after_hsync();
            let failure = transfer(request()?)
                .await
                .err()
                .ok_or("injected second window succeeded")?;
            assert!(failure.has_recoverable_stage());
            assert_eq!(
                protocol.get("final").await,
                Some(Bytes::from_static(b"old"))
            );
            drop(failure);
        }
        let outcome = transfer(request()?).await?;
        assert_eq!(
            outcome.recovery,
            if size > INTERVAL {
                crate::transfer::EffectiveRecovery::Checkpointed
            } else {
                crate::transfer::EffectiveRecovery::SkippedBelowCheckpointThreshold
            }
        );
        assert_eq!(protocol.get("final").await, Some(content));
        assert_eq!(protocol.len().await, 2);
        assert_eq!(protocol.io_peaks().2, if size > INTERVAL { 2 } else { 1 });
        assert_eq!(protocol.hsync_calls(), usize::from(size > INTERVAL));
    }
    Ok(())
}

#[tokio::test]
async fn all_policies_copy_with_independent_read_and_write_limits() -> Result {
    for policy in [
        TransferPolicy::Checkpointed,
        TransferPolicy::AtomicReplace,
        TransferPolicy::Direct,
    ] {
        for (read, write) in [(2, 5), (5, 2)] {
            let protocol = Arc::new(MemoryHdfs::default());
            protocol.configure_io(read, write);
            let content = Bytes::from_static(b"0123456789abcdefghijklmnop");
            protocol.insert("source", content.clone()).await;
            protocol
                .insert(
                    "final",
                    Bytes::from_static(b"old and longer contents--------------------"),
                )
                .await;
            if policy == TransferPolicy::Direct {
                protocol.fail_rename_after_commit();
            }
            let request = TransferRequest::new(
                TransferIdentity::new(format!("hdfs-policy-{policy:?}-{read}"))?,
                connect(protocol.clone(), test_identity("source-policy")?)?,
                StoragePath::new("source")?,
                connect(protocol.clone(), test_identity("target-policy")?)?,
                StoragePath::new("final")?,
                InflightLimits::new(8, 128, 8)?,
                CancellationToken::new(),
            )
            .with_transfer_policy(policy)
            .with_read_back_verification(ReadBackVerification::Disabled);
            transfer(request).await?;
            assert_eq!(protocol.get("final").await, Some(content));
            assert_eq!(protocol.len().await, 2);
            let (reads, largest_write, appends) = protocol.io_peaks();
            assert!(reads > 1, "multi-chunk source must overlap reads");
            assert!(largest_write <= write);
            assert_eq!(appends, 1, "below-threshold files use one ordered append");
        }
    }
    Ok(())
}

#[tokio::test]
async fn source_prefetch_obeys_chunks_bytes_and_operations() -> Result {
    for (chunks, bytes, operations) in [(1, 128, 128), (128, 2, 128), (128, 128, 1)] {
        let protocol = Arc::new(MemoryHdfs::default());
        protocol.configure_io(2, 8);
        protocol.insert("source", Bytes::from_static(b"abcd")).await;
        let storage = connect(protocol.clone(), test_identity("bounded")?)?;
        let source = storage.read_source(&PreflightPolicy::production())?;
        let cancel = CancellationToken::new();
        let (runtime, mut ordered) = InflightRuntime::channel(
            InflightConfig::new(chunks, bytes, operations)?,
            0,
            4,
            cancel.clone(),
        )?;
        let budget = ReadBudget::new(runtime.clone());
        let mut stream = source
            .read(ReadRequest {
                path: StoragePath::new("source")?,
                range: None,
                expected_source: None,
                maximum_chunk_bytes: 2,
                read_inflight: 128,
                read_budget: Some(budget.clone()),
                cancel,
                source_qos: None,
            })
            .await?;
        for offset in [0, 2] {
            let data = stream.next().await.transpose()?.ok_or("missing read")?;
            if offset == 0 {
                assert!(
                    tokio::time::timeout(std::time::Duration::from_millis(20), stream.next())
                        .await
                        .is_err()
                );
            }
            runtime
                .complete_read(
                    budget.take(offset).ok_or("missing admission")?,
                    offset,
                    data,
                )
                .await?;
            assert!(ordered.next().await.transpose()?.is_some());
        }
        assert!(stream.next().await.is_none());
        assert_eq!(protocol.io_peaks().0, 1);
    }
    Ok(())
}

#[tokio::test]
async fn direct_refuses_same_source_and_target() -> Result {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol
        .insert("file", Bytes::from_static(b"preserve me"))
        .await;
    let storage = connect(protocol.clone(), test_identity("same")?)?;
    let request = TransferRequest::new(
        TransferIdentity::new("same-hdfs")?,
        storage.clone(),
        StoragePath::new("file")?,
        storage,
        StoragePath::new("file")?,
        InflightLimits::new(2, 128, 2)?,
        CancellationToken::new(),
    )
    .with_transfer_policy(TransferPolicy::Direct);
    assert!(transfer(request).await.is_err());
    assert_eq!(
        protocol.get("file").await,
        Some(Bytes::from_static(b"preserve me"))
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn hdfs_to_local_copies_mode_mtime_and_verifies_positioned_content() -> Result {
    use std::os::unix::fs::MetadataExt as _;
    let protocol = Arc::new(MemoryHdfs::default());
    let content = Bytes::from((0_u8..=255).cycle().take(4096).collect::<Vec<_>>());
    protocol.insert("source", content.clone()).await;
    protocol.configure_io(128, 128);
    let source = connect(protocol, test_identity("hdfs-positioned-metadata")?)?;
    for policy in [
        TransferPolicy::Checkpointed,
        TransferPolicy::AtomicReplace,
        TransferPolicy::Direct,
    ] {
        let root = tempfile::tempdir()?;
        let baseline = root.path().join("baseline");
        std::fs::write(&baseline, b"")?;
        let owner = std::fs::metadata(&baseline)?;
        let destination = crate::storage::connect_backend(crate::storage::BackendConfig::Local(
            crate::storage::LocalBackendConfig {
                root: root.path().into(),
                identity: crate::storage::backends::local::test_identity("hdfs-local-mode"),
                read_concurrency: NonZeroUsize::new(4).ok_or("read depth")?,
                write_concurrency: NonZeroUsize::new(4).ok_or("write depth")?,
            },
        ))
        .await?;
        let outcome = transfer(
            TransferRequest::new(
                TransferIdentity::new(format!("hdfs-local-{policy:?}"))?,
                source.clone(),
                StoragePath::new("source")?,
                destination,
                StoragePath::new("final")?,
                InflightLimits::new(4, 512, 4)?,
                CancellationToken::new(),
            )
            .with_transfer_policy(policy)
            .with_read_back_verification(ReadBackVerification::Enabled),
        )
        .await?;
        assert_eq!(outcome.blake3, Some(*blake3::hash(&content).as_bytes()));
        assert_eq!(std::fs::read(root.path().join("final"))?, content);
        let observed = std::fs::metadata(root.path().join("final"))?;
        assert_eq!(observed.mode() & 0o7777, 0o640);
        assert_eq!((observed.uid(), observed.gid()), (owner.uid(), owner.gid()));
        assert_eq!((observed.mtime(), observed.mtime_nsec()), (0, 0));
        let report = outcome.metadata.ok_or("metadata was skipped")?;
        assert!(report.loss_report().losses().contains(&(
            crate::metadata::MetadataFamily::OwnershipMode,
            crate::metadata::SemanticLoss::OwnerAndGroupDropped,
        )));
    }
    Ok(())
}

#[tokio::test]
async fn hdfs_source_metadata_is_bound_and_does_not_fabricate_numeric_ownership() -> Result {
    let protocol = Arc::new(MemoryHdfs::default());
    protocol.insert("source", Bytes::from_static(b"old")).await;
    let storage = connect(protocol.clone(), test_identity("bound-metadata")?)?;
    let path = StoragePath::new("source")?;
    let descriptor = storage
        .read_source(&PreflightPolicy::production())?
        .describe(&path)
        .await?;
    let metadata = storage.metadata(&PreflightPolicy::production())?;
    let plan = metadata
        .copied_metadata_observation_plan()
        .ok_or("copy plan missing")?;
    let before = protocol.stat_calls();
    let observed = metadata
        .observe_copy_bound(&path, &descriptor.source_identity, plan)
        .await?;
    assert_eq!(protocol.stat_calls(), before + 1);
    assert_eq!(observed.mode_without_ownership, Some(0o640));
    assert!(observed.observations.ownership_mode().value().is_none());
    protocol
        .insert("source", Bytes::from_static(b"replacement"))
        .await;
    assert!(
        metadata
            .observe_copy_bound(&path, &descriptor.source_identity, plan)
            .await
            .is_err()
    );
    Ok(())
}
