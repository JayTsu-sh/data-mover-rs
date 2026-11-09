//! 元数据写入和 ACL 读回的真实请求故障；取消不把未知写入当成成功。
use std::{env, time::Duration};

use data_mover::model::{
    FailureClass, MetadataObservation, ObservationMode, ObservationPlan, Operation, OwnershipMode,
    StoragePath, StorageTimestamp, TimePrecision, TimestampMetadata, Transience,
};
use data_mover::storage::{MetadataMutation, Storage, StorageRoleFailure, create_directory_all};
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

use super::fault::control;
use super::{Result, common, connect};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires run-nfs-direct-contract.py --contract metadata-cancel on an owned prefix"]
async fn real_nfs_metadata_fault_contract() -> Result {
    let family = env::var("LAB_NFS_METADATA_FAMILY").unwrap_or_else(|_| "mode".into());
    for mode in ["flash", "disconnect", "stall", "cancel", "lost_ack"] {
        if env::var("LAB_NFS_DIRECT_ONLY").is_ok_and(|selected| selected != mode) {
            continue;
        }
        timeout(Duration::from_secs(55), exercise(&family, mode)).await??;
    }
    Ok(())
}

async fn mutation(storage: &Storage, path: &StoragePath, family: &str) -> Result<MetadataMutation> {
    Ok(match family {
        "mode" => MetadataMutation::Mode(0o640),
        "owner" => MetadataMutation::NumericOwnership(OwnershipMode {
            uid: 0,
            gid: 0,
            mode: 0o640,
        }),
        "time" => MetadataMutation::Timestamps(TimestampMetadata {
            accessed: Some(StorageTimestamp::new(
                1_700_000_000_123_456_789,
                TimePrecision::Nanoseconds,
            )?),
            modified: Some(StorageTimestamp::new(
                1_700_000_001_123_456_789,
                TimePrecision::Nanoseconds,
            )?),
            created: None,
        }),
        "acl" => {
            let observed = storage
                .metadata()?
                .observe(
                    path,
                    ObservationPlan::default().with_acl(ObservationMode::Required),
                )
                .await?;
            let MetadataObservation::Value { value, .. } = observed.acl() else {
                return Err("真实 NFS ACL 观察缺失".into());
            };
            MetadataMutation::Acl(value.clone())
        }
        _ => return Err("未知元数据测试类别".into()),
    })
}

async fn exercise(family: &str, mode: &str) -> Result {
    control("normal").await?;
    let target = connect(&env::var("LAB_NFS_DIRECT_PROXY_URL")?).await?;
    let path = StoragePath::new(format!("metadata-{family}-{mode}"))?;
    create_directory_all(&target, &path).await?;
    let root = tempfile::tempdir()?;
    let file = StoragePath::new(format!("{}/keep", path.as_str()))?;
    std::fs::create_dir(root.path().join(path.as_str()))?;
    std::fs::write(root.path().join(file.as_str()), b"metadata retained body")?;
    let source = common::connect_local(root.path(), 2).await?;
    super::persistence::copy(&source, &target, &file).await?;
    let mutation = mutation(&target, &path, family).await?;
    control(&format!("commit-{mode}")).await?;
    let token = CancellationToken::new();
    let metadata = target.metadata()?;
    let operation = metadata.apply(&path, mutation.clone(), token.clone());
    let started = Instant::now();
    let result = if mode == "cancel" {
        let cancel = async {
            control("wait-fault").await?;
            let instant = Instant::now();
            token.cancel();
            Ok::<_, Box<dyn std::error::Error>>(instant)
        };
        let (result, signalled) = timeout(Duration::from_secs(15), async {
            tokio::join!(operation, cancel)
        })
        .await?;
        let latency = signalled?.elapsed();
        assert!(latency < Duration::from_secs(1), "{latency:?}");
        assert!(
            matches!(&result, Err(StorageRoleFailure::Entry(error))
            if error.operation() == Operation::Metadata && error.class() == FailureClass::Cancelled
                && error.transience() == Transience::Unknown && error.diagnostic().contains("unconfirmed")),
            "{result:?}"
        );
        println!(
            "NFS metadata family={family} fault=cancel return_after_signal_ms={}",
            latency.as_millis()
        );
        result
    } else {
        operation.await
    };
    println!(
        "NFS metadata family={family} fault={mode} elapsed_ms={} result={result:?}",
        started.elapsed().as_millis()
    );
    match &result {
        Err(StorageRoleFailure::Entry(error)) => {
            println!("NFS metadata cause={}", error.diagnostic());
        }
        Err(StorageRoleFailure::Session(error)) => {
            println!("NFS metadata cause={}", error.diagnostic());
        }
        Ok(()) => assert!(matches!(mode, "flash" | "lost_ack")),
    }
    control("normal").await?;
    let retry = connect(&env::var("LAB_NFS_DIRECT_URL")?).await?;
    retry
        .metadata()?
        .apply(&path, mutation, CancellationToken::new())
        .await?;
    assert_eq!(
        std::fs::read(root.path().join(file.as_str()))?,
        b"metadata retained body"
    );
    println!(
        "NFS metadata family={family} fault={mode} explicit_recovery=true source_retained=true"
    );
    Ok(())
}
