//! 精确修改历史版本元数据；真实服务与故障代理由隔离 runner 提供。
use std::{env, fs, time::Duration};

use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

use super::{TestResult, connected};
use data_mover::metadata::{
    AclTarget, MetadataPlan, MetadataPlanRequest, MetadataPolicies, MetadataPolicy, MetadataTarget,
    MetadataVerificationError, OwnershipTarget, TimestampTargetCapability, ValueTarget,
    compile_metadata_plan,
};
use data_mover::model::{
    MetadataObservation, MetadataObservations, MetadataProvenance, ObjectTag, StoragePath,
};

fn captured_plan(tags: Vec<ObjectTag>) -> TestResult<MetadataPlan> {
    let observations = MetadataObservations::new(
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        MetadataObservation::Value {
            value: tags,
            provenance: MetadataProvenance::Inline,
        },
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
    )?;
    Ok(compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: MetadataTarget {
            acl: AclTarget::NotApplicable,
            xattrs: ValueTarget::NotApplicable,
            tags: ValueTarget::Supported,
            ownership_mode: OwnershipTarget::NotApplicable,
            timestamps: TimestampTargetCapability::Unsupported,
        },
        policies: MetadataPolicies::default().with_tags(MetadataPolicy::RequireExact),
        principal_mapper: None,
    })?)
}

#[tokio::test]
#[ignore = "requires run-version-metadata-local.py owned real versioned S3 and controlled proxy"]
async fn exact_version_metadata_with_real_requests() -> TestResult {
    let storage = connected(&env::var("LAB_VERSION_METADATA_URL")?).await?;
    let version = env::var("LAB_VERSION_METADATA_ID")?;
    let mode = env::var("LAB_VERSION_METADATA_MODE")?;
    let control = env::var("LAB_VERSION_METADATA_CONTROL")?;
    let path = StoragePath::new("payload")?;
    let plan = captured_plan(vec![ObjectTag::new("raw", "updated")?])?;
    let cancel = CancellationToken::new();
    let cancelling = cancel.clone();
    let should_cancel = mode == "cancel";
    let watcher = tokio::spawn(async move {
        if should_cancel {
            sleep(Duration::from_millis(250)).await;
            cancelling.cancel();
        }
    });
    fs::write(&control, &mode)?;
    let start = Instant::now();
    let result = timeout(
        Duration::from_secs(8),
        plan.apply_to_version(storage.metadata()?.as_ref(), &path, &version, cancel),
    )
    .await;
    let elapsed = start.elapsed();
    fs::write(&control, "normal")?;
    watcher.await?;
    println!(
        "version metadata fault={mode} elapsed_ms={}",
        elapsed.as_millis()
    );
    let result = result?;
    if mode == "normal" {
        result?;
    } else {
        let failure = result.err().ok_or("injected metadata fault succeeded")?;
        let detail = failure.to_string();
        println!("version metadata fault={mode} detail={detail}");
        assert_eq!(failure.failures().len(), 1);
        match mode.as_str() {
            "deny" => assert!(detail.contains("PermissionDenied")),
            "cancel" => {
                assert!(detail.contains("cancel") || detail.contains("Cancel"));
                assert!(elapsed < Duration::from_secs(1));
            }
            _ => assert!(detail.contains("Connectivity") || detail.contains("Timeout")),
        }
        // 精确版本 tags 写入幂等；结果不明后仍只重试同一版本，不重传数据。
        plan.apply_to_version(
            storage.metadata()?.as_ref(),
            &path,
            &version,
            CancellationToken::new(),
        )
        .await?;
    }
    let verified = plan
        .verify_version(&storage, &path, &version, CancellationToken::new())
        .await?;
    assert_eq!(verified.size, Some(3));
    let empty = captured_plan(Vec::new())?;
    empty
        .apply_to_version(
            storage.metadata()?.as_ref(),
            &path,
            &version,
            CancellationToken::new(),
        )
        .await?;
    empty
        .verify_version(&storage, &path, &version, CancellationToken::new())
        .await?;
    plan.apply_to_version(
        storage.metadata()?.as_ref(),
        &path,
        &version,
        CancellationToken::new(),
    )
    .await?;
    assert!(matches!(
        plan.verify_published(&storage, &path, Some(&version), CancellationToken::new())
            .await,
        Err(MetadataVerificationError::VersionChanged { .. })
    ));
    Ok(())
}
