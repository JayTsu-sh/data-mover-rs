//! 隔离真实 S3 的 Direct 原生复制与故障契约，runner 独立读取所有最终版本。
use std::{env, error::Error, fs, time::Duration};

use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

use super::{TestResult, connected};
use data_mover::model::{FailureClass, StoragePath};
use data_mover::storage::StorageRoleFailure;
use data_mover::transfer::{
    EffectiveRecovery, MetadataFailurePolicy, ReadBackVerification, TransferFailure,
    TransferOutcome, TransferPhase, TransferPolicy, TransferRequest, TransferRoute, transfer,
};

fn class(error: &TransferFailure) -> TestResult<FailureClass> {
    match error
        .source()
        .and_then(|cause| cause.downcast_ref::<StorageRoleFailure>())
    {
        Some(StorageRoleFailure::Entry(cause)) => Ok(cause.class()),
        Some(StorageRoleFailure::Session(cause)) => Ok(cause.class()),
        None => Err("missing typed storage failure".into()),
    }
}

fn check_success(outcome: &TransferOutcome) {
    assert_eq!(outcome.route, TransferRoute::Native);
    assert_eq!(outcome.recovery, EffectiveRecovery::Disabled);
    assert_eq!(outcome.transferred_bytes, 1024);
    assert_eq!(outcome.source_qos.native_requests, 1);
    assert!(outcome.destination_version.is_some());
}

#[tokio::test]
#[ignore = "requires run-direct-native-local.py owned real S3 and controlled proxy"]
async fn direct_native_with_real_requests() -> TestResult {
    let base = env::var("LAB_DIRECT_NATIVE_URL")?;
    let control = env::var("LAB_DIRECT_NATIVE_CONTROL")?;
    let mode = env::var("LAB_DIRECT_NATIVE_MODE")?;
    let source = connected(&format!("{base}/{mode}/src")).await?;
    let destination = connected(&format!("{base}/{mode}/target")).await?;
    let cancel = CancellationToken::new();
    let request = TransferRequest::new(
        source,
        StoragePath::new("payload")?,
        destination,
        cancel.clone(),
    )
    .with_transfer_policy(TransferPolicy::Direct)
    .with_metadata_failure_policy(MetadataFailurePolicy::PublishData)
    .with_read_back_verification(if mode.ends_with("full") {
        ReadBackVerification::Enabled
    } else {
        ReadBackVerification::Disabled
    });
    fs::write(&control, &mode)?;
    let cancelling = mode == "cancel";
    let watcher = tokio::spawn(async move {
        if cancelling {
            sleep(Duration::from_millis(250)).await;
            cancel.cancel();
        }
    });
    let start = Instant::now();
    let result = timeout(Duration::from_secs(8), transfer(request.clone())).await;
    let elapsed = start.elapsed();
    fs::write(&control, "normal")?;
    watcher.await?;
    println!(
        "Direct native fault={mode} elapsed_ms={}",
        elapsed.as_millis()
    );
    let result = result?;
    match mode.as_str() {
        "quick" | "full" | "lost_ack" => check_success(&result?),
        "tags_quick" | "tags_full" => {
            let failure = result.err().ok_or("metadata refusal succeeded")?;
            assert_eq!(failure.phase(), TransferPhase::Metadata);
            check_success(
                failure
                    .published_data()
                    .ok_or("published data evidence missing")?,
            );
            assert!(failure.to_string().contains("PermissionDenied"));
        }
        "deny" | "disconnect" | "stall" | "flash" | "cancel" => {
            let failure = result.err().ok_or("injected copy fault succeeded")?;
            assert_eq!(failure.phase(), TransferPhase::Transfer);
            assert!(failure.published_data().is_none());
            let cause = class(&failure)?;
            println!("Direct native fault={mode} class={cause:?}");
            match mode.as_str() {
                "deny" => assert_eq!(cause, FailureClass::PermissionDenied),
                "cancel" => {
                    assert_eq!(cause, FailureClass::Cancelled);
                    assert!(elapsed < Duration::from_secs(1));
                }
                _ => assert!(matches!(
                    cause,
                    FailureClass::Connectivity | FailureClass::Timeout
                )),
            }
            // 故障恢复后显式重试；取消不重用已取消请求。
            if mode != "cancel" {
                check_success(&transfer(request).await?);
            }
        }
        _ => return Err("unknown Direct native test mode".into()),
    }
    Ok(())
}
