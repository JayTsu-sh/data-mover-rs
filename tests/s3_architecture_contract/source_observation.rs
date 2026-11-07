//! 真正的源端属性读取故障不能变成路径缺失；恢复窗口覆盖 stat 和属性读取。
use std::{env, fs, time::Duration};

use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

use super::{TestResult, connected};
use data_mover::model::{FailureClass, ObservationMode, ObservationPlan, StoragePath};
use data_mover::storage::{
    EntryObservationFailure, NamespaceReadCause, NamespaceReadFailure, NamespaceReadPolicy,
    SourceObservationReadOutcome, StorageRoleFailure, read_source_observation,
};

fn check_result(
    fault: &str,
    elapsed: Duration,
    result: Result<SourceObservationReadOutcome, NamespaceReadFailure>,
) -> TestResult {
    match fault {
        "normal" | "flash" | "recover_stall" => {
            let entry = result?.entry.ok_or("source must remain present")?;
            assert_eq!(entry.metadata().tags().value().map(Vec::len), Some(0));
        }
        "cancel" => {
            assert!(matches!(
                result,
                Err(NamespaceReadFailure::Cancelled { .. })
            ));
            assert!(elapsed < Duration::from_secs(1));
        }
        "readonly" | "missing_metadata" => {
            let class = if fault == "readonly" {
                FailureClass::PermissionDenied
            } else {
                FailureClass::NotFound
            };
            assert!(
                matches!(result, Err(NamespaceReadFailure::Operation { attempts: 1, last, .. })
                if matches!(*last, NamespaceReadCause::Observation(ref error)
                    if matches!(error.as_ref(), EntryObservationFailure::Role(StorageRoleFailure::Entry(error)) if error.class()==class)))
            );
        }
        "disconnect" | "stall" | "alternate" => {
            assert!(matches!(
                result,
                Err(NamespaceReadFailure::Operation { .. })
            ));
            assert!(elapsed >= Duration::from_secs(3) && elapsed < Duration::from_millis(4500));
        }
        _ => return Err("unknown source-observation fault".into()),
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires run-source-purge-local.py --source-observation owned real S3 and controlled proxy"]
async fn bounded_source_observation_with_real_requests() -> TestResult {
    let storage = connected(&env::var("LAB_S3_PURGE_URL")?).await?;
    let control = env::var("LAB_S3_PURGE_CONTROL")?;
    let fault = env::var("LAB_S3_PURGE_FAULT")?;
    fs::write(&control, &fault)?;
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    let cancelling = fault == "cancel";
    let watcher = tokio::spawn(async move {
        if cancelling {
            sleep(Duration::from_millis(250)).await;
            signal.cancel();
        }
    });
    let policy = NamespaceReadPolicy {
        operation_timeout: Duration::from_secs(1),
        recovery_window: Duration::from_secs(3),
        retry_interval: Duration::from_millis(100),
    };
    let plan = ObservationPlan::default().with_tags(ObservationMode::Required);
    let start = Instant::now();
    let result = read_source_observation(
        &storage,
        &StoragePath::new("payload")?,
        plan,
        policy,
        &cancel,
    )
    .await;
    let elapsed = start.elapsed();
    fs::write(&control, "normal")?;
    watcher.await?;
    println!(
        "source-observation fault={fault} elapsed_ms={} result={result:?}",
        elapsed.as_millis()
    );
    check_result(&fault, elapsed, result)?;
    assert!(
        read_source_observation(
            &storage,
            &StoragePath::new("missing")?,
            plan,
            policy,
            &CancellationToken::new()
        )
        .await?
        .entry
        .is_none()
    );
    Ok(())
}
