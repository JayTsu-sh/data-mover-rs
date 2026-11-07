//! 真正的 S3 stat 链路；隔离 runner 控制 HEAD 故障并独立检查没有发生写入。
use std::{env, fs, time::Duration};

use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

use super::{TestResult, connected};
use data_mover::model::{FailureClass, StoragePath};
use data_mover::storage::{
    NamespaceReadCause, NamespaceReadFailure, NamespaceReadPolicy, StorageRoleFailure,
    read_namespace_entry,
};

#[tokio::test]
#[ignore = "requires run-source-purge-local.py --namespace-read owned real S3 and controlled proxy"]
async fn bounded_namespace_read_with_real_requests() -> TestResult {
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
    let start = Instant::now();
    let result =
        read_namespace_entry(&storage, &StoragePath::new("payload")?, policy, &cancel).await;
    let elapsed = start.elapsed();
    fs::write(&control, "normal")?;
    watcher.await?;
    println!(
        "namespace-read fault={fault} elapsed_ms={} result={result:?}",
        elapsed.as_millis()
    );
    match fault.as_str() {
        "normal" | "flash" | "recover_stall" => assert!(result?.descriptor.is_some()),
        "cancel" => {
            assert!(matches!(
                result,
                Err(NamespaceReadFailure::Cancelled { .. })
            ));
            assert!(elapsed < Duration::from_secs(1));
        }
        "readonly" => {
            assert!(
                matches!(result, Err(NamespaceReadFailure::Operation { last, attempts: 1, .. }) if matches!(*last, NamespaceReadCause::Role(StorageRoleFailure::Entry(ref e)) if e.class() == FailureClass::PermissionDenied))
            );
        }
        "disconnect" | "stall" => {
            assert!(matches!(
                result,
                Err(NamespaceReadFailure::Operation { .. })
            ));
            assert!(elapsed >= Duration::from_secs(3));
            assert!(elapsed < Duration::from_millis(4500));
        }
        _ => return Err("unknown namespace-read fault".into()),
    }
    assert!(
        read_namespace_entry(
            &storage,
            &StoragePath::new("missing")?,
            policy,
            &CancellationToken::new()
        )
        .await?
        .descriptor
        .is_none()
    );
    Ok(())
}
