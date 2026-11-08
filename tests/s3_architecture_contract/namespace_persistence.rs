//! 真实对象桶持久化确认：HEAD 故障不能成功，复核本身不得产生数据读写或新版本。

use std::{env, fs, time::Duration};

use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

use super::{TestResult, connected};
use data_mover::model::{FailureClass, StoragePath};
use data_mover::storage::StorageRoleFailure;

#[tokio::test]
#[ignore = "requires run-source-purge-local.py --namespace-persistence owned RustFS and proxy"]
async fn published_object_persistence_with_real_requests() -> TestResult {
    let storage = connected(&env::var("LAB_S3_PURGE_URL")?).await?;
    let namespace = storage.namespace()?;
    let expected = namespace.stat(&StoragePath::new("payload")?).await?;
    let pinned = storage.read_source()?.describe(&expected.path).await?;
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
    let started = Instant::now();
    let result = timeout(
        Duration::from_secs(10),
        namespace.persist_observed(&expected, cancel),
    )
    .await;
    let elapsed = started.elapsed();
    fs::write(&control, "normal")?;
    watcher.await?;
    let result = result?;
    println!(
        "target-persistence fault={fault} elapsed_ms={} result={result:?}",
        elapsed.as_millis()
    );
    match fault.as_str() {
        "normal" => result?,
        "flash" | "recover_stall" => {
            if let Err(error) = result {
                assert!(matches!(
                    class(&error),
                    FailureClass::Connectivity | FailureClass::Timeout
                ));
                println!("target-persistence recovered fault={fault} original={error}");
                namespace
                    .persist_observed(&expected, CancellationToken::new())
                    .await?;
            }
        }
        "readonly" => assert_eq!(failure_class(result), FailureClass::PermissionDenied),
        "cancel" => {
            assert_eq!(failure_class(result), FailureClass::Cancelled);
            assert!(elapsed < Duration::from_secs(1));
        }
        "disconnect" | "stall" => {
            assert!(matches!(
                failure_class(result),
                FailureClass::Connectivity | FailureClass::Timeout
            ));
            assert!(elapsed < Duration::from_secs(4));
        }
        _ => return Err("unknown persistence fault".into()),
    }
    namespace
        .persist_observed(&pinned, CancellationToken::new())
        .await?;
    assert_eq!(namespace.stat(&expected.path).await?, expected);
    Ok(())
}

fn class(error: &StorageRoleFailure) -> FailureClass {
    match error {
        StorageRoleFailure::Entry(error) => error.class(),
        StorageRoleFailure::Session(error) => error.class(),
    }
}

fn failure_class(result: Result<(), StorageRoleFailure>) -> FailureClass {
    match result {
        Err(error) => class(&error),
        Ok(()) => panic!("the injected failure must not confirm persistence"),
    }
}
