//! 真实删除标记 HEAD／当前绑定 LIST 的只读持久性核验。
use std::{env, fs, time::Duration};

use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

use super::{TestResult, connected};
use data_mover::model::{FailureClass, SourceVersion, StoragePath};
use data_mover::storage::Storage;
use data_mover::storage::{Namespace, SourceDescriptor, StorageRoleFailure};
use data_mover::transfer::{TransferPhase, TransferRequest, transfer};

#[tokio::test]
#[ignore = "requires run-source-purge-local.py --marker-persistence owned RustFS and proxy"]
async fn published_marker_persistence_with_real_requests() -> TestResult {
    let storage = connected(&env::var("LAB_S3_PURGE_URL")?).await?;
    let source = storage.read_source()?;
    let namespace = storage.namespace()?;
    let path = StoragePath::new("payload")?;
    let historical = source
        .describe_version(
            &path,
            &SourceVersion::Id(env::var("LAB_S3_HISTORICAL_MARKER")?),
        )
        .await?;
    let mut current = source
        .describe_version(
            &path,
            &SourceVersion::Id(env::var("LAB_S3_CURRENT_MARKER")?),
        )
        .await?;
    for marker in [&historical, &current] {
        assert!(
            marker.size.is_none()
                && marker.content_version.is_none()
                && marker.listing.listing_only
        );
        assert!(
            marker
                .listing
                .history
                .as_ref()
                .is_some_and(|history| !history.is_complete()
                    && history.records().len() == 1
                    && history.records()[0].is_delete_marker())
        );
        namespace
            .persist_observed(marker, CancellationToken::new())
            .await?;
    }
    refuse_marker_transfer(&storage, &historical).await?;
    current.version = SourceVersion::Current;
    faulted_confirmation(namespace.as_ref(), &current).await?;
    // 旧标记不会因当前绑定变化而不可描述，整个验证不读取数据、不改写对象。
    assert_eq!(
        source.describe_version(&path, historical.version()).await?,
        historical
    );
    namespace
        .persist_observed(&historical, CancellationToken::new())
        .await?;
    Ok(())
}

async fn faulted_confirmation(
    namespace: &dyn Namespace,
    expected: &SourceDescriptor,
) -> TestResult {
    let control = env::var("LAB_S3_PURGE_CONTROL")?;
    let fault = env::var("LAB_S3_PURGE_FAULT")?;
    fs::write(&control, &fault)?;
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    let cancelling = fault.ends_with("cancel");
    let watcher = tokio::spawn(async move {
        if cancelling {
            sleep(Duration::from_millis(250)).await;
            signal.cancel();
        }
    });
    let started = Instant::now();
    let result = timeout(
        Duration::from_secs(10),
        namespace.persist_observed(expected, cancel),
    )
    .await;
    let elapsed = started.elapsed();
    fs::write(&control, "normal")?;
    watcher.await?;
    let result = result?;
    println!(
        "marker-persistence fault={fault} elapsed_ms={} result={result:?}",
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
                println!("marker-persistence recovered fault={fault} original={error}");
            }
            namespace
                .persist_observed(expected, CancellationToken::new())
                .await?;
        }
        "readonly" => assert_eq!(failure_class(result), FailureClass::PermissionDenied),
        "missing_id" | "wrong_id" | "invalid_type" => {
            assert_eq!(failure_class(result), FailureClass::Protocol);
        }
        "replaced" => assert_eq!(failure_class(result), FailureClass::Conflict),
        "missing" => assert_eq!(failure_class(result), FailureClass::NotFound),
        "cancel" | "list_cancel" => {
            assert_eq!(failure_class(result), FailureClass::Cancelled);
            assert!(elapsed < Duration::from_secs(1));
        }
        "disconnect" | "stall" | "list_disconnect" | "list_stall" => {
            assert!(matches!(
                failure_class(result),
                FailureClass::Connectivity | FailureClass::Timeout
            ));
            assert!(elapsed < Duration::from_secs(4));
        }
        _ => return Err("unknown marker persistence fault".into()),
    }
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
        Ok(()) => panic!("injected failure must not confirm marker persistence"),
    }
}

async fn refuse_marker_transfer(storage: &Storage, marker: &SourceDescriptor) -> TestResult {
    let result = transfer(
        TransferRequest::new(
            storage.clone(),
            marker.path.clone(),
            storage.clone(),
            CancellationToken::new(),
        )
        .with_source_version(marker.version.clone()),
    )
    .await;
    let error = result.err().ok_or("marker was transferred as empty data")?;
    assert_eq!(error.phase(), TransferPhase::Describe);
    println!("marker ordinary-transfer refused_at=Describe before_destination_mutation=true");
    Ok(())
}
