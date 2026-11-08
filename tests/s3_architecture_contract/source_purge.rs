//! 真实 S3 链路的永久清理与故障验证；runner 创建独立的普通桶或版本化桶。

use std::{env, fs, time::Duration};

use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

use super::{TestResult, connected};
use data_mover::model::{FailureClass, ObjectHistory, ObservedEntry, StoragePath, VersionRecord};
use data_mover::storage::{
    CollectLimits, ListRequest, PurgeOutcome, Storage, StorageRoleFailure, collect_listing,
};

async fn observed(storage: &Storage, key: &str) -> TestResult<Option<ObservedEntry>> {
    let root = StoragePath::root();
    let request = ListRequest::new(root.clone(), CancellationToken::new());
    let page =
        collect_listing(storage.namespace()?.list(request), CollectLimits::UNBOUNDED).await?;
    let (entries, failures) = page.into_descriptors(&root);
    assert!(failures.is_empty());
    entries
        .into_iter()
        .find(|entry| entry.path.as_str() == key)
        .map(|entry| {
            Ok(ObservedEntry::new(
                entry.path,
                entry.kind,
                entry.size,
                entry.inline_timestamps.and_then(|times| times.modified),
                entry.source_identity,
            )?
            .with_history(entry.listing.history)?)
        })
        .transpose()
}

#[tokio::test]
#[ignore = "requires an isolated real S3 fixture and optional controlled fault proxy"]
async fn permanent_source_purge_with_real_requests() -> TestResult {
    let storage = connected(&env::var("LAB_S3_PURGE_URL")?).await?;
    let key = env::var("LAB_S3_PURGE_KEY")?;
    let source = observed(&storage, &key)
        .await?
        .ok_or("seeded source missing")?;
    let expected_records = source
        .history()
        .map_or(1, |history| history.records().len() as u64);
    let fault = env::var("LAB_S3_PURGE_FAULT").unwrap_or_else(|_| "normal".into());
    let control = env::var("LAB_S3_PURGE_CONTROL").ok();
    if let Some(control) = &control {
        fs::write(control, &fault)?;
    }
    let cancel = CancellationToken::new();
    let cancelling = cancel.clone();
    let mode = fault.clone();
    let watcher = tokio::spawn(async move {
        if mode == "cancel" {
            sleep(Duration::from_millis(250)).await;
            cancelling.cancel();
        }
    });
    let started = Instant::now();
    let result = timeout(
        Duration::from_secs(35),
        storage.namespace()?.purge_observed(&source, cancel),
    )
    .await;
    if let Some(control) = &control {
        fs::write(control, "normal")?;
    }
    watcher.await?;
    println!(
        "source purge fault={fault} elapsed_ms={}",
        started.elapsed().as_millis()
    );
    let result = result?;
    match fault.as_str() {
        "flash" | "lost_ack" => {
            if let Err(error) = result {
                let class = match error {
                    StorageRoleFailure::Entry(ref cause) => cause.class(),
                    StorageRoleFailure::Session(ref cause) => cause.class(),
                };
                assert!(matches!(
                    class,
                    FailureClass::Connectivity | FailureClass::Timeout
                ));
                println!(
                    "recovered source purge fault={fault} first_failure={class:?}; retrying exact version IDs"
                );
                storage
                    .namespace()?
                    .purge_observed(&source, CancellationToken::new())
                    .await?;
            }
            assert!(observed(&storage, &key).await?.is_none());
        }
        "normal" => {
            assert_eq!(
                result?,
                PurgeOutcome::Deleted {
                    records: expected_records
                }
            );
            assert!(observed(&storage, &key).await?.is_none());
        }
        "disconnect" | "stall" | "cancel" | "readonly" => {
            let error = result
                .err()
                .ok_or("fault injection unexpectedly succeeded")?;
            let class = match error {
                StorageRoleFailure::Entry(ref e) => e.class(),
                StorageRoleFailure::Session(ref e) => e.class(),
            };
            println!("source purge failure_class={class:?}");
            match fault.as_str() {
                "readonly" => assert_eq!(class, FailureClass::PermissionDenied),
                "cancel" => {
                    assert_eq!(class, FailureClass::Cancelled);
                    assert!(started.elapsed() < Duration::from_secs(2));
                }
                _ => assert!(matches!(
                    class,
                    FailureClass::Connectivity | FailureClass::Timeout
                )),
            }
            assert_source_retained(&storage, &key, &source).await?;
        }
        _ => return Err("unknown source purge fault mode".into()),
    }
    assert!(observed(&storage, "sentinel").await?.is_some());
    Ok(())
}

async fn assert_source_retained(
    storage: &Storage,
    key: &str,
    source: &ObservedEntry,
) -> TestResult {
    let retained = observed(storage, key)
        .await?
        .ok_or("current source record was deleted after fault")?;
    assert_eq!(retained.source_identity(), source.source_identity());
    assert_eq!(
        retained
            .history()
            .and_then(ObjectHistory::latest)
            .map(VersionRecord::identity),
        source
            .history()
            .and_then(ObjectHistory::latest)
            .map(VersionRecord::identity)
    );
    Ok(())
}
