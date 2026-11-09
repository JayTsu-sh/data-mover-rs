//! 私有 SDK 批量版本删除的真实链路；外部 runner 独立核对保留的当前版本和哨兵。
use std::{env, fs, time::Duration};

use tokio::time::{Instant, sleep, timeout};

use super::*;
use crate::model::{FailureClass, Transience};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
#[ignore = "requires the isolated --version-batch source-purge runner"]
async fn real_exact_version_batch_and_faults() -> TestResult {
    let storage = S3Storage::new(&env::var("LAB_S3_PURGE_URL")?, None).await?;
    let page = storage
        .list_versions_page("payload", None)
        .await
        .map_err(|cause| format!("list failed: {cause:?}"))?;
    assert!(page.next.is_none() && page.rejected.is_empty());
    let older: Vec<_> = page
        .versions
        .into_iter()
        .chain(page.markers)
        .filter(|entry| entry.key == "payload" && !entry.facts.is_latest)
        .map(|entry| (entry.key, SourceVersion::Id(entry.facts.version_id)))
        .collect();
    let mode = env::var("LAB_S3_PURGE_FAULT")?;
    let performance = mode.starts_with("serial") || mode.starts_with("bulk");
    assert_eq!(older.len(), if performance { 127 } else { 3 });
    assert!(
        older
            .iter()
            .any(|(_, version)| *version == SourceVersion::Id("null".into()))
    );
    let older = if mode.starts_with("current_") {
        vec![("payload".into(), SourceVersion::Current)]
    } else {
        older
    };
    let control = env::var("LAB_S3_PURGE_CONTROL")?;
    fs::write(&control, &mode)?;
    let started = Instant::now();
    if matches!(mode.as_str(), "cancel" | "current_cancel") {
        let result = timeout(Duration::from_millis(250), delete(&storage, &older)).await;
        assert!(result.is_err());
        fs::write(&control, "normal")?;
        sleep(Duration::from_millis(50)).await;
        println!(
            "exact version batch cancelled_ms={}",
            started.elapsed().as_millis()
        );
        return Ok(());
    }
    if performance {
        if mode.starts_with("serial") {
            for (key, version) in &older {
                let SourceVersion::Id(id) = version else {
                    return Err("unpinned performance input".into());
                };
                assert!(storage.delete_version(key, id).await.is_ok());
            }
        } else {
            assert!(delete(&storage, &older).await.iter().all(Result::is_ok));
        }
        println!(
            "exact_version_perf mode={mode} phase_us={} records={}",
            started.elapsed().as_micros(),
            older.len()
        );
        return Ok(());
    }
    let results = timeout(Duration::from_secs(10), delete(&storage, &older)).await?;
    fs::write(&control, "normal")?;
    println!(
        "exact version batch mode={mode} elapsed_ms={} results={results:?}",
        started.elapsed().as_millis()
    );
    verify_results(&storage, &older, &mode, &results).await
}

async fn verify_results(
    storage: &S3Storage,
    older: &[(String, SourceVersion)],
    mode: &str,
    results: &[S3Result<()>],
) -> TestResult {
    assert_eq!(results.len(), older.len());
    let current = mode.starts_with("current_");
    let fault = mode.strip_prefix("current_").unwrap_or(mode);
    match fault {
        "normal" => assert!(results.iter().all(Result::is_ok)),
        "partial_denial" => {
            assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 2);
            assert_eq!(
                results
                    .iter()
                    .filter(|result| matches!(result, Err(S3ProtocolFailure::Entry {
                class: FailureClass::PermissionDenied, transience: Transience::Permanent, diagnostic
            }) if diagnostic.contains("AccessDenied")))
                    .count(),
                1
            );
        }
        "missing_id" | "missing_ack" | "unknown_id" | "duplicate_ack" => {
            assert!(results.iter().any(|result| matches!(
                result,
                Err(S3ProtocolFailure::Session {
                    class: FailureClass::Protocol,
                    transience: Transience::Unknown,
                    ..
                })
            )));
        }
        "readonly" => assert!(results.iter().all(|result| matches!(
            result,
            Err(S3ProtocolFailure::Entry {
                class: FailureClass::PermissionDenied,
                ..
            })
        ))),
        "disconnect" | "stall" | "flash" | "lost_ack" => {
            assert!(results.iter().all(|result| matches!(
                result,
                Err(S3ProtocolFailure::Session {
                    class: FailureClass::Connectivity | FailureClass::Timeout,
                    ..
                })
            )));
            if fault == "flash" || (!current && fault == "lost_ack") {
                println!("explicit exact-ID retry after {mode}; original failures={results:?}");
                assert!(delete(storage, older).await.iter().all(Result::is_ok));
            }
        }
        _ => return Err("unknown version-batch fault".into()),
    }
    Ok(())
}
