//! 独占 S3 夹具核验 Direct 已确认输出的失败清理，源及旧版本由父进程独立读取。
use std::{env, error::Error, io, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Instant, sleep, timeout},
};
use tokio_util::sync::CancellationToken;

use super::{TestResult, connected};
use data_mover::model::{FailureClass, StoragePath, Transience};
use data_mover::storage::StorageRoleFailure;
use data_mover::transfer::{
    FailedDataCleanup, MetadataFailurePolicy, TransferFailure, TransferPhase, TransferPolicy,
    TransferRequest, transfer,
};

async fn wait_for_read(address: String) -> io::Result<()> {
    timeout(Duration::from_secs(15), async {
        loop {
            let mut stream = TcpStream::connect(&address).await?;
            stream
                .write_all(b"GET /read-seen HTTP/1.0\r\nHost: owned\r\n\r\n")
                .await?;
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await?;
            if response.starts_with(b"HTTP/1.0 204") {
                return Ok(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(io::Error::other)?
}

fn check_failure(failure: &TransferFailure, mode: &str) {
    if mode == "cancel" {
        let cause = failure
            .source()
            .and_then(|cause| cause.downcast_ref::<StorageRoleFailure>());
        assert!(
            matches!(cause, Some(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Cancelled),
            "{failure}"
        );
    }
    if mode == "metadata" {
        assert_eq!(failure.phase(), TransferPhase::Metadata);
        assert!(failure.published_data().is_some());
    } else {
        assert_eq!(
            failure.phase(),
            if mode == "data403" {
                TransferPhase::Transfer
            } else {
                TransferPhase::Verify
            }
        );
        assert!(failure.published_data().is_none());
    }
    let expected = match mode {
        "delete403" => Some(FailureClass::PermissionDenied),
        "disconnect" | "lostack" => Some(FailureClass::Connectivity),
        "stall" => Some(FailureClass::Timeout),
        _ => None,
    };
    let actual = failure.cleanup_failure().map(|error| match error {
        StorageRoleFailure::Entry(cause) => cause.class(),
        StorageRoleFailure::Session(cause) => cause.class(),
    });
    assert_eq!(actual, expected, "{failure}");
    if matches!(mode, "disconnect" | "stall" | "lostack") {
        let transience = failure.cleanup_failure().map(|error| match error {
            StorageRoleFailure::Entry(cause) => cause.transience(),
            StorageRoleFailure::Session(cause) => cause.transience(),
        });
        assert_eq!(transience, Some(Transience::Unknown), "{failure}");
    }
    println!("Direct failed-data mode={mode} cause={failure}");
}

#[tokio::test]
#[ignore = "requires run-direct-data-cleanup-local.py isolated S3 and owned proxy"]
async fn confirmed_direct_data_cleanup_with_real_requests() -> TestResult {
    let mode = env::var("LAB_DIRECT_CLEANUP_MODE")?;
    let source = connected(&env::var("LAB_DIRECT_CLEANUP_SOURCE")?).await?;
    let target = connected(&env::var("LAB_DIRECT_CLEANUP_TARGET")?).await?;
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    let control = env::var("LAB_DIRECT_CLEANUP_CONTROL")?;
    let cancelling = mode == "cancel";
    let watcher = tokio::spawn(async move {
        if cancelling {
            wait_for_read(control).await?;
            sleep(Duration::from_millis(250)).await;
            signal.cancel();
        }
        Ok::<_, io::Error>(())
    });
    let key = env::var("LAB_DIRECT_CLEANUP_KEY").unwrap_or_else(|_| "payload".into());
    let request = TransferRequest::new(source, StoragePath::new(key)?, target, cancel)
        .with_transfer_policy(TransferPolicy::Direct)
        .with_failed_data_cleanup(FailedDataCleanup::RemoveOwned)
        .with_metadata_failure_policy(MetadataFailurePolicy::PublishData);
    let started = Instant::now();
    let result = timeout(Duration::from_secs(40), transfer(request)).await?;
    watcher.await??;
    if mode == "normal" {
        result?;
    } else {
        let failure = result
            .err()
            .ok_or("injected failure unexpectedly succeeded")?;
        check_failure(
            &failure.cleanup_for_policy(TransferPolicy::Direct).await,
            &mode,
        );
    }
    println!(
        "Direct cleanup mode={mode} elapsed_ms={}",
        started.elapsed().as_millis()
    );
    Ok(())
}
