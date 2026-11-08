//! 只影响测试自身连接的代理；先确认一次真实 WRITE，再注入网络异常。
use std::{env, sync::Arc, time::Duration};

use bytes::Bytes;
use data_mover::model::{EntryOperationFailure, FailureClass, Operation, StoragePath, Transience};
use data_mover::storage::{
    ByteStream, FinalDestination, PrepareRequest, PublicationDisposition, PublishRequest, Storage,
    StorageRoleFailure, WriteProgress, WriteProgressObserver,
};
use data_mover::transfer::{
    FailedDataCleanup, MetadataTransferMode, ReadBackVerification, TransferPolicy, TransferRequest,
    transfer,
};
use futures::stream;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Notify,
    time::{Instant, sleep, timeout},
};
use tokio_util::sync::CancellationToken;

use super::{Result, common, connect};

async fn control(mode: &str) -> Result {
    let mut socket = TcpStream::connect(env::var("LAB_NFS_DIRECT_CONTROL")?).await?;
    socket
        .write_all(format!("GET /{mode} HTTP/1.0\r\nHost: loopback\r\n\r\n").as_bytes())
        .await?;
    let mut response = Vec::new();
    socket.read_to_end(&mut response).await?;
    if !response.starts_with(b"HTTP/1.0 204") {
        return Err("NFS control refused fault".into());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires run-nfs-direct-contract.py and its private NFS relay"]
async fn real_nfs_direct_fault_contract() -> Result {
    for mode in ["flash", "disconnect", "stall", "cancel", "lost_ack"] {
        if env::var("LAB_NFS_DIRECT_ONLY").is_ok_and(|selected| selected != mode) {
            continue;
        }
        timeout(Duration::from_secs(50), Box::pin(fault(mode))).await??;
    }
    Ok(())
}

async fn fault(mode: &'static str) -> Result {
    control("normal").await?;
    let target = connect(&env::var("LAB_NFS_DIRECT_PROXY_URL")?).await?;
    let path = StoragePath::new(format!("fault-{mode}"))?;
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join(path.as_str()), b"headtail")?;
    let source = common::connect_local(root.path(), 2).await?;
    let destination = target.staged_destination()?;
    let mut stage = destination
        .prepare_direct(
            PrepareRequest {
                final_destination: FinalDestination::new(path.clone()),
                source: source.read_source()?.describe(&path).await?,
                recovery_binding: [9; 32],
            },
            CancellationToken::new(),
        )
        .await?;
    let owned = target.namespace()?.stat(&path).await?.source_identity;
    let written = Arc::new(Notify::new());
    let wake = written.clone();
    stage.set_write_observer(Some(WriteProgressObserver::new(move |event| {
        if matches!(event, WriteProgress::Confirmed { .. }) {
            wake.notify_one();
        }
    })));
    let armed = Arc::new(Notify::new());
    let input = fault_input(mode, written, armed.clone());
    let cancelling = async {
        armed.notified().await;
        sleep(Duration::from_millis(250)).await;
    };
    let start = Instant::now();
    let result = if mode == "cancel" {
        // role.write 的取消由调用方丢弃在途 future；不是完整迁移请求的取消验收。
        tokio::select! { result = destination.write(&stage, input) => Some(result), () = cancelling => None }
    } else {
        Some(destination.write(&stage, input).await)
    };
    let elapsed = start.elapsed();
    control("normal").await?;
    match result {
        Some(Ok(evidence)) => {
            assert!(matches!(mode, "flash" | "lost_ack"));
            assert_eq!(evidence.persisted_bytes, 8);
            let evidence = destination
                .publish(
                    &stage,
                    PublishRequest {
                        expected_size: 8,
                        expected_blake3: None,
                        cancel: CancellationToken::new(),
                    },
                )
                .await
                .map_err(|error| error.error)?;
            assert_eq!(evidence.disposition, PublicationDisposition::Published);
        }
        outcome => {
            if mode == "cancel" {
                assert!(elapsed < Duration::from_secs(1));
            }
            println!(
                "real NFS Direct fault={mode} elapsed_ms={} write={outcome:?}",
                elapsed.as_millis()
            );
            if let Some(Err(error)) = &outcome {
                println!("real NFS Direct fault={mode} cause={}", diagnostic(error));
            }
            let cleaned = destination.remove_failed_direct(stage).await;
            println!("real NFS Direct fault={mode} cleanup={cleaned:?}");
            if let Err(error) = &cleaned {
                println!(
                    "real NFS Direct fault={mode} cleanup_cause={}",
                    diagnostic(error)
                );
            }
            if cleaned.is_err() {
                // 测试记录未知关闭，不把它冒充自动清理成功；只重置已确认归属的夹具。
                let inspector = connect(&env::var("LAB_NFS_DIRECT_URL")?).await?;
                let current = inspector.namespace()?.stat(&path).await?;
                assert_eq!(current.source_identity.stable_bytes(), owned.stable_bytes());
                inspector.namespace()?.delete(&path).await?;
            }
        }
    }
    retry(&source, &path).await?;
    assert_eq!(std::fs::read(root.path().join(path.as_str()))?, b"headtail");
    println!("real NFS Direct fault={mode} source_retained=true explicit_retry_complete=true");
    Ok(())
}

fn diagnostic(error: &StorageRoleFailure) -> &str {
    match error {
        StorageRoleFailure::Entry(error) => error.diagnostic(),
        StorageRoleFailure::Session(error) => error.diagnostic(),
    }
}

fn fault_input(mode: &'static str, written: Arc<Notify>, signal: Arc<Notify>) -> ByteStream {
    Box::pin(stream::unfold((0, written), move |(index, written)| {
        let signal = signal.clone();
        async move {
            if index == 0 {
                return Some((Ok(Bytes::from_static(b"head")), (1, written)));
            }
            if index != 1 {
                return None;
            }
            written.notified().await;
            let result = control(mode)
                .await
                .map(|()| Bytes::from_static(b"tail"))
                .map_err(|_| {
                    StorageRoleFailure::Entry(
                        EntryOperationFailure::new(
                            StoragePath::root(),
                            Operation::Write,
                            FailureClass::Protocol,
                            Transience::Permanent,
                            "lab fault control failed",
                        )
                        .unwrap_or_else(|_| unreachable!("static diagnostic")),
                    )
                });
            signal.notify_one();
            Some((result, (2, written)))
        }
    }))
}

async fn retry(source: &Storage, path: &StoragePath) -> Result {
    let target = connect(&env::var("LAB_NFS_DIRECT_URL")?).await?;
    transfer(
        TransferRequest::new(
            source.clone(),
            path.clone(),
            target,
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Direct)
        .with_metadata_mode(MetadataTransferMode::CallerManaged)
        .with_read_back_verification(ReadBackVerification::Enabled)
        .with_failed_data_cleanup(FailedDataCleanup::RemoveOwned),
    )
    .await?;
    Ok(())
}
