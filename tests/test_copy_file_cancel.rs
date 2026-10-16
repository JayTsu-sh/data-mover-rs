//! Cancellation behavior through the public transfer request.

use std::time::{Duration, Instant};

use data_mover::model::StoragePath;
use data_mover::storage::{SourceQosGroup, SourceQosPolicy};
use data_mover::transfer::{InflightLimits, TransferPolicy, TransferRequest, transfer};
use tokio_util::sync::CancellationToken;

mod common;

#[tokio::test]
async fn pre_cancelled_transfer_preserves_an_existing_final()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("blob.bin"), b"replacement")?;
    std::fs::write(destination_root.path().join("blob.bin"), b"old-final")?;
    let cancel = CancellationToken::new();
    cancel.cancel();

    let result = transfer(
        TransferRequest::new(
            common::connect_local(source_root.path(), 4).await?,
            StoragePath::new("blob.bin")?,
            common::connect_local(destination_root.path(), 4).await?,
            InflightLimits::new(2, 64 * 1024, 2)?,
            cancel,
        )
        .with_transfer_policy(TransferPolicy::AtomicReplace),
    )
    .await;

    let Err(failure) = result else {
        panic!("pre-cancelled transfer unexpectedly succeeded");
    };
    assert_eq!(
        failure.phase(),
        data_mover::transfer::TransferPhase::Preflight
    );
    assert_eq!(
        failure.side(),
        data_mover::transfer::TransferSide::Orchestration
    );
    assert_eq!(
        std::fs::read(destination_root.path().join("blob.bin"))?,
        b"old-final"
    );
    assert_eq!(std::fs::read_dir(destination_root.path())?.count(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn cancellation_stops_a_shaped_transfer_before_the_entire_payload()
-> Result<(), Box<dyn std::error::Error>> {
    const SIZE: usize = 16 * 1024 * 1024;
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("blob.bin"), vec![0xcd; SIZE])?;
    let cancel = CancellationToken::new();
    let qos = SourceQosGroup::new(SourceQosPolicy::new(
        Some((4 * 1024 * 1024, 4 * 1024 * 1024, Duration::ZERO)),
        64 * 1024,
        None,
    )?);
    let request = TransferRequest::new(
        common::connect_local(source_root.path(), 4).await?,
        StoragePath::new("blob.bin")?,
        common::connect_local(destination_root.path(), 4).await?,
        InflightLimits::new(4, 256 * 1024, 4)?,
        cancel.clone(),
    )
    .with_source_qos(qos);
    let attempt = tokio::spawn(transfer(request));
    tokio::time::sleep(Duration::from_millis(200)).await;
    let started = Instant::now();
    cancel.cancel();
    let Err(failure) = attempt.await? else {
        panic!("cancelled transfer unexpectedly succeeded");
    };

    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(failure.source_qos().logical_bytes, SIZE as u64);
    assert!(failure.source_qos().client_streamed_shaped_bytes < SIZE as u64);
    assert!(failure.source_qos().source_read_operations > 0);
    if failure.has_unpublished_stage() {
        failure.discard_stage().await?;
    }
    assert!(!destination_root.path().join("blob.bin").exists());
    assert_eq!(std::fs::read_dir(destination_root.path())?.count(), 0);
    Ok(())
}
