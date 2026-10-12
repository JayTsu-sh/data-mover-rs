//! Source-read `QoS` coverage through the public transfer request.

use std::time::{Duration, Instant};

use data_mover::model::StoragePath;
use data_mover::storage::{SourceQosGroup, SourceQosPolicy};
use data_mover::transfer::{InflightLimits, TransferRequest, transfer};
use tokio_util::sync::CancellationToken;

mod common;

#[tokio::test]
async fn source_qos_caps_reads_and_reports_payload_once() -> Result<(), Box<dyn std::error::Error>>
{
    const SIZE: usize = 2 * 1024 * 1024;
    const READ: u64 = 64 * 1024;
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    let payload = vec![0x5a; SIZE];
    std::fs::write(source_root.path().join("blob.bin"), &payload)?;
    let qos = SourceQosGroup::new(SourceQosPolicy::new(None, READ, None)?);

    let outcome = transfer(
        TransferRequest::new(
            common::connect_local(source_root.path(), 8).await?,
            StoragePath::new("blob.bin")?,
            common::connect_local(destination_root.path(), 8).await?,
            StoragePath::new("blob.bin")?,
            InflightLimits::new(8, 1024 * 1024, 8)?,
            CancellationToken::new(),
        )
        .with_source_qos(qos),
    )
    .await?;

    assert_eq!(outcome.transferred_bytes, SIZE as u64);
    assert_eq!(outcome.source_qos.logical_bytes, SIZE as u64);
    assert_eq!(outcome.source_qos.client_streamed_shaped_bytes, SIZE as u64);
    assert_eq!(
        outcome.source_qos.source_read_operations,
        (SIZE as u64).div_ceil(READ)
    );
    assert_eq!(
        std::fs::read(destination_root.path().join("blob.bin"))?,
        payload
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn hard_source_bandwidth_limit_shapes_a_public_transfer()
-> Result<(), Box<dyn std::error::Error>> {
    const SIZE: usize = 1024 * 1024;
    const RATE: u64 = 4 * 1024 * 1024;
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    let payload = vec![0xa5; SIZE];
    std::fs::write(source_root.path().join("blob.bin"), &payload)?;
    let qos = SourceQosGroup::new(SourceQosPolicy::new(
        Some((RATE, RATE, Duration::ZERO)),
        64 * 1024,
        None,
    )?);
    let request = TransferRequest::new(
        common::connect_local(source_root.path(), 4).await?,
        StoragePath::new("blob.bin")?,
        common::connect_local(destination_root.path(), 4).await?,
        StoragePath::new("blob.bin")?,
        InflightLimits::new(4, 256 * 1024, 4)?,
        CancellationToken::new(),
    )
    .with_source_qos(qos);

    let started = Instant::now();
    let outcome = transfer(request).await?;
    assert!(
        started.elapsed() >= Duration::from_millis(225),
        "1 MiB at a 4 MiB/s hard limit should take about 250 ms; got {:?}",
        started.elapsed()
    );
    assert_eq!(outcome.source_qos.logical_bytes, SIZE as u64);
    assert_eq!(outcome.source_qos.client_streamed_shaped_bytes, SIZE as u64);
    let minimum_reads = (SIZE as u64).div_ceil(64 * 1024);
    assert!(
        outcome.source_qos.source_read_operations >= minimum_reads,
        "the 64 KiB read cap requires at least {minimum_reads} operations, got {}",
        outcome.source_qos.source_read_operations
    );
    assert_eq!(
        std::fs::read(destination_root.path().join("blob.bin"))?,
        payload
    );
    Ok(())
}
