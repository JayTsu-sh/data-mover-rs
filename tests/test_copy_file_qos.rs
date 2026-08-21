use std::path::Path;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use data_mover::{CopyOptions, CreateStorageOptions, QosManager, StorageEnum, create_storage};

mod common;
use common::AssertTestValue;

#[tokio::test]
async fn bandwidth_grants_do_not_consume_protocol_iops() {
    let qos = QosManager::try_new_with_burst("100MiB/s", 64 * 1024, Some(1000))
        .assert_value("create qos");

    assert!(qos.acquire_bandwidth_grant(64 * 1024).await > 0);
    assert_eq!(qos.stats().total_iops.load(Ordering::Relaxed), 0);

    qos.acquire_iops().await;
    assert_eq!(qos.stats().total_iops.load(Ordering::Relaxed), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_file_splits_source_reads_at_qos_grants_and_counts_payload_once() {
    const MIB: usize = 1024 * 1024;
    const FILE_SIZE: usize = 2 * MIB;
    const STORAGE_CHUNK: u64 = MIB as u64;
    const BURST: u64 = 64 * 1024;

    let suffix = std::process::id();
    let src_dir = format!("/tmp/data-mover-copy-qos-src-{suffix}");
    let dst_dir = format!("/tmp/data-mover-copy-qos-dst-{suffix}");
    let _ = tokio::fs::remove_dir_all(&src_dir).await;
    let _ = tokio::fs::remove_dir_all(&dst_dir).await;
    tokio::fs::create_dir_all(&src_dir)
        .await
        .assert_value("create source directory");
    tokio::fs::create_dir_all(&dst_dir)
        .await
        .assert_value("create destination directory");
    tokio::fs::write(format!("{src_dir}/blob.bin"), vec![0x5a; FILE_SIZE])
        .await
        .assert_value("write source fixture");

    let src = create_storage(
        &src_dir,
        CreateStorageOptions::new(Some(STORAGE_CHUNK), false),
    )
    .await
    .assert_value("create source storage");
    let dst = create_storage(
        &dst_dir,
        CreateStorageOptions::new(Some(STORAGE_CHUNK), true),
    )
    .await
    .assert_value("create destination storage");
    let entry = src
        .get_metadata(Path::new("blob.bin"))
        .await
        .assert_value("read source metadata");
    let qos =
        QosManager::try_new_with_burst("4MiB/s", BURST, Some(10_000)).assert_value("create qos");

    let started = Instant::now();
    StorageEnum::copy_file(
        &src,
        &dst,
        &entry,
        CopyOptions {
            qos: Some(qos.clone()),
            is_source_reserved: true,
            ..Default::default()
        },
    )
    .await
    .assert_value("copy file");
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_millis(450),
        "2 MiB at a hard 4 MiB/s limit must take about 500 ms; got {elapsed:?}"
    );
    assert_eq!(
        qos.stats().total_bytes.load(Ordering::Relaxed),
        FILE_SIZE as u64,
        "payload bandwidth must be charged once on source reads"
    );
    assert!(
        qos.stats().total_iops.load(Ordering::Relaxed) >= 32,
        "a 64 KiB burst must split 1 MiB storage chunks into actual source IOs"
    );

    let copied = tokio::fs::read(format!("{dst_dir}/blob.bin"))
        .await
        .assert_value("read copied file");
    assert_eq!(copied.len(), FILE_SIZE);
    let _ = tokio::fs::remove_dir_all(src_dir).await;
    let _ = tokio::fs::remove_dir_all(dst_dir).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_file_single_chunk_cannot_bypass_hard_peak_with_a_large_burst() {
    const MIB: usize = 1024 * 1024;
    let suffix = std::process::id();
    let src_dir = format!("/tmp/data-mover-copy-qos-single-src-{suffix}");
    let dst_dir = format!("/tmp/data-mover-copy-qos-single-dst-{suffix}");
    let _ = tokio::fs::remove_dir_all(&src_dir).await;
    let _ = tokio::fs::remove_dir_all(&dst_dir).await;
    tokio::fs::create_dir_all(&src_dir)
        .await
        .assert_value("create source directory");
    tokio::fs::create_dir_all(&dst_dir)
        .await
        .assert_value("create destination directory");
    tokio::fs::write(format!("{src_dir}/blob.bin"), vec![0xa5; MIB])
        .await
        .assert_value("write source fixture");

    let src = create_storage(&src_dir, CreateStorageOptions::new(Some(MIB as u64), false))
        .await
        .assert_value("create source storage");
    let dst = create_storage(&dst_dir, CreateStorageOptions::new(Some(MIB as u64), true))
        .await
        .assert_value("create destination storage");
    let entry = src
        .get_metadata(Path::new("blob.bin"))
        .await
        .assert_value("read source metadata");
    let qos = QosManager::try_new_with_burst("4MiB/s", 2 * MIB as u64, Some(10_000))
        .assert_value("create qos");

    let started = Instant::now();
    StorageEnum::copy_file(
        &src,
        &dst,
        &entry,
        CopyOptions {
            qos: Some(qos.clone()),
            is_source_reserved: true,
            ..Default::default()
        },
    )
    .await
    .assert_value("copy file");

    assert!(
        started.elapsed() >= Duration::from_millis(225),
        "1 MiB at a hard 4 MiB/s peak must take about 250 ms"
    );
    assert_eq!(qos.stats().total_bytes.load(Ordering::Relaxed), MIB as u64);
    assert!(qos.stats().total_iops.load(Ordering::Relaxed) >= 20);

    let _ = tokio::fs::remove_dir_all(src_dir).await;
    let _ = tokio::fs::remove_dir_all(dst_dir).await;
}
