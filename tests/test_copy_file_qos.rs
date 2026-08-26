use std::future::Future;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use data_mover::{CopyOptions, CreateStorageOptions, QosManager, StorageEnum, create_storage};
use num_traits::ToPrimitive;

mod common;
use common::AssertTestValue;

async fn run_copy_with_bandwidth_samples<F>(
    copy: F,
    qos: &QosManager,
    soft_mibps: f64,
    hard_mibps: f64,
) -> data_mover::Result<Duration>
where
    F: Future<Output = data_mover::Result<()>>,
{
    let started = Instant::now();
    tokio::pin!(copy);
    let mut interval = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_millis(100),
        Duration::from_millis(100),
    );
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut checkpoints = vec![(Duration::ZERO, 0_u64)];
    loop {
        tokio::select! {
            result = &mut copy => {
                result?;
                break;
            }
            _ = interval.tick() => {
                let total_bytes = qos.stats().total_bytes.load(Ordering::Relaxed);
                checkpoints.push((started.elapsed(), total_bytes));
            }
        }
    }
    let elapsed = started.elapsed();
    let total_bytes = qos.stats().total_bytes.load(Ordering::Relaxed);
    if checkpoints.last().is_some_and(|(sampled_at, _)| {
        elapsed.saturating_sub(*sampled_at) < Duration::from_millis(50)
    }) && checkpoints.len() > 1
    {
        checkpoints.pop();
    }
    checkpoints.push((elapsed, total_bytes));
    let samples: Vec<(u128, f64)> = checkpoints
        .windows(2)
        .map(|window| {
            let (previous_at, previous_bytes) = window[0];
            let (sampled_at, sampled_bytes) = window[1];
            let window_mibps = sampled_bytes
                .saturating_sub(previous_bytes)
                .to_f64()
                .unwrap_or(f64::MAX)
                / (1024.0 * 1024.0)
                / sampled_at
                    .saturating_sub(previous_at)
                    .as_secs_f64()
                    .max(f64::EPSILON);
            (sampled_at.as_millis(), window_mibps)
        })
        .collect();
    let smoothed_samples: Vec<(u128, f64)> = checkpoints
        .iter()
        .enumerate()
        .skip(1)
        .map(|(index, &(sampled_at, sampled_bytes))| {
            let window_start = sampled_at.saturating_sub(Duration::from_millis(500));
            let start_index = checkpoints[..index]
                .partition_point(|(checkpoint_at, _)| *checkpoint_at <= window_start)
                .saturating_sub(1);
            let (previous_at, previous_bytes) = checkpoints[start_index];
            let window_mibps = sampled_bytes
                .saturating_sub(previous_bytes)
                .to_f64()
                .unwrap_or(f64::MAX)
                / (1024.0 * 1024.0)
                / sampled_at
                    .saturating_sub(previous_at)
                    .as_secs_f64()
                    .max(f64::EPSILON);
            (sampled_at.as_millis(), window_mibps)
        })
        .collect();
    print_bandwidth_chart("Instant (100 ms)", &samples, soft_mibps, hard_mibps);
    println!();
    print_bandwidth_chart("Smooth (500 ms)", &smoothed_samples, soft_mibps, hard_mibps);
    Ok(elapsed)
}

fn print_bandwidth_chart(title: &str, samples: &[(u128, f64)], soft_mibps: f64, hard_mibps: f64) {
    const X_STEP: usize = 6;
    let upper = hard_mibps.ceil().to_i64().unwrap_or(i64::MAX);
    let minimum = samples
        .iter()
        .map(|(_, rate)| *rate)
        .fold(hard_mibps, f64::min);
    let lower = (minimum.floor().to_i64().unwrap_or(0) - 1).max(0);
    let rows = usize::try_from(upper - lower + 1).unwrap_or(1);
    let width = samples.len().saturating_sub(1) * X_STEP + 1;
    let mut graph = vec![vec![' '; width]; rows];
    let points: Vec<(usize, usize)> = samples
        .iter()
        .enumerate()
        .map(|(index, (_, rate))| {
            let level = rate.round().to_i64().unwrap_or(upper).clamp(lower, upper);
            (index * X_STEP, usize::try_from(upper - level).unwrap_or(0))
        })
        .collect();

    for pair in points.windows(2) {
        let (x1, y1) = pair[0];
        let (x2, y2) = pair[1];
        if y1 == y2 {
            for cell in &mut graph[y1][x1 + 1..x2] {
                *cell = '─';
            }
            continue;
        }
        let middle = usize::midpoint(x1, x2);
        for cell in &mut graph[y1][x1 + 1..middle] {
            *cell = '─';
        }
        for cell in &mut graph[y2][middle + 1..x2] {
            *cell = '─';
        }
        let (top, bottom) = (y1.min(y2), y1.max(y2));
        for row in &mut graph[top + 1..bottom] {
            row[middle] = '│';
        }
        if y2 < y1 {
            graph[y1][middle] = '╯';
            graph[y2][middle] = '╭';
        } else {
            graph[y1][middle] = '╮';
            graph[y2][middle] = '╰';
        }
    }
    for &(x, y) in &points {
        graph[y][x] = '●';
    }

    println!("{title} bandwidth MiB/s");
    for (index, row) in graph.iter().enumerate() {
        let rate = upper - i64::try_from(index).unwrap_or(0);
        println!("{rate:>6}.0 ┤ {}", row.iter().collect::<String>());
    }
    println!("       └{}", "─".repeat(width));
    print!("        ");
    for (millis, _) in samples {
        print!("{millis:>6}");
    }
    println!(" ms");
    print!("  Rate  ");
    for (_, rate) in samples {
        print!("{rate:>6.1}");
    }
    println!(" MiB/s");
    println!();
    println!("  Hard {hard_mibps:>5.1} MiB/s {}", "─".repeat(width));
    println!("  Soft {soft_mibps:>5.1} MiB/s {}", "─".repeat(width));
}

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

#[tokio::test(flavor = "multi_thread")]
async fn copy_starting_with_full_credit_runs_at_but_not_above_the_hard_limit() {
    const MIB: usize = 1024 * 1024;
    const FILE_SIZE: usize = 12 * MIB;
    let suffix = std::process::id();
    let src_dir = format!("/tmp/data-mover-copy-qos-dual-src-{suffix}");
    let dst_dir = format!("/tmp/data-mover-copy-qos-dual-dst-{suffix}");
    let _ = tokio::fs::remove_dir_all(&src_dir).await;
    let _ = tokio::fs::remove_dir_all(&dst_dir).await;
    tokio::fs::create_dir_all(&src_dir)
        .await
        .assert_value("create source directory");
    tokio::fs::create_dir_all(&dst_dir)
        .await
        .assert_value("create destination directory");
    tokio::fs::write(format!("{src_dir}/blob.bin"), vec![0x3c; FILE_SIZE])
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
    let qos = QosManager::try_new_with_limits(
        "8MiB/s",
        "12MiB/s",
        Duration::from_millis(500),
        MIB as u64,
        Some(10_000),
    )
    .assert_value("create dual-rate qos");

    assert_eq!(qos.config().credit_capacity_bytes, Some(2 * MIB as u64));
    // At transfer start the derived 2 MiB credit is full. At 12 MiB/s it is
    // consumed net of the 8 MiB/s refill in 500 ms, during which 6 MiB moves.
    // The remaining 6 MiB then exposes the return to the 8 MiB/s soft rate.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let copy = StorageEnum::copy_file(
        &src,
        &dst,
        &entry,
        CopyOptions {
            qos: Some(qos.clone()),
            is_source_reserved: true,
            ..Default::default()
        },
    );
    let elapsed = run_copy_with_bandwidth_samples(copy, &qos, 8.0, 12.0)
        .await
        .assert_value("copy file with samples");

    assert!(
        elapsed >= Duration::from_millis(1150),
        "12 MiB with only 500 ms of peak credit must not finish too early; got {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(1400),
        "full 2 MiB credit should beat the 1.5 s soft-only duration; got {elapsed:?}"
    );
    assert_eq!(
        qos.stats().total_bytes.load(Ordering::Relaxed),
        FILE_SIZE as u64
    );

    let _ = tokio::fs::remove_dir_all(src_dir).await;
    let _ = tokio::fs::remove_dir_all(dst_dir).await;
}
