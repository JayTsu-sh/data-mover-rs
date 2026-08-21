//! Regression tests for `StorageEnum::copy_file_with_cancel`.
//!
//! Local-only — no S3/NFS server required.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use data_mover::error::StorageError;
use data_mover::{QosManager, ResumeContext, StorageEnum, create_storage};
use tokio_util::sync::CancellationToken;

trait AssertTestValue {
    type Value;
    fn assert_value(self, context: &str) -> Self::Value;
}

impl<T, E: std::fmt::Debug> AssertTestValue for Result<T, E> {
    type Value = T;

    fn assert_value(self, context: &str) -> T {
        match self {
            Ok(value) => value,
            Err(error) => panic!("{context}: {error:?}"),
        }
    }
}

const SRC_DIR: &str = "/tmp/data-mover-cancel-src";
const DST_DIR: &str = "/tmp/data-mover-cancel-dst";

async fn reset_dirs(src: &str, dst: &str) {
    let _ = tokio::fs::remove_dir_all(src).await;
    let _ = tokio::fs::remove_dir_all(dst).await;
    tokio::fs::create_dir_all(src)
        .await
        .assert_value("test value should be present");
    tokio::fs::create_dir_all(dst)
        .await
        .assert_value("test value should be present");
}

async fn write_blob(path: &str, size: usize) {
    use tokio::io::AsyncWriteExt;
    let mut f = tokio::fs::File::create(path)
        .await
        .assert_value("test value should be present");
    let buf = vec![0xCDu8; 64 * 1024];
    let mut written = 0;
    while written < size {
        let n = (size - written).min(buf.len());
        f.write_all(&buf[..n])
            .await
            .assert_value("test value should be present");
        written += n;
    }
    f.flush().await.assert_value("test value should be present");
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_file_returns_cancelled_when_token_pre_cancelled() {
    let src_dir = format!("{SRC_DIR}-pre");
    let dst_dir = format!("{DST_DIR}-pre");
    reset_dirs(&src_dir, &dst_dir).await;
    let blob = format!("{src_dir}/blob.bin");
    write_blob(&blob, 1024).await;

    let src = create_storage(&src_dir, data_mover::CreateStorageOptions::default())
        .await
        .assert_value("test value should be present");
    let dst = create_storage(&dst_dir, data_mover::CreateStorageOptions::new(None, true))
        .await
        .assert_value("test value should be present");
    let entry = src
        .get_metadata(Path::new("blob.bin"))
        .await
        .assert_value("test value should be present");

    let token = CancellationToken::new();
    token.cancel();

    let res = StorageEnum::copy_file(
        &src,
        &dst,
        &entry,
        data_mover::CopyOptions {
            is_source_reserved: true,
            cancel: Some(token),
            ..Default::default()
        },
    )
    .await;

    assert!(
        matches!(res, Err(StorageError::Cancelled)),
        "expected Cancelled, got {res:?}"
    );
    assert!(
        dst.get_metadata(Path::new("blob.bin")).await.is_err(),
        "no destination object should have been written"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_file_aborts_mid_transfer_on_token_cancel() {
    // 16 MiB blob + 4 MiB/s QoS → ~4 s without cancel.
    // Cancel after 200 ms → expect Cancelled within ~1 s (one chunk).
    let src_dir = format!("{SRC_DIR}-mid");
    let dst_dir = format!("{DST_DIR}-mid");
    reset_dirs(&src_dir, &dst_dir).await;
    let blob = format!("{src_dir}/blob.bin");
    write_blob(&blob, 16 * 1024 * 1024).await;

    let src = create_storage(&src_dir, data_mover::CreateStorageOptions::default())
        .await
        .assert_value("test value should be present");
    let dst = create_storage(&dst_dir, data_mover::CreateStorageOptions::new(None, true))
        .await
        .assert_value("test value should be present");
    let entry = src
        .get_metadata(Path::new("blob.bin"))
        .await
        .assert_value("test value should be present");

    let qos =
        QosManager::try_new(Some("4MiB/s"), 1.0, None).assert_value("test value should be present");
    let counter = Arc::new(AtomicU64::new(0));
    let token = CancellationToken::new();

    let token2 = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        token2.cancel();
    });

    let started = Instant::now();
    let res = StorageEnum::copy_file(
        &src,
        &dst,
        &entry,
        data_mover::CopyOptions {
            qos: Some(qos),
            is_source_reserved: true,
            bytes_counter: Some(counter.clone()),
            cancel: Some(token),
            ..Default::default()
        },
    )
    .await;
    let elapsed = started.elapsed();

    assert!(
        matches!(res, Err(StorageError::Cancelled)),
        "expected Cancelled, got {res:?}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "cancel should resolve well before unconstrained 4 s deadline (took {elapsed:?})"
    );
    assert!(
        counter.load(Ordering::Relaxed) < 16 * 1024 * 1024,
        "should not have transferred the full blob before cancel"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_file_without_cancel_still_works_via_compat_wrapper() {
    let src_dir = format!("{SRC_DIR}-compat");
    let dst_dir = format!("{DST_DIR}-compat");
    reset_dirs(&src_dir, &dst_dir).await;
    let blob = format!("{src_dir}/blob.bin");
    write_blob(&blob, 256 * 1024).await;

    let src = create_storage(&src_dir, data_mover::CreateStorageOptions::default())
        .await
        .assert_value("test value should be present");
    let dst = create_storage(&dst_dir, data_mover::CreateStorageOptions::new(None, true))
        .await
        .assert_value("test value should be present");
    let entry = src
        .get_metadata(Path::new("blob.bin"))
        .await
        .assert_value("test value should be present");

    // Old (unchanged) signature — must still work.
    StorageEnum::copy_file(
        &src,
        &dst,
        &entry,
        data_mover::CopyOptions {
            is_source_reserved: true,
            ..Default::default()
        },
    )
    .await
    .assert_value("legacy copy_file path");
    assert!(dst.get_metadata(Path::new("blob.bin")).await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn resumable_nas_copy_returns_cancelled_before_touching_destination() {
    let src_dir = format!("{SRC_DIR}-resume-pre");
    let dst_dir = format!("{DST_DIR}-resume-pre");
    reset_dirs(&src_dir, &dst_dir).await;
    let blob = format!("{src_dir}/blob.bin");
    write_blob(&blob, 1024).await;
    tokio::fs::write(format!("{dst_dir}/blob.bin"), b"old-final")
        .await
        .assert_value("seed final destination");

    let src = create_storage(&src_dir, data_mover::CreateStorageOptions::default())
        .await
        .assert_value("create resume source");
    let dst = create_storage(&dst_dir, data_mover::CreateStorageOptions::new(None, true))
        .await
        .assert_value("create resume destination");
    let entry = src
        .get_metadata(Path::new("blob.bin"))
        .await
        .assert_value("read resume source metadata");
    let token = CancellationToken::new();
    token.cancel();

    let result = StorageEnum::copy_file_resumable(
        &src,
        &dst,
        &entry,
        data_mover::CopyOptions {
            is_source_reserved: true,
            cancel: Some(token),
            ..Default::default()
        },
        ResumeContext {
            part_relative_path: "blob.bin.part".into(),
            missing_intervals: vec![(0, entry.get_size())],
            on_committed: Arc::new(|_, _| {}),
        },
    )
    .await;

    assert!(matches!(result, Err(StorageError::Cancelled)));
    assert_eq!(
        tokio::fs::read(format!("{dst_dir}/blob.bin"))
            .await
            .assert_value("read preserved final destination"),
        b"old-final"
    );
    assert!(
        tokio::fs::metadata(format!("{dst_dir}/blob.bin.part"))
            .await
            .is_err()
    );
}
