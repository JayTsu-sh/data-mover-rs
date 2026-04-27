//! Regression tests for `StorageEnum::copy_file_with_cancel`.
//!
//! Local-only — no S3/NFS server required.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use data_mover::error::StorageError;
use data_mover::{QosManager, StorageEnum, create_storage};
use tokio_util::sync::CancellationToken;

const SRC_DIR: &str = "/tmp/data-mover-cancel-src";
const DST_DIR: &str = "/tmp/data-mover-cancel-dst";

async fn reset_dirs(src: &str, dst: &str) {
    let _ = tokio::fs::remove_dir_all(src).await;
    let _ = tokio::fs::remove_dir_all(dst).await;
    tokio::fs::create_dir_all(src).await.unwrap();
    tokio::fs::create_dir_all(dst).await.unwrap();
}

async fn write_blob(path: &str, size: usize) {
    use tokio::io::AsyncWriteExt;
    let mut f = tokio::fs::File::create(path).await.unwrap();
    let buf = vec![0xCDu8; 64 * 1024];
    let mut written = 0;
    while written < size {
        let n = (size - written).min(buf.len());
        f.write_all(&buf[..n]).await.unwrap();
        written += n;
    }
    f.flush().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_file_returns_cancelled_when_token_pre_cancelled() {
    let src_dir = format!("{SRC_DIR}-pre");
    let dst_dir = format!("{DST_DIR}-pre");
    reset_dirs(&src_dir, &dst_dir).await;
    let blob = format!("{src_dir}/blob.bin");
    write_blob(&blob, 1024).await;

    let src = create_storage(&src_dir, None).await.unwrap();
    let dst = create_storage(&dst_dir, None).await.unwrap();
    let entry = src.get_metadata(Path::new("blob.bin")).await.unwrap();

    let token = CancellationToken::new();
    token.cancel();

    let res = StorageEnum::copy_file_with_cancel(&src, &dst, &entry, None, false, true, None, Some(token)).await;

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

    let src = create_storage(&src_dir, None).await.unwrap();
    let dst = create_storage(&dst_dir, None).await.unwrap();
    let entry = src.get_metadata(Path::new("blob.bin")).await.unwrap();

    let qos = QosManager::try_new(Some("4MiB/s"), 1.0, None).unwrap();
    let counter = Arc::new(AtomicU64::new(0));
    let token = CancellationToken::new();

    let token2 = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        token2.cancel();
    });

    let started = Instant::now();
    let res = StorageEnum::copy_file_with_cancel(
        &src,
        &dst,
        &entry,
        Some(qos),
        false,
        true,
        Some(counter.clone()),
        Some(token),
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

    let src = create_storage(&src_dir, None).await.unwrap();
    let dst = create_storage(&dst_dir, None).await.unwrap();
    let entry = src.get_metadata(Path::new("blob.bin")).await.unwrap();

    // Old (unchanged) signature — must still work.
    StorageEnum::copy_file(&src, &dst, &entry, None, false, true, None)
        .await
        .expect("legacy copy_file path");
    assert!(dst.get_metadata(Path::new("blob.bin")).await.is_ok());
}
