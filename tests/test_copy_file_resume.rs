//! Tests for `StorageEnum::copy_file_resumable` (byte-level resume).
//!
//! Local-only — no S3/NFS server required.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use data_mover::{CommitCallback, ResumeContext, StorageEnum, create_storage};

const BLOCK: u64 = 64 * 1024;
const SIZE: usize = 256 * 1024; // 4 blocks → multi-chunk

async fn reset_dirs(src: &str, dst: &str) {
    let _ = tokio::fs::remove_dir_all(src).await;
    let _ = tokio::fs::remove_dir_all(dst).await;
    tokio::fs::create_dir_all(src).await.unwrap();
    tokio::fs::create_dir_all(dst).await.unwrap();
}

/// 写一个内容可验证的 blob：byte i = (i % 251)。
async fn write_pattern(path: &str, size: usize) {
    use tokio::io::AsyncWriteExt;
    let mut f = tokio::fs::File::create(path).await.unwrap();
    let buf: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    f.write_all(&buf).await.unwrap();
    f.flush().await.unwrap();
}

fn pattern_vec(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

fn collecting_callback() -> (CommitCallback, Arc<Mutex<Vec<(u64, u64)>>>) {
    let committed: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let c = committed.clone();
    let cb: CommitCallback = Arc::new(move |offset, len| {
        c.lock().unwrap().push((offset, len));
    });
    (cb, committed)
}

#[tokio::test(flavor = "multi_thread")]
async fn resumable_fresh_full_copy() {
    let src_dir = "/tmp/dm-resume-fresh-src";
    let dst_dir = "/tmp/dm-resume-fresh-dst";
    reset_dirs(src_dir, dst_dir).await;
    write_pattern(&format!("{src_dir}/blob.bin"), SIZE).await;

    let src = create_storage(src_dir, Some(BLOCK), false).await.unwrap();
    let dst = create_storage(dst_dir, Some(BLOCK), true).await.unwrap();
    let entry = src.get_metadata(Path::new("blob.bin")).await.unwrap();

    let (cb, committed) = collecting_callback();
    let counter = Arc::new(AtomicU64::new(0));
    let resume = ResumeContext {
        part_relative_path: Path::new("blob.bin.terrasync-part").to_path_buf(),
        missing_intervals: vec![(0, SIZE as u64)],
        on_committed: cb,
    };

    StorageEnum::copy_file_resumable(
        &src,
        &dst,
        &entry,
        None,
        false,
        true,
        Some(counter.clone()),
        resume,
    )
    .await
    .expect("resumable full copy");

    // 最终文件存在且内容正确
    let out = tokio::fs::read(format!("{dst_dir}/blob.bin"))
        .await
        .unwrap();
    assert_eq!(out, pattern_vec(SIZE), "final content mismatch");
    // .part 已 rename 消失
    assert!(
        tokio::fs::metadata(format!("{dst_dir}/blob.bin.terrasync-part"))
            .await
            .is_err(),
        ".part should be renamed away"
    );
    // 回调上报 (offset, len)，累计 len == 文件大小
    let total: u64 = committed
        .lock()
        .unwrap()
        .iter()
        .map(|(_offset, len)| len)
        .sum();
    assert_eq!(total, SIZE as u64, "committed coverage should equal size");
    assert_eq!(counter.load(Ordering::Relaxed), SIZE as u64);
}

#[tokio::test(flavor = "multi_thread")]
async fn resumable_continues_from_partial_part() {
    let src_dir = "/tmp/dm-resume-cont-src";
    let dst_dir = "/tmp/dm-resume-cont-dst";
    reset_dirs(src_dir, dst_dir).await;
    write_pattern(&format!("{src_dir}/blob.bin"), SIZE).await;

    // 预置 .part：已写正确的前半段（模拟上次中断）
    let half = SIZE / 2;
    let full = pattern_vec(SIZE);
    tokio::fs::write(format!("{dst_dir}/blob.bin.terrasync-part"), &full[..half])
        .await
        .unwrap();

    let src = create_storage(src_dir, Some(BLOCK), false).await.unwrap();
    let dst = create_storage(dst_dir, Some(BLOCK), true).await.unwrap();
    let entry = src.get_metadata(Path::new("blob.bin")).await.unwrap();

    let (cb, _committed) = collecting_callback();
    let counter = Arc::new(AtomicU64::new(0));
    let resume = ResumeContext {
        part_relative_path: Path::new("blob.bin.terrasync-part").to_path_buf(),
        // 只补后半段
        missing_intervals: vec![(half as u64, SIZE as u64)],
        on_committed: cb,
    };

    StorageEnum::copy_file_resumable(
        &src,
        &dst,
        &entry,
        None,
        false,
        true,
        Some(counter.clone()),
        resume,
    )
    .await
    .expect("resumable continue copy");

    // 最终文件 = 完整内容（前半保留 + 后半续写）
    let out = tokio::fs::read(format!("{dst_dir}/blob.bin"))
        .await
        .unwrap();
    assert_eq!(out, full, "resumed file content mismatch");
    // 只写了后半段
    assert_eq!(
        counter.load(Ordering::Relaxed),
        (SIZE - half) as u64,
        "should only write missing half"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn resumable_truncates_leftover_tail() {
    let src_dir = "/tmp/dm-resume-trunc-src";
    let dst_dir = "/tmp/dm-resume-trunc-dst";
    reset_dirs(src_dir, dst_dir).await;
    write_pattern(&format!("{src_dir}/blob.bin"), SIZE).await;

    // 预置一个比目标更长的 .part（遗留尾部垃圾），missing=full → 全量重写 + set_len 规整
    let mut longer = pattern_vec(SIZE);
    longer.extend(std::iter::repeat_n(0xFFu8, 4096));
    tokio::fs::write(format!("{dst_dir}/blob.bin.terrasync-part"), &longer)
        .await
        .unwrap();

    let src = create_storage(src_dir, Some(BLOCK), false).await.unwrap();
    let dst = create_storage(dst_dir, Some(BLOCK), true).await.unwrap();
    let entry = src.get_metadata(Path::new("blob.bin")).await.unwrap();

    let (cb, _committed) = collecting_callback();
    let resume = ResumeContext {
        part_relative_path: Path::new("blob.bin.terrasync-part").to_path_buf(),
        missing_intervals: vec![(0, SIZE as u64)],
        on_committed: cb,
    };

    StorageEnum::copy_file_resumable(&src, &dst, &entry, None, false, true, None, resume)
        .await
        .expect("resumable truncate copy");

    let out = tokio::fs::read(format!("{dst_dir}/blob.bin"))
        .await
        .unwrap();
    assert_eq!(
        out.len(),
        SIZE,
        "leftover tail must be truncated to exact size"
    );
    assert_eq!(out, pattern_vec(SIZE), "content mismatch after truncate");
}
