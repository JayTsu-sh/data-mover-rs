//! T2–T5 — issue #21：`resume_prepare`/`write_chunk_stream`/`commit_chunk_stream`
//! 三段式字节级续传 API 的单测。
//!
//! Local-only — no S3/NFS server required. S3 特有分支（part 对齐、
//! non-contiguous 报错、`ListParts` 反推）与 NFS/CIFS 的落盘语义差异需要真实
//! 存储环境，未在本地覆盖；由 issue #21 测试计划标注为需在有存储环境时补跑。

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use data_mover::error::StorageError;
use data_mover::{CommitCallback, DataChunk, ResumeContext, StorageEnum, create_storage};

const BLOCK: u64 = 64 * 1024;
const SIZE: usize = 256 * 1024; // 4 blocks → multi-chunk

async fn reset_dirs(a: &str, b: &str) {
    let _ = tokio::fs::remove_dir_all(a).await;
    let _ = tokio::fs::remove_dir_all(b).await;
    tokio::fs::create_dir_all(a).await.unwrap();
    tokio::fs::create_dir_all(b).await.unwrap();
}

async fn write_pattern(path: &str, size: usize, seed: u8) {
    use tokio::io::AsyncWriteExt;
    let mut f = tokio::fs::File::create(path).await.unwrap();
    let buf = pattern_vec(size, seed);
    f.write_all(&buf).await.unwrap();
    f.flush().await.unwrap();
}

fn pattern_vec(size: usize, seed: u8) -> Vec<u8> {
    (0..size)
        .map(|i| ((i as u8).wrapping_add(seed)) % 251)
        .collect()
}

fn noop_callback() -> CommitCallback {
    Arc::new(|_offset, _len| {})
}

fn collecting_callback() -> (CommitCallback, Arc<Mutex<Vec<(u64, u64)>>>) {
    let committed: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let c = committed.clone();
    let cb: CommitCallback = Arc::new(move |offset, len| {
        c.lock().unwrap().push((offset, len));
    });
    (cb, committed)
}

/// 构造一个仅用于取 `EntryEnum` 形状（`relative_path`/`size`/`uid`/`gid`/`mode`）
/// 的源目录；测试里不通过它实际读取数据，DataChunk 由测试直接手工构造。
async fn shape_entry(dir: &str, name: &str, size: usize, seed: u8) -> data_mover::EntryEnum {
    write_pattern(&format!("{dir}/{name}"), size, seed).await;
    let storage = create_storage(dir, Some(BLOCK), false).await.unwrap();
    storage.get_metadata(Path::new(name)).await.unwrap()
}

/// T2：`write_chunk_stream` 顺序写 → `commit_chunk_stream`（rename）原子提交生效。
#[tokio::test(flavor = "multi_thread")]
async fn t2_write_chunk_stream_sequential_then_commit_is_atomic() {
    let shape_dir = "/tmp/dm-t2-shape";
    let dst_dir = "/tmp/dm-t2-dst";
    reset_dirs(shape_dir, dst_dir).await;

    let entry = shape_entry(shape_dir, "blob.bin", SIZE, 0).await;
    let dst = create_storage(dst_dir, Some(BLOCK), true).await.unwrap();
    let part_path = Path::new("blob.bin.terrasync-part");

    let (missing, handle) = StorageEnum::resume_prepare(&dst, &entry, part_path, false)
        .await
        .expect("resume_prepare");
    assert_eq!(
        missing,
        vec![(0, SIZE as u64)],
        "fresh prepare covers full file"
    );

    let full = pattern_vec(SIZE, 0);
    let half = SIZE / 2;
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(DataChunk {
        offset: 0,
        data: Bytes::copy_from_slice(&full[..half]),
    })
    .await
    .unwrap();
    tx.send(DataChunk {
        offset: half as u64,
        data: Bytes::copy_from_slice(&full[half..]),
    })
    .await
    .unwrap();
    drop(tx);

    let (cb, committed) = collecting_callback();
    StorageEnum::write_chunk_stream(&dst, &entry, rx, &handle, None, cb)
        .await
        .expect("write_chunk_stream");

    let part_full_path = format!("{dst_dir}/blob.bin.terrasync-part");
    let final_full_path = format!("{dst_dir}/blob.bin");

    // 提交前：.part 内容已正确写完，最终路径尚不存在。
    assert_eq!(tokio::fs::read(&part_full_path).await.unwrap(), full);
    assert!(
        tokio::fs::metadata(&final_full_path).await.is_err(),
        "final path must not exist before commit_chunk_stream"
    );
    let total: u64 = committed.lock().unwrap().iter().map(|(_, l)| l).sum();
    assert_eq!(
        total, SIZE as u64,
        "on_committed coverage should equal size"
    );

    StorageEnum::commit_chunk_stream(&dst, &entry, SIZE as u64, handle)
        .await
        .expect("commit_chunk_stream");

    // 提交后：最终路径存在且内容正确，.part 因 rename 消失。
    assert_eq!(tokio::fs::read(&final_full_path).await.unwrap(), full);
    assert!(
        tokio::fs::metadata(&part_full_path).await.is_err(),
        ".part should be renamed away after commit"
    );
}

/// T3：`write_chunk_stream` 对乱序、重复的 chunk 按 offset 幂等（NAS 随机写，
/// 后写覆盖同一 offset 区间不会破坏最终内容）。
///
/// 「缺 chunk → commit 前拦截」子场景与 T5 共用同一条 hash 校验路径，见 T5。
#[tokio::test(flavor = "multi_thread")]
async fn t3_write_chunk_stream_out_of_order_and_duplicate_is_idempotent() {
    let shape_dir = "/tmp/dm-t3-shape";
    let dst_dir = "/tmp/dm-t3-dst";
    reset_dirs(shape_dir, dst_dir).await;

    let entry = shape_entry(shape_dir, "blob.bin", SIZE, 0).await;
    let dst = create_storage(dst_dir, Some(BLOCK), true).await.unwrap();
    let part_path = Path::new("blob.bin.terrasync-part");

    let (_missing, handle) = StorageEnum::resume_prepare(&dst, &entry, part_path, false)
        .await
        .expect("resume_prepare");

    let full = pattern_vec(SIZE, 0);
    let half = SIZE / 2;
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    // 乱序：先发后半，再发前半，最后重复发送一次前半（幂等写同一 offset 区间）。
    tx.send(DataChunk {
        offset: half as u64,
        data: Bytes::copy_from_slice(&full[half..]),
    })
    .await
    .unwrap();
    tx.send(DataChunk {
        offset: 0,
        data: Bytes::copy_from_slice(&full[..half]),
    })
    .await
    .unwrap();
    tx.send(DataChunk {
        offset: 0,
        data: Bytes::copy_from_slice(&full[..half]),
    })
    .await
    .unwrap();
    drop(tx);

    StorageEnum::write_chunk_stream(&dst, &entry, rx, &handle, None, noop_callback())
        .await
        .expect("write_chunk_stream with out-of-order + duplicate chunks");

    StorageEnum::commit_chunk_stream(&dst, &entry, SIZE as u64, handle)
        .await
        .expect("commit_chunk_stream");

    let out = tokio::fs::read(format!("{dst_dir}/blob.bin"))
        .await
        .unwrap();
    assert_eq!(
        out, full,
        "out-of-order + duplicate offset writes must not corrupt final content"
    );
}

/// T4：中途中断（drop 发送端）后，重跑 `resume_prepare` 只反推真正缺失的区间，
/// 重跑 `write_chunk_stream` 只补齐该区间。
#[tokio::test(flavor = "multi_thread")]
async fn t4_resume_after_partial_write_only_fills_missing_range() {
    let shape_dir = "/tmp/dm-t4-shape";
    let dst_dir = "/tmp/dm-t4-dst";
    reset_dirs(shape_dir, dst_dir).await;

    let entry = shape_entry(shape_dir, "blob.bin", SIZE, 0).await;
    let dst = create_storage(dst_dir, Some(BLOCK), true).await.unwrap();
    let part_path = Path::new("blob.bin.terrasync-part");

    let (missing1, handle1) = StorageEnum::resume_prepare(&dst, &entry, part_path, false)
        .await
        .expect("first resume_prepare");
    assert_eq!(missing1, vec![(0, SIZE as u64)]);

    let full = pattern_vec(SIZE, 0);
    let half = SIZE / 2;

    // 只送前半，随后 drop 发送端，模拟传输中途中断。
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(DataChunk {
        offset: 0,
        data: Bytes::copy_from_slice(&full[..half]),
    })
    .await
    .unwrap();
    drop(tx);

    let counter1 = Arc::new(AtomicU64::new(0));
    StorageEnum::write_chunk_stream(
        &dst,
        &entry,
        rx,
        &handle1,
        Some(counter1.clone()),
        noop_callback(),
    )
    .await
    .expect("partial write_chunk_stream");
    assert_eq!(counter1.load(Ordering::Relaxed), half as u64);

    // 重跑 prepare：应只反推缺失的后半区间。
    let (missing2, handle2) = StorageEnum::resume_prepare(&dst, &entry, part_path, true)
        .await
        .expect("second resume_prepare (resume=true)");
    assert_eq!(
        missing2,
        vec![(half as u64, SIZE as u64)],
        "resume_prepare must only report the truly missing tail"
    );

    // 只补后半。
    let (tx2, rx2) = tokio::sync::mpsc::channel(4);
    tx2.send(DataChunk {
        offset: half as u64,
        data: Bytes::copy_from_slice(&full[half..]),
    })
    .await
    .unwrap();
    drop(tx2);

    let counter2 = Arc::new(AtomicU64::new(0));
    StorageEnum::write_chunk_stream(
        &dst,
        &entry,
        rx2,
        &handle2,
        Some(counter2.clone()),
        noop_callback(),
    )
    .await
    .expect("resumed write_chunk_stream");
    assert_eq!(
        counter2.load(Ordering::Relaxed),
        (SIZE - half) as u64,
        "resumed write should only transfer the missing half"
    );

    StorageEnum::commit_chunk_stream(&dst, &entry, SIZE as u64, handle2)
        .await
        .expect("commit_chunk_stream");

    let out = tokio::fs::read(format!("{dst_dir}/blob.bin"))
        .await
        .unwrap();
    assert_eq!(out, full, "final content must be complete after resume");
}

/// T5：hash 校验失败 → 不触发 rename（commit），最终路径不被污染，`.part`
/// 原样保留供后续重试。
#[tokio::test(flavor = "multi_thread")]
async fn t5_hash_mismatch_blocks_commit_and_preserves_partial() {
    let shape_dir = "/tmp/dm-t5-shape";
    let dst_dir = "/tmp/dm-t5-dst";
    reset_dirs(shape_dir, dst_dir).await;

    let entry = shape_entry(shape_dir, "blob.bin", SIZE, 0).await;
    let shape = create_storage(shape_dir, Some(BLOCK), false).await.unwrap();
    let dst = create_storage(dst_dir, Some(BLOCK), true).await.unwrap();

    // 预置一个内容全错的 .part（模拟数据损坏/缺 chunk 后残留），但调用方
    // 误以为已经补齐（missing_intervals 为空）。
    let corrupted = vec![0xEEu8; SIZE];
    let part_full_path = format!("{dst_dir}/blob.bin.terrasync-part");
    tokio::fs::write(&part_full_path, &corrupted).await.unwrap();

    let (cb, _committed) = collecting_callback();
    let resume = ResumeContext {
        part_relative_path: Path::new("blob.bin.terrasync-part").to_path_buf(),
        missing_intervals: vec![],
        on_committed: cb,
    };

    let res =
        StorageEnum::copy_file_resumable(&shape, &dst, &entry, None, true, true, None, resume)
            .await;
    assert!(
        matches!(res, Err(StorageError::OperationError(_))),
        "hash mismatch must surface as an error, got {res:?}"
    );

    let final_full_path = format!("{dst_dir}/blob.bin");
    assert!(
        tokio::fs::metadata(&final_full_path).await.is_err(),
        "final path must not exist: commit (rename) must not have happened"
    );
    let part = tokio::fs::read(&part_full_path).await.unwrap();
    assert_eq!(
        part, corrupted,
        ".part must be preserved unchanged for a future retry, not deleted or renamed"
    );
}
