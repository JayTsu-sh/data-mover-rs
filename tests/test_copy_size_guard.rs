//! issue #58 — `copy_file` 写端字节计数断言 + mismatch 清理的截断注入测试。
//!
//! 注入方式：`get_metadata` 取 entry（记录原 size）后截断源文件，模拟扫描与
//! 拷贝之间源被并发截断/变更——读端提前 EOF，旧行为写端静默少写、拷贝
//! "成功"。新行为：无论 `enable_integrity_check` 开关，写端本地计数断言
//! 拦截并保证目标端无残留坏文件。
//!
//! Local-only — no S3/NFS server required. S3 multipart 续传的会话字节断言
//! （Complete 前不提交）需要真实 S3 环境，未在本地覆盖。NAS 续传路径回归
//! 由 tests/test_copy_file_resume.rs 既有用例保障。
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;

use data_mover::{StorageEnum, create_storage};

const BLOCK: u64 = 64 * 1024;
const SINGLE_SIZE: usize = 8 * 1024; // < BLOCK → 单块路径
const MULTI_SIZE: usize = 256 * 1024; // 4 blocks → 多块 pipeline

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

fn truncate_file(path: &str, len: u64) {
    let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.set_len(len).unwrap();
}

/// 取 entry 后截断源文件再 copy_file，断言 Err + 目标端无残留。
async fn assert_truncated_copy_guarded(
    tag: &str,
    size: usize,
    truncated: u64,
    enable_integrity_check: bool,
) {
    let src_dir = format!("/tmp/dm-sizeguard-{tag}-src");
    let dst_dir = format!("/tmp/dm-sizeguard-{tag}-dst");
    reset_dirs(&src_dir, &dst_dir).await;
    write_pattern(&format!("{src_dir}/blob.bin"), size).await;

    let src = create_storage(&src_dir, Some(BLOCK), false).await.unwrap();
    let dst = create_storage(&dst_dir, Some(BLOCK), true).await.unwrap();
    let entry = src.get_metadata(Path::new("blob.bin")).await.unwrap();

    // 注入：entry 已带原 size，此时截断源文件（模拟扫描后源被并发变更）
    truncate_file(&format!("{src_dir}/blob.bin"), truncated);

    let err = StorageEnum::copy_file(
        &src,
        &dst,
        &entry,
        None,
        enable_integrity_check,
        true,
        None,
    )
    .await
    .expect_err("truncated source must fail the copy");
    assert!(
        err.to_string().contains("size check failed"),
        "unexpected error: {err}"
    );
    // 目标端无残留坏文件（单块：写前拦截未落地；多块：落地后被清理）
    assert!(
        tokio::fs::metadata(format!("{dst_dir}/blob.bin"))
            .await
            .is_err(),
        "destination must have no residual bad file"
    );
}

/// 单块路径 + integrity 关闭：写前本地计数断言拦截，目标无残留。
#[tokio::test(flavor = "multi_thread")]
async fn single_chunk_truncated_source_no_integrity() {
    assert_truncated_copy_guarded("single-noint", SINGLE_SIZE, 5 * 1024, false).await;
}

/// 单块路径 + integrity 开启：同样在写前拦截（断言不依赖开关）。
#[tokio::test(flavor = "multi_thread")]
async fn single_chunk_truncated_source_with_integrity() {
    assert_truncated_copy_guarded("single-int", SINGLE_SIZE, 5 * 1024, true).await;
}

/// 多块 pipeline + integrity 关闭：写端累计计数断言拦截并清理已落地坏文件。
#[tokio::test(flavor = "multi_thread")]
async fn multi_chunk_truncated_source_no_integrity() {
    assert_truncated_copy_guarded("multi-noint", MULTI_SIZE, 150 * 1024, false).await;
}

/// 多块 pipeline + integrity 开启：计数断言先于 hash 读回拦截，目标被清理。
#[tokio::test(flavor = "multi_thread")]
async fn multi_chunk_truncated_source_with_integrity() {
    assert_truncated_copy_guarded("multi-int", MULTI_SIZE, 150 * 1024, true).await;
}

/// happy path 回归：新断言不影响正常拷贝（单块 + 多块，integrity 开启）。
#[tokio::test(flavor = "multi_thread")]
async fn intact_copy_still_succeeds() {
    for (tag, size) in [("happy-single", SINGLE_SIZE), ("happy-multi", MULTI_SIZE)] {
        let src_dir = format!("/tmp/dm-sizeguard-{tag}-src");
        let dst_dir = format!("/tmp/dm-sizeguard-{tag}-dst");
        reset_dirs(&src_dir, &dst_dir).await;
        write_pattern(&format!("{src_dir}/blob.bin"), size).await;

        let src = create_storage(&src_dir, Some(BLOCK), false).await.unwrap();
        let dst = create_storage(&dst_dir, Some(BLOCK), true).await.unwrap();
        let entry = src.get_metadata(Path::new("blob.bin")).await.unwrap();

        StorageEnum::copy_file(&src, &dst, &entry, None, true, true, None)
            .await
            .expect("intact copy must succeed");
        let out = tokio::fs::read(format!("{dst_dir}/blob.bin")).await.unwrap();
        assert_eq!(out, pattern_vec(size), "content mismatch for {tag}");
    }
}
