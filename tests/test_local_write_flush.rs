//! Regression: `LocalStorage::write()` 全量写路径（`copy_file` → `write_data`）
//! 最后一个 chunk 写完后未 flush，后台阻塞写任务可能在句柄 drop 前尚未落盘，
//! 概率性（约 1/10~1/50）丢失文件尾字节且无报错。
//!
//! 这是压力式复现：单次全量拷贝一个「尾块很小」的文件、逐字节校验,循环多次以
//! 稳定暴露该 race（修复前多次迭代内几乎必现；修复后恒过、确定性通过）。
//! Local-only — 无需任何外部存储服务。
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;

use data_mover::{StorageEnum, create_storage};

const BLOCK: u64 = 64 * 1024;
// 4 个整块 + 一个 137B 小尾块 → 最后一次 write() 写小 chunk、其后无 seek,正是暴露点。
const SIZE: usize = 4 * 64 * 1024 + 137;
const ITERS: usize = 256;

async fn write_pattern(path: &str, size: usize) {
    use tokio::io::AsyncWriteExt;
    let mut f = tokio::fs::File::create(path).await.unwrap();
    let buf: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    f.write_all(&buf).await.unwrap();
    f.flush().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn full_copy_no_tail_loss_under_repetition() {
    let src_dir = "/tmp/dm-flush-src";
    let dst_dir = "/tmp/dm-flush-dst";
    let expected: Vec<u8> = (0..SIZE).map(|i| (i % 251) as u8).collect();

    for iter in 0..ITERS {
        let _ = tokio::fs::remove_dir_all(src_dir).await;
        let _ = tokio::fs::remove_dir_all(dst_dir).await;
        tokio::fs::create_dir_all(src_dir).await.unwrap();
        tokio::fs::create_dir_all(dst_dir).await.unwrap();
        write_pattern(&format!("{src_dir}/blob.bin"), SIZE).await;

        let src = create_storage(src_dir, Some(BLOCK), false).await.unwrap();
        let dst = create_storage(dst_dir, Some(BLOCK), true).await.unwrap();
        let entry = src.get_metadata(Path::new("blob.bin")).await.unwrap();

        StorageEnum::copy_file(&src, &dst, &entry, None, false, true, None)
            .await
            .unwrap();

        let got = tokio::fs::read(format!("{dst_dir}/blob.bin")).await.unwrap();
        assert_eq!(
            got.len(),
            expected.len(),
            "iter {iter}: 目标文件长度不符（尾字节丢失）"
        );
        assert_eq!(got, expected, "iter {iter}: 目标内容与源逐字节不符");
    }
}
