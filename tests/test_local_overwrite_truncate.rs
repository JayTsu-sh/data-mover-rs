//! T0 — issue #21：Local 覆盖写必须 truncate 目标文件。
//!
//! 回归场景：先写一个较大的源文件并 copy 到目标（生成较大的目标文件），
//! 再用一个更短的新源文件覆盖同一目标路径。修复前 `LocalStorage::create_file`
//! 不截断已存在文件，`write_data`/`write_file` 顺序写不规整尾部，导致目标文件
//! 残留旧内容的尾部字节（数据损坏）。
//!
//! Local-only — no S3/NFS server required.

use std::path::Path;

use data_mover::StorageEnum;

const BLOCK: u64 = 2 * 1024 * 1024; // 与 LocalStorage 默认 block_size 一致

async fn reset_dirs(src: &str, dst: &str) {
    let _ = tokio::fs::remove_dir_all(src).await;
    let _ = tokio::fs::remove_dir_all(dst).await;
    tokio::fs::create_dir_all(src).await.unwrap();
    tokio::fs::create_dir_all(dst).await.unwrap();
}

async fn write_pattern(path: &str, size: usize, seed: u8) {
    use tokio::io::AsyncWriteExt;
    let mut f = tokio::fs::File::create(path).await.unwrap();
    let buf: Vec<u8> = (0..size).map(|i| ((i as u8).wrapping_add(seed)) % 251).collect();
    f.write_all(&buf).await.unwrap();
    f.flush().await.unwrap();
}

fn pattern_vec(size: usize, seed: u8) -> Vec<u8> {
    (0..size).map(|i| ((i as u8).wrapping_add(seed)) % 251).collect()
}

/// T0：多块（> block_size）文件覆盖场景——10MB 源覆盖为 3MB 源后，目标应精确为 3MB
/// 且内容与新源完全一致，不得残留旧文件的尾部字节。
#[tokio::test(flavor = "multi_thread")]
async fn overwrite_shorter_file_truncates_stale_tail_multi_chunk() {
    let src_dir = "/tmp/dm-local-truncate-multi-src";
    let dst_dir = "/tmp/dm-local-truncate-multi-dst";
    reset_dirs(src_dir, dst_dir).await;

    const BIG: usize = 10 * 1024 * 1024;
    const SMALL: usize = 3 * 1024 * 1024;

    write_pattern(&format!("{src_dir}/blob.bin"), BIG, 0).await;

    let src = data_mover::create_storage(src_dir, Some(BLOCK), false)
        .await
        .unwrap();
    let dst = data_mover::create_storage(dst_dir, Some(BLOCK), true)
        .await
        .unwrap();
    let entry = src.get_metadata(Path::new("blob.bin")).await.unwrap();

    // 第一次拷贝：生成 10MB 目标文件
    StorageEnum::copy_file(&src, &dst, &entry, None, false, true, None)
        .await
        .expect("first full copy");
    let out = tokio::fs::read(format!("{dst_dir}/blob.bin")).await.unwrap();
    assert_eq!(out.len(), BIG, "first copy should produce full-size file");

    // 用更短的新内容覆盖同一目标路径
    write_pattern(&format!("{src_dir}/blob.bin"), SMALL, 7).await;
    let entry2 = src.get_metadata(Path::new("blob.bin")).await.unwrap();
    StorageEnum::copy_file(&src, &dst, &entry2, None, false, true, None)
        .await
        .expect("overwrite copy with shorter file");

    let out = tokio::fs::read(format!("{dst_dir}/blob.bin")).await.unwrap();
    assert_eq!(
        out.len(),
        SMALL,
        "destination must be truncated to the new (shorter) size, no stale tail bytes"
    );
    assert_eq!(
        out,
        pattern_vec(SMALL, 7),
        "destination content must match the new source exactly"
    );
}

/// 单块（<= block_size）文件覆盖场景，覆盖 `write_file` 直写路径（同样的 truncate bug）。
#[tokio::test(flavor = "multi_thread")]
async fn overwrite_shorter_file_truncates_stale_tail_single_chunk() {
    let src_dir = "/tmp/dm-local-truncate-single-src";
    let dst_dir = "/tmp/dm-local-truncate-single-dst";
    reset_dirs(src_dir, dst_dir).await;

    const BIG: usize = 512 * 1024;
    const SMALL: usize = 128 * 1024;

    write_pattern(&format!("{src_dir}/blob.bin"), BIG, 0).await;

    let src = data_mover::create_storage(src_dir, Some(BLOCK), false)
        .await
        .unwrap();
    let dst = data_mover::create_storage(dst_dir, Some(BLOCK), true)
        .await
        .unwrap();
    let entry = src.get_metadata(Path::new("blob.bin")).await.unwrap();

    StorageEnum::copy_file(&src, &dst, &entry, None, false, true, None)
        .await
        .expect("first full copy");

    write_pattern(&format!("{src_dir}/blob.bin"), SMALL, 3).await;
    let entry2 = src.get_metadata(Path::new("blob.bin")).await.unwrap();
    StorageEnum::copy_file(&src, &dst, &entry2, None, false, true, None)
        .await
        .expect("overwrite copy with shorter file");

    let out = tokio::fs::read(format!("{dst_dir}/blob.bin")).await.unwrap();
    assert_eq!(out.len(), SMALL, "destination must be truncated (single-chunk write_file path)");
    assert_eq!(out, pattern_vec(SMALL, 3), "destination content must match the new source exactly");
}
