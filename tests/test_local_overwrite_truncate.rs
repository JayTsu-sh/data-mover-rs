//! Role-based Local replacement must publish the shorter file without stale trailing bytes.

use std::path::Path;

use data_mover::model::StoragePath;
use data_mover::storage::Storage;
use data_mover::transfer::{TransferPolicy, TransferRequest, transfer};
use tokio_util::sync::CancellationToken;

mod common;

async fn write_pattern(path: &Path, size: usize, seed: u8) -> Result<(), std::io::Error> {
    let bytes = pattern_vec(size, seed);
    tokio::fs::write(path, bytes).await
}

fn pattern_vec(size: usize, seed: u8) -> Vec<u8> {
    (0..size)
        .map(|index| {
            (u8::try_from(index % 256)
                .unwrap_or_default()
                .wrapping_add(seed))
                % 251
        })
        .collect()
}

async fn copy(source: &Storage, destination: &Storage) -> Result<(), Box<dyn std::error::Error>> {
    let path = StoragePath::new("blob.bin")?;
    transfer(
        TransferRequest::new(
            source.clone(),
            path.clone(),
            destination.clone(),
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::AtomicReplace),
    )
    .await?;
    Ok(())
}

async fn verify_overwrite(
    big: usize,
    small: usize,
    seed: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    let source = common::connect_local(source_root.path(), 8).await?;
    let destination = common::connect_local(destination_root.path(), 8).await?;
    let source_path = source_root.path().join("blob.bin");
    let destination_path = destination_root.path().join("blob.bin");

    write_pattern(&source_path, big, 0).await?;
    copy(&source, &destination).await?;
    assert_eq!(
        tokio::fs::metadata(&destination_path).await?.len(),
        big as u64
    );

    write_pattern(&source_path, small, seed).await?;
    copy(&source, &destination).await?;
    let actual = tokio::fs::read(destination_path).await?;
    assert_eq!(
        actual.len(),
        small,
        "replacement size must not retain the old tail"
    );
    assert_eq!(actual, pattern_vec(small, seed));
    Ok(())
}

/// A transfer far larger than the negotiated chunk verifies staging and publication on replace.
#[tokio::test(flavor = "multi_thread")]
async fn overwrite_shorter_file_truncates_stale_tail_multi_chunk()
-> Result<(), Box<dyn std::error::Error>> {
    verify_overwrite(10 * 1024 * 1024, 3 * 1024 * 1024, 7).await
}

/// Replacing a small file exercises the transfer engine's single-chunk route.
#[tokio::test(flavor = "multi_thread")]
async fn overwrite_shorter_file_truncates_stale_tail_single_chunk()
-> Result<(), Box<dyn std::error::Error>> {
    verify_overwrite(512 * 1024, 128 * 1024, 3).await
}
