//! Local positional-I/O contract tests.
//!
//! These tests exercise public copy/stream seams with more chunks than the Local
//! write queue depth.  Every byte is derived from its absolute offset so misplaced,
//! duplicated, or swapped chunks cannot accidentally produce the expected digest.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use data_mover::{CopyOptions, DataChunk, StorageEnum, TransferConcurrency, create_storage};

mod common;
use common::AssertTestValue;

static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

fn unique_test_dir(label: &str) -> PathBuf {
    let serial = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "data-mover-{label}-{}-{serial}",
        std::process::id()
    ))
}

fn offset_pattern(size: usize) -> Vec<u8> {
    (0..size)
        .map(|offset| {
            let mixed = (offset as u64)
                .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                .rotate_left(17)
                ^ 0xa5a5_5a5a_d3c1_b2e7;
            mixed.to_le_bytes()[offset % 8]
        })
        .collect()
}

async fn reset_dir(path: &Path) {
    let _ = tokio::fs::remove_dir_all(path).await;
    tokio::fs::create_dir_all(path)
        .await
        .assert_value("create test directory");
}

/// Full Local -> Local copy covers positional reads and writes. Seventeen source
/// blocks ensure the transfer exceeds the Local write queue depth of eight.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_copy_beyond_write_queue_reopens_with_identical_size_and_digest() {
    const BLOCK_SIZE: usize = 64 * 1024;
    const BLOCK: u64 = BLOCK_SIZE as u64;
    const SIZE: usize = 17 * BLOCK_SIZE + 137;

    let root = unique_test_dir("positional-copy");
    let source_dir = root.join("source");
    let destination_dir = root.join("destination");
    reset_dir(&source_dir).await;
    reset_dir(&destination_dir).await;

    let expected = offset_pattern(SIZE);
    tokio::fs::write(source_dir.join("blob.bin"), &expected)
        .await
        .assert_value("write source fixture");

    let source_url = source_dir.to_string_lossy();
    let destination_url = destination_dir.to_string_lossy();
    let source = create_storage(&source_url, Some(BLOCK), false)
        .await
        .assert_value("create source storage")
        .with_transfer_concurrency(
            TransferConcurrency::new(4, 8).assert_value("source concurrency"),
        );
    let destination = create_storage(&destination_url, Some(BLOCK), true)
        .await
        .assert_value("create destination storage");
    let entry = source
        .get_metadata(Path::new("blob.bin"))
        .await
        .assert_value("read source metadata");

    StorageEnum::copy_file(
        &source,
        &destination,
        &entry,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: true,
            ..Default::default()
        },
    )
    .await
    .assert_value("copy local file with positional I/O");

    // Reopen from the filesystem instead of observing the write handle or source buffer.
    let actual = tokio::fs::read(destination_dir.join("blob.bin"))
        .await
        .assert_value("reopen copied file");
    let metadata = tokio::fs::metadata(destination_dir.join("blob.bin"))
        .await
        .assert_value("read copied metadata");
    assert_eq!(metadata.len(), SIZE as u64, "copied size must match");
    assert_eq!(
        blake3::hash(&actual),
        blake3::hash(&expected),
        "reopened destination digest must match the source"
    );
    assert_eq!(
        actual, expected,
        "all copied offsets must contain the right bytes"
    );

    let _ = tokio::fs::remove_dir_all(root).await;
}

/// Simulate chunk shapes produced by NFS (1 MiB), Local (2 MiB), S3 (5 MiB),
/// and CIFS (8 MiB). Chunks arrive in reverse order and exceed the Local queue
/// depth, exercising a Local destination independently of the source protocol.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_destination_accepts_cross_protocol_chunk_shapes_out_of_order() {
    const MIB: usize = 1024 * 1024;
    const SHAPES: [usize; 12] = [
        MIB,
        2 * MIB,
        5 * MIB,
        8 * MIB,
        MIB,
        2 * MIB,
        5 * MIB,
        8 * MIB,
        MIB,
        2 * MIB,
        5 * MIB,
        8 * MIB + 137,
    ];
    let size: usize = SHAPES.iter().sum();

    let root = unique_test_dir("protocol-shaped-writes");
    let shape_dir = root.join("shape");
    let destination_dir = root.join("destination");
    reset_dir(&shape_dir).await;
    reset_dir(&destination_dir).await;

    let expected = Bytes::from(offset_pattern(size));
    tokio::fs::write(shape_dir.join("blob.bin"), &expected)
        .await
        .assert_value("write entry-shape fixture");
    let shape_url = shape_dir.to_string_lossy();
    let destination_url = destination_dir.to_string_lossy();
    let shape_storage = create_storage(&shape_url, None, false)
        .await
        .assert_value("create shape storage");
    let entry = shape_storage
        .get_metadata(Path::new("blob.bin"))
        .await
        .assert_value("read shape metadata");
    let destination = create_storage(&destination_url, None, true)
        .await
        .assert_value("create destination storage");
    let part_path = Path::new("blob.bin.terrasync-part");
    let (_missing, handle) = StorageEnum::resume_prepare(&destination, &entry, part_path, false)
        .await
        .assert_value("prepare positional stream");

    let mut chunks = Vec::with_capacity(SHAPES.len());
    let mut offset = 0usize;
    for len in SHAPES {
        chunks.push(DataChunk {
            offset: offset as u64,
            data: expected.slice(offset..offset + len),
        });
        offset += len;
    }
    let duplicate = DataChunk {
        offset: chunks[3].offset,
        data: chunks[3].data.clone(),
    };
    let (tx, rx) = tokio::sync::mpsc::channel(chunks.len() + 1);
    for chunk in chunks.into_iter().rev() {
        tx.send(chunk)
            .await
            .assert_value("send reverse-order protocol-shaped chunk");
    }
    tx.send(duplicate)
        .await
        .assert_value("send duplicate idempotent chunk");
    drop(tx);

    StorageEnum::write_chunk_stream(
        &destination,
        &entry,
        rx,
        &handle,
        None,
        std::sync::Arc::new(|_, _| {}),
    )
    .await
    .assert_value("write protocol-shaped chunks");
    StorageEnum::commit_chunk_stream(&destination, &entry, size as u64, handle)
        .await
        .assert_value("commit protocol-shaped chunks");

    let actual = tokio::fs::read(destination_dir.join("blob.bin"))
        .await
        .assert_value("reopen committed file");
    assert_eq!(actual.len(), size, "committed size must match");
    assert_eq!(
        blake3::hash(&actual),
        blake3::hash(&expected),
        "cross-protocol chunk shapes must preserve the complete digest"
    );
    assert_eq!(actual.as_slice(), expected.as_ref());

    let _ = tokio::fs::remove_dir_all(root).await;
}
