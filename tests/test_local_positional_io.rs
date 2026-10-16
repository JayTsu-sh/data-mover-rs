//! Local positional-I/O coverage through the public role and transfer APIs.

use data_mover::model::{SourceVersion, StoragePath};
use data_mover::storage::ReadRequest;
use data_mover::transfer::{TransferRequest, transfer};
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

mod common;

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

/// Full role-based Local -> Local transfer exceeds the old write-queue depth and round-trips all
/// absolute offsets through a fresh destination read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_transfer_beyond_write_queue_reopens_with_identical_digest()
-> Result<(), Box<dyn std::error::Error>> {
    const SIZE: usize = 17 * 64 * 1024 + 137;
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    let expected = offset_pattern(SIZE);
    tokio::fs::write(source_root.path().join("blob.bin"), &expected).await?;
    let source = common::connect_local(source_root.path(), 4).await?;
    let destination = common::connect_local(destination_root.path(), 8).await?;
    let path = StoragePath::new("blob.bin")?;

    let outcome = transfer(TransferRequest::new(
        source,
        path.clone(),
        destination,
        CancellationToken::new(),
    ))
    .await?;

    let actual = tokio::fs::read(destination_root.path().join(path.as_str())).await?;
    assert_eq!(actual.len(), SIZE);
    assert_eq!(outcome.blake3, Some(*blake3::hash(&expected).as_bytes()));
    assert_eq!(actual, expected);
    Ok(())
}

/// The Local read role exposes exact absolute-offset coverage without the retired caller-built
/// `DataChunk` protocol. Local staged-role unit tests exercise out-of-order writes, duplicate
/// offsets, holes, and short positional writes at the adapter boundary.
#[tokio::test]
async fn local_read_role_returns_complete_positioned_coverage()
-> Result<(), Box<dyn std::error::Error>> {
    const SIZE: usize = 5 * 1024 * 1024 + 137;
    const CHUNK: usize = 1024 * 1024;
    let root = tempfile::tempdir()?;
    let expected = offset_pattern(SIZE);
    tokio::fs::write(root.path().join("blob.bin"), &expected).await?;
    let storage = common::connect_local(root.path(), 8).await?;
    let source = storage.read_source()?;
    assert!(source.supports_positioned_read());
    let mut stream = source
        .read_positioned(ReadRequest {
            path: StoragePath::new("blob.bin")?,
            range: Some(0..SIZE as u64),
            expected_source: None,
            maximum_chunk_bytes: CHUNK,
            read_inflight: 8,
            read_budget: None,
            cancel: CancellationToken::new(),
            source_qos: None,
            version: SourceVersion::Current,
        })
        .await?;

    let mut actual = vec![0; SIZE];
    let mut covered = vec![false; SIZE];
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let start = usize::try_from(chunk.offset)?;
        let end = start
            .checked_add(chunk.data.len())
            .ok_or("range overflow")?;
        assert!(end <= SIZE, "positioned read exceeded requested range");
        assert!(
            covered[start..end].iter().all(|seen| !seen),
            "overlap at {start}..{end}"
        );
        actual[start..end].copy_from_slice(&chunk.data);
        covered[start..end].fill(true);
    }
    assert!(
        covered.iter().all(|seen| *seen),
        "positioned read left a hole"
    );
    assert_eq!(actual, expected);
    Ok(())
}
