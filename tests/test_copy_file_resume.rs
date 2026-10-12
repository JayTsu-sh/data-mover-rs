//! Public role-transfer retry identity and output verification.
//!
//! The retired caller-built `ResumeContext` and `.part` layout are covered at the adapter and
//! engine boundaries by role-level checkpoint tests; callers now resubmit the same transfer
//! request shape and data-mover decides whether a durable stage can be resumed.

use data_mover::model::StoragePath;
use data_mover::transfer::{InflightLimits, TransferRequest, transfer};
use tokio_util::sync::CancellationToken;

mod common;

const SIZE: usize = 256 * 1024;

fn payload() -> Vec<u8> {
    (0..SIZE)
        .map(|index| u8::try_from(index % 251).unwrap_or_default())
        .collect()
}

#[tokio::test]
async fn repeated_role_transfers_derive_the_same_identity_and_publish_exact_content()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    let expected = payload();
    std::fs::write(source_root.path().join("blob.bin"), &expected)?;
    let source = common::connect_local(source_root.path(), 4).await?;
    let destination = common::connect_local(destination_root.path(), 4).await?;
    let path = StoragePath::new("blob.bin")?;
    let limits = InflightLimits::new(4, 128 * 1024, 4)?;

    let first = transfer(TransferRequest::new(
        source.clone(),
        path.clone(),
        destination.clone(),
        path.clone(),
        limits,
        CancellationToken::new(),
    ))
    .await?;
    let second = transfer(TransferRequest::new(
        source,
        path.clone(),
        destination,
        path.clone(),
        limits,
        CancellationToken::new(),
    ))
    .await?;

    assert_eq!(first.identity, second.identity);
    assert_eq!(second.transferred_bytes, SIZE as u64);
    assert_eq!(second.blake3, Some(*blake3::hash(&expected).as_bytes()));
    assert_eq!(
        std::fs::read(destination_root.path().join(path.as_str()))?,
        expected
    );
    assert_eq!(std::fs::read_dir(destination_root.path())?.count(), 1);
    Ok(())
}
