use std::num::NonZeroUsize;

use data_mover::model::{BackendIdentity, BackendKind, StoragePath};
use data_mover::storage::{LocalTransferConfig, connect_local_transfer};
use data_mover::transfer::{InflightLimits, TransferIdentity, TransferRequest, transfer};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn public_local_facade_runs_the_unified_transfer_lifecycle()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    tokio::fs::write(
        source_root.path().join("payload.bin"),
        b"architecture-ready",
    )
    .await?;
    let source = connect_local_transfer(LocalTransferConfig {
        root: source_root.path().to_path_buf(),
        identity: BackendIdentity::new(BackendKind::Local, "local-source")?,
        write_concurrency: NonZeroUsize::new(2).ok_or("non-zero")?,
    })?;
    let destination = connect_local_transfer(LocalTransferConfig {
        root: destination_root.path().to_path_buf(),
        identity: BackendIdentity::new(BackendKind::Local, "local-destination")?,
        write_concurrency: NonZeroUsize::new(2).ok_or("non-zero")?,
    })?;
    let path = StoragePath::new("payload.bin")?;
    let request = TransferRequest::new(
        TransferIdentity::new("local-facade-transfer")?,
        source,
        path.clone(),
        destination,
        path,
        InflightLimits::new(2, 128 * 1024, 2)?,
        CancellationToken::new(),
    );

    let outcome = transfer(request).await?;

    assert_eq!(outcome.transferred_bytes, 18);
    assert_eq!(
        tokio::fs::read(destination_root.path().join("payload.bin")).await?,
        b"architecture-ready"
    );
    Ok(())
}
