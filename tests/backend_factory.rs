use std::num::NonZeroUsize;

use data_mover::model::{BackendIdentity, BackendKind, StoragePath};
use data_mover::storage::{BackendConfig, CifsBackendConfig, LocalBackendConfig, connect_backend};
use data_mover::transfer::{InflightLimits, TransferIdentity, TransferRequest, transfer};
use tokio_util::sync::CancellationToken;

#[test]
fn cifs_config_debug_never_exposes_credentials() -> Result<(), Box<dyn std::error::Error>> {
    let config = CifsBackendConfig {
        server: "server".to_string(),
        share: "share".to_string(),
        root: None,
        username: "sensitive-user".to_string(),
        password: "sensitive-password".to_string(),
        identity: BackendIdentity::new(BackendKind::Cifs, "cifs-fixture")?,
    };

    let debug = format!("{config:?}");

    assert!(!debug.contains("sensitive-user"));
    assert!(!debug.contains("sensitive-password"));
    Ok(())
}

#[tokio::test]
async fn explicit_factory_handles_source_and_destination_without_pair_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    tokio::fs::write(source_root.path().join("payload.bin"), b"protocol-neutral").await?;
    let source = connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: source_root.path().to_path_buf(),
        identity: BackendIdentity::new(BackendKind::Local, "factory-source")?,
        read_concurrency: NonZeroUsize::new(2).ok_or("non-zero")?,
        write_concurrency: NonZeroUsize::new(2).ok_or("non-zero")?,
    }))
    .await?;
    let destination = connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: destination_root.path().to_path_buf(),
        identity: BackendIdentity::new(BackendKind::Local, "factory-destination")?,
        read_concurrency: NonZeroUsize::new(2).ok_or("non-zero")?,
        write_concurrency: NonZeroUsize::new(2).ok_or("non-zero")?,
    }))
    .await?;
    let path = StoragePath::new("payload.bin")?;
    let request = TransferRequest::new(
        TransferIdentity::new("factory-transfer")?,
        source,
        path.clone(),
        destination,
        path,
        InflightLimits::new(2, 128 * 1024, 2)?,
        CancellationToken::new(),
    );

    let outcome = transfer(request).await?;

    assert_eq!(outcome.transferred_bytes, 16);
    assert_eq!(
        tokio::fs::read(destination_root.path().join("payload.bin")).await?,
        b"protocol-neutral"
    );
    Ok(())
}
