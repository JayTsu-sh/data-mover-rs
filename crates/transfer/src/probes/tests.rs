use std::{error::Error, num::NonZeroUsize};

use crate::model::{BackendIdentity, BackendKind};
use crate::probes::{check_connectivity, probe_server_time};
use crate::storage::backends::local::connect_backend;
use crate::storage::{LocalBackendConfig, Storage};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[tokio::test]
async fn local_connectivity_performs_io_and_clock_probe_creates_nothing() -> TestResult {
    let root = tempfile::tempdir()?;
    let slots = NonZeroUsize::new(1).ok_or("nonzero")?;
    let storage = connect_backend(LocalBackendConfig {
        rename_context: None,
        read_chunk_bytes: None,
        write_chunk_bytes: None,
        root: root.path().to_path_buf(),
        read_concurrency: slots,
        write_concurrency: slots,
    })?;
    check_connectivity(&storage).await?;
    assert_eq!(probe_server_time(&storage).await?, None);
    assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    Ok(())
}

#[tokio::test]
async fn remote_clock_composition_writes_observes_and_removes_its_probe() -> TestResult {
    let root = tempfile::tempdir()?;
    let slots = NonZeroUsize::new(1).ok_or("nonzero")?;
    let local = connect_backend(LocalBackendConfig {
        rename_context: None,
        read_chunk_bytes: None,
        write_chunk_bytes: None,
        root: root.path().to_path_buf(),
        read_concurrency: slots,
        write_concurrency: slots,
    })?;

    // A remote-kind facade over real local roles exercises the shared composition without a
    // network fixture. Local's public clock shortcut is tested independently above.
    let remote = Storage::connected(
        BackendIdentity::new(BackendKind::Hdfs, "clock-test")?,
        local.capabilities().clone(),
        Some(local.read_source()?),
        Some(local.staged_destination()?),
        Some(local.namespace()?),
        Some(local.metadata()?),
        None,
    )?;
    let before = std::time::SystemTime::now();
    let time = probe_server_time(&remote)
        .await?
        .ok_or("missing server time")?;
    let lower = i128::try_from(before.duration_since(std::time::UNIX_EPOCH)?.as_nanos())?;
    let upper = i128::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos(),
    )?;
    assert!(time.unix_nanos() >= lower - 1_000_000_000 && time.unix_nanos() <= upper);
    assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    Ok(())
}
