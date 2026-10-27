//! Source permission plans keep fresh data private while ordinary transfers are writing it.
#![cfg(target_os = "linux")]

use std::env::var;
use std::error::Error;
use std::fs::{self, Permissions};
use std::num::NonZeroUsize;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use data_mover::model::StoragePath;
use data_mover::storage::{BackendConfig, LocalBackendConfig, Storage, connect_backend};
use data_mover::transfer::{
    SourceQosGroup, SourceQosPolicy, TransferPolicy, TransferRequest, transfer,
};
use endpoint::connect_with_options;
use tempfile::TempDir;
use tokio::spawn;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const PAYLOAD_BYTES: usize = 2 << 20;

#[allow(dead_code)]
#[path = "../examples/support/endpoint.rs"]
mod endpoint;

async fn local(root: &Path) -> TestResult<Storage> {
    Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: root.to_owned(),
        rename_context: None,
        read_chunk_bytes: NonZeroUsize::new(64 << 10),
        write_chunk_bytes: NonZeroUsize::new(64 << 10),
        read_concurrency: NonZeroUsize::MIN,
        write_concurrency: NonZeroUsize::MIN,
    }))
    .await?)
}

fn payload_path(root: &Path, direct: bool) -> TestResult<Option<PathBuf>> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        if (direct && name == "source") || name.to_string_lossy().ends_with(".stage") {
            let length = entry.metadata()?.len();
            if length > 0 && length < PAYLOAD_BYTES as u64 {
                return Ok(Some(entry.path()));
            }
        }
    }
    Ok(None)
}

async fn partial_payload(root: &Path, direct: bool) -> TestResult<PathBuf> {
    timeout(Duration::from_secs(10), async {
        loop {
            if let Some(path) = payload_path(root, direct)? {
                return Ok(path);
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}

async fn interrupt_private_transfer(source: &Storage, policy: TransferPolicy) -> TestResult {
    let root = TempDir::new()?;
    let destination = local(root.path()).await?;
    let cancel = CancellationToken::new();
    let qos = SourceQosGroup::new(SourceQosPolicy::new(
        Some((256 << 10, 256 << 10, Duration::ZERO)),
        64 << 10,
        None,
    )?);
    let request = TransferRequest::new(
        source.clone(),
        StoragePath::new("source")?,
        destination,
        cancel.clone(),
    )
    .with_transfer_policy(policy)
    .with_source_qos(qos);
    let running = spawn(transfer(request));
    let payload = partial_payload(root.path(), policy == TransferPolicy::Direct).await;
    // Cancel even when observation fails, so a failed assertion cannot leave a detached writer.
    cancel.cancel();
    let outcome = running.await?;
    let payload = payload?;
    assert!(
        outcome.is_err(),
        "transfer must stop before applying metadata"
    );
    assert_eq!(fs::metadata(payload)?.permissions().mode() & 0o777, 0o600);
    Ok(())
}

#[tokio::test]
async fn source_mode_keeps_incomplete_data_private() -> TestResult {
    let root = TempDir::new()?;
    let source_path = root.path().join("source");
    fs::write(&source_path, vec![0x61; PAYLOAD_BYTES])?;
    fs::set_permissions(source_path, Permissions::from_mode(0o600))?;
    let source = local(root.path()).await?;
    for policy in [
        TransferPolicy::Checkpointed,
        TransferPolicy::AtomicReplace,
        TransferPolicy::Direct,
    ] {
        interrupt_private_transfer(&source, policy).await?;
    }
    Ok(())
}

async fn acknowledge_private_stage(control: &Path) -> TestResult {
    fs::write(
        control.join("private.ready"),
        b"inspect nonempty stage mode=0600\n",
    )?;
    timeout(Duration::from_mins(1), async {
        loop {
            if control.join("private.continue").try_exists()? {
                return Ok(());
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await?
}

/// The external driver checks the stage with server-side stat before acknowledging it.
#[tokio::test]
#[ignore = "requires an isolated NFS destination and external server inspection"]
async fn real_nfs_private_source_stage() -> Result<(), Box<dyn Error>> {
    let root = TempDir::new()?;
    let path = root.path().join("source");
    fs::write(&path, vec![0x61; 8 << 20])?;
    fs::set_permissions(path, Permissions::from_mode(0o600))?;
    let source = connect_with_options(
        root.path().to_str().ok_or("non-UTF8 fixture")?,
        false,
        Some(64 << 10),
    )
    .await?;
    let destination =
        connect_with_options(&var("PRIVATE_CREATION_NFS_URL")?, true, Some(64 << 10)).await?;
    let cancel = CancellationToken::new();
    let qos = SourceQosGroup::new(SourceQosPolicy::new(
        Some((64 << 10, 64 << 10, Duration::ZERO)),
        64 << 10,
        None,
    )?);
    let request = TransferRequest::new(
        source,
        StoragePath::new("source")?,
        destination,
        cancel.clone(),
    )
    .with_source_qos(qos);
    let running = spawn(transfer(request));
    let observed = acknowledge_private_stage(Path::new(&var("PRIVATE_CREATION_CONTROL")?)).await;
    cancel.cancel();
    let outcome = running.await?;
    observed.map_err(|error| error as Box<dyn Error>)?;
    assert!(
        outcome.is_err(),
        "source must remain unpublished during inspection"
    );
    println!("PASS interrupted real NFS transfer after server inspected private source data");
    Ok(())
}
