//! Creation permissions are checked before writing or publication, in child processes so each
//! umask is isolated from Rust's concurrently running tests.
#![cfg(target_os = "linux")]

use std::env::{current_exe, var_os};
use std::error::Error;
use std::fs::{self, Permissions};
use std::num::NonZeroUsize;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::process::Command;

use data_mover::model::{
    BackendIdentity, BackendKind, EntryKind, IdentityStrength, SourceIdentity, StoragePath,
};
use data_mover::storage::{
    BackendConfig, DestinationPrepareRequest, FinalDestination, LocalBackendConfig, PrepareRequest,
    ResumeMode, SourceDescriptor, Storage, connect_backend, create_directory_all,
};
use data_mover::transfer::{TransferPolicy, TransferRequest, transfer};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const CHILD: &str = "DATA_MOVER_CREATION_MODE_CHILD";

async fn local(root: &Path) -> TestResult<Storage> {
    Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: root.to_owned(),
        rename_context: None,
        read_chunk_bytes: None,
        write_chunk_bytes: None,
        read_concurrency: NonZeroUsize::MIN,
        write_concurrency: NonZeroUsize::MIN,
    }))
    .await?)
}

fn mode(path: &Path) -> TestResult<u32> {
    Ok(fs::metadata(path)?.permissions().mode() & 0o7777)
}

/// No source permission facts: this request exercises creation defaults alone.
fn request(path: &str) -> TestResult<PrepareRequest> {
    Ok(PrepareRequest {
        final_destination: FinalDestination::new(StoragePath::new(path)?),
        source: SourceDescriptor::new(
            StoragePath::new("source")?,
            EntryKind::File,
            Some(0),
            SourceIdentity::new(
                BackendIdentity::new(BackendKind::S3, "creation-mode-source")?,
                IdentityStrength::StableWithinBackend,
                b"source",
            )?,
        ),
        recovery_binding: [1; 32],
    })
}

#[test]
fn local_creation_modes_ignore_umask() -> TestResult {
    for mask in ["0000", "0077", "0100", "0777"] {
        let status = Command::new("sh")
            .args(["-c", "umask \"$1\"; shift; exec \"$@\"", "mode-test", mask])
            .arg(current_exe()?)
            .args(["--exact", "local_creation_modes_child", "--nocapture"])
            .env(CHILD, "1")
            .status()?;
        assert!(status.success(), "creation permissions with umask {mask}");
    }
    Ok(())
}

fn private_fixture() -> TestResult<tempfile::TempDir> {
    let root = tempfile::tempdir()?;
    // Fixture accessibility must not depend on the deliberately restrictive child umask.
    fs::set_permissions(root.path(), Permissions::from_mode(0o700))?;
    Ok(root)
}

#[tokio::test]
async fn local_creation_modes_child() -> TestResult {
    if var_os(CHILD).is_none() {
        return Ok(());
    }
    let root = private_fixture()?;
    let storage = local(root.path()).await?;
    verify_namespace_creation(&storage, root.path()).await?;
    verify_staged_creation(&storage, root.path()).await?;
    verify_direct_creation(&storage, root.path()).await?;
    verify_source_modes(&storage, root.path()).await
}

async fn verify_namespace_creation(storage: &Storage, root: &Path) -> TestResult {
    create_directory_all(storage, &StoragePath::new("namespace/child")?).await?;
    for relative in ["namespace", "namespace/child"] {
        assert_eq!(mode(&root.join(relative))?, 0o755);
    }
    fs::set_permissions(root.join("namespace"), Permissions::from_mode(0o710))?;
    create_directory_all(storage, &StoragePath::new("namespace/child")?).await?;
    assert_eq!(mode(&root.join("namespace"))?, 0o710);
    Ok(())
}

async fn verify_staged_creation(storage: &Storage, root: &Path) -> TestResult {
    let destination = storage.staged_destination()?;
    for recoverable in [false, true] {
        let parent = format!("staged-{recoverable}/child");
        let stage = destination
            .prepare_at_destination(
                DestinationPrepareRequest::new(request(&format!("{parent}/file"))?, [2; 32])
                    .with_resume(ResumeMode::Restart)
                    .with_recoverable(recoverable),
            )
            .await?;
        assert_eq!(mode(&root.join(format!("staged-{recoverable}")))?, 0o755);
        assert_eq!(mode(&root.join(&parent))?, 0o755);
        let stage_file = fs::read_dir(root.join(&parent))?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .find(|entry| entry.file_name().to_string_lossy().ends_with(".stage"))
            .ok_or("missing data stage")?;
        assert_eq!(mode(&stage_file.path())?, 0o644);
        destination.discard(stage).await?;
    }
    Ok(())
}

async fn verify_direct_creation(storage: &Storage, root: &Path) -> TestResult {
    let destination = storage.staged_destination()?;
    let direct_path = root.join("direct/child/file");
    let direct = destination
        .prepare_direct(request("direct/child/file")?, CancellationToken::new())
        .await?;
    assert_eq!(mode(&direct_path)?, 0o644);
    assert_eq!(mode(&root.join("direct"))?, 0o755);
    assert_eq!(mode(&root.join("direct/child"))?, 0o755);
    destination.discard(direct).await?;
    fs::set_permissions(&direct_path, Permissions::from_mode(0o600))?;
    let existing = destination
        .prepare_direct(request("direct/child/file")?, CancellationToken::new())
        .await?;
    assert_eq!(
        mode(&direct_path)?,
        0o600,
        "an existing direct target keeps its mode"
    );
    destination.discard(existing).await?;
    Ok(())
}

async fn verify_source_modes(storage: &Storage, root: &Path) -> TestResult {
    // Source metadata still wins over creation defaults for every transfer policy.
    let source_root = private_fixture()?;
    fs::write(source_root.path().join("source"), b"payload")?;
    let source = local(source_root.path()).await?;
    for source_mode in [0o600, 0o755] {
        fs::set_permissions(
            source_root.path().join("source"),
            Permissions::from_mode(source_mode),
        )?;
        for policy in [
            TransferPolicy::Checkpointed,
            TransferPolicy::AtomicReplace,
            TransferPolicy::Direct,
        ] {
            transfer(
                TransferRequest::new(
                    source.clone(),
                    StoragePath::new("source")?,
                    storage.clone(),
                    CancellationToken::new(),
                )
                .with_transfer_policy(policy),
            )
            .await?;
            assert_eq!(mode(&root.join("source"))?, source_mode);
        }
    }
    Ok(())
}
