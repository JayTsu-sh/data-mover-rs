//! Native filesystem runtime coverage, shared by Windows and Linux.
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Duration;

use data_mover::model::StoragePath;
use data_mover::storage::{BackendConfig, LocalBackendConfig, Storage, connect_backend};
use data_mover::transfer::{
    InflightLimits, ReadBackVerification, TransferIdentity, TransferPolicy, TransferRequest,
    transfer,
};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn local(root: &Path) -> TestResult<Storage> {
    Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: root.to_owned(),
        read_concurrency: NonZeroUsize::new(8).ok_or("invalid concurrency")?,
        write_concurrency: NonZeroUsize::new(8).ok_or("invalid concurrency")?,
    }))
    .await?)
}

async fn request(source: &Path, destination: &Path) -> TestResult<TransferRequest> {
    Ok(TransferRequest::new(
        TransferIdentity::new("native-runtime")?,
        local(source).await?,
        StoragePath::new("file.bin")?,
        local(destination).await?,
        StoragePath::new("file.bin")?,
        InflightLimits::new(8, 8 * 1024 * 1024, 8)?,
        CancellationToken::new(),
    ))
}

async fn copy_cases(policy: TransferPolicy) -> TestResult {
    for size in [4096, 4 * 1024 * 1024, 40 * 1024 * 1024, 65 * 1024 * 1024] {
        let source = tempfile::tempdir()?;
        let destination = tempfile::tempdir()?;
        let payload = (0..size)
            .map(|offset| u8::try_from((offset + offset / 251) % 251))
            .collect::<Result<Vec<_>, _>>()?;
        std::fs::write(source.path().join("file.bin"), &payload)?;
        for read_back in [
            ReadBackVerification::Enabled,
            ReadBackVerification::Disabled,
        ] {
            for overwrite in [false, true] {
                let final_path = destination.path().join("file.bin");
                if overwrite {
                    std::fs::write(&final_path, vec![0xff; size + 17])?;
                }
                let outcome = tokio::time::timeout(
                    Duration::from_mins(2),
                    transfer(
                        request(source.path(), destination.path())
                            .await?
                            .with_transfer_policy(policy)
                            .with_read_back_verification(read_back),
                    ),
                )
                .await??;
                assert_eq!(outcome.transferred_bytes, size as u64);
                assert_eq!(std::fs::read(&final_path)?, payload);
                // Publication must release handles so Windows permits immediate cleanup.
                std::fs::remove_file(final_path)?;
                assert_eq!(std::fs::read_dir(destination.path())?.count(), 0);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn atomic_replace_copies_and_overwrites_without_leaking_handles() -> TestResult {
    copy_cases(TransferPolicy::AtomicReplace).await
}

#[tokio::test]
async fn checkpointed_copies_and_overwrites_without_leaking_handles() -> TestResult {
    copy_cases(TransferPolicy::Checkpointed).await
}

#[tokio::test]
async fn precancelled_copy_preserves_existing_destination() -> TestResult {
    let source = tempfile::tempdir()?;
    let destination = tempfile::tempdir()?;
    std::fs::write(source.path().join("file.bin"), b"new content")?;
    std::fs::write(destination.path().join("file.bin"), b"existing")?;
    for policy in [TransferPolicy::AtomicReplace, TransferPolicy::Checkpointed] {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let request = TransferRequest::new(
            TransferIdentity::new("native-cancel")?,
            local(source.path()).await?,
            StoragePath::new("file.bin")?,
            local(destination.path()).await?,
            StoragePath::new("file.bin")?,
            InflightLimits::new(1, 1024 * 1024, 128)?,
            cancel,
        )
        .with_transfer_policy(policy);
        assert!(transfer(request).await.is_err());
        assert_eq!(
            std::fs::read(destination.path().join("file.bin"))?,
            b"existing"
        );
        assert_eq!(std::fs::read_dir(destination.path())?.count(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn checkpointed_persists_large_transfer_and_cleans_recovery_files() -> TestResult {
    let source = tempfile::tempdir()?;
    let destination = tempfile::tempdir()?;
    // Cross the Local 256 MiB durable checkpoint window, including record persistence.
    let payload = vec![0x5a; 257 * 1024 * 1024];
    std::fs::write(source.path().join("file.bin"), &payload)?;
    let outcome = tokio::time::timeout(
        Duration::from_mins(2),
        transfer(
            request(source.path(), destination.path())
                .await?
                .with_transfer_policy(TransferPolicy::Checkpointed),
        ),
    )
    .await??;
    assert_eq!(
        outcome.recovery,
        data_mover::transfer::EffectiveRecovery::Checkpointed
    );
    assert_eq!(std::fs::read(destination.path().join("file.bin"))?, payload);
    std::fs::remove_file(destination.path().join("file.bin"))?;
    assert_eq!(std::fs::read_dir(destination.path())?.count(), 0);
    Ok(())
}

#[tokio::test]
async fn checkpointed_syncs_new_parent_directories() -> TestResult {
    let source = tempfile::tempdir()?;
    let destination = tempfile::tempdir()?;
    std::fs::write(source.path().join("file.bin"), b"new parent")?;
    let request = TransferRequest::new(
        TransferIdentity::new("native-new-parent")?,
        local(source.path()).await?,
        StoragePath::new("file.bin")?,
        local(destination.path()).await?,
        StoragePath::new("new/child/file.bin")?,
        InflightLimits::new(1, 1024 * 1024, 1)?,
        CancellationToken::new(),
    )
    .with_transfer_policy(TransferPolicy::Checkpointed);
    transfer(request).await?;
    let parent = destination.path().join("new/child");
    assert_eq!(std::fs::read(parent.join("file.bin"))?, b"new parent");
    std::fs::remove_file(parent.join("file.bin"))?;
    assert_eq!(std::fs::read_dir(parent)?.count(), 0);
    Ok(())
}
