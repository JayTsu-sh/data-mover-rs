//! 目录和断链的确认只核验 namespace，不打开或跟随链接目标。
use std::{env, time::Duration};

use super::fault::control;
use tokio::time::{Instant, sleep, timeout};

use data_mover::model::{
    EntryKind, FailureClass, StoragePath, StorageTimestamp, SymlinkTarget, SymlinkTargetEncoding,
    TimePrecision, TimestampMetadata,
};
use data_mover::storage::{
    CreateSymlinkRequest, MetadataMutation, Storage, StorageRoleFailure, create_directory_all,
};
use tokio_util::sync::CancellationToken;

use super::persistence::copy;
use super::{Result, common, connect};

fn path(value: &str) -> Result<StoragePath> {
    Ok(StoragePath::new(value)?)
}

async fn confirm(target: &Storage, name: &str) -> Result {
    let expected = target.namespace()?.stat(&path(name)?).await?;
    target
        .namespace()?
        .persist_observed(&expected, CancellationToken::new())
        .await?;
    Ok(())
}

async fn link(target: &Storage, name: &str, value: &str) -> Result {
    let time = Some(StorageTimestamp::new(
        1_700_000_000_123_456_789,
        TimePrecision::Nanoseconds,
    )?);
    target
        .namespace()?
        .create_symlink(CreateSymlinkRequest {
            path: path(name)?,
            target: SymlinkTarget::new(
                SymlinkTargetEncoding::UnixBytes,
                value.as_bytes().to_vec(),
            )?,
            uid: None,
            gid: None,
            accessed: time,
            modified: time,
        })
        .await?;
    Ok(())
}

async fn reset(target: &Storage) -> Result {
    for name in [
        "nonfile-directory/child",
        "nonfile-directory-held/child",
        "nonfile-link",
        "nonfile-link-held",
        "nonfile-directory-held",
        "nonfile-directory",
        "nonfile-empty-directory",
    ] {
        match target.namespace()?.delete(&path(name)?).await {
            Ok(()) => {}
            Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an exclusively prepared LAB_NFS_DIRECT_URL prefix"]
async fn real_nfs_directory_and_symlink_persistence_contract() -> Result {
    let target = connect(&env::var("LAB_NFS_DIRECT_URL")?).await?;
    reset(&target).await?;
    let time = Some(StorageTimestamp::new(
        1_700_000_000_123_456_789,
        TimePrecision::Nanoseconds,
    )?);
    for name in ["nonfile-directory", "nonfile-empty-directory"] {
        create_directory_all(&target, &path(name)?).await?;
        if name == "nonfile-directory" {
            let root = tempfile::tempdir()?;
            std::fs::create_dir(root.path().join(name))?;
            std::fs::write(root.path().join(name).join("child"), b"child")?;
            let source = common::connect_local(root.path(), 2).await?;
            copy(&source, &target, &path("nonfile-directory/child")?).await?;
        }
        target
            .metadata()?
            .apply(
                &path(name)?,
                MetadataMutation::Timestamps(TimestampMetadata {
                    accessed: time,
                    modified: time,
                    created: None,
                }),
                CancellationToken::new(),
            )
            .await?;
        target
            .metadata()?
            .apply(
                &path(name)?,
                MetadataMutation::Mode(0o555),
                CancellationToken::new(),
            )
            .await?;
        confirm(&target, name).await?;
        assert_eq!(
            target
                .namespace()?
                .stat(&path(name)?)
                .await?
                .inline_timestamps()
                .and_then(|times| times.modified),
            time
        );
    }
    let expected = target
        .namespace()?
        .stat(&path("nonfile-directory")?)
        .await?;
    target
        .namespace()?
        .rename(
            &path("nonfile-directory")?,
            &path("nonfile-directory-held")?,
        )
        .await?;
    create_directory_all(&target, &path("nonfile-directory")?).await?;
    let refused = target
        .namespace()?
        .persist_observed(&expected, CancellationToken::new())
        .await
        .err()
        .ok_or("replaced directory accepted")?;
    assert!(
        matches!(refused, StorageRoleFailure::Entry(error) if error.class() == FailureClass::Conflict)
    );
    link(&target, "nonfile-link", "../opaque/missing").await?;
    confirm(&target, "nonfile-link").await?;
    let expected = target.namespace()?.stat(&path("nonfile-link")?).await?;
    assert_eq!(expected.kind, EntryKind::Symlink);
    target
        .namespace()?
        .rename(&path("nonfile-link")?, &path("nonfile-link-held")?)
        .await?;
    link(&target, "nonfile-link", "../other/missing").await?;
    let refused = target
        .namespace()?
        .persist_observed(&expected, CancellationToken::new())
        .await
        .err()
        .ok_or("replaced symlink accepted")?;
    assert!(
        matches!(refused, StorageRoleFailure::Entry(error) if error.class() == FailureClass::Conflict)
    );
    confirm(&target, "nonfile-link").await?;
    println!(
        "real NFS non-file persistence readonly_directory,empty_directory,broken_symlink=true mtime_preserved=true replacement_refused=true"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires private relay configured with --operation-tag lookup_path"]
async fn real_nfs_nonfile_persistence_fault_contract() -> Result {
    for directory in [true, false] {
        for mode in ["flash", "disconnect", "stall", "cancel", "lost_ack"] {
            if env::var("LAB_NFS_DIRECT_ONLY").is_ok_and(|selected| selected != mode) {
                continue;
            }
            timeout(
                Duration::from_secs(35),
                Box::pin(nonfile_fault(directory, mode)),
            )
            .await??;
        }
    }
    Ok(())
}

async fn cancel_at_lookup(cancel: CancellationToken) -> Result<Instant> {
    control("wait-fault").await?;
    sleep(Duration::from_millis(250)).await;
    let sent = Instant::now();
    cancel.cancel();
    Ok(sent)
}

async fn nonfile_fault(directory: bool, mode: &'static str) -> Result {
    control("normal").await?;
    let target = connect(&env::var("LAB_NFS_DIRECT_PROXY_URL")?).await?;
    let kind = if directory { "directory" } else { "link" };
    let name = format!("nonfile-fault-{kind}-{mode}");
    if directory {
        create_directory_all(&target, &path(&name)?).await?;
    } else {
        match target.namespace()?.delete(&path(&name)?).await {
            Ok(()) => {}
            Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        link(&target, &name, "../opaque/missing").await?;
    }
    let namespace = target.namespace()?;
    let expected = namespace.stat(&path(&name)?).await?;
    // 控制接口按 runner 配置的 lookup_path tag 武装，不向目录或链接发送 COMMIT。
    control(&format!("commit-{mode}")).await?;
    let cancel = CancellationToken::new();
    let started = Instant::now();
    let operation = namespace.persist_observed(&expected, cancel.clone());
    let result = if mode == "cancel" {
        let (result, sent) = tokio::join!(operation, cancel_at_lookup(cancel));
        assert!(sent?.elapsed() < Duration::from_secs(1));
        assert!(
            matches!(&result, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Cancelled)
        );
        result
    } else {
        operation.await
    };
    println!(
        "real NFS nonfile kind={kind} fault={mode} elapsed_ms={} result={result:?}",
        started.elapsed().as_millis()
    );
    if matches!(mode, "flash" | "lost_ack") {
        result?;
    } else {
        assert!(result.is_err());
    }
    control("normal").await?;
    let retry = connect(&env::var("LAB_NFS_DIRECT_URL")?).await?;
    confirm(&retry, &name).await?;
    if !directory {
        assert_eq!(
            retry
                .namespace()?
                .read_link(&path(&name)?)
                .await?
                .as_bytes(),
            b"../opaque/missing"
        );
    }
    Ok(())
}
