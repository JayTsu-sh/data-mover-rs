//! 独占前缀内的源删除原语；备份字节、残留项和权限由 runner 另行独立读取。
use std::{
    env,
    path::{Path, PathBuf},
    time::Duration,
};

use data_mover::model::{
    EntryKind, FailureClass, ObservationMode, ObservationPlan, ObservedEntry, StoragePath,
    SymlinkTarget, SymlinkTargetEncoding, Transience,
};
use data_mover::storage::{
    CreateSymlinkRequest, MetadataMutation, PurgeFailure, PurgeOutcome, Storage,
    StorageRoleFailure, create_directory_all, observe_descriptor,
};
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

use super::fault::control;
use super::{Result, common, connect, persistence::copy, with_identity};

fn path(name: &str) -> Result<StoragePath> {
    Ok(StoragePath::new(name)?)
}
async fn observed(source: &Storage, name: &str) -> Result<ObservedEntry> {
    Ok(observe_descriptor(
        source,
        source.namespace()?.stat(&path(name)?).await?,
        ObservationPlan::default()
            .with_timestamps(ObservationMode::InlineOnly)
            .with_ownership_mode(ObservationMode::InlineOnly),
    )
    .await?)
}
async fn publish(
    seed: &Storage,
    remote: &Storage,
    root: &Path,
    name: &str,
    bytes: &[u8],
) -> Result {
    let file = root.join(name);
    std::fs::create_dir_all(file.parent().ok_or("missing parent")?)?;
    std::fs::write(file, bytes)?;
    copy(seed, remote, &path(name)?).await
}
async fn backup(
    source: &Storage,
    target: &Storage,
    root: &Path,
    name: &str,
    expected: &[u8],
) -> Result<ObservedEntry> {
    let entry = observed(source, name).await?;
    copy(source, target, &path(name)?).await?;
    assert_eq!(std::fs::read(root.join(name))?, expected);
    Ok(entry)
}
async fn conflict(source: &Storage, entry: &ObservedEntry) -> Result {
    let error = source
        .namespace()?
        .purge_observed(entry, CancellationToken::new())
        .await
        .err()
        .ok_or("changed source was purged")?;
    assert!(
        matches!(error.source.as_ref(), StorageRoleFailure::Entry(error) if error.class() == FailureClass::Conflict)
    );
    Ok(())
}
async fn deleted(source: &Storage, entry: &ObservedEntry) -> Result {
    assert!(matches!(
        source
            .namespace()?
            .purge_observed(entry, CancellationToken::new())
            .await?,
        PurgeOutcome::Deleted { records: 1 }
    ));
    assert!(
        matches!(source.namespace()?.stat(entry.path()).await, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an owned LAB_NFS_DIRECT_URL and a prepared purge-hidden-dir artifact"]
async fn real_nfs_protected_source_purge_contract() -> Result {
    let url = env::var("LAB_NFS_DIRECT_URL")?;
    let source = connect(&url).await?;
    let seed_root = tempfile::tempdir()?;
    let seed = common::connect_local(seed_root.path(), 2).await?;
    let fallback = tempfile::tempdir()?;
    let backup_root = env::var_os("LAB_NFS_PURGE_BACKUP_DIR")
        .map_or_else(|| fallback.path().to_path_buf(), PathBuf::from);
    std::fs::create_dir_all(&backup_root)?;
    let target = common::connect_local(&backup_root, 2).await?;
    publish(&seed, &source, seed_root.path(), "purge-file", b"original").await?;
    let entry = backup(&source, &target, &backup_root, "purge-file", b"original").await?;
    deleted(&source, &entry).await?;
    assert!(matches!(
        source
            .namespace()?
            .purge_observed(&entry, CancellationToken::new())
            .await?,
        PurgeOutcome::AlreadyAbsent
    ));
    rewrite(&source, &seed, &target, seed_root.path(), &backup_root).await?;
    replacement(&source, &seed, &target, seed_root.path(), &backup_root).await?;
    replaced_parent(
        &source,
        &seed,
        &target,
        seed_root.path(),
        &backup_root,
        &url,
    )
    .await?;
    directories(&source, &seed, &target, seed_root.path(), &backup_root).await?;
    symlink(&source, &backup_root).await?;
    permission_refusal(
        &source,
        &seed,
        &target,
        seed_root.path(),
        &backup_root,
        &url,
    )
    .await?;
    let identity = observed(&source, "purge-hidden-dir")
        .await?
        .source_identity()
        .clone();
    let root = ObservedEntry::new(
        StoragePath::root(),
        EntryKind::Directory,
        None,
        None,
        identity,
    )?;
    assert!(
        source
            .namespace()?
            .purge_observed(&root, CancellationToken::new())
            .await
            .is_err()
    );
    println!(
        "real NFS source purge confirmed,idempotent=true rewrite_replacement_parent_refused=true empty_directory,symlink=true hidden_nonempty,permission_refused=true backups_retained=true"
    );
    Ok(())
}

async fn rewrite(
    source: &Storage,
    seed: &Storage,
    target: &Storage,
    root: &Path,
    copies: &Path,
) -> Result {
    let name = "purge-rewrite";
    publish(seed, source, root, name, b"before").await?;
    let entry = backup(source, target, copies, name, b"before").await?;
    publish(seed, source, root, name, b"after!").await?;
    let mut original = *entry
        .metadata()
        .timestamps()
        .value()
        .ok_or("missing original times")?;
    original.created = None;
    source
        .metadata()?
        .apply(
            &path(name)?,
            MetadataMutation::Timestamps(original),
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        source
            .namespace()?
            .stat(&path(name)?)
            .await?
            .source_identity,
        *entry.source_identity()
    );
    conflict(source, &entry).await
}
async fn replacement(
    source: &Storage,
    seed: &Storage,
    target: &Storage,
    root: &Path,
    copies: &Path,
) -> Result {
    let name = "purge-replaced";
    publish(seed, source, root, name, b"original").await?;
    let entry = backup(source, target, copies, name, b"original").await?;
    source
        .namespace()?
        .rename(&path(name)?, &path("purge-held")?)
        .await?;
    publish(seed, source, root, name, b"replacement").await?;
    conflict(source, &entry).await
}
async fn replaced_parent(
    source: &Storage,
    seed: &Storage,
    target: &Storage,
    root: &Path,
    copies: &Path,
    url: &str,
) -> Result {
    let name = "purge-parent/entry";
    publish(seed, source, root, name, b"original").await?;
    let entry = backup(source, target, copies, name, b"original").await?;
    // 不同 AUTH_SYS 端点拥有独立的全局目录缓存键，rename 不能顺便失效原连接的缓存。
    let other = connect(&with_identity(url, 4242)).await?;
    other
        .namespace()?
        .rename(&path("purge-parent")?, &path("purge-parent-held")?)
        .await?;
    create_directory_all(&other, &path("purge-parent")?).await?;
    publish(seed, &other, root, name, b"replacement").await?;
    conflict(source, &entry).await
}
async fn directories(
    source: &Storage,
    seed: &Storage,
    target: &Storage,
    root: &Path,
    copies: &Path,
) -> Result {
    create_directory_all(source, &path("purge-after-child")?).await?;
    let directory = observed(source, "purge-after-child").await?;
    let name = "purge-after-child/entry";
    publish(seed, source, root, name, b"child").await?;
    let child = backup(source, target, copies, name, b"child").await?;
    conflict(source, &directory).await?;
    deleted(source, &child).await?;
    deleted(source, &directory).await?;
    conflict(source, &observed(source, "purge-hidden-dir").await?).await
}
async fn symlink(source: &Storage, copies: &Path) -> Result {
    source
        .namespace()?
        .create_symlink(CreateSymlinkRequest {
            path: path("purge-link")?,
            target: SymlinkTarget::new(
                SymlinkTargetEncoding::UnixBytes,
                b"../opaque/missing".to_vec(),
            )?,
            uid: None,
            gid: None,
            accessed: None,
            modified: None,
        })
        .await?;
    let entry = observed(source, "purge-link").await?;
    std::os::unix::fs::symlink("../opaque/missing", copies.join("purge-link"))?;
    deleted(source, &entry).await
}
async fn permission_refusal(
    source: &Storage,
    seed: &Storage,
    target: &Storage,
    root: &Path,
    copies: &Path,
    url: &str,
) -> Result {
    let name = "purge-readonly-parent/file";
    publish(seed, source, root, name, b"denied").await?;
    for (name, mode) in [(name, 0o666), ("purge-readonly-parent", 0o555)] {
        source
            .metadata()?
            .apply(
                &path(name)?,
                MetadataMutation::Mode(mode),
                CancellationToken::new(),
            )
            .await?;
    }
    let limited = connect(&with_identity(url, 4242)).await?;
    let entry = backup(&limited, target, copies, name, b"denied").await?;
    let error = limited
        .namespace()?
        .purge_observed(&entry, CancellationToken::new())
        .await
        .err()
        .ok_or("readonly parent allowed deletion")?;
    assert!(
        matches!(error.source.as_ref(), StorageRoleFailure::Entry(error) if error.class() == FailureClass::PermissionDenied)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an owned remove-aware private relay"]
async fn real_nfs_source_purge_fault_contract() -> Result {
    for mode in [
        "flash",
        "disconnect",
        "stall",
        "cancel",
        "lost_ack",
        "bad_session",
    ] {
        if (mode == "bad_session" && env::var("LAB_NFS_DIRECT_URL")?.contains("version=3"))
            || env::var("LAB_NFS_DIRECT_ONLY").is_ok_and(|selected| selected != mode)
        {
            continue;
        }
        tokio::time::timeout(
            std::time::Duration::from_secs(35),
            Box::pin(purge_fault(mode)),
        )
        .await??;
    }
    Ok(())
}

type PurgeResult = std::result::Result<PurgeOutcome, PurgeFailure>;

async fn purge_fault(mode: &str) -> Result {
    control("normal").await?;
    let source = connect(&env::var("LAB_NFS_DIRECT_PROXY_URL")?).await?;
    let root = tempfile::tempdir()?;
    let seed = common::connect_local(root.path(), 2).await?;
    let copies = env::var_os("LAB_NFS_PURGE_BACKUP_DIR")
        .map(PathBuf::from)
        .ok_or("persistent owned backup directory required")?;
    std::fs::create_dir_all(&copies)?;
    let target = common::connect_local(&copies, 2).await?;
    let name = format!("purge-fault-{mode}");
    publish(
        &seed,
        &source,
        root.path(),
        &name,
        b"guarded source payload",
    )
    .await?;
    let entry = backup(&source, &target, &copies, &name, b"guarded source payload").await?;
    let v3 = env::var("LAB_NFS_DIRECT_URL")?.contains("version=3");
    let refused = check_fault_result(attempt_purge(&source, &entry, mode).await?, mode, v3)?;
    control("normal").await?;
    if refused {
        protect_replacement(
            &source,
            &seed,
            root.path(),
            &name,
            &entry,
            v3 && mode == "lost_ack",
        )
        .await?;
    } else {
        assert!(
            matches!(source.namespace()?.stat(&path(&name)?).await, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound)
        );
    }
    assert_eq!(
        std::fs::read(copies.join(&name))?,
        b"guarded source payload"
    );
    Ok(())
}

async fn attempt_purge(source: &Storage, entry: &ObservedEntry, mode: &str) -> Result<PurgeResult> {
    control(&format!("commit-{mode}")).await?;
    let cancel = CancellationToken::new();
    let started = Instant::now();
    let namespace = source.namespace()?;
    let operation = namespace.purge_observed(entry, cancel.clone());
    let result = if mode == "cancel" {
        let trigger = async {
            control("wait-fault").await?;
            sleep(Duration::from_millis(250)).await;
            cancel.cancel();
            Ok::<_, Box<dyn std::error::Error>>(())
        };
        let (result, trigger) = tokio::join!(operation, trigger);
        trigger?;
        result
    } else {
        operation.await
    };
    println!(
        "real NFS source purge fault={mode} elapsed_ms={} result={result:?}",
        started.elapsed().as_millis()
    );
    Ok(result)
}

fn check_fault_result(result: PurgeResult, mode: &str, v3: bool) -> Result<bool> {
    let refused = v3 || matches!(mode, "disconnect" | "stall" | "cancel" | "bad_session");
    if refused {
        let error = result
            .err()
            .ok_or("unconfirmed source purge became success")?;
        let (class, transience, diagnostic) = match error.source.as_ref() {
            StorageRoleFailure::Entry(error) => {
                (error.class(), error.transience(), error.diagnostic())
            }
            StorageRoleFailure::Session(error) => {
                (error.class(), error.transience(), error.diagnostic())
            }
        };
        assert_eq!(
            class,
            match mode {
                "stall" => FailureClass::Timeout,
                "cancel" => FailureClass::Cancelled,
                _ => FailureClass::Connectivity,
            }
        );
        assert_eq!(transience, Transience::Unknown);
        if mode == "bad_session" {
            assert!(diagnostic.contains("bad session"), "{diagnostic}");
        }
    } else {
        assert!(matches!(result?, PurgeOutcome::Deleted { records: 1 }));
    }
    Ok(refused)
}

async fn protect_replacement(
    source: &Storage,
    seed: &Storage,
    root: &Path,
    name: &str,
    entry: &ObservedEntry,
    already_removed: bool,
) -> Result {
    // 代理未转发的请求保留原项；丢失删除响应时源可能已删。两者都不能盲重试同名项。
    if already_removed {
        assert!(matches!(source.namespace()?.stat(&path(name)?).await,
            Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound));
    } else {
        assert_eq!(
            source
                .namespace()?
                .stat(&path(name)?)
                .await?
                .source_identity,
            *entry.source_identity()
        );
        source
            .namespace()?
            .rename(&path(name)?, &path(&format!("{name}-held"))?)
            .await?;
    }
    publish(seed, source, root, name, b"unrelated replacement").await?;
    conflict(source, entry).await?;
    println!(
        "real NFS source purge replacement retained after {name}; original_already_removed={already_removed}, backup intact"
    );
    Ok(())
}
