//! 真实整文件 COMMIT、只读属性与缓存祖先替换；服务端另行独立检查内容。
use std::{env, time::Duration};

use super::fault::control;
use tokio::time::{Instant, sleep, timeout};

use data_mover::model::{FailureClass, StoragePath};
use data_mover::storage::{MetadataMutation, Storage, StorageRoleFailure, create_directory_all};
use data_mover::transfer::{
    MetadataTransferMode, PublicationDurability, TransferPolicy, TransferRequest, transfer,
};
use tokio_util::sync::CancellationToken;

use super::{Result, common, connect};

async fn copy(source: &Storage, target: &Storage, path: &StoragePath) -> Result {
    transfer(
        TransferRequest::new(
            source.clone(),
            path.clone(),
            target.clone(),
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Direct)
        .with_metadata_mode(MetadataTransferMode::CallerManaged)
        .with_publication_durability(PublicationDurability::Required),
    )
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an exclusively prepared LAB_NFS_DIRECT_URL prefix"]
async fn real_nfs_published_file_persistence_contract() -> Result {
    let target = connect(&env::var("LAB_NFS_DIRECT_URL")?).await?;
    reset_fixture(&target).await?;
    let root = tempfile::tempdir()?;
    let source = common::connect_local(root.path(), 2).await?;
    for (name, bytes) in [
        ("persist-file", b"durable ordinary file".as_slice()),
        ("persist-empty", b"".as_slice()),
    ] {
        std::fs::write(root.path().join(name), bytes)?;
        let path = StoragePath::new(name)?;
        copy(&source, &target, &path).await?;
        let expected = target.namespace()?.stat(&path).await?;
        target
            .namespace()?
            .persist_observed(&expected, CancellationToken::new())
            .await?;
        target
            .metadata()?
            .apply(
                &path,
                MetadataMutation::Mode(0o444),
                CancellationToken::new(),
            )
            .await?;
        let readonly = target.namespace()?.stat(&path).await?;
        target
            .namespace()?
            .persist_observed(&readonly, CancellationToken::new())
            .await?;
        assert_eq!(std::fs::read(root.path().join(name))?, bytes);
    }
    replacement(&source, &target, root.path()).await?;
    parent_replacement(&source, &target, root.path()).await?;
    println!(
        "real NFS persistence ordinary,empty,readonly=true replaced_file,parent_refused=true source_retained=true"
    );
    Ok(())
}

async fn reset_fixture(target: &Storage) -> Result {
    for name in [
        "persist-file",
        "persist-empty",
        "persist-replaced",
        "persist-held",
        "persist-parent/file",
        "persist-parent-held/file",
        "persist-parent",
        "persist-parent-held",
    ] {
        match target.namespace()?.delete(&StoragePath::new(name)?).await {
            Ok(()) => {}
            Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

async fn replacement(source: &Storage, target: &Storage, root: &std::path::Path) -> Result {
    let path = StoragePath::new("persist-replaced")?;
    std::fs::write(root.join(path.as_str()), b"original")?;
    copy(source, target, &path).await?;
    let expected = target.namespace()?.stat(&path).await?;
    target
        .namespace()?
        .rename(&path, &StoragePath::new("persist-held")?)
        .await?;
    std::fs::write(root.join(path.as_str()), b"replaced")?;
    copy(source, target, &path).await?;
    let error = target
        .namespace()?
        .persist_observed(&expected, CancellationToken::new())
        .await
        .err()
        .ok_or("replacement accepted")?;
    assert!(
        matches!(error, StorageRoleFailure::Entry(error) if error.class() == FailureClass::Conflict)
    );
    Ok(())
}

async fn parent_replacement(source: &Storage, target: &Storage, root: &std::path::Path) -> Result {
    std::fs::create_dir_all(root.join("persist-parent"))?;
    std::fs::write(root.join("persist-parent/file"), b"original")?;
    let path = StoragePath::new("persist-parent/file")?;
    create_directory_all(target, &StoragePath::new("persist-parent")?).await?;
    copy(source, target, &path).await?;
    let expected = target.namespace()?.stat(&path).await?;
    target
        .namespace()?
        .rename(
            &StoragePath::new("persist-parent")?,
            &StoragePath::new("persist-parent-held")?,
        )
        .await?;
    create_directory_all(target, &StoragePath::new("persist-parent")?).await?;
    std::fs::write(root.join("persist-parent/file"), b"replaced")?;
    copy(source, target, &path).await?;
    let error = target
        .namespace()?
        .persist_observed(&expected, CancellationToken::new())
        .await
        .err()
        .ok_or("cached parent accepted")?;
    assert!(
        matches!(error, StorageRoleFailure::Entry(error) if error.class() == FailureClass::Conflict)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the private NFS relay armed on COMMIT requests"]
async fn real_nfs_persistence_fault_contract() -> Result {
    for mode in ["flash", "disconnect", "stall", "cancel", "lost_ack"] {
        if env::var("LAB_NFS_DIRECT_ONLY").is_ok_and(|selected| selected != mode) {
            continue;
        }
        timeout(Duration::from_secs(50), Box::pin(persistence_fault(mode))).await??;
    }
    Ok(())
}

async fn cancel_after_commit(cancel: CancellationToken) -> Result<Instant> {
    control("wait-fault").await?;
    sleep(Duration::from_millis(250)).await;
    let signalled = Instant::now();
    cancel.cancel();
    Ok(signalled)
}

async fn persistence_fault(mode: &'static str) -> Result {
    control("normal").await?;
    let target = connect(&env::var("LAB_NFS_DIRECT_PROXY_URL")?).await?;
    let path = StoragePath::new(format!("persist-fault-{mode}"))?;
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join(path.as_str()), b"headtail")?;
    let source = common::connect_local(root.path(), 2).await?;
    copy(&source, &target, &path).await?;
    let expected = target.namespace()?.stat(&path).await?;
    control(&format!("commit-{mode}")).await?;
    let cancel = CancellationToken::new();
    let started = Instant::now();
    let namespace = target.namespace()?;
    let operation = namespace.persist_observed(&expected, cancel.clone());
    let result = if mode == "cancel" {
        let (result, signalled) = tokio::join!(operation, cancel_after_commit(cancel));
        let latency = signalled?.elapsed();
        assert!(latency < Duration::from_secs(1));
        assert!(
            matches!(&result, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Cancelled)
        );
        println!(
            "real NFS persistence fault=cancel return_after_signal_ms={}",
            latency.as_millis()
        );
        result
    } else {
        operation.await
    };
    println!(
        "real NFS persistence fault={mode} elapsed_ms={} result={result:?}",
        started.elapsed().as_millis()
    );
    if let Err(error) = result {
        let detail = match &error {
            StorageRoleFailure::Entry(error) => error.diagnostic(),
            StorageRoleFailure::Session(error) => error.diagnostic(),
        };
        println!("real NFS persistence fault={mode} cause={detail}");
    } else {
        assert!(matches!(mode, "flash" | "lost_ack"));
    }
    control("normal").await?;
    let retry = connect(&env::var("LAB_NFS_DIRECT_URL")?).await?;
    let current = retry.namespace()?.stat(&path).await?;
    retry
        .namespace()?
        .persist_observed(&current, CancellationToken::new())
        .await?;
    assert_eq!(std::fs::read(root.path().join(path.as_str()))?, b"headtail");
    println!(
        "real NFS persistence fault={mode} explicit_persistence_retry=true source_retained=true"
    );
    Ok(())
}
