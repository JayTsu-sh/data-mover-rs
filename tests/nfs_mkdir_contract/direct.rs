//! 在 runner 独占前缀内验证真实 Direct 传输、发布与清理，最终内容由服务端独立读取。

use std::{env, sync::Arc, time::Duration};

use bytes::Bytes;
use data_mover::model::{
    EntryOperationFailure, FailureClass, Operation, OwnershipMode, StoragePath, Transience,
};
use data_mover::storage::{
    FinalDestination, MetadataMutation, PrepareRequest, Storage, StorageRoleFailure, WriteProgress,
    WriteProgressObserver, create_directory_all,
};
use data_mover::transfer::{
    FailedDataCleanup, MetadataFailurePolicy, MetadataTransferMode, ReadBackVerification,
    TransferPhase, TransferPolicy, TransferRequest, transfer,
};
use futures::stream;
use tokio::sync::Notify;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::{Result, chmod, common, connect, with_identity};

#[path = "direct/fault.rs"]
mod fault;
#[path = "direct/nonfile.rs"]
mod nonfile;
#[path = "direct/persistence.rs"]
mod persistence;
#[path = "direct/purge.rs"]
mod purge;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires run-nfs-direct-contract.py and its owned NFS prefix"]
async fn real_nfs_direct_data_and_metadata_contract() -> Result {
    let url = env::var("LAB_NFS_DIRECT_URL")?;
    let target = connect(&url).await?;
    let root = tempfile::tempdir()?;
    let source = common::connect_local(root.path(), 2).await?;
    for full in [false, true] {
        let name = if full { "normal-full" } else { "normal-quick" };
        for payload in [&vec![0x41; 1_048_576][..], b"direct-final-payload"] {
            std::fs::write(root.path().join(name), payload)?;
            let outcome = transfer(
                TransferRequest::new(
                    source.clone(),
                    StoragePath::new(name)?,
                    target.clone(),
                    CancellationToken::new(),
                )
                .with_transfer_policy(TransferPolicy::Direct)
                .with_failed_data_cleanup(FailedDataCleanup::RemoveOwned)
                .with_read_back_verification(if full {
                    ReadBackVerification::Enabled
                } else {
                    ReadBackVerification::Disabled
                }),
            )
            .await?;
            assert_eq!(outcome.transferred_bytes, payload.len() as u64);
            assert_eq!(std::fs::read(root.path().join(name))?, payload);
        }
    }
    std::fs::write(root.path().join("empty"), b"")?;
    transfer(
        TransferRequest::new(
            source.clone(),
            StoragePath::new("empty")?,
            target.clone(),
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Direct)
        .with_failed_data_cleanup(FailedDataCleanup::RemoveOwned),
    )
    .await?;
    source_alias(&target).await?;
    source_failure(&source, &target, root.path()).await?;
    replacement(&source, &target, root.path()).await?;
    readonly_parent(&url, &source, &target, root.path()).await?;
    metadata_refusal(&url, &source, root.path()).await?;
    println!(
        "real NFS Direct normal=quick,full overwrite=long_to_short empty=0 source_failure=cleaned metadata_failure=retained"
    );
    Ok(())
}

async fn source_alias(target: &Storage) -> Result {
    let failure = transfer(
        TransferRequest::new(
            target.clone(),
            StoragePath::new("normal-quick")?,
            target.clone(),
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Direct),
    )
    .await
    .err()
    .ok_or("source alias was copied")?;
    assert_eq!(failure.phase(), TransferPhase::Prepare);
    println!("real NFS Direct source alias refused before mutation=true");
    Ok(())
}

async fn replacement(source: &Storage, target: &Storage, root: &std::path::Path) -> Result {
    let path = StoragePath::new("replacement")?;
    std::fs::write(root.join(path.as_str()), b"owned")?;
    let destination = target.staged_destination()?;
    let stage = destination
        .prepare_direct(
            PrepareRequest {
                final_destination: FinalDestination::new(path.clone()),
                source: source.read_source()?.describe(&path).await?,
                recovery_binding: [8; 32],
            },
            CancellationToken::new(),
        )
        .await?;
    destination
        .write(
            &stage,
            Box::pin(stream::iter([Ok(Bytes::from_static(b"owned"))])),
        )
        .await?;
    target
        .namespace()?
        .rename(&path, &StoragePath::new("held-replacement")?)
        .await?;
    std::fs::write(root.join(path.as_str()), b"unrelated replacement")?;
    transfer(
        TransferRequest::new(
            source.clone(),
            path.clone(),
            target.clone(),
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Direct),
    )
    .await?;
    let error = destination
        .remove_failed_direct(stage)
        .await
        .err()
        .ok_or("replacement was deleted")?;
    assert!(
        matches!(error, StorageRoleFailure::Entry(error) if error.class() == FailureClass::Conflict)
    );
    assert_eq!(target.namespace()?.stat(&path).await?.size, Some(21));
    println!("real NFS Direct replacement protected=true original_fh_held=true");
    Ok(())
}

async fn source_failure(source: &Storage, target: &Storage, root: &std::path::Path) -> Result {
    let path = StoragePath::new("source-failure")?;
    std::fs::write(root.join(path.as_str()), b"full source payload")?;
    let described = source.read_source()?.describe(&path).await?;
    let destination = target.staged_destination()?;
    let mut stage = destination
        .prepare_direct(
            PrepareRequest {
                final_destination: FinalDestination::new(path.clone()),
                source: described,
                recovery_binding: [7; 32],
            },
            CancellationToken::new(),
        )
        .await?;
    let confirmed = Arc::new(Notify::new());
    let notify = confirmed.clone();
    stage.set_write_observer(Some(WriteProgressObserver::new(move |event| {
        if matches!(event, WriteProgress::Confirmed { .. }) {
            notify.notify_one();
        }
    })));
    let error = StorageRoleFailure::Entry(EntryOperationFailure::new(
        path.clone(),
        Operation::Read,
        FailureClass::Corruption,
        Transience::Permanent,
        "injected source failure after confirmed NFS write",
    )?);
    let input = stream::unfold((0, confirmed), move |(index, confirmed)| {
        let error = error.clone();
        async move {
            match index {
                0 => Some((Ok(Bytes::from_static(b"partial")), (1, confirmed))),
                1 => {
                    confirmed.notified().await;
                    Some((Err(error), (2, confirmed)))
                }
                _ => None,
            }
        }
    });
    assert!(
        timeout(
            Duration::from_secs(10),
            destination.write(&stage, Box::pin(input))
        )
        .await?
        .is_err()
    );
    destination.remove_failed_direct(stage).await?;
    assert!(
        matches!(target.namespace()?.stat(&path).await, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound)
    );
    assert_eq!(
        std::fs::read(root.join(path.as_str()))?,
        b"full source payload"
    );
    Ok(())
}

async fn metadata_refusal(url: &str, source: &Storage, root: &std::path::Path) -> Result {
    let path = StoragePath::new("metadata-refusal")?;
    std::fs::write(root.join(path.as_str()), b"metadata failure retains data")?;
    let target = connect(&with_identity(url, 4242)).await?;
    let error = transfer(
        TransferRequest::new(
            source.clone(),
            path.clone(),
            target.clone(),
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Direct)
        .with_failed_data_cleanup(FailedDataCleanup::RemoveOwned)
        .with_metadata_failure_policy(MetadataFailurePolicy::PublishData),
    )
    .await
    .err()
    .ok_or("unprivileged ownership change succeeded")?;
    assert_eq!(error.phase(), TransferPhase::Metadata, "{error}");
    assert!(error.published_data().is_some(), "{error}");
    let error = error.cleanup_for_policy(TransferPolicy::Direct).await;
    assert!(error.cleanup_failure().is_none(), "{error}");
    assert_eq!(target.namespace()?.stat(&path).await?.size, Some(29));
    assert_eq!(
        std::fs::read(root.join(path.as_str()))?,
        b"metadata failure retains data"
    );
    println!("real NFS Direct metadata refusal detail={error}");
    Ok(())
}

async fn readonly_parent(
    url: &str,
    source: &Storage,
    target: &Storage,
    root: &std::path::Path,
) -> Result {
    let parent = "readonly-parent";
    create_directory_all(target, &StoragePath::new(parent)?).await?;
    chmod(target, parent, 0o777).await?;
    std::fs::create_dir(root.join(parent))?;
    let path = StoragePath::new("readonly-parent/file")?;
    std::fs::write(root.join(path.as_str()), b"old")?;
    transfer(
        TransferRequest::new(
            source.clone(),
            path.clone(),
            target.clone(),
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Direct),
    )
    .await?;
    target
        .metadata()?
        .apply(
            &path,
            MetadataMutation::NumericOwnership(OwnershipMode {
                uid: 4242,
                gid: 4242,
                mode: 0o666,
            }),
            CancellationToken::new(),
        )
        .await?;
    chmod(target, parent, 0o555).await?;
    std::fs::write(
        root.join(path.as_str()),
        b"writable file in readonly parent",
    )?;
    let writer = connect(&with_identity(url, 4242)).await?;
    transfer(
        TransferRequest::new(
            source.clone(),
            path.clone(),
            writer.clone(),
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Direct)
        .with_metadata_mode(MetadataTransferMode::CallerManaged),
    )
    .await?;
    assert_eq!(
        writer.namespace()?.stat(&path).await?.size,
        Some(b"writable file in readonly parent".len() as u64)
    );
    println!("real NFS Direct readonly parent overwrite=true");
    Ok(())
}
