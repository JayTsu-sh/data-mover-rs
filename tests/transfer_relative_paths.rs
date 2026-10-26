//! Copying a scanned storage root to another root preserves relative paths by default.
use std::num::NonZeroUsize;
use std::path::Path;

use data_mover::model::{EntryKind, StoragePath};
use data_mover::storage::{BackendConfig, LocalBackendConfig, Storage, connect_backend};
use data_mover::transfer::{TransferPolicy, TransferRequest, transfer};
use data_mover::traversal::{
    StorageTraversalSource, TraversalItem, TraversalOutcome, TraversalRequest, TraversalSource as _,
};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn local(root: &Path) -> TestResult<Storage> {
    Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
        rename_context: None,
        read_chunk_bytes: None,
        write_chunk_bytes: None,
        root: root.to_owned(),
        read_concurrency: NonZeroUsize::MIN,
        write_concurrency: NonZeroUsize::MIN,
    }))
    .await?)
}

#[tokio::test]
async fn scanned_files_keep_their_relative_paths_between_different_roots() -> TestResult {
    for observed in [false, true] {
        for policy in [TransferPolicy::Checkpointed, TransferPolicy::AtomicReplace] {
            let temp = tempfile::tempdir()?;
            let source_root = temp.path().join("source-prefix");
            let destination_root = temp.path().join("destination-prefix");
            std::fs::create_dir_all(source_root.join("a"))?;
            // As for every file transfer, the destination's parent directory already exists.
            std::fs::create_dir_all(destination_root.join("a"))?;
            std::fs::write(source_root.join("a/b.txt"), b"nested payload")?;
            std::fs::write(temp.path().join("outside.txt"), b"outside the source root")?;
            let source = local(&source_root).await?;
            let destination = local(&destination_root).await?;
            let mut scan = StorageTraversalSource::new(&source)?
                .traverse(TraversalRequest::new(StoragePath::root()));
            let mut copied = Vec::new();
            while let Some(item) = scan.next_item().await {
                let TraversalItem::Entry(entry) = item else {
                    return Err(format!("unexpected scan item: {item:?}").into());
                };
                if entry.kind() != EntryKind::File {
                    continue;
                }
                copied.push(entry.path().as_str().to_owned());
                let request = if observed {
                    TransferRequest::from_observation(
                        source.clone(),
                        *entry,
                        destination.clone(),
                        CancellationToken::new(),
                    )?
                } else {
                    TransferRequest::new(
                        source.clone(),
                        entry.path().clone(),
                        destination.clone(),
                        CancellationToken::new(),
                    )
                }
                .with_transfer_policy(policy);
                assert_eq!(request.final_path(), request.source_path());
                let identity = request.identity();
                assert_eq!(transfer(request).await?.identity, identity);
            }
            assert!(matches!(
                scan.finish().await?,
                TraversalOutcome::Completed(_)
            ));
            assert_eq!(copied, ["a/b.txt"]);
            assert_eq!(
                std::fs::read(destination_root.join("a/b.txt"))?,
                b"nested payload"
            );
            assert!(!destination_root.join("source-prefix").exists());
            assert!(!destination_root.join("outside.txt").exists());
        }
    }
    Ok(())
}
