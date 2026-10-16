//! Create, inspect and remove an isolated nested Local directory.
use data_mover::model::StoragePath;
use data_mover::storage::{
    BackendConfig, DeleteTreeItem, DeleteTreeOutcome, DeleteTreeRequest, LocalBackendConfig,
    connect_backend, create_directory_all, delete_tree,
};
use std::num::NonZeroUsize;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let slots = NonZeroUsize::new(4).ok_or("nonzero")?;
    let storage = connect_backend(BackendConfig::Local(LocalBackendConfig {
        read_chunk_bytes: None,
        write_chunk_bytes: None,
        root: root.path().to_owned(),
        read_concurrency: slots,
        write_concurrency: slots,
    }))
    .await?;
    let path = StoragePath::new("dir1/dir2/dir3")?;
    create_directory_all(&storage, &path).await?;
    println!("{:?}", storage.namespace()?.stat(&path).await?);
    let mut session = delete_tree(
        &storage,
        DeleteTreeRequest {
            root: StoragePath::new("dir1")?,
            delete_root: true,
            max_inflight_operations: slots,
            max_buffered_items: slots,
            cancel: CancellationToken::new(),
        },
    )?;
    let mut failures = 0;
    while let Some(item) = session.next_item().await {
        if matches!(item, DeleteTreeItem::EntryFailure(_)) {
            failures += 1;
        }
    }
    if !matches!(session.finish().await?, DeleteTreeOutcome::Completed(_)) || failures != 0 {
        return Err("directory cleanup failed".into());
    }
    Ok(())
}
