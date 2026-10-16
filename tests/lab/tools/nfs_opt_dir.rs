//! Creates and removes a nested directory under an NFS URL (`e2e-nfs` step 4).
//!
//! ```text
//! cargo run --example nfs_opt_dir -- 'nfs://host/export[:/sub]?uid=0&gid=0&version=4.1'
//! ```
//!
//! Works only inside its own scratch directory `dm-e2e-opt-dir-<nanos>` below the URL's root, and
//! removes exactly that directory at the end: nothing else under the export is touched.

use std::env;
use std::num::NonZeroUsize;
use std::time::{SystemTime, UNIX_EPOCH};

use data_mover::error::StorageError;
use data_mover::model::{EntryKind, FailureClass, StoragePath};
use data_mover::storage::{
    BackendConfig, DeleteTreeItem, DeleteTreeOutcome, DeleteTreeRequest, NfsBackendConfig, Storage,
    StorageRoleFailure, connect_backend, create_directory_all, delete_tree,
};
use tokio_util::sync::CancellationToken;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

const NESTED: &str = "dir1/dir2/dir3";

#[tokio::main]
async fn main() -> Result<()> {
    let url = env::args().nth(1).ok_or_else(|| {
        StorageError::ConfigError("usage: nfs_opt_dir <nfs://host/export[:/sub]?...>".to_string())
    })?;
    let storage = connect(&url).await?;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| StorageError::OperationError(error.to_string()))?
        .as_nanos();
    let scratch = format!("dm-e2e-opt-dir-{nanos}");
    // Only a definite NotFound permits this run to adopt the fresh scratch path.
    let path = StoragePath::new(&scratch)?;
    match storage.namespace()?.stat(&path).await {
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => {}
        Err(error) => return Err(error.into()),
        Ok(_) => return Err(format!("{scratch} already exists").into()),
    }
    create_directory_all(&storage, &path).await?;

    let checked = create_twice(&url, &storage, &scratch).await;
    let removed = remove(&storage, &scratch).await;
    checked?;
    removed?;
    println!("removed {scratch}");
    Ok(())
}

/// Creates `NESTED` below the scratch directory, then creates it again through a second connection
/// rooted at the scratch directory. The second connection has its own root handle and so an empty
/// handle cache: every level goes to the server as `mkdir` of an existing directory.
async fn connect(url: &str) -> Result<Storage> {
    Ok(connect_backend(BackendConfig::Nfs(NfsBackendConfig {
        url: url.to_owned(),
        block_size: None,
        ensure_dir: false,
    }))
    .await?)
}

async fn create_twice(url: &str, storage: &Storage, scratch: &str) -> Result<()> {
    let nested = StoragePath::new(format!("{scratch}/{NESTED}"))?;
    create_directory_all(storage, &nested).await?;
    let rooted = connect(&sub_root_url(url, scratch)).await?;
    let path = StoragePath::new(NESTED)?;
    create_directory_all(&rooted, &path).await?;
    let entry = rooted.namespace()?.stat(&path).await?;
    if entry.kind != EntryKind::Directory {
        return Err(format!("{nested} is not a directory").into());
    }
    println!("created {nested} (twice)");
    Ok(())
}

/// Requires successful per-entry events, the scratch root deletion and positive completion.
async fn remove(storage: &Storage, scratch: &str) -> Result<()> {
    let slots = NonZeroUsize::new(4).ok_or("nonzero")?;
    let root = StoragePath::new(scratch)?;
    let mut session = delete_tree(
        storage,
        DeleteTreeRequest {
            root: root.clone(),
            delete_root: true,
            max_inflight_operations: slots,
            max_buffered_items: slots,
            cancel: CancellationToken::new(),
        },
    )?;
    let mut root_removed = false;
    let mut failures = Vec::new();
    while let Some(event) = session.next_item().await {
        match event {
            DeleteTreeItem::Deleted { path, kind }
                if path == root && kind == EntryKind::Directory =>
            {
                root_removed = true;
            }
            DeleteTreeItem::EntryFailure(error) => failures.push(format!("{error:?}")),
            DeleteTreeItem::Deleted { .. } => {}
        }
    }
    let complete = matches!(session.finish().await?, DeleteTreeOutcome::Completed(_));
    if !complete || !failures.is_empty() || !root_removed {
        return Err(format!(
            "removing {scratch} failed (root removed: {root_removed}): {failures:?}"
        )
        .into());
    }
    Ok(())
}

/// `nfs://host/export[:/sub]?query` with `dir` appended to the sub-root.
fn sub_root_url(url: &str, dir: &str) -> String {
    let (base, query) = url
        .split_once('?')
        .map_or((url, String::new()), |(base, query)| {
            (base, format!("?{query}"))
        });
    let path_start = base
        .find("://")
        .and_then(|scheme_end| {
            base[scheme_end + 3..]
                .find('/')
                .map(|slash| scheme_end + 3 + slash)
        })
        .unwrap_or(base.len());
    let separator = if base[path_start..].contains(':') {
        "/"
    } else {
        ":/"
    };
    format!("{}{separator}{dir}{query}", base.trim_end_matches('/'))
}
