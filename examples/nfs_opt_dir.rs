//! Creates and removes a nested directory under an NFS URL (`e2e-nfs` step 4).
//!
//! ```text
//! cargo run --example nfs_opt_dir -- 'nfs://host/export[:/sub]?uid=0&gid=0&version=4.1'
//! ```
//!
//! Works only inside its own scratch directory `dm-e2e-opt-dir-<nanos>` below the URL's root, and
//! removes exactly that directory at the end: nothing else under the export is touched.

use std::env;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use data_mover::error::StorageError;
use data_mover::{NFSStorage, Result};

const NESTED: &str = "dir1/dir2/dir3";

#[tokio::main]
async fn main() -> Result<()> {
    let url = env::args().nth(1).ok_or_else(|| {
        StorageError::ConfigError("usage: nfs_opt_dir <nfs://host/export[:/sub]?...>".to_string())
    })?;
    let storage = NFSStorage::new(&url, None).await?;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| StorageError::OperationError(error.to_string()))?
        .as_nanos();
    let scratch = format!("dm-e2e-opt-dir-{nanos}");
    // Never adopt an existing directory: everything this example deletes, it created.
    if storage.get_metadata(Path::new(&scratch)).await.is_ok() {
        return Err(StorageError::OperationError(format!(
            "{scratch} already exists"
        )));
    }
    storage.create_dir_all(Path::new(&scratch)).await?;

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
async fn create_twice(url: &str, storage: &NFSStorage, scratch: &str) -> Result<()> {
    let nested = Path::new(scratch).join(NESTED);
    storage.create_dir_all(&nested).await?;
    let rooted = NFSStorage::new(&sub_root_url(url, scratch), None).await?;
    rooted.create_dir_all(Path::new(NESTED)).await?;
    let entry = rooted.get_metadata(Path::new(NESTED)).await?;
    if !entry.get_is_dir() {
        return Err(StorageError::OperationError(format!(
            "{} is not a directory",
            nested.display()
        )));
    }
    println!("created {} (twice)", nested.display());
    Ok(())
}

/// Deletes the scratch directory and requires every event to succeed, including the one for the
/// scratch directory itself: `delete_dir_all` alone reports no failure.
async fn remove(storage: &NFSStorage, scratch: &str) -> Result<()> {
    let events = storage.delete_dir_all_with_progress(Some(Path::new(scratch)), 4)?;
    let mut root_removed = false;
    let mut failures = Vec::new();
    while let Some(event) = events.next().await {
        if let Some(error) = event.error {
            failures.push(format!("{}: {error}", event.relative_path.display()));
        } else if event.is_dir && event.relative_path == Path::new(scratch) {
            root_removed = true;
        }
    }
    if !failures.is_empty() || !root_removed {
        return Err(StorageError::OperationError(format!(
            "removing {scratch} failed (root removed: {root_removed}): {failures:?}"
        )));
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
