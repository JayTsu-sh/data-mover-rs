//! Lists a legacy storage through `StorageEnum` — any URL `create_storage` takes (S3, NFS, HDFS, a
//! local path) — and prints one line per entry, so a script can check exactly which paths a walk
//! reports; optionally deletes one directory first.
//!
//! ```text
//! cargo run --example s3_listing -- 's3://AK:SK@bucket.host:9000/prefix' [--sub dir] [--walkdir-2]
//!     [--delete-dir dir]
//! ```
//!
//! `walkdir` prints `ENTRY <path relative to the storage root> <versionId or ->`, `walkdir_2` prints
//! `FILE|DIR <path>`, and `ERROR …` for a reported error (exit 1). A walk that has not finished within
//! `--timeout-secs` exits 2: a walk that re-lists a directory forever must not hang the caller.

use std::path::Path;
use std::process::exit;
use std::time::Duration;

use clap::Parser;
use data_mover::dir_tree::NdxEvent;
use data_mover::error::StorageError;
use data_mover::storage_enum::{StorageEnum, create_storage};
use data_mover::{CreateStorageOptions, Result, StorageEntryMessage, WalkOptions};
use tokio::time::timeout;

#[derive(Debug, Parser)]
struct Args {
    /// `s3://AK:SK@bucket.host:port/prefix` (or any `s3+…://` profile).
    url: String,
    /// Walk from this directory below the storage root.
    #[arg(long)]
    sub: Option<String>,
    /// Use the paged `walkdir_2` instead of `walkdir`.
    #[arg(long)]
    walkdir_2: bool,
    /// Delete this directory (`delete_dir_all_with_progress`) before walking.
    #[arg(long)]
    delete_dir: Option<String>,
    #[arg(long, default_value_t = 60)]
    timeout_secs: u64,
}

async fn delete_dir(storage: &StorageEnum, dir: &str) -> Result<()> {
    // An empty path is the storage root: the whole prefix, or the whole bucket without one.
    if dir.trim_matches('/').is_empty() {
        return Err(StorageError::ConfigError(
            "--delete-dir needs a directory below the storage root".to_string(),
        ));
    }
    let events = storage.delete_dir_all_with_progress(Some(Path::new(dir)), 4)?;
    while let Some(event) = events.next().await {
        if let Some(error) = event.error {
            return Err(StorageError::OperationError(format!(
                "delete {dir}: {error}"
            )));
        }
    }
    println!("DELETED {dir}");
    Ok(())
}

/// Returns whether the walk reported an error.
async fn walk(storage: &StorageEnum, sub: Option<&Path>) -> Result<bool> {
    let mut failed = false;
    let entries = storage.walkdir(sub, WalkOptions::default()).await?;
    while let Some(message) = entries.next().await {
        match message {
            StorageEntryMessage::Scanned(entry) => println!(
                "ENTRY {} {}",
                entry.get_relative_path().display(),
                entry.get_version_id().unwrap_or("-")
            ),
            StorageEntryMessage::Error { path, reason, .. } => {
                println!("ERROR {}: {reason}", path.display());
                failed = true;
            }
            _ => {}
        }
    }
    Ok(failed)
}

/// Returns whether the walk reported an error.
async fn walk_2(storage: &StorageEnum, sub: Option<&Path>) -> Result<bool> {
    let mut failed = false;
    let events = storage.walkdir_2(sub, None, None, None, 4, false).await?;
    while let Some(event) = events.next().await {
        match event {
            NdxEvent::Page(page) => {
                for file in &page.files {
                    println!("FILE {}", file.entry.get_relative_path().display());
                }
                for dir in &page.subdirs {
                    println!("DIR {}", dir.entry.get_relative_path().display());
                }
            }
            NdxEvent::Error { path, reason } => {
                println!("ERROR {path}: {reason}");
                failed = true;
            }
            NdxEvent::Done => break,
        }
    }
    Ok(failed)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let storage = create_storage(&args.url, CreateStorageOptions::default()).await?;
    if let Some(dir) = &args.delete_dir {
        delete_dir(&storage, dir).await?;
    }
    let sub = args.sub.as_deref().map(Path::new);
    let walked = if args.walkdir_2 {
        timeout(
            Duration::from_secs(args.timeout_secs),
            walk_2(&storage, sub),
        )
        .await
    } else {
        timeout(Duration::from_secs(args.timeout_secs), walk(&storage, sub)).await
    };
    let Ok(result) = walked else {
        println!("TIMEOUT after {} s", args.timeout_secs);
        exit(2);
    };
    if result? {
        exit(1);
    }
    Ok(())
}
