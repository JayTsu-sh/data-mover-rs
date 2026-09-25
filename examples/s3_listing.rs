//! Lists a legacy storage through `StorageEnum` — any URL `create_storage` takes (S3, NFS, HDFS, a
//! local path) — and prints one line per entry, so a script can check exactly which paths a walk
//! reports; optionally deletes one directory first.
//!
//! ```text
//! cargo run --example s3_listing -- 's3://AK:SK@bucket.host:9000/prefix' [--sub dir] [--walkdir-2]
//!     [--delete-dir dir]
//! ```
//!
//! Without the URL argument the URL is read from `S3_LISTING_URL`, so a script need not put
//! credentials on a command line.
//!
//! `walkdir` prints `ENTRY <path relative to the storage root> <versionId or -> latest=<0|1>
//! marker=<0|1> count=<version count or ->` (a versioned bucket lists every version and delete
//! marker), `walkdir_2` prints `FILE <path>` with the same four fields and `DIR <path>`, and
//! `ERROR …` for a reported error (exit 1). `count` is the key's entries in `walkdir` (versions and
//! delete markers) but its versions only in `walkdir_2`, which lists no markers. A walk that has not finished within
//! `--timeout-secs` exits 2: a walk that re-lists a directory forever must not hang the caller.

use std::env;
use std::path::Path;
use std::process::exit;
use std::time::Duration;

use clap::Parser;
use data_mover::dir_tree::NdxEvent;
use data_mover::error::StorageError;
use data_mover::storage_enum::{StorageEnum, create_storage};
use data_mover::{CreateStorageOptions, EntryEnum, Result, StorageEntryMessage, WalkOptions};
use tokio::time::timeout;

/// Where the URL is read from when it is not given as an argument.
const URL_ENV: &str = "S3_LISTING_URL";

#[derive(Debug, Parser)]
struct Args {
    /// `s3://AK:SK@bucket.host:port/prefix` (or any `s3+…://` profile); `S3_LISTING_URL` if absent.
    url: Option<String>,
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

/// `<path> <versionId or -> latest=<0|1> marker=<0|1> count=<version count or ->`.
fn describe(entry: &EntryEnum) -> String {
    format!(
        "{} {} latest={} marker={} count={}",
        entry.get_relative_path().display(),
        entry.get_version_id().unwrap_or("-"),
        u8::from(entry.get_is_latest()),
        u8::from(entry.get_is_delete_marker()),
        entry
            .get_version_count()
            .map_or_else(|| "-".to_string(), |count| count.to_string())
    )
}

/// Returns whether the walk reported an error.
async fn walk(storage: &StorageEnum, sub: Option<&Path>) -> Result<bool> {
    let mut failed = false;
    let entries = storage.walkdir(sub, WalkOptions::default()).await?;
    while let Some(message) = entries.next().await {
        match message {
            StorageEntryMessage::Scanned(entry) => println!("ENTRY {}", describe(&entry)),
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
                    println!("FILE {}", describe(&file.entry));
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
    let mut args = Args::parse();
    let Some(url) = args.url.take().or_else(|| env::var(URL_ENV).ok()) else {
        return Err(StorageError::ConfigError(format!(
            "give the storage URL or set {URL_ENV}"
        )));
    };
    let storage = create_storage(&url, CreateStorageOptions::default()).await?;
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
