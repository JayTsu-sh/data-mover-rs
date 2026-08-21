use std::path::{Path, PathBuf};

use clap::Parser;
use data_mover::{ConsistencyCheck, Result, StorageEnum, create_storage};

mod hdfs_support;

fn creation_options(location: &str) -> data_mover::CreateStorageOptions {
    data_mover::CreateStorageOptions {
        backend: if location.starts_with("hdfs://") {
            data_mover::BackendConfig::Hdfs(hdfs_support::config())
        } else {
            data_mover::BackendConfig::Default
        },
        ..Default::default()
    }
}

#[derive(Debug, Parser)]
#[command(about = "Reopen one stored file and print its exact size and BLAKE3 hash")]
struct Args {
    #[arg(long)]
    storage: String,

    #[arg(long)]
    path: String,

    /// Signal that metadata resolution is complete before starting the data read.
    #[arg(long, requires = "go_file")]
    ready_file: Option<PathBuf>,

    /// Wait for this file before starting the data read.
    #[arg(long, requires = "ready_file")]
    go_file: Option<PathBuf>,

    /// Nightly fault-injection aid: delay consumption of each bounded read chunk.
    #[arg(long, default_value_t = 0)]
    chunk_delay_ms: u64,

    /// Print only the applicable permission bits instead of reading file data.
    #[arg(long)]
    print_mode: bool,
}

async fn compute_hash(
    storage: &StorageEnum,
    entry: &data_mover::EntryEnum,
    delay_ms: u64,
) -> Result<String> {
    if delay_ms == 0 {
        return storage
            .compute_hash(entry.get_relative_path(), entry.get_size())
            .await;
    }
    let (mut receiver, reader) =
        StorageEnum::read_chunk_stream(storage, entry, None, None, true, 1);
    let mut read = 0_u64;
    while let Some(chunk) = receiver.recv().await {
        read = read
            .checked_add(u64::try_from(chunk.data.len()).map_err(|_| {
                data_mover::error::StorageError::OperationError(
                    "inspection chunk length does not fit u64".to_string(),
                )
            })?)
            .ok_or_else(|| {
                data_mover::error::StorageError::OperationError(
                    "inspection read length overflowed".to_string(),
                )
            })?;
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
    }
    let hash = reader
        .await
        .map_err(|error| {
            data_mover::error::StorageError::OperationError(format!(
                "inspection read task panicked: {error}"
            ))
        })??
        .ok_or_else(|| {
            data_mover::error::StorageError::OperationError(
                "inspection read did not return a hash".to_string(),
            )
        })?;
    if read != entry.get_size() {
        return Err(data_mover::error::StorageError::OperationError(format!(
            "inspection read length mismatch: expected {}, got {read}",
            entry.get_size()
        )));
    }
    Ok(hash.finalize())
}

async fn wait_for_go(ready_file: Option<&Path>, go_file: Option<&Path>) -> Result<()> {
    let (Some(ready_file), Some(go_file)) = (ready_file, go_file) else {
        return Ok(());
    };
    tokio::fs::write(ready_file, b"ready\n").await?;
    for _ in 0..3_000 {
        if tokio::fs::try_exists(go_file).await? {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    Err(data_mover::error::StorageError::OperationError(
        "inspection start barrier timed out".to_string(),
    ))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let storage = create_storage(&args.storage, creation_options(&args.storage)).await?;
    let path = Path::new(&args.path);
    let entry = storage.get_metadata(path).await?;
    if !entry.get_is_regular_file() {
        return Err(data_mover::error::StorageError::OperationError(
            "inspection target is not a regular file".to_string(),
        ));
    }
    if args.print_mode {
        let mode = entry.get_mode().ok_or_else(|| {
            data_mover::error::StorageError::OperationError(
                "inspection target has no permission mode".to_string(),
            )
        })?;
        println!("{:o}", mode & 0o7777);
        return Ok(());
    }
    let size = entry.get_size();
    wait_for_go(args.ready_file.as_deref(), args.go_file.as_deref()).await?;
    let hash = compute_hash(&storage, &entry, args.chunk_delay_ms).await?;
    println!("{size}\t{hash}");
    Ok(())
}
