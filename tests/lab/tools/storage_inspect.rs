use std::path::{Path, PathBuf};

use clap::Parser;
#[allow(dead_code)]
#[path = "../../../examples/support/endpoint.rs"]
mod endpoint_support;
use data_mover::model::{EntryKind, StoragePath};
use data_mover::storage::{ReadRequest, SourceDescriptor, Storage};
use endpoint_support::Result;
use futures::StreamExt as _;

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
    storage: &Storage,
    entry: &SourceDescriptor,
    delay_ms: u64,
) -> Result<String> {
    let size = entry.size.ok_or("inspection size is unknown")?;
    let source = storage.read_source()?;
    let mut stream = source
        .read(ReadRequest {
            path: entry.path.clone(),
            range: Some(0..size),
            expected_source: Some(entry.source_identity.clone()),
            maximum_chunk_bytes: 2 * 1024 * 1024,
            read_inflight: 4,
            read_budget: None,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
            version: entry.version().clone(),
        })
        .await?;
    let mut read = 0_u64;
    let mut hash = blake3::Hasher::new();
    while let Some(chunk) = stream.next().await {
        let bytes = chunk?;
        read = read
            .checked_add(u64::try_from(bytes.len())?)
            .ok_or("inspection length overflow")?;
        hash.update(&bytes);
        if delay_ms != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
    }
    if read != size {
        return Err(format!("inspection read length mismatch: expected {size}, got {read}").into());
    }
    Ok(hash.finalize().to_hex().to_string())
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
    Err("inspection start barrier timed out".into())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let storage = endpoint_support::connect(&args.storage).await?;
    let path = StoragePath::new(args.path.clone())?;
    let entry = storage.read_source()?.describe(&path).await?;
    if entry.kind != EntryKind::File {
        return Err("inspection target is not a regular file".into());
    }
    if args.print_mode {
        let facts = storage.metadata()?.permissions(&entry).await?;
        println!(
            "{:o}",
            facts
                .mode
                .ok_or("inspection target has no permission mode")?
                & 0o7777
        );
        return Ok(());
    }
    let size = entry.size.ok_or("inspection size is unknown")?;
    wait_for_go(args.ready_file.as_deref(), args.go_file.as_deref()).await?;
    let hash = compute_hash(&storage, &entry, args.chunk_delay_ms).await?;
    println!("{size}\t{hash}");
    Ok(())
}
