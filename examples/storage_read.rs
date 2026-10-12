//! Reads bounded chunks from a described source and verifies the complete byte count.
use clap::Parser;
use data_mover::model::{EntryKind, StoragePath};
use data_mover::storage::{PreflightPolicy, ReadRequest};
use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

#[allow(dead_code)]
#[path = "support/endpoint.rs"]
mod endpoint_support;
use endpoint_support::Result;

#[derive(Parser)]
struct Args {
    /// A local directory or storage URL.
    #[arg(long)]
    storage: String,
    /// File path relative to the storage root.
    #[arg(long)]
    path: String,
}

#[tokio::main]
async fn main() -> Result {
    let args = Args::parse();
    let storage = endpoint_support::connect_with_options(&args.storage, false, None).await?;
    let source = storage.read_source(&PreflightPolicy::production())?;
    let path = StoragePath::new(args.path)?;
    let described = source.describe(&path).await?;
    if described.kind != EntryKind::File {
        return Err("read target must be a regular file".into());
    }
    let size = described.size.ok_or("source size is unknown")?;
    let mut chunks = source
        .read(ReadRequest {
            path,
            range: Some(0..size),
            expected_source: Some(described.source_identity.clone()),
            maximum_chunk_bytes: 2 * 1024 * 1024,
            read_inflight: 4,
            read_budget: None,
            cancel: CancellationToken::new(),
            source_qos: None,
            version: described.version().clone(),
        })
        .await?;
    let mut bytes = 0_u64;
    let mut hash = blake3::Hasher::new();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk?;
        bytes = bytes
            .checked_add(u64::try_from(chunk.len())?)
            .ok_or("size overflow")?;
        hash.update(&chunk);
    }
    if bytes != size {
        return Err(format!("expected {size} bytes, read {bytes}").into());
    }
    println!("{bytes}\t{}", hash.finalize().to_hex());
    Ok(())
}
