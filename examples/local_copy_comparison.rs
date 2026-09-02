use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::{Parser, ValueEnum};
use data_mover::model::{BackendIdentity, BackendKind, StoragePath};
use data_mover::storage::{BackendConfig, LocalBackendConfig, connect_backend};
use data_mover::transfer::{InflightLimits, TransferIdentity, TransferRequest, transfer};
use data_mover::{
    CopyOptions, CreateStorageOptions, StorageEnum, TransferConcurrency, create_storage,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Implementation {
    /// The pre-architecture `LocalStorage` path routed through `StorageEnum`.
    Legacy,
    /// The legacy public path plus the durability barrier absent from its normal copy lifecycle.
    LegacyDurable,
    /// The role-based Local backend routed through the unified transfer lifecycle.
    Optimized,
}

#[derive(Debug, Parser)]
#[command(about = "Compare legacy and role-based Local-to-Local copy implementations")]
struct Args {
    #[arg(long, value_enum)]
    implementation: Implementation,
    #[arg(long)]
    source: PathBuf,
    #[arg(long)]
    destination: PathBuf,
    #[arg(long)]
    path: String,
    #[arg(long, default_value_t = 2 * 1024 * 1024)]
    chunk_bytes: usize,
    #[arg(long, default_value_t = 8)]
    read_inflight: usize,
    #[arg(long, default_value_t = 8)]
    write_inflight: usize,
}

fn non_zero(value: usize, field: &'static str) -> Result<NonZeroUsize, Box<dyn std::error::Error>> {
    NonZeroUsize::new(value).ok_or_else(|| format!("{field} must be greater than zero").into())
}

async fn legacy_copy(
    args: &Args,
    durable: bool,
) -> Result<(u64, u128), Box<dyn std::error::Error>> {
    let chunk_bytes = u64::try_from(args.chunk_bytes)?;
    let source = create_storage(
        &args.source.to_string_lossy(),
        CreateStorageOptions::new(Some(chunk_bytes), false),
    )
    .await?
    .with_transfer_concurrency(TransferConcurrency::new(
        args.read_inflight,
        args.write_inflight,
    )?);
    let destination = create_storage(
        &args.destination.to_string_lossy(),
        CreateStorageOptions::new(Some(chunk_bytes), true),
    )
    .await?
    .with_transfer_concurrency(TransferConcurrency::new(
        args.read_inflight,
        args.write_inflight,
    )?);

    let started = Instant::now();
    let entry = source.get_metadata(Path::new(&args.path)).await?;
    let bytes = entry.get_size();
    StorageEnum::copy_file(
        &source,
        &destination,
        &entry,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: true,
            ..Default::default()
        },
    )
    .await?;
    if durable {
        tokio::fs::OpenOptions::new()
            .read(true)
            .open(args.destination.join(&args.path))
            .await?
            .sync_data()
            .await?;
    }
    Ok((bytes, started.elapsed().as_nanos()))
}

async fn optimized_copy(args: &Args) -> Result<(u64, u128), Box<dyn std::error::Error>> {
    let read_concurrency = non_zero(args.read_inflight, "read inflight")?;
    let write_concurrency = non_zero(args.write_inflight, "write inflight")?;
    let source = connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: args.source.clone(),
        identity: BackendIdentity::new(BackendKind::Local, "local-comparison-source")?,
        read_concurrency,
        write_concurrency,
    }))
    .await?;
    let destination = connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: args.destination.clone(),
        identity: BackendIdentity::new(BackendKind::Local, "local-comparison-destination")?,
        read_concurrency,
        write_concurrency,
    }))
    .await?;
    let path = StoragePath::new(args.path.clone())?;
    let inflight = args.read_inflight.max(args.write_inflight);
    let inflight_bytes = args
        .chunk_bytes
        .checked_mul(inflight)
        .ok_or("inflight byte budget overflowed")?;
    let request = TransferRequest::new(
        TransferIdentity::new(format!(
            "local-comparison-{}-{}",
            std::process::id(),
            args.path
        ))?,
        source,
        path.clone(),
        destination,
        path,
        InflightLimits::new(inflight, inflight_bytes, inflight)?,
        CancellationToken::new(),
    );

    let started = Instant::now();
    let outcome = transfer(request).await?;
    Ok((outcome.transferred_bytes, started.elapsed().as_nanos()))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.chunk_bytes == 0 {
        return Err("chunk bytes must be greater than zero".into());
    }
    tokio::fs::create_dir_all(&args.destination).await?;
    let (bytes, elapsed_ns) = match args.implementation {
        Implementation::Legacy => legacy_copy(&args, false).await?,
        Implementation::LegacyDurable => legacy_copy(&args, true).await?,
        Implementation::Optimized => optimized_copy(&args).await?,
    };
    println!("bytes={bytes}\telapsed_ns={elapsed_ns}");
    Ok(())
}
