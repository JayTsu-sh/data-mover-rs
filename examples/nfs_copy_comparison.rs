use std::path::Path;
use std::time::Instant;

use clap::{Parser, Subcommand, ValueEnum};
use data_mover::model::StoragePath;
use data_mover::storage::{BackendConfig, NfsBackendConfig, connect_backend};
use data_mover::transfer::TransferPolicy;
use data_mover::transfer::{InflightLimits, TransferIdentity, TransferRequest, transfer};
use data_mover::{
    CopyOptions, CreateStorageOptions, StorageEnum, TransferConcurrency, create_storage,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Implementation {
    /// The pre-architecture NFS path routed through `StorageEnum`.
    Legacy,
    /// The role-based NFS backend routed through the unified transfer lifecycle.
    Optimized,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum TransferMode {
    Checkpointed,
    AtomicReplace,
}

#[derive(Debug, Parser)]
#[command(about = "Compare legacy and role-based NFS-to-NFS copy implementations")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Copy {
        #[arg(long, value_enum)]
        implementation: Implementation,
        #[arg(long)]
        source: String,
        #[arg(long)]
        destination: String,
        #[arg(long)]
        path: String,
        #[arg(long, default_value_t = 2 * 1024 * 1024)]
        chunk_bytes: usize,
        #[arg(long, default_value_t = 8)]
        read_inflight: usize,
        #[arg(long, default_value_t = 8)]
        write_inflight: usize,
        #[arg(long, value_enum, default_value_t = TransferMode::Checkpointed)]
        transfer_policy: TransferMode,
    },
    /// Removes every entry below one isolated NFS benchmark root.
    Cleanup {
        #[arg(long)]
        url: String,
    },
}

struct CopyArgs {
    source: String,
    destination: String,
    path: String,
    chunk_bytes: usize,
    read_inflight: usize,
    write_inflight: usize,
    transfer_policy: TransferMode,
}

fn legacy_options(
    args: &CopyArgs,
    ensure_dir: bool,
) -> Result<CreateStorageOptions, Box<dyn std::error::Error>> {
    Ok(CreateStorageOptions::new(
        Some(u64::try_from(args.chunk_bytes)?),
        ensure_dir,
    ))
}

async fn legacy_copy(args: &CopyArgs) -> Result<(u64, u128), Box<dyn std::error::Error>> {
    let source = create_storage(&args.source, legacy_options(args, false)?)
        .await?
        .with_transfer_concurrency(TransferConcurrency::new(
            args.read_inflight,
            args.write_inflight,
        )?);
    let destination = create_storage(&args.destination, legacy_options(args, true)?)
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
    Ok((bytes, started.elapsed().as_nanos()))
}

async fn optimized_copy(args: &CopyArgs) -> Result<(u64, u128), Box<dyn std::error::Error>> {
    let source = connect_backend(BackendConfig::Nfs(NfsBackendConfig {
        url: args.source.clone(),
        block_size: Some(u64::try_from(args.chunk_bytes)?),
        ensure_dir: false,
    }))
    .await?;
    let destination = connect_backend(BackendConfig::Nfs(NfsBackendConfig {
        url: args.destination.clone(),
        block_size: Some(u64::try_from(args.chunk_bytes)?),
        ensure_dir: true,
    }))
    .await?;
    let path = StoragePath::new(args.path.clone())?;
    let inflight = args.read_inflight.max(args.write_inflight);
    let inflight_bytes = args
        .chunk_bytes
        .checked_mul(inflight)
        .ok_or("inflight byte budget overflowed")?;
    let request = TransferRequest::new(
        source,
        path.clone(),
        destination,
        path,
        InflightLimits::new(inflight, inflight_bytes, inflight)?,
        CancellationToken::new(),
    )
    .with_identity_override(TransferIdentity::from_label(format!(
        "nfs-comparison-{}-{}",
        std::process::id(),
        args.path
    ))?)
    .with_transfer_policy(match args.transfer_policy {
        TransferMode::Checkpointed => TransferPolicy::Checkpointed,
        TransferMode::AtomicReplace => TransferPolicy::AtomicReplace,
    });

    let started = Instant::now();
    let outcome = transfer(request).await?;
    Ok((outcome.transferred_bytes, started.elapsed().as_nanos()))
}

async fn cleanup(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let storage = create_storage(url, CreateStorageOptions::new(None, true)).await?;
    let StorageEnum::NFS(storage) = storage else {
        return Err("cleanup URL did not create an NFS storage".into());
    };
    storage.delete_dir_all(None).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match Args::parse().command {
        Command::Copy {
            implementation,
            source,
            destination,
            path,
            chunk_bytes,
            read_inflight,
            write_inflight,
            transfer_policy,
        } => {
            if chunk_bytes == 0 {
                return Err("chunk bytes must be greater than zero".into());
            }
            let args = CopyArgs {
                source,
                destination,
                path,
                chunk_bytes,
                read_inflight,
                write_inflight,
                transfer_policy,
            };
            let (bytes, elapsed_ns) = match implementation {
                Implementation::Legacy => legacy_copy(&args).await?,
                Implementation::Optimized => optimized_copy(&args).await?,
            };
            println!("bytes={bytes}\telapsed_ns={elapsed_ns}");
        }
        Command::Cleanup { url } => cleanup(&url).await?,
    }
    Ok(())
}
