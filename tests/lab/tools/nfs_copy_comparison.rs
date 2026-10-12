use std::num::NonZeroUsize;
use std::time::Instant;

use clap::{Parser, Subcommand, ValueEnum};
use data_mover::model::StoragePath;
use data_mover::storage::{
    BackendConfig, DeleteTreeItem, DeleteTreeOutcome, DeleteTreeRequest, NfsBackendConfig,
    connect_backend, delete_tree,
};
use data_mover::transfer::TransferPolicy;
use data_mover::transfer::{InflightLimits, TransferIdentity, TransferRequest, transfer};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Implementation {
    /// The role-based NFS backend routed through the unified transfer lifecycle.
    Optimized,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum TransferMode {
    Checkpointed,
    AtomicReplace,
}

#[derive(Debug, Parser)]
#[command(about = "Measure role-based NFS-to-NFS transfer policies")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Copy {
        #[arg(long, value_enum, default_value_t = Implementation::Optimized)]
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
    let storage = connect_backend(BackendConfig::Nfs(NfsBackendConfig {
        url: url.to_owned(),
        block_size: None,
        ensure_dir: false,
    }))
    .await?;
    let slots = NonZeroUsize::new(4).ok_or("nonzero")?;
    let mut session = delete_tree(
        &storage,
        DeleteTreeRequest {
            root: StoragePath::root(),
            delete_root: false,
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
        return Err("cleanup did not complete without failures".into());
    }
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
                Implementation::Optimized => optimized_copy(&args).await?,
            };
            println!("bytes={bytes}\telapsed_ns={elapsed_ns}");
        }
        Command::Cleanup { url } => cleanup(&url).await?,
    }
    Ok(())
}
