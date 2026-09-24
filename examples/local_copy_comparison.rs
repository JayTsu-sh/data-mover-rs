use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::{Parser, ValueEnum};
use data_mover::model::StoragePath;
use data_mover::storage::{BackendConfig, LocalBackendConfig, connect_backend};
use data_mover::transfer::{
    InflightLimits, ReadBackVerification, TransferIdentity, TransferPolicy, TransferRequest,
    transfer,
};
use data_mover::{
    CopyOptions, CreateStorageOptions, StorageEnum, TransferConcurrency, create_storage,
};
use futures::{StreamExt, TryStreamExt, stream};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Implementation {
    /// The pre-architecture `LocalStorage` path routed through `StorageEnum`.
    Legacy,
    /// The legacy public path plus an explicit final data sync (small copies already sync).
    LegacyDurable,
    /// The role-based Local backend routed through the unified transfer lifecycle.
    Optimized,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum TransferMode {
    Checkpointed,
    AtomicReplace,
    Direct,
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
    /// Copy path/0.bin through path/(file_count-1).bin when greater than one.
    #[arg(long, default_value_t = 1)]
    file_count: usize,
    #[arg(long, default_value_t = 1)]
    file_concurrency: usize,
    /// `AtomicReplace` copy without recovery checkpoints or Local final durability barriers.
    #[arg(long)]
    atomic_replace: bool,
    /// `Checkpointed` retains final durability barriers; other policies omit them for Local.
    #[arg(long, value_enum, conflicts_with = "atomic_replace")]
    transfer_policy: Option<TransferMode>,
    /// Omit destination read-back and content hashing when no other policy needs a digest.
    #[arg(long)]
    no_read_back: bool,
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
    let bytes = stream::iter(0..args.file_count)
        .map(|index| {
            let source = &source;
            let destination = &destination;
            async move {
                let path = copy_path(args, index);
                let entry = source.get_metadata(Path::new(&path)).await?;
                let bytes = entry.get_size();
                StorageEnum::copy_file(
                    source,
                    destination,
                    &entry,
                    CopyOptions {
                        enable_integrity_check: !args.no_read_back,
                        is_source_reserved: true,
                        ..Default::default()
                    },
                )
                .await?;
                if durable {
                    tokio::fs::OpenOptions::new()
                        .read(true)
                        .open(args.destination.join(&path))
                        .await?
                        .sync_data()
                        .await?;
                }
                Ok::<u64, Box<dyn std::error::Error>>(bytes)
            }
        })
        .buffer_unordered(args.file_concurrency)
        .try_fold(0_u64, |total, bytes| async move { Ok(total + bytes) })
        .await?;
    Ok((bytes, started.elapsed().as_nanos()))
}

async fn optimized_copy(args: &Args) -> Result<(u64, u128), Box<dyn std::error::Error>> {
    let read_concurrency = non_zero(args.read_inflight, "read inflight")?;
    let write_concurrency = non_zero(args.write_inflight, "write inflight")?;
    let source = connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: args.source.clone(),
        read_concurrency,
        write_concurrency,
    }))
    .await?;
    let destination = connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: args.destination.clone(),
        read_concurrency,
        write_concurrency,
    }))
    .await?;
    let inflight = args.read_inflight.max(args.write_inflight);
    let inflight_bytes = args
        .chunk_bytes
        .checked_mul(inflight)
        .ok_or("inflight byte budget overflowed")?;
    let started = Instant::now();
    let bytes = stream::iter(0..args.file_count)
        .map(|index| {
            let source = source.clone();
            let destination = destination.clone();
            async move {
                let path = StoragePath::new(copy_path(args, index))?;
                let request = TransferRequest::new(
                    TransferIdentity::new(format!(
                        "local-comparison-{}-{index}",
                        std::process::id(),
                    ))?,
                    source,
                    path.clone(),
                    destination,
                    path,
                    InflightLimits::new(inflight, inflight_bytes, inflight)?,
                    CancellationToken::new(),
                );
                let request = if args.atomic_replace {
                    request.with_transfer_policy(TransferPolicy::AtomicReplace)
                } else {
                    request.with_transfer_policy(
                        match args.transfer_policy.unwrap_or(TransferMode::Checkpointed) {
                            TransferMode::Checkpointed => TransferPolicy::Checkpointed,
                            TransferMode::AtomicReplace => TransferPolicy::AtomicReplace,
                            TransferMode::Direct => TransferPolicy::Direct,
                        },
                    )
                };
                let request = if args.no_read_back {
                    request.with_read_back_verification(ReadBackVerification::Disabled)
                } else {
                    request
                };
                let outcome = transfer(request).await?;
                Ok::<u64, Box<dyn std::error::Error>>(outcome.transferred_bytes)
            }
        })
        .buffer_unordered(args.file_concurrency)
        .try_fold(0_u64, |total, bytes| async move { Ok(total + bytes) })
        .await?;
    Ok((bytes, started.elapsed().as_nanos()))
}

fn copy_path(args: &Args, index: usize) -> String {
    if args.file_count == 1 {
        args.path.clone()
    } else {
        format!("{}/{index}.bin", args.path)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    non_zero(args.file_count, "file count")?;
    non_zero(args.file_concurrency, "file concurrency")?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_policy(extra: &[&str]) -> Result<Args, clap::Error> {
        Args::try_parse_from(
            [
                "local_copy_comparison",
                "--implementation",
                "optimized",
                "--source",
                "/source",
                "--destination",
                "/destination",
                "--path",
                "file.bin",
            ]
            .into_iter()
            .chain(extra.iter().copied()),
        )
    }

    #[test]
    fn accepts_checkpointed_and_atomic_replace_but_rejects_removed_policies() {
        assert!(parse_policy(&["--transfer-policy", "checkpointed"]).is_ok());
        assert!(parse_policy(&["--transfer-policy", "atomic-replace"]).is_ok());
        assert!(parse_policy(&["--transfer-policy", "direct"]).is_ok());
        assert!(parse_policy(&["--atomic-replace"]).is_ok());
        assert!(parse_policy(&["--transfer-policy", "auto"]).is_err());
        assert!(parse_policy(&["--transfer-policy", "quick"]).is_err());
        assert!(parse_policy(&["--quick"]).is_err());
        assert!(parse_policy(&["--transfer-policy", "restart-from-zero"]).is_err());
        assert!(parse_policy(&["--restart-from-zero"]).is_err());
        assert!(parse_policy(&["--atomic-replace", "--transfer-policy", "checkpointed"]).is_err());
    }
}
