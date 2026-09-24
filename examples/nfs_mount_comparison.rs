//! Compare mounted Local and user-space NFS using the same ordinary transfer entry.
use std::num::NonZeroUsize;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use data_mover::TransferConcurrency;
use data_mover::model::{BackendKind, StoragePath};
use data_mover::storage::{BackendConfig, LocalBackendConfig, NfsBackendConfig, connect_backend};
use data_mover::transfer::{
    InflightLimits, ReadBackVerification, TransferIdentity, TransferPolicy, TransferRequest,
    transfer,
};
use futures::{StreamExt, TryStreamExt, stream};
use tokio_util::sync::CancellationToken;

type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Transport {
    Mounted,
    Client,
}
#[derive(Clone, Copy, Debug, ValueEnum)]
enum Policy {
    Checkpointed,
    AtomicReplace,
    Direct,
}

#[derive(Parser)]
struct Args {
    #[arg(long, value_enum)]
    transport: Transport,
    /// Override the source independently to exercise mixed-backend transfers.
    #[arg(long, value_enum)]
    source_transport: Option<Transport>,
    #[arg(long, value_enum)]
    policy: Policy,
    #[arg(long)]
    source: String,
    #[arg(long)]
    destination: String,
    #[arg(long)]
    path: String,
    #[arg(long, default_value_t = 1)]
    files: usize,
    #[arg(long, default_value_t = 1)]
    concurrency: usize,
    // Equalize the requested stream chunk with the measured NFS protocol maximum.
    #[arg(long, default_value_t = 1_044_480)]
    chunk_bytes: usize,
    #[arg(long)]
    read_back: bool,
}

async fn endpoint(
    args: &Args,
    transport: Transport,
    root: &str,
    side: &str,
    concurrency: TransferConcurrency,
) -> Result<data_mover::storage::Storage, Error> {
    let config = match transport {
        Transport::Mounted => BackendConfig::Local(LocalBackendConfig {
            root: root.into(),
            read_concurrency: NonZeroUsize::new(concurrency.read()).ok_or("invalid concurrency")?,
            write_concurrency: NonZeroUsize::new(concurrency.write())
                .ok_or("invalid concurrency")?,
        }),
        Transport::Client => BackendConfig::Nfs(NfsBackendConfig {
            url: root.into(),
            block_size: Some(u64::try_from(args.chunk_bytes)?),
            ensure_dir: side == "destination",
        }),
    };
    Ok(connect_backend(config).await?)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    if args.files == 0 || args.concurrency == 0 {
        return Err("files and concurrency must be positive".into());
    }
    let source_transport = args.source_transport.unwrap_or(args.transport);
    let source_concurrency = concurrency(source_transport)?;
    let destination_concurrency = concurrency(args.transport)?;
    let concurrency =
        TransferConcurrency::new(source_concurrency.read(), destination_concurrency.write())?;
    let source = endpoint(
        &args,
        source_transport,
        &args.source,
        "source",
        source_concurrency,
    )
    .await?;
    let destination = endpoint(
        &args,
        args.transport,
        &args.destination,
        "destination",
        destination_concurrency,
    )
    .await?;
    let slots = concurrency.read().max(concurrency.write());
    let inflight = InflightLimits::new(
        slots,
        args.chunk_bytes
            .checked_mul(concurrency.read())
            .ok_or("byte budget overflow")?,
        concurrency.read(),
    )?;
    let policy = match args.policy {
        Policy::Checkpointed => TransferPolicy::Checkpointed,
        Policy::AtomicReplace => TransferPolicy::AtomicReplace,
        Policy::Direct => TransferPolicy::Direct,
    };
    let started = Instant::now();
    let bytes = stream::iter(0..args.files)
        .map(|index| {
            let source = source.clone();
            let destination = destination.clone();
            let args = &args;
            async move {
                let path = StoragePath::new(if args.files == 1 {
                    args.path.clone()
                } else {
                    format!("{}/{index}.bin", args.path)
                })?;
                let request = TransferRequest::new(
                    source,
                    path.clone(),
                    destination,
                    path,
                    inflight,
                    CancellationToken::new(),
                )
                .with_identity_override(TransferIdentity::from_label(format!(
                    "mount-compare-{}-{index}",
                    std::process::id()
                ))?)
                .with_transfer_policy(policy)
                .with_read_back_verification(if args.read_back {
                    ReadBackVerification::Enabled
                } else {
                    ReadBackVerification::Disabled
                });
                Ok::<_, Error>(transfer(request).await?.transferred_bytes)
            }
        })
        .buffer_unordered(args.concurrency)
        .try_fold(0_u64, |total, n| async move { Ok(total + n) })
        .await?;
    println!(
        "bytes={bytes} elapsed_ns={} read_inflight={} write_inflight={} chunk_bytes={}",
        started.elapsed().as_nanos(),
        concurrency.read(),
        concurrency.write(),
        args.chunk_bytes
    );
    Ok(())
}

fn concurrency(transport: Transport) -> Result<TransferConcurrency, Error> {
    let backend = match transport {
        Transport::Mounted => BackendKind::Local,
        Transport::Client => BackendKind::Nfs,
    };
    Ok(TransferConcurrency::from_env(
        backend,
        TransferConcurrency::new(8, 8)?,
    )?)
}
