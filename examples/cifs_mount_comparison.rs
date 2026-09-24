//! Compare mounted Local and user-space CIFS using the same ordinary transfer entry.
use std::num::NonZeroUsize;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use data_mover::TransferConcurrency;
use data_mover::model::{BackendKind, StoragePath};
use data_mover::storage::{
    BackendConfig, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy, LocalBackendConfig,
    connect_backend,
};
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

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Signing {
    Required,
    WhenRequired,
}

#[derive(Parser)]
struct Args {
    #[arg(long, value_enum)]
    transport: Transport,
    /// Override only the source for mixed-path diagnostic runs.
    #[arg(long, value_enum)]
    source_transport: Option<Transport>,
    #[arg(long, value_enum, default_value = "when-required")]
    signing: Signing,
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
    // Payload budget is separate from the protocol-negotiated request limits.
    #[arg(long, default_value_t = 2 * 1024 * 1024)]
    chunk_bytes: usize,
    /// Fix the payload byte budget independently of read concurrency.
    #[arg(long)]
    byte_budget: Option<usize>,
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
        Transport::Client => BackendConfig::Cifs(CifsBackendConfig {
            signing_policy: match args.signing {
                Signing::Required => CifsSigningPolicy::Required,
                Signing::WhenRequired => CifsSigningPolicy::WhenRequired,
            },
            guest_policy: CifsGuestPolicy::Deny,
            server: std::env::var(if side == "source" {
                "CIFS_REAL_SERVER"
            } else {
                "CIFS_REAL_SECOND_SERVER"
            })?,
            share: std::env::var("CIFS_REAL_SHARE")?,
            username: std::env::var("CIFS_REAL_USER")?,
            password: std::env::var("CIFS_REAL_PASS")?,
            root: Some(root.to_owned()),
            ensure_dir: side == "destination",
        }),
    };
    Ok(connect_backend(config).await?)
}

#[tokio::main(worker_threads = 8)]
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
        args.byte_budget.unwrap_or(
            args.chunk_bytes
                .checked_mul(concurrency.read())
                .ok_or("byte budget overflow")?,
        ),
        concurrency.read(),
    )?;
    let policy = match args.policy {
        Policy::Checkpointed => TransferPolicy::Checkpointed,
        Policy::AtomicReplace => TransferPolicy::AtomicReplace,
        Policy::Direct => TransferPolicy::Direct,
    };
    let start_unix_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
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
                    "cifs-mount-compare-{}-{index}",
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
        "bytes={bytes} elapsed_ns={} read_inflight={} write_inflight={} chunk_bytes={} start_unix_ns={start_unix_ns}",
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
        Transport::Client => BackendKind::Cifs,
    };
    Ok(TransferConcurrency::from_env(
        backend,
        TransferConcurrency::new(8, 8)?,
    )?)
}
