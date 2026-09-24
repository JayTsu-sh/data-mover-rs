//! Copies one file through the role-based transfer engine to or from S3 and prints one line of
//! `key=value` facts, so a script can compare write strategies against a real server. See
//! `.claude/skills/e2e-s3/scripts/staged_matrix.sh`, which drives it.
//!
//! ```text
//! cargo run --example s3_staged_copy -- \
//!     --source /tmp/dm-s3 --source-path f200m --seed-bytes 209715200 \
//!     --destination s3:run1 --destination-path f200m --policy checkpointed
//! ```
//!
//! `s3:<prefix>` is that prefix of the bucket named by `S3_HOST` / `S3_BUCKET` / `S3_AK` /
//! `S3_SK` / `S3_USE_HTTPS` (the e2e-s3 `.env`); anything else is a local directory. A second run
//! with the same `--identity` resumes a checkpointed transfer the first run left behind
//! (recovery records live under `DATA_MOVER_RECOVERY_DIR`).

use std::env;
use std::error::Error;
use std::fs;
use std::io::Write as _;
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use data_mover::model::{BackendIdentity, BackendKind, StoragePath};
use data_mover::storage::{
    BackendConfig, LocalBackendConfig, S3BackendConfig, Storage, connect_backend,
};
use data_mover::transfer::{
    InflightLimits, ReadBackVerification, SourceQosGroup, SourceQosPolicy, TransferIdentity,
    TransferPolicy, TransferRequest, transfer,
};
use tokio_util::sync::CancellationToken;

type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;

#[derive(Debug, Parser)]
#[command(about = "Copy one file to or from S3 through the transfer engine and report the outcome")]
struct Args {
    #[arg(long)]
    source: String,
    #[arg(long)]
    source_path: String,
    /// Create the local source file with this many deterministic bytes if it does not exist yet;
    /// an existing file of another size is refused, so a resume keeps the source identity.
    #[arg(long)]
    seed_bytes: Option<u64>,
    #[arg(long)]
    destination: String,
    #[arg(long)]
    destination_path: String,
    #[arg(long, value_enum, default_value_t = Policy::Checkpointed)]
    policy: Policy,
    #[arg(long, value_enum, default_value_t = OnOff::On)]
    read_back: OnOff,
    /// Transfer identity; reuse it to resume what an interrupted run left behind.
    #[arg(long, default_value = "s3-staged-copy")]
    identity: String,
    /// Hard limit on source read bandwidth, so an interruption lands mid-transfer.
    #[arg(long)]
    bandwidth: Option<u64>,
    /// Cancel the transfer after this many milliseconds.
    #[arg(long)]
    cancel_after_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Policy {
    Checkpointed,
    Atomic,
    Direct,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum OnOff {
    On,
    Off,
}

fn env_var(name: &str) -> Result<String> {
    env::var(name).map_err(|_| format!("{name} is not set (source the e2e-s3 .env)").into())
}

/// `s3://AK:SK@bucket.host/prefix` — the crate takes the key and secret as written, undecoded.
fn s3_url(prefix: &str) -> Result<String> {
    let scheme = if env::var("S3_USE_HTTPS").is_ok_and(|value| value == "true") {
        "s3+https"
    } else {
        "s3"
    };
    Ok(format!(
        "{scheme}://{}:{}@{}.{}/{}",
        env_var("S3_AK")?,
        env_var("S3_SK")?,
        env_var("S3_BUCKET")?,
        env_var("S3_HOST")?,
        prefix.trim_start_matches('/')
    ))
}

async fn connect(endpoint: &str, side: &str) -> Result<Storage> {
    let config = if endpoint.starts_with("s3://") {
        return Err("give `s3:<prefix>` with the e2e-s3 .env, not an s3:// URL".into());
    } else if let Some(prefix) = endpoint.strip_prefix("s3:") {
        BackendConfig::S3(S3BackendConfig {
            url: s3_url(prefix)?,
            identity: BackendIdentity::new(BackendKind::S3, side)?,
            block_size: None,
        })
    } else {
        fs::create_dir_all(endpoint)?;
        let slots = NonZeroUsize::new(4).ok_or("non-zero")?;
        BackendConfig::Local(LocalBackendConfig {
            root: endpoint.into(),
            identity: BackendIdentity::new(BackendKind::Local, side)?,
            read_concurrency: slots,
            write_concurrency: slots,
        })
    };
    Ok(connect_backend(config).await?)
}

/// Deterministic bytes, written once: a resume must see the same file (and inode) it started on.
fn seed(root: &str, path: &str, bytes: u64) -> Result {
    let file = Path::new(root).join(path);
    if let Ok(existing) = fs::metadata(&file) {
        if existing.len() == bytes {
            return Ok(());
        }
        return Err(format!(
            "{} exists with {} bytes, not {bytes}",
            file.display(),
            existing.len()
        )
        .into());
    }
    if let Some(parent) = file.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut out = fs::File::create(&file)?;
    let mut reader = blake3::Hasher::new().update(path.as_bytes()).finalize_xof();
    let mut block = vec![0_u8; 1 << 20];
    let mut left = bytes;
    while left > 0 {
        let take = usize::try_from(left.min(block.len() as u64))?;
        reader.fill(&mut block[..take]);
        out.write_all(&block[..take])?;
        left -= take as u64;
    }
    out.sync_all()?;
    Ok(())
}

fn request(
    args: &Args,
    source: Storage,
    destination: Storage,
    cancel: CancellationToken,
) -> Result<TransferRequest> {
    let mut request = TransferRequest::new(
        TransferIdentity::new(args.identity.clone())?,
        source,
        StoragePath::new(args.source_path.as_str())?,
        destination,
        StoragePath::new(args.destination_path.as_str())?,
        InflightLimits::new(8, 64 << 20, 8)?,
        cancel,
    )
    .with_transfer_policy(match args.policy {
        Policy::Checkpointed => TransferPolicy::Checkpointed,
        Policy::Atomic => TransferPolicy::AtomicReplace,
        Policy::Direct => TransferPolicy::Direct,
    })
    .with_read_back_verification(match args.read_back {
        OnOff::On => ReadBackVerification::Enabled,
        OnOff::Off => ReadBackVerification::Disabled,
    });
    if let Some(rate) = args.bandwidth {
        let policy = SourceQosPolicy::new(Some((rate, rate, Duration::ZERO)), 8 << 20, None)?;
        request = request.with_source_qos(SourceQosGroup::new(policy));
    }
    Ok(request)
}

#[tokio::main]
async fn main() -> Result {
    let args = Args::parse();
    if let Some(bytes) = args.seed_bytes {
        seed(&args.source, &args.source_path, bytes)?;
    }
    let source = connect(&args.source, "source").await?;
    let destination = connect(&args.destination, "destination").await?;
    let cancel = CancellationToken::new();
    if let Some(ms) = args.cancel_after_ms {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            cancel.cancel();
        });
    }
    let started = Instant::now();
    let result = transfer(request(&args, source, destination, cancel)?).await;
    let elapsed = started.elapsed().as_millis();
    match result {
        Ok(outcome) => println!(
            "result=ok elapsed_ms={elapsed} bytes={} route={:?} recovery={:?} read_back={:?}",
            outcome.transferred_bytes, outcome.route, outcome.recovery, outcome.read_back
        ),
        Err(failure) => {
            println!("result=failed elapsed_ms={elapsed} failure=\"{failure}\"");
            if env::var_os("S3_STAGED_COPY_DEBUG").is_some() {
                eprintln!("{failure:#?}");
            }
            std::process::exit(1);
        }
    }
    Ok(())
}
