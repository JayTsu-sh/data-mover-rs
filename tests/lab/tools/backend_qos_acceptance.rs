//! Real source-backend acceptance through the public transfer lifecycle.
use std::io::Read as _;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::Parser;
use data_mover::model::{SourceVersion, StoragePath};
use data_mover::storage::{
    BackendConfig, LocalBackendConfig, ReadRequest, SourceQosGroup, SourceQosPolicy, Storage,
    connect_backend,
};
use data_mover::transfer::{ReadBackVerification, TransferPolicy, TransferRequest, transfer};
use futures::StreamExt as _;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

#[allow(dead_code)]
#[path = "../../../examples/support/endpoint.rs"]
mod endpoint_support;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser)]
struct Args {
    /// Seed the isolated source endpoint from the local golden fixture.
    #[arg(long)]
    seed: bool,
    /// Delete only this run's source file.
    #[arg(long)]
    cleanup: bool,
    #[arg(long)]
    source: PathBuf,
    #[arg(long)]
    destination: PathBuf,
    #[arg(long)]
    path: String,
    #[arg(long)]
    chunk_bytes: NonZeroUsize,
    /// Use the public transfer lifecycle, including its default verification/durability.
    #[arg(long)]
    copy: bool,
    /// Record exact grants for finite envelope checks; disable for production-cost timing.
    #[arg(long, default_value = "enabled")]
    admission_trace: TraceMode,
    /// Cross-backend payload timing: atomic publication; independent full read-back outside timing.
    #[arg(long, default_value = "default")]
    transfer_profile: TransferProfile,
    /// Delete the verified destination after measuring; supports remote endpoints too.
    #[arg(long, default_value = "keep")]
    destination_cleanup: DestinationCleanup,
    #[arg(long, requires = "hard_bytes")]
    soft_bytes: Option<u64>,
    #[arg(long, requires = "soft_bytes")]
    hard_bytes: Option<u64>,
    #[arg(long, requires = "hard_iops")]
    soft_iops: Option<u32>,
    #[arg(long, requires = "soft_iops")]
    hard_iops: Option<u32>,
    #[arg(long, default_value_t = 500)]
    peak_ms: u64,
    /// Accumulate soft credit before the measurement; never included in throughput.
    #[arg(long, default_value_t = 0)]
    idle_ms: u64,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum TraceMode {
    Enabled,
    Disabled,
}

#[derive(Clone, Copy, clap::ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
enum TransferProfile {
    Default,
    Cross,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum DestinationCleanup {
    Keep,
    Delete,
}

#[derive(Serialize)]
struct Event {
    seconds: f64,
    bytes: usize,
}

#[derive(Serialize)]
struct Measurement {
    bytes: u64,
    operations: u64,
    seconds: f64,
    mibps: f64,
    iops: f64,
    digest: String,
    events: Vec<Event>,
    admissions: Vec<Event>,
    effective_read_bytes: u64,
    verification_seconds: f64,
    transfer_profile: TransferProfile,
}

async fn connect(root: &Path, chunk: NonZeroUsize, concurrency: usize) -> Result<Storage> {
    let concurrency = NonZeroUsize::new(concurrency).ok_or("zero concurrency")?;
    Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
        rename_context: None,
        root: root.to_path_buf(),
        read_chunk_bytes: Some(chunk),
        write_chunk_bytes: None,
        read_concurrency: concurrency,
        write_concurrency: concurrency,
    }))
    .await?)
}

fn digest_file(path: &Path) -> Result<(u64, String)> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0; 1024 * 1024];
    let mut bytes = 0;
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
        bytes += count as u64;
    }
    Ok((bytes, hasher.finalize().to_hex().to_string()))
}

async fn qos_group(args: &Args, read_cap: u64) -> Result<Option<SourceQosGroup>> {
    let peak = Duration::from_millis(args.peak_ms);
    let group = if args.soft_bytes.is_some() || args.soft_iops.is_some() {
        Some(SourceQosGroup::new(SourceQosPolicy::new(
            args.soft_bytes
                .zip(args.hard_bytes)
                .map(|(s, h)| (s, h, peak)),
            read_cap,
            args.soft_iops
                .zip(args.hard_iops)
                .map(|(s, h)| (s, h, peak)),
        )?))
    } else {
        None
    };
    if let Some(group) = &group
        && matches!(args.admission_trace, TraceMode::Enabled)
    {
        group.enable_admission_trace().await;
    }
    Ok(group)
}

async fn admission_events(group: Option<&SourceQosGroup>, started: Instant) -> Vec<Event> {
    match group {
        Some(group) => group
            .take_admission_trace()
            .await
            .into_iter()
            .map(|(at, bytes)| Event {
                seconds: at
                    .into_std()
                    .saturating_duration_since(started)
                    .as_secs_f64(),
                bytes: usize::try_from(bytes).unwrap_or(usize::MAX),
            })
            .collect(),
        None => Vec::new(),
    }
}

async fn prepare_source(args: &Args, path: &StoragePath) -> Result<Storage> {
    let endpoint = std::env::var("QOS_SOURCE")?;
    let source = endpoint_support::connect_with_options(
        &endpoint,
        args.seed,
        Some(args.chunk_bytes.get() as u64),
    )
    .await?;
    if args.cleanup {
        source.namespace()?.delete(path).await?;
        println!("{{\"cleaned\":true}}");
        return Ok(source);
    }
    if args.seed {
        let local = connect(&args.source, args.chunk_bytes, 8).await?;
        transfer(
            TransferRequest::new(
                local,
                path.clone(),
                source.clone(),
                CancellationToken::new(),
            )
            .with_transfer_policy(data_mover::transfer::TransferPolicy::AtomicReplace),
        )
        .await?;
        println!("{{\"seeded\":true}}");
        return Ok(source);
    }
    source.read_source()?.describe(path).await?;
    Ok(source)
}

async fn connect_destination(args: &Args) -> Result<Storage> {
    if let Ok(endpoint) = std::env::var("QOS_DESTINATION") {
        Ok(endpoint_support::connect_with_options(
            &endpoint,
            true,
            Some(args.chunk_bytes.get() as u64),
        )
        .await?)
    } else {
        connect(&args.destination, args.chunk_bytes, 8).await
    }
}

async fn digest_storage(storage: &Storage, path: &StoragePath) -> Result<(u64, String)> {
    let mut stream = storage
        .read_source()?
        .read(ReadRequest {
            path: path.clone(),
            range: None,
            expected_source: None,
            maximum_chunk_bytes: 2 * 1024 * 1024,
            read_inflight: 4,
            read_budget: None,
            cancel: CancellationToken::new(),
            source_qos: None,
            version: SourceVersion::Current,
        })
        .await?;
    let mut hasher = blake3::Hasher::new();
    let mut bytes = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        bytes += chunk.len() as u64;
        hasher.update(&chunk);
    }
    Ok((bytes, hasher.finalize().to_hex().to_string()))
}

struct CopyMeasurement {
    bytes: u64,
    operations: u64,
    seconds: f64,
    digest: String,
    started: Instant,
    verification_seconds: f64,
}

async fn copy_measured(
    args: &Args,
    source: Storage,
    destination: Storage,
    path: StoragePath,
    group: Option<&SourceQosGroup>,
    read_cap: u64,
) -> Result<CopyMeasurement> {
    let mut request = TransferRequest::new(
        source,
        path.clone(),
        destination.clone(),
        CancellationToken::new(),
    );
    if matches!(args.transfer_profile, TransferProfile::Cross) {
        request = request
            .with_transfer_policy(TransferPolicy::AtomicReplace)
            .with_read_back_verification(ReadBackVerification::Disabled);
    }
    let shaped = group.is_some();
    if let Some(group) = group {
        request = request.with_source_qos(group.clone());
    }
    let started = Instant::now();
    let outcome = transfer(request).await?;
    let seconds = started.elapsed().as_secs_f64();
    let bytes = outcome.transferred_bytes;
    let operations = if shaped {
        if outcome.source_qos.client_streamed_shaped_bytes != bytes {
            return Err("source QoS payload accounting mismatch".into());
        }
        outcome.source_qos.source_read_operations
    } else {
        // Local has fixed-sized full reads. No-QoS transfers expose no read counter.
        bytes.div_ceil(read_cap)
    };
    let verify_started = Instant::now();
    let (destination_bytes, digest) = digest_storage(&destination, &path).await?;
    let verification_seconds = verify_started.elapsed().as_secs_f64();
    if destination_bytes != bytes {
        return Err("destination length mismatch".into());
    }
    Ok(CopyMeasurement {
        bytes,
        operations,
        seconds,
        digest,
        started,
        verification_seconds,
    })
}

fn init_tracing() {
    if std::env::var_os("RUST_LOG").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .with_ansi(false)
            .try_init();
    }
}

fn expected_fixture(args: &Args) -> Result<(u64, String)> {
    if args.cleanup {
        Ok((0, String::new()))
    } else {
        digest_file(&args.source.join(&args.path))
    }
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<()> {
    let args = Args::parse();
    init_tracing();
    let path = StoragePath::new(&args.path)?;
    // Independent full-file hash outside measurement also warms all cases equally.
    let (expected_bytes, expected_digest) = expected_fixture(&args)?;
    let source = prepare_source(&args, &path).await?;
    if args.seed || args.cleanup {
        return Ok(());
    }
    let read_cap = source.read_source()?.maximum_read_chunk_bytes() as u64;
    let destination = connect_destination(&args).await?;
    let group = qos_group(&args, read_cap).await?;
    tokio::time::sleep(Duration::from_millis(args.idle_ms)).await;
    let mut events = Vec::new();
    let bytes;
    let operations;
    let digest;
    let seconds;
    let started;
    let mut verification_seconds = 0.0;
    if args.copy {
        let copied = copy_measured(
            &args,
            source,
            destination.clone(),
            path.clone(),
            group.as_ref(),
            read_cap,
        )
        .await?;
        bytes = copied.bytes;
        operations = copied.operations;
        seconds = copied.seconds;
        digest = copied.digest;
        started = copied.started;
        verification_seconds = copied.verification_seconds;
    } else {
        let budget = group.as_ref().map(SourceQosGroup::transfer_budget);
        started = Instant::now();
        let mut stream = source
            .read_source()?
            .read(ReadRequest {
                path: path.clone(),
                range: None,
                expected_source: None,
                maximum_chunk_bytes: args.chunk_bytes.get(),
                read_inflight: 1,
                read_budget: None,
                cancel: CancellationToken::new(),
                source_qos: budget.clone(),
                version: SourceVersion::Current,
            })
            .await?;
        let mut hasher = blake3::Hasher::new();
        let mut total = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            events.push(Event {
                seconds: started.elapsed().as_secs_f64(),
                bytes: chunk.len(),
            });
            hasher.update(&chunk);
            total += chunk.len() as u64;
        }
        seconds = started.elapsed().as_secs_f64();
        bytes = total;
        operations = events.len() as u64;
        digest = hasher.finalize().to_hex().to_string();
        if let Some(budget) = budget {
            let stats = budget.stats();
            if stats.client_streamed_shaped_bytes != bytes
                || stats.source_read_operations != operations
            {
                return Err("real-read event count differs from source QoS counters".into());
            }
        }
    }
    if bytes != expected_bytes || digest != expected_digest {
        return Err("full-content verification failed".into());
    }
    if args.copy && matches!(args.destination_cleanup, DestinationCleanup::Delete) {
        destination.namespace()?.delete(&path).await?;
    }
    #[allow(clippy::cast_precision_loss)]
    let measurement = Measurement {
        bytes,
        operations,
        seconds,
        mibps: bytes as f64 / seconds / 1_048_576.0,
        iops: operations as f64 / seconds,
        digest,
        events,
        admissions: admission_events(group.as_ref(), started).await,
        effective_read_bytes: read_cap,
        verification_seconds,
        transfer_profile: args.transfer_profile,
    };
    println!("{}", serde_json::to_string(&measurement)?);
    Ok(())
}
