//! Real Local reads and public transfers; JSON measurements for run-local-qos.py.
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
use data_mover::transfer::{TransferRequest, transfer};
use futures::StreamExt as _;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser)]
struct Args {
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

async fn qos_group(args: &Args) -> Result<Option<SourceQosGroup>> {
    let peak = Duration::from_millis(args.peak_ms);
    let group = if args.soft_bytes.is_some() || args.soft_iops.is_some() {
        Some(SourceQosGroup::new(SourceQosPolicy::new(
            args.soft_bytes
                .zip(args.hard_bytes)
                .map(|(s, h)| (s, h, peak)),
            args.chunk_bytes.get() as u64,
            args.soft_iops
                .zip(args.hard_iops)
                .map(|(s, h)| (s, h, peak)),
        )?))
    } else {
        None
    };
    if let Some(group) = &group {
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

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<()> {
    let args = Args::parse();
    let path = StoragePath::new(&args.path)?;
    // Independent full-file hash outside measurement also warms all cases equally.
    let (expected_bytes, expected_digest) = digest_file(&args.source.join(&args.path))?;
    let source = connect(
        &args.source,
        args.chunk_bytes,
        if args.copy { 8 } else { 1 },
    )
    .await?;
    let destination = connect(&args.destination, args.chunk_bytes, 8).await?;
    let group = qos_group(&args).await?;
    tokio::time::sleep(Duration::from_millis(args.idle_ms)).await;
    let mut events = Vec::new();
    let bytes;
    let operations;
    let digest;
    let seconds;
    let started;
    if args.copy {
        let mut request = TransferRequest::new(source, path, destination, CancellationToken::new());
        let shaped = group.is_some();
        if let Some(group) = &group {
            request = request.with_source_qos(group.clone());
        }
        started = Instant::now();
        let outcome = transfer(request).await?;
        seconds = started.elapsed().as_secs_f64();
        bytes = outcome.transferred_bytes;
        operations = if shaped {
            if outcome.source_qos.client_streamed_shaped_bytes != bytes {
                return Err("source QoS payload accounting mismatch".into());
            }
            outcome.source_qos.source_read_operations
        } else {
            // Local has fixed-sized full reads. No-QoS transfers expose no read counter.
            bytes.div_ceil(args.chunk_bytes.get() as u64)
        };
        let (destination_bytes, destination_digest) =
            digest_file(&args.destination.join(&args.path))?;
        if destination_bytes != bytes {
            return Err("destination length mismatch".into());
        }
        digest = destination_digest;
    } else {
        let budget = group.as_ref().map(SourceQosGroup::transfer_budget);
        started = Instant::now();
        let mut stream = source
            .read_source()?
            .read(ReadRequest {
                path,
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
    };
    println!("{}", serde_json::to_string(&measurement)?);
    Ok(())
}
