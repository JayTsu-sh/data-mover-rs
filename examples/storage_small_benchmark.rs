use std::time::Instant;

use clap::{Parser, ValueEnum};
use data_mover::{
    CopyOptions, Result, StorageEntryMessage, StorageEnum, WalkOptions, create_storage,
};

struct Measurement {
    entries: u64,
    p95_scheduling_latency_ms: f64,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Operation {
    Scan,
    Copy,
}

#[derive(Debug, Parser)]
#[command(about = "Measure small-entry scan or copy at data-mover's public storage seam")]
struct Args {
    #[arg(long, value_enum)]
    operation: Operation,
    #[arg(long)]
    source: String,
    #[arg(long, required_if_eq("operation", "copy"))]
    destination: Option<String>,
    #[arg(long)]
    expected_entries: u64,
    #[arg(long, default_value_t = 2 * 1024 * 1024)]
    chunk_bytes: u64,
}

fn options(block_size: u64, ensure_dir: bool) -> data_mover::CreateStorageOptions {
    data_mover::CreateStorageOptions {
        block_size: Some(block_size),
        ensure_dir,
        ..Default::default()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let source = create_storage(&args.source, options(args.chunk_bytes, false)).await?;
    let started = Instant::now();
    let measurement = match args.operation {
        Operation::Scan => scan(&source).await?,
        Operation::Copy => {
            let destination_url = args.destination.as_deref().ok_or_else(|| {
                data_mover::error::StorageError::ConfigError(
                    "copy benchmark requires --destination".to_string(),
                )
            })?;
            let destination =
                create_storage(destination_url, options(args.chunk_bytes, true)).await?;
            copy(&source, &destination).await?
        }
    };
    if measurement.entries != args.expected_entries {
        return Err(data_mover::error::StorageError::OperationError(format!(
            "entry count mismatch: expected {}, got {}",
            args.expected_entries, measurement.entries
        )));
    }
    println!(
        "entries={}\telapsed_ms={:.3}\tp95_scheduling_latency_ms={:.3}",
        measurement.entries,
        started.elapsed().as_secs_f64() * 1000.0,
        measurement.p95_scheduling_latency_ms
    );
    Ok(())
}

fn measurement(entries: u64, mut latencies_ms: Vec<f64>) -> Measurement {
    latencies_ms.sort_by(f64::total_cmp);
    let index = (95 * latencies_ms.len()).div_ceil(100).saturating_sub(1);
    Measurement {
        entries,
        p95_scheduling_latency_ms: latencies_ms.get(index).copied().unwrap_or(0.0),
    }
}

async fn scan(source: &StorageEnum) -> Result<Measurement> {
    let receiver = source.walkdir(None, WalkOptions::default()).await?;
    let mut entries = 0;
    let mut latencies_ms = Vec::new();
    let mut previous = Instant::now();
    while let Some(message) = receiver.next().await {
        match message {
            StorageEntryMessage::Scanned(_) => {
                let now = Instant::now();
                latencies_ms.push(now.duration_since(previous).as_secs_f64() * 1000.0);
                previous = now;
                entries += 1;
            }
            StorageEntryMessage::Error { path, reason, .. } => {
                return Err(data_mover::error::StorageError::OperationError(format!(
                    "scan failed for {}: {reason}",
                    path.display()
                )));
            }
            _ => {}
        }
    }
    Ok(measurement(entries, latencies_ms))
}

async fn copy(source: &StorageEnum, destination: &StorageEnum) -> Result<Measurement> {
    let receiver = source.walkdir(None, WalkOptions::default()).await?;
    let mut pending = Vec::new();
    while let Some(message) = receiver.next().await {
        match message {
            StorageEntryMessage::Scanned(entry) if !entry.get_is_dir() => {
                pending.push(entry);
            }
            StorageEntryMessage::Scanned(entry) => destination.create_dir_all(&entry).await?,
            StorageEntryMessage::Error { path, reason, .. } => {
                return Err(data_mover::error::StorageError::OperationError(format!(
                    "scan failed for {}: {reason}",
                    path.display()
                )));
            }
            _ => {}
        }
    }
    let queued_at = Instant::now();
    let mut scheduling_latencies_ms = Vec::with_capacity(pending.len());
    for entry in &pending {
        scheduling_latencies_ms.push(queued_at.elapsed().as_secs_f64() * 1000.0);
        StorageEnum::copy_file(
            source,
            destination,
            entry,
            CopyOptions {
                enable_integrity_check: true,
                is_source_reserved: true,
                ..Default::default()
            },
        )
        .await?;
    }
    let entries = u64::try_from(pending.len()).map_err(|_| {
        data_mover::error::StorageError::OperationError(
            "small benchmark entry count exceeds u64".to_string(),
        )
    })?;
    Ok(measurement(entries, scheduling_latencies_ms))
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Args, Operation};

    #[test]
    fn copy_requires_destination() {
        let error = Args::try_parse_from([
            "storage_small_benchmark",
            "--operation",
            "copy",
            "--source",
            "/tmp/source",
            "--expected-entries",
            "10",
        ])
        .expect_err("copy without destination must fail");
        assert!(error.to_string().contains("--destination"));
    }

    #[test]
    fn scan_does_not_require_destination() {
        let args = Args::try_parse_from([
            "storage_small_benchmark",
            "--operation",
            "scan",
            "--source",
            "/tmp/source",
            "--expected-entries",
            "10",
        ])
        .expect("scan arguments should parse");
        assert!(matches!(args.operation, Operation::Scan));
    }
}
