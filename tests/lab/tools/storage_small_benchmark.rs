//! Small-entry performance gate, preserving the v1 public-seam measurement definitions.
//!
//! Scan latency is the measured interval between entry deliveries. Copy latency is the
//! measured wait from admitting a complete batch until each concurrency-one transfer starts.
//! Source connection is outside elapsed time; destination connection is inside, as in v1.

use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use data_mover::model::{EntryKind, StoragePath};
use data_mover::storage::{Storage, create_directory_all};
use data_mover::transfer::{ReadBackVerification, TransferRequest, transfer};
use data_mover::traversal::{
    StorageTraversalSource, TraversalItem, TraversalOutcome, TraversalRequest, TraversalSource,
};
use tokio_util::sync::CancellationToken;

#[allow(dead_code)]
#[path = "../../../examples/support/copy.rs"]
mod copy_support;
#[allow(dead_code)]
#[path = "../../../examples/support/endpoint.rs"]
mod endpoint_support;
use endpoint_support::{Result, connect_with_options};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Operation {
    Scan,
    Copy,
}

#[derive(Debug, Parser)]
#[command(about = "Measure small-entry scan or copy through data-mover storage roles")]
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

struct Measurement {
    entries: u64,
    p95_scheduling_latency: Duration,
}

#[tokio::main]
async fn main() -> Result {
    let args = Args::parse();
    // Validate before connection, even for scan, so the recorded chunk condition is meaningful.
    copy_support::limits(args.chunk_bytes)?;
    let source = connect_with_options(&args.source, false, Some(args.chunk_bytes)).await?;
    let started = Instant::now();
    let measurement = match args.operation {
        Operation::Scan => scan(&source).await?,
        Operation::Copy => {
            let destination = connect_with_options(
                args.destination
                    .as_deref()
                    .ok_or("copy requires --destination")?,
                true,
                Some(args.chunk_bytes),
            )
            .await?;
            copy(&source, &destination, args.chunk_bytes).await?
        }
    };
    if measurement.entries != args.expected_entries {
        return Err(format!(
            "entry count mismatch: expected {}, got {}",
            args.expected_entries, measurement.entries
        )
        .into());
    }
    println!(
        "entries={}\telapsed_ms={:.3}\tp95_scheduling_latency_ms={:.3}",
        measurement.entries,
        started.elapsed().as_secs_f64() * 1000.0,
        measurement.p95_scheduling_latency.as_secs_f64() * 1000.0,
    );
    Ok(())
}

fn measurement(mut latencies: Vec<Duration>) -> Result<Measurement> {
    latencies.sort_unstable();
    // ceil(0.95 * n) - 1, without floating-point rank rounding or multiplication overflow.
    let index = (latencies.len() - latencies.len() / 20).saturating_sub(1);
    Ok(Measurement {
        entries: u64::try_from(latencies.len())?,
        p95_scheduling_latency: latencies.get(index).copied().unwrap_or_default(),
    })
}

async fn scan(source: &Storage) -> Result<Measurement> {
    let source = StorageTraversalSource::new(source)?;
    let mut session = source.traverse(TraversalRequest::new(StoragePath::root()));
    let mut latencies = Vec::new();
    let mut previous = Instant::now();
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(_) => {
                let now = Instant::now();
                latencies.push(now.duration_since(previous));
                previous = now;
            }
            TraversalItem::DirectoryListed(_) | TraversalItem::Checkpoint(_) => {}
            other => return Err(format!("scan did not deliver a valid entry: {other:?}").into()),
        }
    }
    require_complete(session.finish().await?)?;
    measurement(latencies)
}

async fn copy(source: &Storage, destination: &Storage, chunk_bytes: u64) -> Result<Measurement> {
    let traversal = StorageTraversalSource::new(source)?;
    let mut session = traversal.traverse(TraversalRequest::new(StoragePath::root()));
    let mut pending = Vec::new();
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) if entry.kind() == EntryKind::File => {
                pending.push(entry.path().clone());
            }
            TraversalItem::Entry(entry) if entry.kind() == EntryKind::Directory => {
                create_directory_all(destination, entry.path()).await?;
            }
            TraversalItem::DirectoryListed(_) | TraversalItem::Checkpoint(_) => {}
            other => {
                return Err(format!("copy scan did not deliver a valid entry: {other:?}").into());
            }
        }
    }
    require_complete(session.finish().await?)?;
    let limits = copy_support::limits(chunk_bytes)?;
    let mut latencies = Vec::with_capacity(pending.len());
    let queued_at = Instant::now();
    for path in pending {
        latencies.push(queued_at.elapsed());
        transfer(
            TransferRequest::new(
                source.clone(),
                path.clone(),
                destination.clone(),
                path,
                limits,
                CancellationToken::new(),
            )
            .with_read_back_verification(ReadBackVerification::Enabled),
        )
        .await?;
    }
    measurement(latencies)
}

fn require_complete(outcome: TraversalOutcome) -> Result {
    if matches!(outcome, TraversalOutcome::Completed(_)) {
        Ok(())
    } else {
        Err(format!("benchmark traversal did not complete: {outcome:?}").into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_requires_destination_but_scan_does_not() -> Result {
        let arguments = [
            "benchmark",
            "--source",
            "/tmp/source",
            "--expected-entries",
            "100",
        ];
        assert!(
            Args::try_parse_from(arguments.into_iter().chain(["--operation", "copy"])).is_err()
        );
        let args = Args::try_parse_from(arguments.into_iter().chain(["--operation", "scan"]))?;
        assert!(matches!(args.operation, Operation::Scan));
        Ok(())
    }

    #[test]
    fn percentile_keeps_the_v1_nearest_rank_definition() -> Result {
        let measured = measurement((1..=100).rev().map(Duration::from_millis).collect())?;
        assert_eq!(measured.entries, 100);
        assert_eq!(measured.p95_scheduling_latency, Duration::from_millis(95));
        assert_eq!(
            measurement(Vec::new())?.p95_scheduling_latency,
            Duration::ZERO
        );
        Ok(())
    }

    #[tokio::test]
    async fn local_scan_and_copy_measure_real_work() -> Result {
        let source_root = tempfile::tempdir()?;
        let destination_root = tempfile::tempdir()?;
        for index in 0..20 {
            std::fs::write(
                source_root.path().join(format!("entry-{index}")),
                [index; 7],
            )?;
        }
        let source =
            connect_with_options(source_root.path().to_str().ok_or("UTF-8")?, false, None).await?;
        let destination = connect_with_options(
            destination_root.path().to_str().ok_or("UTF-8")?,
            false,
            None,
        )
        .await?;
        assert_eq!(scan(&source).await?.entries, 20);
        let copied = copy(&source, &destination, 2 * 1024 * 1024).await?;
        assert_eq!(copied.entries, 20);
        assert!(copied.p95_scheduling_latency > Duration::ZERO);
        for index in 0..20 {
            assert_eq!(
                std::fs::read(destination_root.path().join(format!("entry-{index}")))?,
                [index; 7]
            );
        }
        assert!(require_complete(TraversalOutcome::Cancelled).is_err());
        Ok(())
    }
}
