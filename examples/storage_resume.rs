use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use clap::{Parser, ValueEnum};
use data_mover::error::StorageError;
use data_mover::{CommitCallback, Result, StorageEnum, StreamHandle, create_storage};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Phase {
    Interrupt,
    Resume,
}

#[derive(Debug, Parser)]
#[command(about = "Exercise a resumable copy in two independent processes")]
struct Args {
    #[arg(long)]
    source: String,
    #[arg(long)]
    destination: String,
    #[arg(long)]
    path: PathBuf,
    #[arg(long)]
    phase: Phase,
    #[arg(long, default_value_t = 5 * 1024 * 1024)]
    block_size: u64,
}

fn operation_error(message: impl Into<String>) -> StorageError {
    StorageError::OperationError(message.into())
}

fn interval_len(intervals: &[(u64, u64)]) -> u64 {
    intervals
        .iter()
        .map(|(start, end)| end.saturating_sub(*start))
        .sum()
}

async fn transfer_intervals(
    source: &StorageEnum,
    destination: &StorageEnum,
    entry: &data_mover::EntryEnum,
    handle: &StreamHandle,
    intervals: Vec<(u64, u64)>,
) -> Result<u64> {
    let expected = interval_len(&intervals);
    let bytes = Arc::new(AtomicU64::new(0));
    let committed = Arc::new(AtomicU64::new(0));
    let committed_callback = committed.clone();
    let callback: CommitCallback = Arc::new(move |_offset, len| {
        committed_callback.fetch_add(len, Ordering::Relaxed);
    });
    let (rx, read_task) =
        StorageEnum::read_chunk_stream(source, entry, Some(intervals), None, false, 8);

    StorageEnum::write_chunk_stream(
        destination,
        entry,
        rx,
        handle,
        Some(bytes.clone()),
        callback,
    )
    .await?;
    read_task
        .await
        .map_err(|error| operation_error(format!("read task failed: {error}")))??;

    let written = bytes.load(Ordering::Relaxed);
    let durable = committed.load(Ordering::Relaxed);
    if written != expected || durable != expected {
        return Err(operation_error(format!(
            "transfer accounting mismatch: expected {expected}, wrote {written}, committed {durable}"
        )));
    }
    Ok(written)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let source = create_storage(&args.source, Some(args.block_size), false).await?;
    let destination = create_storage(&args.destination, Some(args.block_size), true).await?;
    let entry = source.get_metadata(&args.path).await?;
    let size = entry.get_size();
    if size <= args.block_size {
        return Err(operation_error(format!(
            "fixture must be larger than block size: size={size}, block_size={}",
            args.block_size
        )));
    }

    let part_path = PathBuf::from(format!("{}.terrasync-part", args.path.display()));
    let resume = matches!(args.phase, Phase::Resume);
    let (missing, handle) =
        StorageEnum::resume_prepare(&destination, &entry, &part_path, resume).await?;

    match args.phase {
        Phase::Interrupt => {
            if missing != vec![(0, size)] {
                return Err(operation_error(format!(
                    "fresh transfer unexpectedly has non-full missing intervals: {missing:?}"
                )));
            }
            let prefix_end = match &handle {
                StreamHandle::S3 { part_size, .. } => *part_size,
                StreamHandle::Nas { .. } => size / 2,
            };
            let written = transfer_intervals(
                &source,
                &destination,
                &entry,
                &handle,
                vec![(0, prefix_end)],
            )
            .await?;
            println!("interrupted after {written} durable bytes; final object not committed");
        }
        Phase::Resume => {
            let expected = interval_len(&missing);
            if expected == 0 || expected >= size {
                return Err(operation_error(format!(
                    "resume did not discover a partial transfer: size={size}, missing={missing:?}"
                )));
            }
            let written =
                transfer_intervals(&source, &destination, &entry, &handle, missing).await?;
            StorageEnum::commit_chunk_stream(&destination, &entry, size, handle).await?;

            let source_hash = source.compute_hash(&args.path, size).await?;
            let destination_hash = destination
                .compute_hash(Path::new(&args.path), size)
                .await?;
            if source_hash != destination_hash {
                return Err(operation_error(format!(
                    "hash mismatch after resume: source={source_hash}, destination={destination_hash}"
                )));
            }
            if destination.get_metadata(&part_path).await.is_ok() {
                return Err(operation_error(format!(
                    "temporary path still exists after commit: {}",
                    part_path.display()
                )));
            }
            println!("resumed {written} bytes and verified {size} bytes");
        }
    }
    Ok(())
}
