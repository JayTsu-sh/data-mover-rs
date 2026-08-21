use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use clap::{Parser, ValueEnum};
use data_mover::error::StorageError;
use data_mover::{CommitCallback, EntryEnum, Result, StorageEnum, StreamHandle, create_storage};
use serde::{Deserialize, Serialize};

mod hdfs_support;

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
    /// Write receiver-discovered durable state as JSON for another process.
    #[arg(long)]
    state_file: Option<PathBuf>,
    /// State emitted by the interrupted process, used to reject source changes.
    #[arg(long)]
    prior_state_file: Option<PathBuf>,
    /// After publishing interrupt state, remain alive so the lab can SIGKILL us.
    #[arg(long)]
    hold_after_interrupt: bool,
    /// Delete the source only after resume commit and full hash verification.
    #[arg(long)]
    delete_source: bool,
}

#[derive(Deserialize, Serialize)]
struct ResumeState {
    size: u64,
    missing: Vec<(u64, u64)>,
    handle: StreamHandle,
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

fn creation_options(location: &str, ensure_dir: bool) -> data_mover::CreateStorageOptions {
    data_mover::CreateStorageOptions {
        ensure_dir,
        backend: if location.starts_with("hdfs://") {
            data_mover::BackendConfig::Hdfs(hdfs_support::config())
        } else {
            data_mover::BackendConfig::Default
        },
        ..Default::default()
    }
}

fn write_state(
    path: Option<&Path>,
    size: u64,
    missing: &[(u64, u64)],
    handle: &StreamHandle,
) -> Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    let state = serde_json::to_vec(&ResumeState {
        size,
        missing: missing.to_vec(),
        handle: handle.clone(),
    })
    .map_err(|error| operation_error(format!("serialize resume state: {error}")))?;
    std::fs::write(path, state).map_err(StorageError::IoError)
}

fn validate_prior_state(path: Option<&Path>, current_size: u64) -> Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    let encoded = std::fs::read(path).map_err(StorageError::IoError)?;
    let state: ResumeState = serde_json::from_slice(&encoded)
        .map_err(|error| operation_error(format!("parse prior resume state: {error}")))?;
    if state.size != current_size {
        return Err(operation_error(format!(
            "source size changed since interruption: was {}, now {current_size}",
            state.size
        )));
    }
    Ok(())
}

async fn delete_verified_source(
    source: &StorageEnum,
    entry: &EntryEnum,
    path: &Path,
    enabled: bool,
) -> Result<()> {
    if !enabled {
        return Ok(());
    }
    source.delete_file(entry).await?;
    if source.get_metadata(path).await.is_ok() {
        return Err(operation_error(format!(
            "source still exists after verified deletion: {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ResumeState, creation_options, validate_prior_state};

    #[test]
    fn prior_state_rejects_a_changed_source_size() {
        let path = std::env::temp_dir().join(format!(
            "data-mover-resume-state-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        let state = ResumeState {
            size: 16,
            missing: vec![(8, 16)],
            handle: data_mover::StreamHandle::Hdfs {
                part_path: "file.part".into(),
                prefix_len: 8,
                expected_size: 16,
            },
        };
        let encoded = serde_json::to_vec(&state).expect("serialize state");
        std::fs::write(&path, encoded).expect("write state");
        assert!(validate_prior_state(Some(&path), 16).is_ok());
        assert!(validate_prior_state(Some(&path), 17).is_err());
        std::fs::remove_file(path).expect("remove state");
    }

    #[test]
    fn hdfs_locations_select_explicit_backend_configuration() {
        assert!(matches!(
            creation_options("hdfs://user@host:9000/root", true).backend,
            data_mover::BackendConfig::Hdfs(_)
        ));
    }
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
    let source = create_storage(&args.source, creation_options(&args.source, false)).await?;
    let destination =
        create_storage(&args.destination, creation_options(&args.destination, true)).await?;
    let entry = source.get_metadata(&args.path).await?;
    let size = entry.get_size();
    if matches!(args.phase, Phase::Resume) {
        validate_prior_state(args.prior_state_file.as_deref(), size)?;
    }
    if size == 0 {
        return Err(operation_error(format!(
            "fixture must not be empty: {}",
            args.path.display()
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
                StreamHandle::Nas { .. } | StreamHandle::Hdfs { .. } => size / 2,
            };
            let written = transfer_intervals(
                &source,
                &destination,
                &entry,
                &handle,
                vec![(0, prefix_end)],
            )
            .await?;
            let (durable_missing, durable_handle) =
                StorageEnum::resume_prepare(&destination, &entry, &part_path, true).await?;
            write_state(
                args.state_file.as_deref(),
                size,
                &durable_missing,
                &durable_handle,
            )?;
            println!("interrupted after {written} durable bytes; final object not committed");
            if args.hold_after_interrupt {
                std::future::pending::<()>().await;
            }
        }
        Phase::Resume => {
            write_state(args.state_file.as_deref(), size, &missing, &handle)?;
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
            let destination_entry = destination.get_metadata(&args.path).await?;
            if matches!(
                (&entry, &destination_entry),
                (EntryEnum::NAS(_) | EntryEnum::S3(_), EntryEnum::NAS(_))
            ) && destination_entry.get_mtime() != entry.get_mtime()
            {
                return Err(operation_error(format!(
                    "mtime mismatch after resume: source={:?}, destination={:?}",
                    entry.get_mtime(),
                    destination_entry.get_mtime()
                )));
            }
            if destination.get_metadata(&part_path).await.is_ok() {
                return Err(operation_error(format!(
                    "temporary path still exists after commit: {}",
                    part_path.display()
                )));
            }
            delete_verified_source(&source, &entry, &args.path, args.delete_source).await?;
            println!("resumed {written} bytes and verified {size} bytes");
        }
    }
    Ok(())
}
