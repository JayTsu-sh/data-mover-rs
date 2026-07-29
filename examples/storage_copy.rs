use std::path::Path;

use clap::Parser;
use data_mover::{EntryEnum, Result, StorageEnum, create_storage};

#[derive(Debug, Parser)]
#[command(about = "Copy one file between two data-mover storage URLs")]
struct Args {
    /// Source storage URL.
    #[arg(long)]
    source: String,

    /// Destination storage URL.
    #[arg(long)]
    destination: String,

    /// Path relative to both storage roots.
    #[arg(long)]
    path: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let source = create_storage(&args.source, None, false).await?;
    let destination = create_storage(&args.destination, None, true).await?;
    let entry = source.get_metadata(Path::new(&args.path)).await?;

    StorageEnum::copy_file(&source, &destination, &entry, None, true, true, None).await?;

    let copied = destination.get_metadata(Path::new(&args.path)).await?;
    if copied.get_size() != entry.get_size() {
        return Err(data_mover::error::StorageError::OperationError(format!(
            "destination size mismatch: expected {}, got {}",
            entry.get_size(),
            copied.get_size()
        )));
    }
    if matches!((&entry, &copied), (EntryEnum::NAS(_), EntryEnum::NAS(_))) {
        if copied.get_mtime() != entry.get_mtime() {
            return Err(data_mover::error::StorageError::OperationError(format!(
                "destination mtime mismatch: expected {}, got {}",
                entry.get_mtime(),
                copied.get_mtime()
            )));
        }
        if copied.get_mode().map(|mode| mode & 0o7777) != entry.get_mode().map(|mode| mode & 0o7777)
        {
            return Err(data_mover::error::StorageError::OperationError(format!(
                "destination mode mismatch: expected {:?}, got {:?}",
                entry.get_mode().map(|mode| mode & 0o7777),
                copied.get_mode().map(|mode| mode & 0o7777)
            )));
        }
        if copied.get_uid() != entry.get_uid() || copied.get_gid() != entry.get_gid() {
            return Err(data_mover::error::StorageError::OperationError(format!(
                "destination ownership mismatch: expected {:?}:{:?}, got {:?}:{:?}",
                entry.get_uid(),
                entry.get_gid(),
                copied.get_uid(),
                copied.get_gid()
            )));
        }
    }

    println!(
        "copied and verified {} bytes and applicable metadata: {}",
        entry.get_size(),
        args.path
    );
    Ok(())
}
