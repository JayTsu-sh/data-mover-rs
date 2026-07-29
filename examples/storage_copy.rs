use std::path::Path;

use clap::Parser;
use data_mover::{Result, StorageEnum, create_storage};

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

    println!(
        "copied {} bytes: {} -> {} ({})",
        entry.get_size(),
        args.source,
        args.destination,
        args.path
    );
    Ok(())
}
