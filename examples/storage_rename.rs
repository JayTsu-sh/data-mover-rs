use std::path::Path;

use clap::Parser;
use data_mover::{Result, create_storage};

#[derive(Debug, Parser)]
#[command(about = "Rename one entry within a data-mover storage URL")]
struct Args {
    /// Storage URL containing both paths.
    #[arg(long)]
    storage: String,

    /// Source path relative to the storage root.
    #[arg(long)]
    from: String,

    /// Destination path relative to the storage root.
    #[arg(long)]
    to: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let storage = create_storage(&args.storage, None, false).await?;
    let source = storage.get_metadata(Path::new(&args.from)).await?;
    let expected_size = source.get_size();

    storage
        .rename(Path::new(&args.from), Path::new(&args.to))
        .await?;

    let destination = storage.get_metadata(Path::new(&args.to)).await?;
    if destination.get_size() != expected_size {
        return Err(data_mover::error::StorageError::OperationError(format!(
            "destination size mismatch: expected {expected_size}, got {}",
            destination.get_size()
        )));
    }

    println!(
        "renamed and verified {expected_size} bytes: {} -> {}",
        args.from, args.to
    );
    Ok(())
}
