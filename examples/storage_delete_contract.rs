use std::path::Path;

use clap::Parser;
use data_mover::error::StorageError;
use data_mover::{Result, create_storage};

#[derive(Debug, Parser)]
#[command(about = "Verify that deleting an absent file returns FileNotFound")]
struct Args {
    /// Storage URL containing the test fixture.
    #[arg(long)]
    storage: String,

    /// Existing file relative to the storage root.
    #[arg(long)]
    path: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let storage = create_storage(&args.storage, None, false).await?;
    let entry = storage.get_metadata(Path::new(&args.path)).await?;

    storage.delete_file(&entry).await?;
    match storage.delete_file(&entry).await {
        Err(StorageError::FileNotFound(path)) if path == args.path => {
            println!("verified second delete returns FileNotFound: {}", args.path);
            Ok(())
        }
        Err(error) => Err(StorageError::OperationError(format!(
            "second delete returned unexpected error for {}: {error}",
            args.path
        ))),
        Ok(()) => Err(StorageError::OperationError(format!(
            "second delete unexpectedly succeeded for {}",
            args.path
        ))),
    }
}
