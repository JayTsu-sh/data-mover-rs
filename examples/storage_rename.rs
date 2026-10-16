use clap::Parser;
#[allow(dead_code)]
#[path = "support/endpoint.rs"]
mod endpoint_support;
use data_mover::model::StoragePath;

use endpoint_support::Result;

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
    let storage = endpoint_support::connect(&args.storage).await?;
    let namespace = storage.namespace()?;
    let from = StoragePath::new(args.from.clone())?;
    let to = StoragePath::new(args.to.clone())?;
    let source = namespace.stat(&from).await?;
    namespace.rename(&from, &to).await?;
    let destination = namespace.stat(&to).await?;
    if destination.size != source.size {
        return Err(format!(
            "destination size mismatch: expected {:?}, got {:?}",
            source.size, destination.size
        )
        .into());
    }
    println!(
        "renamed and verified {} bytes: {} -> {}",
        source.size.unwrap_or(0),
        args.from,
        args.to
    );
    Ok(())
}
