use clap::Parser;
#[allow(dead_code)]
#[path = "../../../examples/support/endpoint.rs"]
mod endpoint_support;
use data_mover::model::{FailureClass, StoragePath};
use data_mover::storage::StorageRoleFailure;
use endpoint_support::Result;

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
    let storage = endpoint_support::connect(&args.storage).await?;
    let namespace = storage.namespace()?;
    let path = StoragePath::new(args.path.clone())?;
    namespace.delete(&path).await?;
    match namespace.delete(&path).await {
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => {
            println!("verified second delete returns FileNotFound: {}", args.path);
            Ok(())
        }
        other => Err(format!(
            "second delete returned unexpected result for {}: {other:?}",
            args.path
        )
        .into()),
    }
}
