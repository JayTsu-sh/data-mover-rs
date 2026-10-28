//! Streams entries from a storage root and checks completion separately from entry failures.
use clap::Parser;
use data_mover::model::StoragePath;
use data_mover::traversal::{
    StorageTraversalSource, TraversalItem, TraversalOutcome, TraversalRequest, TraversalSource,
};

#[allow(dead_code)]
#[path = "support/endpoint.rs"]
mod endpoint_support;
use endpoint_support::Result;

#[derive(Parser)]
struct Args {
    /// A local directory or storage URL.
    #[arg(long)]
    storage: String,
}

#[tokio::main]
async fn main() -> Result {
    let args = Args::parse();
    let storage = endpoint_support::connect_with_options(&args.storage, false, None).await?;
    let source = StorageTraversalSource::new(&storage)?;
    let mut session = source.traverse(TraversalRequest::new(StoragePath::root()));
    let mut failures = 0;
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => {
                println!("{:?}\t{}", entry.kind(), entry.path().as_str());
            }
            TraversalItem::Failure(failure) => {
                failures += 1;
                eprintln!(
                    "{}: {} — {}",
                    failure.path.as_str(),
                    failure.failure,
                    failure.failure.diagnostic()
                );
            }
            TraversalItem::ObservationFailure(failure) => {
                failures += 1;
                eprintln!(
                    "{}: {:?} {:?} ({:?}, {:?})",
                    failure.path,
                    failure.version,
                    failure.family,
                    failure.class,
                    failure.transience
                );
            }
            TraversalItem::DirectoryListed(_) | TraversalItem::Checkpoint(_) => {}
            other => {
                failures += 1;
                eprintln!("unsupported traversal item: {other:?}");
            }
        }
    }
    match session.finish().await? {
        TraversalOutcome::Completed(done) if failures == 0 => {
            println!("completed: {:?}", done.totals);
            Ok(())
        }
        outcome => Err(format!("traversal incomplete or contains failures: {outcome:?}").into()),
    }
}
