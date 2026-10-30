//! Remove artifacts and payloads only within an explicitly isolated `QoS` run root.
use std::env;
use std::num::NonZeroUsize;

use clap::Parser;
use data_mover::model::{FailureClass, StoragePath};
use data_mover::storage::{
    DeleteTreeItem, DeleteTreeOutcome, DeleteTreeRequest, StorageRoleFailure, delete_tree,
};
use tokio_util::sync::CancellationToken;

#[allow(dead_code)]
#[path = "../../../examples/support/endpoint.rs"]
mod endpoint_support;
mod qos_run_scope;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    run_id: String,
    /// Remove this known fixture after cleaning abandoned transfer artifacts.
    #[arg(long, value_parser = ["64.bin", "128.bin", "512.bin"])]
    file: Option<String>,
    /// Remove children recursively inside the validated run endpoint, retaining its root.
    #[arg(long, conflicts_with = "file")]
    tree: bool,
}

#[tokio::main(worker_threads = 4)]
async fn main() -> endpoint_support::Result {
    let args = Args::parse();
    let endpoint = qos_run_scope::validated_endpoint(&env::var("QOS_SOURCE")?, &args.run_id)?;
    let storage = endpoint_support::connect_with_options(&endpoint, false, None).await?;
    let namespace = storage.namespace()?;
    namespace
        .cleanup_transfer_artifacts(&StoragePath::root())
        .await?;
    if args.tree {
        let mut session = delete_tree(
            &storage,
            DeleteTreeRequest {
                root: StoragePath::root(),
                delete_root: false,
                max_inflight_operations: NonZeroUsize::new(4).ok_or("nonzero concurrency")?,
                max_buffered_items: NonZeroUsize::new(64).ok_or("nonzero buffer")?,
                cancel: CancellationToken::new(),
            },
        )?;
        let mut failures = 0;
        while let Some(item) = session.next_item().await {
            if matches!(item, DeleteTreeItem::EntryFailure(_)) {
                failures += 1;
            }
        }
        if !matches!(session.finish().await?, DeleteTreeOutcome::Completed(_)) || failures != 0 {
            return Err("isolated tree cleanup failed".into());
        }
    }
    if let Some(file) = args.file {
        match namespace.delete(&StoragePath::new(&file)?).await {
            Ok(()) => {}
            Err(StorageRoleFailure::Entry(failure))
                if failure.class() == FailureClass::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    println!("{{\"cleaned\":true}}");
    Ok(())
}
