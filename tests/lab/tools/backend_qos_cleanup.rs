//! Remove artifacts and payloads only within an explicitly isolated `QoS` run root.
use clap::Parser;
use data_mover::model::{FailureClass, StoragePath};
use data_mover::storage::{
    DeleteTreeItem, DeleteTreeOutcome, DeleteTreeRequest, StorageRoleFailure, delete_tree,
};
use std::num::NonZeroUsize;
use tokio_util::sync::CancellationToken;

#[allow(dead_code)]
#[path = "../../../examples/support/endpoint.rs"]
mod endpoint_support;

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
    let endpoint = std::env::var("QOS_SOURCE")?;
    let endpoint_path =
        url::Url::parse(&endpoint).map_or_else(|_| endpoint.clone(), |url| url.path().to_owned());
    if !args.run_id.starts_with("nightly-") && !args.run_id.starts_with("release-") {
        return Err("an isolated lab run identifier is required".into());
    }
    if args.run_id.len() > 88
        || args.run_id.contains("..")
        || !args
            .run_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
        || !endpoint_path.split('/').any(|part| part == args.run_id)
    {
        return Err("the source endpoint must be scoped to the isolated run".into());
    }
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
