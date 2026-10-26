//! Read-only source contract: enumerate an existing NFS file, then copy that observation to
//! a temporary Local backend and independently compare the content and permissions.
//!
//! `NFS_REAL_URL` selects the export (optionally with a configured sub-root).
//! `NFS_LISTING_FILE` names a small existing regular file relative to that root. Only its
//! parent's direct children are enumerated; the NFS source is never mutated.
mod common;

#[allow(dead_code)]
#[path = "../examples/support/copy.rs"]
mod copy_support;

use std::env;
use std::error::Error;
use std::num::NonZeroUsize;

use data_mover::model::{EntryKind, ObservedEntry, StoragePath};
use data_mover::storage::{BackendConfig, NfsBackendConfig, Storage, connect_backend};
use data_mover::transfer::{TransferRequest, transfer};
use data_mover::traversal::{
    StorageTraversalSource, TraversalItem, TraversalOutcome, TraversalRequest, TraversalSource,
};
use tokio_util::sync::CancellationToken;

#[tokio::test]
#[ignore = "requires a read-only NFS source (NFS_REAL_URL, NFS_LISTING_FILE)"]
async fn listed_nfs_file_can_be_copied_to_local() -> Result<(), Box<dyn Error>> {
    let source = connect_backend(BackendConfig::Nfs(NfsBackendConfig {
        url: env::var("NFS_REAL_URL")?,
        transfer_concurrency: None,
        block_size: None,
        ensure_dir: false,
    }))
    .await?;
    let path = StoragePath::new(env::var("NFS_LISTING_FILE")?)?;
    let observed = listed_file(&source, &path).await?;
    assert_eq!(observed.kind(), EntryKind::File);
    let expected_size = observed.size().ok_or("listed file has no size")?;
    let destination_root = tempfile::tempdir()?;
    let destination = common::connect_local(destination_root.path(), 2).await?;
    copy_support::create_parent(&destination, &path).await?;
    let request = TransferRequest::from_observation(
        source.clone(),
        observed,
        destination.clone(),
        CancellationToken::new(),
    )?;
    let outcome = transfer(request).await?;
    assert_eq!(outcome.transferred_bytes, expected_size);
    let verified = copy_support::verify(&source, &destination, &path).await?;
    assert_eq!(verified, expected_size);
    eprintln!("listed and copied {verified} bytes; content and permissions verified");
    Ok(())
}

async fn listed_file(
    source: &Storage,
    path: &StoragePath,
) -> Result<ObservedEntry, Box<dyn Error>> {
    let parent = path
        .as_str()
        .rsplit_once('/')
        .map_or("", |(parent, _)| parent);
    let mut request = TraversalRequest::new(StoragePath::new(parent)?);
    request.max_depth = Some(NonZeroUsize::MIN);
    let traversal = StorageTraversalSource::new(source)?;
    let mut session = traversal.traverse(request);
    let mut observed = None;
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) if entry.path() == path => observed = Some(entry),
            TraversalItem::Failure(failure) => {
                return Err(format!("listing failed: {failure:?}").into());
            }
            _ => {}
        }
    }
    let TraversalOutcome::Completed(done) = session.finish().await? else {
        return Err("source listing did not complete".into());
    };
    assert_eq!(done.totals.failed_children, 0);
    assert_eq!(done.totals.listing_failures, 0);
    Ok(*observed.ok_or("requested file was not enumerated")?)
}
