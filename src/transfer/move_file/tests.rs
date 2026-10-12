use std::{error::Error, num::NonZeroUsize, path::Path, sync::Arc};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::model::{EntryOperationFailure, FailureClass, Operation, StoragePath, Transience};
use crate::storage::backends::local::connect_transfer;
use crate::storage::{
    ListRequest, ListingStream, Namespace, PreflightPolicy, Storage, StorageRoleFailure,
};
use crate::transfer::{
    InflightLimits, MoveError, ReadBackVerification, SourceVersion, TransferRequest, move_file,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn connect(root: &Path) -> TestResult<Storage> {
    let slots = NonZeroUsize::new(2).ok_or("nonzero")?;
    connect_transfer(root, slots, slots)
}

fn request(source: Storage, destination: Storage) -> TestResult<TransferRequest> {
    Ok(TransferRequest::new(
        source,
        StoragePath::new("source")?,
        destination,
        StoragePath::new("final")?,
        InflightLimits::new(2, 64 * 1024, 2)?,
        CancellationToken::new(),
    ))
}

#[tokio::test]
async fn verified_move_removes_source_and_keeps_content_and_mode() -> TestResult {
    let from = tempfile::tempdir()?;
    let to = tempfile::tempdir()?;
    std::fs::write(from.path().join("source"), b"verified payload")?;
    let permissions = std::fs::metadata(from.path().join("source"))?.permissions();
    let outcome = move_file(
        request(connect(from.path())?, connect(to.path())?)?
            .with_read_back_verification(ReadBackVerification::Disabled),
    )
    .await?;
    assert_eq!(outcome.read_back, ReadBackVerification::Enabled);
    assert!(outcome.blake3.is_some());
    assert!(!from.path().join("source").exists());
    assert_eq!(std::fs::read(to.path().join("final"))?, b"verified payload");
    assert_eq!(
        std::fs::metadata(to.path().join("final"))?.permissions(),
        permissions
    );
    Ok(())
}

#[tokio::test]
async fn preflight_refusal_and_cancel_keep_source_and_destination_untouched() -> TestResult {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("source"), b"keep")?;
    let storage = connect(root.path())?;
    let mut same = request(storage.clone(), storage.clone())?;
    same.final_path = same.source_path.clone();
    assert!(matches!(move_file(same).await, Err(error) if error.published.is_none()));
    let historical = request(storage.clone(), storage.clone())?
        .with_source_version(SourceVersion::Id("old".into()));
    assert!(matches!(move_file(historical).await, Err(error) if error.published.is_none()));
    let cancelled = request(storage.clone(), storage)?;
    cancelled.cancel.cancel();
    assert!(
        matches!(move_file(cancelled).await, Err(error) if matches!(error.error, MoveError::Cancelled))
    );
    assert_eq!(std::fs::read(root.path().join("source"))?, b"keep");
    assert!(!root.path().join("final").exists());
    Ok(())
}

#[tokio::test]
async fn copy_failure_keeps_source() -> TestResult {
    let from = tempfile::tempdir()?;
    let to = tempfile::tempdir()?;
    std::fs::write(from.path().join("source"), b"keep")?;
    std::fs::create_dir(to.path().join("final"))?;
    std::fs::write(to.path().join("final/child"), b"keep too")?;
    let Err(failure) = move_file(request(connect(from.path())?, connect(to.path())?)?).await else {
        return Err("published over a nonempty directory".into());
    };
    assert!(failure.published.is_none());
    assert_eq!(std::fs::read(from.path().join("source"))?, b"keep");
    assert_eq!(std::fs::read(to.path().join("final/child"))?, b"keep too");
    Ok(())
}

struct DenyDelete;

#[async_trait]
impl Namespace for DenyDelete {
    fn list(&self, request: ListRequest) -> ListingStream {
        ListingStream::failed(request.path.clone(), denied(&request.path))
    }

    async fn delete(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        Err(denied(path))
    }
}

fn denied(path: &StoragePath) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            Operation::Namespace,
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "test delete refused",
        )
        .unwrap_or_else(|_| unreachable!("valid failure")),
    )
}

#[tokio::test]
async fn delete_failure_returns_published_copy_and_preserves_source() -> TestResult {
    let from = tempfile::tempdir()?;
    let to = tempfile::tempdir()?;
    std::fs::write(from.path().join("source"), b"copied")?;
    let storage = connect(from.path())?;
    let policy = PreflightPolicy::production();
    let source = Storage::connected(
        storage.identity().clone(),
        storage.capabilities().clone(),
        Some(storage.read_source(&policy)?),
        Some(storage.staged_destination(&policy)?),
        Some(Arc::new(DenyDelete)),
        Some(storage.metadata(&policy)?),
        None,
    )?;
    let Err(failure) = move_file(request(source, connect(to.path())?)?).await else {
        return Err("source removal unexpectedly succeeded".into());
    };
    assert!(failure.published.is_some());
    assert!(matches!(failure.error, MoveError::Source(_)));
    assert_eq!(std::fs::read(from.path().join("source"))?, b"copied");
    assert_eq!(std::fs::read(to.path().join("final"))?, b"copied");
    Ok(())
}
