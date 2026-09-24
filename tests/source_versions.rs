//! ADR-0006 C6 on a real Local tree: a source without versions refuses a named version at every
//! entry point — describe, read and the copy-metadata observation — instead of silently using the
//! current file.

use std::num::NonZeroUsize;

use data_mover::model::{FailureClass, ObservationPlan, SourceVersion, StoragePath};
use data_mover::storage::{
    BackendConfig, LocalBackendConfig, PreflightPolicy, ReadRequest, Storage, StorageRoleFailure,
    connect_backend,
};
use data_mover::transfer::{
    InflightLimits, TransferPhase, TransferRequest, TransferSide, transfer,
};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn local(root: &std::path::Path) -> TestResult<Storage> {
    Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: root.to_path_buf(),
        read_concurrency: NonZeroUsize::new(2).ok_or("non-zero")?,
        write_concurrency: NonZeroUsize::new(2).ok_or("non-zero")?,
    }))
    .await?)
}

fn unsupported<T>(result: &Result<T, StorageRoleFailure>) -> bool {
    matches!(result, Err(StorageRoleFailure::Entry(failure)) if failure.class() == FailureClass::Unsupported)
}

#[tokio::test]
async fn a_source_without_versions_refuses_a_named_version_everywhere() -> TestResult {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"payload")?;
    let storage = local(root.path()).await?;
    let path = StoragePath::new("file")?;
    let named = SourceVersion::Id("v1".into());
    let source = storage.read_source(&PreflightPolicy::production())?;
    assert!(!source.supports_source_versions());

    let described = source.describe_version(&path, &named).await;
    assert!(unsupported(&described), "{described:?}");
    let current = source
        .describe_version(&path, &SourceVersion::Current)
        .await?;
    assert_eq!(*current.version(), SourceVersion::Current);

    let read = source
        .read(ReadRequest {
            path: path.clone(),
            range: None,
            expected_source: Some(current.source_identity.clone()),
            maximum_chunk_bytes: 1024,
            read_inflight: 1,
            read_budget: None,
            cancel: CancellationToken::new(),
            source_qos: None,
            version: named.clone(),
        })
        .await;
    assert!(unsupported(&read), "a named version was read");

    let observed = storage
        .metadata(&PreflightPolicy::production())?
        .observe_copy_bound_version(
            &path,
            &current.source_identity,
            &named,
            ObservationPlan::default(),
        )
        .await;
    assert!(unsupported(&observed), "a named version was observed");
    Ok(())
}

/// A transfer asking a source without versions for one fails at preflight, before the destination
/// is touched: no final file and no `.data-mover-*` artifact.
#[tokio::test]
async fn a_named_version_from_a_source_without_versions_fails_before_any_write() -> TestResult {
    let source_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"payload")?;
    let destination_root = tempfile::tempdir()?;
    let request = TransferRequest::new(
        local(source_root.path()).await?,
        StoragePath::new("file")?,
        local(destination_root.path()).await?,
        StoragePath::new("copy")?,
        InflightLimits::new(2, 64 * 1024, 2)?,
        CancellationToken::new(),
    )
    .with_source_version(SourceVersion::Id("v1".into()));
    let Err(failure) = transfer(request).await else {
        return Err("a named version was copied from Local".into());
    };
    assert_eq!(failure.phase(), TransferPhase::Preflight);
    assert_eq!(failure.side(), TransferSide::Source);
    let role = std::error::Error::source(&failure)
        .and_then(|cause| cause.downcast_ref::<StorageRoleFailure>());
    assert!(
        matches!(role, Some(StorageRoleFailure::Entry(entry)) if entry.class() == FailureClass::Unsupported),
        "{failure:?}"
    );
    assert_eq!(std::fs::read_dir(destination_root.path())?.count(), 0);
    Ok(())
}
