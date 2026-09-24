//! ADR-0006 C6 on a real Local tree: a source without versions refuses a named version at every
//! entry point — describe, read and the copy-metadata observation — instead of silently using the
//! current file.

use std::num::NonZeroUsize;

use data_mover::model::{FailureClass, ObservationPlan, SourceVersion, StoragePath};
use data_mover::storage::{
    BackendConfig, LocalBackendConfig, PreflightPolicy, ReadRequest, Storage, StorageRoleFailure,
    connect_backend,
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
