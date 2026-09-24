//! ADR-0006 C5 on real Local storage: the transfer identity is derived from the two endpoints and
//! paths, so independent connections — as a fresh process would make — name the same transfer, and
//! the ordinary entry and the expert destination half agree on it.

use std::num::NonZeroUsize;
use std::path::Path;

use data_mover::model::{ObservedEntry, SourceVersion, StoragePath};
use data_mover::storage::{
    BackendConfig, LocalBackendConfig, PreflightPolicy, Storage, connect_backend,
};
use data_mover::transfer::{
    ExpertDestinationRequest, ExpertDestinationSession, InflightLimits, TransferIdentity,
    TransferPhase, TransferPolicy, TransferRequest, transfer,
};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn local(root: &Path) -> TestResult<Storage> {
    Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: root.to_path_buf(),
        read_concurrency: NonZeroUsize::new(2).ok_or("non-zero")?,
        write_concurrency: NonZeroUsize::new(2).ok_or("non-zero")?,
    }))
    .await?)
}

fn limits() -> TestResult<InflightLimits> {
    Ok(InflightLimits::new(2, 256 * 1024, 2)?)
}

async fn request(
    source: &Path,
    destination: &Path,
    final_path: &str,
) -> TestResult<TransferRequest> {
    Ok(TransferRequest::new(
        local(source).await?,
        StoragePath::new("in.bin")?,
        local(destination).await?,
        StoragePath::new(final_path)?,
        limits()?,
        CancellationToken::new(),
    ))
}

fn roots() -> TestResult<(tempfile::TempDir, tempfile::TempDir)> {
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("in.bin"), b"payload")?;
    Ok((source, tempfile::tempdir()?))
}

#[tokio::test]
async fn independent_connections_derive_one_identity() -> TestResult {
    let (source, destination) = roots()?;
    let first = request(source.path(), destination.path(), "out.bin").await?;
    let second = request(source.path(), destination.path(), "out.bin").await?;
    assert_eq!(first.identity(), second.identity());

    let source_storage = local(source.path()).await?;
    let destination_storage = local(destination.path()).await?;
    assert_eq!(
        first.identity(),
        TransferIdentity::derive(
            source_storage.identity(),
            &StoragePath::new("in.bin")?,
            &SourceVersion::Current,
            destination_storage.identity(),
            &StoragePath::new("out.bin")?,
        )
    );
    let elsewhere = request(source.path(), destination.path(), "other.bin").await?;
    assert_ne!(first.identity(), elsewhere.identity());
    Ok(())
}

/// A second root spelled through a symlink is the same endpoint: Local endpoints are realpaths.
#[cfg(unix)]
#[tokio::test]
async fn a_symlinked_spelling_of_a_root_derives_the_same_identity() -> TestResult {
    let (source, destination) = roots()?;
    let links = tempfile::tempdir()?;
    let linked = links.path().join("source");
    std::os::unix::fs::symlink(source.path(), &linked)?;
    assert_eq!(
        request(source.path(), destination.path(), "out.bin")
            .await?
            .identity(),
        request(&linked, destination.path(), "out.bin")
            .await?
            .identity()
    );
    Ok(())
}

#[tokio::test]
async fn the_outcome_reports_the_derived_identity_or_the_override() -> TestResult {
    let (source, destination) = roots()?;
    let derived = request(source.path(), destination.path(), "derived.bin").await?;
    let expected = derived.identity();
    assert_eq!(transfer(derived).await?.identity, expected);

    let label = TransferIdentity::from_label("caller-job-7")?;
    let overridden = request(source.path(), destination.path(), "labelled.bin")
        .await?
        .with_identity_override(label);
    assert_eq!(overridden.identity(), label);
    assert_eq!(transfer(overridden).await?.identity, label);
    assert_eq!(
        std::fs::read(destination.path().join("labelled.bin"))?,
        b"payload"
    );
    Ok(())
}

async fn observe(storage: &Storage, path: &str) -> TestResult<ObservedEntry> {
    let descriptor = storage
        .read_source(&PreflightPolicy::production())?
        .describe(&StoragePath::new(path)?)
        .await?;
    Ok(ObservedEntry::new(
        descriptor.path,
        descriptor.kind,
        descriptor.size,
        None,
        descriptor.source_identity,
    )?)
}

/// The expert destination half derives its identity from the observation it receives, so a remote
/// destination names the transfer without being told. (Its binding still differs from the ordinary
/// route's: the observation carries no content version — a known gap, see ADR-0007.)
#[tokio::test]
async fn the_expert_destination_derives_the_same_identity_as_a_request() -> TestResult {
    let (source, destination) = roots()?;
    let observation = observe(&local(source.path()).await?, "in.bin").await?;
    let session = ExpertDestinationSession::prepare(ExpertDestinationRequest::new(
        observation,
        256 * 1024,
        local(destination.path()).await?,
        StoragePath::new("out.bin")?,
        limits()?,
        CancellationToken::new(),
    ))
    .await?;
    let identity = session.identity();
    session.discard().await?;
    assert_eq!(
        identity,
        request(source.path(), destination.path(), "out.bin")
            .await?
            .identity()
    );
    Ok(())
}

fn checkpointed_half(
    observation: ObservedEntry,
    destination: Storage,
) -> TestResult<ExpertDestinationRequest> {
    Ok(ExpertDestinationRequest::new(
        observation,
        64 * 1024,
        destination,
        StoragePath::new("out.bin")?,
        limits()?,
        CancellationToken::new(),
    )
    .with_transfer_policy(TransferPolicy::Checkpointed))
}

/// Two concurrent transfers of the same pair now share an identity and so a binding: while one
/// holds the recovery lease, the other is refused before it writes anything, and the holder's
/// stage is left intact. (Distinct caller labels used to keep them apart; with derived identities
/// the per-file lease is what stops two writers of one destination file in one host. The lease
/// lives in the local recovery store, which C21 removes: revisit this test then.)
#[tokio::test]
async fn a_concurrent_transfer_of_the_same_pair_is_refused() -> TestResult {
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("in.bin"), vec![0x5a_u8; 1024 * 1024])?;
    let destination = tempfile::tempdir()?;
    let observation = observe(&local(source.path()).await?, "in.bin").await?;
    let held = ExpertDestinationSession::prepare(checkpointed_half(
        observation.clone(),
        local(destination.path()).await?,
    )?)
    .await?;
    let refused = ExpertDestinationSession::prepare(checkpointed_half(
        observation,
        local(destination.path()).await?,
    )?)
    .await;
    let Err(failure) = refused else {
        return Err("a second transfer of the same pair was admitted".into());
    };
    assert_eq!(
        failure.phase(),
        TransferPhase::RecoveryRegistration,
        "{failure}"
    );
    assert!(!failure.has_recoverable_stage());
    let stage_names = |root: &Path| -> TestResult<Vec<String>> {
        let mut names = std::fs::read_dir(root)?
            .map(|entry| Ok(entry?.file_name().to_string_lossy().into_owned()))
            .collect::<TestResult<Vec<_>>>()?;
        names.sort();
        Ok(names)
    };
    let before = stage_names(destination.path())?;
    assert!(
        before.iter().any(|name| name.starts_with(".data-mover-")),
        "the holder has a stage: {before:?}"
    );
    held.discard().await?;
    assert!(!destination.path().join("out.bin").exists());
    Ok(())
}
