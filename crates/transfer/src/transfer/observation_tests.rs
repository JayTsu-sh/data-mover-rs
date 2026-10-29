//! Observed requests use the same transfer lifecycle, with a source constraint and version pin.
use std::error::Error;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::{TransferPhase, TransferRequest, TransferRoute, transfer};
use crate::model::{EntryKind, ObservedEntry, SourceVersion, StoragePath};
use crate::storage::backends::s3::tests::{
    MemoryS3, Versioning, endpoint_of, native_context, source_storage,
};
use crate::storage::backends::{local, s3};
use crate::storage::listing::testing::listed;
use crate::storage::{SourceDescriptor, Storage};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn observation(descriptor: SourceDescriptor) -> TestResult<ObservedEntry> {
    Ok(ObservedEntry::new(
        descriptor.path,
        descriptor.kind,
        descriptor.size,
        descriptor
            .inline_timestamps
            .and_then(|times| times.modified),
        descriptor.source_identity,
    )?
    .with_history(descriptor.listing.history))
}

fn request(
    source: &Storage,
    observed: ObservedEntry,
    destination: &Storage,
) -> TestResult<TransferRequest> {
    Ok(TransferRequest::from_observation(
        source.clone(),
        observed,
        destination.clone(),
        CancellationToken::new(),
    )?)
}

async fn bucket(histories: bool) -> TestResult<(Arc<MemoryS3>, Storage, ObservedEntry)> {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    protocol
        .put_version("src/source", "v1", Bytes::from_static(b"first!"))
        .await;
    protocol
        .put_version("src/source", "v2", Bytes::from_static(b"second"))
        .await;
    protocol.answer_at_connect(if histories {
        s3::S3VersioningAtConnect::Enabled
    } else {
        s3::S3VersioningAtConnect::Never
    });
    let storage = source_storage(&protocol, "src")?;
    let (mut children, _) = listed(&*storage.namespace()?, &StoragePath::root()).await?;
    assert_eq!(children.len(), 1);
    let observed = observation(children.remove(0))?;
    Ok((protocol, storage, observed))
}

#[tokio::test]
async fn observed_version_is_pinned_for_streaming_and_native_copy() -> TestResult {
    for (native, route) in [
        (false, TransferRoute::Streaming),
        (true, TransferRoute::Native),
    ] {
        let (protocol, storage, observed) = bucket(true).await?;
        let request = request(
            &storage,
            observed,
            &s3::connect(
                protocol.clone(),
                endpoint_of(&protocol),
                native.then(native_context),
            )?,
        )?;
        assert_eq!(request.source_version(), &SourceVersion::Id("v2".into()));
        let expected_identity = TransferRequest::new(
            storage.clone(),
            StoragePath::new("source")?,
            s3::connect(
                protocol.clone(),
                endpoint_of(&protocol),
                native.then(native_context),
            )?,
            CancellationToken::new(),
        )
        .with_source_version(SourceVersion::Id("v2".into()))
        .identity();
        assert_eq!(request.identity(), expected_identity);
        protocol
            .put_version("src/source", "v3", Bytes::from_static(b"newest"))
            .await;
        let outcome = transfer(request).await?;
        assert_eq!(outcome.route, route);
        assert_eq!(
            protocol.objects.lock().await.get("source"),
            Some(&Bytes::from_static(b"second"))
        );
    }
    Ok(())
}

#[tokio::test]
async fn current_object_observation_accepts_unchanged_and_refuses_same_size_replacement()
-> TestResult {
    for changed in [false, true] {
        let (protocol, storage, observed) = bucket(false).await?;
        let request = request(
            &storage,
            observed,
            &s3::connect(
                protocol.clone(),
                endpoint_of(&protocol),
                Some(native_context()),
            )?,
        )?;
        assert_eq!(request.source_version(), &SourceVersion::Current);
        if changed {
            protocol
                .put_version("src/source", "v3", Bytes::from_static(b"newest"))
                .await;
        }
        match transfer(request).await {
            Ok(_) if !changed => {
                assert_eq!(
                    protocol.objects.lock().await.get("source"),
                    Some(&Bytes::from_static(b"second"))
                );
            }
            Err(error) if changed => {
                assert_eq!(error.phase(), TransferPhase::Describe);
                assert!(!protocol.objects.lock().await.contains_key("source"));
                assert_eq!(protocol.objects.lock().await.len(), 1);
            }
            _ => return Err("observation validation did not match source state".into()),
        }
    }
    Ok(())
}

#[tokio::test]
async fn observation_cannot_be_retargeted_to_another_version() -> TestResult {
    let (protocol, storage, observed) = bucket(true).await?;
    let request = request(
        &storage,
        observed,
        &s3::connect(
            protocol.clone(),
            endpoint_of(&protocol),
            Some(native_context()),
        )?,
    )?
    .with_source_version(SourceVersion::Id("v1".into()));
    let Err(error) = transfer(request).await else {
        return Err("retargeted observation was accepted".into());
    };
    assert_eq!(error.phase(), TransferPhase::Preflight);
    assert_eq!(protocol.objects.lock().await.len(), 1);
    assert!(!protocol.objects.lock().await.contains_key("source"));
    Ok(())
}

#[tokio::test]
async fn invalid_observations_are_refused_when_constructing_a_request() -> TestResult {
    let (protocol, storage, observed) = bucket(true).await?;
    let (_, other, _) = bucket(true).await?;
    assert!(request(&other, observed.clone(), &storage).is_err());
    let directory = ObservedEntry::new(
        observed.path().clone(),
        EntryKind::Directory,
        None,
        None,
        observed.source_identity().clone(),
    )?;
    assert!(request(&storage, directory, &storage).is_err());
    let segments = observed.into_segments(1);
    assert!(request(&storage, segments[0].clone(), &storage).is_err());
    assert!(request(&storage, segments[1].clone(), &storage).is_ok());
    protocol.put_delete_marker("src/source", "deleted").await;
    let (mut children, _) = listed(&*storage.namespace()?, &StoragePath::root()).await?;
    assert!(request(&storage, observation(children.remove(0))?, &storage).is_err());
    Ok(())
}

#[tokio::test]
async fn local_observation_detects_in_place_edits_and_preserves_the_destination() -> TestResult {
    for changed in [false, true] {
        let source_root = tempfile::tempdir()?;
        let destination_root = tempfile::tempdir()?;
        let path = source_root.path().join("source");
        std::fs::write(&path, b"before")?;
        filetime::set_file_mtime(&path, filetime::FileTime::from_unix_time(1_700_000_000, 0))?;
        std::fs::write(destination_root.path().join("source"), b"existing")?;
        let (source, _) =
            local::test_source_storage(source_root.path(), &source_root.path().to_string_lossy())?;
        let destination = local::test_destination_storage(
            destination_root.path(),
            &destination_root.path().to_string_lossy(),
        )?;
        let observed = observation(
            source
                .read_source()?
                .describe(&StoragePath::new("source")?)
                .await?,
        )?;
        let request = request(&source, observed, &destination)?;
        if changed {
            std::fs::write(&path, b"edited")?;
            filetime::set_file_mtime(&path, filetime::FileTime::from_unix_time(1_700_000_001, 0))?;
        }
        match transfer(request).await {
            Ok(_) if !changed => assert_eq!(
                std::fs::read(destination_root.path().join("source"))?,
                b"before"
            ),
            Err(error) if changed => {
                assert_eq!(error.phase(), TransferPhase::Describe);
                assert_eq!(
                    std::fs::read(destination_root.path().join("source"))?,
                    b"existing"
                );
                assert_eq!(std::fs::read_dir(destination_root.path())?.count(), 1);
            }
            _ => return Err("local observation validation did not match source state".into()),
        }
    }
    Ok(())
}
