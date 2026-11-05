//! Observed requests use the same transfer lifecycle, with a source constraint and version pin.
use std::error::Error;
use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;
use data_mover_traversal::{
    StorageTraversalSource, TraversalItem, TraversalRequest, TraversalSource,
};
use tokio_util::sync::CancellationToken;

use super::{
    ExpertSourceRequest, ExpertSourceSession, TransferPhase, TransferRequest, TransferRoute,
    transfer,
};
use crate::model::{
    EntryKind, ObservedEntry, SourceVersion, StoragePath, StorageTimestamp, TimePrecision,
    VersionRecord,
};
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
    .with_history(descriptor.listing.history)
    .unwrap_or_else(|error| panic!("{error}")))
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

#[tokio::test]
async fn shared_observation_pins_one_historical_record_without_cloning_history() -> TestResult {
    for native in [false, true] {
        let (protocol, storage, observed) = bucket(true).await?;
        let observed = Arc::new(observed);
        let request = TransferRequest::from_shared_observation(
            storage,
            Arc::clone(&observed),
            Some(0),
            s3::connect(
                protocol.clone(),
                endpoint_of(&protocol),
                native.then(native_context),
            )?,
            CancellationToken::new(),
        )?;
        assert_eq!(request.source_version(), &SourceVersion::Id("v1".into()));
        let cloned = request.clone();
        assert_eq!(Arc::strong_count(&observed), 3);
        drop(cloned);
        protocol
            .put_version("src/source", "v3", Bytes::from_static(b"newest"))
            .await;
        transfer(request).await?;
        assert_eq!(
            protocol.objects.lock().await.get("source"),
            Some(&Bytes::from_static(b"first!"))
        );
        assert_eq!(
            observed.history().ok_or("missing history")?.records().len(),
            2
        );
        assert_eq!(Arc::strong_count(&observed), 1);
    }
    Ok(())
}

#[tokio::test]
async fn shared_current_observation_rejects_replacement_before_destination_writes() -> TestResult {
    let (protocol, storage, observed) = bucket(false).await?;
    let observed = Arc::new(observed);
    let destination = s3::connect(protocol.clone(), endpoint_of(&protocol), None)?;
    assert!(
        TransferRequest::from_shared_observation(
            storage.clone(),
            Arc::clone(&observed),
            Some(7),
            destination.clone(),
            CancellationToken::new()
        )
        .is_err()
    );
    let request = TransferRequest::from_shared_observation(
        storage,
        observed,
        None,
        destination,
        CancellationToken::new(),
    )?;
    protocol
        .put_version("src/source", "v3", Bytes::from_static(b"newest"))
        .await;
    let failure = transfer(request)
        .await
        .err()
        .ok_or("changed source was accepted")?;
    assert_eq!(failure.phase(), TransferPhase::Describe);
    assert!(!protocol.objects.lock().await.contains_key("source"));
    Ok(())
}

#[tokio::test]
async fn shared_history_directory_selects_its_real_object_path() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    protocol.answer_at_connect(s3::S3VersioningAtConnect::Enabled);
    protocol.put_version("src/p/", "old", Bytes::new()).await;
    protocol.put_version("src/p/", "new", Bytes::new()).await;
    protocol
        .put_version("src/p/child", "child", Bytes::from_static(b"child"))
        .await;
    let storage = source_storage(&protocol, "src")?;
    let (children, _) = listed(&*storage.namespace()?, &StoragePath::root()).await?;
    let descriptor = children
        .into_iter()
        .find(|child| child.path.as_str() == "p")
        .ok_or("missing directory")?;
    let mut descriptor = descriptor;
    let (objects, _) = listed(&*storage.namespace()?, &StoragePath::new("p")?).await?;
    descriptor.listing.history = objects
        .into_iter()
        .find(|object| object.path.as_str() == "p/")
        .ok_or("missing own object")?
        .listing
        .history;
    let observed = Arc::new(observation(descriptor)?);
    assert_eq!(observed.kind(), EntryKind::Directory);
    let destination = s3::connect(protocol.clone(), endpoint_of(&protocol), None)?;
    let request = TransferRequest::from_shared_observation(
        storage,
        observed,
        Some(0),
        destination,
        CancellationToken::new(),
    )?;
    assert_eq!(request.source_path().as_str(), "p/");
    transfer(request).await?;
    assert_eq!(protocol.objects.lock().await.get("p/"), Some(&Bytes::new()));
    assert!(!protocol.objects.lock().await.contains_key("p"));
    Ok(())
}

#[tokio::test]
async fn shared_selected_null_rejects_overwrite_and_rejects_marker_indices() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    protocol.answer_at_connect(s3::S3VersioningAtConnect::Enabled);
    protocol
        .put_version("src/source", "null", Bytes::from_static(b"old-null"))
        .await;
    protocol.put_delete_marker("src/source", "marker").await;
    protocol
        .put_version("src/source", "current", Bytes::from_static(b"latest"))
        .await;
    let storage = source_storage(&protocol, "src")?;
    let (mut children, _) = listed(&*storage.namespace()?, &StoragePath::root()).await?;
    let observed = Arc::new(observation(children.remove(0))?);
    let history = observed.history().ok_or("missing history")?;
    let null = history
        .records()
        .iter()
        .position(|record| record.version_id().is_none())
        .ok_or("missing null")?;
    let marker = history
        .records()
        .iter()
        .position(VersionRecord::is_delete_marker)
        .ok_or("missing marker")?;
    let destination = s3::connect(protocol.clone(), endpoint_of(&protocol), None)?;
    for invalid in [marker, history.records().len()] {
        assert!(
            TransferRequest::from_shared_observation(
                storage.clone(),
                Arc::clone(&observed),
                Some(invalid),
                destination.clone(),
                CancellationToken::new()
            )
            .is_err()
        );
    }
    let request = TransferRequest::from_shared_observation(
        storage,
        observed,
        Some(null),
        destination,
        CancellationToken::new(),
    )?;
    protocol
        .put_version(
            "src/source",
            "null",
            Bytes::from_static(b"changed-null-content"),
        )
        .await;
    let failure = transfer(request)
        .await
        .err()
        .ok_or("overwritten null was accepted")?;
    assert_eq!(failure.phase(), TransferPhase::Describe);
    assert!(!protocol.objects.lock().await.contains_key("source"));
    Ok(())
}

#[tokio::test]
async fn shared_expert_source_pins_old_versions_and_bounds_chunks_without_cloning_history()
-> TestResult {
    let (_, storage, observed) = bucket(true).await?;
    let observed = Arc::new(observed);
    let index = observed
        .history()
        .ok_or("history")?
        .records()
        .iter()
        .position(|record| record.version_id() == Some("v1"))
        .ok_or("v1")?;
    let expected_key = observed.history().ok_or("history")?.records()[index]
        .identity()
        .identity_key();
    let request = ExpertSourceRequest::from_shared_observation(
        storage,
        Arc::clone(&observed),
        Some(index),
        CancellationToken::new(),
    )?
    .with_maximum_chunk_bytes(NonZeroUsize::new(3).ok_or("nonzero")?);
    let cloned = request.clone();
    assert_eq!(Arc::strong_count(&observed), 3);
    drop(cloned);
    let session = ExpertSourceSession::open(request).await?;
    assert_eq!(session.offer().identity_key, expected_key);
    assert_eq!(session.offer().maximum_chunk_bytes, 3);
    assert_eq!(Arc::strong_count(&observed), 1);
    let mut payload = session.stream_from(0)?;
    let mut bytes = Vec::new();
    while let Some(chunk) = payload.next_chunk().await? {
        assert!(chunk.len() <= 3);
        bytes.extend_from_slice(&chunk);
    }
    let evidence = payload.finish().await?;
    assert_eq!(bytes, b"first!");
    assert_eq!(evidence.blake3, *blake3::hash(&bytes).as_bytes());
    Ok(())
}

#[tokio::test]
async fn shared_expert_source_rejects_replaced_empty_objects_before_producing() -> TestResult {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("empty"), [])?;
    let (storage, _) = local::test_source_storage(root.path(), &root.path().to_string_lossy())?;
    let path = StoragePath::new("empty")?;
    let observed = Arc::new(observation(storage.read_source()?.describe(&path).await?)?);
    let request = ExpertSourceRequest::from_shared_observation(
        storage,
        observed,
        None,
        CancellationToken::new(),
    )?;
    std::fs::write(root.path().join("replacement"), [])?;
    std::fs::rename(root.path().join("replacement"), root.path().join("empty"))?;
    let failure = ExpertSourceSession::open(request)
        .await
        .err()
        .ok_or("stale empty observation was admitted")?;
    assert_eq!(failure.phase(), TransferPhase::Describe);
    Ok(())
}

async fn scanned(storage: &Storage, path: &str) -> TestResult<Arc<ObservedEntry>> {
    let mut scan =
        StorageTraversalSource::new(storage)?.traverse(TraversalRequest::new(StoragePath::root()));
    let mut found = None;
    while let Some(item) = scan.next_item().await {
        if let TraversalItem::Entry(entry) = item
            && entry.path().as_str() == path
        {
            found = Some(Arc::from(entry));
        }
    }
    scan.finish().await?;
    found.ok_or_else(|| "missing native scan observation".into())
}

#[tokio::test]
async fn shared_expert_source_reads_marker_leaf_and_selected_directory_history_at_the_real_key()
-> TestResult {
    for versioned in [false, true] {
        let protocol = Arc::new(MemoryS3::default());
        if versioned {
            protocol.set_versioning(Versioning::Enabled);
        }
        protocol.put_version("src/p/", "old", Bytes::new()).await;
        if versioned {
            protocol.put_version("src/p/", "new", Bytes::new()).await;
        }
        protocol
            .put_version("src/p/child", "child", Bytes::from_static(b"child"))
            .await;
        let storage = source_storage(&protocol, "src")?;
        let observed = scanned(&storage, "p").await?;
        assert_eq!(observed.kind(), EntryKind::Directory);
        let index = observed.history().and_then(|history| {
            history
                .records()
                .iter()
                .position(|record| record.version_id() == Some("old"))
        });
        if versioned {
            assert!(index.is_some());
        } else {
            assert!(observed.directory_marker_object().is_some());
        }
        let session = ExpertSourceSession::open(ExpertSourceRequest::from_shared_observation(
            storage,
            observed,
            index,
            CancellationToken::new(),
        )?)
        .await?;
        assert_eq!(session.offer().source_size, 0);
        let mut payload = session.stream_from(0)?;
        assert!(payload.next_chunk().await?.is_none());
        let evidence = payload.finish().await?;
        assert_eq!(evidence.blake3, *blake3::hash(&[]).as_bytes());
    }
    Ok(())
}

#[tokio::test]
async fn shared_expert_source_refuses_delete_marker_and_out_of_range_selection() -> TestResult {
    let (protocol, storage, _) = bucket(true).await?;
    protocol.put_delete_marker("src/source", "deleted").await;
    let observed = scanned(&storage, "source").await?;
    let records = observed.history().ok_or("history")?.records();
    let marker = records
        .iter()
        .position(VersionRecord::is_delete_marker)
        .ok_or("delete marker")?;
    for index in [marker, records.len()] {
        assert!(
            ExpertSourceRequest::from_shared_observation(
                storage.clone(),
                Arc::clone(&observed),
                Some(index),
                CancellationToken::new()
            )
            .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn expert_source_refuses_mutable_empty_s3_object_when_head_time_changes() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .put_version("src/empty", "null", Bytes::new())
        .await;
    let before = StorageTimestamp::new(1_000_000_000, TimePrecision::Seconds)?;
    *protocol.last_modified.lock().await = Some(before);
    let storage = source_storage(&protocol, "src")?;
    let descriptor = storage
        .read_source()?
        .describe(&StoragePath::new("empty")?)
        .await?;
    assert_eq!(
        descriptor
            .inline_timestamps
            .and_then(|times| times.modified),
        Some(before)
    );
    let observed = Arc::new(observation(descriptor)?);
    let request = ExpertSourceRequest::from_shared_observation(
        storage,
        observed,
        None,
        CancellationToken::new(),
    )?;
    *protocol.last_modified.lock().await = Some(StorageTimestamp::new(
        2_000_000_000,
        TimePrecision::Seconds,
    )?);
    let failure = ExpertSourceSession::open(request)
        .await
        .err()
        .ok_or("changed mutable empty object was admitted")?;
    assert_eq!(failure.phase(), TransferPhase::Describe);
    Ok(())
}

#[tokio::test]
async fn expert_source_compares_modification_times_at_the_coarser_declared_precision() -> TestResult
{
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .put_version("src/empty", "null", Bytes::new())
        .await;
    *protocol.last_modified.lock().await = Some(StorageTimestamp::new(
        1_000_000_000,
        TimePrecision::Seconds,
    )?);
    let storage = source_storage(&protocol, "src")?;
    let descriptor = storage
        .read_source()?
        .describe(&StoragePath::new("empty")?)
        .await?;
    let observed = Arc::new(ObservedEntry::new(
        descriptor.path,
        descriptor.kind,
        descriptor.size,
        Some(StorageTimestamp::new(
            1_250_000_000,
            TimePrecision::Milliseconds,
        )?),
        descriptor.source_identity,
    )?);
    let request = ExpertSourceRequest::from_shared_observation(
        storage,
        observed,
        None,
        CancellationToken::new(),
    )?;
    ExpertSourceSession::open(request.clone()).await?;
    *protocol.last_modified.lock().await = Some(StorageTimestamp::new(
        2_000_000_000,
        TimePrecision::Seconds,
    )?);
    assert!(ExpertSourceSession::open(request).await.is_err());
    Ok(())
}
