//! Scanned S3 markers use the caller's directory workflow locally and retain raw keys on S3.

use std::error::Error;
use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::model::{EntryKind, ObjectHistory, ObservedEntry, StoragePath};
use crate::storage::backends::s3::tests::{
    MemoryS3, Versioning, content_md5, endpoint_of, native_context,
};
use crate::storage::backends::s3::{S3Protocol as _, connect};
use crate::storage::{
    BackendConfig, LocalBackendConfig, Storage, connect_backend, create_directory_all,
};
use crate::transfer::{
    DIRECTORY_OBJECT_SKIP_REASON, DirectoryObjectAction, TransferRequest, directory_object_action,
    transfer,
};
use crate::traversal::{
    StorageTraversalSource, TraversalItem, TraversalOrder, TraversalOutcome, TraversalRequest,
    TraversalSource,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn bucket(protocol: &Arc<MemoryS3>) -> TestResult<Storage> {
    connect(
        protocol.clone(),
        endpoint_of(protocol),
        Some(native_context()),
    )
}

async fn put(protocol: &MemoryS3, key: &str, body: &'static [u8]) -> TestResult {
    let bytes = Bytes::from_static(body);
    protocol
        .put_object(key, bytes.clone().into(), &content_md5(&bytes))
        .await
        .map_err(|e| format!("{e:?}"))?;
    Ok(())
}

async fn local(root: &Path) -> TestResult<Storage> {
    Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: root.into(),
        rename_context: None,
        read_concurrency: NonZeroUsize::MIN,
        write_concurrency: NonZeroUsize::MIN,
        read_chunk_bytes: None,
        write_chunk_bytes: None,
    }))
    .await?)
}

async fn scan(storage: &Storage, order: TraversalOrder) -> TestResult<Vec<ObservedEntry>> {
    let request = TraversalRequest::new(StoragePath::root()).order(order);
    let mut session = StorageTraversalSource::new(storage)?.traverse(request);
    let mut entries = Vec::new();
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => entries.push(*entry),
            TraversalItem::Failure(failure) => return Err(format!("{failure:?}").into()),
            _ => {}
        }
    }
    assert!(matches!(
        session.finish().await?,
        TraversalOutcome::Completed(_)
    ));
    Ok(entries)
}

async fn marker_source() -> TestResult<Storage> {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    for (key, body) in [
        ("empty/", &b""[..]),
        ("empty/", &b""[..]),
        ("data/", &b"content"[..]),
        ("data/", &b""[..]),
        ("deleted/", &b""[..]),
    ] {
        put(&protocol, key, body).await?;
    }
    protocol
        .delete_object("deleted/")
        .await
        .map_err(|e| format!("{e:?}"))?;
    bucket(&protocol)
}

#[tokio::test]
async fn scanned_markers_create_empty_local_directories_and_content_is_skipped() -> TestResult {
    let source = marker_source().await?;
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let root = tempfile::tempdir()?;
        let destination = local(root.path()).await?;
        let entries = scan(&source, order).await?;
        assert_eq!(entries.len(), 3, "{entries:?}");
        for entry in entries {
            let (kind, expected) = match entry.path().as_str() {
                "empty" => (EntryKind::Directory, DirectoryObjectAction::Copy),
                "deleted" => (EntryKind::Directory, DirectoryObjectAction::NothingToDo),
                "data/" => (
                    EntryKind::File,
                    DirectoryObjectAction::Skip(DIRECTORY_OBJECT_SKIP_REASON),
                ),
                path => return Err(format!("unexpected scanned path: {path}").into()),
            };
            assert_eq!(entry.kind(), kind);
            assert!(
                entry.history().is_some(),
                "marker history must survive scanning"
            );
            assert_eq!(directory_object_action(&entry, &destination), expected);
            if expected == DirectoryObjectAction::Copy {
                create_directory_all(&destination, entry.path()).await?;
            }
        }
        assert!(root.path().join("empty").is_dir());
        assert_eq!(fs::read_dir(root.path().join("empty"))?.count(), 0);
        assert!(!root.path().join("data").exists());
        assert!(!root.path().join("deleted").exists());
        assert_eq!(fs::read_dir(root.path())?.count(), 1);
    }
    Ok(())
}

/// An unversioned S3 destination keeps the current zero-byte object at the exact source key.
#[tokio::test]
async fn mixed_history_copies_its_empty_current_object_to_unversioned_s3() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "data/", b"content").await?;
    put(&protocol, "data/", b"").await?;
    let source = bucket(&protocol)?;
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let target = Arc::new(MemoryS3::default());
        target.set_versioning(Versioning::Off);
        let destination = connect(target.clone(), endpoint_of(&target), None)?;
        let mut entries = scan(&source, order).await?;
        assert_eq!(entries.len(), 1);
        let entry = entries.pop().ok_or("source content object missing")?;
        assert_eq!(entry.kind(), EntryKind::File);
        assert_eq!(entry.path().as_str(), "data/");
        assert_eq!(entry.size(), Some(0));
        assert_eq!(
            entry.history().and_then(ObjectHistory::largest_version),
            Some(7)
        );
        assert_eq!(
            directory_object_action(&entry, &destination),
            DirectoryObjectAction::Copy
        );
        transfer(TransferRequest::from_observation(
            source.clone(),
            entry,
            destination.clone(),
            CancellationToken::new(),
        )?)
        .await?;
        {
            let objects = target.objects.lock().await;
            assert_eq!(objects.len(), 1);
            assert_eq!(objects.get("data/").map(Bytes::len), Some(0));
        }
        let rescanned = scan(&destination, order).await?;
        assert_eq!(rescanned.len(), 1);
        assert_eq!(rescanned[0].kind(), EntryKind::Directory);
        assert_eq!(rescanned[0].path().as_str(), "data");
        assert!(rescanned[0].history().is_none());
    }
    Ok(())
}

#[tokio::test]
async fn a_child_copy_creates_its_parent_after_the_content_object_is_skipped() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "parent/", b"directory content").await?;
    put(&protocol, "parent/", b"").await?;
    put(&protocol, "parent/child", b"child bytes").await?;
    let source = bucket(&protocol)?;
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let root = tempfile::tempdir()?;
        let destination = local(root.path()).await?;
        let entries = scan(&source, order).await?;
        let content = entries
            .iter()
            .find(|entry| entry.path().as_str() == "parent/")
            .ok_or("content directory object missing from scan")?;
        assert_eq!(content.kind(), EntryKind::File);
        assert_eq!(
            directory_object_action(content, &destination),
            DirectoryObjectAction::Skip(DIRECTORY_OBJECT_SKIP_REASON)
        );
        assert!(!root.path().join("parent").exists());
        let child = entries
            .into_iter()
            .find(|entry| entry.path().as_str() == "parent/child")
            .ok_or("child missing from scan")?;
        assert_eq!(
            directory_object_action(&child, &destination),
            DirectoryObjectAction::Copy
        );
        // Deliberately leave the scanned parent Directory uncreated: normal file transfer
        // must create its own missing parents, independently of the skipped content object.
        transfer(TransferRequest::from_observation(
            source.clone(),
            child,
            destination,
            CancellationToken::new(),
        )?)
        .await?;
        assert!(root.path().join("parent").is_dir());
        assert_eq!(fs::read(root.path().join("parent/child"))?, b"child bytes");
        assert_eq!(fs::read_dir(root.path().join("parent"))?.count(), 1);
    }
    Ok(())
}

async fn unversioned_marker_source() -> TestResult<Storage> {
    let protocol = Arc::new(MemoryS3::default());
    put(&protocol, "empty/", b"").await?;
    put(&protocol, "virtual/child", b"child bytes").await?;
    bucket(&protocol)
}

#[tokio::test]
async fn unversioned_marker_survives_snapshot_and_copies_its_exact_s3_key() -> TestResult {
    let source = unversioned_marker_source().await?;
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let entries = scan(&source, order).await?;
        let entry = entries
            .iter()
            .find(|entry| entry.path().as_str() == "empty")
            .ok_or("marker directory missing from scan")?;
        assert_eq!(entry.kind(), EntryKind::Directory);
        assert!(entry.history().is_none());
        let marker = entry
            .directory_marker_object()
            .ok_or("raw marker missing")?;
        assert_eq!(marker.kind(), EntryKind::File);
        assert_eq!(marker.path().as_str(), "empty/");
        assert_eq!(marker.size(), Some(0));
        assert!(marker.history().is_none());
        let restored =
            ObservedEntry::decode_snapshot(entry.encode_snapshot().as_bytes(), source.identity())?;
        assert_eq!(&restored, entry);
        assert_eq!(restored.directory_marker_object(), Some(marker));
        let target = Arc::new(MemoryS3::default());
        let destination = connect(target.clone(), endpoint_of(&target), None)?;
        assert_eq!(
            directory_object_action(&restored, &destination),
            DirectoryObjectAction::Copy
        );
        let copy = TransferRequest::from_observation(
            source.clone(),
            restored,
            destination,
            CancellationToken::new(),
        )?;
        assert_eq!(copy.source_path().as_str(), "empty/");
        transfer(copy).await?;
        let objects = target.objects.lock().await;
        assert_eq!(objects.len(), 1);
        assert_eq!(objects.get("empty/").map(Bytes::len), Some(0));
        assert!(!objects.contains_key("empty"));
    }
    Ok(())
}

#[tokio::test]
async fn virtual_directory_is_not_synthesized_into_an_s3_marker() -> TestResult {
    let source = unversioned_marker_source().await?;
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let entries = scan(&source, order).await?;
        let directory = entries
            .iter()
            .find(|entry| entry.path().as_str() == "virtual")
            .ok_or("virtual directory missing from scan")?;
        assert_eq!(directory.kind(), EntryKind::Directory);
        assert!(directory.directory_marker_object().is_none());
        let restored = ObservedEntry::decode_snapshot(
            directory.encode_snapshot().as_bytes(),
            source.identity(),
        )?;
        assert_eq!(&restored, directory);
        let target = Arc::new(MemoryS3::default());
        let destination = connect(target.clone(), endpoint_of(&target), None)?;
        assert!(
            TransferRequest::from_observation(
                source.clone(),
                restored,
                destination.clone(),
                CancellationToken::new(),
            )
            .is_err()
        );
        assert!(target.objects.lock().await.is_empty());
        let child = entries
            .into_iter()
            .find(|entry| entry.path().as_str() == "virtual/child")
            .ok_or("virtual directory child missing from scan")?;
        transfer(TransferRequest::from_observation(
            source.clone(),
            child,
            destination,
            CancellationToken::new(),
        )?)
        .await?;
        let objects = target.objects.lock().await;
        assert_eq!(objects.len(), 1);
        assert_eq!(
            objects.get("virtual/child").map(AsRef::as_ref),
            Some(&b"child bytes"[..])
        );
        assert!(!objects.contains_key("virtual/"));
    }
    Ok(())
}
