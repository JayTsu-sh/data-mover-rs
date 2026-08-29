use std::io;
use std::path::{Path, PathBuf};

use futures::StreamExt as _;

use super::*;
use crate::model::{BackendIdentity, BackendKind, StoragePath};
use crate::storage::{ReadRequest, ReadSource};

#[tokio::test]
async fn role_describes_and_reads_an_exact_local_range() -> Result<(), Box<dyn std::error::Error>> {
    let root = TestRoot::new()?;
    std::fs::write(root.path().join("source.bin"), b"0123456789")?;
    let identity = BackendIdentity::new(BackendKind::Local, "source-root")?;
    let source = LocalReadSource::new(root.path(), identity)?;
    let path = StoragePath::new("source.bin")?;

    let descriptor = source.describe(&path).await?;
    assert_eq!(descriptor.size, Some(10));
    let mut stream = source
        .read(ReadRequest {
            path,
            range: Some(3..8),
            expected_source: None,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
        })
        .await?;

    assert_eq!(
        stream.next().await.transpose()?.as_deref(),
        Some(&b"34567"[..])
    );
    assert!(stream.next().await.is_none());
    Ok(())
}

#[tokio::test]
async fn role_splits_large_ranges_into_bounded_chunks() -> Result<(), Box<dyn std::error::Error>> {
    let root = TestRoot::new()?;
    let payload = vec![0x6b; 2 * 1024 * 1024 + 17];
    std::fs::write(root.path().join("large.bin"), &payload)?;
    let identity = BackendIdentity::new(BackendKind::Local, "large-source-root")?;
    let source = LocalReadSource::new(root.path(), identity)?;
    let mut stream = source
        .read(ReadRequest {
            path: StoragePath::new("large.bin")?,
            range: None,
            expected_source: None,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
        })
        .await?;
    let mut rebuilt = Vec::new();
    let mut chunks = 0;

    while let Some(bytes) = stream.next().await.transpose()? {
        assert!(bytes.len() <= 1024 * 1024);
        rebuilt.extend_from_slice(&bytes);
        chunks += 1;
    }
    assert_eq!(chunks, 3);
    assert_eq!(rebuilt, payload);
    Ok(())
}

#[tokio::test]
async fn one_stream_remains_bound_to_the_file_opened_at_read_start()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TestRoot::new()?;
    let original = vec![0x41; 2 * 1024 * 1024];
    let replacement = vec![0x42; original.len()];
    let path = root.path().join("source.bin");
    std::fs::write(&path, &original)?;
    let identity = BackendIdentity::new(BackendKind::Local, "stable-open-root")?;
    let source = LocalReadSource::new(root.path(), identity)?;
    let mut stream = source
        .read(ReadRequest {
            path: StoragePath::new("source.bin")?,
            range: None,
            expected_source: None,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
        })
        .await?;
    let first = stream
        .next()
        .await
        .transpose()?
        .ok_or("missing first chunk")?;
    std::fs::rename(&path, root.path().join("original.bin"))?;
    std::fs::write(&path, replacement)?;
    let mut rebuilt = first.to_vec();

    while let Some(bytes) = stream.next().await.transpose()? {
        rebuilt.extend_from_slice(&bytes);
    }
    assert_eq!(rebuilt, original);
    Ok(())
}

#[tokio::test]
async fn expected_identity_rejects_replacement_between_describe_and_read()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TestRoot::new()?;
    let path = root.path().join("source.bin");
    std::fs::write(&path, b"original")?;
    let identity = BackendIdentity::new(BackendKind::Local, "identity-bound-root")?;
    let source = LocalReadSource::new(root.path(), identity)?;
    let storage_path = StoragePath::new("source.bin")?;
    let descriptor = source.describe(&storage_path).await?;
    std::fs::remove_file(&path)?;
    std::fs::write(&path, b"replaced")?;

    let result = source
        .read(ReadRequest {
            path: storage_path,
            range: None,
            expected_source: Some(descriptor.source_identity),
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
        })
        .await;

    assert!(matches!(
        result,
        Err(StorageRoleFailure::Entry(error))
            if error.class() == FailureClass::Conflict
                && error.transience() == Transience::Permanent
    ));
    Ok(())
}

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> io::Result<Self> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("data-mover-source-{}-{id}", std::process::id()));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
