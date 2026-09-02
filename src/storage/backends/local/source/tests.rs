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
    let source = LocalReadSource::new(root.path(), identity, 4)?;
    let path = StoragePath::new("source.bin")?;

    let descriptor = source.describe(&path).await?;
    assert_eq!(descriptor.size, Some(10));
    let mut stream = source
        .read(ReadRequest {
            path,
            range: Some(3..8),
            expected_source: None,
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
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
    let source = LocalReadSource::new(root.path(), identity, 4)?;
    let mut stream = source
        .read(ReadRequest {
            path: StoragePath::new("large.bin")?,
            range: None,
            expected_source: None,
            maximum_chunk_bytes: 4 * 1024 * 1024,
            read_inflight: 4,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
        })
        .await?;
    let mut rebuilt = Vec::new();
    let mut chunk_lengths = Vec::new();

    while let Some(bytes) = stream.next().await.transpose()? {
        assert!(bytes.len() <= LOCAL_MAX_READ_CHUNK_BYTES);
        chunk_lengths.push(bytes.len());
        rebuilt.extend_from_slice(&bytes);
    }
    assert_eq!(chunk_lengths, [LOCAL_MAX_READ_CHUNK_BYTES, 17]);
    assert_eq!(rebuilt, payload);
    Ok(())
}

#[tokio::test]
async fn caller_chunk_ceiling_negotiates_below_local_maximum()
-> Result<(), Box<dyn std::error::Error>> {
    const REQUESTED_MAXIMUM: usize = 64 * 1024;
    let root = TestRoot::new()?;
    let payload = vec![0x2c; 3 * REQUESTED_MAXIMUM + 17];
    std::fs::write(root.path().join("negotiated.bin"), &payload)?;
    let identity = BackendIdentity::new(BackendKind::Local, "negotiated-source-root")?;
    let source = LocalReadSource::new(root.path(), identity, 4)?;
    let mut stream = source
        .read(ReadRequest {
            path: StoragePath::new("negotiated.bin")?,
            range: None,
            expected_source: None,
            maximum_chunk_bytes: REQUESTED_MAXIMUM,
            read_inflight: 2,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
        })
        .await?;
    let mut rebuilt = Vec::new();
    while let Some(chunk) = stream.next().await.transpose()? {
        assert!(chunk.len() <= REQUESTED_MAXIMUM);
        rebuilt.extend_from_slice(&chunk);
    }

    assert_eq!(rebuilt, payload);
    assert!(source.peak_read_concurrency() <= 2);
    assert_eq!(source.read_stream_count(), 1);
    Ok(())
}

#[tokio::test]
async fn one_stream_reads_inflight_but_emits_in_admission_order()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TestRoot::new()?;
    let mut payload = Vec::with_capacity(3 * 1024 * 1024);
    payload.extend(std::iter::repeat_n(0x11, 1024 * 1024));
    payload.extend(std::iter::repeat_n(0x22, 1024 * 1024));
    payload.extend(std::iter::repeat_n(0x33, 1024 * 1024));
    std::fs::write(root.path().join("inflight.bin"), &payload)?;
    let identity = BackendIdentity::new(BackendKind::Local, "inflight-source-root")?;
    let source = LocalReadSource::new(root.path(), identity, 3)?;
    let gate = source.gate_read_at(0);
    let mut stream = source
        .read(ReadRequest {
            path: StoragePath::new("inflight.bin")?,
            range: None,
            expected_source: None,
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
        })
        .await?;
    let reading = tokio::spawn(async move {
        let first = stream.next().await;
        (first, stream)
    });
    gate.wait_started().await;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    while source.read_completion_order().len() < 2 {
        if tokio::time::Instant::now() >= deadline {
            return Err("later inflight reads did not complete".into());
        }
        tokio::task::yield_now().await;
    }
    gate.release();
    let (first, mut stream) = reading.await?;
    let mut chunks = Vec::new();
    if let Some(bytes) = first.transpose()? {
        chunks.push(bytes);
    }

    while let Some(bytes) = stream.next().await.transpose()? {
        chunks.push(bytes);
    }

    assert_eq!(source.peak_read_concurrency(), 3);
    assert_eq!(source.read_call_count(), 3);
    let completion_order = source.read_completion_order();
    assert_eq!(completion_order.len(), 3);
    assert_ne!(completion_order[0], 0);
    assert_eq!(chunks.len(), 3);
    assert!(chunks[0].iter().all(|byte| *byte == 0x11));
    assert!(chunks[1].iter().all(|byte| *byte == 0x22));
    assert!(chunks[2].iter().all(|byte| *byte == 0x33));
    Ok(())
}

#[tokio::test]
async fn inflight_stream_fast_fails_when_a_later_range_becomes_short()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TestRoot::new()?;
    let file_path = root.path().join("short.bin");
    std::fs::write(&file_path, vec![0x4d; 2 * 1024 * 1024])?;
    let identity = BackendIdentity::new(BackendKind::Local, "short-read-root")?;
    let source = LocalReadSource::new(root.path(), identity, 2)?;
    let gate = source.gate_read_at(1024 * 1024);
    let mut stream = source
        .read(ReadRequest {
            path: StoragePath::new("short.bin")?,
            range: None,
            expected_source: None,
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
        })
        .await?;

    assert_eq!(
        stream.next().await.transpose()?.map(|bytes| bytes.len()),
        Some(1024 * 1024)
    );
    gate.wait_started().await;
    std::fs::OpenOptions::new()
        .write(true)
        .open(file_path)?
        .set_len(1024 * 1024)?;
    gate.release();
    let result = stream.next().await;

    assert!(matches!(
        result,
        Some(Err(StorageRoleFailure::Entry(error)))
            if error.class() == FailureClass::Corruption
                && error.transience() == Transience::Unknown
    ));
    Ok(())
}

#[tokio::test]
async fn cancellation_stops_an_active_inflight_stream_without_waiting_for_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TestRoot::new()?;
    std::fs::write(root.path().join("cancel.bin"), vec![0x6a; 3 * 1024 * 1024])?;
    let identity = BackendIdentity::new(BackendKind::Local, "cancel-read-root")?;
    let source = LocalReadSource::new(root.path(), identity, 3)?;
    source.delay_reads(std::time::Duration::from_secs(1));
    let cancel = tokio_util::sync::CancellationToken::new();
    let mut stream = source
        .read(ReadRequest {
            path: StoragePath::new("cancel.bin")?,
            range: None,
            expected_source: None,
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            cancel: cancel.clone(),
            source_qos: None,
        })
        .await?;
    let reading = tokio::spawn(async move { stream.next().await });
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    while source.read_call_count() < 3 {
        if tokio::time::Instant::now() >= deadline {
            return Err("inflight reads did not start".into());
        }
        tokio::task::yield_now().await;
    }

    let started = std::time::Instant::now();
    cancel.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_millis(200), reading).await??;

    assert!(started.elapsed() < std::time::Duration::from_millis(200));
    assert!(matches!(
        result,
        Some(Err(StorageRoleFailure::Entry(error)))
            if error.class() == FailureClass::Cancelled
                && error.transience() == Transience::Transient
    ));
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
    let source = LocalReadSource::new(root.path(), identity, 4)?;
    let mut stream = source
        .read(ReadRequest {
            path: StoragePath::new("source.bin")?,
            range: None,
            expected_source: None,
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
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
    let source = LocalReadSource::new(root.path(), identity, 4)?;
    let storage_path = StoragePath::new("source.bin")?;
    let descriptor = source.describe(&storage_path).await?;
    std::fs::remove_file(&path)?;
    std::fs::write(&path, b"replaced")?;

    let result = source
        .read(ReadRequest {
            path: storage_path,
            range: None,
            expected_source: Some(descriptor.source_identity),
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
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
