use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::model::{BackendKind, FailureClass};

struct FakeProtocol {
    payload: Bytes,
    handle: Bytes,
    maximum_read_chunk_bytes: usize,
    described: AtomicUsize,
    opened: Mutex<usize>,
    active_reads: Arc<AtomicUsize>,
    maximum_active_reads: Arc<AtomicUsize>,
}

struct FakeCursor {
    payload: Bytes,
    active_reads: Arc<AtomicUsize>,
    maximum_active_reads: Arc<AtomicUsize>,
}

#[async_trait]
impl NfsReadCursor for FakeCursor {
    async fn read_at(&self, offset: u64, count: usize) -> Result<Bytes, NfsProtocolFailure> {
        let active = self.active_reads.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum_active_reads
            .fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let start = usize::try_from(offset).map_err(|_| NfsProtocolFailure::protocol())?;
        let result = self.payload.slice(start..start + count);
        self.active_reads.fetch_sub(1, Ordering::SeqCst);
        Ok(result)
    }
}

#[async_trait]
impl NfsSourceProtocol for FakeProtocol {
    fn maximum_read_chunk_bytes(&self) -> usize {
        self.maximum_read_chunk_bytes
    }

    async fn describe(
        &self,
        _path: &StoragePath,
    ) -> Result<NfsSourceObservation, NfsProtocolFailure> {
        self.described.fetch_add(1, Ordering::SeqCst);
        Ok(NfsSourceObservation {
            kind: EntryKind::File,
            size: Some(self.payload.len() as u64),
            file_handle: self.handle.clone(),
            content_version: Bytes::from_static(b"version-1"),
        })
    }

    async fn open(
        &self,
        _path: &StoragePath,
    ) -> Result<(Box<dyn NfsReadCursor>, Bytes), NfsProtocolFailure> {
        *self
            .opened
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
        Ok((
            Box::new(FakeCursor {
                payload: self.payload.clone(),
                active_reads: Arc::clone(&self.active_reads),
                maximum_active_reads: Arc::clone(&self.maximum_active_reads),
            }),
            self.handle.clone(),
        ))
    }
}

fn adapter(protocol: Arc<FakeProtocol>) -> NfsReadSourceAdapter {
    NfsReadSourceAdapter::with_protocol(
        protocol,
        BackendIdentity::new(BackendKind::Nfs, "test-export")
            .unwrap_or_else(|error| panic!("{error}")),
    )
}

#[tokio::test]
async fn one_stream_uses_one_open_cursor_and_exact_ranges() -> Result<(), Box<dyn std::error::Error>>
{
    let protocol = Arc::new(FakeProtocol {
        payload: Bytes::from(vec![7; 2 * 1024 * 1024 + 3]),
        handle: Bytes::from_static(b"stable-file-handle"),
        maximum_read_chunk_bytes: 1024 * 1024,
        described: AtomicUsize::new(0),
        opened: Mutex::new(0),
        active_reads: Arc::new(AtomicUsize::new(0)),
        maximum_active_reads: Arc::new(AtomicUsize::new(0)),
    });
    let source = adapter(Arc::clone(&protocol));
    let path = StoragePath::new("large.bin")?;
    let descriptor = source.describe(&path).await?;
    let mut stream = source
        .read(ReadRequest {
            path,
            range: Some(1..1_500_001),
            expected_source: Some(descriptor.source_identity),
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            read_budget: None,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
        })
        .await?;
    let mut total = 0;
    while let Some(chunk) = stream.next().await.transpose()? {
        assert!(chunk.len() <= MAX_ROLE_READ);
        total += chunk.len();
    }
    assert_eq!(total, 1_500_000);
    assert_eq!(
        protocol.described.load(Ordering::SeqCst),
        1,
        "an explicit range already carries the size boundary and must not re-describe"
    );
    assert_eq!(
        *protocol
            .opened
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        1
    );
    Ok(())
}

#[tokio::test]
async fn shared_budget_bounds_nfs_prefetch_without_serializing_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(FakeProtocol {
        payload: Bytes::from(vec![7; 4 * 1024 * 1024]),
        handle: Bytes::from_static(b"stable-file-handle"),
        maximum_read_chunk_bytes: 1024 * 1024,
        described: AtomicUsize::new(0),
        opened: Mutex::new(0),
        active_reads: Arc::new(AtomicUsize::new(0)),
        maximum_active_reads: Arc::new(AtomicUsize::new(0)),
    });
    let source = adapter(Arc::clone(&protocol));
    let path = StoragePath::new("large.bin")?;
    let descriptor = source.describe(&path).await?;
    let cancel = tokio_util::sync::CancellationToken::new();
    let (runtime, mut ordered) = crate::storage::InflightRuntime::channel(
        crate::storage::InflightConfig::new(128, 2 * 1024 * 1024, 128)?,
        0,
        protocol.payload.len() as u64,
        cancel.clone(),
    )?;
    let budget = crate::storage::ReadBudget::new(runtime.clone());
    let mut stream = source
        .read(ReadRequest {
            path,
            range: None,
            expected_source: Some(descriptor.source_identity),
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 128,
            read_budget: Some(budget.clone()),
            cancel,
            source_qos: None,
        })
        .await?;
    let mut offset = 0;
    while let Some(bytes) = stream.next().await.transpose()? {
        let length = bytes.len() as u64;
        runtime
            .complete_read(
                budget.take(offset).ok_or("missing reservation")?,
                offset,
                bytes,
            )
            .await?;
        assert!(ordered.next().await.transpose()?.is_some());
        offset += length;
    }
    assert_eq!(offset, protocol.payload.len() as u64);
    assert_eq!(protocol.maximum_active_reads.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn opened_identity_detects_change_and_precancel_avoids_another_open()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(FakeProtocol {
        payload: Bytes::from_static(b"payload"),
        handle: Bytes::from_static(b"current-handle"),
        maximum_read_chunk_bytes: 1024 * 1024,
        described: AtomicUsize::new(0),
        opened: Mutex::new(0),
        active_reads: Arc::new(AtomicUsize::new(0)),
        maximum_active_reads: Arc::new(AtomicUsize::new(0)),
    });
    let source = adapter(Arc::clone(&protocol));
    let path = StoragePath::new("file")?;
    let other = SourceIdentity::new(
        BackendIdentity::new(BackendKind::Nfs, "test-export")?,
        IdentityStrength::StableWithinBackend,
        b"old-handle",
    )?;
    let changed = source
        .read(ReadRequest {
            path: path.clone(),
            range: None,
            expected_source: Some(other),
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            read_budget: None,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
        })
        .await;
    assert!(
        matches!(changed, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Conflict)
    );

    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    let cancelled = source
        .read(ReadRequest {
            path,
            range: None,
            expected_source: None,
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            read_budget: None,
            cancel,
            source_qos: None,
        })
        .await;
    assert!(
        matches!(cancelled, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Cancelled)
    );
    assert_eq!(
        *protocol
            .opened
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        1
    );
    Ok(())
}

#[tokio::test]
async fn negotiated_protocol_limit_prevents_internal_read_resplitting()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(FakeProtocol {
        payload: Bytes::from_static(b"abcdefgh"),
        handle: Bytes::from_static(b"stable-file-handle"),
        maximum_read_chunk_bytes: 3,
        described: AtomicUsize::new(0),
        opened: Mutex::new(0),
        active_reads: Arc::new(AtomicUsize::new(0)),
        maximum_active_reads: Arc::new(AtomicUsize::new(0)),
    });
    let source = adapter(protocol);
    assert_eq!(source.maximum_read_chunk_bytes(), 3);
    let path = StoragePath::new("file")?;
    let descriptor = source.describe(&path).await?;
    let mut stream = source
        .read(ReadRequest {
            path,
            range: None,
            expected_source: Some(descriptor.source_identity),
            maximum_chunk_bytes: 8,
            read_inflight: 4,
            read_budget: None,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
        })
        .await?;
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next().await.transpose()? {
        chunks.push(chunk.len());
    }
    assert_eq!(chunks, vec![3, 3, 2]);
    Ok(())
}

#[test]
fn connectivity_is_session_scoped_not_entry_scoped() -> Result<(), Box<dyn std::error::Error>> {
    let failure = role_failure(
        &StoragePath::new("file")?,
        crate::model::Operation::Read,
        NfsProtocolFailure::new(
            FailureClass::Connectivity,
            crate::model::Transience::Unknown,
        ),
    );
    assert!(matches!(failure, StorageRoleFailure::Session(_)));
    Ok(())
}

struct DelayedPrefixCursor;
#[async_trait]
impl NfsReadCursor for DelayedPrefixCursor {
    async fn read_at(&self, offset: u64, count: usize) -> Result<Bytes, NfsProtocolFailure> {
        tokio::time::sleep(std::time::Duration::from_millis(if offset == 0 {
            25
        } else {
            1
        }))
        .await;
        let offset = usize::try_from(offset).map_err(|_| {
            NfsProtocolFailure::new(FailureClass::InvalidInput, Transience::Permanent)
        })?;
        Ok(Bytes::from_static(b"abcdefghijkl").slice(offset..offset + count))
    }
}

#[tokio::test]
async fn positioned_nfs_read_does_not_wait_for_delayed_first_range()
-> Result<(), Box<dyn std::error::Error>> {
    let state = ReadState {
        cursor: Arc::new(DelayedPrefixCursor),
        path: StoragePath::new("source")?,
        next_issue: 0,
        next_emit: 0,
        end: 12,
        maximum_chunk_bytes: 4,
        read_concurrency: 3,
        inflight: Either::Right(FuturesUnordered::new()),
        cancel: tokio_util::sync::CancellationToken::new(),
        qos: None,
        budget: None,
    };
    let mut input = Box::pin(futures::stream::try_unfold(state, read_next));
    let first = input.next().await.ok_or("missing first read")??;
    assert_ne!(first.offset, 0);
    let mut chunks = vec![first];
    while let Some(chunk) = input.next().await.transpose()? {
        chunks.push(chunk);
    }
    chunks.sort_by_key(|chunk| chunk.offset);
    assert_eq!(
        chunks
            .into_iter()
            .flat_map(|chunk| chunk.data.to_vec())
            .collect::<Vec<_>>(),
        b"abcdefghijkl"
    );
    Ok(())
}
