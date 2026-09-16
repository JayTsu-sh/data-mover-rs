use super::super::protocol::{HdfsReadCursor, HdfsWriteSession};
use super::*;
use crate::model::BackendKind;
use std::{
    ops::Range,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct Payload {
    bytes: Bytes,
    gate_prefix: AtomicBool,
    prefix_started: tokio::sync::Notify,
    release_prefix: tokio::sync::Notify,
    later_finished: tokio::sync::Notify,
    calls: AtomicUsize,
}

#[async_trait]
impl HdfsReadCursor for Payload {
    async fn read_range(&self, range: Range<u64>) -> Result<Bytes, StorageRoleFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if range.start == 0 && self.gate_prefix.load(Ordering::SeqCst) {
            self.prefix_started.notify_one();
            self.release_prefix.notified().await;
        } else {
            self.later_finished.notify_one();
        }
        Ok(self.bytes.slice(
            usize::try_from(range.start).unwrap_or(usize::MAX)
                ..usize::try_from(range.end).unwrap_or(usize::MAX),
        ))
    }
}

struct ReadProtocol {
    payload: Arc<Payload>,
    use_cursor: bool,
}

impl ReadProtocol {
    fn new(gate_prefix: bool, use_cursor: bool) -> Arc<Self> {
        Arc::new(Self {
            payload: Arc::new(Payload {
                bytes: Bytes::from_static(b"abcdefghijkl"),
                gate_prefix: AtomicBool::new(gate_prefix),
                prefix_started: tokio::sync::Notify::new(),
                release_prefix: tokio::sync::Notify::new(),
                later_finished: tokio::sync::Notify::new(),
                calls: AtomicUsize::new(0),
            }),
            use_cursor,
        })
    }
}

fn unsupported(path: &StoragePath) -> StorageRoleFailure {
    failure(path, Operation::Write, FailureClass::Unsupported)
}

#[async_trait]
impl HdfsProtocol for ReadProtocol {
    fn maximum_read_chunk_bytes(&self) -> usize {
        4
    }
    fn read_concurrency(&self) -> usize {
        8
    }
    async fn open_reader(
        &self,
        _path: &StoragePath,
    ) -> Result<Option<Arc<dyn HdfsReadCursor>>, StorageRoleFailure> {
        Ok(self
            .use_cursor
            .then(|| self.payload.clone() as Arc<dyn HdfsReadCursor>))
    }
    async fn stat(&self, path: &StoragePath) -> Result<HdfsEntryFacts, StorageRoleFailure> {
        Ok(HdfsEntryFacts {
            path: path.clone(),
            kind: EntryKind::File,
            size: Some(self.payload.bytes.len() as u64),
            atime: 0,
            mtime: 123,
            mode: 0o600,
            owner: "owner".into(),
            group: "group".into(),
            replication: Some(1),
            block_size: Some(128 * 1024 * 1024),
        })
    }
    async fn read_range(
        &self,
        _path: &StoragePath,
        range: Range<u64>,
    ) -> Result<Bytes, StorageRoleFailure> {
        self.payload.read_range(range).await
    }
    async fn list(&self, path: &StoragePath) -> Result<Vec<HdfsEntryFacts>, StorageRoleFailure> {
        Err(unsupported(path))
    }
    async fn create_directory(&self, path: &StoragePath) -> Result<(), StorageRoleFailure> {
        Err(unsupported(path))
    }
    async fn delete(&self, path: &StoragePath, _kind: EntryKind) -> Result<(), StorageRoleFailure> {
        Err(unsupported(path))
    }
    async fn rename(
        &self,
        from: &StoragePath,
        _to: &StoragePath,
        _overwrite: bool,
    ) -> Result<(), StorageRoleFailure> {
        Err(unsupported(from))
    }
    async fn claim_stage(
        &self,
        from: &StoragePath,
        _claimed: &StoragePath,
    ) -> Result<(), StorageRoleFailure> {
        Err(unsupported(from))
    }
    async fn create_empty_stage_exclusive(
        &self,
        path: &StoragePath,
    ) -> Result<(), StorageRoleFailure> {
        Err(unsupported(path))
    }
    async fn open_stage_writer(
        &self,
        path: &StoragePath,
        _start_offset: u64,
        _direct: bool,
    ) -> Result<Box<dyn HdfsWriteSession + '_>, StorageRoleFailure> {
        Err(unsupported(path))
    }
    async fn set_mapped_ownership(
        &self,
        path: &StoragePath,
        _owner: &str,
        _group: &str,
        _mode: u32,
    ) -> Result<(), StorageRoleFailure> {
        Err(unsupported(path))
    }
    async fn set_mode(&self, path: &StoragePath, _mode: u32) -> Result<(), StorageRoleFailure> {
        Err(unsupported(path))
    }
    async fn set_timestamps(
        &self,
        path: &StoragePath,
        _atime: Option<i64>,
        _mtime: Option<i64>,
    ) -> Result<(), StorageRoleFailure> {
        Err(unsupported(path))
    }
}

fn source(protocol: &Arc<ReadProtocol>) -> Result<HdfsReadSource, Box<dyn std::error::Error>> {
    Ok(HdfsReadSource::new(
        protocol.clone(),
        BackendIdentity::new(BackendKind::Hdfs, "positioned-source")?,
    ))
}

fn request(range: Range<u64>) -> Result<ReadRequest, Box<dyn std::error::Error>> {
    Ok(ReadRequest {
        path: StoragePath::new("source")?,
        range: Some(range),
        expected_source: None,
        maximum_chunk_bytes: 4,
        read_inflight: 8,
        read_budget: None,
        cancel: CancellationToken::new(),
        source_qos: None,
    })
}

#[tokio::test]
async fn positioned_delivers_later_range_while_prefix_is_blocked() -> TestResult {
    for use_cursor in [false, true] {
        let protocol = ReadProtocol::new(true, use_cursor);
        let source = source(&protocol)?;
        assert!(source.supports_positioned_read());
        let mut input = source.read_positioned(request(0..8)?).await?;
        let later = tokio::time::timeout(Duration::from_secs(3), input.next()).await;
        protocol.payload.release_prefix.notify_one();
        let later = later?.transpose()?.ok_or("missing later chunk")?;
        assert_eq!(later.offset, 4);
        assert_eq!(&later.data[..], b"efgh");
        let first = input.next().await.transpose()?.ok_or("missing prefix")?;
        assert_eq!(first.offset, 0);
        assert_eq!(&first.data[..], b"abcd");
        assert!(input.next().await.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn positioned_range_preserves_absolute_offsets_and_short_tail() -> TestResult {
    let protocol = ReadProtocol::new(false, true);
    let mut input = source(&protocol)?.read_positioned(request(3..10)?).await?;
    let mut chunks = Vec::new();
    while let Some(chunk) = input.next().await.transpose()? {
        chunks.push((chunk.offset, chunk.data));
    }
    chunks.sort_by_key(|(offset, _)| *offset);
    assert_eq!(
        chunks,
        [
            (3, Bytes::from_static(b"defg")),
            (7, Bytes::from_static(b"hij"))
        ]
    );
    Ok(())
}

#[tokio::test]
async fn ordered_fallback_waits_for_prefix_despite_later_completion() -> TestResult {
    let protocol = ReadProtocol::new(true, true);
    let mut input = source(&protocol)?.read(request(0..8)?).await?;
    let first = {
        let next = input.next();
        tokio::pin!(next);
        tokio::select! {
            _ = &mut next => panic!("ordered source emitted past blocked prefix"),
            () = protocol.payload.later_finished.notified() => {},
        }
        protocol.payload.release_prefix.notify_one();
        next.await.transpose()?.ok_or("missing prefix")?
    };
    assert_eq!(first, Bytes::from_static(b"abcd"));
    assert_eq!(
        input
            .next()
            .await
            .transpose()?
            .ok_or("missing later chunk")?,
        Bytes::from_static(b"efgh")
    );
    Ok(())
}

#[tokio::test]
async fn positioned_prefetch_reserves_chunk_and_byte_capacity_before_reading() -> TestResult {
    use crate::storage::{InflightConfig, InflightRuntime, ReadBudget};
    for (chunks, bytes) in [(1, 32), (8, 4)] {
        let protocol = ReadProtocol::new(false, true);
        let mut request = request(0..8)?;
        let (runtime, mut ordered) = InflightRuntime::channel(
            InflightConfig::new(chunks, bytes, 8)?,
            0,
            8,
            request.cancel.clone(),
        )?;
        let budget = ReadBudget::new(runtime.clone());
        request.read_budget = Some(budget.clone());
        let mut input = source(&protocol)?.read_positioned(request).await?;
        let first = input
            .next()
            .await
            .transpose()?
            .ok_or("missing first chunk")?;
        assert_eq!(protocol.payload.calls.load(Ordering::SeqCst), 1);
        runtime
            .complete_read(
                budget.take(first.offset).ok_or("missing reservation")?,
                first.offset,
                first.data,
            )
            .await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), input.next())
                .await
                .is_err()
        );
        assert_eq!(protocol.payload.calls.load(Ordering::SeqCst), 1);
        assert!(ordered.next().await.transpose()?.is_some());
        let second = input
            .next()
            .await
            .transpose()?
            .ok_or("missing second chunk")?;
        runtime
            .complete_read(
                budget.take(second.offset).ok_or("missing reservation")?,
                second.offset,
                second.data,
            )
            .await?;
        assert!(ordered.next().await.transpose()?.is_some());
        assert!(input.next().await.is_none());
        assert_eq!(protocol.payload.calls.load(Ordering::SeqCst), 2);
    }
    Ok(())
}

#[tokio::test]
async fn positioned_cancellation_retains_cancelled_classification() -> TestResult {
    let protocol = ReadProtocol::new(true, true);
    let request = request(0..4)?;
    let cancel = request.cancel.clone();
    let mut input = source(&protocol)?.read_positioned(request).await?;
    let next = input.next();
    tokio::pin!(next);
    tokio::select! {
        _ = &mut next => panic!("blocked prefix unexpectedly completed"),
        () = protocol.payload.prefix_started.notified() => {},
    }
    cancel.cancel();
    assert!(
        matches!(next.await, Some(Err(StorageRoleFailure::Entry(error))) if error.class() == FailureClass::Cancelled)
    );
    Ok(())
}
