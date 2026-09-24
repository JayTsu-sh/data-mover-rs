use super::source::{CifsReadCursor, ReadState};
use crate::model::{SourceVersion, StoragePath};
use crate::storage::{InflightConfig, InflightRuntime, ReadBudget, ReadRequest};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt as _;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Cursor {
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
}
#[async_trait]
impl CifsReadCursor for Cursor {
    fn maximum_read_chunk(&self) -> u32 {
        4
    }
    async fn read_at(&self, offset: u64, count: u32) -> smb_domain::Result<Bytes> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(if offset == 0 {
            20
        } else {
            1
        }))
        .await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        let start = usize::try_from(offset)?;
        Ok(Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz012345")
            .slice(start..start + count as usize))
    }
    async fn close(self: Box<Self>) -> smb_domain::Result<()> {
        self.closed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn reads_overlap_but_emit_in_order_and_obey_all_budgets()
-> Result<(), Box<dyn std::error::Error>> {
    for (chunks, bytes, operations, expected_peak) in
        [(4, 16, 4, 4), (1, 16, 4, 1), (4, 4, 4, 1), (4, 16, 1, 1)]
    {
        let cancel = tokio_util::sync::CancellationToken::new();
        let (runtime, mut ordered) = InflightRuntime::channel(
            InflightConfig::new(chunks, bytes, operations)?,
            0,
            16,
            cancel.clone(),
        )?;
        let budget = ReadBudget::new(runtime.clone());
        let peak = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicUsize::new(0));
        let cursor = Cursor {
            active: Arc::default(),
            peak: peak.clone(),
            calls: calls.clone(),
            closed: closed.clone(),
        };
        let mut stream = super::read_pipeline::stream(ReadState {
            cursor: Box::new(cursor),
            range: 0..16,
            request: ReadRequest {
                path: StoragePath::new("source")?,
                range: Some(0..16),
                expected_source: None,
                maximum_chunk_bytes: 4,
                read_inflight: 128,
                read_budget: Some(budget.clone()),
                cancel,
                source_qos: None,
                version: SourceVersion::Current,
            },
        });
        let mut offset = 0;
        let mut result = Vec::new();
        while let Some(bytes) = stream.next().await.transpose()? {
            result.extend_from_slice(&bytes);
            let len = bytes.len() as u64;
            runtime
                .complete_read(
                    budget.take(offset).ok_or("missing admission")?,
                    offset,
                    bytes,
                )
                .await?;
            assert!(ordered.next().await.transpose()?.is_some());
            offset += len;
        }
        assert_eq!(result, b"abcdefghijklmnop");
        assert_eq!(peak.load(Ordering::SeqCst), expected_peak);
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(closed.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn read_pipeline_can_fill_configured_depth_above_eight()
-> Result<(), Box<dyn std::error::Error>> {
    for depth in [1, 8, 16, 24] {
        let peak = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicUsize::new(0));
        let cursor = Cursor {
            active: Arc::default(),
            peak: peak.clone(),
            calls: Arc::default(),
            closed: closed.clone(),
        };
        let mut stream = super::read_pipeline::stream(ReadState {
            cursor: Box::new(cursor),
            range: 0..32,
            request: ReadRequest {
                path: StoragePath::new("source")?,
                range: Some(0..32),
                expected_source: None,
                maximum_chunk_bytes: 1,
                read_inflight: depth,
                read_budget: None,
                cancel: tokio_util::sync::CancellationToken::new(),
                source_qos: None,
                version: SourceVersion::Current,
            },
        });
        let mut result = Vec::new();
        while let Some(bytes) = stream.next().await.transpose()? {
            result.extend_from_slice(&bytes);
        }
        assert_eq!(result, b"abcdefghijklmnopqrstuvwxyz012345");
        assert_eq!(peak.load(Ordering::SeqCst), depth);
        assert_eq!(closed.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn positioned_reads_emit_completed_ranges_before_the_delayed_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let closed = Arc::new(AtomicUsize::new(0));
    let cursor = Cursor {
        active: Arc::default(),
        peak: Arc::default(),
        calls: Arc::default(),
        closed: closed.clone(),
    };
    let mut stream = super::read_pipeline::positioned_stream(ReadState {
        cursor: Box::new(cursor),
        range: 0..16,
        request: ReadRequest {
            path: StoragePath::new("source")?,
            range: Some(0..16),
            expected_source: None,
            maximum_chunk_bytes: 4,
            read_inflight: 4,
            read_budget: None,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
            version: SourceVersion::Current,
        },
    });
    let first = stream.next().await.ok_or("no first chunk")??;
    assert_ne!(first.offset, 0);
    let mut chunks = vec![first];
    while let Some(chunk) = stream.next().await.transpose()? {
        chunks.push(chunk);
    }
    chunks.sort_by_key(|chunk| chunk.offset);
    assert_eq!(
        chunks
            .into_iter()
            .flat_map(|chunk| chunk.data.to_vec())
            .collect::<Vec<_>>(),
        b"abcdefghijklmnop"
    );
    assert_eq!(closed.load(Ordering::SeqCst), 1);
    Ok(())
}
