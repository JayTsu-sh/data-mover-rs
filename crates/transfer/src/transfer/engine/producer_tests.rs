use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt as _;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::TransferFailure;
use super::producer::*;
use crate::model::{
    BackendIdentity, EntryKind, IdentityStrength, SourceIdentity, SourceVersion, StoragePath,
};
use crate::storage::{ByteStream, ReadRequest, ReadSource, SourceDescriptor, StorageRoleFailure};

type TestResult = Result<(), Box<dyn std::error::Error>>;
type ProducerTask = tokio::task::JoinHandle<Result<Option<[u8; 32]>, TransferFailure>>;

struct Source {
    descriptor: SourceDescriptor,
    chunks: Vec<Bytes>,
    emitted: Arc<AtomicUsize>,
    blocked: Arc<Notify>,
}

#[async_trait]
impl ReadSource for Source {
    fn maximum_read_chunk_bytes(&self) -> usize {
        4
    }

    async fn describe(&self, _: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        Ok(self.descriptor.clone())
    }

    async fn read(&self, _: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        let emitted = self.emitted.clone();
        let blocked = self.blocked.clone();
        Ok(Box::pin(futures::stream::iter(self.chunks.clone()).map(
            move |chunk| {
                if emitted.fetch_add(1, Ordering::SeqCst) == 4 {
                    blocked.notify_one();
                }
                Ok(chunk)
            },
        )))
    }
}

fn source(chunks: Vec<Bytes>, size: u64) -> Result<Arc<Source>, Box<dyn std::error::Error>> {
    let backend = BackendIdentity::new("local".parse()?, "producer-window")?;
    Ok(Arc::new(Source {
        descriptor: SourceDescriptor::new(
            StoragePath::new("source")?,
            EntryKind::File,
            Some(size),
            SourceIdentity::new(backend, IdentityStrength::PathScoped, b"source")?,
        ),
        chunks,
        emitted: Arc::default(),
        blocked: Arc::default(),
    }))
}

fn start(source: &Arc<Source>, write_start: u64) -> (PayloadReceiver, ProducerTask) {
    let cancel = CancellationToken::new();
    let (sender, receiver) = payload_channel(cancel.clone(), source.descriptor.path.clone());
    let task = tokio::spawn(produce(ProducerRequest {
        source: source.clone(),
        path: source.descriptor.path.clone(),
        source_identity: source.descriptor.source_identity.clone(),
        version: SourceVersion::Current,
        cancel,
        sender,
        size: source.descriptor.size.unwrap_or(0),
        write_start,
        source_qos: None,
        hash_content: true,
    }));
    (receiver, task)
}

#[tokio::test]
async fn slow_consumer_stops_reads_and_dropping_it_stops_the_producer() -> TestResult {
    let source = source(vec![Bytes::from_static(b"data"); 100], 400)?;
    let (receiver, task) = start(&source, 0);
    tokio::time::timeout(Duration::from_secs(5), source.blocked.notified()).await?;
    assert_eq!(source.emitted.load(Ordering::SeqCst), 5);
    drop(receiver);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), task)
            .await??
            .is_err()
    );
    assert_eq!(source.emitted.load(Ordering::SeqCst), 5);
    Ok(())
}

#[tokio::test]
async fn truncated_source_is_an_error_in_the_stream_and_in_completion() -> TestResult {
    let source = source(vec![Bytes::from_static(b"data")], 8)?;
    let (mut receiver, task) = start(&source, 0);
    assert_eq!(
        receiver.next().await.transpose()?.as_deref(),
        Some(b"data".as_slice())
    );
    assert!(
        receiver
            .next()
            .await
            .ok_or("missing source error")?
            .is_err()
    );
    assert!(receiver.next().await.is_none());
    assert!(task.await?.is_err());
    Ok(())
}

#[tokio::test]
async fn resume_slices_share_payload_while_hashing_the_complete_source() -> TestResult {
    let bytes = Bytes::from(b"abcdefgh".to_vec());
    let source = source(vec![bytes.slice(..4), bytes.slice(4..)], 8)?;
    let (mut receiver, task) = start(&source, 3);
    let first = receiver.next().await.transpose()?.ok_or("missing suffix")?;
    assert_eq!(first.as_ref(), b"d");
    assert_eq!(first.as_ptr(), bytes.slice(3..).as_ptr());
    assert_eq!(
        receiver.next().await.transpose()?.as_deref(),
        Some(b"efgh".as_slice())
    );
    assert!(receiver.next().await.is_none());
    assert_eq!(task.await??, Some(*blake3::hash(&bytes).as_bytes()));
    Ok(())
}
