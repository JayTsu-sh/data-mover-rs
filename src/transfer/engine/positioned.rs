//! Completion-ordered writes with a separately ordered, admission-bounded source digest.
use std::collections::BTreeMap;

use futures::stream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::storage::{PositionedByteStream, PositionedChunk, ReadAdmission, ReadBudget};

pub(super) async fn transfer(
    request: &TransferRequest,
    source: Arc<dyn ReadSource>,
    destination: &Arc<dyn StagedDestination>,
    descriptor: &SourceDescriptor,
    stage: &PreparedStage,
    size: u64,
    source_qos: Option<SourceQosBudget>,
) -> Result<(WriteEvidence, Option<[u8; 32]>), TransferFailure> {
    let (runtime, queue) = inflight_channel(
        request.inflight,
        stage.write_offset,
        size,
        request.cancel.clone(),
    )?;
    let producer_request = ProducerRequest {
        source,
        path: descriptor.path.clone(),
        source_identity: descriptor.source_identity.clone(),
        version: descriptor.version.clone(),
        cancel: request.cancel.clone(),
        runtime,
        failure: Arc::default(),
        size,
        write_start: stage.write_offset,
        source_qos,
        hash_content: request.needs_source_digest() || stage.write_offset == 0,
    };
    let (sender, receiver) = mpsc::channel(request.inflight.chunks);
    let producer = tokio::spawn(async move {
        // Retain the admission context for the source, without feeding its ordered queue.
        let _queue = queue;
        match produce_positioned(&producer_request, &sender).await {
            Ok(digest) => Ok(digest),
            Err(error) => {
                let _ = sender.send(Err(error.clone())).await;
                Err(TransferFailure::role(
                    TransferPhase::Transfer,
                    TransferSide::Source,
                    error,
                ))
            }
        }
    });
    let stream = guarded_stream(receiver, request.cancel.clone(), descriptor.path.clone());
    settle_transfer(destination.write_positioned(stage, stream).await, producer).await
}

fn guarded_stream(
    receiver: mpsc::Receiver<Result<QueuedChunk, StorageRoleFailure>>,
    cancel: CancellationToken,
    path: StoragePath,
) -> PositionedByteStream {
    Box::pin(stream::unfold(
        (receiver, cancel, path, false),
        |(mut receiver, cancel, path, ended)| async move {
            if ended {
                return None;
            }
            let item = tokio::select! {
                biased;
                () = cancel.cancelled() => Some(Err(source_failure(&path, FailureClass::Cancelled))),
                item = receiver.recv() => item,
            }?;
            let ended = item.is_err();
            Some((
                item.map(|queued| queued.chunk),
                (receiver, cancel, path, ended),
            ))
        },
    ))
}

/// Keeps admissions alive while an early missing chunk prevents hashing later chunks.
/// `Bytes::clone` shares the payload with the positioned writer without copying it.
struct QueuedChunk {
    chunk: PositionedChunk,
    _admission: Arc<ReadAdmission>,
}

struct DigestPrefix {
    next: u64,
    end: u64,
    hasher: Option<blake3::Hasher>,
    pending: BTreeMap<u64, (Bytes, Arc<ReadAdmission>)>,
}

impl DigestPrefix {
    fn insert(&mut self, chunk: &PositionedChunk, admission: Arc<ReadAdmission>) -> bool {
        let Some(end) = chunk.offset.checked_add(chunk.data.len() as u64) else {
            return false;
        };
        if chunk.data.len() > admission.reserved_length()
            || chunk.data.is_empty()
            || chunk.offset < self.next
            || end > self.end
            || self
                .pending
                .range(..=chunk.offset)
                .next_back()
                .is_some_and(|(offset, (data, _))| *offset + data.len() as u64 > chunk.offset)
            || self
                .pending
                .range(chunk.offset..)
                .next()
                .is_some_and(|(offset, _)| *offset < end)
        {
            return false;
        }
        self.pending
            .insert(chunk.offset, (chunk.data.clone(), admission));
        true
    }

    fn advance(&mut self) {
        while let Some((data, _admission)) = self.pending.remove(&self.next) {
            if let Some(hasher) = &mut self.hasher {
                hasher.update(&data);
            }
            self.next += data.len() as u64;
        }
    }
}

async fn produce_positioned(
    request: &ProducerRequest,
    sender: &mpsc::Sender<Result<QueuedChunk, StorageRoleFailure>>,
) -> Result<Option<[u8; 32]>, StorageRoleFailure> {
    let read_start = if request.hash_content {
        0
    } else {
        request.write_start
    };
    let budget = ReadBudget::new(request.runtime.clone());
    let mut source = request
        .source
        .read_positioned(ReadRequest {
            path: request.path.clone(),
            range: Some(read_start..request.size),
            expected_source: Some(request.source_identity.clone()),
            maximum_chunk_bytes: request.runtime.negotiated_chunk_ceiling(),
            read_inflight: request.runtime.read_depth(),
            read_budget: Some(budget.clone()),
            cancel: request.cancel.clone(),
            source_qos: request.source_qos.clone(),
            version: request.version.clone(),
        })
        .await?;
    let mut prefix = DigestPrefix {
        next: read_start,
        end: request.size,
        hasher: request.hash_content.then(blake3::Hasher::new),
        pending: BTreeMap::new(),
    };
    loop {
        let item = tokio::select! {
            biased;
            () = request.cancel.cancelled() => return Err(source_failure(&request.path, FailureClass::Cancelled)),
            item = source.next() => item,
        };
        let Some(chunk) = item else { break };
        let chunk = chunk?;
        let admission = budget
            .take(chunk.offset)
            .ok_or_else(|| source_failure(&request.path, FailureClass::Internal))?;
        let admission = Arc::new(admission);
        if !prefix.insert(&chunk, Arc::clone(&admission)) {
            return Err(source_failure(&request.path, FailureClass::Corruption));
        }
        let end = chunk.offset + chunk.data.len() as u64;
        if end > request.write_start {
            let skip = usize::try_from(request.write_start.saturating_sub(chunk.offset))
                .map_err(|_| source_failure(&request.path, FailureClass::Corruption))?;
            let output = PositionedChunk {
                offset: chunk.offset + skip as u64,
                data: chunk.data.slice(skip..),
            };
            tokio::select! {
                biased;
                () = request.cancel.cancelled() => return Err(source_failure(&request.path, FailureClass::Cancelled)),
                sent = sender.send(Ok(QueuedChunk { chunk: output, _admission: admission })) => sent.map_err(|_| source_failure(&request.path, FailureClass::Cancelled))?,
            }
        }
        // Delivery to the writer does not wait for this prefix to become contiguous.
        prefix.advance();
    }
    if prefix.next != request.size {
        return Err(source_failure(&request.path, FailureClass::Corruption));
    }
    Ok(prefix.hasher.map(|hasher| *hasher.finalize().as_bytes()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::model::IdentityStrength;
    use async_trait::async_trait;

    struct ReversedSource {
        release_prefix: Arc<tokio::sync::Notify>,
        observed_budget: Arc<Mutex<Option<ReadBudget>>>,
    }

    #[async_trait]
    impl ReadSource for ReversedSource {
        async fn describe(&self, _: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
            unreachable!("producer already has a descriptor")
        }
        async fn read(
            &self,
            _: ReadRequest,
        ) -> Result<crate::storage::ByteStream, StorageRoleFailure> {
            unreachable!("positioned source must not use the ordered path")
        }
        async fn read_positioned(
            &self,
            request: ReadRequest,
        ) -> Result<PositionedByteStream, StorageRoleFailure> {
            let budget = request.read_budget.unwrap();
            *self.observed_budget.lock().unwrap() = Some(budget.clone());
            // Reserve in file order, but finish the later read first.
            let first = budget.reserve(4, true).await.unwrap().unwrap();
            let second = budget.reserve(4, true).await.unwrap().unwrap();
            let gate = Arc::clone(&self.release_prefix);
            let items = vec![
                (4, Bytes::from_static(b"efgh"), second),
                (0, Bytes::from_static(b"abcd"), first),
            ];
            Ok(Box::pin(futures::stream::iter(items).then(
                move |(offset, data, admission)| {
                    let gate = Arc::clone(&gate);
                    let budget = budget.clone();
                    async move {
                        if offset == 0 {
                            gate.notified().await;
                        }
                        budget.ready(offset, admission);
                        Ok(PositionedChunk { offset, data })
                    }
                },
            )))
        }
    }

    fn request(source: Arc<dyn ReadSource>) -> (ProducerRequest, OrderedChunks) {
        let cancel = tokio_util::sync::CancellationToken::new();
        let (runtime, queue) =
            InflightRuntime::channel(InflightConfig::new(2, 8, 2).unwrap(), 0, 8, cancel.clone())
                .unwrap();
        (
            ProducerRequest {
                source,
                path: StoragePath::new("source").unwrap(),
                source_identity: SourceIdentity::new(
                    crate::storage::backends::local::test_identity("positioned-test"),
                    IdentityStrength::PathScoped,
                    b"source",
                )
                .unwrap(),
                version: SourceVersion::Current,
                cancel,
                runtime,
                failure: Arc::default(),
                size: 8,
                write_start: 0,
                source_qos: None,
                hash_content: true,
            },
            queue,
        )
    }

    #[tokio::test]
    async fn later_chunk_reaches_writer_before_prefix_and_digest_stays_ordered_and_bounded() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let observed_budget = Arc::new(Mutex::new(None));
        let (request, _queue) = request(Arc::new(ReversedSource {
            release_prefix: Arc::clone(&gate),
            observed_budget: Arc::clone(&observed_budget),
        }));
        let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
        let task = tokio::spawn(async move { produce_positioned(&request, &sender).await });
        let later = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(later.chunk.offset, 4);
        assert_eq!(later.chunk.data, b"efgh"[..]);
        drop(later);
        let budget = observed_budget.lock().unwrap().clone().unwrap();
        // Retaining the digest reorder buffer must retain its admission too.
        assert!(budget.reserve(1, false).await.unwrap().is_none());
        gate.notify_one();
        let first = receiver.recv().await.unwrap().unwrap();
        assert_eq!(first.chunk.offset, 0);
        drop(first);
        assert_eq!(
            task.await.unwrap().unwrap(),
            Some(*blake3::hash(b"abcdefgh").as_bytes())
        );
        assert!(budget.reserve(4, false).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn queued_writes_retain_budget_and_verified_resume_trims_only_output() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let observed_budget = Arc::new(Mutex::new(None));
        let (mut request, _queue) = request(Arc::new(ReversedSource {
            release_prefix: Arc::clone(&gate),
            observed_budget: Arc::clone(&observed_budget),
        }));
        request.write_start = 2;
        let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
        let task = tokio::spawn(async move { produce_positioned(&request, &sender).await });
        let later = receiver.recv().await.unwrap().unwrap();
        gate.notify_one();
        assert_eq!(
            task.await.unwrap().unwrap(),
            Some(*blake3::hash(b"abcdefgh").as_bytes())
        );
        let budget = observed_budget.lock().unwrap().clone().unwrap();
        // Hashing is finished, but both payloads still have write-side owners.
        assert!(budget.reserve(1, false).await.unwrap().is_none());
        drop(later);
        let first = receiver.recv().await.unwrap().unwrap();
        assert_eq!(first.chunk.offset, 2);
        assert_eq!(first.chunk.data, b"cd"[..]);
        drop(first);
        assert!(budget.reserve(8, false).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn cancellation_while_prefix_missing_is_structured_and_releases_admissions() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let observed_budget = Arc::new(Mutex::new(None));
        let (request, _queue) = request(Arc::new(ReversedSource {
            release_prefix: gate,
            observed_budget: Arc::clone(&observed_budget),
        }));
        let cancel = request.cancel.clone();
        let path = request.path.clone();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
        let task = tokio::spawn(async move { produce_positioned(&request, &sender).await });
        assert_eq!(receiver.recv().await.unwrap().unwrap().chunk.offset, 4);
        cancel.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            result.unwrap_err(),
            source_failure(&path, FailureClass::Cancelled)
        );
    }
}
