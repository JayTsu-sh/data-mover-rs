use std::sync::Arc;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures::{
    StreamExt as _,
    stream::{FuturesOrdered, FuturesUnordered, try_unfold},
};
use tokio_util::sync::CancellationToken;

use super::protocol::{HdfsEntryFacts, HdfsProtocol, cancelled, entry_failure};
use crate::model::{
    BackendIdentity, EntryKind, FailureClass, IdentityStrength, Operation, SourceIdentity,
    StoragePath, Transience,
};
use crate::storage::{
    ByteStream, PositionedByteStream, PositionedChunk, ReadRequest, ReadSource, SourceDescriptor,
    SourceQosBudget, StorageRoleFailure,
};

pub(super) struct HdfsReadSource {
    protocol: Arc<dyn HdfsProtocol>,
    identity: BackendIdentity,
}

impl HdfsReadSource {
    pub(super) fn new<P: HdfsProtocol + 'static>(
        protocol: Arc<P>,
        identity: BackendIdentity,
    ) -> Self {
        Self { protocol, identity }
    }

    pub(super) async fn descriptor(
        &self,
        path: &StoragePath,
    ) -> Result<SourceDescriptor, StorageRoleFailure> {
        let observed = self.protocol.stat(path).await?;
        descriptor(&self.identity, path.clone(), &observed)
    }
}

#[async_trait]
impl ReadSource for HdfsReadSource {
    fn supports_read_budget(&self) -> bool {
        true
    }
    fn maximum_read_chunk_bytes(&self) -> usize {
        self.protocol.maximum_read_chunk_bytes().max(1)
    }

    async fn describe(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.descriptor(path).await
    }

    fn supports_positioned_read(&self) -> bool {
        true
    }

    async fn read_positioned(
        &self,
        request: ReadRequest,
    ) -> Result<PositionedByteStream, StorageRoleFailure> {
        Ok(Box::pin(try_unfold(
            self.read_state(request, false).await?,
            read_next,
        )))
    }

    async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        Ok(Box::pin(
            try_unfold(self.read_state(request, true).await?, read_next)
                .map(|item| item.map(|chunk| chunk.data)),
        ))
    }
}

impl HdfsReadSource {
    async fn read_state(
        &self,
        request: ReadRequest,
        ordered: bool,
    ) -> Result<ReadState, StorageRoleFailure> {
        if request.cancel.is_cancelled() {
            return Err(cancelled(&request.path, Operation::Read));
        }
        if request.maximum_chunk_bytes == 0 || request.read_inflight == 0 {
            return Err(failure(
                &request.path,
                Operation::Read,
                FailureClass::InvalidInput,
            ));
        }
        let observed = self.descriptor(&request.path).await?;
        if request
            .expected_source
            .as_ref()
            .is_some_and(|value| value != &observed.source_identity)
        {
            return Err(failure(
                &request.path,
                Operation::Read,
                FailureClass::Conflict,
            ));
        }
        if observed.kind != EntryKind::File {
            return Err(failure(
                &request.path,
                Operation::Read,
                FailureClass::Unsupported,
            ));
        }
        let size = observed
            .size
            .ok_or_else(|| failure(&request.path, Operation::Read, FailureClass::Protocol))?;
        let range = request.range.unwrap_or(0..size);
        if range.end < range.start || range.end > size {
            return Err(failure(
                &request.path,
                Operation::Read,
                FailureClass::InvalidInput,
            ));
        }
        let cursor = self.protocol.open_reader(&request.path).await?;
        let confirmed = self.descriptor(&request.path).await?;
        if confirmed.source_identity != observed.source_identity {
            return Err(failure(
                &request.path,
                Operation::Read,
                FailureClass::Conflict,
            ));
        }
        let state = ReadState {
            protocol: Arc::clone(&self.protocol),
            path: request.path,
            cursor,
            next_issue: range.start,
            inflight: if ordered {
                ReadQueue::Ordered(FuturesOrdered::new())
            } else {
                ReadQueue::Positioned(FuturesUnordered::new())
            },
            budget: request.read_budget,
            concurrency: request
                .read_inflight
                .min(self.protocol.read_concurrency().max(1)),
            end: range.end,
            maximum_chunk_bytes: request
                .maximum_chunk_bytes
                .min(self.maximum_read_chunk_bytes()),
            cancel: request.cancel,
            qos: request.source_qos,
        };
        Ok(state)
    }
}

struct ReadState {
    protocol: Arc<dyn HdfsProtocol>,
    cursor: Option<Arc<dyn super::protocol::HdfsReadCursor>>,
    path: StoragePath,
    next_issue: u64,
    end: u64,
    maximum_chunk_bytes: usize,
    concurrency: usize,
    inflight: ReadQueue,
    budget: Option<crate::storage::ReadBudget>,
    cancel: CancellationToken,
    qos: Option<SourceQosBudget>,
}

type ReadResult = (
    u64,
    u64,
    Result<Bytes, StorageRoleFailure>,
    Option<crate::storage::ReadAdmission>,
);
type ReadFuture = futures::future::BoxFuture<'static, ReadResult>;

enum ReadQueue {
    Ordered(FuturesOrdered<ReadFuture>),
    Positioned(FuturesUnordered<ReadFuture>),
}

impl ReadQueue {
    fn len(&self) -> usize {
        match self {
            Self::Ordered(queue) => queue.len(),
            Self::Positioned(queue) => queue.len(),
        }
    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn push_back(&mut self, future: ReadFuture) {
        match self {
            Self::Ordered(queue) => queue.push_back(future),
            Self::Positioned(queue) => queue.push(future),
        }
    }
    async fn next(&mut self) -> Option<ReadResult> {
        match self {
            Self::Ordered(queue) => queue.next().await,
            Self::Positioned(queue) => queue.next().await,
        }
    }
}

async fn fill_pipeline(state: &mut ReadState) -> Result<(), StorageRoleFailure> {
    while state.inflight.len() < state.concurrency && state.next_issue < state.end {
        let requested = (state.end - state.next_issue).min(state.maximum_chunk_bytes as u64);
        let admission = if let Some(budget) = &state.budget {
            let Some(permit) = budget
                .reserve(
                    usize::try_from(requested).map_err(|_| {
                        failure(&state.path, Operation::Read, FailureClass::InvalidInput)
                    })?,
                    state.inflight.is_empty(),
                )
                .await
                .map_err(|_| cancelled(&state.path, Operation::Read))?
            else {
                break;
            };
            Some(permit)
        } else {
            None
        };
        let count = if let Some(qos) = &state.qos {
            qos.admit_read(requested, &state.cancel)
                .await
                .map_err(|_| cancelled(&state.path, Operation::Read))?
        } else {
            requested
        };
        let offset = state.next_issue;
        let protocol = Arc::clone(&state.protocol);
        let cursor = state.cursor.clone();
        let path = state.path.clone();
        let cancel = state.cancel.clone();
        state.inflight.push_back(Box::pin(async move {
            let read = async {
                match cursor {
                    Some(cursor) => cursor.read_range(offset..offset + count).await,
                    None => protocol.read_range(&path, offset..offset + count).await,
                }
            };
            let result = tokio::select! {
                biased;
                () = cancel.cancelled() => Err(cancelled(&path, Operation::Read)),
                result = read => result,
            };
            (offset, count, result, admission)
        }));
        state.next_issue += count;
    }
    Ok(())
}

async fn read_next(
    mut state: ReadState,
) -> Result<Option<(PositionedChunk, ReadState)>, StorageRoleFailure> {
    fill_pipeline(&mut state).await?;
    let next = tokio::select! {
        biased;
        () = state.cancel.cancelled() => return Err(cancelled(&state.path, Operation::Read)),
        next = state.inflight.next() => next,
    };
    let Some((offset, count, result, admission)) = next else {
        return Ok(None);
    };
    let bytes = result?;
    if bytes.len() as u64 != count {
        return Err(failure(
            &state.path,
            Operation::Read,
            FailureClass::Corruption,
        ));
    }
    if let Some(qos) = &state.qos {
        qos.record_read_bytes(count);
    }
    if let (Some(budget), Some(admission)) = (&state.budget, admission) {
        budget.ready(offset, admission);
    }
    Ok(Some((
        PositionedChunk {
            offset,
            data: bytes,
        },
        state,
    )))
}

pub(super) fn descriptor(
    identity: &BackendIdentity,
    path: StoragePath,
    facts: &HdfsEntryFacts,
) -> Result<SourceDescriptor, StorageRoleFailure> {
    let source_identity = identity_for(identity, &path, facts)?;
    // `stat` and `list` share this one constructor, so both paths report the same inline facts.
    // HDFS has no change or birth time, so `created` stays absent rather than being faked.
    Ok(
        SourceDescriptor::new(path, facts.kind, facts.size, source_identity)
            .with_backend_fact(backend_fact(facts))
            .with_inline_timestamps(super::metadata::timestamps(facts.atime, facts.mtime))
            .with_inline_mode(facts.mode),
    )
}

fn identity_for(
    identity: &BackendIdentity,
    path: &StoragePath,
    facts: &HdfsEntryFacts,
) -> Result<SourceIdentity, StorageRoleFailure> {
    let mut value = BytesMut::with_capacity(path.as_str().len() + 48);
    value.extend_from_slice(b"data-mover:hdfs-path-identity:v1\0");
    put_text(&mut value, path.as_str());
    value.put_u64(facts.size.unwrap_or_default());
    value.put_i64(facts.mtime);
    SourceIdentity::new(identity.clone(), IdentityStrength::PathScoped, value)
        .map_err(|_| failure(path, Operation::Observe, FailureClass::Protocol))
}

fn backend_fact(facts: &HdfsEntryFacts) -> Bytes {
    let mut value = BytesMut::new();
    value.extend_from_slice(b"data-mover:hdfs-entry-facts:v1\0");
    value.put_u32(facts.mode);
    value.put_i64(facts.atime);
    value.put_i64(facts.mtime);
    put_text(&mut value, &facts.owner);
    put_text(&mut value, &facts.group);
    value.put_u32(facts.replication.unwrap_or_default());
    value.put_u64(facts.block_size.unwrap_or_default());
    value.freeze()
}

fn put_text(output: &mut BytesMut, value: &str) {
    output.put_u32(u32::try_from(value.len()).unwrap_or(u32::MAX));
    output.extend_from_slice(value.as_bytes());
}

fn failure(path: &StoragePath, operation: Operation, class: FailureClass) -> StorageRoleFailure {
    entry_failure(path, operation, class, Transience::Permanent)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(size: u64, mtime: i64) -> HdfsEntryFacts {
        HdfsEntryFacts {
            path: StoragePath::new("file").unwrap_or_else(|error| panic!("{error}")),
            kind: EntryKind::File,
            size: Some(size),
            mtime,
            atime: 0,
            mode: 0o640,
            owner: "alice".into(),
            group: "users".into(),
            replication: Some(3),
            block_size: Some(128 * 1024 * 1024),
        }
    }

    #[test]
    fn path_scoped_identity_detects_size_and_mtime_changes()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend = BackendIdentity::new(crate::model::BackendKind::Hdfs, "cluster")?;
        let path = StoragePath::new("file")?;
        let baseline = descriptor(&backend, path.clone(), &sample(4, 10))?;
        assert_eq!(
            baseline.source_identity.strength(),
            IdentityStrength::PathScoped
        );
        assert_ne!(
            baseline.source_identity,
            descriptor(&backend, path.clone(), &sample(5, 10))?.source_identity
        );
        assert_ne!(
            baseline.source_identity,
            descriptor(&backend, path, &sample(4, 11))?.source_identity
        );
        Ok(())
    }
}

#[cfg(test)]
#[path = "source_tests.rs"]
mod source_tests;
