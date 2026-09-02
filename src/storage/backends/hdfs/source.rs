use std::sync::Arc;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures::stream::try_unfold;
use tokio_util::sync::CancellationToken;

use super::protocol::{HdfsEntryFacts, HdfsProtocol, cancelled, entry_failure};
use crate::model::{
    BackendIdentity, EntryKind, FailureClass, IdentityStrength, Operation, SourceIdentity,
    StoragePath, Transience,
};
use crate::storage::{
    ByteStream, ReadRequest, ReadSource, SourceDescriptor, SourceQosBudget, StorageRoleFailure,
};

const MAX_READ_CHUNK: u64 = 1024 * 1024;

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
    fn maximum_read_chunk_bytes(&self) -> usize {
        usize::try_from(MAX_READ_CHUNK).unwrap_or(usize::MAX)
    }

    async fn describe(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.descriptor(path).await
    }

    async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
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
        let state = ReadState {
            protocol: Arc::clone(&self.protocol),
            path: request.path,
            next: range.start,
            end: range.end,
            maximum_chunk_bytes: request.maximum_chunk_bytes,
            cancel: request.cancel,
            qos: request.source_qos,
        };
        Ok(Box::pin(try_unfold(state, read_next)))
    }
}

struct ReadState {
    protocol: Arc<dyn HdfsProtocol>,
    path: StoragePath,
    next: u64,
    end: u64,
    maximum_chunk_bytes: usize,
    cancel: CancellationToken,
    qos: Option<SourceQosBudget>,
}

async fn read_next(mut state: ReadState) -> Result<Option<(Bytes, ReadState)>, StorageRoleFailure> {
    if state.next == state.end {
        return Ok(None);
    }
    if state.cancel.is_cancelled() {
        return Err(cancelled(&state.path, Operation::Read));
    }
    let requested = (state.end - state.next)
        .min(MAX_READ_CHUNK)
        .min(state.maximum_chunk_bytes as u64);
    let count = admit_read(&state, requested).await?;
    let bytes = tokio::select! {
        biased;
        () = state.cancel.cancelled() => return Err(cancelled(&state.path, Operation::Read)),
        result = state.protocol.read_range(&state.path, state.next..state.next + count) => result?,
    };
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
    state.next += count;
    Ok(Some((bytes, state)))
}

async fn admit_read(state: &ReadState, requested: u64) -> Result<u64, StorageRoleFailure> {
    match &state.qos {
        Some(qos) => qos
            .admit_read(requested, &state.cancel)
            .await
            .map_err(|_| cancelled(&state.path, Operation::Read)),
        None => Ok(requested),
    }
}

pub(super) fn descriptor(
    identity: &BackendIdentity,
    path: StoragePath,
    facts: &HdfsEntryFacts,
) -> Result<SourceDescriptor, StorageRoleFailure> {
    let source_identity = identity_for(identity, &path, facts)?;
    Ok(
        SourceDescriptor::new(path, facts.kind, facts.size, source_identity)
            .with_backend_fact(backend_fact(facts)),
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
