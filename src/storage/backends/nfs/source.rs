use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt as _;
use futures::stream::FuturesOrdered;

use crate::model::{
    BackendIdentity, BackendSessionFailure, EntryKind, EntryOperationFailure, FailureClass,
    IdentityStrength, Operation, SourceIdentity, StoragePath, Transience,
};
use crate::storage::{ByteStream, ReadRequest, ReadSource, SourceDescriptor, StorageRoleFailure};

const MAX_ROLE_READ: usize = 1024 * 1024;

#[derive(Clone)]
pub(crate) struct NfsSourceObservation {
    pub(crate) kind: EntryKind,
    pub(crate) size: Option<u64>,
    pub(crate) file_handle: Bytes,
    pub(crate) content_version: Bytes,
}

#[derive(Clone, Copy)]
pub(crate) struct NfsProtocolFailure {
    pub(crate) class: FailureClass,
    pub(crate) transience: Transience,
}

impl NfsProtocolFailure {
    pub(crate) const fn protocol() -> Self {
        Self {
            class: FailureClass::Protocol,
            transience: Transience::Permanent,
        }
    }
}

#[async_trait]
pub(crate) trait NfsReadCursor: Send + Sync {
    async fn read_at(&self, offset: u64, count: usize) -> Result<Bytes, NfsProtocolFailure>;
}

#[async_trait]
pub(crate) trait NfsSourceProtocol: Send + Sync {
    fn maximum_read_chunk_bytes(&self) -> usize {
        MAX_ROLE_READ
    }
    async fn describe(
        &self,
        path: &StoragePath,
    ) -> Result<NfsSourceObservation, NfsProtocolFailure>;
    async fn open(
        &self,
        path: &StoragePath,
    ) -> Result<(Box<dyn NfsReadCursor>, Bytes), NfsProtocolFailure>;
}

pub(crate) struct NfsReadSourceAdapter {
    protocol: Arc<dyn NfsSourceProtocol>,
    identity: BackendIdentity,
}

impl NfsReadSourceAdapter {
    pub(crate) fn new(protocol: Arc<dyn NfsSourceProtocol>, identity: BackendIdentity) -> Self {
        Self { protocol, identity }
    }

    #[cfg(test)]
    fn with_protocol(protocol: Arc<dyn NfsSourceProtocol>, identity: BackendIdentity) -> Self {
        Self { protocol, identity }
    }

    async fn descriptor(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        let observed = self
            .protocol
            .describe(path)
            .await
            .map_err(|error| role_failure(path, Operation::Observe, error))?;
        let source_identity = SourceIdentity::new(
            self.identity.clone(),
            IdentityStrength::StableWithinBackend,
            &observed.file_handle,
        )
        .map_err(|_| {
            role_failure(
                path,
                Operation::Observe,
                NfsProtocolFailure {
                    class: FailureClass::Protocol,
                    transience: Transience::Permanent,
                },
            )
        })?;
        Ok(SourceDescriptor {
            path: path.clone(),
            kind: observed.kind,
            size: observed.size,
            source_identity,
            backend_fact: None,
            content_version: Some(observed.content_version),
        })
    }
}

#[async_trait]
impl ReadSource for NfsReadSourceAdapter {
    fn supports_read_budget(&self) -> bool {
        true
    }
    fn maximum_read_chunk_bytes(&self) -> usize {
        self.protocol.maximum_read_chunk_bytes().max(1)
    }

    async fn describe(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.descriptor(path).await
    }

    async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        if request.cancel.is_cancelled() {
            return Err(cancelled(&request.path));
        }
        if request.maximum_chunk_bytes == 0 || request.read_inflight == 0 {
            return Err(entry_failure(
                &request.path,
                Operation::Read,
                FailureClass::InvalidInput,
                Transience::Permanent,
            ));
        }
        let range = if let Some(range) = request.range {
            range
        } else {
            let observed = self.descriptor(&request.path).await?;
            0..observed.size.ok_or_else(|| {
                entry_failure(
                    &request.path,
                    Operation::Read,
                    FailureClass::Unsupported,
                    Transience::Permanent,
                )
            })?
        };
        if range.end < range.start {
            return Err(entry_failure(
                &request.path,
                Operation::Read,
                FailureClass::InvalidInput,
                Transience::Permanent,
            ));
        }
        let (cursor, opened_handle) = self
            .protocol
            .open(&request.path)
            .await
            .map_err(|error| role_failure(&request.path, Operation::Read, error))?;
        let opened_identity = SourceIdentity::new(
            self.identity.clone(),
            IdentityStrength::StableWithinBackend,
            opened_handle,
        )
        .map_err(|_| {
            entry_failure(
                &request.path,
                Operation::Read,
                FailureClass::Protocol,
                Transience::Permanent,
            )
        })?;
        if request
            .expected_source
            .as_ref()
            .is_some_and(|expected| expected != &opened_identity)
        {
            return Err(entry_failure(
                &request.path,
                Operation::Read,
                FailureClass::Conflict,
                Transience::Permanent,
            ));
        }
        let state = ReadState {
            cursor: Arc::from(cursor),
            path: request.path,
            next_issue: range.start,
            next_emit: range.start,
            end: range.end,
            maximum_chunk_bytes: request
                .maximum_chunk_bytes
                .min(self.protocol.maximum_read_chunk_bytes().max(1)),
            read_concurrency: request.read_inflight,
            inflight: FuturesOrdered::new(),
            cancel: request.cancel,
            qos: request.source_qos,
            budget: request.read_budget,
        };
        Ok(Box::pin(futures::stream::try_unfold(state, read_next)))
    }
}

struct ReadState {
    cursor: Arc<dyn NfsReadCursor>,
    path: StoragePath,
    next_issue: u64,
    next_emit: u64,
    end: u64,
    maximum_chunk_bytes: usize,
    read_concurrency: usize,
    inflight: FuturesOrdered<NfsReadFuture>,
    cancel: tokio_util::sync::CancellationToken,
    qos: Option<crate::storage::SourceQosBudget>,
    budget: Option<crate::storage::ReadBudget>,
}

type NfsReadFuture = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = (
                    u64,
                    usize,
                    Result<Bytes, NfsProtocolFailure>,
                    Option<crate::storage::ReadAdmission>,
                ),
            > + Send,
    >,
>;

async fn fill_read_pipeline(state: &mut ReadState) -> Result<(), StorageRoleFailure> {
    while state.inflight.len() < state.read_concurrency && state.next_issue < state.end {
        if state.cancel.is_cancelled() {
            return Err(cancelled(&state.path));
        }
        let requested = (state.end - state.next_issue).min(state.maximum_chunk_bytes as u64);
        let admission = if let Some(budget) = &state.budget {
            let Some(admission) = budget
                .reserve(
                    usize::try_from(requested).map_err(|_| {
                        entry_failure(
                            &state.path,
                            Operation::Read,
                            FailureClass::InvalidInput,
                            Transience::Permanent,
                        )
                    })?,
                    state.inflight.is_empty(),
                )
                .await
                .map_err(|_| cancelled(&state.path))?
            else {
                break;
            };
            Some(admission)
        } else {
            None
        };
        let granted = if let Some(qos) = &state.qos {
            qos.admit_read(requested, &state.cancel)
                .await
                .map_err(|_| cancelled(&state.path))?
        } else {
            requested
        };
        let count = usize::try_from(granted).map_err(|_| {
            entry_failure(
                &state.path,
                Operation::Read,
                FailureClass::InvalidInput,
                Transience::Permanent,
            )
        })?;
        let offset = state.next_issue;
        let cursor = Arc::clone(&state.cursor);
        let cancel = state.cancel.clone();
        state.inflight.push_back(Box::pin(async move {
            let result = tokio::select! {
                biased;
                () = cancel.cancelled() => Err(NfsProtocolFailure {
                    class: FailureClass::Cancelled,
                    transience: Transience::Transient,
                }),
                result = cursor.read_at(offset, count) => result,
            };
            (offset, count, result, admission)
        }));
        state.next_issue = state.next_issue.checked_add(granted).ok_or_else(|| {
            entry_failure(
                &state.path,
                Operation::Read,
                FailureClass::InvalidInput,
                Transience::Permanent,
            )
        })?;
    }
    Ok(())
}

async fn read_next(mut state: ReadState) -> Result<Option<(Bytes, ReadState)>, StorageRoleFailure> {
    fill_read_pipeline(&mut state).await?;
    if state.inflight.is_empty() {
        return Ok(None);
    }
    let next = tokio::select! {
        biased;
        () = state.cancel.cancelled() => return Err(cancelled(&state.path)),
        result = state.inflight.next() => result,
    };
    let (offset, count, result, admission) = next.ok_or_else(|| {
        entry_failure(
            &state.path,
            Operation::Read,
            FailureClass::Internal,
            Transience::Unknown,
        )
    })?;
    if offset != state.next_emit {
        return Err(entry_failure(
            &state.path,
            Operation::Read,
            FailureClass::Internal,
            Transience::Unknown,
        ));
    }
    let bytes = result.map_err(|error| role_failure(&state.path, Operation::Read, error))?;
    if bytes.len() != count {
        return Err(entry_failure(
            &state.path,
            Operation::Read,
            FailureClass::Corruption,
            Transience::Unknown,
        ));
    }
    if let Some(qos) = &state.qos {
        qos.record_read_bytes(bytes.len() as u64);
    }
    state.next_emit = state
        .next_emit
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| {
            entry_failure(
                &state.path,
                Operation::Read,
                FailureClass::InvalidInput,
                Transience::Permanent,
            )
        })?;
    if let (Some(budget), Some(admission)) = (&state.budget, admission) {
        budget.ready(offset, admission);
    }
    Ok(Some((bytes, state)))
}

pub(super) fn cancelled(path: &StoragePath) -> StorageRoleFailure {
    entry_failure(
        path,
        Operation::Read,
        FailureClass::Cancelled,
        Transience::Transient,
    )
}

pub(super) fn role_failure(
    path: &StoragePath,
    operation: Operation,
    error: NfsProtocolFailure,
) -> StorageRoleFailure {
    if error.class == FailureClass::Connectivity {
        return StorageRoleFailure::Session(
            BackendSessionFailure::new(
                operation,
                error.class,
                error.transience,
                "NFS session failed",
            )
            .unwrap_or_else(|_| unreachable!("static diagnostic is valid")),
        );
    }
    entry_failure(path, operation, error.class, error.transience)
}

pub(super) fn entry_failure(
    path: &StoragePath,
    operation: Operation,
    class: FailureClass,
    transience: Transience,
) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            operation,
            class,
            transience,
            "NFS role failed",
        )
        .unwrap_or_else(|_| unreachable!("static diagnostic is valid")),
    )
}

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;
