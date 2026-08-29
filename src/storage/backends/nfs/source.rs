use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::model::{
    BackendIdentity, BackendSessionFailure, EntryKind, EntryOperationFailure, FailureClass,
    IdentityStrength, Operation, SourceIdentity, StoragePath, Transience,
};
use crate::storage::{ByteStream, ReadRequest, ReadSource, SourceDescriptor, StorageRoleFailure};

const MAX_ROLE_READ: u64 = 1024 * 1024;

#[derive(Clone)]
pub(crate) struct NfsSourceObservation {
    pub(crate) kind: EntryKind,
    pub(crate) size: Option<u64>,
    pub(crate) file_handle: Bytes,
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
pub(crate) trait NfsReadCursor: Send {
    async fn read_at(&mut self, offset: u64, count: usize) -> Result<Bytes, NfsProtocolFailure>;
}

#[async_trait]
pub(crate) trait NfsSourceProtocol: Send + Sync {
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
        })
    }
}

#[async_trait]
impl ReadSource for NfsReadSourceAdapter {
    async fn describe(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.descriptor(path).await
    }

    async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        if request.cancel.is_cancelled() {
            return Err(cancelled(&request.path));
        }
        let observed = self.descriptor(&request.path).await?;
        let range = request.range.unwrap_or(
            0..observed.size.ok_or_else(|| {
                entry_failure(
                    &request.path,
                    Operation::Read,
                    FailureClass::Unsupported,
                    Transience::Permanent,
                )
            })?,
        );
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
            cursor,
            path: request.path,
            next: range.start,
            end: range.end,
            cancel: request.cancel,
            qos: request.source_qos,
        };
        Ok(Box::pin(futures::stream::try_unfold(state, read_next)))
    }
}

struct ReadState {
    cursor: Box<dyn NfsReadCursor>,
    path: StoragePath,
    next: u64,
    end: u64,
    cancel: tokio_util::sync::CancellationToken,
    qos: Option<crate::storage::SourceQosBudget>,
}

async fn read_next(mut state: ReadState) -> Result<Option<(Bytes, ReadState)>, StorageRoleFailure> {
    if state.next == state.end {
        return Ok(None);
    }
    if state.cancel.is_cancelled() {
        return Err(cancelled(&state.path));
    }
    let requested = (state.end - state.next).min(MAX_ROLE_READ);
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
    let bytes = tokio::select! {
        biased;
        () = state.cancel.cancelled() => return Err(cancelled(&state.path)),
        result = state.cursor.read_at(state.next, count) => {
            result.map_err(|error| role_failure(&state.path, Operation::Read, error))?
        }
    };
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
    state.next += bytes.len() as u64;
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
