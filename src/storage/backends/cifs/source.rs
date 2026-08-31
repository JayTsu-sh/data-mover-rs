use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};

use crate::model::{
    BackendIdentity, BackendSessionFailure, EntryKind, EntryOperationFailure, FailureClass,
    IdentityStrength, Operation, SourceIdentity, StoragePath, Transience,
};
use crate::storage::{ByteStream, ReadRequest, ReadSource, SourceDescriptor, StorageRoleFailure};

#[derive(Clone)]
pub(super) struct CifsSourceFacts {
    pub(super) kind: EntryKind,
    pub(super) size: u64,
    pub(super) identity: Bytes,
}

#[async_trait]
pub(super) trait CifsReadCursor: Send {
    fn maximum_read_chunk(&self) -> u32;
    async fn read_at(&mut self, offset: u64, count: u32) -> smb_domain::Result<Bytes>;
    async fn close(self: Box<Self>) -> smb_domain::Result<()>;
}

#[async_trait]
pub(super) trait CifsSourceProtocol: Send + Sync {
    async fn describe(&self, path: &StoragePath) -> smb_domain::Result<CifsSourceFacts>;
    async fn open(
        &self,
        path: &StoragePath,
    ) -> smb_domain::Result<(Box<dyn CifsReadCursor>, CifsSourceFacts)>;
}

pub(super) struct CifsReadSource {
    protocol: Arc<dyn CifsSourceProtocol>,
    identity: BackendIdentity,
}

impl CifsReadSource {
    pub(super) fn new<P>(protocol: Arc<P>, identity: BackendIdentity) -> Self
    where
        P: CifsSourceProtocol + 'static,
    {
        Self { protocol, identity }
    }

    async fn descriptor(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        let facts = self
            .protocol
            .describe(path)
            .await
            .map_err(|error| classify(path, Operation::Observe, &error))?;
        descriptor_from_facts(&self.identity, path, &facts, Operation::Observe)
    }

    async fn open_state(&self, request: ReadRequest) -> Result<ReadState, StorageRoleFailure> {
        let descriptor = self.descriptor(&request.path).await?;
        if descriptor.kind != EntryKind::File {
            return Err(entry_failure(
                &request.path,
                Operation::Read,
                FailureClass::Unsupported,
            ));
        }
        let range = checked_range(&request, descriptor.size.unwrap_or_default())?;
        let (cursor, opened_facts) = self
            .protocol
            .open(&request.path)
            .await
            .map_err(|error| classify(&request.path, Operation::Read, &error))?;
        let opened = descriptor_from_facts(
            &self.identity,
            &request.path,
            &opened_facts,
            Operation::Read,
        )?;
        if request
            .expected_source
            .as_ref()
            .is_some_and(|expected| expected != &opened.source_identity)
        {
            let _ = cursor.close().await;
            return Err(entry_failure(
                &request.path,
                Operation::Read,
                FailureClass::Conflict,
            ));
        }
        Ok(ReadState::new(request, cursor, range))
    }
}

#[async_trait]
impl ReadSource for CifsReadSource {
    async fn describe(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.descriptor(path).await
    }

    async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        if request.cancel.is_cancelled() {
            return Err(entry_failure(
                &request.path,
                Operation::Read,
                FailureClass::Cancelled,
            ));
        }
        let state = self.open_state(request).await?;
        Ok(Box::pin(futures::stream::try_unfold(state, read_next)))
    }
}

struct ReadState {
    cursor: Box<dyn CifsReadCursor>,
    path: StoragePath,
    next: u64,
    end: u64,
    cancel: tokio_util::sync::CancellationToken,
    qos: Option<crate::storage::SourceQosBudget>,
}

impl ReadState {
    fn new(request: ReadRequest, cursor: Box<dyn CifsReadCursor>, range: Range<u64>) -> Self {
        Self {
            cursor,
            path: request.path,
            next: range.start,
            end: range.end,
            cancel: request.cancel,
            qos: request.source_qos,
        }
    }
}

fn checked_range(request: &ReadRequest, size: u64) -> Result<Range<u64>, StorageRoleFailure> {
    let range = request.range.clone().unwrap_or(0..size);
    if range.end < range.start || range.end > size {
        return Err(entry_failure(
            &request.path,
            Operation::Read,
            FailureClass::InvalidInput,
        ));
    }
    Ok(range)
}

async fn read_next(mut state: ReadState) -> Result<Option<(Bytes, ReadState)>, StorageRoleFailure> {
    if state.next == state.end {
        return state
            .cursor
            .close()
            .await
            .map(|()| None)
            .map_err(|error| classify(&state.path, Operation::Read, &error));
    }
    if state.cancel.is_cancelled() {
        let _ = state.cursor.close().await;
        return Err(entry_failure(
            &state.path,
            Operation::Read,
            FailureClass::Cancelled,
        ));
    }
    let requested = (state.end - state.next).min(u64::from(state.cursor.maximum_read_chunk()));
    let granted = match &state.qos {
        Some(qos) => {
            let Ok(granted) = qos.admit_read(requested, &state.cancel).await else {
                let _ = state.cursor.close().await;
                return Err(entry_failure(
                    &state.path,
                    Operation::Read,
                    FailureClass::Cancelled,
                ));
            };
            granted
        }
        None => requested,
    };
    let count = u32::try_from(granted)
        .map_err(|_| entry_failure(&state.path, Operation::Read, FailureClass::InvalidInput))?;
    let read_result = tokio::select! {
        biased;
        () = state.cancel.cancelled() => {
            Err(entry_failure(&state.path, Operation::Read, FailureClass::Cancelled))
        }
        result = state.cursor.read_at(state.next, count) => {
            result.map_err(|error| classify(&state.path, Operation::Read, &error))
        }
    };
    let bytes = match read_result {
        Ok(bytes) => bytes,
        Err(error) => {
            let _ = state.cursor.close().await;
            return Err(error);
        }
    };
    if bytes.len() != count as usize {
        let _ = state.cursor.close().await;
        return Err(entry_failure(
            &state.path,
            Operation::Read,
            FailureClass::Corruption,
        ));
    }
    if let Some(qos) = &state.qos {
        qos.record_read_bytes(bytes.len() as u64);
    }
    state.next += bytes.len() as u64;
    Ok(Some((bytes, state)))
}

pub(super) fn descriptor_from_facts(
    identity: &BackendIdentity,
    path: &StoragePath,
    facts: &CifsSourceFacts,
    operation: Operation,
) -> Result<SourceDescriptor, StorageRoleFailure> {
    let mut identity_bytes =
        BytesMut::with_capacity(path.as_str().len() + facts.identity.len() + 8);
    identity_bytes.put_u32(u32::try_from(path.as_str().len()).unwrap_or(u32::MAX));
    identity_bytes.extend_from_slice(path.as_str().as_bytes());
    identity_bytes.extend_from_slice(&facts.identity);
    let source_identity = SourceIdentity::new(
        identity.clone(),
        IdentityStrength::PathScoped,
        identity_bytes.freeze(),
    )
    .map_err(|_| entry_failure(path, operation, FailureClass::Protocol))?;
    Ok(SourceDescriptor::new(
        path.clone(),
        facts.kind,
        Some(facts.size),
        source_identity,
    ))
}

pub(super) fn classify(
    path: &StoragePath,
    operation: Operation,
    error: &smb_domain::Error,
) -> StorageRoleFailure {
    let (class, transience) = match error {
        smb_domain::Error::ReceivedErrorMessage(status, _)
        | smb_domain::Error::UnexpectedMessageStatus(status) => classify_status(*status),
        smb_domain::Error::NotFound(_) => (FailureClass::NotFound, Transience::Permanent),
        smb_domain::Error::MissingPermissions(_) => {
            (FailureClass::PermissionDenied, Transience::Permanent)
        }
        smb_domain::Error::InvalidArgument(_) => {
            (FailureClass::InvalidInput, Transience::Permanent)
        }
        smb_domain::Error::UnsupportedOperation(_) => {
            (FailureClass::Unsupported, Transience::Permanent)
        }
        smb_domain::Error::Cancelled(_) => (FailureClass::Cancelled, Transience::Transient),
        smb_domain::Error::ConnectionStopped
        | smb_domain::Error::SessionInvalidated
        | smb_domain::Error::RuntimeTerminated
        | smb_domain::Error::TransportError(_) => {
            return StorageRoleFailure::Session(
                BackendSessionFailure::new(
                    operation,
                    FailureClass::Connectivity,
                    Transience::Unknown,
                    "CIFS session failed",
                )
                .unwrap_or_else(|_| unreachable!("static diagnostic is valid")),
            );
        }
        _ => (FailureClass::Protocol, Transience::Unknown),
    };
    if class == FailureClass::Connectivity {
        return StorageRoleFailure::Session(
            BackendSessionFailure::new(operation, class, transience, "CIFS session failed")
                .unwrap_or_else(|_| unreachable!("static diagnostic is valid")),
        );
    }
    entry_failure_with_transience(path, operation, class, transience)
}

fn classify_status(status: u32) -> (FailureClass, Transience) {
    use smb_domain::protocol::Status;

    match Status::try_from(status) {
        Ok(Status::ObjectNameNotFound | Status::ObjectPathNotFound) => {
            (FailureClass::NotFound, Transience::Permanent)
        }
        Ok(Status::AccessDenied) => (FailureClass::PermissionDenied, Transience::Permanent),
        Ok(Status::WrongPassword | Status::LogonFailure | Status::UserAccountLockedOut) => {
            (FailureClass::Authentication, Transience::Permanent)
        }
        Ok(Status::ObjectNameCollision) => (FailureClass::Conflict, Transience::Permanent),
        Ok(Status::SharingViolation | Status::DeletePending) => {
            (FailureClass::Conflict, Transience::Transient)
        }
        Ok(Status::DiskFull) => (FailureClass::Capacity, Transience::Permanent),
        Ok(Status::InvalidParameter | Status::ObjectNameInvalid) => {
            (FailureClass::InvalidInput, Transience::Permanent)
        }
        Ok(Status::NotImplemented | Status::NotSupported | Status::DeviceFeatureNotSupported) => {
            (FailureClass::Unsupported, Transience::Permanent)
        }
        Ok(Status::Cancelled) => (FailureClass::Cancelled, Transience::Transient),
        Ok(Status::IoTimeout | Status::NetworkNameDeleted | Status::NetworkSessionExpired) => {
            (FailureClass::Connectivity, Transience::Transient)
        }
        _ => (FailureClass::Protocol, Transience::Unknown),
    }
}

pub(super) fn entry_failure(
    path: &StoragePath,
    operation: Operation,
    class: FailureClass,
) -> StorageRoleFailure {
    entry_failure_with_transience(path, operation, class, Transience::Permanent)
}

fn entry_failure_with_transience(
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
            "CIFS entry operation failed",
        )
        .unwrap_or_else(|_| unreachable!("static diagnostic is valid")),
    )
}

#[cfg(test)]
mod classification_tests {
    use super::*;

    #[test]
    fn an_existing_destination_is_a_permanent_conflict() {
        assert_eq!(
            classify_status(smb_domain::protocol::Status::ObjectNameCollision as u32),
            (FailureClass::Conflict, Transience::Permanent)
        );
    }

    #[test]
    fn an_in_use_destination_remains_a_transient_conflict() {
        assert_eq!(
            classify_status(smb_domain::protocol::Status::SharingViolation as u32),
            (FailureClass::Conflict, Transience::Transient)
        );
    }
}
