use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

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
    pub(super) maximum_read_chunk: u32,
}

#[async_trait]
pub(super) trait CifsReadCursor: Send + Sync {
    fn maximum_read_chunk(&self) -> u32;
    async fn read_at(&self, offset: u64, count: u32) -> smb_domain::Result<Bytes>;
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

/// `STATUS_NOT_A_DIRECTORY`: a directory open hit a file.
const STATUS_NOT_A_DIRECTORY: u32 = 0xC000_0103;
/// `STATUS_CANNOT_DELETE`: the read-only attribute (permanent) or a mapped / in-use file
/// (clears on its own). The server does not distinguish them, so the transience is `Unknown`
/// rather than a guess that would either foreclose or force a retry.
const STATUS_CANNOT_DELETE: u32 = 0xC000_0121;

pub(super) struct CifsReadSource {
    protocol: Arc<dyn CifsSourceProtocol>,
    identity: BackendIdentity,
    maximum_read_chunk: AtomicU32,
    read_inflight: usize,
}

impl CifsReadSource {
    pub(super) fn new<P>(protocol: Arc<P>, identity: BackendIdentity) -> Self
    where
        P: CifsSourceProtocol + 'static,
    {
        Self {
            protocol,
            identity,
            maximum_read_chunk: AtomicU32::new(u32::MAX),
            read_inflight: 8,
        }
    }

    pub(super) fn with_read_inflight(mut self, depth: std::num::NonZeroUsize) -> Self {
        self.read_inflight = depth.get();
        self
    }

    async fn descriptor(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        let facts = self
            .protocol
            .describe(path)
            .await
            .map_err(|error| classify(path, Operation::Observe, &error))?;
        self.maximum_read_chunk
            .store(facts.maximum_read_chunk.max(1), Ordering::Relaxed);
        descriptor_from_facts(&self.identity, path, &facts, Operation::Observe)
    }

    async fn open_state(&self, mut request: ReadRequest) -> Result<ReadState, StorageRoleFailure> {
        let (cursor, opened_facts) = self
            .protocol
            .open(&request.path)
            .await
            .map_err(|error| classify(&request.path, Operation::Read, &error))?;
        let opened = match descriptor_from_facts(
            &self.identity,
            &request.path,
            &opened_facts,
            Operation::Read,
        ) {
            Ok(value) => value,
            Err(error) => {
                let _ = cursor.close().await;
                return Err(error);
            }
        };
        let range = match checked_range(&request, opened_facts.size) {
            Ok(range) if opened.kind == EntryKind::File => range,
            _ => {
                let _ = cursor.close().await;
                return Err(entry_failure(
                    &request.path,
                    Operation::Read,
                    FailureClass::InvalidInput,
                ));
            }
        };
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
        request.read_inflight = request.read_inflight.min(self.read_inflight);
        Ok(ReadState::new(request, cursor, range))
    }
}

#[async_trait]
impl ReadSource for CifsReadSource {
    fn supports_read_budget(&self) -> bool {
        true
    }
    fn maximum_read_chunk_bytes(&self) -> usize {
        usize::try_from(self.maximum_read_chunk.load(Ordering::Relaxed)).unwrap_or(usize::MAX)
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
    ) -> Result<crate::storage::PositionedByteStream, StorageRoleFailure> {
        if request.cancel.is_cancelled() {
            return Err(entry_failure(
                &request.path,
                Operation::Read,
                FailureClass::Cancelled,
            ));
        }
        if request.maximum_chunk_bytes == 0 || request.read_inflight == 0 {
            return Err(entry_failure(
                &request.path,
                Operation::Read,
                FailureClass::InvalidInput,
            ));
        }
        Ok(super::read_pipeline::positioned_stream(
            self.open_state(request).await?,
        ))
    }

    async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        if request.cancel.is_cancelled() {
            return Err(entry_failure(
                &request.path,
                Operation::Read,
                FailureClass::Cancelled,
            ));
        }
        if request.maximum_chunk_bytes == 0 || request.read_inflight == 0 {
            return Err(entry_failure(
                &request.path,
                Operation::Read,
                FailureClass::InvalidInput,
            ));
        }
        let state = self.open_state(request).await?;
        Ok(super::read_pipeline::stream(state))
    }
}

pub(super) struct ReadState {
    pub(super) cursor: Box<dyn CifsReadCursor>,
    pub(super) request: ReadRequest,
    pub(super) range: Range<u64>,
}

impl ReadState {
    fn new(request: ReadRequest, cursor: Box<dyn CifsReadCursor>, range: Range<u64>) -> Self {
        Self {
            cursor,
            request,
            range,
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

    // Statuses the pinned smb-rs `Status` enum does not model yet.
    match status {
        STATUS_NOT_A_DIRECTORY => return (FailureClass::Conflict, Transience::Permanent),
        STATUS_CANNOT_DELETE => return (FailureClass::PermissionDenied, Transience::Unknown),
        _ => {}
    }
    match Status::try_from(status) {
        Ok(Status::ObjectNameNotFound | Status::ObjectPathNotFound) => {
            (FailureClass::NotFound, Transience::Permanent)
        }
        Ok(Status::AccessDenied) => (FailureClass::PermissionDenied, Transience::Permanent),
        Ok(Status::WrongPassword | Status::LogonFailure | Status::UserAccountLockedOut) => {
            (FailureClass::Authentication, Transience::Permanent)
        }
        Ok(Status::ObjectNameCollision | Status::DirectoryNotEmpty) => {
            (FailureClass::Conflict, Transience::Permanent)
        }
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
    entry_failure_with_transience(
        path,
        operation,
        class,
        if class == FailureClass::Cancelled {
            Transience::Transient
        } else {
            Transience::Permanent
        },
    )
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
