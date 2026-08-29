use std::sync::Arc;

use async_trait::async_trait;

use crate::model::{
    BackendIdentity, EntryKind, FailureClass, IdentityStrength, Operation, SourceIdentity,
    Transience,
};
use crate::storage::{ByteStream, ReadRequest, ReadSource, SourceDescriptor, StorageRoleFailure};

use super::{S3Protocol, S3ProtocolFailure};

pub(crate) struct S3ReadSource<P> {
    protocol: Arc<P>,
    identity: BackendIdentity,
    chunk_size: u64,
}

impl<P> S3ReadSource<P> {
    pub(crate) const fn new(protocol: Arc<P>, identity: BackendIdentity) -> Self {
        Self {
            protocol,
            identity,
            chunk_size: 8 * 1024 * 1024,
        }
    }
}

#[async_trait]
impl<P: S3Protocol + 'static> ReadSource for S3ReadSource<P> {
    async fn describe(
        &self,
        path: &crate::model::StoragePath,
    ) -> Result<SourceDescriptor, StorageRoleFailure> {
        let facts = self
            .protocol
            .head(path.as_str())
            .await
            .map_err(|e| role_failure(path, Operation::Observe, e))?;
        let stable = facts.version_id.as_deref().unwrap_or(&facts.etag);
        let source_identity = SourceIdentity::new(
            self.identity.clone(),
            if facts.version_id.is_some() {
                IdentityStrength::VersionScoped
            } else {
                IdentityStrength::PathScoped
            },
            stable,
        )
        .map_err(|e| entry(path, Operation::Observe, e.to_string()))?;
        Ok(SourceDescriptor {
            path: path.clone(),
            kind: EntryKind::File,
            size: Some(facts.size),
            source_identity,
            backend_fact: None,
        })
    }

    async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        if request.cancel.is_cancelled() {
            return Err(cancelled(&request.path, Operation::Read));
        }
        let facts = self
            .protocol
            .head(request.path.as_str())
            .await
            .map_err(|e| role_failure(&request.path, Operation::Read, e))?;
        let stable = facts.version_id.as_deref().unwrap_or(&facts.etag);
        let opened_identity = SourceIdentity::new(
            self.identity.clone(),
            if facts.version_id.is_some() {
                IdentityStrength::VersionScoped
            } else {
                IdentityStrength::PathScoped
            },
            stable,
        )
        .map_err(|e| entry(&request.path, Operation::Read, e.to_string()))?;
        if request
            .expected_source
            .as_ref()
            .is_some_and(|expected| expected != &opened_identity)
        {
            return Err(entry(
                &request.path,
                Operation::Read,
                "S3 source identity changed",
            ));
        }
        let requested = request.range.unwrap_or(0..facts.size);
        if requested.start > requested.end || requested.end > facts.size {
            return Err(entry(&request.path, Operation::Read, "invalid S3 range"));
        }
        let protocol = self.protocol.clone();
        let path = request.path;
        let chunk_size = self.chunk_size;
        let cancel = request.cancel;
        let qos = request.source_qos;
        let state = (
            protocol,
            path,
            requested.start,
            requested.end,
            chunk_size,
            cancel,
            qos,
        );
        Ok(Box::pin(futures::stream::try_unfold(
            state,
            |(protocol, path, offset, limit, chunk_size, cancel, qos)| async move {
                if offset == limit {
                    return Ok(None);
                }
                if cancel.is_cancelled() {
                    return Err(cancelled(&path, Operation::Read));
                }
                let requested = (offset + chunk_size).min(limit) - offset;
                let granted = if let Some(budget) = &qos {
                    budget
                        .admit_read(requested, &cancel)
                        .await
                        .map_err(|_| cancelled(&path, Operation::Read))?
                } else {
                    requested
                };
                let end = offset + granted;
                let bytes = protocol
                    .get_range(path.as_str(), offset..end)
                    .await
                    .map_err(|e| role_failure(&path, Operation::Read, e))?;
                if bytes.len() as u64 != end - offset {
                    return Err(entry(&path, Operation::Read, "short S3 range response"));
                }
                if let Some(budget) = &qos {
                    budget.record_read_bytes(bytes.len() as u64);
                }
                Ok(Some((
                    bytes,
                    (protocol, path, end, limit, chunk_size, cancel, qos),
                )))
            },
        )))
    }
}

#[allow(clippy::expect_used)]
pub(super) fn cancelled(
    path: &crate::model::StoragePath,
    operation: Operation,
) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        crate::model::EntryOperationFailure::new(
            path.clone(),
            operation,
            FailureClass::Cancelled,
            Transience::Permanent,
            "operation cancelled",
        )
        .expect("static cancellation failure is valid"),
    )
}

#[allow(clippy::expect_used)]
pub(super) fn role_failure(
    path: &crate::model::StoragePath,
    operation: Operation,
    failure: S3ProtocolFailure,
) -> StorageRoleFailure {
    match failure {
        S3ProtocolFailure::Entry {
            class,
            transience,
            diagnostic,
        } => StorageRoleFailure::Entry(
            crate::model::EntryOperationFailure::new(
                path.clone(),
                operation,
                class,
                transience,
                diagnostic,
            )
            .expect("protocol diagnostics are bounded before crossing the role seam"),
        ),
        S3ProtocolFailure::Session {
            class,
            transience,
            diagnostic,
        } => StorageRoleFailure::Session(
            crate::model::BackendSessionFailure::new(operation, class, transience, diagnostic)
                .expect("protocol diagnostics are bounded before crossing the role seam"),
        ),
    }
}

#[allow(clippy::expect_used)]
pub(super) fn entry(
    path: &crate::model::StoragePath,
    operation: Operation,
    diagnostic: impl Into<String>,
) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        crate::model::EntryOperationFailure::new(
            path.clone(),
            operation,
            FailureClass::Protocol,
            Transience::Unknown,
            diagnostic,
        )
        .unwrap_or_else(|_| {
            crate::model::EntryOperationFailure::new(
                path.clone(),
                operation,
                FailureClass::Internal,
                Transience::Permanent,
                "invalid adapter diagnostic",
            )
            .expect("static diagnostic is valid")
        }),
    )
}

pub(super) fn classified_entry(
    path: &crate::model::StoragePath,
    operation: Operation,
    class: FailureClass,
    transience: Transience,
    diagnostic: impl Into<String>,
) -> StorageRoleFailure {
    match crate::model::EntryOperationFailure::new(
        path.clone(),
        operation,
        class,
        transience,
        diagnostic,
    ) {
        Ok(failure) => StorageRoleFailure::Entry(failure),
        Err(_) => entry(path, operation, "invalid adapter diagnostic"),
    }
}
