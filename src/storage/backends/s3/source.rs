use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::model::{
    BackendIdentity, EntryKind, FailureClass, IdentityStrength, Operation, SourceIdentity,
    SourceVersion, StoragePath, Transience,
};
use crate::storage::ListingFacts;
use crate::storage::{ByteStream, ReadRequest, ReadSource, SourceDescriptor, StorageRoleFailure};

use super::{S3ObjectFacts, S3Protocol, S3ProtocolFailure, is_real_version_id};

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

/// A real version id: not absent, empty or `"null"` — the id an unversioned bucket, or an object
/// written before versioning, reports.
fn real_version(facts: &S3ObjectFacts) -> Option<&str> {
    facts
        .version_id
        .as_deref()
        .filter(|version| is_real_version_id(version))
}

/// The version a describe pins: the requested one, or for `Current` the real version it found.
/// An object without one stays `Current`, guarded by its `ETag` alone, as before versions.
fn pinned_version(facts: &S3ObjectFacts, requested: &SourceVersion) -> SourceVersion {
    match requested {
        SourceVersion::Id(_) => requested.clone(),
        SourceVersion::Current => real_version(facts).map_or(SourceVersion::Current, |version| {
            SourceVersion::Id(version.to_string())
        }),
    }
}

/// Whether a HEAD for `requested` answered with that version. The `"null"` version is reported as
/// `"null"` or not at all.
fn answers_version(facts: &S3ObjectFacts, requested: &str) -> bool {
    match facts.version_id.as_deref() {
        Some(reported) => reported == requested,
        None => requested == "null",
    }
}

impl<P: S3Protocol + 'static> S3ReadSource<P> {
    async fn head(
        &self,
        path: &StoragePath,
        version: &SourceVersion,
        operation: Operation,
    ) -> Result<S3ObjectFacts, StorageRoleFailure> {
        match version.version_id() {
            None => self.protocol.head(path.as_str()).await,
            Some(version) => self.protocol.head_version(path.as_str(), version).await,
        }
        .map_err(|e| role_failure(path, operation, e))
    }
}

pub(super) fn object_identity(
    backend: &BackendIdentity,
    facts: &super::S3ObjectFacts,
) -> Result<SourceIdentity, crate::model::ModelValueError> {
    // The null version can be replaced; its ETag must remain part of source identity.
    let version = real_version(facts);
    SourceIdentity::new(
        backend.clone(),
        if version.is_some() {
            IdentityStrength::VersionScoped
        } else {
            IdentityStrength::PathScoped
        },
        version.unwrap_or(&facts.etag),
    )
}

#[async_trait]
impl<P: S3Protocol + 'static> ReadSource for S3ReadSource<P> {
    fn maximum_read_chunk_bytes(&self) -> usize {
        usize::try_from(self.chunk_size).unwrap_or(usize::MAX)
    }

    async fn describe(
        &self,
        path: &crate::model::StoragePath,
    ) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.describe_version(path, &SourceVersion::Current).await
    }

    fn supports_source_versions(&self) -> bool {
        true
    }

    /// Describes one version, pinning it for every later read: `Current` becomes the real version
    /// it found. The `ETag` is the content version, so a binding changes when the bytes of a
    /// version do.
    async fn describe_version(
        &self,
        path: &StoragePath,
        version: &SourceVersion,
    ) -> Result<SourceDescriptor, StorageRoleFailure> {
        let facts = self.head(path, version, Operation::Observe).await?;
        if let Some(requested) = version.version_id()
            && !answers_version(&facts, requested)
        {
            // A store that ignores `?versionId=` answers with the current object; copying that
            // under the requested version's name would be silently wrong.
            return Err(classified_entry(
                path,
                Operation::Observe,
                FailureClass::Unsupported,
                Transience::Permanent,
                "the S3 store did not answer with the requested version",
            ));
        }
        let source_identity = object_identity(&self.identity, &facts)
            .map_err(|e| entry(path, Operation::Observe, e.to_string()))?;
        Ok(SourceDescriptor {
            path: path.clone(),
            kind: EntryKind::File,
            size: Some(facts.size),
            source_identity,
            backend_fact: None,
            content_version: (!facts.etag.is_empty())
                .then(|| Bytes::copy_from_slice(facts.etag.as_bytes())),
            inline_timestamps: None,
            inline_mode: None,
            version: pinned_version(&facts, version),
            listing: ListingFacts::default(),
        })
    }

    /// A listing of current objects cannot see a version id, so it identifies an object by its
    /// `ETag` (`PathScoped`), where a describe on a versioned bucket pins the version
    /// (`VersionScoped`). The two name the same object when the listed `ETag` is the described
    /// version's `ETag`; any other difference — another `ETag`, another version — does not match.
    fn observation_matches(&self, observed: &SourceIdentity, described: &SourceDescriptor) -> bool {
        if observed == &described.source_identity {
            return true;
        }
        let Some(etag) = described.content_version.as_ref() else {
            return false;
        };
        observed.strength() == IdentityStrength::PathScoped
            && SourceIdentity::new(self.identity.clone(), IdentityStrength::PathScoped, etag)
                .is_ok_and(|listed| &listed == observed)
    }

    async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        if request.cancel.is_cancelled() {
            return Err(cancelled(&request.path, Operation::Read));
        }
        if request.maximum_chunk_bytes == 0 || request.read_inflight == 0 {
            return Err(entry(
                &request.path,
                Operation::Read,
                "invalid streaming read limits",
            ));
        }
        let facts = self
            .head(&request.path, &request.version, Operation::Read)
            .await?;
        let opened_identity = object_identity(&self.identity, &facts)
            .map_err(|e| entry(&request.path, Operation::Read, e.to_string()))?;
        if request
            .expected_source
            .as_ref()
            .is_some_and(|expected| expected != &opened_identity)
        {
            return Err(classified_entry(
                &request.path,
                Operation::Read,
                FailureClass::Conflict,
                Transience::Permanent,
                "S3 source identity changed",
            ));
        }
        let requested = request.range.unwrap_or(0..facts.size);
        if requested.start > requested.end || requested.end > facts.size {
            return Err(entry(&request.path, Operation::Read, "invalid S3 range"));
        }
        let protocol = self.protocol.clone();
        let path = request.path;
        let chunk_size = self.chunk_size.min(request.maximum_chunk_bytes as u64);
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
            facts,
        );
        Ok(Box::pin(futures::stream::try_unfold(
            state,
            |(protocol, path, offset, limit, chunk_size, cancel, qos, facts)| async move {
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
                    .get_range(path.as_str(), offset..end, &facts)
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
                    (protocol, path, end, limit, chunk_size, cancel, qos, facts),
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
