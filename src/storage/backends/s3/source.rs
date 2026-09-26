use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::model::{
    BackendIdentity, EntryKind, FailureClass, IdentityStrength, ModelValueError, Operation,
    SourceIdentity, SourceVersion, StoragePath, Transience,
};
use crate::storage::ListingFacts;
use crate::storage::{
    ByteStream, ReadRequest, ReadSource, SourceDescriptor, SourceQosBudget, StorageRoleFailure,
};
use tokio_util::sync::CancellationToken;

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
        let versions = self.protocol.bucket_capabilities().versions;
        let mut facts = match version.version_id() {
            None => self.protocol.head(path.as_str()).await,
            Some(_) if !versions => {
                return Err(classified_entry(
                    path,
                    operation,
                    FailureClass::Unsupported,
                    Transience::Permanent,
                    "this S3 bucket keeps no versions to select (bucket_type=nas)",
                ));
            }
            Some(version) => self.protocol.head_version(path.as_str(), version).await,
        }
        .map_err(|e| role_failure(path, operation, e))?;
        if !versions {
            // A bucket without versions pins nothing by version, even one whose HEAD names one (a
            // versioned object bucket marked `bucket_type=nas`): the `ETag` identifies the object.
            facts.version_id = None;
        }
        Ok(facts)
    }
}

/// The identity of the object at `path` as `facts` describe it: `VersionScoped(version id)` for a
/// real version, otherwise `PathScoped(path, ETag)` — the `"null"` version can be replaced, so its
/// `ETag` stays part of the identity, and the path keeps two objects with the same content at two
/// keys (every empty object has the same `ETag`) from sharing one identity key. An object without
/// an `ETag` and without a real version has nothing to guard its content and is refused.
pub(super) fn object_identity(
    backend: &BackendIdentity,
    path: &StoragePath,
    facts: &super::S3ObjectFacts,
) -> Result<SourceIdentity, ModelValueError> {
    match real_version(facts) {
        Some(version) => {
            SourceIdentity::new(backend.clone(), IdentityStrength::VersionScoped, version)
        }
        None => path_etag_identity(backend, path, &facts.etag),
    }
}

/// `PathScoped` identity of the current (or `"null"`) object at `path` with `etag`: the path's
/// length (u32, big-endian), the path, then the `ETag` — the length-prefixed path layout of the
/// CIFS path-scoped identity. An empty `ETag` is refused, as before the path was included.
pub(super) fn path_etag_identity(
    backend: &BackendIdentity,
    path: &StoragePath,
    etag: impl AsRef<[u8]>,
) -> Result<SourceIdentity, ModelValueError> {
    let etag = etag.as_ref();
    if etag.is_empty() {
        return Err(ModelValueError::new(
            "source_identity",
            "an S3 object without a version has no ETag to identify it",
        ));
    }
    SourceIdentity::new(
        backend.clone(),
        IdentityStrength::PathScoped,
        path_scoped_bytes(path, etag),
    )
}

/// `path` length-prefixed (u32, big-endian), then `tail`.
pub(super) fn path_scoped_bytes(path: &StoragePath, tail: &[u8]) -> Vec<u8> {
    let path = path.as_str().as_bytes();
    let mut bytes = Vec::with_capacity(4 + path.len() + tail.len());
    bytes.extend_from_slice(&u32::try_from(path.len()).unwrap_or(u32::MAX).to_be_bytes());
    bytes.extend_from_slice(path);
    bytes.extend_from_slice(tail);
    bytes
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

    /// A bucket with stored versions (not an ONTAP NAS bucket, P7 §14.2).
    fn supports_source_versions(&self) -> bool {
        self.protocol.bucket_capabilities().versions
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
        let source_identity = object_identity(&self.identity, path, &facts)
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
            inline_ownership: None,
            version: pinned_version(&facts, version),
            listing: ListingFacts::default(),
        })
    }

    /// A listing of current objects cannot see a version id, so it identifies an object by its
    /// path and `ETag` (`PathScoped`), where a describe on a versioned bucket pins the version
    /// (`VersionScoped`). The two name the same object when the listed identity is the described
    /// path with the described version's `ETag`; any other difference — another `ETag`, another
    /// version, another path — does not match.
    fn observation_matches(&self, observed: &SourceIdentity, described: &SourceDescriptor) -> bool {
        if observed == &described.source_identity {
            return true;
        }
        let Some(etag) = described.content_version.as_ref() else {
            return false;
        };
        observed.strength() == IdentityStrength::PathScoped
            && path_etag_identity(&self.identity, &described.path, etag)
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
        let opened_identity = object_identity(&self.identity, &request.path, &facts)
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
        // Without conditional reads nothing pins the ranges to the described object: each
        // response is checked against it, and one more HEAD after the last range closes the window
        // at the tail of the last response (P7 §14.2, decision 7).
        let confirm = !self.protocol.bucket_capabilities().conditional_reads
            && requested.start < requested.end;
        let state = ReadState {
            protocol: self.protocol.clone(),
            path: request.path,
            offset: requested.start,
            limit: requested.end,
            chunk_size: self.chunk_size.min(request.maximum_chunk_bytes as u64),
            cancel: request.cancel,
            qos: request.source_qos,
            facts,
            confirm,
        };
        Ok(Box::pin(futures::stream::try_unfold(state, |state| {
            next_range(state)
        })))
    }
}

/// Where a streamed read stands.
struct ReadState<P> {
    protocol: Arc<P>,
    path: StoragePath,
    offset: u64,
    limit: u64,
    chunk_size: u64,
    cancel: CancellationToken,
    qos: Option<SourceQosBudget>,
    facts: S3ObjectFacts,
    /// Confirm with a HEAD, once the last range is read, that the object is still the described
    /// one (a bucket without conditional reads).
    confirm: bool,
}

/// The next range of a streamed read, or its end.
async fn next_range<P: S3Protocol>(
    mut state: ReadState<P>,
) -> Result<Option<(Bytes, ReadState<P>)>, StorageRoleFailure> {
    let path = &state.path;
    if state.offset == state.limit {
        if state.confirm {
            confirm_unchanged(&*state.protocol, path, &state.facts).await?;
        }
        return Ok(None);
    }
    if state.cancel.is_cancelled() {
        return Err(cancelled(path, Operation::Read));
    }
    let requested = (state.offset + state.chunk_size).min(state.limit) - state.offset;
    let granted = if let Some(budget) = &state.qos {
        budget
            .admit_read(requested, &state.cancel)
            .await
            .map_err(|_| cancelled(path, Operation::Read))?
    } else {
        requested
    };
    let end = state.offset + granted;
    let bytes = state
        .protocol
        .get_range(path.as_str(), state.offset..end, &state.facts)
        .await
        .map_err(|e| role_failure(path, Operation::Read, e))?;
    if bytes.len() as u64 != end - state.offset {
        return Err(entry(path, Operation::Read, "short S3 range response"));
    }
    if let Some(budget) = &state.qos {
        budget.record_read_bytes(bytes.len() as u64);
    }
    state.offset = end;
    Ok(Some((bytes, state)))
}

/// After the last range of an unpinned read: the object must still be the one described — same
/// `ETag`, size and (when both have one) `Last-Modified` — or the read is a permanent `Conflict`,
/// as a 412 would have made it where reads are pinned.
async fn confirm_unchanged<P: S3Protocol>(
    protocol: &P,
    path: &StoragePath,
    described: &S3ObjectFacts,
) -> Result<(), StorageRoleFailure> {
    let changed = || {
        classified_entry(
            path,
            Operation::Read,
            FailureClass::Conflict,
            Transience::Permanent,
            "S3 source changed during read",
        )
    };
    let current = match protocol.head(path.as_str()).await {
        Ok(current) => current,
        Err(S3ProtocolFailure::Entry {
            class: FailureClass::NotFound,
            ..
        }) => return Err(changed()),
        Err(failure) => return Err(role_failure(path, Operation::Read, failure)),
    };
    let modified = matches!(
        (current.last_modified, described.last_modified),
        (Some(now), Some(then)) if now != then
    );
    if current.etag.trim_matches('"') != described.etag.trim_matches('"')
        || current.size != described.size
        || modified
    {
        return Err(changed());
    }
    Ok(())
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
