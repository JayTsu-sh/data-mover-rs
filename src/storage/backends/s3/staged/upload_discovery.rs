//! What prepare finds beside a final key (ADR-0006 C15b): the `.upload` pointer and the uploads
//! in progress on the key, as the discovery driver looks at them, and the contiguous prefix of an
//! upload's parts a resume continues from.

use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;

use super::super::source::role_failure;
use super::super::{S3PartFacts, S3Protocol, S3ProtocolFailure};
use super::upload_pointer::{self, StoredPointer, UploadRecord, accepted};
use crate::model::{FailureClass, Operation, StoragePath};
use crate::storage::discovery::DestinationArtifacts;
use crate::storage::pointer::DestinationPointer;
use crate::storage::{DestinationPrepareRequest, RestartReason, ResumeMode, StorageRoleFailure};

fn is_not_found(failure: &S3ProtocolFailure) -> bool {
    matches!(
        failure,
        S3ProtocolFailure::Entry {
            class: FailureClass::NotFound,
            ..
        }
    )
}

/// The contiguous prefix of an upload's parts and the bytes it holds: parts `1..=k`, each exactly
/// `part_size` bytes and within the source's size, except that the last may be shorter when it
/// ends exactly at the source's size. Everything from the first part that breaks the rule on is
/// uploaded again.
pub(super) fn contiguous_prefix(
    mut parts: Vec<S3PartFacts>,
    part_size: u64,
    source_size: Option<u64>,
) -> (u64, Vec<(i32, String)>) {
    parts.sort_by_key(|part| part.number);
    let mut bytes = 0_u64;
    let mut prefix = Vec::new();
    for part in parts {
        let number = i32::try_from(prefix.len() + 1).unwrap_or(i32::MAX);
        let end = bytes.saturating_add(part.size);
        let full = part.size == part_size && source_size.is_none_or(|size| end <= size);
        let short_last = part.size < part_size && source_size == Some(end);
        if part.number != number || !(full || short_last) {
            break;
        }
        bytes = end;
        prefix.push((part.number, part.etag));
        if source_size == Some(bytes) {
            break;
        }
    }
    (bytes, prefix)
}

/// A final key's pointer and uploads, as the discovery driver looks at them.
pub(super) struct S3Artifacts<'a, P> {
    protocol: &'a P,
    final_path: &'a StoragePath,
    pointer: &'a StoragePath,
    request: &'a DestinationPrepareRequest,
    /// The upload record of the pointer `read_pointer` found, when this backend accepts it.
    found: Mutex<Option<UploadRecord>>,
    /// That upload's contiguous prefix, as `observe_stage` listed it.
    prefix: Mutex<Vec<(i32, String)>>,
    /// The pointer `read_pointer` found (bytes and version), whether accepted or not: a clean-up
    /// deletes exactly that version, and a resume deletes it once its own pointer replaced it.
    found_pointer: Mutex<Option<StoredPointer>>,
}

/// What discovery left for a resume: the accepted pointer's upload record, that pointer as read
/// (its version and bytes), and the upload's contiguous prefix.
pub(super) struct Found {
    pub(super) record: Option<UploadRecord>,
    pub(super) pointer: Option<StoredPointer>,
    pub(super) prefix: Vec<(i32, String)>,
}

impl<'a, P> S3Artifacts<'a, P> {
    pub(super) fn new(
        protocol: &'a P,
        final_path: &'a StoragePath,
        pointer: &'a StoragePath,
        request: &'a DestinationPrepareRequest,
    ) -> Self {
        Self {
            protocol,
            final_path,
            pointer,
            request,
            found: Mutex::new(None),
            prefix: Mutex::new(Vec::new()),
            found_pointer: Mutex::new(None),
        }
    }

    /// What discovery left: what a resume continues.
    pub(super) fn into_found(self) -> Found {
        Found {
            record: self
                .found
                .into_inner()
                .unwrap_or_else(PoisonError::into_inner),
            pointer: self
                .found_pointer
                .into_inner()
                .unwrap_or_else(PoisonError::into_inner),
            prefix: self
                .prefix
                .into_inner()
                .unwrap_or_else(PoisonError::into_inner),
        }
    }

    fn found_pointer(&self) -> Option<StoredPointer> {
        self.found_pointer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn accepts(&self, pointer: &DestinationPointer) -> Option<UploadRecord> {
        accepted(pointer).filter(|record| record.holds(self.request.prepare.source.size))
    }
}

#[async_trait]
impl<P: S3Protocol> DestinationArtifacts for S3Artifacts<'_, P> {
    async fn read_pointer(
        &self,
        _final_path: &StoragePath,
        limit: usize,
    ) -> Result<Option<Vec<u8>>, StorageRoleFailure> {
        let stored = upload_pointer::read(self.protocol, self.pointer, limit).await?;
        let bytes = stored.as_ref().map(|stored| stored.bytes.clone());
        let record = bytes
            .as_deref()
            .and_then(|bytes| DestinationPointer::decode(bytes).ok())
            .and_then(|pointer| self.accepts(&pointer));
        *self.found.lock().unwrap_or_else(PoisonError::into_inner) = record;
        *self
            .found_pointer
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = stored;
        Ok(bytes)
    }

    /// With a pointer: the contiguous prefix of its upload's parts, or `None` when the upload is
    /// gone. Without one: `Some(0)` when any upload is in progress on the key.
    async fn observe_stage(
        &self,
        _final_path: &StoragePath,
    ) -> Result<Option<u64>, StorageRoleFailure> {
        let key = self.final_path.as_str();
        let record = self
            .found
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let Some(record) = record else {
            let uploads = self
                .protocol
                .list_uploads(key)
                .await
                .map_err(|error| role_failure(self.final_path, Operation::Observe, error))?;
            return Ok((!uploads.is_empty()).then_some(0));
        };
        match self.protocol.list_parts(key, &record.upload_id).await {
            Ok(parts) => {
                let size = self.request.prepare.source.size;
                let (bytes, prefix) = contiguous_prefix(parts, record.part_size, size);
                *self.prefix.lock().unwrap_or_else(PoisonError::into_inner) = prefix;
                Ok(Some(bytes))
            }
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(role_failure(self.final_path, Operation::Observe, error)),
        }
    }

    async fn remove_pointer(&self, _final_path: &StoragePath) -> Result<(), StorageRoleFailure> {
        let Some(found) = self.found_pointer() else {
            return upload_pointer::delete(self.protocol, self.pointer, None, &[]).await;
        };
        let stale = [found.bytes.as_slice()];
        upload_pointer::delete(
            self.protocol,
            self.pointer,
            found.version.as_deref(),
            &stale,
        )
        .await
    }

    async fn remove_stage(&self, _final_path: &StoragePath) -> Result<(), StorageRoleFailure> {
        upload_pointer::abort_uploads(self.protocol, self.final_path, None).await
    }

    fn accepts_pointer(&self, pointer: &DestinationPointer) -> bool {
        self.accepts(pointer).is_some()
    }
}

/// Why a leftover pointer beside a small object is cleaned up: a single `PutObject` resumes
/// nothing.
pub(super) fn leftover_reason(request: &DestinationPrepareRequest, bytes: &[u8]) -> RestartReason {
    if request.resume == ResumeMode::Restart {
        return RestartReason::Requested;
    }
    match DestinationPointer::decode(bytes)
        .ok()
        .filter(|pointer| accepted(pointer).is_some())
    {
        None => RestartReason::PointerCorrupt,
        Some(pointer) if pointer.transfer_identity != request.transfer_identity => {
            RestartReason::OtherTransfer
        }
        Some(pointer) if pointer.binding != request.prepare.recovery_binding => {
            RestartReason::BindingChanged
        }
        Some(_) => RestartReason::Requested,
    }
}
