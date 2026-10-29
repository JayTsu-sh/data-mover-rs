//! What prepare finds beside a final key (ADR-0006 C15b): the `.upload` pointer and the uploads
//! in progress on the key, as the discovery driver looks at them, and the contiguous prefix of an
//! upload's parts a resume continues from.

use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;

use super::super::source::{classified_entry, role_failure};
use super::super::{S3PartFacts, S3Protocol, S3ProtocolFailure};
use super::upload_pointer::{self, Removal, StoredPointer, UploadRecord, accepted};
use crate::model::{FailureClass, Operation, StoragePath, Transience};
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

/// Full, unambiguous parts in the frozen geometry, including islands past a gap. An opaque
/// `ETag` is a receipt, not a content hash. Malformed or duplicate part facts fail closed.
pub(super) fn validated_parts(
    mut parts: Vec<S3PartFacts>,
    part_size: u64,
    source_size: Option<u64>,
) -> Option<(u64, Vec<(i32, String)>)> {
    parts.sort_by_key(|part| part.number);
    let mut accepted = Vec::with_capacity(parts.len());
    let mut prefix = 0;
    let mut previous = 0;
    for part in parts {
        if !(1..=10_000).contains(&part.number) || part.number == previous || part.etag.is_empty() {
            return None;
        }
        let start = u64::try_from(part.number - 1)
            .ok()?
            .checked_mul(part_size)?;
        let end = start.checked_add(part.size)?;
        let full = part.size == part_size && source_size.is_none_or(|size| end <= size);
        let tail = part.size > 0 && part.size < part_size && source_size == Some(end);
        if !(full || tail) {
            return None;
        }
        if start == prefix {
            prefix = end;
        }
        previous = part.number;
        accepted.push((part.number, part.etag));
    }
    Some((prefix, accepted))
}

/// A final key's pointer and uploads, as the discovery driver looks at them.
pub(super) struct S3Artifacts<'a, P> {
    protocol: &'a P,
    final_path: &'a StoragePath,
    pointer: &'a StoragePath,
    request: &'a DestinationPrepareRequest,
    /// A single-request prepare already read this pointer. Discovery consumes it once instead
    /// of issuing a second HEAD/GET, then follows the ordinary validation path.
    preloaded: Mutex<Option<StoredPointer>>,
    /// The upload record of the pointer `read_pointer` found, when this backend accepts it.
    found: Mutex<Option<UploadRecord>>,
    checkpoint: Mutex<Option<Bytes>>,
    published: Mutex<Option<super::super::S3WriteFacts>>,
    /// That upload's contiguous prefix, as `observe_stage` listed it.
    prefix: Mutex<Vec<(i32, String)>>,
    /// The pointer `read_pointer` found (bytes and version), whether accepted or not: a clean-up
    /// deletes exactly that version, and a resume deletes it once its own pointer replaced it.
    found_pointer: Mutex<Option<StoredPointer>>,
    /// The bytes of the pointer `remove_pointer` had to leave in place (Object Lock).
    held: Mutex<Option<Vec<u8>>>,
}

/// What discovery left for a resume: the accepted pointer's upload record, that pointer as read
/// (its version and bytes), and the upload's contiguous prefix.
pub(super) struct Found {
    pub(super) record: Option<UploadRecord>,
    pub(super) pointer: Option<StoredPointer>,
    pub(super) prefix: Vec<(i32, String)>,
    pub(super) published: Option<super::super::S3WriteFacts>,
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
            preloaded: Mutex::new(None),
            found: Mutex::new(None),
            checkpoint: Mutex::new(None),
            published: Mutex::new(None),
            prefix: Mutex::new(Vec::new()),
            found_pointer: Mutex::new(None),
            held: Mutex::new(None),
        }
    }

    pub(super) fn with_preloaded(mut self, pointer: Option<StoredPointer>) -> Self {
        self.preloaded = Mutex::new(pointer);
        self
    }

    /// The bytes of a leftover pointer the clean-up could not remove: Object Lock holds it and
    /// the store refused a delete marker too. The new upload's fence treats it as no pointer.
    pub(super) fn held_pointer(&self) -> Option<Vec<u8>> {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// What discovery left: what a resume continues.
    pub(super) fn into_found(self) -> Found {
        Found {
            published: self
                .published
                .into_inner()
                .unwrap_or_else(PoisonError::into_inner),
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
        let preloaded = self
            .preloaded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let stored = match preloaded {
            Some(mut pointer) => {
                pointer.bytes.truncate(limit);
                Some(pointer)
            }
            None => upload_pointer::read(self.protocol, self.pointer, limit).await?,
        };
        let bytes = stored.as_ref().map(|stored| stored.bytes.clone());
        let decoded = bytes
            .as_deref()
            .and_then(|bytes| DestinationPointer::decode(bytes).ok());
        let record = decoded.as_ref().and_then(|pointer| self.accepts(pointer));
        if let Some(pointer) = &decoded
            && pointer.checkpoint.is_some()
            && pointer.binding == self.request.prepare.recovery_binding
            && pointer.transfer_identity == self.request.transfer_identity
            && self.request.resume == ResumeMode::Discover
        {
            let valid = pointer
                .checkpoint
                .as_deref()
                .and_then(|bytes| crate::storage::checkpoint::CheckpointData::decode(bytes).ok())
                .is_some_and(|data| {
                    Some(data.size) == self.request.prepare.source.size
                        && crate::storage::range_hash::RangeHash::decode(&data.hash, data.size)
                            .is_ok()
                });
            if !valid || record.is_none() {
                return Err(classified_entry(
                    self.final_path,
                    Operation::Prepare,
                    FailureClass::Corruption,
                    Transience::Permanent,
                    "invalid S3 source hash checkpoint or multipart evidence",
                ));
            }
        }
        *self
            .checkpoint
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = decoded
            .filter(|pointer| {
                self.request.resume == ResumeMode::Discover
                    && pointer.binding == self.request.prepare.recovery_binding
                    && pointer.transfer_identity == self.request.transfer_identity
            })
            .and_then(|pointer| pointer.checkpoint);
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
                let checkpointed = self
                    .checkpoint
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .is_some();
                let (bytes, prefix) = if checkpointed {
                    validated_parts(parts, record.part_size, size).ok_or_else(|| {
                        classified_entry(
                            self.final_path,
                            Operation::Prepare,
                            FailureClass::Corruption,
                            Transience::Permanent,
                            "S3 ListParts contradicts the checkpoint's frozen geometry",
                        )
                    })?
                } else {
                    contiguous_prefix(parts, record.part_size, size)
                };
                if checkpointed
                    && let Some(data) = self
                        .checkpoint
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .as_deref()
                        .and_then(|bytes| {
                            crate::storage::checkpoint::CheckpointData::decode(bytes).ok()
                        })
                    && bytes < data.prefix()
                    && self.request.resume == ResumeMode::Discover
                {
                    return Err(classified_entry(
                        self.final_path,
                        Operation::Prepare,
                        FailureClass::Corruption,
                        Transience::Permanent,
                        "S3 ListParts lost the committed checkpoint prefix",
                    ));
                }
                *self.prefix.lock().unwrap_or_else(PoisonError::into_inner) = prefix;
                Ok(Some(bytes))
            }
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(role_failure(self.final_path, Operation::Observe, error)),
        }
    }

    /// Removes the pointer read; one Object Lock keeps in place (the store refuses a delete
    /// marker too) is left there with a warning and remembered (see [`Self::held_pointer`]), so
    /// a held leftover does not stop every later transfer of the key.
    async fn remove_pointer(&self, _final_path: &StoragePath) -> Result<(), StorageRoleFailure> {
        let Some(found) = self.found_pointer() else {
            return upload_pointer::delete(self.protocol, self.pointer, None, &[])
                .await
                .map(drop);
        };
        let stale = [found.bytes.as_slice()];
        let removal = upload_pointer::delete(
            self.protocol,
            self.pointer,
            found.version.as_deref(),
            &stale,
        )
        .await?;
        if removal == Removal::Held {
            *self.held.lock().unwrap_or_else(PoisonError::into_inner) = Some(found.bytes);
        }
        Ok(())
    }

    async fn remove_stage(&self, _final_path: &StoragePath) -> Result<(), StorageRoleFailure> {
        upload_pointer::abort_uploads(self.protocol, self.final_path, None).await
    }

    fn accepts_pointer(&self, pointer: &DestinationPointer) -> bool {
        self.accepts(pointer).is_some()
    }

    fn continues_from_stage(&self) -> bool {
        self.checkpoint
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    async fn observe_final(&self, _path: &StoragePath) -> Result<Option<u64>, StorageRoleFailure> {
        let Some(found) = self.found_pointer() else {
            return Ok(None);
        };
        let pointer = DestinationPointer::decode(&found.bytes).map_err(|_| {
            classified_entry(
                self.final_path,
                Operation::Prepare,
                FailureClass::Corruption,
                Transience::Permanent,
                "invalid S3 published checkpoint",
            )
        })?;
        let facts =
            super::published_recovery::observe_final(self.protocol, self.final_path, &pointer)
                .await?;
        *self
            .published
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(facts);
        Ok(self.request.prepare.source.size)
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

#[cfg(test)]
mod tests {
    use super::{contiguous_prefix, validated_parts};
    use crate::storage::backends::s3::S3PartFacts;

    const PART: u64 = 8 * 1024 * 1024;

    fn part(number: i32, size: u64) -> S3PartFacts {
        S3PartFacts {
            number,
            size,
            etag: format!("etag-{number}"),
        }
    }

    /// An empty part counts only as the whole of an empty source; after a prefix that already
    /// ends at the source's size nothing more is taken, and an empty part of an unknown size, or
    /// before the end, stops the prefix.
    #[test]
    fn an_empty_part_counts_only_as_an_empty_source() {
        assert_eq!(
            contiguous_prefix(vec![part(1, 0)], PART, Some(0)),
            (0, vec![(1, "etag-1".to_string())])
        );
        let (bytes, prefix) = contiguous_prefix(vec![part(1, PART), part(2, 0)], PART, Some(PART));
        assert_eq!((bytes, prefix.len()), (PART, 1));
        assert_eq!(contiguous_prefix(vec![part(1, 0)], PART, None), (0, vec![]));
        assert_eq!(
            contiguous_prefix(vec![part(1, 0)], PART, Some(1)),
            (0, vec![])
        );
        assert_eq!(
            contiguous_prefix(vec![part(1, 0), part(2, PART)], PART, Some(PART)),
            (0, vec![])
        );
    }

    #[test]
    fn checkpoint_parts_keep_islands_after_gaps() {
        let observed = validated_parts(
            vec![part(4, 17), part(1, PART), part(3, PART)],
            PART,
            Some(3 * PART + 17),
        );
        assert_eq!(
            observed,
            Some((
                PART,
                vec![
                    (1, "etag-1".to_string()),
                    (3, "etag-3".to_string()),
                    (4, "etag-4".to_string())
                ]
            ))
        );
    }

    #[test]
    fn checkpoint_parts_reject_duplicate_numbers_and_invalid_geometry() {
        for parts in [
            vec![part(1, PART), part(1, PART)],
            vec![part(0, PART)],
            vec![part(10_001, PART)],
            vec![part(2, PART - 1)],
            vec![part(3, PART)],
        ] {
            assert!(validated_parts(parts, PART, Some(2 * PART)).is_none());
        }
    }
}
