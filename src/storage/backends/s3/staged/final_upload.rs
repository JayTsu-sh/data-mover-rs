//! The state of a multipart upload on the final key (ADR-0006 C15b), shared by the streaming
//! writer ([`at_destination`](super::at_destination)) and the native copy that fills it with
//! `UploadPartCopy` ([`native_final`](super::native_final), C18).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use uuid::Uuid;

use super::super::S3WriteFacts;
use super::super::source::classified_entry;
use super::PART_SIZE;
use super::parts::UploadedParts;
use super::upload_pointer::{NONCE_BYTES, UploadRecord};
use crate::model::{FailureClass, ObjectTag, Operation, StoragePath, Transience};
use crate::storage::{DestinationPrepareRequest, PreparedStage, StorageRoleFailure};

/// A multipart upload on the final key, kept at the destination.
pub(super) struct FinalUpload {
    pub(super) upload_id: String,
    pub(super) part_size: usize,
    /// The source's size, when known: a different byte count is never completed.
    pub(super) expected_size: Option<u64>,
    pub(super) transfer_identity: [u8; 32],
    pub(super) nonce: [u8; NONCE_BYTES],
    pub(super) pointer: StoragePath,
    /// Whether this stage wrote its pointer; a pointer gone after that was removed by another
    /// writer.
    pub(super) pointer_written: AtomicBool,
    /// The bytes of a leftover pointer discovery could not remove — Object Lock holds it and the
    /// store refuses a delete marker too (Ceph RGW / DXN). Until this stage writes its own, a
    /// pointer holding exactly these bytes fences as no pointer, not as another writer's.
    pub(super) held_pointer: Option<Vec<u8>>,
    pub(super) state: Mutex<UploadState>,
}

#[derive(Default)]
pub(super) struct UploadState {
    /// H from the last successful pointer PUT, never merely the producer's in-memory hash.
    pub(super) anchored_hash: Option<crate::storage::range_hash::RangeHash>,
    /// Every part the completion names (number, `ETag`): the resumed prefix, then what `write`
    /// sent.
    pub(super) parts: Vec<(i32, String)>,
    /// Whether every part `write` sent came back with its MD5 as `ETag`; `None` when it sent none.
    /// The parts of one upload share its encryption, so the resumed prefix answers the same way.
    pub(super) md5_etags: Option<bool>,
    /// The bytes the parts hold once `write` finished.
    pub(super) written: Option<u64>,
    /// Whether this attempt filled the upload with server-side copies (`UploadPartCopy`) rather
    /// than bytes it sent.
    pub(super) native: bool,
    /// Tags to set once the object exists.
    pub(super) tags: Option<Vec<ObjectTag>>,
    /// What the completion reported.
    pub(super) published: Option<S3WriteFacts>,
    pub(super) checkpoint_released: bool,
    /// Only explicitly authorized marker ids (for example, a known delete receipt). Never
    /// infer ownership from surrounding payloads; new v2 cleanup creates no markers.
    pub(super) checkpoint_cleanup_markers: Vec<String>,
    pub(super) recovered_receipts: Option<[u8; 32]>,
    /// The version this stage's pointer write reported (`"null"` included), which deleting it
    /// names; `None` in an unversioned bucket — or when a resume could not delete the version it
    /// replaced, so that a plain delete hides both behind a marker.
    pub(super) pointer_version: Option<String>,
    pub(super) latest_pointer_version: Option<String>,
    pointer_hide: bool,
    /// Contents no pointer version may keep once ours is deleted: ours, and a replaced one's.
    pub(super) stale_pointers: Vec<Vec<u8>>,
}

impl FinalUpload {
    pub(super) fn new(
        request: &DestinationPrepareRequest,
        upload_id: String,
        part_size: u64,
        pointer: StoragePath,
    ) -> Result<Self, StorageRoleFailure> {
        let path = request.prepare.final_destination.path();
        Ok(Self {
            upload_id,
            part_size: usize::try_from(part_size).map_err(|_| {
                classified_entry(
                    path,
                    Operation::Prepare,
                    FailureClass::Unsupported,
                    Transience::Permanent,
                    "S3 part size cannot fit address space",
                )
            })?,
            expected_size: request.prepare.source.size,
            transfer_identity: request.transfer_identity,
            nonce: *Uuid::new_v4().as_bytes(),
            pointer,
            pointer_written: AtomicBool::new(false),
            held_pointer: None,
            state: Mutex::new(UploadState::default()),
        })
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, UploadState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn pointer_written(&self) -> bool {
        self.pointer_written.load(Ordering::Acquire)
    }

    /// Records what `write` sent: `size` bytes in all, which must be the source's size when it is
    /// known.
    pub(super) fn record_written(
        &self,
        path: &StoragePath,
        sent: UploadedParts,
        resumed_parts: usize,
        size: u64,
    ) -> Result<(), StorageRoleFailure> {
        if self.expected_size.is_some_and(|expected| expected != size) {
            return Err(classified_entry(
                path,
                Operation::Write,
                FailureClass::InvalidInput,
                Transience::Permanent,
                "the S3 upload input differs from the source's size",
            ));
        }
        let mut state = self.lock();
        state.md5_etags = (sent.parts.len() > resumed_parts).then_some(sent.md5_etags);
        state.parts = sent.parts;
        state.written = Some(size);
        Ok(())
    }

    /// Records the pointer this stage wrote: the version its write reported and its bytes.
    pub(super) fn record_pointer(&self, version: Option<String>, bytes: Vec<u8>) {
        let mut state = self.lock();
        state.latest_pointer_version.clone_from(&version);
        state.pointer_version = if state.pointer_hide { None } else { version };
        // Hash snapshots can approach 1 MiB; retaining one per intent batch is unbounded.
        // The cleanup matcher recognizes all v2 generations with this nonce, so retain only
        // the latest one and any previous owner's pointer needed by takeover cleanup.
        if crate::storage::pointer::DestinationPointer::decode(&bytes)
            .is_ok_and(|pointer| pointer.checkpoint.is_some())
        {
            state.stale_pointers.retain(|old| {
                crate::storage::pointer::DestinationPointer::decode(old)
                    .ok()
                    .and_then(|pointer| super::upload_pointer::accepted(&pointer))
                    .is_none_or(|record| record.nonce != self.nonce)
            });
        }
        state.stale_pointers.push(bytes);
    }

    /// Records the pointer a resume replaced. When its version could not be deleted, deleting
    /// ours by version would make it current again: ours is then hidden behind a delete marker.
    pub(super) fn forget_replaced(&self, bytes: Vec<u8>, deleted: bool) {
        let mut state = self.lock();
        state.stale_pointers.push(bytes);
        if !deleted {
            state.pointer_hide = true;
            state.pointer_version = None;
        }
    }

    pub(super) fn hide_pointer_versions(&self) {
        let mut state = self.lock();
        state.pointer_hide = true;
        state.pointer_version = None;
    }

    pub(super) fn has_pointer_history(&self) -> bool {
        let current = self.lock();
        current.pointer_hide || current.latest_pointer_version.is_some()
    }

    /// Parts a streamed `write` keeps in flight: the backend's configured maximum, unless the upload resumed
    /// at a part size larger than a streamed upload of this source plans (a native copy's 64 MiB,
    /// ADR-0006 C18) — then as many as fit in the bytes those would take, at least one, so a
    /// streamed resume of a native upload buffers two such parts, not five.
    pub(super) fn streamed_inflight(&self, maximum: usize) -> usize {
        let planned = self
            .expected_size
            .unwrap_or(0)
            .div_ceil(10_000)
            .max(PART_SIZE as u64);
        let maximum = maximum.max(1);
        let budget = planned.saturating_mul(maximum as u64);
        let fitting = budget / (self.part_size as u64).max(1);
        usize::try_from(fitting)
            .unwrap_or(maximum)
            .clamp(1, maximum)
    }

    pub(super) fn extension(&self) -> Option<Bytes> {
        UploadRecord {
            nonce: self.nonce,
            part_size: self.part_size as u64,
            upload_id: self.upload_id.clone(),
        }
        .encode()
    }
}

/// The upload on the final key `stage` is, if it is one.
pub(super) fn of(stage: &PreparedStage) -> Option<&FinalUpload> {
    stage
        .backend_state
        .as_deref()?
        .downcast_ref::<FinalUpload>()
}
