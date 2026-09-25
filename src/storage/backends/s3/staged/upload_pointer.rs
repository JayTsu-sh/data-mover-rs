//! The `.upload` pointer object of a multipart upload on the final key (ADR-0006 C15b).
//!
//! The pointer is `.data-mover-<d>.upload` beside the final key: a `DMDPTR01` pointer without a
//! durable prefix — the service's `ListParts` is the durable record, and a pointer rewritten at
//! every checkpoint would leave a version behind each time in a versioned bucket — whose extension
//! names the upload:
//!
//! | Bytes | Field |
//! |---|---|
//! | 8 | tag `DMS3UP01` |
//! | 16 | fence nonce, drawn at every prepare |
//! | 8 | part size, u64 little-endian |
//! | 2 | upload id length, u16 little-endian |
//! | n | upload id (UTF-8, not empty) |
//!
//! It is written with one `PutObject` carrying `Content-MD5` (atomic: no temporary), read with a
//! HEAD and a ranged GET pinned to what the HEAD saw, and deleted with `DeleteObject`.
//!
//! In a versioned bucket (ADR-0006 C17) a pointer is deleted by the version its write or read
//! reported, so no delete marker is left and no pointer version piles up; a version Object Lock
//! will not let go is hidden behind a delete marker instead, with a warning. A bucket that reports
//! no version (unversioned) gets a plain `DeleteObject`, as before.

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD};
use bytes::Bytes;
use md5::{Digest as _, Md5};

use super::super::source::{classified_entry, role_failure};
use super::super::{S3Protocol, S3ProtocolFailure, is_real_version_id};
use super::{MIN_MULTIPART_PART_SIZE, cleanup_result};
use crate::model::{FailureClass, Operation, StoragePath, Transience};
use crate::storage::StorageRoleFailure;
use crate::storage::artifacts::{ArtifactKind, is_artifact_path, sibling_artifact};
use crate::storage::pointer::DestinationPointer;

/// The tag every S3 upload pointer's extension starts with.
const POINTER_TAG: &[u8; 8] = b"DMS3UP01";
pub(super) const NONCE_BYTES: usize = 16;
/// Tag, nonce, part size and the upload id's length.
const FIXED_BYTES: usize = POINTER_TAG.len() + NONCE_BYTES + 8 + 2;
/// The largest part S3 accepts.
const MAX_PART_SIZE: u64 = 5 * 1024 * 1024 * 1024;
/// The most parts one upload holds.
const MAX_PARTS: u64 = 10_000;

/// What the pointer's extension records about the upload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct UploadRecord {
    pub(super) nonce: [u8; NONCE_BYTES],
    pub(super) part_size: u64,
    pub(super) upload_id: String,
}

impl UploadRecord {
    /// The extension bytes; `None` for an upload id too long for a pointer (over 4 KiB in all).
    pub(super) fn encode(&self) -> Option<Bytes> {
        let id = self.upload_id.as_bytes();
        let length = u16::try_from(id.len()).ok()?;
        if id.is_empty() || FIXED_BYTES + id.len() > 4096 {
            return None;
        }
        let mut out = Vec::with_capacity(FIXED_BYTES + id.len());
        out.extend_from_slice(POINTER_TAG);
        out.extend_from_slice(&self.nonce);
        out.extend_from_slice(&self.part_size.to_le_bytes());
        out.extend_from_slice(&length.to_le_bytes());
        out.extend_from_slice(id);
        Some(Bytes::from(out))
    }

    /// Decodes exactly the layout above: the tag, a length that matches, a non-empty UTF-8 upload
    /// id and a part size S3 accepts.
    pub(super) fn decode(extension: &[u8]) -> Option<Self> {
        if extension.len() < FIXED_BYTES || !extension.starts_with(POINTER_TAG) {
            return None;
        }
        let nonce: [u8; NONCE_BYTES] = extension[8..24].try_into().ok()?;
        let part_size = u64::from_le_bytes(extension[24..32].try_into().ok()?);
        let length = usize::from(u16::from_le_bytes(extension[32..34].try_into().ok()?));
        let id = extension.get(FIXED_BYTES..)?;
        if id.len() != length
            || id.is_empty()
            || !(MIN_MULTIPART_PART_SIZE..=MAX_PART_SIZE).contains(&part_size)
        {
            return None;
        }
        Some(Self {
            nonce,
            part_size,
            upload_id: String::from_utf8(id.to_vec()).ok()?,
        })
    }

    /// Whether parts of this size can hold `size` bytes within the part limit.
    pub(super) fn holds(&self, size: Option<u64>) -> bool {
        size.is_none_or(|size| size.div_ceil(self.part_size) <= MAX_PARTS)
    }
}

/// The upload record of a pointer an S3 upload on the final key wrote: no durable prefix, and an
/// extension that decodes exactly.
pub(super) fn accepted(pointer: &DestinationPointer) -> Option<UploadRecord> {
    if pointer.durable_prefix.is_some() {
        return None;
    }
    UploadRecord::decode(&pointer.extension)
}

fn failure(path: &StoragePath, operation: Operation, class: FailureClass) -> StorageRoleFailure {
    classified_entry(
        path,
        operation,
        class,
        Transience::Permanent,
        "S3 upload pointer",
    )
}

/// The pointer beside a final key. A key that names no object (empty, or ending in `/`) or lies
/// inside a transfer artifact is refused.
pub(super) fn pointer_path(final_path: &StoragePath) -> Result<StoragePath, StorageRoleFailure> {
    if is_artifact_path(final_path.as_str()) {
        return Err(failure(
            final_path,
            Operation::Prepare,
            FailureClass::InvalidInput,
        ));
    }
    sibling_artifact(final_path, ArtifactKind::Upload)
        .ok_or_else(|| failure(final_path, Operation::Prepare, FailureClass::InvalidInput))
}

fn is_not_found(failure: &S3ProtocolFailure) -> bool {
    matches!(
        failure,
        S3ProtocolFailure::Entry {
            class: FailureClass::NotFound,
            ..
        }
    )
}

/// A pointer as read: its bytes and the version the HEAD reported (as spelled, `"null"`
/// included; `None` in an unversioned bucket).
#[derive(Clone, Default)]
pub(super) struct StoredPointer {
    pub(super) bytes: Vec<u8>,
    pub(super) version: Option<String>,
}

/// The pointer's bytes, up to `limit` of them, and its version; `None` when it is absent (or went
/// away between the HEAD and the GET).
pub(super) async fn read<P: S3Protocol>(
    protocol: &P,
    pointer: &StoragePath,
    limit: usize,
) -> Result<Option<StoredPointer>, StorageRoleFailure> {
    let facts = match protocol.head(pointer.as_str()).await {
        Ok(facts) => facts,
        Err(error) if is_not_found(&error) => return Ok(None),
        Err(error) => return Err(role_failure(pointer, Operation::Observe, error)),
    };
    let version = facts.version_id.clone();
    let length = facts.size.min(limit as u64);
    let bytes = if length == 0 {
        Vec::new()
    } else {
        match protocol
            .get_range(pointer.as_str(), 0..length, &facts)
            .await
        {
            Ok(bytes) => bytes.to_vec(),
            Err(error) if is_not_found(&error) => return Ok(None),
            Err(error) => return Err(role_failure(pointer, Operation::Observe, error)),
        }
    };
    Ok(Some(StoredPointer { bytes, version }))
}

/// Writes the pointer with one `PutObject` carrying `Content-MD5` and returns the version it
/// created, as reported (see [`StoredPointer::version`]). A failed PUT whose object is there
/// anyway, byte for byte (a lost reply), counts as written; its version is the one the read-back
/// saw — the bytes carry this prepare's nonce, so no other write made them.
pub(super) async fn put<P: S3Protocol>(
    protocol: &P,
    pointer: &StoragePath,
    bytes: Vec<u8>,
) -> Result<Option<String>, StorageRoleFailure> {
    let digest = BASE64_STANDARD.encode(Md5::digest(&bytes));
    let limit = bytes.len() + 1;
    let error = match protocol
        .put_object(pointer.as_str(), Bytes::from(bytes.clone()), &digest)
        .await
    {
        Ok(facts) => return Ok(facts.reported_version),
        Err(error) => error,
    };
    match read(protocol, pointer, limit).await {
        Ok(Some(found)) if found.bytes == bytes => Ok(found.version),
        _ => Err(role_failure(pointer, Operation::Write, error)),
    }
}

/// Deletes the pointer; an absent one is fine. Named by `version` (what its write or read
/// reported), it is deleted for good: in a versioned bucket a plain `DeleteObject` would keep the
/// version behind a delete marker. A version that cannot be deleted by id — Object Lock refuses it,
/// the store does not delete by version, or refuses `versionId=null` — falls back to a plain
/// `DeleteObject`, with a warning: in a versioned bucket a delete marker then hides it, and a retry
/// could never delete it either. Without a version (an unversioned bucket) it is a plain
/// `DeleteObject`.
///
/// `stale` lists the contents a pointer version may hold that must not outlive this deletion (the
/// deleted pointer's own bytes, and those of a pointer a resume replaced): after a delete by id,
/// the version below becomes current, and a `PutObject` the SDK re-sent after a lost reply leaves
/// a byte-identical version there. See [`sweep`].
pub(super) async fn delete<P: S3Protocol>(
    protocol: &P,
    pointer: &StoragePath,
    version: Option<&str>,
    stale: &[&[u8]],
) -> Result<(), StorageRoleFailure> {
    let Some(version) = version else {
        return cleanup_result(pointer, protocol.delete_object(pointer.as_str()).await);
    };
    if delete_by_id(protocol, pointer, version).await? {
        sweep(protocol, pointer, version, stale).await;
    }
    Ok(())
}

/// Deletes one pointer version by id; `false` when it had to be hidden behind a delete marker
/// instead (see [`delete`]).
async fn delete_by_id<P: S3Protocol>(
    protocol: &P,
    pointer: &StoragePath,
    version: &str,
) -> Result<bool, StorageRoleFailure> {
    match protocol.delete_version(pointer.as_str(), version).await {
        Err(error) if not_deletable_by_version(&error, version) => {
            tracing::warn!(
                pointer = %pointer.as_str(),
                version,
                ?error,
                "could not delete the S3 upload pointer's version (Object Lock?); hiding it behind a delete marker"
            );
            cleanup_result(pointer, protocol.delete_object(pointer.as_str()).await)?;
            Ok(false)
        }
        deleted => cleanup_result(pointer, deleted).map(|()| true),
    }
}

/// The most versions [`sweep`] deletes after one pointer deletion.
const MAX_SWEEPS: usize = 4;

/// After a pointer version was deleted by id: while the pointer that is current now holds one of
/// the `stale` contents (a duplicate the SDK's retry of a committed `PutObject` left, or the
/// version a resume replaced), deletes that version too, at most [`MAX_SWEEPS`] times. Another
/// writer's pointer is left alone. Best effort: the object is already published or discarded, so a
/// failure here is logged — the next prepare of the key cleans up what is left.
async fn sweep<P: S3Protocol>(protocol: &P, pointer: &StoragePath, deleted: &str, stale: &[&[u8]]) {
    let limit = stale.iter().map(|bytes| bytes.len()).max().unwrap_or(0) + 1;
    let mut last = deleted.to_string();
    for _ in 0..MAX_SWEEPS {
        let found = match read(protocol, pointer, limit).await {
            Ok(Some(found)) if stale.contains(&found.bytes.as_slice()) => found,
            Ok(_) => return,
            Err(error) => {
                tracing::warn!(pointer = %pointer.as_str(), ?error, "could not check the S3 upload pointer after deleting it");
                return;
            }
        };
        let Some(version) = found.version.filter(|version| *version != last) else {
            return;
        };
        match delete_by_id(protocol, pointer, &version).await {
            Ok(true) => last = version,
            Ok(false) => return,
            Err(error) => {
                tracing::warn!(pointer = %pointer.as_str(), version, ?error, "could not delete a stale S3 upload pointer version");
                return;
            }
        }
    }
    tracing::warn!(pointer = %pointer.as_str(), "stale S3 upload pointer versions remain after {MAX_SWEEPS} deletions");
}

/// Deletes an earlier writer's pointer version after a take-over wrote a new one over it, so it
/// cannot come back once ours is deleted. Best effort: a failure is logged and reported as `false`
/// — the replaced version is still there, so ours must then be hidden behind a delete marker rather
/// than deleted by version, or the replaced one would become current again.
///
/// A replaced `"null"` was overwritten by our write unless ours reported a real version: in a
/// suspended or unversioned bucket there is only one `"null"` version, now ours (some stores send
/// no version header for it), so deleting `"null"` would delete our own pointer.
pub(super) async fn delete_replaced<P: S3Protocol>(
    protocol: &P,
    pointer: &StoragePath,
    replaced: Option<&str>,
    ours: Option<&str>,
) -> bool {
    let overwritten = |replaced: &str| {
        Some(replaced) == ours || (replaced == "null" && !ours.is_some_and(is_real_version_id))
    };
    let Some(replaced) = replaced.filter(|replaced| !overwritten(replaced)) else {
        return true;
    };
    let deleted = protocol.delete_version(pointer.as_str(), replaced).await;
    match cleanup_result(pointer, deleted) {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!(
                pointer = %pointer.as_str(),
                version = replaced,
                ?error,
                "could not delete the S3 upload pointer version a resume replaced"
            );
            false
        }
    }
}

/// A version that a delete by id cannot remove: Object Lock (a legal hold or retention) or a
/// policy without `DeleteObjectVersion` (`PermissionDenied`), a store that does not delete by
/// version (`Unsupported`), or one that refuses the id `"null"` it reported (`InvalidInput`).
pub(super) fn not_deletable_by_version(failure: &S3ProtocolFailure, version: &str) -> bool {
    matches!(
        failure,
        S3ProtocolFailure::Entry {
            class: FailureClass::PermissionDenied | FailureClass::Unsupported,
            ..
        }
    ) || (version == "null"
        && matches!(
            failure,
            S3ProtocolFailure::Entry {
                class: FailureClass::InvalidInput,
                ..
            }
        ))
}

/// Aborts every upload in progress on exactly `key` except `keep`; an upload already gone is fine.
pub(super) async fn abort_uploads<P: S3Protocol>(
    protocol: &P,
    path: &StoragePath,
    keep: Option<&str>,
) -> Result<(), StorageRoleFailure> {
    let uploads = protocol
        .list_uploads(path.as_str())
        .await
        .map_err(|error| role_failure(path, Operation::Namespace, error))?;
    for upload in uploads {
        if !keep.is_some_and(|kept| same_upload(kept, &upload)) {
            cleanup_result(path, protocol.abort_multipart(path.as_str(), &upload).await)?;
        }
    }
    Ok(())
}

/// Whether `listed`, an id `ListMultipartUploads` reported, names the upload `issued`, the id
/// `CreateMultipartUpload` returned. They are equal on most stores; `MinIO` issues
/// base64url(`<deployment id>.<upload uuid>`) but lists the bare uuid (seen on
/// RELEASE.2023-03-20), and accepts either afterwards — so a resume that compared the two
/// spellings byte for byte aborted its own upload.
pub(super) fn same_upload(issued: &str, listed: &str) -> bool {
    issued == listed
        || URL_SAFE_NO_PAD
            .decode(issued)
            .ok()
            .and_then(|decoded| String::from_utf8(decoded).ok())
            .is_some_and(|decoded| {
                decoded
                    .rsplit_once('.')
                    .is_some_and(|(deployment, uuid)| !deployment.is_empty() && uuid == listed)
            })
}

#[cfg(test)]
mod tests {
    use super::{UploadRecord, accepted, same_upload};
    use crate::storage::pointer::DestinationPointer;

    fn record(upload_id: &str) -> UploadRecord {
        UploadRecord {
            nonce: [7; 16],
            part_size: 8 << 20,
            upload_id: upload_id.to_string(),
        }
    }

    type TestResult = Result<(), &'static str>;

    #[test]
    fn a_record_round_trips_and_pins_its_layout() -> TestResult {
        let bytes = record("up-1").encode().ok_or("encodes")?;
        let mut expected = b"DMS3UP01".to_vec();
        expected.extend_from_slice(&[7; 16]);
        expected.extend_from_slice(&(8_u64 << 20).to_le_bytes());
        expected.extend_from_slice(&4_u16.to_le_bytes());
        expected.extend_from_slice(b"up-1");
        assert_eq!(bytes.as_ref(), expected.as_slice());
        assert_eq!(UploadRecord::decode(&bytes), Some(record("up-1")));
        Ok(())
    }

    #[test]
    fn only_the_exact_shape_decodes() -> TestResult {
        let good = record("up-1").encode().ok_or("encodes")?.to_vec();
        let mut other_tag = good.clone();
        other_tag[7] = b'2';
        let mut short = good.clone();
        short.pop();
        let mut long = good.clone();
        long.push(b'x');
        let mut tiny_parts = good.clone();
        tiny_parts[24..32].copy_from_slice(&(1_u64 << 20).to_le_bytes());
        let mut empty_id = good[..34].to_vec();
        empty_id[32..34].copy_from_slice(&0_u16.to_le_bytes());
        let mut not_utf8 = good.clone();
        not_utf8[34] = 0xff;
        for (name, bytes) in [
            ("tag", other_tag),
            ("short", short),
            ("long", long),
            ("part size", tiny_parts),
            ("empty id", empty_id),
            ("utf-8", not_utf8),
        ] {
            assert_eq!(UploadRecord::decode(&bytes), None, "{name}");
        }
        assert_eq!(record("").encode(), None);
        assert_eq!(record(&"x".repeat(4096)).encode(), None);
        Ok(())
    }

    #[test]
    fn a_pointer_with_a_durable_prefix_or_another_tag_is_refused() -> TestResult {
        let extension = record("up-1").encode().ok_or("encodes")?;
        let pointer = |prefix, extension| DestinationPointer {
            binding: [1; 32],
            transfer_identity: [2; 32],
            durable_prefix: prefix,
            extension,
        };
        assert!(accepted(&pointer(None, extension.clone())).is_some());
        assert!(accepted(&pointer(Some(0), extension)).is_none());
        let mut hdfs = b"DMHSTG01".to_vec();
        hdfs.extend_from_slice(&[0; 16]);
        assert!(accepted(&pointer(None, hdfs.into())).is_none());
        Ok(())
    }

    #[test]
    fn a_part_size_must_hold_the_source_within_the_part_limit() {
        let record = record("up-1");
        assert!(record.holds(None));
        assert!(record.holds(Some(10_000 * (8 << 20))));
        assert!(!record.holds(Some(10_000 * (8 << 20) + 1)));
    }

    /// `MinIO` lists the bare uuid of an upload it issued as base64url(`<deployment>.<uuid>`).
    #[test]
    fn a_minio_upload_is_the_same_under_either_spelling() {
        let issued = "MjhhZjQzOGUtYzAxMC00MGUxLWI1MDctNmE2MzE3MWNhMDgzLjg1MDM0NWEzLThmZjAtNDk1OS1hMWQzLTE4MTUxYWQ0ZTY2YQ";
        let listed = "850345a3-8ff0-4959-a1d3-18151ad4e66a";
        assert!(same_upload(issued, listed));
        assert!(same_upload(listed, listed));
        assert!(!same_upload(issued, "150345a3-8ff0-4959-a1d3-18151ad4e66a"));
        assert!(!same_upload(listed, issued));
        assert!(!same_upload("upload-1", "upload-2"));
    }
}
