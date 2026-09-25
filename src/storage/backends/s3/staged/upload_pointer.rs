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

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD};
use bytes::Bytes;
use md5::{Digest as _, Md5};

use super::super::source::{classified_entry, role_failure};
use super::super::{S3Protocol, S3ProtocolFailure};
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

/// The pointer's bytes, up to `limit` of them; `None` when it is absent (or went away between the
/// HEAD and the GET).
pub(super) async fn read<P: S3Protocol>(
    protocol: &P,
    pointer: &StoragePath,
    limit: usize,
) -> Result<Option<Vec<u8>>, StorageRoleFailure> {
    let facts = match protocol.head(pointer.as_str()).await {
        Ok(facts) => facts,
        Err(error) if is_not_found(&error) => return Ok(None),
        Err(error) => return Err(role_failure(pointer, Operation::Observe, error)),
    };
    let length = facts.size.min(limit as u64);
    if length == 0 {
        return Ok(Some(Vec::new()));
    }
    match protocol
        .get_range(pointer.as_str(), 0..length, &facts)
        .await
    {
        Ok(bytes) => Ok(Some(bytes.to_vec())),
        Err(error) if is_not_found(&error) => Ok(None),
        Err(error) => Err(role_failure(pointer, Operation::Observe, error)),
    }
}

/// Writes the pointer with one `PutObject` carrying `Content-MD5`. A failed PUT whose object is
/// there anyway, byte for byte (a lost reply), counts as written.
pub(super) async fn put<P: S3Protocol>(
    protocol: &P,
    pointer: &StoragePath,
    bytes: Vec<u8>,
) -> Result<(), StorageRoleFailure> {
    let digest = BASE64_STANDARD.encode(Md5::digest(&bytes));
    let limit = bytes.len() + 1;
    let Err(error) = protocol
        .put_object(pointer.as_str(), Bytes::from(bytes.clone()), &digest)
        .await
    else {
        return Ok(());
    };
    match read(protocol, pointer, limit).await {
        Ok(Some(found)) if found == bytes => Ok(()),
        _ => Err(role_failure(pointer, Operation::Write, error)),
    }
}

/// Deletes the pointer; an absent one is fine.
pub(super) async fn delete<P: S3Protocol>(
    protocol: &P,
    pointer: &StoragePath,
) -> Result<(), StorageRoleFailure> {
    cleanup_result(pointer, protocol.delete_object(pointer.as_str()).await)
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
