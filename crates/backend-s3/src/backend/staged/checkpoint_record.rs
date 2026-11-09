//! S3 evidence for v2 hash pointers: exact durable part receipts and the published object.

use bytes::Bytes;

use super::super::{S3WriteFacts, is_real_version_id};
use super::final_upload::FinalUpload;
use super::hash_checkpoint::part_range;
use super::upload_pointer::UploadRecord;
use crate::storage::checkpoint::missing_ranges;
use crate::storage::{PreparedStage, StorageRoleFailure};

const MAGIC: &[u8; 8] = b"DMS3CP02";
const MAX_EXTENSION_BYTES: usize = 4096;

pub(super) struct CheckpointRecord {
    pub(super) upload: UploadRecord,
    pub(super) receipts: [u8; 32],
    pub(super) completion_etag: Option<String>,
    pub(super) published: Option<S3WriteFacts>,
}

impl CheckpointRecord {
    pub(super) fn encode(&self) -> Option<Bytes> {
        let mut out = MAGIC.to_vec();
        put(&mut out, &self.upload.encode()?)?;
        out.extend_from_slice(&self.receipts);
        put(
            &mut out,
            self.completion_etag.as_deref().unwrap_or("").as_bytes(),
        )?;
        out.push(u8::from(self.published.is_some()));
        if let Some(facts) = &self.published {
            if !valid_facts(facts) {
                return None;
            }
            put(&mut out, facts.etag.as_bytes())?;
            put(
                &mut out,
                facts.version_id.as_deref().unwrap_or("").as_bytes(),
            )?;
            put(
                &mut out,
                facts.reported_version.as_deref().unwrap_or("").as_bytes(),
            )?;
        }
        (out.len() <= MAX_EXTENSION_BYTES).then(|| Bytes::from(out))
    }

    pub(super) fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > MAX_EXTENSION_BYTES {
            return None;
        }
        let mut bytes = bytes.strip_prefix(MAGIC)?;
        let upload = UploadRecord::decode(take(&mut bytes)?)?;
        let receipts = bytes.get(..32)?.try_into().ok()?;
        bytes = bytes.get(32..)?;
        let completion_etag = std::str::from_utf8(take(&mut bytes)?).ok()?;
        let completion_etag = (!completion_etag.is_empty()).then(|| completion_etag.to_string());
        let flag = *bytes.first()?;
        bytes = bytes.get(1..)?;
        let published = match flag {
            0 => None,
            1 => Some(take_facts(&mut bytes)?),
            _ => return None,
        };
        bytes.is_empty().then_some(Self {
            upload,
            receipts,
            completion_etag,
            published,
        })
    }
}

fn valid_facts(facts: &S3WriteFacts) -> bool {
    let observed = facts.version_id.as_deref();
    !facts.etag.is_empty()
        && observed.is_none_or(is_real_version_id)
        && match facts.reported_version.as_deref() {
            None => true,
            Some("null") => observed.is_none(),
            Some(reported) => is_real_version_id(reported) && observed == Some(reported),
        }
}

fn take_facts(bytes: &mut &[u8]) -> Option<S3WriteFacts> {
    let etag = std::str::from_utf8(take(bytes)?).ok()?.to_owned();
    let observed = std::str::from_utf8(take(bytes)?).ok()?;
    let version_id = (!observed.is_empty()).then(|| observed.to_owned());
    let reported = std::str::from_utf8(take(bytes)?).ok()?;
    let reported_version = (!reported.is_empty()).then(|| reported.to_owned());
    let facts = S3WriteFacts {
        etag,
        version_id,
        reported_version,
    };
    valid_facts(&facts).then_some(facts)
}

fn put(out: &mut Vec<u8>, bytes: &[u8]) -> Option<()> {
    out.extend_from_slice(&u16::try_from(bytes.len()).ok()?.to_le_bytes());
    out.extend_from_slice(bytes);
    Some(())
}

fn take<'a>(bytes: &mut &'a [u8]) -> Option<&'a [u8]> {
    let length = usize::from(u16::from_le_bytes(bytes.get(..2)?.try_into().ok()?));
    let value = bytes.get(2..2 + length)?;
    *bytes = bytes.get(2 + length..)?;
    Some(value)
}

/// Digest only receipts belonging to checkpoint D. Parts outside D can be independently adopted
/// from H on resume, but cannot replace a previously committed receipt unnoticed.
pub(super) fn receipts(
    upload: &FinalUpload,
    size: u64,
    ranges: &[std::ops::Range<u64>],
) -> Option<[u8; 32]> {
    receipt_digest(upload, size, ranges, ReceiptFormat::Quoted)
}

/// Keep old v1 digests readable without guessing per-part quote combinations. All-quoted
/// checkpoints are byte-identical; original/matching and uniformly bare legacy records are
/// accepted only after the same strict opaque-value validation. Mixed ambiguous records fail.
pub(super) fn receipts_match(
    expected: &[u8; 32],
    upload: &FinalUpload,
    size: u64,
    ranges: &[std::ops::Range<u64>],
) -> bool {
    [
        ReceiptFormat::Quoted,
        ReceiptFormat::Original,
        ReceiptFormat::Bare,
    ]
    .into_iter()
    .any(|format| receipt_digest(upload, size, ranges, format).as_ref() == Some(expected))
}

#[derive(Clone, Copy)]
enum ReceiptFormat {
    Quoted,
    Original,
    Bare,
}

fn receipt_digest(
    upload: &FinalUpload,
    size: u64,
    ranges: &[std::ops::Range<u64>],
    format: ReceiptFormat,
) -> Option<[u8; 32]> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"data-mover S3 durable part receipts v1");
    let mut parts = upload.lock().parts.clone();
    parts.sort_by_key(|part| part.0);
    for (number, etag) in parts {
        let range = part_range(upload, number, size);
        if missing_ranges(ranges, range.clone()).is_empty() {
            hash.update(&number.to_le_bytes());
            hash.update(&range.start.to_le_bytes());
            hash.update(&range.end.to_le_bytes());
            hash_receipt_etag(&mut hash, &etag, format)?;
        }
    }
    Some(*hash.finalize().as_bytes())
}

/// Bare compatibility values must still obey opaque-tag byte syntax. Do not strip arbitrary
/// quotes, fold case, unescape, or reinterpret an opaque receipt as an MD5 checksum.
fn receipt_body(etag: &str) -> Option<&str> {
    let body = match etag.strip_prefix('"') {
        Some(body) => body.strip_suffix('"')?,
        None => etag,
    };
    (!body.is_empty()
        && body
            .bytes()
            .all(|byte| byte == 0x21 || (0x23..=0x7e).contains(&byte) || byte >= 0x80))
    .then_some(body)
}

fn hash_receipt_etag(hash: &mut blake3::Hasher, etag: &str, format: ReceiptFormat) -> Option<()> {
    let body = receipt_body(etag)?;
    let value = if matches!(format, ReceiptFormat::Original) {
        etag
    } else {
        body
    };
    let quoted = matches!(format, ReceiptFormat::Quoted);
    hash.update(&(value.len() as u64 + if quoted { 2 } else { 0 }).to_le_bytes());
    if quoted {
        hash.update(b"\"");
    }
    hash.update(value.as_bytes());
    if quoted {
        hash.update(b"\"");
    }
    Some(())
}

pub(super) fn extension(
    stage: &PreparedStage,
    upload: &FinalUpload,
    checkpointed: bool,
) -> Result<Bytes, StorageRoleFailure> {
    let invalid =
        || super::hash_checkpoint::invalid(stage, "invalid or oversized S3 checkpoint extension");
    let base = upload.extension().ok_or_else(invalid)?;
    if !checkpointed {
        return Ok(base);
    }
    let (published, recovered_receipts, completion_etag) = {
        let current = upload.lock();
        let etags: Vec<_> = current.parts.iter().map(|part| part.1.clone()).collect();
        let composite = (current.md5_etags == Some(true))
            .then(|| super::super::composite_etag(&etags))
            .flatten();
        (
            current.published.clone(),
            current.recovered_receipts,
            composite,
        )
    };
    let record = CheckpointRecord {
        upload: UploadRecord::decode(&base).ok_or_else(invalid)?,
        receipts: recovered_receipts
            .or_else(|| {
                receipts(
                    upload,
                    upload.expected_size.unwrap_or(0),
                    &stage.checkpoint_ranges(),
                )
            })
            .ok_or_else(invalid)?,
        completion_etag,
        published,
    };
    record.encode().ok_or_else(invalid)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn checkpoint_preserves_observed_pin_without_inventing_a_write_receipt()
    -> Result<(), Box<dyn Error>> {
        let facts = S3WriteFacts {
            etag: "\"read-pin\"".into(),
            version_id: Some("observed-version".into()),
            reported_version: None,
        };
        let record = CheckpointRecord {
            upload: UploadRecord {
                nonce: [4; 16],
                part_size: 5 * 1024 * 1024,
                upload_id: "owned-upload".into(),
            },
            receipts: [7; 32],
            completion_etag: None,
            published: Some(facts.clone()),
        };
        let encoded = record.encode().ok_or("valid current record")?;
        let restored = CheckpointRecord::decode(&encoded).ok_or("current record decodes")?;
        assert_eq!(restored.published, Some(facts));
        Ok(())
    }

    #[test]
    fn current_record_rejects_old_versions_and_conflicting_acknowledgments()
    -> Result<(), Box<dyn Error>> {
        let mut record = CheckpointRecord {
            upload: UploadRecord {
                nonce: [4; 16],
                part_size: 5 * 1024 * 1024,
                upload_id: "owned-upload".into(),
            },
            receipts: [7; 32],
            completion_etag: None,
            published: Some(S3WriteFacts::new(
                "\"owned\"".into(),
                Some("created-version".into()),
            )),
        };
        let encoded = record.encode().ok_or("valid acknowledged record")?;
        assert_eq!(
            CheckpointRecord::decode(&encoded)
                .ok_or("decode")?
                .published,
            record.published
        );
        for magic in [b"DMS3CP01", b"DMS3CP03"] {
            let mut other = encoded.to_vec();
            other[..8].copy_from_slice(magic);
            assert!(CheckpointRecord::decode(&other).is_none());
        }
        let facts = record.published.as_mut().ok_or("facts")?;
        facts.reported_version = Some("different-version".into());
        assert!(
            record.encode().is_none(),
            "a read pin cannot contradict its acknowledgment"
        );
        assert!(CheckpointRecord::decode(&vec![0; MAX_EXTENSION_BYTES + 1]).is_none());
        assert!(CheckpointRecord::decode(&encoded[..encoded.len() - 1]).is_none());
        Ok(())
    }
}
