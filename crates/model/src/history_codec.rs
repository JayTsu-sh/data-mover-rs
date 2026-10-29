//! The history section of an entry snapshot (format 7, P7 §10.4): one byte saying whether the
//! entry carries a history, then, when it does, the segment:
//!
//! ```text
//! 1 | flags u8 (1: server order, 2: complete, 4: has largest) | segment u32 LE | count u32 LE |
//! [largest u64 LE: the whole key's largest version] |
//! count × { flags u8 (1: latest, 2: delete marker, 4: has an id, 8: has metadata) | [id] |
//!           [object: size u64 LE | ETag] | modified | identity strength u8 | identity bytes |
//!           [metadata] }
//! ```
//!
//! Byte strings are length-prefixed as everywhere in the snapshot; a record's identity belongs to
//! the entry's backend, so only its strength and bytes are stored. A count of 0 or above
//! [`MAX_HISTORY_RECORDS`], or a flag this format does not define, is `Malformed`.

use std::str;

use super::{Cursor, IdentityStrength, SourceIdentity, decode_time, encode_time, put_bytes};
use crate::model::metadata_observation;
use crate::model::{
    BackendIdentity, HistoryOrder, MAX_HISTORY_RECORDS, ObjectHistory, RecordKind,
    SnapshotDecodeError, VersionRecord,
};

const ORDER_SERVER: u8 = 1;
const COMPLETE: u8 = 2;
const HAS_LARGEST: u8 = 4;

const LATEST: u8 = 1;
const DELETE_MARKER: u8 = 2;
const HAS_ID: u8 = 4;
const HAS_METADATA: u8 = 8;

/// Records a decode allocates room for up front, whatever count the bytes claim.
const PREALLOCATED: usize = 1024;

pub(super) fn encode(output: &mut Vec<u8>, history: Option<&ObjectHistory>) {
    let Some(history) = history else {
        output.push(0);
        return;
    };
    output.push(1);
    let mut flags = 0;
    if history.order() == HistoryOrder::Server {
        flags |= ORDER_SERVER;
    }
    if history.is_complete() {
        flags |= COMPLETE;
    }
    if history.largest_version().is_some() {
        flags |= HAS_LARGEST;
    }
    output.push(flags);
    output.extend_from_slice(&history.segment().to_le_bytes());
    let Ok(count) = u32::try_from(history.records().len()) else {
        unreachable!("a history segment holds at most MAX_HISTORY_RECORDS records");
    };
    output.extend_from_slice(&count.to_le_bytes());
    if let Some(largest) = history.largest_version() {
        output.extend_from_slice(&largest.to_le_bytes());
    }
    for record in history.records() {
        encode_record(output, record);
    }
}

fn encode_record(output: &mut Vec<u8>, record: &VersionRecord) {
    let mut flags = 0;
    if record.is_latest() {
        flags |= LATEST;
    }
    if record.is_delete_marker() {
        flags |= DELETE_MARKER;
    }
    if record.version_id().is_some() {
        flags |= HAS_ID;
    }
    if record.metadata().is_some() {
        flags |= HAS_METADATA;
    }
    output.push(flags);
    if let Some(id) = record.version_id() {
        put_bytes(output, id.as_bytes());
    }
    if let RecordKind::Object { size, etag, .. } = record.kind() {
        output.extend_from_slice(&size.to_le_bytes());
        put_bytes(output, etag.as_bytes());
    }
    encode_time(output, record.modified());
    output.push(record.identity().strength().tag());
    put_bytes(output, record.identity().stable_bytes.as_slice());
    if let Some(metadata) = record.metadata() {
        metadata_observation::encode(metadata, output);
    }
}

/// The history section, its records' identities on `backend` (the entry's).
pub(super) fn decode(
    cursor: &mut Cursor<'_>,
    backend: &BackendIdentity,
) -> Result<Option<Box<ObjectHistory>>, SnapshotDecodeError> {
    match cursor.byte()? {
        0 => return Ok(None),
        1 => {}
        _ => return Err(SnapshotDecodeError::Malformed),
    }
    let flags = cursor.byte()?;
    if flags & !(ORDER_SERVER | COMPLETE | HAS_LARGEST) != 0 {
        return Err(SnapshotDecodeError::Malformed);
    }
    let segment = cursor.u32()?;
    let count = cursor.u32()? as usize;
    if count == 0 || count > MAX_HISTORY_RECORDS {
        return Err(SnapshotDecodeError::Malformed);
    }
    let largest = (flags & HAS_LARGEST != 0)
        .then(|| cursor.u64())
        .transpose()?;
    let mut records = Vec::with_capacity(count.min(PREALLOCATED));
    for _ in 0..count {
        records.push(decode_record(cursor, backend)?);
    }
    let order = if flags & ORDER_SERVER == 0 {
        HistoryOrder::Inferred
    } else {
        HistoryOrder::Server
    };
    ObjectHistory::segment_of(records, order, segment, flags & COMPLETE != 0, largest)
        .map(|history| Some(Box::new(history)))
        .map_err(|_| SnapshotDecodeError::Malformed)
}

fn decode_record(
    cursor: &mut Cursor<'_>,
    backend: &BackendIdentity,
) -> Result<VersionRecord, SnapshotDecodeError> {
    let flags = cursor.byte()?;
    if flags & !(LATEST | DELETE_MARKER | HAS_ID | HAS_METADATA) != 0 {
        return Err(SnapshotDecodeError::Malformed);
    }
    let version_id = if flags & HAS_ID == 0 {
        None
    } else {
        Some(utf8(cursor.bytes()?)?.to_string())
    };
    let object = if flags & DELETE_MARKER == 0 {
        Some((cursor.u64()?, utf8(cursor.bytes()?)?.to_string()))
    } else {
        None
    };
    let modified = decode_time(cursor)?;
    let kind = match object {
        Some((size, etag)) => RecordKind::Object {
            size,
            etag,
            modified,
        },
        None => RecordKind::DeleteMarker { modified },
    };
    let strength =
        IdentityStrength::from_tag(cursor.byte()?).ok_or(SnapshotDecodeError::Malformed)?;
    let identity = SourceIdentity::new(backend.clone(), strength, cursor.bytes()?)
        .map_err(|_| SnapshotDecodeError::Malformed)?;
    let metadata = if flags & HAS_METADATA == 0 {
        None
    } else {
        Some(Box::new(metadata_observation::decode(cursor)?))
    };
    VersionRecord::new(version_id, flags & LATEST != 0, kind, identity, metadata)
        .map_err(|_| SnapshotDecodeError::Malformed)
}

fn utf8(bytes: &[u8]) -> Result<&str, SnapshotDecodeError> {
    str::from_utf8(bytes).map_err(|_| SnapshotDecodeError::Malformed)
}
