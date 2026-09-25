//! The entry snapshot codec: `ObservedEntry` to and from its opaque, versioned bytes.

use super::{
    BackendIdentity, BackendKind, EntryIdentityKey, EntryKind, EntrySnapshot, EntryVersion,
    IdentityStrength, MAX_MODEL_FIELD_BYTES, ObservedEntry, PrivateBackendEntryFacts,
    SnapshotDecodeError, SourceIdentity, StoragePath, StorageTimestamp, SymlinkTarget,
    SymlinkTargetEncoding,
};
use crate::model::metadata_observation;
use crate::model::{SpecialFileKind, TimePrecision};

const MAGIC: &[u8; 4] = b"DMES";
/// v5 (ADR-0006 C4c): the backend identity is no longer stored — every entry of one scan shares it,
/// and the caller supplies it when decoding; a 4-byte endpoint fingerprint tells a wrong endpoint
/// from a corrupted key. v4 carried the identity per entry and still decodes.
const VERSION: u8 = 5;
pub(super) const VERSION_WITH_BACKEND_ID: u8 = 4;
/// v6 (ADR-0006 C22): v5 plus the listed version of an entry from a versioned traversal (its
/// version id, whether it is the latest, whether it is a delete marker), before the identity key.
/// Only an entry that carries a version is written as v6; every other entry stays v5, byte for byte.
const VERSION_WITH_ENTRY_VERSION: u8 = 6;

impl PrivateBackendEntryFacts {
    fn encode(&self, output: &mut Vec<u8>) {
        let (tag, bytes) = match self {
            Self::None => (0, &[][..]),
            Self::Local(bytes) => (1, bytes.as_slice()),
            Self::Nfs(bytes) => (2, bytes.as_slice()),
            Self::Cifs(bytes) => (3, bytes.as_slice()),
            Self::S3(bytes) => (4, bytes.as_slice()),
            Self::Hdfs(bytes) => (5, bytes.as_slice()),
        };
        output.push(tag);
        put_bytes(output, bytes);
    }
}

impl ObservedEntry {
    pub(super) fn encode(&self, with_backend_id: bool) -> EntrySnapshot {
        let mut output = Vec::new();
        output.extend_from_slice(MAGIC);
        output.push(match (with_backend_id, &self.version) {
            (true, _) => VERSION_WITH_BACKEND_ID,
            (false, None) => VERSION,
            (false, Some(_)) => VERSION_WITH_ENTRY_VERSION,
        });
        output.push(backend_tag(self.backend_kind));
        put_bytes(&mut output, self.path.as_str().as_bytes());
        encode_kind(&mut output, self.kind);
        match &self.symlink_target {
            Some(target) => {
                output.push(1);
                output.push(match target.encoding() {
                    SymlinkTargetEncoding::UnixBytes => 0,
                    SymlinkTargetEncoding::WindowsWide => 1,
                });
                put_bytes(&mut output, target.as_bytes());
            }
            None => output.push(0),
        }
        encode_size(&mut output, self.size);
        encode_time(&mut output, self.modified);
        output.push(self.source_identity.strength.tag());
        if with_backend_id {
            put_bytes(
                &mut output,
                self.source_identity.backend.stable_id().as_bytes(),
            );
        } else {
            output.extend_from_slice(&endpoint_fingerprint(&self.source_identity.backend));
        }
        put_bytes(&mut output, &self.source_identity.stable_bytes);
        output.push(2);
        metadata_observation::encode(&self.metadata, &mut output);
        self.backend_fact.encode(&mut output);
        if !with_backend_id && let Some(version) = &self.version {
            encode_version(&mut output, version);
        }
        output.extend_from_slice(self.identity_key.as_bytes());
        EntrySnapshot(output)
    }
}

pub(in crate::model) struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Cursor<'a> {
    pub(in crate::model) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn take(&mut self, len: usize) -> Result<&'a [u8], SnapshotDecodeError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(SnapshotDecodeError::FieldTooLarge)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(SnapshotDecodeError::Truncated)?;
        self.offset = end;
        Ok(value)
    }
    pub(in crate::model) fn byte(&mut self) -> Result<u8, SnapshotDecodeError> {
        Ok(self.take(1)?[0])
    }
    pub(in crate::model) fn u32(&mut self) -> Result<u32, SnapshotDecodeError> {
        let mut value = [0; 4];
        value.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(value))
    }
    pub(in crate::model) fn bytes(&mut self) -> Result<&'a [u8], SnapshotDecodeError> {
        let len = self.u32()? as usize;
        if len > MAX_MODEL_FIELD_BYTES {
            return Err(SnapshotDecodeError::FieldTooLarge);
        }
        self.take(len)
    }
}

pub(super) fn decode_snapshot(
    bytes: &[u8],
    backend: &BackendIdentity,
) -> Result<ObservedEntry, SnapshotDecodeError> {
    let mut cursor = Cursor::new(bytes);
    if cursor.take(4)? != MAGIC {
        return Err(SnapshotDecodeError::InvalidMagic);
    }
    let version = cursor.byte()?;
    if ![VERSION, VERSION_WITH_BACKEND_ID, VERSION_WITH_ENTRY_VERSION].contains(&version) {
        return Err(SnapshotDecodeError::UnsupportedVersion);
    }
    let backend_kind = backend_from_tag(cursor.byte()?).ok_or(SnapshotDecodeError::Malformed)?;
    let path = std::str::from_utf8(cursor.bytes()?).map_err(|_| SnapshotDecodeError::Malformed)?;
    let path = StoragePath::new(path).map_err(|_| SnapshotDecodeError::Malformed)?;
    let kind = decode_kind(&mut cursor)?;
    let symlink_target = decode_symlink_target(&mut cursor, kind)?;
    let size = decode_size(&mut cursor)?;
    let modified = decode_time(&mut cursor)?;
    let source_identity = decode_source_identity(&mut cursor, version, backend_kind, backend)?;
    let schema_version = cursor.byte()?;
    if schema_version != 2 {
        return Err(SnapshotDecodeError::Malformed);
    }
    let metadata = metadata_observation::decode(&mut cursor)?;
    let backend_fact = decode_facts(&mut cursor, backend_kind)?;
    let entry_version = (version == VERSION_WITH_ENTRY_VERSION)
        .then(|| decode_version(&mut cursor).map(Box::new))
        .transpose()?;
    let identity_key = decode_identity_key(&mut cursor, bytes.len(), &source_identity)?;
    Ok(ObservedEntry {
        identity_key,
        backend_kind,
        path,
        kind,
        size,
        modified,
        symlink_target,
        source_identity,
        metadata,
        backend_fact,
        version: entry_version,
    })
}

/// The stored identity key, which must end the snapshot and match the key `source_identity`
/// derives.
fn decode_identity_key(
    cursor: &mut Cursor<'_>,
    len: usize,
    source_identity: &SourceIdentity,
) -> Result<EntryIdentityKey, SnapshotDecodeError> {
    let mut encoded_key = [0; 32];
    encoded_key.copy_from_slice(cursor.take(32)?);
    if cursor.offset != len {
        return Err(SnapshotDecodeError::TrailingData);
    }
    let identity_key = source_identity.identity_key();
    if encoded_key != *identity_key.as_bytes() {
        return Err(SnapshotDecodeError::IdentityMismatch);
    }
    Ok(identity_key)
}

/// The source identity of a snapshot: strength, backend (the caller's for v5, checked by kind and
/// endpoint fingerprint; the stored one for v4, checked by kind), and stable bytes.
fn decode_source_identity(
    cursor: &mut Cursor<'_>,
    version: u8,
    backend_kind: BackendKind,
    backend: &BackendIdentity,
) -> Result<SourceIdentity, SnapshotDecodeError> {
    let strength =
        IdentityStrength::from_tag(cursor.byte()?).ok_or(SnapshotDecodeError::Malformed)?;
    if backend.kind() != backend_kind {
        return Err(SnapshotDecodeError::BackendMismatch);
    }
    let backend = if version == VERSION_WITH_BACKEND_ID {
        let backend_id =
            std::str::from_utf8(cursor.bytes()?).map_err(|_| SnapshotDecodeError::Malformed)?;
        BackendIdentity::new(backend_kind, backend_id)
            .map_err(|_| SnapshotDecodeError::Malformed)?
    } else if cursor.take(4)? == endpoint_fingerprint(backend) {
        backend.clone()
    } else {
        return Err(SnapshotDecodeError::BackendMismatch);
    };
    SourceIdentity::new(backend, strength, cursor.bytes()?)
        .map_err(|_| SnapshotDecodeError::Malformed)
}

/// Four bytes of the endpoint's hash: enough to tell a wrong endpoint from a corrupted key, which
/// the 32-byte identity key check then decides for the rest (a wrong endpoint sharing the four
/// bytes, 1 in 2^32, is reported as `IdentityMismatch`).
fn endpoint_fingerprint(backend: &BackendIdentity) -> [u8; 4] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"data-mover/endpoint-fingerprint/v1\0");
    hasher.update(backend.stable_id().as_bytes());
    let hash = hasher.finalize();
    let mut fingerprint = [0; 4];
    fingerprint.copy_from_slice(&hash.as_bytes()[..4]);
    fingerprint
}

fn decode_symlink_target(
    cursor: &mut Cursor<'_>,
    kind: EntryKind,
) -> Result<Option<SymlinkTarget>, SnapshotDecodeError> {
    match cursor.byte()? {
        0 if kind != EntryKind::Symlink => Ok(None),
        1 if kind == EntryKind::Symlink => {
            let encoding = match cursor.byte()? {
                0 => SymlinkTargetEncoding::UnixBytes,
                1 => SymlinkTargetEncoding::WindowsWide,
                _ => return Err(SnapshotDecodeError::Malformed),
            };
            SymlinkTarget::new(encoding, cursor.bytes()?.to_vec())
                .map(Some)
                .map_err(|_| SnapshotDecodeError::Malformed)
        }
        _ => Err(SnapshotDecodeError::Malformed),
    }
}

pub(in crate::model) fn put_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    let Ok(len) = u32::try_from(bytes.len()) else {
        unreachable!("model field invariant limits encoded lengths");
    };
    output.extend_from_slice(&len.to_le_bytes());
    output.extend_from_slice(bytes);
}
pub(super) const fn backend_tag(kind: BackendKind) -> u8 {
    match kind {
        BackendKind::Local => 0,
        BackendKind::Nfs => 1,
        BackendKind::Cifs => 2,
        BackendKind::S3 => 3,
        BackendKind::Hdfs => 4,
    }
}
const fn backend_from_tag(tag: u8) -> Option<BackendKind> {
    match tag {
        0 => Some(BackendKind::Local),
        1 => Some(BackendKind::Nfs),
        2 => Some(BackendKind::Cifs),
        3 => Some(BackendKind::S3),
        4 => Some(BackendKind::Hdfs),
        _ => None,
    }
}
fn encode_kind(output: &mut Vec<u8>, kind: EntryKind) {
    let tag = match kind {
        EntryKind::File => 0,
        EntryKind::Directory => 1,
        EntryKind::Symlink => 2,
        EntryKind::Special(SpecialFileKind::BlockDevice) => 3,
        EntryKind::Special(SpecialFileKind::CharacterDevice) => 4,
        EntryKind::Special(SpecialFileKind::Fifo) => 5,
        EntryKind::Special(SpecialFileKind::Socket) => 6,
    };
    output.push(tag);
}
fn decode_kind(cursor: &mut Cursor<'_>) -> Result<EntryKind, SnapshotDecodeError> {
    match cursor.byte()? {
        0 => Ok(EntryKind::File),
        1 => Ok(EntryKind::Directory),
        2 => Ok(EntryKind::Symlink),
        3 => Ok(EntryKind::Special(SpecialFileKind::BlockDevice)),
        4 => Ok(EntryKind::Special(SpecialFileKind::CharacterDevice)),
        5 => Ok(EntryKind::Special(SpecialFileKind::Fifo)),
        6 => Ok(EntryKind::Special(SpecialFileKind::Socket)),
        _ => Err(SnapshotDecodeError::Malformed),
    }
}
fn encode_size(output: &mut Vec<u8>, size: Option<u64>) {
    match size {
        Some(value) => {
            output.push(1);
            output.extend_from_slice(&value.to_le_bytes());
        }
        None => output.push(0),
    }
}
fn decode_size(cursor: &mut Cursor<'_>) -> Result<Option<u64>, SnapshotDecodeError> {
    match cursor.byte()? {
        0 => Ok(None),
        1 => {
            let mut value = [0; 8];
            value.copy_from_slice(cursor.take(8)?);
            Ok(Some(u64::from_le_bytes(value)))
        }
        _ => Err(SnapshotDecodeError::Malformed),
    }
}
pub(in crate::model) fn encode_time(output: &mut Vec<u8>, time: Option<StorageTimestamp>) {
    match time {
        Some(value) => {
            output.push(1);
            output.extend_from_slice(&value.unix_nanos().to_le_bytes());
            output.push(match value.precision() {
                TimePrecision::Seconds => 0,
                TimePrecision::Milliseconds => 1,
                TimePrecision::Microseconds => 2,
                TimePrecision::Nanoseconds => 3,
                TimePrecision::HundredNanoseconds => 4,
            });
        }
        None => output.push(0),
    }
}
pub(in crate::model) fn decode_time(
    cursor: &mut Cursor<'_>,
) -> Result<Option<StorageTimestamp>, SnapshotDecodeError> {
    match cursor.byte()? {
        0 => Ok(None),
        1 => {
            let mut value = [0; 16];
            value.copy_from_slice(cursor.take(16)?);
            let precision = match cursor.byte()? {
                0 => TimePrecision::Seconds,
                1 => TimePrecision::Milliseconds,
                2 => TimePrecision::Microseconds,
                3 => TimePrecision::Nanoseconds,
                4 => TimePrecision::HundredNanoseconds,
                _ => return Err(SnapshotDecodeError::Malformed),
            };
            StorageTimestamp::new(i128::from_le_bytes(value), precision)
                .map(Some)
                .map_err(|_| SnapshotDecodeError::Malformed)
        }
        _ => Err(SnapshotDecodeError::Malformed),
    }
}
fn decode_facts(
    cursor: &mut Cursor<'_>,
    kind: BackendKind,
) -> Result<PrivateBackendEntryFacts, SnapshotDecodeError> {
    let tag = cursor.byte()?;
    let bytes = cursor.bytes()?.to_vec();
    match (tag, kind) {
        (0, _) if bytes.is_empty() => Ok(PrivateBackendEntryFacts::None),
        (1, BackendKind::Local) => Ok(PrivateBackendEntryFacts::Local(bytes)),
        (2, BackendKind::Nfs) => Ok(PrivateBackendEntryFacts::Nfs(bytes)),
        (3, BackendKind::Cifs) => Ok(PrivateBackendEntryFacts::Cifs(bytes)),
        (4, BackendKind::S3) => Ok(PrivateBackendEntryFacts::S3(bytes)),
        (5, BackendKind::Hdfs) => Ok(PrivateBackendEntryFacts::Hdfs(bytes)),
        _ => Err(SnapshotDecodeError::Malformed),
    }
}

/// Flags of the v6 version record.
const VERSION_LATEST: u8 = 1;
const VERSION_DELETE_MARKER: u8 = 2;
const VERSION_HAS_ID: u8 = 4;

fn encode_version(output: &mut Vec<u8>, version: &EntryVersion) {
    let mut flags = 0;
    if version.is_latest() {
        flags |= VERSION_LATEST;
    }
    if version.is_delete_marker() {
        flags |= VERSION_DELETE_MARKER;
    }
    if version.id().is_some() {
        flags |= VERSION_HAS_ID;
    }
    output.push(flags);
    if let Some(id) = version.id() {
        put_bytes(output, id.as_bytes());
    }
}

fn decode_version(cursor: &mut Cursor<'_>) -> Result<EntryVersion, SnapshotDecodeError> {
    let flags = cursor.byte()?;
    if flags & !(VERSION_LATEST | VERSION_DELETE_MARKER | VERSION_HAS_ID) != 0 {
        return Err(SnapshotDecodeError::Malformed);
    }
    let id = if flags & VERSION_HAS_ID == 0 {
        None
    } else {
        let id =
            std::str::from_utf8(cursor.bytes()?).map_err(|_| SnapshotDecodeError::Malformed)?;
        Some(id.to_string())
    };
    EntryVersion::new(
        id,
        flags & VERSION_LATEST != 0,
        flags & VERSION_DELETE_MARKER != 0,
    )
    .map_err(|_| SnapshotDecodeError::Malformed)
}
