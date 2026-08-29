use std::fmt;

use super::{
    BackendIdentity, BackendKind, EntryKind, MAX_MODEL_FIELD_BYTES, ModelValueError,
    SpecialFileKind, StoragePath, StorageTimestamp, TimePrecision,
};

const MAGIC: &[u8; 4] = b"DMES";
const VERSION: u8 = 2;

/// How strongly a source identity survives namespace changes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityStrength {
    /// Stable only while the entry remains at the same path.
    PathScoped,
    /// Stable within the connected backend namespace.
    StableWithinBackend,
    /// Stable for one immutable backend version.
    VersionScoped,
}

impl IdentityStrength {
    const fn tag(self) -> u8 {
        match self {
            Self::PathScoped => 0,
            Self::StableWithinBackend => 1,
            Self::VersionScoped => 2,
        }
    }

    const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::PathScoped),
            1 => Some(Self::StableWithinBackend),
            2 => Some(Self::VersionScoped),
            _ => None,
        }
    }
}

/// A small backend-issued identity used only to derive an entry identity key.
#[derive(Clone, Eq, PartialEq)]
pub struct SourceIdentity {
    backend: BackendIdentity,
    strength: IdentityStrength,
    stable_bytes: Vec<u8>,
}

impl fmt::Debug for SourceIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceIdentity")
            .field("backend_kind", &self.backend.kind())
            .field("strength", &self.strength)
            .field("stable_bytes", &"<redacted>")
            .finish()
    }
}

impl SourceIdentity {
    /// Creates a backend-bound identity from stable opaque bytes.
    ///
    /// # Errors
    /// Returns an error when the bytes are empty or exceed the snapshot field limit.
    pub fn new(
        backend: BackendIdentity,
        strength: IdentityStrength,
        stable_bytes: impl AsRef<[u8]>,
    ) -> Result<Self, ModelValueError> {
        let stable_bytes = stable_bytes.as_ref();
        if stable_bytes.is_empty() || stable_bytes.len() > MAX_MODEL_FIELD_BYTES {
            return Err(ModelValueError::new(
                "source_identity",
                "stable bytes must be non-empty and bounded",
            ));
        }
        Ok(Self {
            backend,
            strength,
            stable_bytes: stable_bytes.to_vec(),
        })
    }

    /// Returns the advertised identity strength.
    #[must_use]
    pub const fn strength(&self) -> IdentityStrength {
        self.strength
    }

    /// Derives the fixed opaque comparison key. This is not a content hash.
    #[must_use]
    pub fn identity_key(&self) -> EntryIdentityKey {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"data-mover/source-identity/v1\0");
        hasher.update(&[backend_tag(self.backend.kind()), self.strength.tag()]);
        hasher.update(&(self.backend.stable_id().len() as u64).to_le_bytes());
        hasher.update(self.backend.stable_id().as_bytes());
        hasher.update(&(self.stable_bytes.len() as u64).to_le_bytes());
        hasher.update(&self.stable_bytes);
        EntryIdentityKey(*hasher.finalize().as_bytes())
    }
}

/// A fixed 32-byte comparison projection of a source identity.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct EntryIdentityKey([u8; 32]);

impl EntryIdentityKey {
    /// Returns the opaque key bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for EntryIdentityKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EntryIdentityKey(<opaque-32-bytes>)")
    }
}

/// Metadata observation container reserved for the metadata-plan ticket.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataObservations {
    schema_version: u8,
}

/// Lossless protocol-neutral symlink payload captured without following the link.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SymlinkTargetEncoding {
    /// Unix/NFS-style uninterpreted pathname bytes.
    UnixBytes,
    /// Windows pathname encoded as little-endian UTF-16 code units.
    WindowsWide,
}

/// Exact target bytes together with their pathname encoding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SymlinkTarget {
    encoding: SymlinkTargetEncoding,
    bytes: Vec<u8>,
}

impl SymlinkTarget {
    /// Creates a bounded opaque link target.
    ///
    /// # Errors
    /// Returns an error when the target is empty or exceeds the model field limit.
    pub fn new(
        encoding: SymlinkTargetEncoding,
        bytes: impl Into<Vec<u8>>,
    ) -> Result<Self, ModelValueError> {
        let bytes = bytes.into();
        let malformed_wide = encoding == SymlinkTargetEncoding::WindowsWide && bytes.len() % 2 != 0;
        if bytes.is_empty() || bytes.len() > MAX_MODEL_FIELD_BYTES || malformed_wide {
            return Err(ModelValueError::new(
                "symlink_target",
                "target bytes must be non-empty and bounded",
            ));
        }
        Ok(Self { encoding, bytes })
    }

    /// Returns the exact target payload.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns how the exact target bytes must be interpreted.
    #[must_use]
    pub const fn encoding(&self) -> SymlinkTargetEncoding {
        self.encoding
    }
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) enum PrivateBackendEntryFacts {
    None,
    Local(Vec<u8>),
    Nfs(Vec<u8>),
    Cifs(Vec<u8>),
    S3(Vec<u8>),
    Hdfs(Vec<u8>),
}

impl fmt::Debug for PrivateBackendEntryFacts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, len) = match self {
            Self::None => ("none", 0),
            Self::Local(bytes) => ("local", bytes.len()),
            Self::Nfs(bytes) => ("nfs", bytes.len()),
            Self::Cifs(bytes) => ("cifs", bytes.len()),
            Self::S3(bytes) => ("s3", bytes.len()),
            Self::Hdfs(bytes) => ("hdfs", bytes.len()),
        };
        formatter
            .debug_struct("PrivateBackendEntryFacts")
            .field("kind", &kind)
            .field("len", &len)
            .finish()
    }
}

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

/// An immutable point-in-time observation of one storage entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedEntry {
    identity_key: EntryIdentityKey,
    backend_kind: BackendKind,
    path: StoragePath,
    kind: EntryKind,
    size: Option<u64>,
    modified: Option<StorageTimestamp>,
    symlink_target: Option<SymlinkTarget>,
    source_identity: SourceIdentity,
    metadata: MetadataObservations,
    backend_fact: PrivateBackendEntryFacts,
}

impl ObservedEntry {
    /// Creates an observation containing only neutral facts.
    ///
    /// # Errors
    /// Returns an error for symlinks, which require [`Self::new_symlink`].
    pub fn new(
        path: StoragePath,
        kind: EntryKind,
        size: Option<u64>,
        modified: Option<StorageTimestamp>,
        source_identity: SourceIdentity,
    ) -> Result<Self, ModelValueError> {
        if kind == EntryKind::Symlink {
            return Err(ModelValueError::new(
                "kind",
                "symlinks require an observed target",
            ));
        }
        let identity_key = source_identity.identity_key();
        let backend_kind = source_identity.backend.kind();
        Ok(Self {
            identity_key,
            backend_kind,
            path,
            kind,
            size,
            modified,
            symlink_target: None,
            source_identity,
            metadata: MetadataObservations::default(),
            backend_fact: PrivateBackendEntryFacts::None,
        })
    }

    /// Creates a complete symlink observation with its exact target.
    ///
    /// # Errors
    /// Returns an error when model fields violate their bounds.
    pub fn new_symlink(
        path: StoragePath,
        modified: Option<StorageTimestamp>,
        source_identity: SourceIdentity,
        target: SymlinkTarget,
    ) -> Result<Self, ModelValueError> {
        let identity_key = source_identity.identity_key();
        let backend_kind = source_identity.backend.kind();
        Ok(Self {
            identity_key,
            backend_kind,
            path,
            kind: EntryKind::Symlink,
            size: None,
            modified,
            symlink_target: Some(target),
            source_identity,
            metadata: MetadataObservations::default(),
            backend_fact: PrivateBackendEntryFacts::None,
        })
    }

    /// Attaches backend-owned facts without exposing their encoding publicly.
    #[allow(dead_code)]
    pub(crate) fn with_backend_fact_bytes(
        mut self,
        bytes: Vec<u8>,
    ) -> Result<Self, ModelValueError> {
        if bytes.len() > MAX_MODEL_FIELD_BYTES {
            return Err(ModelValueError::new(
                "backend_fact",
                "exceeds model field limit",
            ));
        }
        self.backend_fact = match self.backend_kind {
            BackendKind::Local => PrivateBackendEntryFacts::Local(bytes),
            BackendKind::Nfs => PrivateBackendEntryFacts::Nfs(bytes),
            BackendKind::Cifs => PrivateBackendEntryFacts::Cifs(bytes),
            BackendKind::S3 => PrivateBackendEntryFacts::S3(bytes),
            BackendKind::Hdfs => PrivateBackendEntryFacts::Hdfs(bytes),
        };
        Ok(self)
    }

    /// Lends private facts only to crate-internal backend/native implementations.
    #[allow(dead_code)]
    pub(crate) const fn backend_facts(&self) -> &PrivateBackendEntryFacts {
        &self.backend_fact
    }

    /// Returns the opaque stable comparison key.
    #[must_use]
    pub const fn identity_key(&self) -> EntryIdentityKey {
        self.identity_key
    }
    /// Returns the owning backend kind.
    #[must_use]
    pub const fn backend_kind(&self) -> BackendKind {
        self.backend_kind
    }
    /// Returns the observed path.
    #[must_use]
    pub const fn path(&self) -> &StoragePath {
        &self.path
    }
    /// Returns the neutral entry kind.
    #[must_use]
    pub const fn kind(&self) -> EntryKind {
        self.kind
    }
    /// Returns the observed logical size when applicable.
    #[must_use]
    pub const fn size(&self) -> Option<u64> {
        self.size
    }
    /// Returns the observed modification time.
    #[must_use]
    pub const fn modified(&self) -> Option<StorageTimestamp> {
        self.modified
    }
    /// Returns the exact symlink target when this observation is a link.
    #[must_use]
    pub const fn symlink_target(&self) -> Option<&SymlinkTarget> {
        self.symlink_target.as_ref()
    }
    /// Returns metadata observations captured by the same enumeration.
    #[must_use]
    pub const fn metadata(&self) -> &MetadataObservations {
        &self.metadata
    }

    /// Encodes a lossless, versioned snapshot owned by data-mover.
    #[must_use]
    pub fn encode_snapshot(&self) -> EntrySnapshot {
        let mut output = Vec::new();
        output.extend_from_slice(MAGIC);
        output.push(VERSION);
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
        put_bytes(
            &mut output,
            self.source_identity.backend.stable_id().as_bytes(),
        );
        put_bytes(&mut output, &self.source_identity.stable_bytes);
        output.push(self.metadata.schema_version);
        self.backend_fact.encode(&mut output);
        output.extend_from_slice(self.identity_key.as_bytes());
        EntrySnapshot(output)
    }

    /// Reconstructs an observation solely from an unchanged snapshot.
    ///
    /// # Errors
    /// Returns a typed error for malformed, unsupported, inconsistent, or trailing encoding.
    pub fn decode_snapshot(bytes: &[u8]) -> Result<Self, SnapshotDecodeError> {
        decode_snapshot(bytes)
    }
}

/// Opaque encoded observation bytes suitable for persistence or transport.
#[derive(Clone, Eq, PartialEq)]
pub struct EntrySnapshot(Vec<u8>);

impl EntrySnapshot {
    /// Returns the opaque bytes without exposing their internal fields.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for EntrySnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EntrySnapshot")
            .field("len", &self.0.len())
            .finish()
    }
}

/// Stable failure categories for snapshot reconstruction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotDecodeError {
    InvalidMagic,
    UnsupportedVersion,
    Truncated,
    FieldTooLarge,
    Malformed,
    IdentityMismatch,
    TrailingData,
}

impl fmt::Display for SnapshotDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "entry snapshot decode failed: {self:?}")
    }
}

impl std::error::Error for SnapshotDecodeError {}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
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
    fn byte(&mut self) -> Result<u8, SnapshotDecodeError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, SnapshotDecodeError> {
        let mut value = [0; 4];
        value.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(value))
    }
    fn bytes(&mut self) -> Result<&'a [u8], SnapshotDecodeError> {
        let len = self.u32()? as usize;
        if len > MAX_MODEL_FIELD_BYTES {
            return Err(SnapshotDecodeError::FieldTooLarge);
        }
        self.take(len)
    }
}

fn decode_snapshot(bytes: &[u8]) -> Result<ObservedEntry, SnapshotDecodeError> {
    let mut cursor = Cursor::new(bytes);
    if cursor.take(4)? != MAGIC {
        return Err(SnapshotDecodeError::InvalidMagic);
    }
    if cursor.byte()? != VERSION {
        return Err(SnapshotDecodeError::UnsupportedVersion);
    }
    let backend_kind = backend_from_tag(cursor.byte()?).ok_or(SnapshotDecodeError::Malformed)?;
    let path = std::str::from_utf8(cursor.bytes()?).map_err(|_| SnapshotDecodeError::Malformed)?;
    let path = StoragePath::new(path).map_err(|_| SnapshotDecodeError::Malformed)?;
    let kind = decode_kind(&mut cursor)?;
    let symlink_target = match cursor.byte()? {
        0 if kind != EntryKind::Symlink => None,
        1 if kind == EntryKind::Symlink => {
            let encoding = match cursor.byte()? {
                0 => SymlinkTargetEncoding::UnixBytes,
                1 => SymlinkTargetEncoding::WindowsWide,
                _ => return Err(SnapshotDecodeError::Malformed),
            };
            Some(
                SymlinkTarget::new(encoding, cursor.bytes()?.to_vec())
                    .map_err(|_| SnapshotDecodeError::Malformed)?,
            )
        }
        _ => return Err(SnapshotDecodeError::Malformed),
    };
    let size = decode_size(&mut cursor)?;
    let modified = decode_time(&mut cursor)?;
    let strength =
        IdentityStrength::from_tag(cursor.byte()?).ok_or(SnapshotDecodeError::Malformed)?;
    let backend_id =
        std::str::from_utf8(cursor.bytes()?).map_err(|_| SnapshotDecodeError::Malformed)?;
    let backend = BackendIdentity::new(backend_kind, backend_id)
        .map_err(|_| SnapshotDecodeError::Malformed)?;
    let source_identity = SourceIdentity::new(backend, strength, cursor.bytes()?)
        .map_err(|_| SnapshotDecodeError::Malformed)?;
    let metadata = MetadataObservations {
        schema_version: cursor.byte()?,
    };
    if metadata.schema_version != 0 {
        return Err(SnapshotDecodeError::Malformed);
    }
    let backend_fact = decode_facts(&mut cursor, backend_kind)?;
    let mut encoded_key = [0; 32];
    encoded_key.copy_from_slice(cursor.take(32)?);
    if cursor.offset != bytes.len() {
        return Err(SnapshotDecodeError::TrailingData);
    }
    let identity_key = source_identity.identity_key();
    if encoded_key != *identity_key.as_bytes() {
        return Err(SnapshotDecodeError::IdentityMismatch);
    }
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
    })
}

fn put_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    let Ok(len) = u32::try_from(bytes.len()) else {
        unreachable!("model field invariant limits encoded lengths");
    };
    output.extend_from_slice(&len.to_le_bytes());
    output.extend_from_slice(bytes);
}
const fn backend_tag(kind: BackendKind) -> u8 {
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
fn encode_time(output: &mut Vec<u8>, time: Option<StorageTimestamp>) {
    match time {
        Some(value) => {
            output.push(1);
            output.extend_from_slice(&value.unix_nanos().to_le_bytes());
            output.push(match value.precision() {
                TimePrecision::Seconds => 0,
                TimePrecision::Milliseconds => 1,
                TimePrecision::Microseconds => 2,
                TimePrecision::Nanoseconds => 3,
            });
        }
        None => output.push(0),
    }
}
fn decode_time(cursor: &mut Cursor<'_>) -> Result<Option<StorageTimestamp>, SnapshotDecodeError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_backend_facts_roundtrip_without_public_inspection() {
        let backend = BackendIdentity::new(BackendKind::Nfs, "cluster")
            .unwrap_or_else(|error| panic!("{error}"));
        let source = SourceIdentity::new(backend, IdentityStrength::StableWithinBackend, b"fh")
            .unwrap_or_else(|error| panic!("{error}"));
        let entry = ObservedEntry::new(StoragePath::root(), EntryKind::File, None, None, source)
            .unwrap_or_else(|error| panic!("{error}"));
        let entry = entry
            .with_backend_fact_bytes(vec![0, 1, 255])
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(!format!("{entry:?}").contains("255"));
        let rebuilt = ObservedEntry::decode_snapshot(entry.encode_snapshot().as_bytes())
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(rebuilt.backend_fact, entry.backend_fact);
    }

    #[test]
    fn symlink_without_target_is_rejected_at_construction_and_decode() {
        let backend = BackendIdentity::new(BackendKind::Local, "local")
            .unwrap_or_else(|error| panic!("{error}"));
        let source = SourceIdentity::new(backend, IdentityStrength::StableWithinBackend, b"inode")
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(
            ObservedEntry::new(
                StoragePath::root(),
                EntryKind::Symlink,
                None,
                None,
                source.clone(),
            )
            .is_err()
        );
        let file = ObservedEntry::new(StoragePath::root(), EntryKind::File, None, None, source)
            .unwrap_or_else(|error| panic!("{error}"));
        let mut malformed = file.encode_snapshot().as_bytes().to_vec();
        malformed[10] = 2;
        assert_eq!(
            ObservedEntry::decode_snapshot(&malformed),
            Err(SnapshotDecodeError::Malformed)
        );
    }
}
