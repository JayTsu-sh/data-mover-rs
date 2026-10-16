use std::fmt;

use super::{
    BackendIdentity, BackendKind, EntryKind, MAX_MODEL_FIELD_BYTES, MetadataObservations,
    ModelValueError, ObjectHistory, SourceVersion, StoragePath, StorageTimestamp, VersionRecord,
};

#[path = "observation_codec.rs"]
mod codec;
#[path = "history_codec.rs"]
mod history_codec;
use codec::backend_tag;
pub(super) use codec::{Cursor, decode_time, encode_time, put_bytes};

/// How strongly a source identity survives namespace changes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityStrength {
    /// Stable only while the entry remains at the same path.
    ///
    /// Contract: the stable bytes of a `PathScoped` identity **must include the path**: CIFS
    /// without a file id and S3 (current objects, `"null"` versions and delete markers) put the
    /// length-prefixed path before what identifies the content, HDFS a domain tag, the
    /// length-prefixed path, size and mtime, Local off Unix the path alone. Content facts alone
    /// (an S3 `ETag`, a size and time) are shared by entries at different paths — every empty S3
    /// object has the same `ETag` — and would give them one identity key, so a consumer joining
    /// two scans by identity key would cross-match them.
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

/// Longest identity held without an allocation. ONTAP's 32-byte NFS handles, a CIFS file id (16)
/// and a Local device + inode (16) fit; longer NFS handles (up to 64 bytes on `NFSv3`, 128 on
/// `NFSv4`) and an S3 path + `ETag` go to the heap. 46 is what fits beside the tag and the
/// length in the same 48 bytes the heap variant's alignment takes anyway.
const INLINE_STABLE_BYTES: usize = 46;
const _: () = assert!(size_of::<StableBytes>() == 48);

/// The bytes of a source identity, inline when small so that describing an entry costs no
/// allocation for them.
#[derive(Clone)]
enum StableBytes {
    Inline {
        len: u8,
        bytes: [u8; INLINE_STABLE_BYTES],
    },
    Heap(Box<[u8]>),
}

impl StableBytes {
    fn new(value: &[u8]) -> Self {
        match u8::try_from(value.len()) {
            Ok(len) if value.len() <= INLINE_STABLE_BYTES => {
                let mut bytes = [0; INLINE_STABLE_BYTES];
                bytes[..value.len()].copy_from_slice(value);
                Self::Inline { len, bytes }
            }
            _ => Self::Heap(value.into()),
        }
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Inline { len, bytes } => &bytes[..usize::from(*len)],
            Self::Heap(bytes) => bytes,
        }
    }
}

impl PartialEq for StableBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for StableBytes {}

/// A small backend-issued identity used only to derive an entry identity key.
#[derive(Clone, Eq, PartialEq)]
pub struct SourceIdentity {
    backend: BackendIdentity,
    strength: IdentityStrength,
    stable_bytes: StableBytes,
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
    pub(crate) const fn backend(&self) -> &BackendIdentity {
        &self.backend
    }
    /// Creates a backend-bound identity from stable opaque bytes. A `PathScoped` identity's bytes
    /// must include the entry's path (see [`IdentityStrength::PathScoped`]).
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
            stable_bytes: StableBytes::new(stable_bytes),
        })
    }

    /// Returns the advertised identity strength.
    #[must_use]
    pub const fn strength(&self) -> IdentityStrength {
        self.strength
    }

    /// The identity bytes, only when they are a rename-stable key.
    ///
    /// `Some` for `StableWithinBackend` identities — the NFS file handle, the CIFS file id
    /// (16 big-endian bytes, matching the legacy `NASEntry.file_handle` encoding) — which a
    /// consumer may use as a `file_handle`-style join key across scans. `None` for
    /// `PathScoped` and `VersionScoped` identities: their bytes change with the path or the
    /// object version, so joining on them would turn every rename into a delete plus a create.
    /// A `PathScoped` identity's bytes always include the path, so its identity key is never
    /// shared by two paths; still, only `StableWithinBackend` identities are rename-stable join
    /// keys.
    #[must_use]
    pub fn stable_bytes(&self) -> Option<&[u8]> {
        matches!(self.strength, IdentityStrength::StableWithinBackend)
            .then_some(self.stable_bytes.as_slice())
    }

    /// Derives the fixed opaque comparison key. This is not a content hash.
    #[must_use]
    pub fn identity_key(&self) -> EntryIdentityKey {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"data-mover/source-identity/v1\0");
        hasher.update(&[backend_tag(self.backend.kind()), self.strength.tag()]);
        hasher.update(&(self.backend.stable_id().len() as u64).to_le_bytes());
        hasher.update(self.backend.stable_id().as_bytes());
        let stable_bytes = self.stable_bytes.as_slice();
        hasher.update(&(stable_bytes.len() as u64).to_le_bytes());
        hasher.update(stable_bytes);
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
    /// The key's history, or one segment of it, when the entry is a key of a versioned bucket
    /// (P7 §10); `None` for every other entry. Boxed: only a versioned bucket's entries have one,
    /// and an entry without one pays a pointer for it.
    history: Option<Box<ObjectHistory>>,
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
            history: None,
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
            history: None,
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

    pub(crate) fn with_metadata(mut self, metadata: MetadataObservations) -> Self {
        self.metadata = metadata;
        self
    }

    pub(crate) fn with_history(mut self, history: Option<Box<ObjectHistory>>) -> Self {
        self.history = history;
        self
    }

    /// This entry once per segment of its history, each of at most `cap` records (P7 Q8): the
    /// same entry — path, identity key, every entry-level field — with segments numbered from 0,
    /// only the last complete. An entry without a history, or with one of at most `cap` records,
    /// is itself.
    pub(crate) fn into_segments(mut self, cap: usize) -> Vec<Self> {
        let Some(history) = self.history.take() else {
            return vec![self];
        };
        if history.records().len() <= cap {
            self.history = Some(history);
            return vec![self];
        }
        history
            .split(cap)
            .into_iter()
            .map(|segment| {
                let mut entry = self.clone();
                entry.history = Some(Box::new(segment));
                entry
            })
            .collect()
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
    /// The identity the source reported, with its [`IdentityStrength`].
    ///
    /// Consumers that detect renames key on this: `StableWithinBackend` identities (NFS file
    /// handle, CIFS file id) survive a rename and map to a `file_handle`-style join key, while
    /// `PathScoped` ones must be treated as absent so the join falls back to paths.
    #[must_use]
    pub const fn source_identity(&self) -> &SourceIdentity {
        &self.source_identity
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
    /// Whether this entry is a directory object (P7 F5): a file stored under a key ending in `/`,
    /// such as `p/d/`, listed by its directory `p/d`. A filesystem cannot store content on a
    /// directory: a consumer copying there decides each such entry with
    /// `transfer::directory_object_action` before it requests a transfer.
    #[must_use]
    pub fn is_directory_object(&self) -> bool {
        self.kind == EntryKind::File && self.path.names_directory_object()
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
    /// The key's history, when this entry is a key of a bucket that was versioned when it was
    /// connected (P7 §10): every stored version and delete marker, oldest first — or one segment
    /// of it, when the key has more than [`MAX_HISTORY_RECORDS`](super::MAX_HISTORY_RECORDS)
    /// records. A key whose latest record is a delete marker is listed too:
    /// [`ObjectHistory::current`] is `None` for it. `None` for every other entry.
    #[must_use]
    pub fn history(&self) -> Option<&ObjectHistory> {
        self.history.as_deref()
    }
    /// The selector that copies this entry's current state with
    /// `TransferRequest::with_source_version`: `Current` for an entry without a history (only a
    /// file entry can be copied), and the current object's own selector for a key with one
    /// ([`VersionRecord::source`]). `None` for a key deleted now, or a segment that is not the
    /// key's last: there is no current object to copy. Each record of a history selects itself
    /// with [`VersionRecord::source`].
    #[must_use]
    pub fn source_version(&self) -> Option<SourceVersion> {
        match self.history.as_deref() {
            None => Some(SourceVersion::Current),
            Some(history) => history.current().and_then(VersionRecord::source),
        }
    }

    /// Encodes a lossless, versioned snapshot owned by data-mover.
    #[must_use]
    pub fn encode_snapshot(&self) -> EntrySnapshot {
        self.encode()
    }

    /// Reconstructs an observation from an unchanged snapshot taken on `backend`.
    ///
    /// A snapshot does not store the backend identity (every entry of one scan shares it): the
    /// caller passes the identity of the storage the scan ran on — `Storage::identity()`, or
    /// `storage::endpoint_identity(&config)` without connecting — and the stored identity key is
    /// recomputed from it. A snapshot taken on another endpoint or backend kind fails with
    /// [`SnapshotDecodeError::BackendMismatch`]. When that happens for a whole generation (the
    /// endpoint moved, or is spelled differently: IP versus hostname, another export alias), the
    /// caller treats it as having no previous generation, not as corruption.
    ///
    /// Only the current format is read (P7 §10.4, a development-phase rule): a snapshot written in
    /// any other — an older build's — is [`SnapshotDecodeError::UnsupportedVersion`], and the
    /// caller treats the entry as never seen (its next scan is a first one).
    ///
    /// # Errors
    /// Returns a typed error for malformed, unsupported, inconsistent, or trailing encoding, or for
    /// a snapshot taken on another backend.
    pub fn decode_snapshot(
        bytes: &[u8],
        backend: &BackendIdentity,
    ) -> Result<Self, SnapshotDecodeError> {
        codec::decode_snapshot(bytes, backend)
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
#[non_exhaustive]
pub enum SnapshotDecodeError {
    /// Not an entry snapshot.
    InvalidMagic,
    /// A snapshot format this build does not read.
    UnsupportedVersion,
    /// The bytes end before the snapshot does.
    Truncated,
    /// A field exceeds the model limits.
    FieldTooLarge,
    /// A field holds an impossible value.
    Malformed,
    /// The stored identity key disagrees with the fields it is derived from: a corrupted entry.
    IdentityMismatch,
    /// The snapshot was taken on another backend kind or endpoint than the one supplied — typically
    /// the whole generation, when the endpoint moved or is spelled differently. A lone occurrence in
    /// a generation that otherwise decodes is a corrupted row, not a moved endpoint.
    BackendMismatch,
    /// Bytes follow the end of the snapshot.
    TrailingData,
}

impl fmt::Display for SnapshotDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "entry snapshot decode failed: {self:?}")
    }
}

impl std::error::Error for SnapshotDecodeError {}

#[cfg(test)]
#[path = "observation_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "snapshot_format_tests.rs"]
mod snapshot_format_tests;
