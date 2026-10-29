//! The neutral description of one source entry that every role hands back: path, kind, size,
//! identity, and the facts the enumerating operation already returned.

use bytes::Bytes;

use crate::model::{
    EntryKind, ObjectHistory, OwnershipMode, RenameEvidence, SourceIdentity, SourceVersion,
    StoragePath, TimestampMetadata,
};

/// Numeric ownership the enumerating operation already returned, in the form the backend's own
/// metadata role would report it from the same attributes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InlineOwnership {
    /// Owner, group and the full mode, exactly as the metadata role's ownership observation.
    Known(OwnershipMode),
    /// The attributes named an owner or group the backend cannot turn into a number (an `NFSv4`
    /// name nfs-rs could not map); the metadata role reports such an entry's ownership as
    /// unsupported, never as a made-up id.
    Unmapped,
}

/// What a listing adds to a child beyond its description (ADR-0006 C22, P7 §10); the default for
/// most listings.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListingFacts {
    /// The key's whole history, from the listing of a versioned bucket; carried onto the observed
    /// entry, each record observed on its own when the plan asks for more than the listing gave.
    pub history: Option<Box<ObjectHistory>>,
    /// A real unversioned zero-byte object represented by this logical directory. Absent on
    /// virtual directories; retained separately so object-store copies do not invent or lose keys.
    pub directory_marker: Option<Box<SourceDescriptor>>,
    /// Nothing behind the child itself can be observed: an S3 common prefix, or a key whose latest
    /// record is a delete marker (the records of its history still can be). The traversal takes
    /// what the listing gave for the entry and asks the metadata role nothing about it.
    pub listing_only: bool,
}

/// A stable neutral source description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceDescriptor {
    pub path: StoragePath,
    pub kind: EntryKind,
    pub size: Option<u64>,
    pub source_identity: SourceIdentity,
    /// Optional evidence for comparing names across scans. Never used as a read or recovery guard.
    pub rename_evidence: Option<RenameEvidence>,
    pub backend_fact: Option<Bytes>,
    /// Content-change observation, separate from stable inode/file-handle identity.
    pub content_version: Option<Bytes>,
    /// Timestamps the listing or stat already returned, so traversal can avoid a per-entry
    /// metadata round trip when nothing beyond timestamps was requested.
    pub inline_timestamps: Option<TimestampMetadata>,
    /// Approximate POSIX permission bits the enumerating operation could already derive, when it
    /// carried enough to derive any. `None` means this path observed nothing about permissions,
    /// which is distinct from an observed `0o644`.
    ///
    /// It is a display-grade value for enumeration output, never an input to metadata
    /// application: a backend that cannot observe real ownership still reports
    /// `ownership_mode` as unsupported through the [`super::Metadata`] role. On CIFS it is derived
    /// from `FILE_ATTRIBUTE_READONLY`, which listings carry; the `Stat` verb builds its
    /// descriptor from facts without the attribute and leaves it `None`.
    pub inline_mode: Option<u32>,
    /// Numeric ownership from the same attributes as the timestamps, so a plan that asks for
    /// ownership needs no per-entry metadata round trip either. `None` when the enumerating
    /// operation carried none; only a backend whose metadata role reads ownership from those
    /// very attributes fills it (NFS, Local on Unix).
    pub inline_ownership: Option<InlineOwnership>,
    /// The source version this description is of, as the describe resolved it: a versioned store
    /// turns `Current` into the version it found, so every later read, metadata observation and
    /// native copy uses the same one. `Current` where the store has no version to pin.
    pub version: SourceVersion,
    /// Facts only a versioned or synthetic listing supplies; see [`ListingFacts`].
    pub listing: ListingFacts,
}

impl SourceDescriptor {
    /// Creates a neutral source descriptor without backend-private facts.
    #[must_use]
    pub fn new(
        path: StoragePath,
        kind: EntryKind,
        size: Option<u64>,
        source_identity: SourceIdentity,
    ) -> Self {
        Self {
            path,
            kind,
            size,
            source_identity,
            rename_evidence: None,
            backend_fact: None,
            content_version: None,
            inline_timestamps: None,
            inline_mode: None,
            inline_ownership: None,
            version: SourceVersion::Current,
            listing: ListingFacts::default(),
        }
    }

    /// The source version this description pins.
    #[must_use]
    pub const fn version(&self) -> &SourceVersion {
        &self.version
    }

    /// Candidate for a consumer's cross-scan rename comparison, if this backend observed one.
    #[must_use]
    pub const fn rename_evidence(&self) -> Option<&RenameEvidence> {
        self.rename_evidence.as_ref()
    }

    #[must_use]
    pub const fn with_rename_evidence(mut self, evidence: Option<RenameEvidence>) -> Self {
        self.rename_evidence = evidence;
        self
    }

    #[must_use]
    pub fn with_backend_fact(mut self, fact: Bytes) -> Self {
        self.backend_fact = Some(fact);
        self
    }

    /// Attaches timestamps that the enumerating operation already returned.
    #[must_use]
    pub const fn with_inline_timestamps(mut self, timestamps: TimestampMetadata) -> Self {
        self.inline_timestamps = Some(timestamps);
        self
    }

    /// Timestamps the enumerating operation already returned, if any.
    #[must_use]
    pub const fn inline_timestamps(&self) -> Option<TimestampMetadata> {
        self.inline_timestamps
    }

    /// Attaches permission bits the enumerating operation could already derive.
    #[must_use]
    pub const fn with_inline_mode(mut self, mode: u32) -> Self {
        self.inline_mode = Some(mode);
        self
    }

    /// Attaches numeric ownership the enumerating operation already returned.
    #[must_use]
    #[cfg_attr(not(unix), allow(dead_code))]
    pub const fn with_inline_ownership(mut self, ownership: InlineOwnership) -> Self {
        self.inline_ownership = Some(ownership);
        self
    }

    /// Permission bits the enumerating operation could already derive, if any.
    #[must_use]
    pub const fn inline_mode(&self) -> Option<u32> {
        self.inline_mode
    }
}
