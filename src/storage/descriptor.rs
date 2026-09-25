//! The neutral description of one source entry that every role hands back: path, kind, size,
//! identity, and the facts the enumerating operation already returned.

use bytes::Bytes;

use crate::model::{
    EntryKind, EntryVersion, SourceIdentity, SourceVersion, StoragePath, TimestampMetadata,
};

/// What a versioned listing adds to a child (ADR-0006 C22); the default for every other listing.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ListingFacts {
    /// The listed version, from a listing of every version; carried onto the observed entry.
    pub(crate) version: Option<Box<EntryVersion>>,
    /// Position among the children that share this child's name and kind, in listing order: a
    /// key's versions oldest first. Orders them when a traversal sorts children by name.
    pub(crate) rank: u32,
    /// Nothing behind the child can be observed: an S3 common prefix or a delete marker. The
    /// traversal takes what the listing gave and asks the metadata role nothing.
    pub(crate) listing_only: bool,
}

/// A stable neutral source description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceDescriptor {
    pub path: StoragePath,
    pub kind: EntryKind,
    pub size: Option<u64>,
    pub source_identity: SourceIdentity,
    pub(crate) backend_fact: Option<Bytes>,
    /// Content-change observation, separate from stable inode/file-handle identity.
    pub(crate) content_version: Option<Bytes>,
    /// Timestamps the listing or stat already returned, so traversal can avoid a per-entry
    /// metadata round trip when nothing beyond timestamps was requested.
    pub(crate) inline_timestamps: Option<TimestampMetadata>,
    /// Approximate POSIX permission bits the enumerating operation could already derive, when it
    /// carried enough to derive any. `None` means this path observed nothing about permissions,
    /// which is distinct from an observed `0o644`.
    ///
    /// It is a display-grade value for enumeration output, never an input to metadata
    /// application: a backend that cannot observe real ownership still reports
    /// `ownership_mode` as unsupported through the [`Metadata`] role. On CIFS it is derived
    /// from `FILE_ATTRIBUTE_READONLY`, which listings carry; the `Stat` verb builds its
    /// descriptor from facts without the attribute and leaves it `None`.
    pub(crate) inline_mode: Option<u32>,
    /// The source version this description is of, as the describe resolved it: a versioned store
    /// turns `Current` into the version it found, so every later read, metadata observation and
    /// native copy uses the same one. `Current` where the store has no version to pin.
    pub(crate) version: SourceVersion,
    /// Facts only a versioned or synthetic listing supplies; see [`ListingFacts`].
    pub(crate) listing: ListingFacts,
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
            backend_fact: None,
            content_version: None,
            inline_timestamps: None,
            inline_mode: None,
            version: SourceVersion::Current,
            listing: ListingFacts::default(),
        }
    }

    /// The source version this description pins.
    #[must_use]
    pub const fn version(&self) -> &SourceVersion {
        &self.version
    }

    pub(crate) fn with_backend_fact(mut self, fact: Bytes) -> Self {
        self.backend_fact = Some(fact);
        self
    }

    /// Attaches timestamps that the enumerating operation already returned.
    #[must_use]
    pub(crate) const fn with_inline_timestamps(mut self, timestamps: TimestampMetadata) -> Self {
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
    pub(crate) const fn with_inline_mode(mut self, mode: u32) -> Self {
        self.inline_mode = Some(mode);
        self
    }

    /// Permission bits the enumerating operation could already derive, if any.
    #[must_use]
    pub const fn inline_mode(&self) -> Option<u32> {
        self.inline_mode
    }
}
