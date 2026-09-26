//! One child of a listed directory ([`ListedChild`]): a [`SourceDescriptor`] with its full path
//! replaced by its name, the path being the listing contract's [`child_path`].

use bytes::Bytes;

use super::DescentHint;
use super::names::{child_path, is_segment};
use crate::model::{
    EntryKind, ModelValueError, SourceIdentity, SourceVersion, StoragePath, TimestampMetadata,
};
use crate::storage::{InlineOwnership, ListingFacts, SourceDescriptor};

/// One child of a listed directory: [`SourceDescriptor`] with its full path replaced by its name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListedChild {
    pub(super) name: String,
    /// File, directory, symlink or special; a CIFS reparse point is a file.
    pub kind: EntryKind,
    /// A file's size; `None` where a size means nothing (a directory, an S3 prefix).
    pub size: Option<u64>,
    /// The stable identity the listing reported for the child.
    pub source_identity: SourceIdentity,
    pub(crate) backend_fact: Option<Bytes>,
    pub(crate) content_version: Option<Bytes>,
    pub(crate) inline_timestamps: Option<TimestampMetadata>,
    pub(crate) inline_mode: Option<u32>,
    pub(crate) inline_ownership: Option<InlineOwnership>,
    pub(crate) version: SourceVersion,
    pub(crate) listing: ListingFacts,
    /// See [`DescentHint`]; meaningful only on a directory.
    pub(super) descent_hint: Option<DescentHint>,
}

impl ListedChild {
    /// A child named `name`, without backend-private facts.
    ///
    /// # Errors
    /// Returns an error when `name` is not one path segment: empty, `.`, `..`, or containing `/`
    /// or NUL.
    pub fn new(
        name: impl Into<String>,
        kind: EntryKind,
        size: Option<u64>,
        source_identity: SourceIdentity,
    ) -> Result<Self, ModelValueError> {
        let name = name.into();
        if !is_segment(&name) {
            return Err(ModelValueError::new(
                "listed_child_name",
                "must be one non-empty path segment other than '.' and '..'",
            ));
        }
        Ok(Self {
            name,
            kind,
            size,
            source_identity,
            backend_fact: None,
            content_version: None,
            inline_timestamps: None,
            inline_mode: None,
            inline_ownership: None,
            version: SourceVersion::Current,
            listing: ListingFacts::default(),
            descent_hint: None,
        })
    }

    /// The same child carrying `hint` for the listing of it, when it is a directory a traversal
    /// descends into ([`DescentHint`]).
    #[must_use]
    pub fn with_descent_hint(mut self, hint: DescentHint) -> Self {
        self.descent_hint = Some(hint);
        self
    }

    /// The hint for listing this child, if its listing gave one.
    #[must_use]
    pub const fn descent_hint(&self) -> Option<&DescentHint> {
        self.descent_hint.as_ref()
    }

    /// The child's name: the last segment of its path.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Timestamps the listing already returned, if any.
    #[must_use]
    pub const fn inline_timestamps(&self) -> Option<TimestampMetadata> {
        self.inline_timestamps
    }

    /// Permission bits the listing could already derive, if any (display-grade, see
    /// [`SourceDescriptor::inline_mode`]).
    #[must_use]
    pub const fn inline_mode(&self) -> Option<u32> {
        self.inline_mode
    }

    /// The descriptor of this child of `directory`, its path being [`child_path`].
    ///
    /// # Errors
    /// Returns an error when the joined path exceeds the storage path limit.
    pub fn into_descriptor(
        self,
        directory: &StoragePath,
    ) -> Result<SourceDescriptor, ModelValueError> {
        let path = child_path(directory, &self.name)?;
        Ok(self.at(path))
    }

    /// The descriptor of this child at `path`.
    pub(super) fn at(self, path: StoragePath) -> SourceDescriptor {
        SourceDescriptor {
            path,
            kind: self.kind,
            size: self.size,
            source_identity: self.source_identity,
            backend_fact: self.backend_fact,
            content_version: self.content_version,
            inline_timestamps: self.inline_timestamps,
            inline_mode: self.inline_mode,
            inline_ownership: self.inline_ownership,
            version: self.version,
            listing: self.listing,
        }
    }
}
