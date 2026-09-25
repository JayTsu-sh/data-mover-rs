//! One listed version of an object in a versioned store (ADR-0006 C22): what a traversal of every
//! version (`TraversalVersions::All`) says about each entry it emits.

use super::{ModelValueError, SourceVersion};

/// The version id an unversioned bucket, or an object written before versioning, reports.
const NULL_VERSION: &str = "null";

/// One stored version, or delete marker, of an object as the listing reported it.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct EntryVersion {
    /// `None` for the `"null"` version: the one an unversioned (or suspended) bucket keeps.
    id: Option<String>,
    latest: bool,
    delete_marker: bool,
}

impl EntryVersion {
    /// A version as the listing spelled it: `"null"` (or an empty id) names no real version.
    ///
    /// # Errors
    /// Returns an error for a real id that is longer than 1024 bytes or contains NUL.
    pub(crate) fn from_listing(
        reported_id: &str,
        latest: bool,
        delete_marker: bool,
    ) -> Result<Self, ModelValueError> {
        let id = (!reported_id.is_empty() && reported_id != NULL_VERSION)
            .then(|| reported_id.to_string());
        Self::new(id, latest, delete_marker)
    }

    pub(crate) fn new(
        id: Option<String>,
        latest: bool,
        delete_marker: bool,
    ) -> Result<Self, ModelValueError> {
        if let Some(id) = &id {
            SourceVersion::Id(id.clone()).validate()?;
            if id == NULL_VERSION {
                return Err(ModelValueError::new(
                    "version",
                    "the null version carries no version id",
                ));
            }
        }
        Ok(Self {
            id,
            latest,
            delete_marker,
        })
    }

    /// The real version id; `None` for the `"null"` version.
    #[must_use]
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// Whether this is the key's current entry (a delete marker can be).
    #[must_use]
    pub const fn is_latest(&self) -> bool {
        self.latest
    }

    /// Whether this entry is a delete marker: it has no content and cannot be copied.
    #[must_use]
    pub const fn is_delete_marker(&self) -> bool {
        self.delete_marker
    }

    /// The selector that copies exactly this version with `TransferRequest::with_source_version`.
    ///
    /// - a real version: `Id(id)`;
    /// - the `"null"` version while it is the latest: `Current` (it is what `Current` copies, and
    ///   an unversioned bucket names no other);
    /// - the `"null"` version after newer versions were written: `Id("null")`, because `Current`
    ///   would copy a newer version;
    /// - a delete marker: `None`.
    #[must_use]
    pub fn source_version(&self) -> Option<SourceVersion> {
        if self.delete_marker {
            return None;
        }
        Some(match (&self.id, self.latest) {
            (Some(id), _) => SourceVersion::Id(id.clone()),
            (None, true) => SourceVersion::Current,
            (None, false) => SourceVersion::Id(NULL_VERSION.to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{EntryVersion, SourceVersion};

    fn listed(id: &str, latest: bool, marker: bool) -> EntryVersion {
        EntryVersion::from_listing(id, latest, marker).unwrap_or_else(|error| panic!("{error}"))
    }

    #[test]
    fn a_real_version_is_selected_by_its_id() {
        let version = listed("v1", false, false);
        assert_eq!(version.id(), Some("v1"));
        assert_eq!(
            version.source_version(),
            Some(SourceVersion::Id("v1".into()))
        );
        assert_eq!(
            listed("v3", true, false).source_version(),
            Some(SourceVersion::Id("v3".into()))
        );
    }

    #[test]
    fn the_null_version_names_no_id_and_is_current_only_while_latest() {
        let latest = listed("null", true, false);
        assert_eq!(latest.id(), None);
        assert_eq!(latest.source_version(), Some(SourceVersion::Current));
        let older = listed("null", false, false);
        assert_eq!(older.id(), None);
        assert_eq!(
            older.source_version(),
            Some(SourceVersion::Id("null".into()))
        );
    }

    #[test]
    fn a_delete_marker_cannot_be_copied() {
        let marker = listed("m1", true, true);
        assert!(marker.is_delete_marker() && marker.is_latest());
        assert_eq!(marker.source_version(), None);
        assert_eq!(listed("null", false, true).source_version(), None);
    }

    #[test]
    fn malformed_ids_are_refused() {
        assert!(EntryVersion::from_listing(&"x".repeat(1025), true, false).is_err());
        assert!(EntryVersion::from_listing("a\0b", true, false).is_err());
        assert!(EntryVersion::new(Some("null".into()), true, false).is_err());
    }
}
