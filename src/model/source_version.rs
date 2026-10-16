//! Which version of a source object a transfer copies (ADR-0006 "Source version selector").

use super::ModelValueError;

/// Longest version id accepted, matching the bound on a transfer label.
const MAX_VERSION_ID_BYTES: usize = 1024;

/// Which version of the source a transfer copies.
///
/// Only a versioned object store (S3) can select a version; every other source accepts `Current`
/// only and refuses `Id` before anything is written at the destination.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub enum SourceVersion {
    /// Whatever is current when the source is described. A versioned store pins that version, so
    /// every later read, the metadata observation and a native copy use it even if a newer one
    /// appears meanwhile.
    #[default]
    Current,
    /// One stored version, named by the store's version id, copied as it is — also when it is not
    /// the current one.
    Id(String),
}

impl SourceVersion {
    /// The selected version id, if one is named.
    #[must_use]
    pub fn version_id(&self) -> Option<&str> {
        match self {
            Self::Current => None,
            Self::Id(version) => Some(version),
        }
    }

    /// Checks a named version id is non-empty, bounded and free of NUL.
    ///
    /// # Errors
    /// Returns an error describing the malformed version id.
    pub fn validate(&self) -> Result<(), ModelValueError> {
        match self.version_id() {
            Some(version)
                if version.is_empty()
                    || version.len() > MAX_VERSION_ID_BYTES
                    || version.contains('\0') =>
            {
                Err(ModelValueError::new(
                    "source_version",
                    "a version id must be non-empty, bounded and free of NUL",
                ))
            }
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SourceVersion;

    #[test]
    fn current_is_the_default_and_names_no_version() {
        assert_eq!(SourceVersion::default(), SourceVersion::Current);
        assert_eq!(SourceVersion::Current.version_id(), None);
        assert!(SourceVersion::Current.validate().is_ok());
    }

    #[test]
    fn a_named_version_is_validated() {
        assert_eq!(SourceVersion::Id("v1".into()).version_id(), Some("v1"));
        assert!(SourceVersion::Id("null".into()).validate().is_ok());
        assert!(SourceVersion::Id("x".repeat(1024)).validate().is_ok());
        for bad in [String::new(), "x".repeat(1025), "a\0b".to_string()] {
            assert!(SourceVersion::Id(bad).validate().is_err());
        }
    }
}
