//! Optional cross-scan rename evidence, separate from runtime source identity.
//!
//! A consumer must keep the context once per scan, require compatible opted-in contexts, and
//! accept only a unique one-to-one candidate of the same entry kind. Hard-link ambiguity or
//! duplicate candidates require path fallback. Content comparison is independent: a renamed
//! entry can also have changed. Heuristic evidence is never proof of object identity.

use std::fmt;

use blake3::Hasher;

use super::{MAX_MODEL_FIELD_BYTES, ModelValueError};

/// The interpretation and encoding version of rename candidate bytes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RenameScheme {
    /// Big-endian inode u64 followed by signed Unix birth-time nanoseconds i64.
    LocalInodeBirthTimeV1,
}

impl RenameScheme {
    pub(super) const fn tag(self) -> u8 {
        match self {
            Self::LocalInodeBirthTimeV1 => 1,
        }
    }

    pub(super) const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::LocalInodeBirthTimeV1),
            _ => None,
        }
    }
}

/// The reliability a candidate provider can promise.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenameQuality {
    /// Useful evidence, but inode reuse or mutable creation time can cause collisions.
    Heuristic,
}

/// An inline candidate token; its scope and policy belong to the scan's [`RenameContext`].
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct RenameEvidence {
    scheme: RenameScheme,
    bytes: [u8; 16],
}

impl RenameEvidence {
    /// Encodes an observed inode and a checked, non-saturating birth timestamp.
    ///
    /// Missing or unrepresentable birth times must produce no evidence at the adapter boundary.
    /// Epoch zero and negative timestamps are valid. This performs no heap allocation.
    #[must_use]
    pub fn local_inode_birth_time(inode: u64, birth_time_nanos: i64) -> Self {
        let mut evidence = Self {
            scheme: RenameScheme::LocalInodeBirthTimeV1,
            bytes: [0; 16],
        };
        evidence.bytes[..8].copy_from_slice(&inode.to_be_bytes());
        evidence.bytes[8..].copy_from_slice(&birth_time_nanos.to_be_bytes());
        evidence
    }

    /// Borrows the fixed token without allocating or copying it.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.bytes
    }

    /// Returns the candidate encoding version.
    #[must_use]
    pub const fn scheme(&self) -> RenameScheme {
        self.scheme
    }

    /// Returns the provider's reliability classification.
    #[must_use]
    pub const fn quality(&self) -> RenameQuality {
        RenameQuality::Heuristic
    }

    pub(super) const fn from_encoded(scheme: RenameScheme, bytes: [u8; 16]) -> Self {
        Self { scheme, bytes }
    }
}

impl fmt::Debug for RenameEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RenameEvidence")
            .field("scheme", &self.scheme)
            .field("quality", &self.quality())
            .field("bytes", &"<redacted>")
            .finish()
    }
}

/// A stable logical dataset identifier, hashed once when the scan context is configured.
///
/// The caller must use the same identifier only for the same filesystem/share dataset, even
/// across remounts. A mount path alone is insufficient: its source can be replaced. Multiple
/// filesystems with overlapping inode spaces must use distinct scopes or disable matching.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct RenameScope([u8; 32]);

impl RenameScope {
    /// Validates and hashes a caller-managed dataset identifier, without retaining its text.
    ///
    /// # Errors
    /// Returns an error for blank, NUL-containing, or oversized identifiers.
    pub fn new(dataset_id: impl AsRef<str>) -> Result<Self, ModelValueError> {
        let dataset_id = dataset_id.as_ref();
        if dataset_id.trim().is_empty()
            || dataset_id.contains('\0')
            || dataset_id.len() > MAX_MODEL_FIELD_BYTES
        {
            return Err(ModelValueError::new(
                "rename_scope",
                "dataset identifier must be non-blank, NUL-free, and bounded",
            ));
        }
        Ok(Self(blake3::derive_key(
            "data-mover/rename-scope/v1",
            dataset_id.as_bytes(),
        )))
    }

    /// Borrows the opaque scope fingerprint.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for RenameScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RenameScope(<opaque-32-bytes>)")
    }
}

/// Whether a consumer may use heuristic evidence to match scans.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RenamePolicy {
    /// Expose candidates for inspection, without authorizing matching.
    #[default]
    CandidatesOnly,
    /// Permit heuristic matching in a dataset whose identity behavior has been verified.
    AllowHeuristic,
}

/// Shared scan-level context; never repeated inside an entry snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RenameContext {
    scope: RenameScope,
    policy: RenamePolicy,
}

impl RenameContext {
    /// Configures one dataset's candidate policy.
    #[must_use]
    pub const fn new(scope: RenameScope, policy: RenamePolicy) -> Self {
        Self { scope, policy }
    }

    /// Returns the stable dataset scope.
    #[must_use]
    pub const fn scope(&self) -> &RenameScope {
        &self.scope
    }

    /// Returns whether heuristic matching was explicitly enabled.
    #[must_use]
    pub const fn policy(&self) -> RenamePolicy {
        self.policy
    }

    /// Returns the scheme this context interprets.
    #[must_use]
    pub const fn scheme(&self) -> RenameScheme {
        RenameScheme::LocalInodeBirthTimeV1
    }

    /// Tests the dataset and scheme independently of matching authorization.
    #[must_use]
    pub fn contexts_compatible(&self, other: &Self) -> bool {
        self.scope == other.scope && self.scheme() == other.scheme()
    }

    /// Whether these two scans authorize heuristic matching of compatible candidates.
    ///
    /// Consumers still must enforce unique one-to-one matching, kind equality, hard-link
    /// ambiguity rejection, and independent content comparison.
    #[must_use]
    pub fn permits_matching(&self, other: &Self) -> bool {
        self.contexts_compatible(other)
            && self.policy == RenamePolicy::AllowHeuristic
            && other.policy == RenamePolicy::AllowHeuristic
    }

    /// Hashes scope, scheme, and policy for a scan/resume fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> [u8; 32] {
        let mut hasher = Hasher::new();
        hasher.update(b"data-mover/rename-context/v1\0");
        hasher.update(self.scope.as_bytes());
        hasher.update(&[self.scheme().tag()]);
        hasher.update(&[u8::from(self.policy == RenamePolicy::AllowHeuristic)]);
        *hasher.finalize().as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evidence_encodes_exact_signed_time_without_allocation() {
        for nanos in [0, -1, i64::MIN, i64::MAX] {
            let evidence = RenameEvidence::local_inode_birth_time(123, nanos);
            assert_eq!(&evidence.as_bytes()[..8], &123_u64.to_be_bytes());
            assert_eq!(&evidence.as_bytes()[8..], &nanos.to_be_bytes());
            assert_eq!(evidence.scheme(), RenameScheme::LocalInodeBirthTimeV1);
            assert_eq!(evidence.quality(), RenameQuality::Heuristic);
        }
    }

    #[test]
    fn matching_requires_the_same_scope_and_both_scan_policies() {
        let scope = RenameScope::new("dataset-1").unwrap_or_else(|error| panic!("{error}"));
        let other = RenameScope::new("dataset-2").unwrap_or_else(|error| panic!("{error}"));
        let enabled = RenameContext::new(scope, RenamePolicy::AllowHeuristic);
        let candidates = RenameContext::new(scope, RenamePolicy::default());
        let unrelated = RenameContext::new(other, RenamePolicy::AllowHeuristic);
        assert!(enabled.permits_matching(&enabled));
        assert!(enabled.contexts_compatible(&candidates));
        assert!(!enabled.permits_matching(&candidates));
        assert!(!candidates.permits_matching(&enabled));
        assert!(!enabled.permits_matching(&unrelated));
        assert_ne!(enabled.fingerprint(), candidates.fingerprint());
        assert_ne!(enabled.fingerprint(), unrelated.fingerprint());
        assert_eq!(enabled.fingerprint(), enabled.fingerprint());
    }

    #[test]
    fn scope_validation_does_not_store_or_expose_the_identifier() {
        for invalid in ["", "  ", "a\0b"] {
            assert!(RenameScope::new(invalid).is_err());
        }
        assert!(RenameScope::new("x".repeat(MAX_MODEL_FIELD_BYTES + 1)).is_err());
        let scope = RenameScope::new("private-dataset").unwrap_or_else(|error| panic!("{error}"));
        assert!(!format!("{scope:?}").contains("private-dataset"));
    }
}
