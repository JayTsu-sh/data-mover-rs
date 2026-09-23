//! What the ordinary copy's metadata negotiation asks of the storage roles: the capabilities a
//! destination declares, and the identity-bound observation a source returns.

use crate::model::{AclEncoding, MetadataObservations, TimePrecision};

/// Destination capabilities used by the copied metadata families.
///
/// `timestamp_precision` and `ownership` describe the baseline every copy carries; a caller
/// cannot turn them off. `acl` and `xattrs` describe families a caller has to ask for — what a
/// destination reports here is only its ability to accept them, never a decision to copy them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CopiedMetadataTarget {
    pub timestamp_precision: TimePrecision,
    pub ownership: CopiedOwnershipTarget,
    /// The ACL encoding this destination writes, if it writes one at all.
    pub acl: CopiedAclTarget,
    /// Whether this destination stores extended attributes.
    pub xattrs: CopiedValueTarget,
}

/// ACL behavior used by the copied metadata families.
///
/// This mirrors `metadata::AclTarget` instead of reusing it because the layering runs the other
/// way: `metadata` is allowed to depend on `storage`, so `storage` cannot name it. The transfer
/// layer, which may see both, translates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopiedAclTarget {
    /// Writes an ACL in this encoding.
    Encoding(AclEncoding),
    /// Stores no ACL at all.
    Unsupported,
}

/// Whether a destination stores a copied value family, with the same layering caveat.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopiedValueTarget {
    Supported,
    Unsupported,
}

/// Ownership behavior used by the ordinary baseline metadata copy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopiedOwnershipTarget {
    /// Preserve numeric owner, group, and mode together.
    Numeric,
    /// Preserve mode while retaining destination-native owner and group.
    ModeOnly,
    /// No implicit numeric-to-native principal mapping.
    Unsupported,
}

/// Identity-bound observations for automatic baseline metadata copying.
/// `mode_without_ownership` carries permissions from a source whose owner/group
/// cannot be projected as numeric IDs. Copying it explicitly loses owner/group.
pub struct CopiedMetadataObservation {
    pub observations: MetadataObservations,
    pub mode_without_ownership: Option<u32>,
}
