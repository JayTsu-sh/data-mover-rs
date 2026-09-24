//! What the ordinary copy's metadata negotiation asks of the storage roles: the capabilities a
//! destination declares, and the identity-bound observation a source returns.

use crate::model::{AclEncoding, MetadataObservations, TimePrecision};

/// Destination capabilities used by the copied metadata families.
///
/// `timestamps` and `ownership` describe the baseline every copy carries; a caller cannot turn
/// them off. `acl` and `xattrs` describe families a caller has to ask for — what a
/// destination reports here is only its ability to accept them, never a decision to copy them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CopiedMetadataTarget {
    /// Whether the modification time is stored, and how finely.
    pub timestamps: CopiedTimestampTarget,
    pub ownership: CopiedOwnershipTarget,
    /// The ACL encoding this destination writes, if it writes one at all.
    pub acl: CopiedAclTarget,
    /// Whether this destination stores extended attributes.
    pub xattrs: CopiedValueTarget,
}

impl CopiedMetadataTarget {
    /// Stores none of what a copy can carry, so no source value could change what the copy does:
    /// every family is skipped because of the destination, and the source need not be read.
    #[must_use]
    pub const fn stores_nothing(&self) -> bool {
        matches!(self.timestamps, CopiedTimestampTarget::NotStored)
            && matches!(self.ownership, CopiedOwnershipTarget::Unsupported)
            && matches!(self.acl, CopiedAclTarget::Unsupported)
            && matches!(self.xattrs, CopiedValueTarget::Unsupported)
    }
}

/// Whether a destination stores a copied file's modification time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopiedTimestampTarget {
    /// Stored, at this precision; a finer source time is quantized and the loss reported.
    Stored(TimePrecision),
    /// Not stored at all — an object store sets its own time when the object is written.
    NotStored,
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
    /// Why owner and group are missing when `mode_without_ownership` is set: the source named them
    /// and the names could not be mapped to ids (an `NFSv4` owner nfs-rs cannot parse), rather
    /// than a source that has no numeric owner at all (HDFS). Reported as its own loss.
    pub owner_names_unmapped: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    const OBJECT_STORE: CopiedMetadataTarget = CopiedMetadataTarget {
        timestamps: CopiedTimestampTarget::NotStored,
        ownership: CopiedOwnershipTarget::Unsupported,
        acl: CopiedAclTarget::Unsupported,
        xattrs: CopiedValueTarget::Unsupported,
    };

    /// Anything the destination does store means the source has to be read.
    #[test]
    fn storing_nothing_means_every_family_is_unsupported() {
        assert!(OBJECT_STORE.stores_nothing());
        for stores_something in [
            CopiedMetadataTarget {
                timestamps: CopiedTimestampTarget::Stored(TimePrecision::Seconds),
                ..OBJECT_STORE
            },
            CopiedMetadataTarget {
                ownership: CopiedOwnershipTarget::ModeOnly,
                ..OBJECT_STORE
            },
            CopiedMetadataTarget {
                acl: CopiedAclTarget::Encoding(AclEncoding::Posix),
                ..OBJECT_STORE
            },
            CopiedMetadataTarget {
                xattrs: CopiedValueTarget::Supported,
                ..OBJECT_STORE
            },
        ] {
            assert!(!stores_something.stores_nothing(), "{stores_something:?}");
        }
    }
}
