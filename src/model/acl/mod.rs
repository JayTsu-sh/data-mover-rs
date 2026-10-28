//! Native ACL preservation and a deliberately restricted POSIX / `NFSv4` mapping.
//!
//! Cross-model operations cover current regular-file read/write/execute access only.
//! Callers must establish a shared numeric identity namespace and equal group resolution,
//! and must gate out directories. Native copying retains each model's full payload.
//! The Linux access profile also excludes extended zero-mask ACLs with other
//! grants: Linux can bypass their named restrictions when the mode group class is zero.
mod codec;
mod mapping;
mod semantics;

pub use codec::{
    NfsAce, PosixAcl, PosixEntry, decode_nfs4, decode_posix, encode_nfs4, encode_posix,
};

use super::{AclEncoding, AclMetadata, OwnershipMode};

/// A malformed observation or an ACL outside the declared mapping contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct AclError(pub(crate) &'static str);

impl AclError {
    /// The reason the ACL observation or requested mapping was rejected.
    #[must_use]
    pub const fn reason(self) -> &'static str {
        self.0
    }
}

/// Preserve a native ACL or produce a proven candidate in the other model.
///
/// # Errors
/// Rejects malformed ACLs and cross-model inputs outside the numeric, regular-file
/// access profile. A successful result does not certify server identity resolution.
pub fn map_acl(
    source: &AclMetadata,
    target: AclEncoding,
    owner: OwnershipMode,
) -> Result<AclMetadata, AclError> {
    mapping::map(source, target, owner)
}

/// Compare native configurations or explicitly qualified current access semantics.
///
/// The `equivalent` flag asserts the numeric identity and regular-file contract
/// for cross-model comparison. Cross-model owner/group equality is also checked here.
///
/// # Errors
/// Rejects incomplete, malformed, or unqualified observations.
pub fn compare_acl(
    source: &AclMetadata,
    destination: &AclMetadata,
    source_owner: OwnershipMode,
    destination_owner: OwnershipMode,
    equivalent: bool,
) -> Result<bool, AclError> {
    if source.encoding() == destination.encoding() {
        return mapping::compare_native(
            source,
            destination,
            source_owner,
            destination_owner,
            equivalent,
        );
    }
    if !equivalent {
        return Err(AclError(
            "cross-model ACL comparison requires explicit equivalence",
        ));
    }
    if source_owner.uid != destination_owner.uid || source_owner.gid != destination_owner.gid {
        return Err(AclError(
            "cross-model ACL owners require the same numeric identities",
        ));
    }
    mapping::compare_cross(source, destination, source_owner, destination_owner)
}

/// Compare typed native fields with protocol-defined normalization only.
///
/// POSIX entry order and a redundant basic NFSACL mask are normalized; raw named
/// permissions and masks remain significant. `NFSv4` ACE order is never changed.
/// Only the identifier-group flag on special principals is normalized.
///
/// # Errors
/// Rejects malformed native payloads or different encodings.
pub fn equivalent_native(
    source: &AclMetadata,
    destination: &AclMetadata,
) -> Result<bool, AclError> {
    mapping::equivalent_native(source, destination)
}

#[cfg(test)]
mod tests;
