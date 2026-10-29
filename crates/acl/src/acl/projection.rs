//! Explicit losses when projecting a native ACL onto current regular-file data access.

use super::{AclEncoding, AclError, AclMetadata, OwnershipMode, map_acl};
use super::{decode_nfs4, decode_posix};

/// A native candidate, or a cross-model candidate proven under the declared identity scope.
/// Issues identify omitted meanings; `None` means this mapper could not prove a candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AclProjection {
    pub candidate: Option<AclMetadata>,
    pub issues: Vec<AclCopyIssue>,
}

/// The original source location, before any entries were removed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AclIssueLocation {
    Acl,
    PosixAccess { index: usize },
    PosixDefault { index: usize },
    NfsAce { index: usize },
}

/// One source meaning omitted by a partial ACL projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AclCopyIssue {
    pub location: AclIssueLocation,
    /// The original principal spelling for an NFS ACE; POSIX base entries have none.
    pub principal: Option<String>,
    pub kind: AclCopyIssueKind,
}

/// Machine-readable projection losses and reasons a candidate was refused.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AclCopyIssueKind {
    DefaultInheritance,
    MaskedPermissionsDropped {
        tag: u32,
        original: u32,
        mask: u32,
        omitted: u32,
    },
    UncopiedMode {
        mode: Option<u32>,
    },
    UncopiedEntry {
        entry: super::UncopiedAclEntry,
    },
    UnsupportedRights {
        rights: u32,
    },
    UnsupportedFlags {
        flags: u32,
    },
    UnsupportedAceType {
        ace_type: u8,
    },
    UnmappedPrincipal {
        principal: String,
    },
    PartialWriteRights {
        rights: u32,
    },
    UnprovenAccess {
        reason: &'static str,
    },
    MetadataRightsNormalized {
        original: u32,
        normalized: u32,
    },
    UnsupportedEncoding,
    OwnershipNotPreserved,
    NoSafeCandidate,
    SourceUnavailable,
    IdentityResolutionUnverified,
}

impl AclCopyIssueKind {
    /// A stable explanation; typed fields carry the original rights, flags, and identities.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::DefaultInheritance => "POSIX default ACL inheritance is not preserved",
            Self::MaskedPermissionsDropped { .. } => {
                "POSIX permissions hidden by the access mask are not preserved as native configuration"
            }
            Self::UncopiedMode { mode: Some(_) } => {
                "source mode-derived POSIX access was not copied"
            }
            Self::UncopiedMode { mode: None } => {
                "source mode-derived POSIX access was not copied; source mode was not observed"
            }
            Self::UncopiedEntry {
                entry: super::UncopiedAclEntry::OpaqueAcl { .. },
            } => "source ACL was not copied; individual native entries were not decoded",
            Self::UncopiedEntry { .. } => "source ACL entry was not copied",
            Self::UnsupportedRights { .. } => "NFSv4 rights are not preserved",
            Self::UnsupportedFlags { .. } => "NFSv4 ACE flags are not preserved",
            Self::UnsupportedAceType { .. } => "NFSv4 ACE type is not preserved",
            Self::UnmappedPrincipal { .. } => "ACL principal has no unambiguous numeric identity",
            Self::PartialWriteRights { .. } => {
                "separate append and overwrite rights cannot map to POSIX"
            }
            Self::UnprovenAccess { reason } => reason,
            Self::MetadataRightsNormalized { .. } => {
                "non-data metadata authorization uses the target template"
            }
            Self::UnsupportedEncoding => "ACL encoding cannot represent this projection",
            Self::OwnershipNotPreserved => "ACL owner or group identity is not preserved",
            Self::IdentityResolutionUnverified => {
                "server identity namespaces and group resolution have not been verified"
            }
            Self::SourceUnavailable => "source ACL was not observed or is unsupported",
            Self::NoSafeCandidate => "no ACL candidate preserves current data authorization",
        }
    }
}

/// Project onto current regular-file read/write/execute authorization.
///
/// Cross-model equivalence is conditional on equal numeric identities and group resolution.
/// Every cross-model result reports that these server conditions remain unverified here.
/// Same-model candidates preserve native configuration and do not certify owner identity.
///
/// # Errors
/// Malformed native observations remain errors rather than partial-copy reports.
pub fn project_acl(
    source: &AclMetadata,
    target: AclEncoding,
    owner: OwnershipMode,
) -> Result<AclProjection, AclError> {
    validate(source)?;
    if source.encoding() == target {
        return Ok(AclProjection {
            candidate: Some(source.clone()),
            issues: Vec::new(),
        });
    }
    let mut projection = match map_acl(source, target, owner) {
        Ok(candidate) => AclProjection {
            candidate: Some(candidate),
            issues: Vec::new(),
        },
        Err(_) => match (source.encoding(), target) {
            (AclEncoding::Posix, AclEncoding::NfsV4) => project_posix(source, target, owner)?,
            (AclEncoding::NfsV4, AclEncoding::Posix) => project_nfs(source, target, owner)?,
            _ => AclProjection {
                candidate: None,
                issues: vec![
                    whole(AclCopyIssueKind::UnsupportedEncoding),
                    whole(AclCopyIssueKind::NoSafeCandidate),
                ],
            },
        },
    };
    if source.encoding() == AclEncoding::Posix
        && target == AclEncoding::NfsV4
        && projection.candidate.is_some()
    {
        projection
            .issues
            .extend(super::uncopied::masked_permissions(source)?);
    }
    projection
        .issues
        .insert(0, whole(AclCopyIssueKind::IdentityResolutionUnverified));
    Ok(projection)
}

fn project_posix(
    source: &AclMetadata,
    target: AclEncoding,
    owner: OwnershipMode,
) -> Result<AclProjection, AclError> {
    let mut posix = decode_posix(source)?;
    let issues = posix
        .default
        .iter()
        .enumerate()
        .flat_map(|(index, entry)| {
            let omitted =
                super::uncopied::posix_issue(*entry, AclIssueLocation::PosixDefault { index });
            [
                AclCopyIssue {
                    kind: AclCopyIssueKind::DefaultInheritance,
                    ..omitted.clone()
                },
                omitted,
            ]
        })
        .collect();
    posix.default.clear();
    Ok(mapped_projection(
        &super::encode_posix(&posix)?,
        target,
        owner,
        issues,
    ))
}

fn project_nfs(
    source: &AclMetadata,
    target: AclEncoding,
    owner: OwnershipMode,
) -> Result<AclProjection, AclError> {
    let (aces, mut issues, blocked) = super::projection_nfs::sanitize(&decode_nfs4(source)?);
    if blocked {
        issues.push(whole(AclCopyIssueKind::NoSafeCandidate));
        return Ok(AclProjection {
            candidate: None,
            issues,
        });
    }
    Ok(mapped_projection(
        &super::encode_nfs4(&aces)?,
        target,
        owner,
        issues,
    ))
}

fn mapped_projection(
    sanitized: &AclMetadata,
    target: AclEncoding,
    owner: OwnershipMode,
    mut issues: Vec<AclCopyIssue>,
) -> AclProjection {
    let candidate = match map_acl(sanitized, target, owner) {
        Ok(candidate) => Some(candidate),
        Err(error) => {
            issues.push(whole(AclCopyIssueKind::UnprovenAccess {
                reason: error.reason(),
            }));
            issues.push(whole(AclCopyIssueKind::NoSafeCandidate));
            None
        }
    };
    AclProjection { candidate, issues }
}

fn whole(kind: AclCopyIssueKind) -> AclCopyIssue {
    AclCopyIssue {
        location: AclIssueLocation::Acl,
        principal: None,
        kind,
    }
}

/// An explicit replacement ACL denying regular-file access, independent of inherited entries.
///
/// # Errors
/// Returns an error if constructing the bounded native ACL payload fails.
pub fn safe_deny_all_acl(target: AclEncoding) -> Result<AclMetadata, AclError> {
    use super::codec::{GROUP_OBJ, OTHER, UNDEFINED, USER_OBJ};
    match target {
        AclEncoding::Posix => super::encode_posix(&super::PosixAcl {
            access: [USER_OBJ, GROUP_OBJ, OTHER]
                .map(|tag| super::PosixEntry {
                    tag,
                    id: UNDEFINED,
                    permissions: 0,
                })
                .into(),
            default: Vec::new(),
        }),
        AclEncoding::NfsV4 => super::encode_nfs4(&[super::NfsAce {
            ace_type: 1,
            flags: 0,
            // DELETE_CHILD (0x40) is directory-only and is removed by regular-file servers.
            permissions: 0x1f_01bf,
            who: "EVERYONE@".into(),
        }]),
        AclEncoding::WindowsSecurityDescriptor => {
            // SECURITY_DESCRIPTOR_RELATIVE: protected, present DACL at offset 20; an empty
            // eight-byte ACL denies access. A NULL DACL would grant everyone access.
            let mut descriptor = vec![0u8; 28];
            descriptor[0] = 1;
            descriptor[2..4].copy_from_slice(&0x9004_u16.to_le_bytes());
            descriptor[16..20].copy_from_slice(&20_u32.to_le_bytes());
            descriptor[20] = 2;
            descriptor[22..24].copy_from_slice(&8_u16.to_le_bytes());
            AclMetadata::new(AclEncoding::WindowsSecurityDescriptor, descriptor)
                .map_err(|_| AclError("cannot construct data-denying Windows ACL"))
        }
    }
}

fn validate(value: &AclMetadata) -> Result<(), AclError> {
    match value.encoding() {
        AclEncoding::Posix => {
            decode_posix(value)?;
        }
        AclEncoding::NfsV4 => {
            decode_nfs4(value)?;
        }
        AclEncoding::WindowsSecurityDescriptor => {}
    }
    Ok(())
}

#[cfg(test)]
#[path = "projection_tests.rs"]
mod tests;
