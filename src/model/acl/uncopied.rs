//! Lossless source entry details for ACL meanings that were not copied.

use super::{
    AclCopyIssue, AclCopyIssueKind, AclEncoding, AclError, AclIssueLocation, AclMetadata,
    PosixEntry, decode_nfs4, decode_posix,
};

/// Original native fields, without translating permissions or principal semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UncopiedAclEntry {
    NfsAce {
        ace_type: u8,
        flags: u32,
        permissions: u32,
    },
    PosixEntry {
        tag: u32,
        id: u32,
        permissions: u32,
    },
    /// The entire payload was omitted; this model does not decode individual entries.
    OpaqueAcl {
        encoding: AclEncoding,
    },
}

/// Describe every original entry omitted when replacing an ACL with a conservative fallback.
///
/// Original order, DENY entries, zero permissions, and POSIX default entries are retained.
/// Windows descriptors receive one whole-ACL issue because this model does not decode ACEs.
/// When POSIX access entries are absent, `source_mode` records the source's mode-derived
/// access. Passing `None` explicitly reports that its mode was not observed.
///
/// # Errors
/// Malformed POSIX and NFS observations remain decoding errors.
pub fn uncopied_acl_entries(
    source: &AclMetadata,
    source_mode: Option<u32>,
) -> Result<Vec<AclCopyIssue>, AclError> {
    match source.encoding() {
        AclEncoding::NfsV4 => Ok(decode_nfs4(source)?
            .into_iter()
            .enumerate()
            .map(|(index, ace)| AclCopyIssue {
                location: AclIssueLocation::NfsAce { index },
                principal: Some(ace.who),
                kind: AclCopyIssueKind::UncopiedEntry {
                    entry: UncopiedAclEntry::NfsAce {
                        ace_type: ace.ace_type,
                        flags: ace.flags,
                        permissions: ace.permissions,
                    },
                },
            })
            .collect()),
        AclEncoding::Posix => {
            let acl = decode_posix(source)?;
            let mode_issue = acl.access.is_empty().then_some(AclCopyIssue {
                location: AclIssueLocation::Acl,
                principal: None,
                kind: AclCopyIssueKind::UncopiedMode { mode: source_mode },
            });
            Ok(mode_issue
                .into_iter()
                .chain(acl.access.into_iter().enumerate().map(|(index, entry)| {
                    posix_issue(entry, AclIssueLocation::PosixAccess { index })
                }))
                .chain(acl.default.into_iter().enumerate().map(|(index, entry)| {
                    posix_issue(entry, AclIssueLocation::PosixDefault { index })
                }))
                .collect())
        }
        encoding @ AclEncoding::WindowsSecurityDescriptor => Ok(vec![AclCopyIssue {
            location: AclIssueLocation::Acl,
            principal: None,
            kind: AclCopyIssueKind::UncopiedEntry {
                entry: UncopiedAclEntry::OpaqueAcl { encoding },
            },
        }]),
    }
}

pub(super) fn posix_issue(entry: PosixEntry, location: AclIssueLocation) -> AclCopyIssue {
    AclCopyIssue {
        location,
        principal: matches!(entry.tag, super::codec::USER | super::codec::GROUP)
            .then(|| entry.id.to_string()),
        kind: AclCopyIssueKind::UncopiedEntry {
            entry: UncopiedAclEntry::PosixEntry {
                tag: entry.tag,
                id: entry.id,
                permissions: entry.permissions,
            },
        },
    }
}

pub(super) fn masked_permissions(source: &AclMetadata) -> Result<Vec<AclCopyIssue>, AclError> {
    use super::codec::{GROUP, GROUP_OBJ, MASK, USER};
    let acl = decode_posix(source)?;
    let mask = super::mapping::permission(&acl.access, MASK).unwrap_or(7);
    Ok(acl
        .access
        .iter()
        .enumerate()
        .filter(|(_, entry)| {
            matches!(entry.tag, USER | GROUP | GROUP_OBJ) && entry.permissions & !mask != 0
        })
        .map(|(index, entry)| AclCopyIssue {
            location: AclIssueLocation::PosixAccess { index },
            principal: matches!(entry.tag, USER | GROUP).then(|| entry.id.to_string()),
            kind: AclCopyIssueKind::MaskedPermissionsDropped {
                tag: entry.tag,
                original: entry.permissions,
                mask,
                omitted: entry.permissions & !mask,
            },
        })
        .collect())
}

#[cfg(test)]
#[path = "uncopied_tests.rs"]
mod tests;
