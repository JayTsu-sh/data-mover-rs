#![allow(clippy::unwrap_used)]

use super::super::tests::{OWNER, fixture};
use super::super::{NfsAce, encode_nfs4, encode_posix, map_acl, project_acl};
use super::*;

#[test]
fn blocked_nfs_projection_lists_every_original_entry_including_denies_and_zero_masks() {
    let source = map_acl(&fixture(), AclEncoding::NfsV4, OWNER).unwrap();
    let mut aces = decode_nfs4(&source).unwrap();
    aces.insert(
        0,
        NfsAce {
            ace_type: 1,
            flags: 0x100,
            permissions: 0x27,
            who: "denied@example".into(),
        },
    );
    aces.push(NfsAce {
        ace_type: 0,
        flags: 0x40,
        permissions: 0,
        who: "zero@example".into(),
    });
    let source = encode_nfs4(&aces).unwrap();
    assert!(
        project_acl(&source, AclEncoding::Posix, OWNER)
            .unwrap()
            .candidate
            .is_none()
    );
    let issues = uncopied_acl_entries(&source, None).unwrap();
    assert_eq!(issues.len(), aces.len());
    for (index, (issue, ace)) in issues.iter().zip(&aces).enumerate() {
        assert_eq!(issue.location, AclIssueLocation::NfsAce { index });
        assert_eq!(issue.principal.as_deref(), Some(ace.who.as_str()));
        assert_eq!(
            issue.kind,
            AclCopyIssueKind::UncopiedEntry {
                entry: UncopiedAclEntry::NfsAce {
                    ace_type: ace.ace_type,
                    flags: ace.flags,
                    permissions: ace.permissions,
                },
            }
        );
    }
}

#[test]
fn posix_fallback_retains_raw_access_default_masks_and_named_identifiers() {
    let mut posix = decode_posix(&fixture()).unwrap();
    posix.default = posix.access.clone();
    let issues = uncopied_acl_entries(&encode_posix(&posix).unwrap(), Some(OWNER.mode)).unwrap();
    assert_eq!(issues.len(), posix.access.len() + posix.default.len());
    for (default, entries) in [(false, &posix.access), (true, &posix.default)] {
        for (index, entry) in entries.iter().enumerate() {
            let location = if default {
                AclIssueLocation::PosixDefault { index }
            } else {
                AclIssueLocation::PosixAccess { index }
            };
            assert!(issues.iter().any(|issue| issue.location == location
                && issue.kind
                    == AclCopyIssueKind::UncopiedEntry {
                        entry: UncopiedAclEntry::PosixEntry {
                            tag: entry.tag,
                            id: entry.id,
                            permissions: entry.permissions,
                        },
                    }));
        }
    }
    assert_eq!(issues[1].principal.as_deref(), Some("42421"));
    assert_eq!(issues[0].principal, None);
}

#[test]
fn windows_whole_acl_omission_does_not_invent_decoded_entries() {
    let source = super::super::safe_deny_all_acl(AclEncoding::WindowsSecurityDescriptor).unwrap();
    let issues = uncopied_acl_entries(&source, None).unwrap();
    assert_eq!(
        issues,
        vec![AclCopyIssue {
            location: AclIssueLocation::Acl,
            principal: None,
            kind: AclCopyIssueKind::UncopiedEntry {
                entry: UncopiedAclEntry::OpaqueAcl {
                    encoding: AclEncoding::WindowsSecurityDescriptor,
                },
            },
        }]
    );
    assert!(issues[0].kind.reason().contains("not decoded"));
}

#[test]
fn malformed_sources_remain_errors_when_building_omission_reports() {
    for encoding in [AclEncoding::Posix, AclEncoding::NfsV4] {
        let source = AclMetadata::new(encoding, vec![1, 0]).unwrap();
        assert!(uncopied_acl_entries(&source, None).is_err());
    }
}

#[test]
fn absent_posix_access_reports_source_mode_and_never_invents_an_unobserved_mode() {
    let source = AclMetadata::new_posix(None, None).unwrap();
    for mode in [Some(0o4751), None] {
        let issues = uncopied_acl_entries(&source, mode).unwrap();
        assert_eq!(
            issues,
            vec![AclCopyIssue {
                location: AclIssueLocation::Acl,
                principal: None,
                kind: AclCopyIssueKind::UncopiedMode { mode },
            }]
        );
        if mode.is_none() {
            assert!(issues[0].kind.reason().contains("not observed"));
        }
    }
}
