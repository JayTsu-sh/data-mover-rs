#![allow(clippy::unwrap_used)]

use super::super::tests::{OWNER, fixture};
use super::super::{NfsAce, decode_nfs4, encode_nfs4, encode_posix};
use super::*;

#[test]
fn extra_metadata_rights_preserve_access_and_report_the_original_ace() {
    let canonical = map_acl(&fixture(), AclEncoding::NfsV4, OWNER).unwrap();
    let mut aces = decode_nfs4(&canonical).unwrap();
    let index = aces
        .iter()
        .position(|ace| ace.who == "42421" && ace.ace_type == 0)
        .unwrap();
    aces[index].permissions |= 0x8 | 0x4_0000;
    let source = encode_nfs4(&aces).unwrap();
    assert!(map_acl(&source, AclEncoding::Posix, OWNER).is_err());
    let projection = project_acl(&source, AclEncoding::Posix, OWNER).unwrap();
    let candidate = projection.candidate.unwrap();
    assert!(super::super::compare_acl(&canonical, &candidate, OWNER, OWNER, true).unwrap());
    assert!(projection.issues.contains(&AclCopyIssue {
        location: AclIssueLocation::NfsAce { index },
        principal: Some("42421".into()),
        kind: AclCopyIssueKind::UnsupportedRights {
            rights: 0x8 | 0x4_0000
        },
    }));
}

#[test]
fn posix_defaults_are_reported_individually_without_losing_access() {
    let original = fixture();
    let mut posix = decode_posix(&original).unwrap();
    posix.default = posix.access.clone();
    let source = encode_posix(&posix).unwrap();
    let projection = project_acl(&source, AclEncoding::NfsV4, OWNER).unwrap();
    assert_eq!(
        projection.candidate,
        Some(map_acl(&original, AclEncoding::NfsV4, OWNER).unwrap())
    );
    assert_eq!(
        projection
            .issues
            .iter()
            .filter(|issue| !matches!(
                issue.kind,
                AclCopyIssueKind::MaskedPermissionsDropped { .. }
            ))
            .count(),
        2 * posix.default.len() + 1
    );
    assert_eq!(
        projection.issues[0].kind,
        AclCopyIssueKind::IdentityResolutionUnverified
    );
    for (index, issue) in projection
        .issues
        .iter()
        .filter(|issue| matches!(issue.kind, AclCopyIssueKind::DefaultInheritance))
        .enumerate()
    {
        assert_eq!(issue.location, AclIssueLocation::PosixDefault { index });
        assert_eq!(issue.kind, AclCopyIssueKind::DefaultInheritance);
    }
}

#[test]
fn removing_metadata_aces_does_not_renumber_later_issues_or_remove_data_denies() {
    let mut posix = decode_posix(&fixture()).unwrap();
    posix
        .access
        .iter_mut()
        .find(|entry| entry.tag == super::super::codec::OTHER)
        .unwrap()
        .permissions = 7;
    let canonical = map_acl(&encode_posix(&posix).unwrap(), AclEncoding::NfsV4, OWNER).unwrap();
    let mut aces = decode_nfs4(&canonical).unwrap();
    aces.insert(
        0,
        NfsAce {
            ace_type: 2,
            flags: 0x10,
            permissions: 1,
            who: "audit@example".into(),
        },
    );
    aces.insert(
        1,
        NfsAce {
            ace_type: 0,
            flags: 0,
            permissions: 0x8,
            who: "metadata@example".into(),
        },
    );
    let deny = aces.iter().position(|ace| ace.ace_type == 1).unwrap();
    aces[deny].flags |= 0x81;
    let projection = project_acl(&encode_nfs4(&aces).unwrap(), AclEncoding::Posix, OWNER).unwrap();
    assert!(
        super::super::compare_acl(
            &canonical,
            &projection.candidate.unwrap(),
            OWNER,
            OWNER,
            true
        )
        .unwrap()
    );
    assert!(projection.issues.iter().any(|issue| issue.location
        == AclIssueLocation::NfsAce { index: deny }
        && issue.kind == AclCopyIssueKind::UnsupportedFlags { flags: 0x81 }));
}

#[test]
fn data_principals_unknown_flags_rights_and_partial_write_are_blockers() {
    let canonical = map_acl(&fixture(), AclEncoding::NfsV4, OWNER).unwrap();
    let original = decode_nfs4(&canonical).unwrap();
    for case in 0..4 {
        let mut aces = original.clone();
        let index = aces
            .iter()
            .position(|ace| ace.ace_type == 0 && ace.who == "42421")
            .unwrap();
        let expected = match case {
            0 => {
                aces[index].who = "alice@example".into();
                AclCopyIssueKind::UnmappedPrincipal {
                    principal: "alice@example".into(),
                }
            }
            1 => {
                aces[index].flags |= 0x100;
                AclCopyIssueKind::UnsupportedFlags { flags: 0x100 }
            }
            2 => {
                aces[index].permissions |= 0x200;
                AclCopyIssueKind::UnsupportedRights { rights: 0x200 }
            }
            _ => {
                aces[index].permissions = (aces[index].permissions & !6) | 2;
                AclCopyIssueKind::PartialWriteRights { rights: 2 }
            }
        };
        let result = project_acl(&encode_nfs4(&aces).unwrap(), AclEncoding::Posix, OWNER).unwrap();
        assert!(result.candidate.is_none());
        assert!(
            result
                .issues
                .iter()
                .any(|issue| issue.location == AclIssueLocation::NfsAce { index }
                    && issue.kind == expected)
        );
        assert!(
            result
                .issues
                .iter()
                .any(|issue| issue.kind == AclCopyIssueKind::NoSafeCandidate)
        );
    }
}

#[test]
fn current_data_deny_cannot_be_dropped_or_reordered_to_produce_a_candidate() {
    let canonical = map_acl(&fixture(), AclEncoding::NfsV4, OWNER).unwrap();
    let mut aces = decode_nfs4(&canonical).unwrap();
    aces.insert(
        0,
        NfsAce {
            ace_type: 1,
            flags: 0,
            permissions: 1,
            who: "EVERYONE@".into(),
        },
    );
    let source = encode_nfs4(&aces).unwrap();
    let result = project_acl(&source, AclEncoding::Posix, OWNER).unwrap();
    assert!(
        result.candidate.is_none(),
        "dropping the leading read deny would expand access"
    );
    assert!(
        result
            .issues
            .iter()
            .any(|issue| matches!(issue.kind, AclCopyIssueKind::UnprovenAccess { .. }))
    );
}

#[test]
fn inheritance_only_data_aces_are_not_current_grants_or_denials() {
    let canonical = map_acl(&fixture(), AclEncoding::NfsV4, OWNER).unwrap();
    let mut aces = decode_nfs4(&canonical).unwrap();
    aces.insert(
        0,
        NfsAce {
            ace_type: 1,
            flags: 1 | 8,
            permissions: 0x27,
            who: "future@example".into(),
        },
    );
    let result = project_acl(&encode_nfs4(&aces).unwrap(), AclEncoding::Posix, OWNER).unwrap();
    assert!(
        super::super::compare_acl(&canonical, &result.candidate.unwrap(), OWNER, OWNER, true)
            .unwrap()
    );
    assert!(result.issues.iter().any(|issue| issue.location
        == AclIssueLocation::NfsAce { index: 0 }
        && issue.principal.as_deref() == Some("future@example")));
}

#[test]
fn native_projection_is_exact_and_malformed_observations_remain_errors() {
    let native = encode_nfs4(&[NfsAce {
        ace_type: 1,
        flags: 0x100,
        permissions: 0x200,
        who: "alice@example".into(),
    }])
    .unwrap();
    let result = project_acl(&native, AclEncoding::NfsV4, OWNER).unwrap();
    assert_eq!(result.candidate, Some(native));
    assert!(result.issues.is_empty());
    let malformed = AclMetadata::new(AclEncoding::NfsV4, vec![1, 0]).unwrap();
    assert!(project_acl(&malformed, AclEncoding::NfsV4, OWNER).is_err());
    assert!(project_acl(&malformed, AclEncoding::Posix, OWNER).is_err());
}

#[test]
fn explicit_fallback_acls_deny_data_for_owner_named_and_other_principals() {
    let posix = decode_posix(&safe_deny_all_acl(AclEncoding::Posix).unwrap()).unwrap();
    let nfs = decode_nfs4(&safe_deny_all_acl(AclEncoding::NfsV4).unwrap()).unwrap();
    assert_eq!(nfs.len(), 1);
    assert_eq!(nfs[0].ace_type, 1);
    assert_eq!(nfs[0].who, "EVERYONE@");
    assert_eq!(nfs[0].flags, 0);
    assert_eq!(nfs[0].permissions, 0x1f_01bf);
    assert_eq!(
        nfs[0].permissions & 0x40,
        0,
        "DELETE_CHILD applies only to directories"
    );
    assert_eq!(posix.access.len(), 3);
    assert!(posix.default.is_empty());
    for uid in [OWNER.uid, 42421, 50000] {
        for member in [false, true] {
            for permissions in 1..8 {
                assert!(!super::super::semantics::posix_allows(
                    &posix,
                    OWNER,
                    uid,
                    &|_| member,
                    permissions
                ));
                assert!(!super::super::semantics::nfs_allows(
                    &nfs,
                    OWNER,
                    uid,
                    &|_| member,
                    super::super::mapping::data_mask(permissions)
                ));
            }
        }
    }
    let windows = safe_deny_all_acl(AclEncoding::WindowsSecurityDescriptor).unwrap();
    let bytes = windows.access().unwrap();
    assert_eq!(bytes.len(), 28);
    assert_eq!(&bytes[2..4], &0x9004_u16.to_le_bytes());
    assert_eq!(&bytes[16..20], &20_u32.to_le_bytes());
    assert_eq!(&bytes[24..26], &0_u16.to_le_bytes());
}

#[test]
fn accepted_strict_inputs_keep_candidates_and_report_unverified_cross_server_identity() {
    let posix = fixture();
    let nfs = map_acl(&posix, AclEncoding::NfsV4, OWNER).unwrap();
    for (source, target) in [
        (&posix, AclEncoding::Posix),
        (&posix, AclEncoding::NfsV4),
        (&nfs, AclEncoding::Posix),
        (&nfs, AclEncoding::NfsV4),
    ] {
        let projected = project_acl(source, target, OWNER).unwrap();
        assert_eq!(
            projected.candidate,
            Some(map_acl(source, target, OWNER).unwrap())
        );
        assert_eq!(
            projected
                .issues
                .iter()
                .filter(|issue| !matches!(
                    issue.kind,
                    AclCopyIssueKind::MaskedPermissionsDropped { .. }
                ))
                .map(|issue| &issue.kind)
                .collect::<Vec<_>>(),
            if source.encoding() == target {
                Vec::new()
            } else {
                vec![&AclCopyIssueKind::IdentityResolutionUnverified]
            }
        );
    }
    let mut native = decode_posix(&posix).unwrap();
    native.default = native.access.clone();
    let native = encode_posix(&native).unwrap();
    assert_eq!(
        project_acl(&native, AclEncoding::Posix, OWNER).unwrap(),
        AclProjection {
            candidate: Some(native),
            issues: Vec::new(),
        }
    );
}

#[test]
fn unknown_ace_types_are_malformed_and_cannot_be_sanitized_as_audit_entries() {
    let mut raw = encode_nfs4(&[NfsAce {
        ace_type: 2,
        flags: 0,
        permissions: 1,
        who: "OWNER@".into(),
    }])
    .unwrap()
    .access()
    .unwrap()
    .to_vec();
    raw[5] = 4;
    let source = AclMetadata::new(AclEncoding::NfsV4, raw).unwrap();
    assert!(project_acl(&source, AclEncoding::Posix, OWNER).is_err());
    let (_, issues, blocked) = super::super::projection_nfs::sanitize(&[NfsAce {
        ace_type: 4,
        flags: 0,
        permissions: 1,
        who: "OWNER@".into(),
    }]);
    assert!(blocked);
    assert!(
        issues
            .iter()
            .any(|issue| issue.kind == AclCopyIssueKind::UnsupportedAceType { ace_type: 4 })
    );
}

#[test]
fn posix_default_omissions_expose_original_permissions() {
    let mut posix = decode_posix(&fixture()).unwrap();
    posix.default = posix.access.clone();
    let source = encode_posix(&posix).unwrap();
    let projection = project_acl(&source, AclEncoding::NfsV4, OWNER).unwrap();
    for (index, entry) in posix.default.iter().enumerate() {
        assert!(projection.issues.iter().any(|issue| issue.location
            == AclIssueLocation::PosixDefault { index }
            && issue.kind
                == AclCopyIssueKind::UncopiedEntry {
                    entry: super::super::UncopiedAclEntry::PosixEntry {
                        tag: entry.tag,
                        id: entry.id,
                        permissions: entry.permissions,
                    },
                }));
    }
    assert!(projection.issues.iter().all(|issue| !matches!(
        (&issue.location, &issue.kind),
        (
            AclIssueLocation::PosixAccess { .. },
            AclCopyIssueKind::UncopiedEntry { .. }
        )
    )));
}

#[test]
fn posix_masked_permissions_are_reported_on_direct_and_default_projection_paths() {
    for defaults in [false, true] {
        let mut posix = decode_posix(&fixture()).unwrap();
        posix
            .access
            .iter_mut()
            .find(|e| e.tag == 4)
            .unwrap()
            .permissions = 7;
        posix
            .access
            .iter_mut()
            .find(|e| e.tag == 8)
            .unwrap()
            .permissions = 5;
        posix.access.reverse(); // Original report indices must not follow canonical sorting.
        if defaults {
            posix.default = posix.access.clone();
        }
        let source = encode_posix(&posix).unwrap();
        let projected = project_acl(&source, AclEncoding::NfsV4, OWNER).unwrap();
        let candidate = projected.candidate.as_ref().unwrap();
        let mut access_only = posix.clone();
        access_only.default.clear();
        let effective = encode_posix(&access_only).unwrap();
        assert!(super::super::compare_acl(&effective, candidate, OWNER, OWNER, true).unwrap());
        assert_masked_issues(&projected, &posix);
        assert!(
            project_acl(&source, AclEncoding::Posix, OWNER)
                .unwrap()
                .issues
                .is_empty()
        );
        assert!(
            project_acl(candidate, AclEncoding::Posix, OWNER)
                .unwrap()
                .issues
                .iter()
                .all(|issue| !matches!(
                    issue.kind,
                    AclCopyIssueKind::MaskedPermissionsDropped { .. }
                ))
        );
    }
}

fn assert_masked_issues(projected: &AclProjection, posix: &super::super::PosixAcl) {
    let issues: Vec<_> = projected
        .issues
        .iter()
        .filter(|issue| {
            matches!(
                issue.kind,
                AclCopyIssueKind::MaskedPermissionsDropped { .. }
            )
        })
        .collect();
    assert_eq!(issues.len(), 3);
    for (index, entry) in posix
        .access
        .iter()
        .enumerate()
        .filter(|(_, entry)| matches!(entry.tag, 2 | 4 | 8) && entry.permissions & !4 != 0)
    {
        assert!(issues.contains(&&AclCopyIssue {
            location: AclIssueLocation::PosixAccess { index },
            principal: matches!(entry.tag, 2 | 8).then(|| entry.id.to_string()),
            kind: AclCopyIssueKind::MaskedPermissionsDropped {
                tag: entry.tag,
                original: entry.permissions,
                mask: 4,
                omitted: entry.permissions & !4,
            },
        }));
    }
}
