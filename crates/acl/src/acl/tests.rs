#![allow(clippy::unwrap_used)]

use super::codec::*;
use super::*;

pub(super) const OWNER: OwnershipMode = OwnershipMode {
    uid: 42000,
    gid: 42001,
    mode: 0o640,
};

fn entry(tag: u32, id: u32, permissions: u32) -> PosixEntry {
    PosixEntry {
        tag,
        id,
        permissions,
    }
}

pub(super) fn fixture() -> AclMetadata {
    encode_posix(&PosixAcl {
        access: vec![
            entry(USER_OBJ, UNDEFINED, 6),
            entry(USER, 42421, 7),
            entry(GROUP_OBJ, UNDEFINED, 4),
            entry(GROUP, 42422, 4),
            entry(MASK, UNDEFINED, 4),
            entry(OTHER, UNDEFINED, 0),
        ],
        default: Vec::new(),
    })
    .unwrap()
}

#[test]
fn accepted_bidirectional_mapping_preserves_current_access_and_native_readback() {
    let source = fixture();
    let target = map_acl(&source, AclEncoding::NfsV4, OWNER).unwrap();
    assert!(compare_acl(&source, &target, OWNER, OWNER, true).unwrap());
    let reverse = map_acl(&target, AclEncoding::Posix, OWNER).unwrap();
    assert!(compare_acl(&target, &reverse, OWNER, OWNER, true).unwrap());
    assert!(
        !equivalent_native(&source, &reverse).unwrap(),
        "masked raw rwx must not silently survive projection"
    );
}

#[test]
fn native_posix_preserves_latent_bits_mask_default_and_unordered_entries() {
    let mut source = decode_posix(&fixture()).unwrap();
    source.default = source.access.clone();
    let source = encode_posix(&source).unwrap();
    assert_eq!(map_acl(&source, AclEncoding::Posix, OWNER).unwrap(), source);
    let mut reordered = decode_posix(&source).unwrap();
    reordered.access.reverse();
    assert!(equivalent_native(&source, &encode_posix(&reordered).unwrap()).unwrap());
    reordered
        .access
        .iter_mut()
        .find(|e| e.tag == USER)
        .unwrap()
        .permissions = 4;
    assert!(!equivalent_native(&source, &encode_posix(&reordered).unwrap()).unwrap());
}

#[test]
fn cross_model_group_split_request_is_rejected_while_native_copy_is_accepted() {
    let mut source = decode_posix(&fixture()).unwrap();
    source
        .access
        .iter_mut()
        .find(|e| e.tag == GROUP)
        .unwrap()
        .permissions = 2;
    source
        .access
        .iter_mut()
        .find(|e| e.tag == MASK)
        .unwrap()
        .permissions = 7;
    let source = encode_posix(&source).unwrap();
    assert!(map_acl(&source, AclEncoding::NfsV4, OWNER).is_err());
    assert!(map_acl(&source, AclEncoding::Posix, OWNER).is_ok());
}

#[test]
fn mapped_acl_missing_named_principal_never_passes_compare() {
    let source = fixture();
    let target = map_acl(&source, AclEncoding::NfsV4, OWNER).unwrap();
    let mut aces = decode_nfs4(&target).unwrap();
    aces.retain(|ace| ace.who != "42421");
    assert!(
        !compare_acl(&source, &encode_nfs4(&aces).unwrap(), OWNER, OWNER, true).unwrap_or(false)
    );
}

#[test]
fn native_nfs4_preserves_order_audit_inheritance_and_non_posix_rights() {
    let aces = vec![
        NfsAce {
            ace_type: 1,
            flags: 0,
            permissions: 0x8_0000,
            who: "alice@example".into(),
        },
        NfsAce {
            ace_type: 0,
            flags: 3,
            permissions: 4 | 0x1_0000,
            who: "EVERYONE@".into(),
        },
        NfsAce {
            ace_type: 2,
            flags: 0x10,
            permissions: 0x2_0000,
            who: "OWNER@".into(),
        },
    ];
    let source = encode_nfs4(&aces).unwrap();
    assert_eq!(map_acl(&source, AclEncoding::NfsV4, OWNER).unwrap(), source);
    assert!(equivalent_native(&source, &source).unwrap());
    let mut reordered = aces.clone();
    reordered.swap(0, 1);
    assert!(!equivalent_native(&source, &encode_nfs4(&reordered).unwrap()).unwrap());
    assert!(map_acl(&source, AclEncoding::Posix, OWNER).is_err());
}

#[test]
fn special_group_flag_is_normalized_without_dropping_named_group_identity() {
    let source = map_acl(&fixture(), AclEncoding::NfsV4, OWNER).unwrap();
    let mut aces = decode_nfs4(&source).unwrap();
    aces.iter_mut().find(|a| a.who == "GROUP@").unwrap().flags ^= 0x40;
    let rewritten = encode_nfs4(&aces).unwrap();
    assert!(equivalent_native(&source, &rewritten).unwrap());
    assert!(compare_acl(&source, &rewritten, OWNER, OWNER, true).unwrap());
    assert!(compare_acl(&source, &rewritten, OWNER, OWNER, false).unwrap());
    aces.iter_mut().find(|a| a.who == "42422").unwrap().flags ^= 0x40;
    assert!(!equivalent_native(&source, &encode_nfs4(&aces).unwrap()).unwrap());
}

#[test]
fn same_group_mask_chain_accepts_combined_read_write_requests() {
    let mut posix = decode_posix(&fixture()).unwrap();
    posix
        .access
        .iter_mut()
        .find(|e| e.tag == GROUP)
        .unwrap()
        .permissions = 6;
    posix
        .access
        .iter_mut()
        .find(|e| e.tag == MASK)
        .unwrap()
        .permissions = 6;
    let source = encode_posix(&posix).unwrap();
    let target = map_acl(&source, AclEncoding::NfsV4, OWNER).unwrap();
    let nfs = decode_nfs4(&target).unwrap();
    let both = |gid| gid == OWNER.gid || gid == 42422;
    assert!(semantics::posix_allows(&posix, OWNER, 43000, &both, 6));
    assert!(semantics::nfs_allows(&nfs, OWNER, 43000, &both, 1 | 2 | 4));
    assert!(compare_acl(&source, &target, OWNER, OWNER, true).unwrap());
}

#[test]
fn owner_named_and_zero_group_precedence_are_preserved_against_everyone() {
    let posix = PosixAcl {
        access: vec![
            entry(USER_OBJ, UNDEFINED, 0),
            entry(USER, 42421, 0),
            entry(GROUP_OBJ, UNDEFINED, 0),
            entry(GROUP, 42422, 0),
            entry(MASK, UNDEFINED, 7),
            entry(OTHER, UNDEFINED, 7),
        ],
        default: Vec::new(),
    };
    let source = encode_posix(&posix).unwrap();
    let target = map_acl(&source, AclEncoding::NfsV4, OWNER).unwrap();
    let nfs = decode_nfs4(&target).unwrap();
    for (uid, group, expected) in [
        (OWNER.uid, false, false),
        (42421, false, false),
        (43000, true, false),
        (43000, false, true),
    ] {
        let member = |gid| group && gid == 42422;
        for request in 1..8 {
            assert_eq!(
                semantics::posix_allows(&posix, OWNER, uid, &member, request),
                expected
            );
            assert_eq!(
                semantics::nfs_allows(&nfs, OWNER, uid, &member, mapping::data_mask(request)),
                expected
            );
        }
    }
    assert!(map_acl(&target, AclEncoding::Posix, OWNER).is_ok());
}

#[test]
fn independent_evaluators_find_naive_group_union_counterexample() {
    let posix = PosixAcl {
        access: vec![
            entry(USER_OBJ, UNDEFINED, 7),
            entry(GROUP_OBJ, UNDEFINED, 4),
            entry(GROUP, 42422, 2),
            entry(MASK, UNDEFINED, 7),
            entry(OTHER, UNDEFINED, 0),
        ],
        default: Vec::new(),
    };
    let nfs = vec![
        NfsAce {
            ace_type: 0,
            flags: 0x40,
            permissions: 1,
            who: "GROUP@".into(),
        },
        NfsAce {
            ace_type: 0,
            flags: 0x40,
            permissions: 2 | 4,
            who: "42422".into(),
        },
    ];
    let both = |gid| gid == OWNER.gid || gid == 42422;
    assert!(semantics::posix_allows(&posix, OWNER, 43000, &both, 4));
    assert!(semantics::posix_allows(&posix, OWNER, 43000, &both, 2));
    assert!(!semantics::posix_allows(&posix, OWNER, 43000, &both, 6));
    assert!(semantics::nfs_allows(&nfs, OWNER, 43000, &both, 1 | 2 | 4));
}

#[test]
fn reverse_rejects_partial_write_audit_unknown_rights_inheritance_and_nonnumeric_names() {
    let source = map_acl(&fixture(), AclEncoding::NfsV4, OWNER).unwrap();
    let original = decode_nfs4(&source).unwrap();
    for change in 0..8 {
        let mut aces = original.clone();
        let target = aces.iter_mut().find(|a| a.who == "42421").unwrap();
        match change {
            0 => target.permissions |= 4,
            1 => target.permissions |= 2,
            2 => target.permissions |= 0x8_0000,
            3 => target.permissions |= 0x4_0000,
            4 => target.ace_type = 2,
            5 => target.flags |= 1,
            6 => target.who = "alice@example".into(),
            _ => target.permissions |= 0x8000_0000,
        }
        let source = encode_nfs4(&aces).unwrap();
        assert!(
            map_acl(&source, AclEncoding::Posix, OWNER).is_err(),
            "must reject case {change}"
        );
    }
}

#[test]
fn comparator_does_not_hide_extra_rights_or_changed_ace_order() {
    let source = fixture();
    let mapped = map_acl(&source, AclEncoding::NfsV4, OWNER).unwrap();
    let original = decode_nfs4(&mapped).unwrap();
    for change in 0..3 {
        let mut aces = original.clone();
        match change {
            0 => aces.last_mut().unwrap().permissions |= 1,
            1 => {
                aces.iter_mut()
                    .find(|a| a.who == "42421")
                    .unwrap()
                    .permissions |= 0x8_0000;
            }
            _ => aces.swap(0, 1),
        }
        assert!(
            !compare_acl(&source, &encode_nfs4(&aces).unwrap(), OWNER, OWNER, true)
                .unwrap_or(false)
        );
    }
}

#[test]
fn cross_model_rejects_default_acl_and_unasserted_or_conflicting_identity() {
    let source = fixture();
    let target = map_acl(&source, AclEncoding::NfsV4, OWNER).unwrap();
    assert!(compare_acl(&source, &target, OWNER, OWNER, false).is_err());
    let different_owner = OwnershipMode {
        uid: 42400,
        ..OWNER
    };
    assert!(compare_acl(&source, &target, OWNER, different_owner, true).is_err());
    let mut posix = decode_posix(&source).unwrap();
    posix.default = posix.access.clone();
    assert!(map_acl(&encode_posix(&posix).unwrap(), AclEncoding::NfsV4, OWNER).is_err());
    posix.default.clear();
    posix.access.iter_mut().find(|e| e.tag == USER).unwrap().id = OWNER.uid;
    assert!(map_acl(&encode_posix(&posix).unwrap(), AclEncoding::NfsV4, OWNER).is_err());
}

#[test]
fn malformed_posix_and_nfs4_observations_are_rejected() {
    let source = fixture();
    let bytes = source.access().unwrap().to_vec();
    for change in 0..5 {
        let mut bytes = bytes.clone();
        match change {
            0 => bytes[0] = 1,
            1 => {
                bytes.pop();
            }
            2 => bytes[6] = 8,
            3 => bytes[4] = 64,
            _ => {
                bytes[8..12].copy_from_slice(&1_u32.to_le_bytes());
            }
        }
        assert!(decode_posix(&AclMetadata::new(AclEncoding::Posix, bytes).unwrap()).is_err());
    }
    let mut invalid = decode_posix(&source).unwrap();
    invalid.access.retain(|e| e.tag != MASK);
    assert!(encode_posix(&invalid).is_err());
    invalid = decode_posix(&source).unwrap();
    invalid.access.push(invalid.access[0]);
    assert!(encode_posix(&invalid).is_err());
    for bytes in [
        vec![1, 1, 0, 0, 0],
        vec![1, 0, 0, 0, 0, 9],
        vec![2, 0, 0, 0, 0],
    ] {
        assert!(decode_nfs4(&AclMetadata::new(AclEncoding::NfsV4, bytes).unwrap()).is_err());
    }
}

#[test]
fn native_basic_mask_normalization_never_discards_a_latent_group_permission() {
    let basic = PosixAcl {
        access: vec![
            entry(USER_OBJ, UNDEFINED, 6),
            entry(GROUP_OBJ, UNDEFINED, 4),
            entry(OTHER, UNDEFINED, 0),
        ],
        default: Vec::new(),
    };
    let source = encode_posix(&basic).unwrap();
    let mut server = basic;
    server.access.push(entry(MASK, UNDEFINED, 4));
    assert!(equivalent_native(&source, &encode_posix(&server).unwrap()).unwrap());
    server
        .access
        .iter_mut()
        .find(|e| e.tag == GROUP_OBJ)
        .unwrap()
        .permissions = 7;
    assert!(!equivalent_native(&source, &encode_posix(&server).unwrap()).unwrap());
}

#[test]
fn known_absent_posix_access_uses_observed_mode_and_still_preserves_defaults() {
    let absent = AclMetadata::new_posix(None, None).unwrap();
    let basic = PosixAcl {
        access: vec![
            entry(USER_OBJ, UNDEFINED, 6),
            entry(GROUP_OBJ, UNDEFINED, 4),
            entry(OTHER, UNDEFINED, 0),
        ],
        default: Vec::new(),
    };
    let basic = encode_posix(&basic).unwrap();
    assert!(compare_acl(&absent, &basic, OWNER, OWNER, false).unwrap());
    let target = map_acl(&absent, AclEncoding::NfsV4, OWNER).unwrap();
    assert!(compare_acl(&absent, &target, OWNER, OWNER, true).unwrap());
    let native = map_acl(&absent, AclEncoding::Posix, OWNER).unwrap();
    assert!(equivalent_native(&native, &basic).unwrap());
}

#[test]
fn every_basic_mode_roundtrips_with_an_independent_semantic_proof() {
    for mode in 0..0o1000 {
        let owner = OwnershipMode { mode, ..OWNER };
        let source = AclMetadata::new_posix(None, None).unwrap();
        let nfs = map_acl(&source, AclEncoding::NfsV4, owner).unwrap();
        let posix = map_acl(&nfs, AclEncoding::Posix, owner).unwrap();
        assert!(
            compare_acl(&source, &nfs, owner, owner, true).unwrap(),
            "mode {mode:o}"
        );
        assert!(
            compare_acl(&nfs, &posix, owner, owner, true).unwrap(),
            "mode {mode:o}"
        );
    }
}

fn zero_group_rights(other: u32, mask: u32) -> PosixAcl {
    PosixAcl {
        access: vec![
            entry(USER_OBJ, UNDEFINED, 6),
            entry(USER, 42421, 0),
            entry(GROUP_OBJ, UNDEFINED, 0),
            entry(GROUP, 42422, 0),
            entry(MASK, UNDEFINED, mask),
            entry(OTHER, UNDEFINED, other),
        ],
        default: Vec::new(),
    }
}

// Linux fs/namei.c acl_permission_check() skips check_acl() when the
// ACL-derived mode's group class is zero. This oracle deliberately wraps the
// POSIX evaluator instead of silently changing its mathematical selection rules.
fn linux_profile_allows(
    posix: &PosixAcl,
    uid: u32,
    member: &impl Fn(u32) -> bool,
    request: u32,
) -> bool {
    let group_mode = posix
        .access
        .iter()
        .find(|e| e.tag == MASK)
        .or_else(|| posix.access.iter().find(|e| e.tag == GROUP_OBJ))
        .map_or(0, |e| e.permissions);
    if uid == OWNER.uid || group_mode != 0 {
        return semantics::posix_allows(posix, OWNER, uid, member, request);
    }
    let permissions = if member(OWNER.gid) {
        0
    } else {
        posix
            .access
            .iter()
            .find(|e| e.tag == OTHER)
            .map_or(0, |e| e.permissions)
    };
    permissions & request == request
}

#[test]
fn reverse_zero_group_union_does_not_disable_linux_named_user_deny() {
    let source = zero_group_rights(4, 4);
    assert!(!linux_profile_allows(&source, 42421, &|_| false, 4));
    let nfs = map_acl(&encode_posix(&source).unwrap(), AclEncoding::NfsV4, OWNER).unwrap();
    let target = map_acl(&nfs, AclEncoding::Posix, OWNER).unwrap();
    let target_posix = decode_posix(&target).unwrap();
    assert!(
        !linux_profile_allows(&target_posix, 42421, &|_| false, 4),
        "reverse mapping must preserve named denial through Linux's zero-group-mode fastpath"
    );
    assert!(!linux_profile_allows(
        &target_posix,
        43000,
        &|gid| gid == 42422,
        4
    ));
    assert!(linux_profile_allows(&target_posix, 43000, &|_| false, 4));
    assert!(compare_acl(&nfs, &target, OWNER, OWNER, true).unwrap());
}

#[test]
fn unsafe_zero_mask_source_cannot_receive_a_cross_model_equivalence_certificate() {
    let unsafe_source = encode_posix(&zero_group_rights(4, 0)).unwrap();
    assert!(linux_profile_allows(
        &decode_posix(&unsafe_source).unwrap(),
        42421,
        &|_| false,
        4
    ));
    assert!(
        map_acl(&unsafe_source, AclEncoding::NfsV4, OWNER).is_err(),
        "Linux skips named restrictions when an extended mask is zero but other grants access"
    );
    let safe_nfs = map_acl(
        &encode_posix(&zero_group_rights(4, 4)).unwrap(),
        AclEncoding::NfsV4,
        OWNER,
    )
    .unwrap();
    assert!(compare_acl(&unsafe_source, &safe_nfs, OWNER, OWNER, true).is_err());
    assert_eq!(
        map_acl(&unsafe_source, AclEncoding::Posix, OWNER).unwrap(),
        unsafe_source,
        "native copying must preserve the original configuration without cross-model qualification"
    );
}

#[test]
fn zero_mask_without_other_grants_stays_in_the_safe_cross_model_subset() {
    let source = encode_posix(&zero_group_rights(0, 0)).unwrap();
    let nfs = map_acl(&source, AclEncoding::NfsV4, OWNER).unwrap();
    let target = map_acl(&nfs, AclEncoding::Posix, OWNER).unwrap();
    assert!(!linux_profile_allows(
        &decode_posix(&target).unwrap(),
        42421,
        &|_| false,
        4
    ));
    assert!(compare_acl(&source, &nfs, OWNER, OWNER, true).unwrap());
}
