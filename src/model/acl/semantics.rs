//! Independent, direct authorization evaluators. Neither evaluator calls the mapper.
use super::codec::{GROUP, GROUP_OBJ, MASK, OTHER, USER, USER_OBJ};
use super::mapping::{DATA, GROUP_FLAG, permission};
use super::{AclError, NfsAce, OwnershipMode, PosixAcl};
use std::collections::BTreeMap;

/// Prove the qualified template by exhaustive principal/permission equivalence classes.
/// Group masks form a chain, so there are at most four classes and sixteen subsets.
/// Named users with equal effective permissions are interchangeable in this template.
pub(super) fn verify(
    posix: &PosixAcl,
    nfs: &[NfsAce],
    owner: OwnershipMode,
) -> Result<(), AclError> {
    let mask = permission(&posix.access, MASK).unwrap_or(7);
    let groups = group_classes(posix, owner, mask);
    let mut masks: Vec<u32> = groups.values().copied().collect();
    masks.sort_unstable();
    masks.dedup();
    if masks.len() > 4 {
        return Err(AclError(
            "ACL group proof exceeded the inclusion-chain bound",
        ));
    }
    let users = user_representatives(posix, owner, mask)?;
    for selection in 0..(1_usize << masks.len()) {
        let member = |gid: u32| {
            groups.get(&gid).is_some_and(|permissions| {
                masks
                    .iter()
                    .position(|p| p == permissions)
                    .is_some_and(|index| selection & (1 << index) != 0)
            })
        };
        for uid in &users {
            for request in 1..8 {
                if posix_allows(posix, owner, *uid, &member, request)
                    != nfs_allows(nfs, owner, *uid, &member, nfs_request(request))
                {
                    return Err(AclError(
                        "independent ACL evaluators found an authorization counterexample",
                    ));
                }
            }
        }
    }
    Ok(())
}

// Keep the request interpretation separate from the ACL mapper: write requires
// both WRITE_DATA and APPEND_DATA, and execute is ACE4_EXECUTE (not POSIX bit 1).
fn nfs_request(request: u32) -> u32 {
    let mut result = 0;
    if request & 4 != 0 {
        result |= 0x1;
    }
    if request & 2 != 0 {
        result |= 0x2 | 0x4;
    }
    if request & 1 != 0 {
        result |= 0x20;
    }
    result
}

fn group_classes(posix: &PosixAcl, owner: OwnershipMode, mask: u32) -> BTreeMap<u32, u32> {
    posix
        .access
        .iter()
        .filter_map(|e| match e.tag {
            GROUP_OBJ => Some((owner.gid, e.permissions & mask)),
            GROUP => Some((e.id, e.permissions & mask)),
            _ => None,
        })
        .collect()
}

fn user_representatives(
    posix: &PosixAcl,
    owner: OwnershipMode,
    mask: u32,
) -> Result<Vec<u32>, AclError> {
    let mut representatives = BTreeMap::new();
    let mut identities = std::collections::BTreeSet::from([owner.uid]);
    for entry in posix.access.iter().filter(|e| e.tag == USER) {
        representatives
            .entry(entry.permissions & mask)
            .or_insert(entry.id);
        identities.insert(entry.id);
    }
    let other = (0..=u32::MAX)
        .find(|uid| !identities.contains(uid))
        .ok_or(AclError("no distinct other identity for ACL proof"))?;
    let mut users = vec![owner.uid, other];
    users.extend(representatives.into_values());
    Ok(users)
}

/// Linux POSIX selection: owner, named user, one matching group covering the
/// entire request, then other only when no group matched. Never union groups.
pub(super) fn posix_allows(
    posix: &PosixAcl,
    owner: OwnershipMode,
    uid: u32,
    member: &impl Fn(u32) -> bool,
    request: u32,
) -> bool {
    if uid == owner.uid {
        return permission(&posix.access, USER_OBJ).unwrap_or(0) & request == request;
    }
    let mask = permission(&posix.access, MASK).unwrap_or(7);
    if let Some(entry) = posix.access.iter().find(|e| e.tag == USER && e.id == uid) {
        return entry.permissions & mask & request == request;
    }
    let mut matched = false;
    for entry in &posix.access {
        let applies = match entry.tag {
            GROUP_OBJ => member(owner.gid),
            GROUP => member(entry.id),
            _ => false,
        };
        if applies {
            matched = true;
            if entry.permissions & mask & request == request {
                return true;
            }
        }
    }
    !matched && permission(&posix.access, OTHER).unwrap_or(0) & request == request
}

/// `NFSv4` walks ACEs in order, deciding only still-undecided requested bits.
pub(super) fn nfs_allows(
    aces: &[NfsAce],
    owner: OwnershipMode,
    uid: u32,
    member: &impl Fn(u32) -> bool,
    request: u32,
) -> bool {
    let mut remaining = request & DATA;
    for ace in aces {
        let applies = match ace.who.as_str() {
            "OWNER@" => uid == owner.uid,
            "GROUP@" => member(owner.gid),
            "EVERYONE@" => true,
            name => name.parse::<u32>().is_ok_and(|id| {
                if ace.flags & GROUP_FLAG != 0 {
                    member(id)
                } else {
                    id == uid
                }
            }),
        };
        if !applies {
            continue;
        }
        let relevant = remaining & ace.permissions;
        if ace.ace_type == 1 && relevant != 0 {
            return false;
        }
        if ace.ace_type == 0 {
            remaining &= !relevant;
        }
        if remaining == 0 {
            return true;
        }
    }
    false
}
