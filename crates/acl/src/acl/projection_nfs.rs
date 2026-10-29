//! Remove only meanings known not to affect current regular-file data authorization.

use std::collections::BTreeSet;

use super::mapping::{
    COMMON_METADATA_READ_PERMISSIONS as COMMON_METADATA, DATA, GROUP_FLAG,
    OWNER_METADATA_WRITE_PERMISSIONS as OWNER_METADATA, WRITE, principal,
};
use super::{AclCopyIssue, AclCopyIssueKind as Kind, AclIssueLocation, NfsAce};

const KNOWN_RIGHTS: u32 = 0x1f_01ff;
const INHERITANCE_FLAGS: u32 = 1 | 2 | 4 | 8 | 0x80;
const AUDIT_FLAGS: u32 = 0x10 | 0x20;

pub(super) fn sanitize(entries: &[NfsAce]) -> (Vec<NfsAce>, Vec<AclCopyIssue>, bool) {
    let data_principals: BTreeSet<_> = entries
        .iter()
        .filter(|ace| ace.ace_type <= 1 && ace.flags & 8 == 0 && ace.permissions & DATA != 0)
        .map(principal_key)
        .collect();
    let mut retained = Vec::new();
    let mut issues = Vec::new();
    let mut blocked = false;
    for (index, ace) in entries.iter().enumerate() {
        blocked |= unknown_authorization(ace, index, &mut issues);
        if matches!(ace.ace_type, 2 | 3) || ace.flags & 8 != 0 {
            record_inactive(ace, index, &mut issues);
            continue;
        }
        record_flags(ace, index, &mut issues);
        let data = ace.permissions & DATA;
        if data == 0 && (ace.ace_type == 1 || !needed_zero_allow(ace, &data_principals)) {
            if ace.permissions != 0 {
                issues.push(issue(
                    ace,
                    index,
                    Kind::UnsupportedRights {
                        rights: ace.permissions,
                    },
                ));
            }
            continue;
        }
        blocked |= unmapped_or_partial(ace, index, &mut issues);
        let mut projected = ace.clone();
        projected.flags &= GROUP_FLAG;
        projected.permissions = data | metadata_template(ace);
        record_metadata(ace, index, &mut issues);
        retained.push(projected);
    }
    (retained, issues, blocked)
}

fn principal_key(ace: &NfsAce) -> (String, u32) {
    (
        ace.who.clone(),
        if special(&ace.who) {
            0
        } else {
            ace.flags & GROUP_FLAG
        },
    )
}

fn special(who: &str) -> bool {
    matches!(who, "OWNER@" | "GROUP@" | "EVERYONE@")
}

fn needed_zero_allow(ace: &NfsAce, data_principals: &BTreeSet<(String, u32)>) -> bool {
    // These empty ALLOWs are scaffolding for POSIX base/zero entries. They grant no DATA.
    special(&ace.who) || data_principals.contains(&principal_key(ace))
}

fn unknown_authorization(ace: &NfsAce, index: usize, issues: &mut Vec<AclCopyIssue>) -> bool {
    let unknown_type = ace.ace_type > 3;
    if unknown_type {
        issues.push(issue(
            ace,
            index,
            Kind::UnsupportedAceType {
                ace_type: ace.ace_type,
            },
        ));
    }
    let known_flags = GROUP_FLAG
        | INHERITANCE_FLAGS
        | if matches!(ace.ace_type, 2 | 3) {
            AUDIT_FLAGS
        } else {
            0
        };
    let flags = ace.flags & !known_flags;
    if flags != 0 {
        issues.push(issue(ace, index, Kind::UnsupportedFlags { flags }));
    }
    let rights = ace.permissions & !KNOWN_RIGHTS;
    if ace.ace_type <= 1 && rights != 0 {
        issues.push(issue(ace, index, Kind::UnsupportedRights { rights }));
    }
    unknown_type || flags != 0 || (ace.ace_type <= 1 && rights != 0)
}

fn record_inactive(ace: &NfsAce, index: usize, issues: &mut Vec<AclCopyIssue>) {
    if ace.ace_type > 1 {
        issues.push(issue(
            ace,
            index,
            Kind::UnsupportedAceType {
                ace_type: ace.ace_type,
            },
        ));
    }
    if ace.flags != 0 {
        issues.push(issue(
            ace,
            index,
            Kind::UnsupportedFlags { flags: ace.flags },
        ));
    }
    if ace.permissions != 0 {
        issues.push(issue(
            ace,
            index,
            Kind::UnsupportedRights {
                rights: ace.permissions,
            },
        ));
    }
}

fn record_flags(ace: &NfsAce, index: usize, issues: &mut Vec<AclCopyIssue>) {
    let flags = ace.flags & INHERITANCE_FLAGS;
    if flags != 0 {
        issues.push(issue(ace, index, Kind::UnsupportedFlags { flags }));
    }
}

fn unmapped_or_partial(ace: &NfsAce, index: usize, issues: &mut Vec<AclCopyIssue>) -> bool {
    let unmapped = principal(ace).is_err();
    if unmapped {
        issues.push(issue(
            ace,
            index,
            Kind::UnmappedPrincipal {
                principal: ace.who.clone(),
            },
        ));
    }
    let rights = ace.permissions & WRITE;
    let partial = rights != 0 && rights != WRITE;
    if partial {
        issues.push(issue(ace, index, Kind::PartialWriteRights { rights }));
    }
    unmapped || partial
}

fn metadata_template(ace: &NfsAce) -> u32 {
    if ace.ace_type == 0 {
        COMMON_METADATA
            | if ace.who == "OWNER@" {
                OWNER_METADATA
            } else {
                0
            }
    } else {
        0
    }
}

fn record_metadata(ace: &NfsAce, index: usize, issues: &mut Vec<AclCopyIssue>) {
    let original = ace.permissions & !DATA;
    let normalized = metadata_template(ace);
    let rights = original & KNOWN_RIGHTS & !normalized;
    if rights != 0 {
        issues.push(issue(ace, index, Kind::UnsupportedRights { rights }));
    }
    if normalized & !original != 0 {
        issues.push(issue(
            ace,
            index,
            Kind::MetadataRightsNormalized {
                original,
                normalized,
            },
        ));
    }
}

fn issue(ace: &NfsAce, index: usize, kind: Kind) -> AclCopyIssue {
    AclCopyIssue {
        location: AclIssueLocation::NfsAce { index },
        principal: Some(ace.who.clone()),
        kind,
    }
}
