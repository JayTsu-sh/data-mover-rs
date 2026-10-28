use super::codec::{GROUP, GROUP_OBJ, MASK, OTHER, UNDEFINED, USER, USER_OBJ};
use super::{AclEncoding, AclError, AclMetadata, NfsAce, OwnershipMode, PosixAcl, PosixEntry};
use super::{decode_nfs4, decode_posix, encode_nfs4, encode_posix, semantics};

pub(super) const GROUP_FLAG: u32 = 0x40;
pub(super) const READ: u32 = 1;
pub(super) const WRITE: u32 = 2 | 4;
pub(super) const EXECUTE: u32 = 0x20;
pub(super) const DATA: u32 = READ | WRITE | EXECUTE;
const BASE: u32 = 0x80 | 0x2_0000 | 0x10_0000;
const OWNER_EXTRA: u32 = 0x100 | 0x4_0000;

pub(super) fn map(
    source: &AclMetadata,
    target: AclEncoding,
    owner: OwnershipMode,
) -> Result<AclMetadata, AclError> {
    if source.encoding() == target {
        validate_native(source)?;
        if target == AclEncoding::Posix && decode_posix(source)?.access.is_empty() {
            return encode_posix(&materialize(decode_posix(source)?, owner));
        }
        return Ok(source.clone());
    }
    match (source.encoding(), target) {
        (AclEncoding::Posix, AclEncoding::NfsV4) => {
            let posix = materialize(decode_posix(source)?, owner);
            qualify_posix(&posix, owner)?;
            let nfs = canonical(&posix);
            semantics::verify(&posix, &nfs, owner)?;
            encode_nfs4(&nfs)
        }
        (AclEncoding::NfsV4, AclEncoding::Posix) => {
            let nfs = decode_nfs4(source)?;
            let posix = reverse(&nfs, owner)?;
            semantics::verify(&posix, &nfs, owner)?;
            encode_posix(&posix)
        }
        _ => Err(AclError("unsupported cross-model ACL encoding")),
    }
}

pub(super) fn compare_native(
    source: &AclMetadata,
    destination: &AclMetadata,
    source_owner: OwnershipMode,
    destination_owner: OwnershipMode,
    _equivalent: bool,
) -> Result<bool, AclError> {
    if source.encoding() == AclEncoding::Posix {
        let source = normalize_posix(materialize(decode_posix(source)?, source_owner));
        let destination =
            normalize_posix(materialize(decode_posix(destination)?, destination_owner));
        return Ok(source == destination);
    }
    equivalent_native(source, destination)
}

pub(super) fn equivalent_native(
    source: &AclMetadata,
    destination: &AclMetadata,
) -> Result<bool, AclError> {
    if source.encoding() != destination.encoding() {
        return Err(AclError(
            "native ACL comparison requires matching encodings",
        ));
    }
    match source.encoding() {
        AclEncoding::Posix => {
            Ok(normalize_posix(decode_posix(source)?)
                == normalize_posix(decode_posix(destination)?))
        }
        AclEncoding::NfsV4 => {
            Ok(normalize_nfs(decode_nfs4(source)?) == normalize_nfs(decode_nfs4(destination)?))
        }
        AclEncoding::WindowsSecurityDescriptor => Ok(source == destination),
    }
}

pub(super) fn compare_cross(
    source: &AclMetadata,
    destination: &AclMetadata,
    source_owner: OwnershipMode,
    destination_owner: OwnershipMode,
) -> Result<bool, AclError> {
    let (posix, nfs, owner) = match (source.encoding(), destination.encoding()) {
        (AclEncoding::Posix, AclEncoding::NfsV4) => (source, destination, source_owner),
        (AclEncoding::NfsV4, AclEncoding::Posix) => (destination, source, destination_owner),
        _ => return Err(AclError("unsupported cross-model ACL encoding")),
    };
    let posix = materialize(decode_posix(posix)?, owner);
    qualify_posix(&posix, owner)?;
    let actual_nfs = decode_nfs4(nfs)?;
    qualify_nfs(&actual_nfs)?;
    if normalize_nfs(canonical(&posix)) != normalize_nfs(actual_nfs.clone()) {
        return Ok(false);
    }
    semantics::verify(&posix, &actual_nfs, owner)?;
    Ok(true)
}

fn validate_native(value: &AclMetadata) -> Result<(), AclError> {
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

pub(super) fn materialize(mut posix: PosixAcl, owner: OwnershipMode) -> PosixAcl {
    if posix.access.is_empty() {
        posix.access = vec![
            entry(USER_OBJ, UNDEFINED, (owner.mode >> 6) & 7),
            entry(GROUP_OBJ, UNDEFINED, (owner.mode >> 3) & 7),
            entry(OTHER, UNDEFINED, owner.mode & 7),
        ];
    }
    posix
}

fn normalize_posix(mut value: PosixAcl) -> PosixAcl {
    for entries in [&mut value.access, &mut value.default] {
        if entries.len() == 4 && !entries.iter().any(|e| matches!(e.tag, USER | GROUP)) {
            let group = entries
                .iter()
                .find(|e| e.tag == GROUP_OBJ)
                .map(|e| e.permissions);
            if entries
                .iter()
                .any(|e| e.tag == MASK && Some(e.permissions) == group)
            {
                entries.retain(|e| e.tag != MASK);
            }
        }
        entries.sort_unstable();
    }
    value
}

fn normalize_nfs(mut entries: Vec<NfsAce>) -> Vec<NfsAce> {
    for ace in &mut entries {
        if matches!(ace.who.as_str(), "OWNER@" | "GROUP@" | "EVERYONE@") {
            ace.flags &= !GROUP_FLAG;
        }
    }
    entries
}

pub(super) fn qualify_posix(value: &PosixAcl, owner: OwnershipMode) -> Result<(), AclError> {
    if !value.default.is_empty() {
        return Err(AclError(
            "cross-model default ACL and inheritance are unsupported",
        ));
    }
    if owner.uid == UNDEFINED || owner.gid == UNDEFINED {
        return Err(AclError("undefined numeric owner identity"));
    }
    if value
        .access
        .iter()
        .any(|e| (e.tag == USER && e.id == owner.uid) || (e.tag == GROUP && e.id == owner.gid))
    {
        return Err(AclError(
            "cross-model named identity aliases the owner or owning group",
        ));
    }
    let mask = permission(&value.access, MASK).unwrap_or(7);
    // Linux namei.c skips ACL evaluation when the mode group class is zero.
    // Named restrictions can then disappear into the other-mode fallback.
    if mask == 0
        && permission(&value.access, OTHER).unwrap_or(0) != 0
        && value.access.iter().any(|e| matches!(e.tag, USER | GROUP))
    {
        return Err(AclError(
            "zero POSIX mask with named entries and other grants is outside the Linux access profile",
        ));
    }
    qualify_group_chain(value, mask)
}

fn qualify_group_chain(value: &PosixAcl, mask: u32) -> Result<(), AclError> {
    let mut distinct = [false; 8];
    for e in value
        .access
        .iter()
        .filter(|e| matches!(e.tag, GROUP_OBJ | GROUP))
    {
        distinct[(e.permissions & mask) as usize] = true;
    }
    for left in 0..8 {
        for right in 0..8 {
            if distinct[left] && distinct[right] && left & right != left && left & right != right {
                return Err(AclError(
                    "cross-model group permissions do not form an inclusion chain",
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn permission(entries: &[PosixEntry], tag: u32) -> Option<u32> {
    entries.iter().find(|e| e.tag == tag).map(|e| e.permissions)
}

pub(super) fn data_mask(permissions: u32) -> u32 {
    (u32::from(permissions & 4 != 0) * READ)
        | (u32::from(permissions & 2 != 0) * WRITE)
        | (u32::from(permissions & 1 != 0) * EXECUTE)
}

fn low_permissions(mask: u32) -> u32 {
    (u32::from(mask & READ != 0) * 4)
        | (u32::from(mask & WRITE == WRITE) * 2)
        | u32::from(mask & EXECUTE != 0)
}

fn canonical(value: &PosixAcl) -> Vec<NfsAce> {
    let mut access = value.access.clone();
    access.sort_unstable();
    let mask = permission(&access, MASK).unwrap_or(7);
    let other = permission(&access, OTHER).unwrap_or(0);
    let owner = permission(&access, USER_OBJ).unwrap_or(0);
    let group_union = access
        .iter()
        .filter(|e| matches!(e.tag, GROUP_OBJ | GROUP))
        .fold(0, |acc, e| acc | (e.permissions & mask));
    let later = group_union | other;
    let mut aces = Vec::new();
    push_pair(&mut aces, "OWNER@".into(), 0, owner, later & !owner, true);
    for e in access.iter().filter(|e| e.tag == USER) {
        let effective = e.permissions & mask;
        push_pair(
            &mut aces,
            e.id.to_string(),
            0,
            effective,
            later & !effective,
            false,
        );
    }
    push_groups(&mut aces, &access, mask, other);
    aces.push(ace(0, "EVERYONE@".into(), 0, data_mask(other) | BASE));
    aces
}

fn push_groups(aces: &mut Vec<NfsAce>, access: &[PosixEntry], mask: u32, other: u32) {
    let groups: Vec<_> = access
        .iter()
        .filter(|e| matches!(e.tag, GROUP_OBJ | GROUP))
        .collect();
    for e in &groups {
        let who = if e.tag == GROUP_OBJ {
            "GROUP@".into()
        } else {
            e.id.to_string()
        };
        aces.push(ace(
            0,
            who,
            GROUP_FLAG,
            data_mask(e.permissions & mask) | BASE,
        ));
    }
    for e in &groups {
        let denied = other & !(e.permissions & mask);
        if denied == 0 {
            continue;
        }
        let who = if e.tag == GROUP_OBJ {
            "GROUP@".into()
        } else {
            e.id.to_string()
        };
        aces.push(ace(1, who, GROUP_FLAG, data_mask(denied)));
    }
}

fn push_pair(
    aces: &mut Vec<NfsAce>,
    who: String,
    flags: u32,
    permissions: u32,
    denied: u32,
    owner: bool,
) {
    if denied != 0 {
        aces.push(ace(1, who.clone(), flags, data_mask(denied)));
    }
    aces.push(ace(
        0,
        who,
        flags,
        data_mask(permissions) | BASE | if owner { OWNER_EXTRA } else { 0 },
    ));
}

fn ace(ace_type: u8, who: String, flags: u32, permissions: u32) -> NfsAce {
    NfsAce {
        ace_type,
        flags,
        permissions,
        who,
    }
}

fn entry(tag: u32, id: u32, permissions: u32) -> PosixEntry {
    PosixEntry {
        tag,
        id,
        permissions,
    }
}

fn reverse(aces: &[NfsAce], owner: OwnershipMode) -> Result<PosixAcl, AclError> {
    qualify_nfs(aces)?;
    let mut access = Vec::new();
    for ace in aces.iter().filter(|e| e.ace_type == 0) {
        let (tag, id) = principal(ace)?;
        access.push(entry(tag, id, low_permissions(ace.permissions)));
    }
    if access.iter().any(|e| matches!(e.tag, USER | GROUP)) {
        let mut mask = access
            .iter()
            .filter(|e| matches!(e.tag, USER | GROUP | GROUP_OBJ))
            .fold(0, |acc, e| acc | e.permissions);
        // With all raw group/named permissions zero, any nonzero mask keeps
        // their denials effective on Linux without granting additional rights.
        if mask == 0 && permission(&access, OTHER).unwrap_or(0) != 0 {
            mask = 7;
        }
        access.push(entry(MASK, UNDEFINED, mask));
    }
    let candidate = PosixAcl {
        access,
        default: Vec::new(),
    };
    super::codec::validate_entries(&candidate.access)?;
    qualify_posix(&candidate, owner)?;
    if normalize_nfs(canonical(&candidate)) != normalize_nfs(aces.to_vec()) {
        return Err(AclError(
            "NFSv4 ACL is outside the canonical POSIX access template",
        ));
    }
    Ok(candidate)
}

fn qualify_nfs(aces: &[NfsAce]) -> Result<(), AclError> {
    for ace in aces {
        if ace.ace_type > 1 {
            return Err(AclError("audit and alarm ACEs cannot map to POSIX"));
        }
        if ace.flags & !GROUP_FLAG != 0 {
            return Err(AclError(
                "NFSv4 inheritance or unknown ACE flags cannot map to POSIX",
            ));
        }
        if ace.permissions & !(DATA | BASE | OWNER_EXTRA) != 0 {
            return Err(AclError(
                "NFSv4 rights are outside the fixed regular-file access profile",
            ));
        }
        let write = ace.permissions & WRITE;
        if write != 0 && write != WRITE {
            return Err(AclError(
                "partial append or overwrite rights cannot map to POSIX",
            ));
        }
        principal(ace)?;
    }
    Ok(())
}

pub(super) fn principal(ace: &NfsAce) -> Result<(u32, u32), AclError> {
    match ace.who.as_str() {
        "OWNER@" => Ok((USER_OBJ, UNDEFINED)),
        "GROUP@" => Ok((GROUP_OBJ, UNDEFINED)),
        "EVERYONE@" => Ok((OTHER, UNDEFINED)),
        name => {
            let id: u32 = name.parse().map_err(|_| {
                AclError("cross-model named principal requires an explicit numeric identity")
            })?;
            if id == UNDEFINED || id.to_string() != name {
                return Err(AclError("ambiguous numeric ACL identity"));
            }
            Ok((
                if ace.flags & GROUP_FLAG != 0 {
                    GROUP
                } else {
                    USER
                },
                id,
            ))
        }
    }
}
