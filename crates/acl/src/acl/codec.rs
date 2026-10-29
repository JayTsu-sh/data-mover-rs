use std::collections::BTreeSet;
use std::str::from_utf8;

use super::{AclEncoding, AclError, AclMetadata};

pub const USER_OBJ: u32 = 1;
pub const USER: u32 = 2;
pub const GROUP_OBJ: u32 = 4;
pub const GROUP: u32 = 8;
pub const MASK: u32 = 16;
pub const OTHER: u32 = 32;
pub const UNDEFINED: u32 = u32::MAX;
const MAX_ENTRIES: usize = 4096;

/// A POSIX ACL entry, using Linux tag values and an undefined base qualifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct PosixEntry {
    pub tag: u32,
    pub id: u32,
    pub permissions: u32,
}

/// Complete access/default observations. Empty vectors mean confirmed absence.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PosixAcl {
    pub access: Vec<PosixEntry>,
    pub default: Vec<PosixEntry>,
}

/// One ordered ACE from the version-1 `NFSv4` native metadata codec.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NfsAce {
    pub ace_type: u8,
    pub flags: u32,
    pub permissions: u32,
    pub who: String,
}

/// Decode Linux POSIX xattr version 2, preserving raw permissions and mask.
///
/// # Errors
/// Rejects invalid tags, qualifiers, permissions, structure, or encoding.
pub fn decode_posix(value: &AclMetadata) -> Result<PosixAcl, AclError> {
    if value.encoding() != AclEncoding::Posix {
        return Err(AclError("expected POSIX ACL encoding"));
    }
    Ok(PosixAcl {
        access: decode_entries(value.access())?,
        default: decode_entries(value.default_acl())?,
    })
}

/// Encode complete POSIX observations in the Linux xattr representation.
///
/// # Errors
/// Rejects malformed ACLs or payloads exceeding the model bound.
pub fn encode_posix(value: &PosixAcl) -> Result<AclMetadata, AclError> {
    AclMetadata::new_posix(
        encode_entries(&value.access)?,
        encode_entries(&value.default)?,
    )
    .map_err(|_| AclError("POSIX ACL exceeds metadata bounds"))
}

fn decode_entries(input: Option<&[u8]>) -> Result<Vec<PosixEntry>, AclError> {
    let Some(mut input) = input else {
        return Ok(Vec::new());
    };
    if take_u32(&mut input)? != 2 || input.len() % 8 != 0 || input.len() / 8 > MAX_ENTRIES {
        return Err(AclError("invalid POSIX ACL xattr header or length"));
    }
    let mut entries = Vec::with_capacity(input.len() / 8);
    while !input.is_empty() {
        entries.push(PosixEntry {
            tag: u32::from(take_u16(&mut input)?),
            permissions: u32::from(take_u16(&mut input)?),
            id: take_u32(&mut input)?,
        });
    }
    if entries.is_empty() {
        return Err(AclError(
            "empty POSIX ACL payload must be represented as absent",
        ));
    }
    validate_entries(&entries)?;
    Ok(entries)
}

fn encode_entries(entries: &[PosixEntry]) -> Result<Option<Vec<u8>>, AclError> {
    validate_entries(entries)?;
    if entries.is_empty() {
        return Ok(None);
    }
    let mut output = 2_u32.to_le_bytes().to_vec();
    for entry in entries {
        let tag = u16::try_from(entry.tag).map_err(|_| AclError("invalid POSIX tag"))?;
        let permissions =
            u16::try_from(entry.permissions).map_err(|_| AclError("invalid POSIX permissions"))?;
        output.extend_from_slice(&tag.to_le_bytes());
        output.extend_from_slice(&permissions.to_le_bytes());
        output.extend_from_slice(&entry.id.to_le_bytes());
    }
    Ok(Some(output))
}

pub fn validate_entries(entries: &[PosixEntry]) -> Result<(), AclError> {
    if entries.is_empty() {
        return Ok(());
    }
    if entries.len() > MAX_ENTRIES {
        return Err(AclError("too many POSIX ACL entries"));
    }
    let mut seen = BTreeSet::new();
    for entry in entries {
        if entry.permissions > 7
            || !matches!(
                entry.tag,
                USER_OBJ | USER | GROUP_OBJ | GROUP | MASK | OTHER
            )
        {
            return Err(AclError("invalid POSIX ACL tag or permissions"));
        }
        let named = matches!(entry.tag, USER | GROUP);
        if named == (entry.id == UNDEFINED) || !seen.insert((entry.tag, entry.id)) {
            return Err(AclError("invalid or duplicate POSIX ACL qualifier"));
        }
    }
    for tag in [USER_OBJ, GROUP_OBJ, OTHER] {
        if !seen.contains(&(tag, UNDEFINED)) {
            return Err(AclError("POSIX ACL lacks an owner, group, or other entry"));
        }
    }
    if entries
        .iter()
        .any(|entry| matches!(entry.tag, USER | GROUP))
        && !seen.contains(&(MASK, UNDEFINED))
    {
        return Err(AclError("extended POSIX ACL lacks a mask"));
    }
    Ok(())
}

/// Decode the ordered `NFSv4` native metadata representation.
///
/// # Errors
/// Rejects invalid versions, entry types, UTF-8, trailing bytes, or bounds.
pub fn decode_nfs4(value: &AclMetadata) -> Result<Vec<NfsAce>, AclError> {
    if value.encoding() != AclEncoding::NfsV4 || value.default_acl().is_some() {
        return Err(AclError("expected one NFSv4 ACL attribute"));
    }
    let mut input = value
        .access()
        .ok_or(AclError("missing NFSv4 ACL payload"))?;
    if take(&mut input, 1)? != [1] {
        return Err(AclError("unsupported NFSv4 ACL codec version"));
    }
    let count = take_u32(&mut input)? as usize;
    if count > MAX_ENTRIES {
        return Err(AclError("too many NFSv4 ACEs"));
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let ace_type = take(&mut input, 1)?[0];
        if ace_type > 3 {
            return Err(AclError("invalid NFSv4 ACE type"));
        }
        let flags = take_u32(&mut input)?;
        let permissions = take_u32(&mut input)?;
        let length = take_u32(&mut input)? as usize;
        let who = from_utf8(take(&mut input, length)?)
            .map_err(|_| AclError("invalid NFSv4 principal UTF-8"))?
            .to_owned();
        entries.push(NfsAce {
            ace_type,
            flags,
            permissions,
            who,
        });
    }
    if !input.is_empty() {
        return Err(AclError("trailing NFSv4 ACL bytes"));
    }
    Ok(entries)
}

/// Encode ordered ACEs without sorting or dropping flags and permission bits.
///
/// # Errors
/// Rejects invalid types or bounds.
pub fn encode_nfs4(entries: &[NfsAce]) -> Result<AclMetadata, AclError> {
    if entries.len() > MAX_ENTRIES {
        return Err(AclError("too many NFSv4 ACEs"));
    }
    let mut output = vec![1];
    let count = u32::try_from(entries.len()).map_err(|_| AclError("too many NFSv4 ACEs"))?;
    output.extend_from_slice(&count.to_le_bytes());
    for entry in entries {
        if entry.ace_type > 3 {
            return Err(AclError("invalid NFSv4 ACE type"));
        }
        let length =
            u32::try_from(entry.who.len()).map_err(|_| AclError("NFSv4 principal too long"))?;
        output.push(entry.ace_type);
        output.extend_from_slice(&entry.flags.to_le_bytes());
        output.extend_from_slice(&entry.permissions.to_le_bytes());
        output.extend_from_slice(&length.to_le_bytes());
        output.extend_from_slice(entry.who.as_bytes());
    }
    AclMetadata::new(AclEncoding::NfsV4, output)
        .map_err(|_| AclError("NFSv4 ACL exceeds metadata bounds"))
}

fn take<'a>(input: &mut &'a [u8], count: usize) -> Result<&'a [u8], AclError> {
    if input.len() < count {
        return Err(AclError("truncated ACL payload"));
    }
    let (value, remaining) = input.split_at(count);
    *input = remaining;
    Ok(value)
}

fn take_u16(input: &mut &[u8]) -> Result<u16, AclError> {
    let bytes = take(input, 2)?
        .try_into()
        .map_err(|_| AclError("truncated ACL integer"))?;
    Ok(u16::from_le_bytes(bytes))
}

fn take_u32(input: &mut &[u8]) -> Result<u32, AclError> {
    let bytes = take(input, 4)?
        .try_into()
        .map_err(|_| AclError("truncated ACL integer"))?;
    Ok(u32::from_le_bytes(bytes))
}
