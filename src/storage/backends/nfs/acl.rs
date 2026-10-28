use crate::model::{AclEncoding, AclMetadata};

const VERSION: u8 = 1;

pub(super) fn encode(value: &nfs_rs::Acl) -> Result<AclMetadata, NfsAclCodecError> {
    let count = u32::try_from(value.aces.len()).map_err(|_| NfsAclCodecError)?;
    let mut output = Vec::new();
    output.push(VERSION);
    output.extend_from_slice(&count.to_le_bytes());
    for ace in &value.aces {
        output.push(match ace.ace_type {
            nfs_rs::AceType::AccessAllowed => 0,
            nfs_rs::AceType::AccessDenied => 1,
            nfs_rs::AceType::SystemAudit => 2,
            nfs_rs::AceType::SystemAlarm => 3,
        });
        output.extend_from_slice(&ace.flags.0.to_le_bytes());
        output.extend_from_slice(&ace.access_mask.0.to_le_bytes());
        let who = ace.who.as_bytes();
        let length = u32::try_from(who.len()).map_err(|_| NfsAclCodecError)?;
        output.extend_from_slice(&length.to_le_bytes());
        output.extend_from_slice(who);
    }
    AclMetadata::new(AclEncoding::NfsV4, output).map_err(|_| NfsAclCodecError)
}

pub(super) fn decode(value: &AclMetadata) -> Result<nfs_rs::Acl, NfsAclCodecError> {
    if value.encoding() != AclEncoding::NfsV4 || value.default_acl().is_some() {
        return Err(NfsAclCodecError);
    }
    let mut input = value.access().ok_or(NfsAclCodecError)?;
    if take_u8(&mut input)? != VERSION {
        return Err(NfsAclCodecError);
    }
    let count = take_u32(&mut input)? as usize;
    if count > 4096 {
        return Err(NfsAclCodecError);
    }
    let mut aces = Vec::with_capacity(count);
    for _ in 0..count {
        let ace_type = match take_u8(&mut input)? {
            0 => nfs_rs::AceType::AccessAllowed,
            1 => nfs_rs::AceType::AccessDenied,
            2 => nfs_rs::AceType::SystemAudit,
            3 => nfs_rs::AceType::SystemAlarm,
            _ => return Err(NfsAclCodecError),
        };
        let flags = nfs_rs::AceFlags(take_u32(&mut input)?);
        let access_mask = nfs_rs::AceMask(take_u32(&mut input)?);
        let length = take_u32(&mut input)? as usize;
        let who = std::str::from_utf8(take(&mut input, length)?)
            .map_err(|_| NfsAclCodecError)?
            .to_owned();
        aces.push(nfs_rs::NfsAce {
            ace_type,
            flags,
            access_mask,
            who,
        });
    }
    if !input.is_empty() {
        return Err(NfsAclCodecError);
    }
    Ok(nfs_rs::Acl { aces })
}

/// The protocol and model keep the complete access/default configuration, including masked bits.
pub(super) fn encode_posix(value: &nfs_rs::PosixAcl) -> Result<AclMetadata, NfsAclCodecError> {
    let entry = |value: &nfs_rs::PosixAclEntry| crate::model::acl::PosixEntry {
        tag: value.tag,
        id: if matches!(value.tag, 2 | 8) {
            value.id
        } else {
            u32::MAX
        },
        permissions: value.permissions,
    };
    crate::model::acl::encode_posix(&crate::model::acl::PosixAcl {
        access: value.access.iter().map(entry).collect(),
        default: value.default.iter().map(entry).collect(),
    })
    .map_err(|_| NfsAclCodecError)
}

pub(super) fn posix_from_mode(mode: u32) -> Vec<nfs_rs::PosixAclEntry> {
    [(1, (mode >> 6) & 7), (4, (mode >> 3) & 7), (32, mode & 7)]
        .into_iter()
        .map(|(tag, permissions)| nfs_rs::PosixAclEntry {
            tag,
            id: u32::MAX,
            permissions,
        })
        .collect()
}

pub(super) fn decode_posix(value: &AclMetadata) -> Result<nfs_rs::PosixAcl, NfsAclCodecError> {
    let mut value = crate::model::acl::decode_posix(value).map_err(|_| NfsAclCodecError)?;
    // Linux SETACL requires canonical tag/qualifier ordering; POSIX configuration order is immaterial.
    value
        .access
        .sort_unstable_by_key(|entry| (entry.tag, entry.id));
    value
        .default
        .sort_unstable_by_key(|entry| (entry.tag, entry.id));
    let entry = |value: crate::model::acl::PosixEntry| nfs_rs::PosixAclEntry {
        tag: value.tag,
        id: value.id,
        permissions: value.permissions,
    };
    Ok(nfs_rs::PosixAcl {
        access: value.access.into_iter().map(entry).collect(),
        default: value.default.into_iter().map(entry).collect(),
    })
}

fn take<'a>(input: &mut &'a [u8], count: usize) -> Result<&'a [u8], NfsAclCodecError> {
    if input.len() < count {
        return Err(NfsAclCodecError);
    }
    let (value, remaining) = input.split_at(count);
    *input = remaining;
    Ok(value)
}

fn take_u8(input: &mut &[u8]) -> Result<u8, NfsAclCodecError> {
    Ok(take(input, 1)?[0])
}

fn take_u32(input: &mut &[u8]) -> Result<u32, NfsAclCodecError> {
    let bytes: [u8; 4] = take(input, 4)?.try_into().map_err(|_| NfsAclCodecError)?;
    Ok(u32::from_le_bytes(bytes))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct NfsAclCodecError;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn nfs_v4_acl_round_trips_in_order_without_principal_loss() {
        let acl = nfs_rs::Acl {
            aces: vec![
                nfs_rs::NfsAce {
                    ace_type: nfs_rs::AceType::AccessAllowed,
                    flags: nfs_rs::AceFlags(3),
                    access_mask: nfs_rs::AceMask(0x12_345),
                    who: "alice@example".to_owned(),
                },
                nfs_rs::NfsAce {
                    ace_type: nfs_rs::AceType::AccessDenied,
                    flags: nfs_rs::AceFlags(7),
                    access_mask: nfs_rs::AceMask(0x54_321),
                    who: "GROUP@example".to_owned(),
                },
            ],
        };
        let encoded = encode(&acl).unwrap_or_else(|_| panic!("ACL encodes"));
        assert_eq!(encoded.encoding(), AclEncoding::NfsV4);
        assert_eq!(decode(&encoded), Ok(acl));
    }

    #[test]
    fn native_posix_codec_preserves_named_masked_permissions_and_defaults() {
        let mut access = posix_from_mode(0o640);
        access.insert(
            1,
            nfs_rs::PosixAclEntry {
                tag: 2,
                id: 42421,
                permissions: 7,
            },
        );
        access.insert(
            3,
            nfs_rs::PosixAclEntry {
                tag: 16,
                id: 0,
                permissions: 4,
            },
        );
        // Linux NFSACL sends real owner/group ids; the xattr format uses undefined ids.
        access[0].id = 12345;
        access[2].id = 12346;
        let value = nfs_rs::PosixAcl {
            access: access.clone(),
            default: access,
        };
        let encoded = encode_posix(&value).unwrap();
        let decoded = decode_posix(&encoded).unwrap();
        assert_eq!(decoded.access[1].permissions, 7);
        assert_eq!(decoded.access[1].id, 42421);
        assert_eq!(decoded.access[3].permissions, 4);
        assert_eq!(decoded.access[0].id, u32::MAX);
        assert_eq!(decoded.access[2].id, u32::MAX);
        assert_eq!(decoded.access, decoded.default);
        let cleared = nfs_rs::PosixAcl {
            access: decoded.access,
            default: vec![],
        };
        assert!(
            decode_posix(&encode_posix(&cleared).unwrap())
                .unwrap()
                .default
                .is_empty()
        );
    }

    #[test]
    fn native_posix_codec_rejects_bad_permissions_and_unknown_tags() {
        for (tag, permissions) in [(2, 8), (3, 4)] {
            let mut access = posix_from_mode(0o640);
            access.push(nfs_rs::PosixAclEntry {
                tag,
                id: 42421,
                permissions,
            });
            assert!(
                encode_posix(&nfs_rs::PosixAcl {
                    access,
                    default: vec![]
                })
                .is_err()
            );
        }
    }

    #[test]
    fn native_posix_set_conversion_orders_valid_entries_for_linux() {
        let mut access = posix_from_mode(0o640);
        access.reverse();
        let encoded = encode_posix(&nfs_rs::PosixAcl {
            access,
            default: vec![],
        })
        .unwrap();
        let decoded = decode_posix(&encoded).unwrap();
        assert_eq!(
            decoded
                .access
                .into_iter()
                .map(|entry| entry.tag)
                .collect::<Vec<_>>(),
            vec![1, 4, 32]
        );
    }

    #[test]
    fn empty_protocol_acl_uses_mode_without_inventing_named_entries() {
        assert_eq!(
            posix_from_mode(0o6751)
                .into_iter()
                .map(|value| (value.tag, value.permissions))
                .collect::<Vec<_>>(),
            vec![(1, 7), (4, 5), (32, 1)]
        );
    }

    #[test]
    fn decoder_rejects_wrong_encoding_truncation_and_trailing_data() {
        let posix =
            AclMetadata::new(AclEncoding::Posix, vec![1]).unwrap_or_else(|error| panic!("{error}"));
        assert!(decode(&posix).is_err());
        let truncated = AclMetadata::new(AclEncoding::NfsV4, vec![VERSION, 1, 0, 0, 0])
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(decode(&truncated).is_err());
        let trailing = AclMetadata::new(AclEncoding::NfsV4, vec![VERSION, 0, 0, 0, 0, 9])
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(decode(&trailing).is_err());
    }
}
