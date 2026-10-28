use nfs_rs::{AceFlags, AceMask, AceType, NfsAce as ProtocolAce};

use crate::model::AclMetadata;
use crate::model::acl::{self, NfsAce, PosixAcl, PosixEntry};

pub(super) fn encode(value: &nfs_rs::Acl) -> Result<AclMetadata, NfsAclCodecError> {
    let entries: Vec<_> = value
        .aces
        .iter()
        .map(|ace| NfsAce {
            ace_type: match ace.ace_type {
                AceType::AccessAllowed => 0,
                AceType::AccessDenied => 1,
                AceType::SystemAudit => 2,
                AceType::SystemAlarm => 3,
            },
            flags: ace.flags.0,
            permissions: ace.access_mask.0,
            who: ace.who.clone(),
        })
        .collect();
    acl::encode_nfs4(&entries).map_err(|_| NfsAclCodecError)
}

pub(super) fn decode(value: &AclMetadata) -> Result<nfs_rs::Acl, NfsAclCodecError> {
    let entries = acl::decode_nfs4(value).map_err(|_| NfsAclCodecError)?;
    let aces = entries
        .into_iter()
        .map(|ace| {
            Ok(ProtocolAce {
                ace_type: match ace.ace_type {
                    0 => AceType::AccessAllowed,
                    1 => AceType::AccessDenied,
                    2 => AceType::SystemAudit,
                    3 => AceType::SystemAlarm,
                    _ => return Err(NfsAclCodecError),
                },
                flags: AceFlags(ace.flags),
                access_mask: AceMask(ace.permissions),
                who: ace.who,
            })
        })
        .collect::<Result<_, _>>()?;
    Ok(nfs_rs::Acl { aces })
}

/// The protocol and model keep the complete access/default configuration, including masked bits.
pub(super) fn encode_posix(value: &nfs_rs::PosixAcl) -> Result<AclMetadata, NfsAclCodecError> {
    let entry = |value: &nfs_rs::PosixAclEntry| PosixEntry {
        tag: value.tag,
        id: if matches!(value.tag, 2 | 8) {
            value.id
        } else {
            u32::MAX
        },
        permissions: value.permissions,
    };
    acl::encode_posix(&PosixAcl {
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
    let mut value = acl::decode_posix(value).map_err(|_| NfsAclCodecError)?;
    // Linux SETACL requires canonical tag/qualifier ordering; POSIX configuration order is immaterial.
    value
        .access
        .sort_unstable_by_key(|entry| (entry.tag, entry.id));
    value
        .default
        .sort_unstable_by_key(|entry| (entry.tag, entry.id));
    let entry = |value: PosixEntry| nfs_rs::PosixAclEntry {
        tag: value.tag,
        id: value.id,
        permissions: value.permissions,
    };
    Ok(nfs_rs::PosixAcl {
        access: value.access.into_iter().map(entry).collect(),
        default: value.default.into_iter().map(entry).collect(),
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct NfsAclCodecError;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::model::AclEncoding;

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
        // Established version-1 bytes remain readable after centralizing the codec.
        let expected = [
            &[1, 2, 0, 0, 0, 0, 3, 0, 0, 0, 0x45, 0x23, 1, 0, 13, 0, 0, 0][..],
            b"alice@example",
            &[1, 7, 0, 0, 0, 0x21, 0x43, 5, 0, 13, 0, 0, 0],
            b"GROUP@example",
        ]
        .concat();
        assert_eq!(encoded.access(), Some(expected.as_slice()));
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
        let truncated = AclMetadata::new(AclEncoding::NfsV4, vec![1, 1, 0, 0, 0])
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(decode(&truncated).is_err());
        let trailing = AclMetadata::new(AclEncoding::NfsV4, vec![1, 0, 0, 0, 0, 9])
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(decode(&trailing).is_err());
    }
}
