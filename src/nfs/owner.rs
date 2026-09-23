//! The uid / gid behind an NFS owner, or none when the number would be made up.

/// The uid or gid nfs-rs parsed for one entry, or `None` when that number is not what the server
/// said.
///
/// `NFSv3` sends numbers, and nfs-rs's are exact. `NFSv4` sends strings (`owner` / `owner_group`).
/// nfs-rs 0.8.4 parses numeric strings (`1000`, `1000@domain`) and `root`, turns every other name
/// into 65534 (nobody) without an error, and leaves an attribute the server did not send as an
/// empty string with id 0 — both are RECOMMENDED attributes, not mandatory. ONTAP sends
/// `name@domain` whenever the SVM can map the id (verified on FAS2750: uid 0 → `root@localdomain`,
/// unmapped uids as numeric strings), so behind a directory service the parsed value would chown
/// every copied file to nobody.
pub(crate) fn owner_id(raw: &str, parsed: u32, owners_are_strings: bool) -> Option<u32> {
    if !owners_are_strings {
        return Some(parsed);
    }
    let name = raw.split_once('@').map_or(raw, |(name, _)| name);
    (!name.is_empty() && (name.parse::<u32>().is_ok() || name == "root")).then_some(parsed)
}

#[cfg(test)]
mod tests {
    use super::owner_id;

    /// What ONTAP sends (FAS2750, 2026-09-23) and what nfs-rs makes of it: a number, `N@domain`
    /// and `root` are exact; any other name is a 65534 nfs-rs made up, and an absent attribute
    /// is a 0 it made up. Neither may become the owner.
    #[test]
    fn nfsv4_owners_nfs_rs_did_not_really_parse_are_not_ids() {
        for (raw, parsed) in [
            ("1000", 1000),
            ("0", 0),
            ("4242@example.com", 4242),
            ("1000@a@b", 1000),
            ("root", 0),
            ("root@localdomain", 0),
        ] {
            assert_eq!(owner_id(raw, parsed, true), Some(parsed), "{raw}");
        }
        for (raw, parsed) in [
            ("alice@example.com", 65534),
            ("nobody@localdomain", 65534),
            ("ROOT@x", 65534),
            ("@domain", 65534),
            ("4294967296", 65534),
            // The server did not send the attribute: nfs-rs leaves `""` and 0.
            ("", 0),
        ] {
            assert_eq!(owner_id(raw, parsed, true), None, "{raw}");
        }
    }

    /// `NFSv3` sends numbers and has no string: whatever the id is, it is the server's.
    #[test]
    fn nfsv3_ids_are_taken_as_sent() {
        assert_eq!(owner_id("", 65534, false), Some(65534));
        assert_eq!(owner_id("", 0, false), Some(0));
    }

    /// The legacy listing entry: an owner nfs-rs could not map is `None`, not 65534, and the mode
    /// every legacy consumer applies without a chown loses the set-id bit that owner came with.
    #[test]
    fn a_legacy_entry_does_not_keep_set_id_bits_for_an_owner_it_lacks() {
        let attrs = nfs_rs::Attr {
            type_: super::super::FType3::NF3REG as u32,
            file_mode: 0o104_755,
            uid: 65534,
            gid: 100,
            owner: "alice@example.com".to_owned(),
            owner_group: "100".to_owned(),
            ..nfs_rs::Attr::default()
        };
        let entry = crate::NASEntry::from_nfs_attrs(
            "tool".to_owned(),
            std::path::PathBuf::from("tool"),
            None,
            &attrs,
            bytes::Bytes::new(),
            super::super::NfsEnrich::default(),
            true,
        );
        assert_eq!(entry.uid, None);
        assert_eq!(entry.gid, Some(100));
        assert_eq!(entry.mode, 0o100_755);
    }
}
