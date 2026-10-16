//! The set-id bits a copy may carry when it does not carry the owner they belong to.

/// `mode` without the set-id bits whose owner is not being carried.
///
/// A file whose owner or group the source could not give (an `NFSv4` name nfs-rs cannot map, a
/// source with no numeric owner) ends up owned by whoever writes it — root, when the copy runs as
/// root. Carrying setuid with an unknown uid, or setgid on a file with an unknown gid, would turn
/// someone else's program into one that runs as that writer. Setgid on a directory only makes new
/// children inherit the directory's group and grants nothing, so it is kept.
pub(crate) const fn without_unowned_set_id(
    mode: u32,
    uid_known: bool,
    gid_known: bool,
    is_dir: bool,
) -> u32 {
    let mut mode = mode;
    if !uid_known {
        mode &= !0o4000;
    }
    if !gid_known && !is_dir {
        mode &= !0o2000;
    }
    mode
}

#[cfg(test)]
mod tests {
    use super::without_unowned_set_id;

    #[test]
    fn set_id_bits_go_with_the_owner_they_belong_to() {
        // Both known: nothing changes.
        assert_eq!(without_unowned_set_id(0o6755, true, true, false), 0o6755);
        // Unknown uid: setuid goes, setgid stays with its known group.
        assert_eq!(without_unowned_set_id(0o6755, false, true, false), 0o2755);
        // Unknown gid on a file: setgid goes, setuid stays with its known owner.
        assert_eq!(without_unowned_set_id(0o6755, true, false, false), 0o4755);
        // Neither known: both go on a file.
        assert_eq!(without_unowned_set_id(0o6755, false, false, false), 0o0755);
        // A directory keeps setgid (group inheritance, no privilege) even with an unknown gid.
        assert_eq!(without_unowned_set_id(0o2775, false, false, true), 0o2775);
        // File type bits above the permission bits are untouched.
        assert_eq!(
            without_unowned_set_id(0o104_755, false, true, false),
            0o100_755
        );
    }
}
