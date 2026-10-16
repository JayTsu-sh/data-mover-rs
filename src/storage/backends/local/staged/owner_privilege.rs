//! Whether this process may give a file to another owner — a fact of the connected Local
//! destination, read once when it connects. `chown(2)` to another user needs `CAP_CHOWN`; without
//! it the owner of a file may still set its own uid and any group it belongs to. Credentials that
//! shrink after connecting bring back the refused write (the file fails), never a silent change.

use std::fs;

use rustix::process::{Gid, getegid, geteuid, getgroups};
#[cfg(target_os = "linux")]
use rustix::thread::{CapabilitySet, capabilities};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct OwnerPrivilege {
    /// May set any owner and group this namespace maps.
    unrestricted: bool,
    uid: u32,
    /// The effective group and the supplementary groups.
    groups: Vec<u32>,
    uid_map: IdMap,
    gid_map: IdMap,
}

impl OwnerPrivilege {
    /// The current process's privilege. A failed group lookup leaves only the effective group,
    /// which errs toward reporting a loss rather than a refused write.
    pub(super) fn current() -> Self {
        let uid = geteuid();
        let mut groups = vec![getegid().as_raw()];
        groups.extend(getgroups().unwrap_or_default().into_iter().map(Gid::as_raw));
        Self {
            unrestricted: unrestricted(cap_chown(), uid.is_root()),
            uid: uid.as_raw(),
            groups,
            uid_map: IdMap::read("/proc/self/uid_map"),
            gid_map: IdMap::read("/proc/self/gid_map"),
        }
    }

    /// Whether a file this process writes may be given `uid` and `gid`.
    pub(super) fn permits(&self, uid: u32, gid: u32) -> bool {
        let own = uid == self.uid && self.groups.contains(&gid);
        (self.unrestricted || own) && self.uid_map.maps(uid) && self.gid_map.maps(gid)
    }
}

/// On Linux only `CAP_CHOWN` in the effective set lets a process chown to another user — root
/// without it (a container that dropped capabilities) is refused like anyone else. Where the
/// capability cannot be read, or there are no capabilities, root is the answer.
const fn unrestricted(cap_chown: Option<bool>, root: bool) -> bool {
    match cap_chown {
        Some(held) => held,
        None => root,
    }
}

#[cfg(target_os = "linux")]
fn cap_chown() -> Option<bool> {
    capabilities(None)
        .ok()
        .map(|sets| sets.effective.contains(CapabilitySet::CHOWN))
}

#[cfg(not(target_os = "linux"))]
const fn cap_chown() -> Option<bool> {
    None
}

/// The ids a user namespace maps (`/proc/self/{uid,gid}_map`). Outside it the kernel refuses
/// the id outright (`EINVAL`), whatever the capabilities. `None` — no map to read — maps all.
#[derive(Clone, Debug, Eq, PartialEq)]
struct IdMap(Option<Vec<(u32, u32)>>);

impl IdMap {
    fn read(path: &str) -> Self {
        Self(
            fs::read_to_string(path)
                .ok()
                .map(|text| parse_id_map(&text)),
        )
    }

    fn maps(&self, id: u32) -> bool {
        self.0.as_ref().is_none_or(|ranges| {
            ranges
                .iter()
                .any(|&(start, count)| id >= start && id - start < count)
        })
    }
}

/// `inside outside count` per line; the inside ranges are the ids this namespace can name.
fn parse_id_map(text: &str) -> Vec<(u32, u32)> {
    text.lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace().map(str::parse::<u32>);
            let start = fields.next()?.ok()?;
            let _outside = fields.next()?.ok()?;
            let count = fields.next()?.ok()?;
            Some((start, count))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: IdMap = IdMap(None);

    fn unprivileged() -> OwnerPrivilege {
        OwnerPrivilege {
            unrestricted: false,
            uid: 1000,
            groups: vec![1000, 10],
            uid_map: ALL,
            gid_map: ALL,
        }
    }

    #[test]
    fn an_unprivileged_writer_keeps_its_own_files_and_its_groups() {
        let privilege = unprivileged();
        assert!(privilege.permits(1000, 1000));
        assert!(privilege.permits(1000, 10), "a supplementary group");
        assert!(!privilege.permits(1000, 0), "a group it is not in");
        assert!(!privilege.permits(0, 1000), "another owner");
    }

    #[test]
    fn a_privileged_writer_may_set_any_owner() {
        let privilege = OwnerPrivilege {
            unrestricted: true,
            ..unprivileged()
        };
        assert!(privilege.permits(0, 0));
    }

    /// Root in a container that dropped `CAP_CHOWN` is refused by the kernel; only the
    /// capability counts where it can be read.
    #[test]
    fn root_without_cap_chown_is_not_unrestricted() {
        assert!(!unrestricted(Some(false), true));
        assert!(unrestricted(Some(true), false));
        assert!(
            unrestricted(None, true),
            "no capabilities to read: root decides"
        );
        assert!(!unrestricted(None, false));
    }

    /// A rootless container: capable inside, but an id the namespace does not map is refused.
    #[test]
    fn an_id_outside_the_namespace_is_never_settable() {
        let map = IdMap(Some(parse_id_map(
            "         0       1000          1\n         1     100000      65536\n",
        )));
        let privilege = OwnerPrivilege {
            unrestricted: true,
            uid_map: map.clone(),
            gid_map: map,
            ..unprivileged()
        };
        assert!(privilege.permits(0, 0));
        assert!(privilege.permits(65536, 65536));
        assert!(!privilege.permits(65537, 0));
        assert!(!privilege.permits(0, 70000));
    }

    #[test]
    fn the_current_process_may_keep_its_own_owner() {
        let current = OwnerPrivilege::current();
        assert!(current.permits(geteuid().as_raw(), getegid().as_raw()));
    }
}
