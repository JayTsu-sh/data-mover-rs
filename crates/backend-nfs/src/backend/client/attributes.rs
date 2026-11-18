//! `NFSv4` 以返回位图证明字段存在；`NFSv3` 的固定 `fattr3` 字段始终存在。
use nfs_rs::Attr;

use super::owner_id;
use crate::storage::InlineOwnership;
use crate::storage::backends::nfs::namespace::inline_ownership;

pub(super) fn present(attrs: &Attr, bit: u32) -> bool {
    attrs.nfs4_attributes.is_none_or(|mask| {
        mask.get((bit / 32) as usize)
            .is_some_and(|word| word & (1 << (bit % 32)) != 0)
    })
}

pub(super) fn ownership_available(attrs: &Attr) -> bool {
    [33, 36, 37].into_iter().all(|bit| present(attrs, bit))
}

pub(super) fn uid(attrs: &Attr, strings: bool) -> Option<u32> {
    present(attrs, 36)
        .then(|| owner_id(&attrs.owner, attrs.uid, strings))
        .flatten()
}

pub(super) fn gid(attrs: &Attr, strings: bool) -> Option<u32> {
    present(attrs, 37)
        .then(|| owner_id(&attrs.owner_group, attrs.gid, strings))
        .flatten()
}

pub(super) fn ownership(attrs: &Attr, strings: bool) -> Option<InlineOwnership> {
    ownership_available(attrs)
        .then(|| inline_ownership(uid(attrs, strings), gid(attrs, strings), attrs.file_mode))
}
