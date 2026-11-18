//! 使用生产 `GLOBAL_CACHE` 验证 fresh 的局部整理，不模拟属性写入或网络成功。
use std::path::PathBuf;

use bytes::Bytes;

use super::{CACHE_WRITES, GLOBAL_CACHE, NFSStorage, reconcile_namespace_cache};

fn scope() -> Bytes {
    Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes())
}

fn key(path: &str, root: &Bytes) -> (PathBuf, Bytes) {
    (PathBuf::from(path), root.clone())
}

#[test]
fn fresh_directory_binding_updates_the_handle_used_by_cached_metadata_lookup() {
    let root = scope();
    let other_root = scope();
    for (path, root) in [
        ("a", &root),
        ("a/b", &root),
        ("a/b/child", &root),
        ("a/bc", &root),
        ("a/b", &other_root),
    ] {
        NFSStorage::cache_directory(key(path, root), &Bytes::from_static(b"old"));
    }
    reconcile_namespace_cache(
        PathBuf::from("a/b"),
        root.clone(),
        &root,
        &Bytes::from_static(b"new"),
        true,
    );
    // lookup_fh_inner 的完整目录 cache key 就是该 key；后续 stat/SETATTR 不能再命中 old。
    assert_eq!(
        GLOBAL_CACHE.get(&key("a/b", &root)),
        Some(Bytes::from_static(b"new"))
    );
    assert!(GLOBAL_CACHE.get(&key("a/b/child", &root)).is_none());
    assert_eq!(
        GLOBAL_CACHE.get(&key("a", &root)),
        Some(Bytes::from_static(b"old"))
    );
    assert_eq!(
        GLOBAL_CACHE.get(&key("a/bc", &root)),
        Some(Bytes::from_static(b"old"))
    );
    assert_eq!(
        GLOBAL_CACHE.get(&key("a/b", &other_root)),
        Some(Bytes::from_static(b"old"))
    );
}

#[test]
fn fresh_uncached_directory_retires_orphaned_descendant_handles() {
    let root = scope();
    NFSStorage::cache_directory(
        key("parent/child", &root),
        &Bytes::from_static(b"stale-child"),
    );
    reconcile_namespace_cache(
        PathBuf::from("parent"),
        root.clone(),
        &root,
        &Bytes::from_static(b"current-parent"),
        true,
    );
    assert!(GLOBAL_CACHE.get(&key("parent/child", &root)).is_none());
    assert_eq!(
        GLOBAL_CACHE.get(&key("parent", &root)),
        Some(Bytes::from_static(b"current-parent"))
    );
}

#[test]
fn fresh_unchanged_directory_keeps_descendants_without_extra_cache_writes() {
    let root = scope();
    let current = Bytes::from_static(b"current");
    NFSStorage::cache_directory(key("parent", &root), &current);
    NFSStorage::cache_directory(key("parent/child", &root), &current);
    let before = CACHE_WRITES.with(std::cell::Cell::get);
    reconcile_namespace_cache(PathBuf::from("parent"), root.clone(), &root, &current, true);
    assert_eq!(CACHE_WRITES.with(std::cell::Cell::get), before);
    assert_eq!(GLOBAL_CACHE.get(&key("parent/child", &root)), Some(current));
}

#[test]
fn fresh_file_replacing_a_cached_directory_forgets_old_directory_handles() {
    let root = scope();
    NFSStorage::cache_directory(key("changed", &root), &Bytes::from_static(b"directory"));
    NFSStorage::cache_directory(key("changed/child", &root), &Bytes::from_static(b"child"));
    reconcile_namespace_cache(
        PathBuf::from("changed"),
        root.clone(),
        &root,
        &Bytes::from_static(b"file"),
        false,
    );
    assert!(GLOBAL_CACHE.get(&key("changed", &root)).is_none());
    assert!(GLOBAL_CACHE.get(&key("changed/child", &root)).is_none());
}

#[test]
fn fresh_regular_file_does_not_populate_the_directory_cache() {
    let root = scope();
    let before = CACHE_WRITES.with(std::cell::Cell::get);
    reconcile_namespace_cache(
        PathBuf::from("file"),
        root.clone(),
        &root,
        &Bytes::from_static(b"file"),
        false,
    );
    assert_eq!(CACHE_WRITES.with(std::cell::Cell::get), before);
    assert!(GLOBAL_CACHE.get(&key("file", &root)).is_none());
}

#[test]
fn changed_root_forgets_only_its_scope_without_rebinding_the_anchor() {
    let root = scope();
    let other = scope();
    let anchor = Bytes::from_static(b"bound-root");
    NFSStorage::cache_directory(key("child", &root), &Bytes::from_static(b"old"));
    NFSStorage::cache_directory(key("child", &other), &Bytes::from_static(b"other"));
    let remains_bound = reconcile_namespace_cache(
        PathBuf::new(),
        root.clone(),
        &anchor,
        &Bytes::from_static(b"new-root"),
        true,
    );
    assert!(!remains_bound);
    assert_eq!(anchor, Bytes::from_static(b"bound-root"));
    assert!(GLOBAL_CACHE.get(&key("child", &root)).is_none());
    assert!(GLOBAL_CACHE.get(&key("", &root)).is_none());
    assert_eq!(
        GLOBAL_CACHE.get(&key("child", &other)),
        Some(Bytes::from_static(b"other"))
    );
}

#[test]
fn unchanged_root_keeps_its_bound_anchor_and_cached_descendants() {
    let root = scope();
    let anchor = Bytes::from_static(b"bound-root");
    NFSStorage::cache_directory(key("child", &root), &Bytes::from_static(b"child"));
    let writes = CACHE_WRITES.with(std::cell::Cell::get);
    assert!(reconcile_namespace_cache(
        PathBuf::new(),
        root.clone(),
        &anchor,
        &anchor,
        true
    ));
    assert_eq!(CACHE_WRITES.with(std::cell::Cell::get), writes);
    assert_eq!(
        GLOBAL_CACHE.get(&key("child", &root)),
        Some(Bytes::from_static(b"child"))
    );
}
