use super::*;
use crate::{LocalStorage, NASEntry};
use std::path::{Path, PathBuf};

fn nas_entry(path: &str, size: u64) -> EntryEnum {
    EntryEnum::NAS(NASEntry {
        name: path.to_string(),
        relative_path: PathBuf::from(path),
        extension: None,
        is_dir: false,
        size,
        atime: 10,
        ctime: 20,
        mtime: 30,
        mode: 0o100_640,
        is_symlink: false,
        hard_links: Some(1),
        uid: Some(1000),
        gid: Some(1000),
        ino: None,
        file_handle: None,
        acl: None,
        owner: None,
        owner_group: None,
        xattrs: None,
    })
}

fn local(root: &Path) -> StorageEnum {
    StorageEnum::Local(LocalStorage::new(root, None))
}

fn local_with_block(root: &Path, block_size: u64) -> StorageEnum {
    StorageEnum::Local(LocalStorage::new(root, Some(block_size)))
}

fn test_roots(name: &str) -> (PathBuf, PathBuf) {
    let nonce = crate::time_util::now_nanos();
    let base = std::env::temp_dir().join(format!("data-mover-integrity-{name}-{nonce}"));
    (base.join("src"), base.join("dest"))
}

fn write_matching_files(src_root: &Path, dest_root: &Path, data: &[u8]) {
    std::fs::create_dir_all(src_root).unwrap();
    std::fs::create_dir_all(dest_root).unwrap();
    let src = src_root.join("item");
    let dest = dest_root.join("item");
    std::fs::write(&src, data).unwrap();
    std::fs::write(&dest, data).unwrap();
    let mtime = filetime::FileTime::from_unix_time(1_700_000_000, 123_456_789);
    filetime::set_file_mtime(src, mtime).unwrap();
    filetime::set_file_mtime(dest, mtime).unwrap();
}

#[tokio::test]
async fn quick_reports_entry_kind_before_metadata() {
    let root = std::env::temp_dir();
    let src = nas_entry("item", 0);
    let mut dest = nas_entry("item", 0);
    let EntryEnum::NAS(dest_fields) = &mut dest else {
        unreachable!()
    };
    dest_fields.is_dir = true;

    let result = IntegrityCheck::check(
        &local(&root),
        &local(&root),
        &src,
        &dest,
        IntegrityCheckMode::Quick,
        None,
    )
    .await;

    assert!(matches!(
        result,
        Err(StorageError::MismatchData(fields))
            if fields == vec![MismatchDataField::EntryKind {
                src: IntegrityEntryKind::File,
                dest: IntegrityEntryKind::Directory,
            }]
    ));
}

#[tokio::test]
async fn quick_reports_size_mismatch() {
    let root = std::env::temp_dir();
    let result = IntegrityCheck::check(
        &local(&root),
        &local(&root),
        &nas_entry("item", 10),
        &nas_entry("item", 11),
        IntegrityCheckMode::Quick,
        None,
    )
    .await;

    assert!(matches!(
        result,
        Err(StorageError::MismatchData(fields))
            if fields == vec![MismatchDataField::Size { src: 10, dest: 11 }]
    ));
}

#[tokio::test]
async fn quick_reports_all_supported_metadata_mismatches() {
    let root = std::env::temp_dir();
    let src = nas_entry("item", 10);
    let mut dest = nas_entry("item", 10);
    let EntryEnum::NAS(dest_fields) = &mut dest else {
        unreachable!()
    };
    dest_fields.mtime = 31;
    dest_fields.uid = Some(1001);
    dest_fields.gid = Some(1002);
    dest_fields.mode = 0o100_600;

    let result = IntegrityCheck::check(
        &local(&root),
        &local(&root),
        &src,
        &dest,
        IntegrityCheckMode::Quick,
        None,
    )
    .await;

    assert!(matches!(
        result,
        Err(StorageError::MismatchMeta(fields)) if fields.len() == 4
    ));
}

#[tokio::test]
async fn symlink_mode_is_not_compared() {
    let root = std::env::temp_dir();
    let mut src = nas_entry("link", 0);
    let mut dest = nas_entry("link", 0);
    let EntryEnum::NAS(src_fields) = &mut src else {
        unreachable!()
    };
    let EntryEnum::NAS(dest_fields) = &mut dest else {
        unreachable!()
    };
    src_fields.is_symlink = true;
    dest_fields.is_symlink = true;
    dest_fields.mode = 0o777;

    let result = IntegrityCheck::check(
        &local(&root),
        &local(&root),
        &src,
        &dest,
        IntegrityCheckMode::Quick,
        None,
    )
    .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn full_compares_streams_with_different_chunk_boundaries() {
    let (src_root, dest_root) = test_roots("match");
    std::fs::create_dir_all(&src_root).unwrap();
    std::fs::create_dir_all(&dest_root).unwrap();
    std::fs::write(src_root.join("item"), b"same bytes").unwrap();
    std::fs::write(dest_root.join("item"), b"same bytes").unwrap();
    let entry = nas_entry("item", 10);

    let result = IntegrityCheck::check(
        &local_with_block(&src_root, 3),
        &local_with_block(&dest_root, 7),
        &entry,
        &entry,
        IntegrityCheckMode::Full,
        None,
    )
    .await;

    assert!(result.is_ok());
    std::fs::remove_dir_all(src_root.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn full_reports_first_mismatching_offset() {
    let (src_root, dest_root) = test_roots("mismatch");
    std::fs::create_dir_all(&src_root).unwrap();
    std::fs::create_dir_all(&dest_root).unwrap();
    std::fs::write(src_root.join("item"), b"same-prefix").unwrap();
    std::fs::write(dest_root.join("item"), b"same-Xrefix").unwrap();
    let entry = nas_entry("item", 11);

    let result = IntegrityCheck::check(
        &local_with_block(&src_root, 3),
        &local_with_block(&dest_root, 5),
        &entry,
        &entry,
        IntegrityCheckMode::Full,
        None,
    )
    .await;

    assert!(matches!(
        result,
        Err(StorageError::MismatchData(fields))
            if fields == vec![MismatchDataField::Content { offset: 5 }]
    ));
    std::fs::remove_dir_all(src_root.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn full_rejects_equal_short_prefixes() {
    let (src_root, dest_root) = test_roots("short");
    std::fs::create_dir_all(&src_root).unwrap();
    std::fs::create_dir_all(&dest_root).unwrap();
    std::fs::write(src_root.join("item"), b"prefix").unwrap();
    std::fs::write(dest_root.join("item"), b"prefix").unwrap();
    let entry = nas_entry("item", 10);

    let result = IntegrityCheck::check(
        &local(&src_root),
        &local(&dest_root),
        &entry,
        &entry,
        IntegrityCheckMode::Full,
        None,
    )
    .await;

    assert!(matches!(
        result,
        Err(StorageError::MismatchData(fields))
            if fields == vec![MismatchDataField::ReadLength {
                side: IntegritySide::Source,
                expected: 10,
                actual: 6,
            }]
    ));
    std::fs::remove_dir_all(src_root.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn cancelled_full_check_does_not_start_io() {
    let root = std::env::temp_dir();
    let token = CancellationToken::new();
    token.cancel();

    let result = IntegrityCheck::check(
        &local(&root),
        &local(&root),
        &nas_entry("missing", 1),
        &nas_entry("missing", 1),
        IntegrityCheckMode::Full,
        Some(&token),
    )
    .await;

    assert!(matches!(result, Err(StorageError::Cancelled)));
}

#[tokio::test]
async fn check_path_resolves_both_entries_and_compares_content() {
    let (src_root, dest_root) = test_roots("path");
    write_matching_files(&src_root, &dest_root, b"path api");

    let result = IntegrityCheck::check_path(
        &local_with_block(&src_root, 2),
        &local_with_block(&dest_root, 5),
        Path::new("item"),
        IntegrityCheckMode::Full,
        None,
    )
    .await;

    assert!(matches!(result, Ok(entry) if entry.get_size() == 8));
    std::fs::remove_dir_all(src_root.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn check_with_source_entry_resolves_only_destination() {
    let (src_root, dest_root) = test_roots("source-entry");
    write_matching_files(&src_root, &dest_root, b"known source");
    let src_storage = local(&src_root);
    let src_entry = src_storage.get_metadata(Path::new("item")).await.unwrap();

    let result = IntegrityCheck::check_with_source_entry(
        &src_storage,
        &local(&dest_root),
        &src_entry,
        IntegrityCheckMode::Full,
        None,
    )
    .await;

    assert!(result.is_ok());
    std::fs::remove_dir_all(src_root.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn check_path_preserves_missing_error_without_retrying() {
    let (src_root, dest_root) = test_roots("missing");
    std::fs::create_dir_all(&src_root).unwrap();
    std::fs::create_dir_all(&dest_root).unwrap();

    let result = IntegrityCheck::check_path(
        &local(&src_root),
        &local(&dest_root),
        Path::new("missing"),
        IntegrityCheckMode::Quick,
        None,
    )
    .await;

    assert!(matches!(
        result,
        Err(StorageError::FileNotFound(_)) | Err(StorageError::IoError(_))
    ));
    std::fs::remove_dir_all(src_root.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn check_path_honors_precancel_before_metadata_io() {
    let token = CancellationToken::new();
    token.cancel();
    let root = std::env::temp_dir();

    let result = IntegrityCheck::check_path(
        &local(&root),
        &local(&root),
        Path::new("missing"),
        IntegrityCheckMode::Quick,
        Some(&token),
    )
    .await;

    assert!(matches!(result, Err(StorageError::Cancelled)));
}
