//! Independent integrity checks for objects that already exist on two storages.

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::StorageError;
use crate::storage_enum::StorageEnum;
use crate::{DataChunk, EntryEnum, Result};

/// Entry type used in structured integrity-check mismatches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityEntryKind {
    File,
    Directory,
    Symlink,
}

/// Side of an integrity comparison that returned an unexpected byte count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegritySide {
    Source,
    Destination,
}

/// Structured content mismatches reported by an independent integrity check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MismatchDataField {
    EntryKind {
        src: IntegrityEntryKind,
        dest: IntegrityEntryKind,
    },
    Size {
        src: u64,
        dest: u64,
    },
    ReadLength {
        side: IntegritySide,
        expected: u64,
        actual: u64,
    },
    StreamOffset {
        side: IntegritySide,
        expected: u64,
        actual: u64,
    },
    Content {
        offset: u64,
    },
}

/// Structured POSIX metadata mismatches reported by an integrity check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MismatchMetaField {
    Mtime { src: i64, dest: i64 },
    Uid { src: u32, dest: u32 },
    Gid { src: u32, dest: u32 },
    Mode { src: u32, dest: u32 },
}

/// Amount of data validation performed by [`IntegrityCheck`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityCheckMode {
    /// Compare entry type, size, and supported metadata without reading file data.
    Quick,
    /// Perform all quick checks and compare file data as aligned byte streams.
    Full,
}

/// Stateless independent integrity checker.
pub struct IntegrityCheck;

impl IntegrityCheck {
    /// Compare two already-resolved entries.
    ///
    /// S3 destinations skip POSIX metadata because those fields cannot be
    /// represented faithfully. File type and content checks still apply.
    pub async fn check(
        src_storage: &StorageEnum,
        dest_storage: &StorageEnum,
        src_entry: &EntryEnum,
        dest_entry: &EntryEnum,
        mode: IntegrityCheckMode,
        cancel: Option<&CancellationToken>,
    ) -> Result<()> {
        let src_kind = entry_kind(src_entry);
        let dest_kind = entry_kind(dest_entry);
        if src_kind != dest_kind {
            return Err(StorageError::MismatchData(vec![
                MismatchDataField::EntryKind {
                    src: src_kind,
                    dest: dest_kind,
                },
            ]));
        }

        if src_kind == IntegrityEntryKind::File {
            Self::check_file_content(
                src_storage,
                dest_storage,
                src_entry,
                dest_entry,
                mode,
                cancel,
            )
            .await?;
        }

        let metadata = collect_metadata_mismatches(
            src_entry,
            dest_entry,
            matches!(dest_storage, StorageEnum::S3(_)),
        );
        if metadata.is_empty() {
            Ok(())
        } else {
            Err(StorageError::MismatchMeta(metadata))
        }
    }

    async fn check_file_content(
        src_storage: &StorageEnum,
        dest_storage: &StorageEnum,
        src_entry: &EntryEnum,
        dest_entry: &EntryEnum,
        mode: IntegrityCheckMode,
        cancel: Option<&CancellationToken>,
    ) -> Result<()> {
        let src_size = src_entry.get_size();
        let dest_size = dest_entry.get_size();
        if src_size != dest_size {
            return Err(StorageError::MismatchData(vec![MismatchDataField::Size {
                src: src_size,
                dest: dest_size,
            }]));
        }

        if mode == IntegrityCheckMode::Quick || src_size == 0 {
            return Ok(());
        }

        compare_file_streams(
            src_storage,
            dest_storage,
            src_entry,
            dest_entry,
            src_size,
            cancel,
        )
        .await
    }
}

const COMPARE_CHANNEL_CAPACITY: usize = 2;

async fn recv_chunk(
    rx: &mut mpsc::Receiver<DataChunk>,
    cancel: Option<&CancellationToken>,
) -> Result<Option<DataChunk>> {
    if let Some(token) = cancel {
        tokio::select! {
            biased;
            () = token.cancelled() => Err(StorageError::Cancelled),
            chunk = rx.recv() => Ok(chunk),
        }
    } else {
        Ok(rx.recv().await)
    }
}

async fn abort_read_tasks(
    src_task: Option<JoinHandle<Result<Option<crate::HashCalculator>>>>,
    dest_task: Option<JoinHandle<Result<Option<crate::HashCalculator>>>>,
) {
    if let Some(task) = &src_task {
        task.abort();
    }
    if let Some(task) = &dest_task {
        task.abort();
    }
    if let Some(task) = src_task {
        let _ = task.await;
    }
    if let Some(task) = dest_task {
        let _ = task.await;
    }
}

async fn finish_read_task(
    task: &mut Option<JoinHandle<Result<Option<crate::HashCalculator>>>>,
) -> Result<()> {
    if let Some(task) = task.take() {
        task.await??;
    }
    Ok(())
}

async fn compare_file_streams(
    src_storage: &StorageEnum,
    dest_storage: &StorageEnum,
    src_entry: &EntryEnum,
    dest_entry: &EntryEnum,
    expected_size: u64,
    cancel: Option<&CancellationToken>,
) -> Result<()> {
    if cancel.is_some_and(CancellationToken::is_cancelled) {
        return Err(StorageError::Cancelled);
    }

    let (mut src_rx, src_task) = StorageEnum::read_chunk_stream(
        src_storage,
        src_entry,
        None,
        None,
        false,
        COMPARE_CHANNEL_CAPACITY,
    );
    let (mut dest_rx, dest_task) = StorageEnum::read_chunk_stream(
        dest_storage,
        dest_entry,
        None,
        None,
        false,
        COMPARE_CHANNEL_CAPACITY,
    );

    let mut src_chunk = None;
    let mut dest_chunk = None;
    let mut src_task = Some(src_task);
    let mut dest_task = Some(dest_task);
    let mut src_index = 0;
    let mut dest_index = 0;
    let mut src_read = 0_u64;
    let mut dest_read = 0_u64;

    let compare_result: Result<()> = async {
        loop {
            if src_chunk.is_none() {
                src_chunk = recv_chunk(&mut src_rx, cancel).await?;
                src_index = 0;
                if let Some(chunk) = &src_chunk
                    && chunk.offset != src_read
                {
                    return Err(StorageError::MismatchData(vec![
                        MismatchDataField::StreamOffset {
                            side: IntegritySide::Source,
                            expected: src_read,
                            actual: chunk.offset,
                        },
                    ]));
                }
            }
            if dest_chunk.is_none() {
                dest_chunk = recv_chunk(&mut dest_rx, cancel).await?;
                dest_index = 0;
                if let Some(chunk) = &dest_chunk
                    && chunk.offset != dest_read
                {
                    return Err(StorageError::MismatchData(vec![
                        MismatchDataField::StreamOffset {
                            side: IntegritySide::Destination,
                            expected: dest_read,
                            actual: chunk.offset,
                        },
                    ]));
                }
            }

            match (&src_chunk, &dest_chunk) {
                (None, None) => break,
                (None, Some(_)) => {
                    finish_read_task(&mut src_task).await?;
                    return Err(StorageError::MismatchData(vec![
                        MismatchDataField::ReadLength {
                            side: IntegritySide::Source,
                            expected: expected_size,
                            actual: src_read,
                        },
                    ]));
                }
                (Some(_), None) => {
                    finish_read_task(&mut dest_task).await?;
                    return Err(StorageError::MismatchData(vec![
                        MismatchDataField::ReadLength {
                            side: IntegritySide::Destination,
                            expected: expected_size,
                            actual: dest_read,
                        },
                    ]));
                }
                (Some(src), Some(dest)) => {
                    let count = (src.data.len() - src_index).min(dest.data.len() - dest_index);
                    let src_bytes = &src.data[src_index..src_index + count];
                    let dest_bytes = &dest.data[dest_index..dest_index + count];
                    if let Some(index) = src_bytes
                        .iter()
                        .zip(dest_bytes)
                        .position(|(src, dest)| src != dest)
                    {
                        return Err(StorageError::MismatchData(vec![
                            MismatchDataField::Content {
                                offset: src_read + index as u64,
                            },
                        ]));
                    }

                    src_index += count;
                    dest_index += count;
                    src_read += count as u64;
                    dest_read += count as u64;
                    if src_index == src.data.len() {
                        src_chunk = None;
                    }
                    if dest_index == dest.data.len() {
                        dest_chunk = None;
                    }
                }
            }
        }
        Ok(())
    }
    .await;

    // A mismatch or cancellation deliberately stops reading the remainder.
    if let Err(error) = compare_result {
        abort_read_tasks(src_task, dest_task).await;
        return Err(error);
    }

    // Both channels are closed, so neither producer can be blocked on send.
    // Preserve backend and join errors before interpreting a short stream as
    // an integrity mismatch.
    tokio::try_join!(
        finish_read_task(&mut src_task),
        finish_read_task(&mut dest_task)
    )?;
    if src_read != expected_size {
        return Err(StorageError::MismatchData(vec![
            MismatchDataField::ReadLength {
                side: IntegritySide::Source,
                expected: expected_size,
                actual: src_read,
            },
        ]));
    }
    if dest_read != expected_size {
        return Err(StorageError::MismatchData(vec![
            MismatchDataField::ReadLength {
                side: IntegritySide::Destination,
                expected: expected_size,
                actual: dest_read,
            },
        ]));
    }
    Ok(())
}

fn entry_kind(entry: &EntryEnum) -> IntegrityEntryKind {
    if entry.get_is_dir() {
        IntegrityEntryKind::Directory
    } else if entry.get_is_symlink() {
        IntegrityEntryKind::Symlink
    } else {
        IntegrityEntryKind::File
    }
}

fn collect_metadata_mismatches(
    src: &EntryEnum,
    dest: &EntryEnum,
    dest_is_s3: bool,
) -> Vec<MismatchMetaField> {
    if dest_is_s3 {
        return Vec::new();
    }

    let mut mismatches = Vec::new();
    if src.get_mtime() != dest.get_mtime() {
        mismatches.push(MismatchMetaField::Mtime {
            src: src.get_mtime(),
            dest: dest.get_mtime(),
        });
    }
    if let (Some(src), Some(dest)) = (src.get_uid(), dest.get_uid())
        && src != dest
    {
        mismatches.push(MismatchMetaField::Uid { src, dest });
    }
    if let (Some(src), Some(dest)) = (src.get_gid(), dest.get_gid())
        && src != dest
    {
        mismatches.push(MismatchMetaField::Gid { src, dest });
    }
    if !src.get_is_symlink()
        && let (Some(src), Some(dest)) = (src.get_mode(), dest.get_mode())
    {
        let src = src & 0o777;
        let dest = dest & 0o777;
        if src != dest {
            mismatches.push(MismatchMetaField::Mode { src, dest });
        }
    }
    mismatches
}

#[cfg(test)]
mod tests {
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
}
