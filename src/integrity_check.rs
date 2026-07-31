//! Independent integrity checks for objects that already exist on two storages.

use std::future::Future;
use std::io::ErrorKind;
use std::path::Path;
use std::pin::Pin;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

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
    /// Resolve both entries at `relative_path`, then compare them.
    ///
    /// Metadata reads run concurrently and retain their original backend error
    /// variants. Confirmed missing paths and permanent errors are not retried.
    pub async fn check_path(
        src_storage: &StorageEnum,
        dest_storage: &StorageEnum,
        relative_path: &Path,
        mode: IntegrityCheckMode,
        cancel: Option<&CancellationToken>,
    ) -> Result<EntryEnum> {
        let resolve = Box::pin(async {
            tokio::try_join!(
                get_metadata_with_retry(src_storage, relative_path, cancel),
                get_metadata_with_retry(dest_storage, relative_path, cancel),
            )
        });
        let (src_entry, dest_entry) = if let Some(token) = cancel {
            tokio::select! {
                biased;
                () = token.cancelled() => return Err(StorageError::Cancelled),
                entries = resolve => entries?,
            }
        } else {
            resolve.await?
        };

        Self::check(
            src_storage,
            dest_storage,
            &src_entry,
            &dest_entry,
            mode,
            cancel,
        )
        .await?;
        Ok(src_entry)
    }

    /// Resolve only the destination entry and compare it with a known source.
    pub async fn check_with_source_entry(
        src_storage: &StorageEnum,
        dest_storage: &StorageEnum,
        src_entry: &EntryEnum,
        mode: IntegrityCheckMode,
        cancel: Option<&CancellationToken>,
    ) -> Result<()> {
        let dest_entry = Box::pin(get_metadata_with_retry(
            dest_storage,
            src_entry.get_relative_path(),
            cancel,
        ))
        .await?;
        Self::check(
            src_storage,
            dest_storage,
            src_entry,
            &dest_entry,
            mode,
            cancel,
        )
        .await
    }

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

const METADATA_RETRY_DELAYS: [Duration; 3] = [
    Duration::from_millis(200),
    Duration::from_millis(500),
    Duration::from_secs(1),
];
const COMPARE_CHANNEL_CAPACITY: usize = 2;

fn is_missing(error: &StorageError) -> bool {
    matches!(
        error,
        StorageError::FileNotFound(_) | StorageError::DirectoryNotFound(_)
    ) || matches!(error, StorageError::IoError(error) if error.kind() == ErrorKind::NotFound)
}

fn is_transient_metadata_error(error: &StorageError) -> bool {
    match error {
        StorageError::IoError(error) => matches!(
            error.kind(),
            ErrorKind::Interrupted
                | ErrorKind::WouldBlock
                | ErrorKind::TimedOut
                | ErrorKind::ConnectionReset
                | ErrorKind::ConnectionAborted
                | ErrorKind::ConnectionRefused
                | ErrorKind::NotConnected
                | ErrorKind::UnexpectedEof
        ),
        // These backends currently expose opaque protocol errors, so the
        // integrity layer can only apply a short bounded retry.
        StorageError::NfsError(_) | StorageError::CifsError(_) | StorageError::S3Error(_) => true,
        _ => false,
    }
}

async fn wait_retry(delay: Duration, cancel: Option<&CancellationToken>) -> Result<()> {
    if let Some(token) = cancel {
        tokio::select! {
            biased;
            () = token.cancelled() => Err(StorageError::Cancelled),
            () = tokio::time::sleep(delay) => Ok(()),
        }
    } else {
        tokio::time::sleep(delay).await;
        Ok(())
    }
}

async fn get_metadata_once(
    storage: &StorageEnum,
    relative_path: &Path,
    cancel: Option<&CancellationToken>,
) -> Result<EntryEnum> {
    if let Some(token) = cancel {
        tokio::select! {
            biased;
            () = token.cancelled() => Err(StorageError::Cancelled),
            result = Box::pin(storage.get_metadata(relative_path)) => result,
        }
    } else {
        Box::pin(storage.get_metadata(relative_path)).await
    }
}

async fn get_metadata_with_retry(
    storage: &StorageEnum,
    relative_path: &Path,
    cancel: Option<&CancellationToken>,
) -> Result<EntryEnum> {
    get_metadata_with_retry_by(
        || Box::pin(get_metadata_once(storage, relative_path, cancel)),
        relative_path,
        &METADATA_RETRY_DELAYS,
        cancel,
    )
    .await
}

async fn get_metadata_with_retry_by<'a, F>(
    mut get_metadata: F,
    relative_path: &Path,
    retry_delays: &[Duration],
    cancel: Option<&CancellationToken>,
) -> Result<EntryEnum>
where
    F: FnMut() -> Pin<Box<dyn Future<Output = Result<EntryEnum>> + Send + 'a>>,
{
    for (attempt, delay) in retry_delays.iter().enumerate() {
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            return Err(StorageError::Cancelled);
        }
        match get_metadata().await {
            Ok(entry) => return Ok(entry),
            Err(error) if is_missing(&error) || !is_transient_metadata_error(&error) => {
                return Err(error);
            }
            Err(error) => {
                warn!(
                    path = %relative_path.display(),
                    attempt = attempt + 1,
                    max_retries = retry_delays.len(),
                    delay_ms = delay.as_millis(),
                    %error,
                    "transient integrity metadata read failed"
                );
                wait_retry(*delay, cancel).await?;
            }
        }
    }
    get_metadata().await
}

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

    let (src_rx, src_task) = StorageEnum::read_chunk_stream(
        src_storage,
        src_entry,
        None,
        None,
        false,
        COMPARE_CHANNEL_CAPACITY,
    );
    let (dest_rx, dest_task) = StorageEnum::read_chunk_stream(
        dest_storage,
        dest_entry,
        None,
        None,
        false,
        COMPARE_CHANNEL_CAPACITY,
    );

    compare_chunk_streams(src_rx, src_task, dest_rx, dest_task, expected_size, cancel).await
}

async fn compare_chunk_streams(
    mut src_rx: mpsc::Receiver<DataChunk>,
    src_task: JoinHandle<Result<Option<crate::HashCalculator>>>,
    mut dest_rx: mpsc::Receiver<DataChunk>,
    dest_task: JoinHandle<Result<Option<crate::HashCalculator>>>,
    expected_size: u64,
    cancel: Option<&CancellationToken>,
) -> Result<()> {
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
    use bytes::Bytes;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    type TestReadTask = JoinHandle<Result<Option<crate::HashCalculator>>>;

    fn test_stream(
        chunks: Vec<DataChunk>,
        result: Result<Option<crate::HashCalculator>>,
    ) -> (mpsc::Receiver<DataChunk>, TestReadTask) {
        let (tx, rx) = mpsc::channel(chunks.len().max(1));
        let task = tokio::spawn(async move {
            for chunk in chunks {
                tx.send(chunk).await.map_err(|_| StorageError::Cancelled)?;
            }
            result
        });
        (rx, task)
    }

    fn chunk(offset: u64, data: &'static [u8]) -> DataChunk {
        DataChunk {
            offset,
            data: Bytes::from_static(data),
        }
    }

    struct AbortFlag(Arc<AtomicBool>);

    impl Drop for AbortFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn pending_stream(aborted: Arc<AtomicBool>) -> (mpsc::Receiver<DataChunk>, TestReadTask) {
        let (tx, rx) = mpsc::channel(1);
        let abort_flag = AbortFlag(aborted);
        let task = tokio::spawn(async move {
            let _abort_flag = abort_flag;
            let _keep_channel_open = tx;
            std::future::pending::<()>().await;
            unreachable!()
        });
        (rx, task)
    }

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
    async fn full_reports_source_stream_offset_mismatch() {
        let (src_rx, src_task) = test_stream(vec![chunk(1, b"abc")], Ok(None));
        let (dest_rx, dest_task) = test_stream(vec![chunk(0, b"abc")], Ok(None));

        let result = compare_chunk_streams(src_rx, src_task, dest_rx, dest_task, 3, None).await;

        assert!(matches!(
            result,
            Err(StorageError::MismatchData(fields))
                if fields == vec![MismatchDataField::StreamOffset {
                    side: IntegritySide::Source,
                    expected: 0,
                    actual: 1,
                }]
        ));
    }

    #[tokio::test]
    async fn full_reports_destination_stream_offset_mismatch() {
        let (src_rx, src_task) = test_stream(vec![chunk(0, b"abc")], Ok(None));
        let (dest_rx, dest_task) = test_stream(vec![chunk(1, b"abc")], Ok(None));

        let result = compare_chunk_streams(src_rx, src_task, dest_rx, dest_task, 3, None).await;

        assert!(matches!(
            result,
            Err(StorageError::MismatchData(fields))
                if fields == vec![MismatchDataField::StreamOffset {
                    side: IntegritySide::Destination,
                    expected: 0,
                    actual: 1,
                }]
        ));
    }

    #[tokio::test]
    async fn full_reports_destination_short_read() {
        let (src_rx, src_task) = test_stream(vec![chunk(0, b"0123456789")], Ok(None));
        let (dest_rx, dest_task) = test_stream(vec![chunk(0, b"012345")], Ok(None));

        let result = compare_chunk_streams(src_rx, src_task, dest_rx, dest_task, 10, None).await;

        assert!(matches!(
            result,
            Err(StorageError::MismatchData(fields))
                if fields == vec![MismatchDataField::ReadLength {
                    side: IntegritySide::Destination,
                    expected: 10,
                    actual: 6,
                }]
        ));
    }

    #[tokio::test]
    async fn full_preserves_backend_error_before_short_read() {
        let backend_error = StorageError::OperationError("injected read failure".to_string());
        let (src_rx, src_task) = test_stream(Vec::new(), Err(backend_error));
        let (dest_rx, dest_task) = test_stream(Vec::new(), Ok(None));

        let result = compare_chunk_streams(src_rx, src_task, dest_rx, dest_task, 10, None).await;

        assert!(matches!(
            result,
            Err(StorageError::OperationError(message))
                if message == "injected read failure"
        ));
    }

    #[tokio::test]
    async fn cancellation_during_full_check_aborts_both_read_tasks() {
        let src_aborted = Arc::new(AtomicBool::new(false));
        let dest_aborted = Arc::new(AtomicBool::new(false));
        let (src_rx, src_task) = pending_stream(Arc::clone(&src_aborted));
        let (dest_rx, dest_task) = pending_stream(Arc::clone(&dest_aborted));
        let token = CancellationToken::new();
        let cancel = token.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            cancel.cancel();
        });

        let result =
            compare_chunk_streams(src_rx, src_task, dest_rx, dest_task, 10, Some(&token)).await;

        assert!(matches!(result, Err(StorageError::Cancelled)));
        assert!(src_aborted.load(Ordering::SeqCst));
        assert!(dest_aborted.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn metadata_retry_succeeds_after_transient_failures() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&attempts);

        let result = get_metadata_with_retry_by(
            move || {
                let attempt = observed.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    if attempt < 2 {
                        Err(StorageError::IoError(std::io::Error::new(
                            ErrorKind::TimedOut,
                            "injected timeout",
                        )))
                    } else {
                        Ok(nas_entry("item", 10))
                    }
                })
            },
            Path::new("item"),
            &[Duration::ZERO, Duration::ZERO],
            None,
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn metadata_retry_exhaustion_returns_final_error() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&attempts);

        let result = get_metadata_with_retry_by(
            move || {
                let attempt = observed.fetch_add(1, Ordering::SeqCst) + 1;
                Box::pin(async move {
                    Err(StorageError::IoError(std::io::Error::new(
                        ErrorKind::TimedOut,
                        format!("injected timeout {attempt}"),
                    )))
                })
            },
            Path::new("item"),
            &[Duration::ZERO, Duration::ZERO],
            None,
        )
        .await;

        assert!(matches!(
            result,
            Err(StorageError::IoError(error))
                if error.kind() == ErrorKind::TimedOut
                    && error.to_string() == "injected timeout 3"
        ));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn cancellation_during_metadata_retry_wait_stops_retries() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&attempts);
        let token = CancellationToken::new();
        let cancel = token.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            cancel.cancel();
        });

        let result = get_metadata_with_retry_by(
            move || {
                observed.fetch_add(1, Ordering::SeqCst);
                Box::pin(async {
                    Err(StorageError::IoError(std::io::Error::new(
                        ErrorKind::TimedOut,
                        "injected timeout",
                    )))
                })
            },
            Path::new("item"),
            &[Duration::from_mins(1)],
            Some(&token),
        )
        .await;

        assert!(matches!(result, Err(StorageError::Cancelled)));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
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
}
