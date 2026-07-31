//! Independent integrity checks for objects that already exist on two storages.

use std::io::ErrorKind;
use std::path::Path;
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
    for (attempt, delay) in METADATA_RETRY_DELAYS.iter().enumerate() {
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            return Err(StorageError::Cancelled);
        }
        match Box::pin(get_metadata_once(storage, relative_path, cancel)).await {
            Ok(entry) => return Ok(entry),
            Err(error) if is_missing(&error) || !is_transient_metadata_error(&error) => {
                return Err(error);
            }
            Err(error) => {
                warn!(
                    path = %relative_path.display(),
                    attempt = attempt + 1,
                    max_retries = METADATA_RETRY_DELAYS.len(),
                    delay_ms = delay.as_millis(),
                    %error,
                    "transient integrity metadata read failed"
                );
                wait_retry(*delay, cancel).await?;
            }
        }
    }
    Box::pin(get_metadata_once(storage, relative_path, cancel)).await
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
mod tests;
