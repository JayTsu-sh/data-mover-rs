//! Bounded tar composition over the source and destination roles.
//!
//! Every file is read through its identity-bound source stream. The archive is written through
//! the expert destination lifecycle, so publication and read-back verification use the same
//! optimized backend implementations as ordinary transfers. No complete file is buffered.

use crate::model::{
    EntryKind, EntryOperationFailure, FailureClass, IdentityStrength, ObservedEntry, Operation,
    SourceIdentity, SourceVersion, StoragePath, Transience, without_unowned_set_id,
};
use crate::storage::{
    ByteStream, ReadRequest, ReadSource, SourceQosBudget, SourceQosGroup, SourceQosStats, Storage,
    StorageRoleFailure,
};
use crate::transfer::{
    ExpertDestinationRequest, ExpertDestinationSession, ExpertSourceEvidence, TransferFailure,
    TransferOutcome, TransferPolicy,
};
use bytes::{Bytes, BytesMut};
use futures::StreamExt as _;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

#[derive(Debug, thiserror::Error)]
pub enum TarFailure {
    #[error("invalid archive: {0}")]
    Invalid(String),
    #[error(transparent)]
    Storage(#[from] StorageRoleFailure),
    /// Retains the destination's cleanup authority when transfer fails.
    #[error(transparent)]
    Transfer(Box<TransferFailure>),
}

/// Archive inputs. Entry paths are also their names in the archive.
/// Request ownership and timestamps during traversal to preserve them in the tar headers.
#[derive(Clone)]
pub struct TarRequest {
    pub source: Storage,
    pub destination: Storage,
    pub entries: Vec<ObservedEntry>,
    pub final_path: StoragePath,
    pub cancel: CancellationToken,
    pub source_qos: Option<SourceQosGroup>,
}

/// Checked archive size, including headers, padding and the two end blocks.
///
/// # Errors
/// Refuses unsupported entries, unknown file sizes and arithmetic overflow.
pub fn calculate_tar_size(entries: &[ObservedEntry]) -> Result<u64, TarFailure> {
    entries.iter().try_fold(1024_u64, |total, entry| {
        let size = entry_size(entry)?;
        let headers = u64::try_from(build_header_for_entry(entry)?.len())
            .map_err(|_| invalid("archive header overflow"))?;
        total
            .checked_add(headers)
            .and_then(|value| value.checked_add(size))
            .and_then(|value| value.checked_add((512 - size % 512) % 512))
            .ok_or_else(|| invalid("archive size overflow"))
    })
}

/// Streams an archive and atomically replaces its destination using the shared transfer engine.
///
/// # Errors
/// Returns invalid input, a source error, or a destination failure retaining cleanup authority.
pub async fn pack(request: TarRequest) -> Result<TransferOutcome, TarFailure> {
    let size = calculate_tar_size(&request.entries)?;
    let observed = archive_observation(&request, size)?;
    let identity_key = observed.identity_key();
    let source = request
        .source
        .read_source()
        .map_err(|error| invalid(&error.to_string()))?;
    let maximum = source.maximum_read_chunk_bytes().max(512);
    let destination = ExpertDestinationSession::prepare(
        ExpertDestinationRequest::new(
            observed,
            maximum,
            request.destination,
            request.final_path.clone(),
            request.cancel.clone(),
        )
        .with_transfer_policy(TransferPolicy::AtomicReplace),
    )
    .await
    .map_err(transfer_failure)?;
    let budget = request
        .source_qos
        .as_ref()
        .map(SourceQosGroup::transfer_budget);
    let evidence = Arc::new(Mutex::new((blake3::Hasher::new(), 0_u64)));
    let tally = Arc::clone(&evidence);
    let state = ArchiveStream {
        source,
        output: None,
        entries: request.entries.into_iter(),
        pending: None,
        reading: None,
        remaining: 0,
        padding: None,
        eof: false,
        maximum,
        cancel: request.cancel,
        budget: budget.clone(),
        path: request.final_path,
    };
    let stream = futures::stream::try_unfold(state, next_piece).map(move |piece| {
        if let Ok(bytes) = &piece {
            let mut tally = tally
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            tally.0.update(bytes);
            tally.1 += bytes.len() as u64;
        }
        piece
    });
    let transferred = destination
        .write(Box::pin(stream))
        .await
        .map_err(transfer_failure)?;
    let (digest, actual) = {
        let tally = evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (*tally.0.finalize().as_bytes(), tally.1)
    };
    // Destination write also checks the complete expected length before returning.
    debug_assert_eq!(actual, size);
    if let Some(budget) = &budget {
        budget.set_logical_bytes(size);
    }
    transferred
        .complete(ExpertSourceEvidence {
            source_size: size,
            blake3: digest,
            identity_key,
            source_qos: budget.as_ref().map_or(
                SourceQosStats {
                    logical_bytes: size,
                    ..Default::default()
                },
                SourceQosBudget::stats,
            ),
        })
        .await
        .map_err(transfer_failure)
}

fn archive_observation(request: &TarRequest, size: u64) -> Result<ObservedEntry, TarFailure> {
    let mut manifest = blake3::Hasher::new();
    manifest.update(b"data-mover/tar/v1\0");
    manifest.update(request.final_path.as_str().as_bytes());
    manifest.update(b"\0");
    for entry in &request.entries {
        manifest.update(entry.identity_key().as_bytes());
        manifest.update(&build_header_for_entry(entry)?);
    }
    let identity = SourceIdentity::new(
        request.source.identity().clone(),
        IdentityStrength::PathScoped,
        manifest.finalize().as_bytes(),
    )
    .map_err(|error| invalid(&error.to_string()))?;
    let observed = ObservedEntry::new(
        request.final_path.clone(),
        EntryKind::File,
        Some(size),
        None,
        identity,
    )
    .map_err(|error| invalid(&error.to_string()))?;
    Ok(observed)
}

struct ArchiveStream {
    output: Option<Bytes>,
    source: Arc<dyn ReadSource>,
    entries: std::vec::IntoIter<ObservedEntry>,
    pending: Option<ObservedEntry>,
    reading: Option<ByteStream>,
    remaining: u64,
    padding: Option<Bytes>,
    eof: bool,
    maximum: usize,
    cancel: CancellationToken,
    budget: Option<SourceQosBudget>,
    path: StoragePath,
}

async fn next_piece(
    mut state: ArchiveStream,
) -> Result<Option<(Bytes, ArchiveStream)>, StorageRoleFailure> {
    let next = match state.output.take() {
        Some(bytes) => Some((bytes, state)),
        None => next_raw(state).await?,
    };
    let Some((mut bytes, mut state)) = next else {
        return Ok(None);
    };
    if bytes.len() > state.maximum {
        let head = bytes.split_to(state.maximum);
        state.output = Some(bytes);
        bytes = head;
    }
    Ok(Some((bytes, state)))
}

async fn next_raw(
    mut state: ArchiveStream,
) -> Result<Option<(Bytes, ArchiveStream)>, StorageRoleFailure> {
    loop {
        if state.cancel.is_cancelled() {
            return Err(stream_failure(&state.path, FailureClass::Cancelled));
        }
        if let Some(reading) = &mut state.reading {
            if let Some(chunk) = reading.next().await {
                let bytes = chunk?;
                state.remaining = state
                    .remaining
                    .checked_sub(bytes.len() as u64)
                    .ok_or_else(|| stream_failure(&state.path, FailureClass::Corruption))?;
                if bytes.is_empty() {
                    continue;
                }
                return Ok(Some((bytes, state)));
            }
            if state.remaining != 0 {
                return Err(stream_failure(&state.path, FailureClass::Corruption));
            }
            state.reading = None;
        }
        if let Some(padding) = state.padding.take() {
            return Ok(Some((padding, state)));
        }
        if let Some(entry) = state.pending.take() {
            let size = entry
                .size()
                .ok_or_else(|| stream_failure(entry.path(), FailureClass::InvalidInput))?;
            state.remaining = size;
            state.padding = tar_padding(size);
            state.reading = Some(
                state
                    .source
                    .read(ReadRequest {
                        path: entry.path().clone(),
                        range: Some(0..size),
                        expected_source: Some(entry.source_identity().clone()),
                        maximum_chunk_bytes: state.maximum,
                        read_inflight: state.source.read_concurrency().max(1),
                        read_budget: None,
                        cancel: state.cancel.clone(),
                        source_qos: state.budget.clone(),
                        version: SourceVersion::Current,
                    })
                    .await?,
            );
            continue;
        }
        if let Some(entry) = state.entries.next() {
            let header = build_header_for_entry(&entry)
                .map_err(|_| stream_failure(entry.path(), FailureClass::InvalidInput))?;
            if entry.kind() == EntryKind::File {
                state.pending = Some(entry);
            }
            return Ok(Some((header, state)));
        }
        if !state.eof {
            state.eof = true;
            return Ok(Some((tar_eof_marker(), state)));
        }
        return Ok(None);
    }
}

fn stream_failure(path: &StoragePath, class: FailureClass) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            Operation::Read,
            class,
            Transience::Permanent,
            "archive source stream failed",
        )
        .unwrap_or_else(|_| unreachable!("static diagnostic")),
    )
}
fn invalid(message: &str) -> TarFailure {
    TarFailure::Invalid(message.to_owned())
}
fn transfer_failure(failure: TransferFailure) -> TarFailure {
    TarFailure::Transfer(Box::new(failure))
}

fn entry_size(entry: &ObservedEntry) -> Result<u64, TarFailure> {
    match entry.kind() {
        EntryKind::File => entry.size().ok_or_else(|| invalid("file size is unknown")),
        EntryKind::Directory | EntryKind::Symlink => Ok(0),
        EntryKind::Special(_) => Err(invalid("unsupported archive entry kind")),
    }
}

fn build_header_for_entry(entry: &ObservedEntry) -> Result<Bytes, TarFailure> {
    let size = entry_size(entry)?;
    let path = if entry.kind() == EntryKind::Directory {
        format!("{}/", entry.path().as_str().trim_end_matches('/'))
    } else {
        entry.path().as_str().to_owned()
    };
    if path.is_empty() || path.starts_with('/') || path.split('/').any(|part| part == "..") {
        return Err(invalid("archive path must be relative and confined"));
    }
    if entry.kind() == EntryKind::Symlink && entry.symlink_target().is_none() {
        return Err(invalid("archive symlink target was not observed"));
    }
    let target = entry
        .symlink_target()
        .map(crate::model::SymlinkTarget::as_bytes)
        .unwrap_or_default();
    if path.contains('\0') || target.contains(&0) {
        return Err(invalid("archive names contain NUL"));
    }
    let owner = entry.metadata().ownership_mode().value();
    let (mode, uid, gid) = header_ownership(
        owner.map_or(0, |owner| owner.mode),
        owner.map(|owner| owner.uid),
        owner.map(|owner| owner.gid),
        entry.kind() == EntryKind::Directory,
    );
    let mtime = entry
        .modified()
        .map(|time| i64::try_from(time.unix_nanos()))
        .transpose()
        .map_err(|_| invalid("timestamp exceeds archive range"))?
        .unwrap_or(0);
    let mut extended = Vec::new();
    if target.len() > 100 && std::str::from_utf8(target).is_err() {
        pax_record(&mut extended, "hdrcharset", b"BINARY");
    }
    if path.len() > 100 {
        pax_record(&mut extended, "path", path.as_bytes());
    }
    if target.len() > 100 {
        pax_record(&mut extended, "linkpath", target);
    }
    if size > 0o77_777_777_777 {
        pax_record(&mut extended, "size", size.to_string().as_bytes());
    }
    if uid > 0o7_777_777 {
        pax_record(&mut extended, "uid", uid.to_string().as_bytes());
    }
    if gid > 0o7_777_777 {
        pax_record(&mut extended, "gid", gid.to_string().as_bytes());
    }
    if mtime < 0 || mtime % 1_000_000_000 != 0 {
        let magnitude = mtime.unsigned_abs();
        pax_record(
            &mut extended,
            "mtime",
            format!(
                "{}{}.{:09}",
                if mtime < 0 { "-" } else { "" },
                magnitude / 1_000_000_000,
                magnitude % 1_000_000_000
            )
            .as_bytes(),
        );
    }
    let mut output = Vec::new();
    if !extended.is_empty() {
        let extended_size =
            u64::try_from(extended.len()).map_err(|_| invalid("archive header overflow"))?;
        output.extend_from_slice(&build_ustar_header(&UstarHeader {
            path: "PaxHeaders/entry",
            size: extended_size,
            mtime: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            type_flag: b'x',
            link_name: b"",
        }));
        output.extend_from_slice(&extended);
        output.resize(output.len() + (512 - extended.len() % 512) % 512, 0);
    }
    output.extend_from_slice(&build_ustar_header(&UstarHeader {
        path: if path.len() <= 100 { &path } else { "PaxEntry" },
        size: if size <= 0o77_777_777_777 { size } else { 0 },
        mtime,
        mode,
        uid,
        gid,
        type_flag: match entry.kind() {
            EntryKind::Directory => b'5',
            EntryKind::Symlink => b'2',
            _ => b'0',
        },
        link_name: if target.len() <= 100 { target } else { b"" },
    }));
    Ok(Bytes::from(output))
}

fn pax_record(output: &mut Vec<u8>, key: &str, value: &[u8]) {
    let mut body = format!(" {key}=").into_bytes();
    body.extend_from_slice(value);
    body.push(b'\n');
    let mut length = body.len() + 1;
    loop {
        let next = body.len() + length.to_string().len();
        if next == length {
            break;
        }
        length = next;
    }
    output.extend_from_slice(length.to_string().as_bytes());
    output.extend_from_slice(&body);
}
struct UstarHeader<'a> {
    path: &'a str,
    size: u64,
    mtime: i64,
    mode: u32,
    uid: u32,
    gid: u32,
    type_flag: u8,
    link_name: &'a [u8],
}

fn build_ustar_header(metadata: &UstarHeader<'_>) -> [u8; 512] {
    let UstarHeader {
        path,
        size,
        mtime,
        mode,
        uid,
        gid,
        type_flag,
        link_name,
    } = *metadata;
    let mut header = [0u8; 512];

    // name (0..100)
    write_bytes(&mut header[0..100], path.as_bytes());

    // mode (100..108) — octal, 7 chars + NUL
    write_octal(&mut header[100..108], u64::from(mode), 7);

    // uid (108..116)
    write_octal(&mut header[108..116], u64::from(uid), 7);

    // gid (116..124)
    write_octal(&mut header[116..124], u64::from(gid), 7);

    // size (124..136) — 11 chars + NUL
    write_octal(&mut header[124..136], size, 11);

    // mtime (136..148) — epoch seconds, 11 chars + NUL
    let mtime_secs = if mtime > 0 {
        u64::try_from(crate::time_util::nanos_to_secs(mtime)).unwrap_or(0)
    } else {
        0
    };
    write_octal(&mut header[136..148], mtime_secs, 11);

    // type_flag (156)
    header[156] = type_flag;

    // link_name (157..257)
    write_bytes(&mut header[157..257], link_name);

    // magic "ustar\0" (257..263)
    header[257..263].copy_from_slice(b"ustar\0");

    // version "00" (263..265)
    header[263..265].copy_from_slice(b"00");

    // checksum (148..156) — 先用空格填充，然后计算
    header[148..156].copy_from_slice(b"        "); // 8 spaces
    let checksum: u64 = header.iter().map(|&b| u64::from(b)).sum();
    write_octal(&mut header[148..156], checksum, 6);
    header[154] = 0; // NUL
    header[155] = b' '; // trailing space

    header
}

/// 将 octal 值写入指定字段（右对齐，前置零）
fn write_octal(field: &mut [u8], value: u64, width: usize) {
    let s = format!("{value:0>width$o}");
    let bytes = s.as_bytes();
    let len = bytes.len().min(field.len() - 1);
    field[..len].copy_from_slice(&bytes[bytes.len() - len..]);
    // 最后一个字节为 NUL
    if len < field.len() {
        field[len] = 0;
    }
}

/// 将字节拷贝到字段中（不超出字段长度）
fn write_bytes(field: &mut [u8], data: &[u8]) {
    let len = data.len().min(field.len());
    field[..len].copy_from_slice(&data[..len]);
}

/// Nobody, the id a tar records for an owner the source could not name.
const TAR_UNKNOWN_ID: u32 = 65534;

/// Mode, uid and gid for a tar header. An owner or group the source could not give (an `NFSv4`
/// name nfs-rs cannot map, see `nfs::owner_id`) is recorded as nobody, never as root (0), and
/// takes its set-id bit with it (`model::without_unowned_set_id`) — a root extraction that keeps
/// owners would otherwise create set-id files owned by someone the source never named.
fn header_ownership(
    mode: u32,
    uid: Option<u32>,
    gid: Option<u32>,
    is_dir: bool,
) -> (u32, u32, u32) {
    (
        without_unowned_set_id(mode, uid.is_some(), gid.is_some(), is_dir),
        uid.unwrap_or(TAR_UNKNOWN_ID),
        gid.unwrap_or(TAR_UNKNOWN_ID),
    )
}

/// 生成 512 字节对齐的 padding
///
/// 如果 `file_size` 不是 512 的倍数，返回对应长度的零填充 Bytes。
/// 如果已对齐则返回 None。
pub(crate) fn tar_padding(file_size: u64) -> Option<Bytes> {
    let remainder = file_size % 512;
    if remainder == 0 {
        return None;
    }
    let padding_len = 512 - remainder;
    let padding = BytesMut::zeroed(
        usize::try_from(padding_len).unwrap_or_else(|_| unreachable!("padding is below 512")),
    )
    .freeze();
    Some(padding)
}

/// 生成 tar EOF marker（两个 512B 全零块）
pub(crate) fn tar_eof_marker() -> Bytes {
    Bytes::from_static(&[0u8; 1024])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AssertTestValue;

    #[test]
    fn an_owner_the_source_could_not_name_is_nobody_without_its_set_id_bit() {
        assert_eq!(
            header_ownership(0o6755, Some(1000), Some(100), false),
            (0o6755, 1000, 100)
        );
        assert_eq!(
            header_ownership(0o6755, None, Some(100), false),
            (0o2755, TAR_UNKNOWN_ID, 100)
        );
        assert_eq!(
            header_ownership(0o6755, Some(1000), None, false),
            (0o4755, 1000, TAR_UNKNOWN_ID)
        );
        // A directory keeps setgid: it only makes children inherit the group.
        assert_eq!(
            header_ownership(0o2775, None, None, true),
            (0o2775, TAR_UNKNOWN_ID, TAR_UNKNOWN_ID)
        );
    }

    #[test]
    fn test_build_ustar_header_basic() {
        let header = build_ustar_header(&UstarHeader {
            path: "test.txt",
            size: 1024,
            mtime: 1_700_000_000_000_000_000,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            type_flag: b'0',
            link_name: b"",
        });

        // magic
        assert_eq!(&header[257..263], b"ustar\0");
        // version
        assert_eq!(&header[263..265], b"00");
        // type_flag
        assert_eq!(header[156], b'0');
        // name starts with "test.txt"
        assert_eq!(&header[0..8], b"test.txt");

        // checksum 字段非空
        assert_ne!(&header[148..156], &[0u8; 8]);
    }

    #[test]
    fn test_build_ustar_header_directory() {
        let header = build_ustar_header(&UstarHeader {
            path: "mydir/",
            size: 0,
            mtime: 1_700_000_000_000_000_000,
            mode: 0o755,
            uid: 0,
            gid: 0,
            type_flag: b'5',
            link_name: b"",
        });
        assert_eq!(header[156], b'5');
        // size should be zero for directories
        // size field (124..136): "00000000000\0"
        let size_field = std::str::from_utf8(&header[124..135]).ok();
        assert!(size_field.is_some());
        assert_eq!(
            u64::from_str_radix(size_field.map_or("0", |s| s.trim_start_matches('0')), 8)
                .unwrap_or(0),
            0
        );
    }

    #[test]
    fn test_checksum_correctness() {
        let header = build_ustar_header(&UstarHeader {
            path: "hello.txt",
            size: 5,
            mtime: 1_000_000_000_000_000_000,
            mode: 0o644,
            uid: 0,
            gid: 0,
            type_flag: b'0',
            link_name: b"",
        });

        // 验证 checksum：将 checksum 字段视为空格，求所有字节的和
        let mut check_header = header;
        check_header[148..156].copy_from_slice(b"        ");
        let expected: u64 = check_header.iter().map(|&b| u64::from(b)).sum();

        // 从 header 读取写入的 checksum 值
        let cksum_str = std::str::from_utf8(&header[148..154]).assert_value("valid utf8");
        let stored_cksum = u64::from_str_radix(cksum_str.trim_start_matches('0'), 8).unwrap_or(0);

        assert_eq!(stored_cksum, expected);
    }

    #[test]
    fn test_calculate_tar_size_empty() {
        let entries: Vec<ObservedEntry> = vec![];
        assert_eq!(
            calculate_tar_size(&entries).assert_value("archive size"),
            1024
        ); // only EOF marker
    }

    #[test]
    fn test_tar_padding() {
        assert!(tar_padding(512).is_none());
        assert!(tar_padding(1024).is_none());

        let padding = tar_padding(100).assert_value("should have padding");
        assert_eq!(padding.len(), 412); // 512 - 100

        let padding = tar_padding(513).assert_value("should have padding");
        assert_eq!(padding.len(), 511); // 1024 - 513
    }

    #[test]
    fn test_tar_eof_marker() {
        let eof = tar_eof_marker();
        assert_eq!(eof.len(), 1024);
        assert!(eof.iter().all(|&b| b == 0));
    }
}
