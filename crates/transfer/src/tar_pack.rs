//! Bounded tar composition over the source and destination roles.
//!
//! Every file is read through its identity-bound source stream. The archive is written through
//! the expert destination lifecycle, so publication and read-back verification use the same
//! optimized backend implementations as ordinary transfers. No complete file is buffered.

use crate::model::{
    EntryKind, EntryOperationFailure, FailureClass, IdentityStrength, ObservedEntry, Operation,
    PrincipalIdentity, SourceIdentity, SourceVersion, StoragePath, Transience,
    without_unowned_set_id,
};
use crate::storage::{
    ByteStream, ReadRequest, ReadSource, SourceDescriptor, SourceQosBudget, SourceQosGroup,
    SourceQosStats, Storage, StorageRoleFailure,
};
use crate::transfer::{
    ExpertDestinationRequest, ExpertDestinationSession, ExpertSourceEvidence, TransferFailure,
    TransferOutcome, TransferPolicy, validate_observation,
};
use bytes::{Bytes, BytesMut};
use futures::StreamExt as _;
use std::borrow::Borrow;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

#[derive(Debug, thiserror::Error)]
pub enum TarFailure {
    #[error("invalid archive: {0}")]
    Invalid(String),
    #[error(transparent)]
    Storage(#[from] StorageRoleFailure),
    /// Retains the original transfer failure, publication status, and any failed artifact cleanup.
    #[error(transparent)]
    Transfer(Box<TransferFailure>),
}

/// Archive inputs. Owned observations and shared `Arc<ObservedEntry>` inputs are accepted
/// without cloning their metadata. The immutable manifest is shared as one Arc slice and retained
/// until the stream finishes; packaging and later manifest persistence can share it without
/// cloning each observation handle. Entry paths are also their names in the archive.
/// Request ownership and timestamps during traversal to preserve them in the tar headers.
#[derive(Clone)]
pub struct TarRequest<E = ObservedEntry> {
    pub source: Storage,
    pub destination: Storage,
    pub entries: Arc<[E]>,
    pub final_path: StoragePath,
    pub cancel: CancellationToken,
    pub source_qos: Option<SourceQosGroup>,
    /// 归档由完整清单生成，支持 `Direct` 或 `AtomicReplace`；不支持文件内续传。
    pub transfer_policy: TransferPolicy,
}

/// Checked archive size, including headers, padding and the two end blocks.
///
/// # Errors
/// Refuses unsupported entries, unknown file sizes and arithmetic overflow.
pub fn calculate_tar_size<E: Borrow<ObservedEntry>>(entries: &[E]) -> Result<u64, TarFailure> {
    entries.iter().try_fold(1024_u64, |total, entry| {
        let entry = entry.borrow();
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

/// 按调用方指定的 `Direct` 或 `AtomicReplace` 策略流式写入归档。
///
/// # Errors
/// `Checkpointed` 在准备目标前明确拒绝，生成的归档流不提供文件内续传。
/// 每个源文件读取前后复核观察身份；Direct 失败可能留下已写入的最终内容。
/// 返回原始传输原因，按对应策略清理暂存；不回滚已发布的最终对象。
pub async fn pack<E: Borrow<ObservedEntry> + Send + Sync + 'static>(
    request: TarRequest<E>,
) -> Result<TransferOutcome, TarFailure> {
    let policy = request.transfer_policy;
    if policy == TransferPolicy::Checkpointed {
        return Err(invalid(
            "generated archives do not support Checkpointed transfers",
        ));
    }
    let size = calculate_tar_size(&request.entries)?;
    let observed = archive_observation(&request, size)?;
    let identity_key = observed.identity_key();
    let source = request
        .source
        .read_source()
        .map_err(|error| invalid(&error.to_string()))?;
    let maximum = source.maximum_read_chunk_bytes().max(512);
    let destination = prepare_archive(&request, observed, maximum).await?;
    let budget = request
        .source_qos
        .as_ref()
        .map(SourceQosGroup::transfer_budget);
    let evidence = Arc::new(Mutex::new(ArchiveEvidence::default()));
    let tally = Arc::clone(&evidence);
    let state = ArchiveStream {
        source,
        output: None,
        entries: request.entries,
        next_entry: 0,
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
        let mut tally = tally
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &piece {
            Ok(bytes) => {
                tally.hash.update(bytes);
                tally.bytes += bytes.len() as u64;
            }
            Err(error) => tally.source_failure = Some(Box::new(error.clone())),
        }
        piece
    });
    let written = destination.write(Box::pin(stream)).await;
    let source_failure = evidence
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .source_failure
        .take();
    let written = match source_failure {
        Some(cause) => written.map_err(|failure| failure.attribute_source_input(cause.as_ref())),
        None => written,
    };
    let transferred = finish_archive_phase(written, policy).await?;
    let (digest, actual) = {
        let tally = evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (*tally.hash.finalize().as_bytes(), tally.bytes)
    };
    // Destination write also checks the complete expected length before returning.
    debug_assert_eq!(actual, size);
    if let Some(budget) = &budget {
        budget.set_logical_bytes(size);
    }
    let completed = transferred
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
        .await;
    finish_archive_phase(completed, policy).await
}

async fn prepare_archive<E: Borrow<ObservedEntry> + Send + Sync>(
    request: &TarRequest<E>,
    observed: ObservedEntry,
    maximum: usize,
) -> Result<ExpertDestinationSession, TarFailure> {
    let inputs = request.entries.iter().map(|entry| {
        let entry = entry.borrow();
        SourceDescriptor::new(
            entry.path().clone(),
            entry.kind(),
            entry.size(),
            entry.source_identity().clone(),
        )
    });
    let prepared = ExpertDestinationSession::prepare_generated(
        ExpertDestinationRequest::new(
            observed,
            maximum,
            request.destination.clone(),
            request.final_path.clone(),
            request.cancel.clone(),
        )
        .with_transfer_policy(request.transfer_policy),
        Some(Box::new(inputs)),
    )
    .await;
    finish_archive_phase(prepared, request.transfer_policy).await
}

fn archive_observation<E: Borrow<ObservedEntry>>(
    request: &TarRequest<E>,
    size: u64,
) -> Result<ObservedEntry, TarFailure> {
    let mut manifest = blake3::Hasher::new();
    manifest.update(b"data-mover/tar/v1\0");
    manifest.update(request.final_path.as_str().as_bytes());
    manifest.update(b"\0");
    for entry in request.entries.iter() {
        let entry = entry.borrow();
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

#[derive(Default)]
struct ArchiveEvidence {
    hash: blake3::Hasher,
    bytes: u64,
    source_failure: Option<Box<StorageRoleFailure>>,
}

struct ArchiveStream<E> {
    output: Option<Bytes>,
    source: Arc<dyn ReadSource>,
    entries: Arc<[E]>,
    next_entry: usize,
    pending: Option<usize>,
    reading: Option<(ByteStream, usize)>,
    remaining: u64,
    padding: Option<Bytes>,
    eof: bool,
    maximum: usize,
    cancel: CancellationToken,
    budget: Option<SourceQosBudget>,
    path: StoragePath,
}

async fn next_piece<E: Borrow<ObservedEntry> + Send + Sync + 'static>(
    mut state: ArchiveStream<E>,
) -> Result<Option<(Bytes, ArchiveStream<E>)>, StorageRoleFailure> {
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

async fn next_raw<E: Borrow<ObservedEntry> + Send + Sync + 'static>(
    mut state: ArchiveStream<E>,
) -> Result<Option<(Bytes, ArchiveStream<E>)>, StorageRoleFailure> {
    loop {
        if state.cancel.is_cancelled() {
            return Err(stream_failure(&state.path, FailureClass::Cancelled));
        }
        if let Some((reading, index)) = &mut state.reading {
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
            describe_archive_source(state.source.as_ref(), state.entries[*index].borrow()).await?;
            state.reading = None;
        }
        if let Some(padding) = state.padding.take() {
            return Ok(Some((padding, state)));
        }
        if let Some(index) = state.pending.take() {
            let entry = state.entries[index].borrow();
            let size = entry
                .size()
                .ok_or_else(|| stream_failure(entry.path(), FailureClass::InvalidInput))?;
            let descriptor = describe_archive_source(state.source.as_ref(), entry).await?;
            state.remaining = size;
            state.padding = tar_padding(size);
            state.reading = Some((
                state
                    .source
                    .read(ReadRequest {
                        path: entry.path().clone(),
                        range: Some(0..size),
                        expected_source: Some(descriptor.source_identity),
                        maximum_chunk_bytes: state.maximum,
                        read_inflight: state.source.read_concurrency().max(1),
                        read_budget: None,
                        cancel: state.cancel.clone(),
                        source_qos: state.budget.clone(),
                        version: descriptor.version,
                    })
                    .await?,
                index,
            ));
            continue;
        }
        if let Some(entry) = state.entries.get(state.next_entry) {
            let index = state.next_entry;
            state.next_entry += 1;
            let observed = entry.borrow();
            let header = build_header_for_entry(observed)
                .map_err(|_| stream_failure(observed.path(), FailureClass::InvalidInput))?;
            if observed.kind() == EntryKind::File {
                state.pending = Some(index);
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

async fn describe_archive_source(
    source: &dyn ReadSource,
    observation: &ObservedEntry,
) -> Result<SourceDescriptor, StorageRoleFailure> {
    let descriptor = source
        .describe_version(observation.path(), &SourceVersion::Current)
        .await?;
    validate_observation(source, observation, &descriptor)
        .map_err(|_| stream_failure(observation.path(), FailureClass::Conflict))?;
    Ok(descriptor)
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
async fn finish_archive_phase<T>(
    result: Result<T, TransferFailure>,
    policy: TransferPolicy,
) -> Result<T, TarFailure> {
    match result {
        Ok(value) => Ok(value),
        Err(failure) => Err(TarFailure::Transfer(Box::new(
            failure.cleanup_for_policy(policy).await,
        ))),
    }
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
    let (mode, uid, gid) = observed_header_ownership(entry);
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
fn observed_header_ownership(entry: &ObservedEntry) -> (u32, u32, u32) {
    let (mode, uid, gid) = if let Some(facts) = entry.permissions() {
        // An unknown facet stays unknown; named principals have no numeric mapping here.
        let numeric = |principal: Option<&PrincipalIdentity>| match principal {
            Some(PrincipalIdentity::Numeric(id)) => Some(*id),
            _ => None,
        };
        (
            facts.mode.unwrap_or(0),
            numeric(facts.owner.as_ref()),
            numeric(facts.group.as_ref()),
        )
    } else {
        let owner = entry.metadata().ownership_mode().value();
        (
            owner.map_or(0, |owner| owner.mode),
            owner.map(|owner| owner.uid),
            owner.map(|owner| owner.gid),
        )
    };
    header_ownership(mode, uid, gid, entry.kind() == EntryKind::Directory)
}

fn header_ownership(
    mode: u32,
    uid: Option<u32>,
    gid: Option<u32>,
    is_dir: bool,
) -> (u32, u32, u32) {
    (
        without_unowned_set_id(mode & 0o7777, uid.is_some(), gid.is_some(), is_dir),
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
#[path = "tar_pack/permission_tests.rs"]
mod permission_tests;

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
