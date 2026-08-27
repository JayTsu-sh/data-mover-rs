use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use data_mover::dir_tree::NdxEvent;
use data_mover::dir_tree::{DirHandle, ReadContext};
use data_mover::hdfs::HdfsScanEvent;
use data_mover::{
    BackendConfig, CopyOptions, CreateStorageOptions, HdfsConfig, IntegrityCheck,
    IntegrityCheckMode, MismatchDataField, ResumeContext, StorageEntryMessage, StorageEnum,
    StreamHandle, TransferConcurrency, WalkOptions, build_hdfs_client, create_storage,
};
use hdfs_native::file::{FileReader, FileWriter};
use hdfs_native::{Client, ClientBuilder, WriteOptions};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

#[cfg(unix)]
fn local_mode(metadata: &std::fs::Metadata) -> u32 {
    metadata.permissions().mode() & 0o7777
}

#[cfg(windows)]
fn local_mode(_: &std::fs::Metadata) -> u32 {
    0
}

#[cfg(unix)]
fn local_mtime(metadata: &std::fs::Metadata) -> i64 {
    metadata.mtime()
}

#[cfg(windows)]
fn local_mtime(_: &std::fs::Metadata) -> i64 {
    0
}

#[cfg(unix)]
fn local_owner(metadata: &std::fs::Metadata) -> (u32, u32) {
    (metadata.uid(), metadata.gid())
}

#[cfg(windows)]
fn local_owner(_: &std::fs::Metadata) -> (u32, u32) {
    (0, 0)
}

#[cfg(unix)]
async fn set_local_mode(path: &std::path::Path, mode: u32) -> std::io::Result<()> {
    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).await
}

#[cfg(windows)]
async fn set_local_mode(_: &std::path::Path, _: u32) -> std::io::Result<()> {
    Ok(())
}

fn valid_hdfs_lab_run_id(run_id: &str) -> bool {
    let Some((kind, suffix)) = run_id.split_once('-') else {
        return false;
    };
    matches!(kind, "nightly" | "release")
        && !suffix.is_empty()
        && suffix.len() <= 80
        && !suffix.contains("..")
        && suffix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn validated_hdfs_lab_root(location: &str) -> Result<data_mover::HdfsLocation, String> {
    let authority_end = location
        .find("://")
        .and_then(|offset| {
            location[offset + 3..]
                .find('/')
                .map(|path| offset + 3 + path)
        })
        .ok_or_else(|| "HDFS lab root must contain an explicit path".to_string())?;
    if location[authority_end..].contains("//") {
        return Err("HDFS lab root must not contain empty path components".to_string());
    }
    let parsed = data_mover::HdfsLocation::parse(location).map_err(|error| error.to_string())?;
    let components = parsed
        .root()
        .split('/')
        .filter(|component| !component.is_empty())
        .collect::<Vec<_>>();
    let ["tmp", parent, run_id, "hdfs"] = components.as_slice() else {
        return Err("HDFS lab root must have the fixed confined shape".to_string());
    };
    let expected_parent = if run_id.starts_with("nightly-") {
        "data-mover-nightly"
    } else if run_id.starts_with("release-") {
        "data-mover-release"
    } else {
        return Err("HDFS lab root has an invalid run kind".to_string());
    };
    if *parent != expected_parent || !valid_hdfs_lab_run_id(run_id) {
        return Err("HDFS lab root does not match its validated run id".to_string());
    }
    Ok(parsed)
}

fn hdfs_lab_root() -> Result<data_mover::HdfsLocation, Box<dyn std::error::Error>> {
    let location = std::env::var("LAB_HDFS_RUN_ROOT")?;
    validated_hdfs_lab_root(&location).map_err(Into::into)
}

fn hdfs_lab_config() -> HdfsConfig {
    let config_dir = std::env::var_os("LAB_HDFS_CONFIG_DIR").map(Into::into);
    let kerberos_credentials =
        std::env::var_os("LAB_HDFS_KEYTAB").map(|keytab| data_mover::HdfsKerberosCredentials {
            keytab: Some(keytab.into()),
            ..Default::default()
        });
    HdfsConfig {
        config_dir,
        kerberos_credentials,
        ..Default::default()
    }
}

fn hdfs_short_user(user: &str) -> &str {
    user.split_once('/')
        .or_else(|| user.split_once('@'))
        .map_or(user, |(short, _)| short)
}

fn hdfs_lab_location(case: &str) -> Result<String, Box<dyn std::error::Error>> {
    let root = hdfs_lab_root()?;
    hdfs_lab_location_as(case, root.user())
}

fn hdfs_lab_location_as(case: &str, user: &str) -> Result<String, Box<dyn std::error::Error>> {
    if case.is_empty()
        || !case
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err("unsafe HDFS lab case name".into());
    }
    if user.is_empty() {
        return Err("empty HDFS lab user".into());
    }
    let root = hdfs_lab_root()?;
    let endpoint = root
        .endpoint()
        .strip_prefix("hdfs://")
        .ok_or("invalid HDFS endpoint")?;
    let user = percent_encoding::utf8_percent_encode(user, percent_encoding::NON_ALPHANUMERIC);
    Ok(format!("hdfs://{user}@{endpoint}{}/{case}", root.root()))
}

fn hdfs_lab_path(case: &str) -> Result<String, Box<dyn std::error::Error>> {
    let location = hdfs_lab_location(case)?;
    Ok(data_mover::HdfsLocation::parse(&location)?
        .root()
        .to_string())
}

fn hdfs_lab_local_path(case: &str) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    if case.is_empty()
        || !case
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err("unsafe local HDFS lab case name".into());
    }
    let root = hdfs_lab_root()?;
    let run_id = root
        .root()
        .split('/')
        .nth(3)
        .ok_or("validated HDFS run root lost its run id")?;
    Ok(std::env::temp_dir()
        .join("data-mover-lab")
        .join(run_id)
        .join("hdfs")
        .join(case))
}

async fn create_empty_hdfs_files(
    storage: &data_mover::HDFSStorage,
    paths: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    for path in paths {
        let path = storage.resolve_path(std::path::Path::new(path))?;
        let mut writer = storage
            .client()
            .create(&path, hdfs_native::WriteOptions::default())
            .await?;
        Box::pin(writer.close()).await?;
    }
    Ok(())
}

async fn create_hdfs_file(
    storage: &data_mover::HDFSStorage,
    relative_path: &str,
    data: bytes::Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = storage.resolve_path(std::path::Path::new(relative_path))?;
    let mut writer = storage
        .client()
        .create(&path, hdfs_native::WriteOptions::default().overwrite(true))
        .await?;
    Box::pin(writer.write_bytes(data)).await?;
    Box::pin(writer.close()).await?;
    Ok(())
}

fn assert_hdfs_resume_handle(
    handle: StreamHandle,
    expected_path: &str,
    expected_prefix: u64,
    expected_size: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let StreamHandle::Hdfs {
        part_path,
        prefix_len,
        expected_size: size,
    } = handle
    else {
        return Err("resume preparation did not return an HDFS handle".into());
    };
    assert_eq!(part_path, std::path::Path::new(expected_path));
    assert_eq!(prefix_len, expected_prefix);
    assert_eq!(size, expected_size);
    Ok(())
}

async fn write_hdfs_resume_tail(
    storage: &StorageEnum,
    entry: &data_mover::EntryEnum,
    handle: &StreamHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    let ranges = Arc::new(Mutex::new(Vec::new()));
    let callback_ranges = ranges.clone();
    let callback: data_mover::CommitCallback = Arc::new(move |offset, length| {
        if let Ok(mut ranges) = callback_ranges.lock() {
            ranges.push((offset, length));
        }
    });
    let counter = Arc::new(AtomicU64::new(0));
    let storage = storage.clone();
    let entry = entry.clone();
    let handle = handle.clone();
    let counter_for_writer = counter.clone();
    let writer = tokio::spawn(async move {
        StorageEnum::write_chunk_stream(
            &storage,
            &entry,
            receiver,
            &handle,
            Some(counter_for_writer),
            callback,
        )
        .await
    });
    sender
        .send(data_mover::DataChunk {
            offset: 8,
            data: bytes::Bytes::from_static(b"89abcdef"),
        })
        .await?;
    sender
        .send(data_mover::DataChunk {
            offset: 4,
            data: bytes::Bytes::from_static(b"4567"),
        })
        .await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while counter.load(Ordering::Relaxed) != 12 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| "HDFS resume writer did not consume the open session")?;
    assert!(
        ranges
            .lock()
            .map_err(|_| "callback ranges poisoned")?
            .is_empty(),
        "HDFS resume progress must wait for writer close"
    );
    drop(sender);
    writer.await??;
    assert_eq!(counter.load(Ordering::Relaxed), 12);
    assert_eq!(
        ranges
            .lock()
            .map_err(|_| "callback ranges poisoned")?
            .as_slice(),
        &[(4, 12)]
    );
    Ok(())
}

async fn assert_hdfs_resume_noop(
    storage: &StorageEnum,
    entry: &data_mover::EntryEnum,
    handle: &StreamHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    drop(sender);
    let callbacks = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let callback_count = callbacks.clone();
    let callback: data_mover::CommitCallback = std::sync::Arc::new(move |_, _| {
        callback_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    });
    let bytes = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    Box::pin(StorageEnum::write_chunk_stream(
        storage,
        entry,
        receiver,
        handle,
        Some(bytes.clone()),
        callback,
    ))
    .await?;
    assert_eq!(bytes.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(callbacks.load(std::sync::atomic::Ordering::Relaxed), 0);
    Ok(())
}

async fn complete_and_commit_hdfs_resume(
    storage: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
    entry: &data_mover::EntryEnum,
    short_handle: &StreamHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    assert!(
        StorageEnum::commit_chunk_stream(storage, entry, 16, short_handle.clone())
            .await
            .is_err()
    );
    let old_final = hdfs.open_file(std::path::Path::new("最终.bin")).await?;
    assert_eq!(
        hdfs.read_at(&old_final, 0, 16).await?,
        b"old-final-data!!"[..]
    );
    Box::pin(write_hdfs_resume_tail(storage, entry, short_handle)).await?;
    let part = std::path::Path::new("临时/最终.bin.part");
    let (missing, handle) = StorageEnum::resume_prepare(storage, entry, part, true).await?;
    assert!(missing.is_empty());
    assert_hdfs_resume_handle(handle.clone(), "临时/最终.bin.part", 16, 16)?;
    Box::pin(assert_hdfs_resume_noop(storage, entry, &handle)).await?;
    StorageEnum::commit_chunk_stream(storage, entry, 16, handle).await?;
    assert!(storage.get_metadata(part).await.is_err());
    let committed = hdfs.open_file(std::path::Path::new("最终.bin")).await?;
    assert_eq!(
        hdfs.read_at(&committed, 0, 16).await?,
        b"0123456789abcdef"[..]
    );
    Ok(())
}

async fn assert_stable_hdfs_resume_binding(
    hdfs: &data_mover::HDFSStorage,
    entry: &data_mover::EntryEnum,
) -> Result<(), Box<dyn std::error::Error>> {
    let stable_request = data_mover::hdfs_transfer_request(
        entry,
        "nightly-resume-transfer",
        std::path::PathBuf::from("稳定/最终.bin"),
    )?;
    let stable = hdfs
        .prepare_stable_tail_transfer(&stable_request, true)
        .await?;
    assert_eq!(stable.prefix_len(), 0);
    create_hdfs_file(
        hdfs,
        stable
            .part_path()
            .to_str()
            .ok_or("stable HDFS partial path is not UTF-8")?,
        bytes::Bytes::from_static(b"0123"),
    )
    .await?;
    let resumed = hdfs
        .prepare_stable_tail_transfer(&stable_request, true)
        .await?;
    assert_eq!(resumed.prefix_len(), 4);

    let changed_source = data_mover::hdfs::HdfsTransferRequest::new(
        "nightly-resume-transfer",
        data_mover::hdfs::HdfsSourceFingerprint::new(16, entry.get_mtime() + 1, None),
        std::path::PathBuf::from("稳定/最终.bin"),
        16,
        0o640,
        Some(2),
    )?;
    assert_ne!(stable_request.partial_path(), changed_source.partial_path());
    let changed = hdfs
        .prepare_stable_tail_transfer(&changed_source, true)
        .await?;
    assert_eq!(changed.prefix_len(), 0);
    Ok(())
}

async fn assert_hdfs_staged_resume_modes(
    hdfs: &data_mover::HDFSStorage,
    entry: &data_mover::EntryEnum,
) -> Result<(), Box<dyn std::error::Error>> {
    use data_mover::hdfs::HdfsResumeMode;

    let request = data_mover::hdfs_transfer_request(
        entry,
        "nightly-mode-transfer",
        std::path::PathBuf::from("模式/最终.bin"),
    )?;
    assert!(
        hdfs.prepare_staged_tail_transfer(&request, HdfsResumeMode::Require)
            .await
            .is_err()
    );
    let initial = hdfs
        .prepare_staged_tail_transfer(&request, HdfsResumeMode::Auto)
        .await?;
    assert_eq!(initial.prefix_len(), 0);
    create_hdfs_file(
        hdfs,
        initial.part_path().to_str().ok_or("invalid partial path")?,
        bytes::Bytes::from_static(b"0123"),
    )
    .await?;
    let automatic = hdfs
        .prepare_staged_tail_transfer(&request, HdfsResumeMode::Auto)
        .await?;
    assert_staged_tail_commit(hdfs, &request, automatic, b"0123456789abcdef").await?;

    let restarted = hdfs
        .prepare_staged_tail_transfer(&request, HdfsResumeMode::Restart)
        .await?;
    assert_eq!(restarted.prefix_len(), 0);
    assert_staged_tail_commit(hdfs, &request, restarted, b"0123456789abcdef").await?;

    create_hdfs_file(
        hdfs,
        request
            .partial_path()
            .to_str()
            .ok_or("invalid partial path")?,
        bytes::Bytes::from_static(b"0123"),
    )
    .await?;
    let required = hdfs
        .prepare_staged_tail_transfer(&request, HdfsResumeMode::Require)
        .await?;
    assert_eq!(required.prefix_len(), 4);
    assert_staged_tail_commit(hdfs, &request, required, b"0123456789abcdef").await?;

    create_hdfs_file(
        hdfs,
        request
            .partial_path()
            .to_str()
            .ok_or("invalid partial path")?,
        bytes::Bytes::from_static(b"0123456789abcdefx"),
    )
    .await?;
    assert!(
        hdfs.prepare_staged_tail_transfer(&request, HdfsResumeMode::Require)
            .await
            .is_err()
    );
    assert_eq!(hdfs.get_metadata(request.partial_path()).await?.size, 17);
    let recovered = hdfs
        .prepare_staged_tail_transfer(&request, HdfsResumeMode::Auto)
        .await?;
    assert_eq!(recovered.prefix_len(), 0);
    hdfs.delete_file(request.partial_path()).await?;
    hdfs.create_dir_all(request.partial_path(), 0o755).await?;
    for mode in [
        HdfsResumeMode::Auto,
        HdfsResumeMode::Restart,
        HdfsResumeMode::Require,
    ] {
        assert!(
            hdfs.prepare_staged_tail_transfer(&request, mode)
                .await
                .is_err()
        );
        assert!(hdfs.get_metadata(request.partial_path()).await?.is_dir);
    }
    Ok(())
}

async fn assert_staged_tail_commit(
    hdfs: &data_mover::HDFSStorage,
    request: &data_mover::hdfs::HdfsTransferRequest,
    state: data_mover::hdfs::HdfsPreparedTransfer,
    payload: &'static [u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let prefix = usize::try_from(state.prefix_len())?;
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    if prefix < payload.len() {
        sender
            .send(data_mover::DataChunk {
                offset: state.prefix_len(),
                data: bytes::Bytes::from_static(&payload[prefix..]),
            })
            .await?;
    }
    drop(sender);
    hdfs.append_prepared_tail(receiver, &state, None, None)
        .await?;
    hdfs.commit_prepared_tail(&state, request.final_path())
        .await?;
    let handle = hdfs.open_file(request.final_path()).await?;
    assert_eq!(
        hdfs.read_at(&handle, 0, payload.len() as u64).await?,
        payload
    );
    Ok(())
}

async fn assert_zero_byte_staged_commit(
    hdfs: &data_mover::HDFSStorage,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = data_mover::hdfs::HdfsTransferRequest::new(
        "nightly-zero-transfer",
        data_mover::hdfs::HdfsSourceFingerprint::new(0, 0, None),
        std::path::PathBuf::from("模式/zero.bin"),
        0,
        0o640,
        Some(2),
    )?;
    let state = hdfs
        .prepare_staged_tail_transfer(&request, data_mover::hdfs::HdfsResumeMode::Auto)
        .await?;
    assert_eq!(state.missing_tail(), None);
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    drop(sender);
    hdfs.append_prepared_tail(receiver, &state, None, None)
        .await?;
    hdfs.commit_prepared_tail(&state, request.final_path())
        .await?;
    assert_eq!(hdfs.get_metadata(request.final_path()).await?.size, 0);
    Ok(())
}

async fn assert_common_recoverable_hdfs_copy(
    destination: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
) -> Result<(), Box<dyn std::error::Error>> {
    let local_root = hdfs_lab_local_path("recoverable-copy")?;
    tokio::fs::create_dir_all(&local_root).await?;
    let payload = b"common recoverable HDFS copy";
    tokio::fs::write(local_root.join("common.bin"), payload).await?;
    let source = create_storage(
        local_root.to_str().ok_or("invalid local lab path")?,
        CreateStorageOptions::new(None, true),
    )
    .await?;
    let entry = source
        .get_metadata(std::path::Path::new("common.bin"))
        .await?;
    let mut identity_hasher = blake3::Hasher::new();
    identity_hasher.update(b"data-mover:hdfs-default-copy-identity:v1\0");
    identity_hasher.update(b"local\0");
    identity_hasher.update(local_root.to_string_lossy().as_bytes());
    identity_hasher.update(b"\0common.bin");
    let identity_digest = identity_hasher.finalize().to_hex();
    let identity = format!("default-copy-{}", &identity_digest[..32]);
    let request = data_mover::hdfs_transfer_request(
        &entry,
        &identity,
        entry.get_relative_path().to_path_buf(),
    )?;
    let split = 9_usize;
    create_hdfs_file(
        hdfs,
        request
            .partial_path()
            .to_str()
            .ok_or("invalid deterministic partial path")?,
        bytes::Bytes::copy_from_slice(&payload[..split]),
    )
    .await?;
    assert!(hdfs.get_metadata(request.final_path()).await.is_err());
    let bytes_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    StorageEnum::copy_file(
        &source,
        destination,
        &entry,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: true,
            bytes_counter: Some(bytes_counter.clone()),
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(
        bytes_counter.load(std::sync::atomic::Ordering::Relaxed),
        u64::try_from(payload.len() - split)?
    );
    assert!(hdfs.get_metadata(request.partial_path()).await.is_err());
    let handle = hdfs.open_file(std::path::Path::new("common.bin")).await?;
    assert_eq!(
        hdfs.read_at(&handle, 0, payload.len() as u64).await?,
        &payload[..]
    );
    tokio::fs::remove_dir_all(local_root).await?;
    Ok(())
}

async fn assert_cancelled_hdfs_partial_disposition(
    destination: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
) -> Result<(), Box<dyn std::error::Error>> {
    let local_root = hdfs_lab_local_path("cancel-disposition")?;
    tokio::fs::create_dir_all(&local_root).await?;
    let source = create_storage(
        local_root.to_str().ok_or("invalid local lab path")?,
        CreateStorageOptions::new(None, true),
    )
    .await?;
    for (name, disposition, partial_exists) in [
        (
            "cancel-preserve.bin",
            data_mover::hdfs::HdfsCancellationDisposition::Preserve,
            true,
        ),
        (
            "cancel-discard.bin",
            data_mover::hdfs::HdfsCancellationDisposition::Discard,
            false,
        ),
    ] {
        let payload = b"cancel after HDFS writer close";
        tokio::fs::write(local_root.join(name), payload).await?;
        let entry = source.get_metadata(std::path::Path::new(name)).await?;
        let identity = format!("nightly-{name}");
        let request = data_mover::hdfs_transfer_request(
            &entry,
            &identity,
            entry.get_relative_path().to_path_buf(),
        )?;
        let cancel = tokio_util::sync::CancellationToken::new();
        let callback_cancel = cancel.clone();
        let callback: data_mover::CommitCallback = Arc::new(move |_, _| {
            callback_cancel.cancel();
        });
        let result = StorageEnum::copy_file_hdfs_recoverable(
            &source,
            destination,
            &entry,
            CopyOptions {
                cancel: Some(cancel),
                is_source_reserved: true,
                ..Default::default()
            },
            data_mover::HdfsRecoverableCopyOptions::new(identity, callback)
                .with_cancellation_disposition(disposition),
        )
        .await;
        assert!(matches!(
            result,
            Err(data_mover::error::StorageError::Cancelled)
        ));
        assert!(hdfs.get_metadata(request.final_path()).await.is_err());
        assert_eq!(
            hdfs.get_metadata(request.partial_path()).await.is_ok(),
            partial_exists
        );
        let sibling = std::path::Path::new("unrelated-transfer.part");
        if hdfs.get_metadata(sibling).await.is_err() {
            create_hdfs_file(
                hdfs,
                "unrelated-transfer.part",
                bytes::Bytes::from_static(b"keep"),
            )
            .await?;
        }
        assert_eq!(hdfs.get_metadata(sibling).await?.size, 4);
    }
    assert_prepared_state_survives_preparation_cancellation(
        &local_root,
        &source,
        destination,
        hdfs,
    )
    .await?;
    tokio::fs::remove_dir_all(local_root).await?;
    Ok(())
}

async fn assert_prepared_state_survives_preparation_cancellation(
    local_root: &std::path::Path,
    source: &StorageEnum,
    destination: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
) -> Result<(), Box<dyn std::error::Error>> {
    let name = "cancel-before-prepare.bin";
    let payload = b"existing state must survive pre-prepare cancellation";
    tokio::fs::write(local_root.join(name), payload).await?;
    let entry = source.get_metadata(std::path::Path::new(name)).await?;
    let identity = "nightly-cancel-before-prepare";
    let request = data_mover::hdfs_transfer_request(
        &entry,
        identity,
        entry.get_relative_path().to_path_buf(),
    )?;
    create_hdfs_file(
        hdfs,
        request
            .partial_path()
            .to_str()
            .ok_or("invalid cancellation partial path")?,
        bytes::Bytes::copy_from_slice(payload),
    )
    .await?;
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    let callback: data_mover::CommitCallback = Arc::new(|_, _| {});
    let result = StorageEnum::copy_file_hdfs_recoverable(
        source,
        destination,
        &entry,
        CopyOptions {
            cancel: Some(cancel),
            is_source_reserved: true,
            ..Default::default()
        },
        data_mover::HdfsRecoverableCopyOptions::new(identity, callback)
            .with_cancellation_disposition(data_mover::hdfs::HdfsCancellationDisposition::Discard),
    )
    .await;
    assert!(matches!(
        result,
        Err(data_mover::error::StorageError::Cancelled)
    ));
    assert_eq!(
        hdfs.get_metadata(request.partial_path()).await?.size,
        u64::try_from(payload.len())?
    );
    Ok(())
}

async fn assert_public_copy_preserves_mid_transfer_cancellation(
    destination: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
) -> Result<(), Box<dyn std::error::Error>> {
    let local_root = hdfs_lab_local_path("public-cancel-preserve")?;
    tokio::fs::create_dir_all(&local_root).await?;
    let name = "public-cancel.bin";
    tokio::fs::write(local_root.join(name), vec![0x5a; 4 * 1024 * 1024]).await?;
    let source = create_storage(
        local_root.to_str().ok_or("invalid local lab path")?,
        CreateStorageOptions::new(Some(64 * 1024), true),
    )
    .await?;
    let entry = source.get_metadata(std::path::Path::new(name)).await?;
    let mut identity_hasher = blake3::Hasher::new();
    identity_hasher.update(b"data-mover:hdfs-default-copy-identity:v1\0local\0");
    identity_hasher.update(local_root.to_string_lossy().as_bytes());
    identity_hasher.update(b"\0public-cancel.bin");
    let identity_digest = identity_hasher.finalize().to_hex();
    let request = data_mover::hdfs_transfer_request(
        &entry,
        &format!("default-copy-{}", &identity_digest[..32]),
        entry.get_relative_path().to_path_buf(),
    )?;
    create_hdfs_file(
        hdfs,
        request
            .partial_path()
            .to_str()
            .ok_or("invalid public cancellation partial path")?,
        bytes::Bytes::from_static(b"trusted-prefix"),
    )
    .await?;
    let cancel = tokio_util::sync::CancellationToken::new();
    let copy_cancel = cancel.clone();
    let source_copy = source.clone();
    let destination_copy = destination.clone();
    let entry_copy = entry.clone();
    let copy = tokio::spawn(async move {
        StorageEnum::copy_file(
            &source_copy,
            &destination_copy,
            &entry_copy,
            CopyOptions {
                qos: Some(data_mover::QosManager::try_new_with_burst(
                    "64KiB/s", 4096, None,
                )?),
                cancel: Some(copy_cancel),
                is_source_reserved: true,
                ..Default::default()
            },
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    cancel.cancel();
    assert!(matches!(
        copy.await?,
        Err(data_mover::error::StorageError::Cancelled)
    ));
    assert!(hdfs.get_metadata(request.final_path()).await.is_err());
    assert!(hdfs.get_metadata(request.partial_path()).await?.size >= 14);
    tokio::fs::remove_dir_all(local_root).await?;
    Ok(())
}

async fn assert_changed_source_is_not_published(
    destination: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
) -> Result<(), Box<dyn std::error::Error>> {
    let local_root = hdfs_lab_local_path("changed-source")?;
    tokio::fs::create_dir_all(&local_root).await?;
    let source_path = local_root.join("changed-source.bin");
    tokio::fs::write(&source_path, b"original-source").await?;
    let source = create_storage(
        local_root.to_str().ok_or("invalid local lab path")?,
        CreateStorageOptions::new(None, true),
    )
    .await?;
    let entry = source
        .get_metadata(std::path::Path::new("changed-source.bin"))
        .await?;
    let request = data_mover::hdfs_transfer_request(
        &entry,
        "nightly-changed-source",
        entry.get_relative_path().to_path_buf(),
    )?;
    let callback_path = source_path.clone();
    let callback: data_mover::CommitCallback = Arc::new(move |_, _| {
        std::fs::write(&callback_path, b"changed-source-content")
            .unwrap_or_else(|error| panic!("change source after HDFS append: {error}"));
    });

    let result = StorageEnum::copy_file_hdfs_recoverable(
        &source,
        destination,
        &entry,
        CopyOptions {
            is_source_reserved: false,
            ..Default::default()
        },
        data_mover::HdfsRecoverableCopyOptions::new("nightly-changed-source", callback)
            .with_cancellation_disposition(data_mover::hdfs::HdfsCancellationDisposition::Discard),
    )
    .await;

    let Err(error) = result else {
        panic!("changed source was unexpectedly published");
    };
    assert!(
        error
            .to_string()
            .contains("source changed after preparation")
    );
    assert!(source_path.exists());
    assert_eq!(
        hdfs.get_metadata(request.partial_path()).await?.size,
        entry.get_size()
    );
    assert!(
        hdfs.get_metadata(std::path::Path::new("changed-source.bin"))
            .await
            .is_err()
    );
    tokio::fs::remove_dir_all(local_root).await?;
    Ok(())
}

async fn assert_hdfs_overwrite_rename(
    storage: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
) -> Result<(), Box<dyn std::error::Error>> {
    Box::pin(create_hdfs_file(
        hdfs,
        "replacement.bin",
        bytes::Bytes::from_static(b"replacement"),
    ))
    .await?;
    storage
        .rename(
            std::path::Path::new("replacement.bin"),
            std::path::Path::new("新父目录/renamed.bin"),
        )
        .await?;
    let handle = hdfs
        .open_file(std::path::Path::new("新父目录/renamed.bin"))
        .await?;
    assert_eq!(hdfs.read_at(&handle, 0, 64).await?, b"replacement"[..]);
    Ok(())
}

async fn append_hdfs_tail(
    hdfs: &data_mover::HDFSStorage,
    data: &bytes::Bytes,
    split: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    create_hdfs_file(hdfs, "append.bin", data.slice(..split)).await?;
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let middle = split + 600_000;
    sender
        .send(data_mover::DataChunk {
            offset: u64::try_from(middle)?,
            data: data.slice(middle..),
        })
        .await?;
    sender
        .send(data_mover::DataChunk {
            offset: u64::try_from(split)?,
            data: data.slice(split..middle),
        })
        .await?;
    drop(sender);
    hdfs.append_stream(
        receiver,
        std::path::Path::new("append.bin"),
        u64::try_from(split)?,
        u64::try_from(data.len())?,
    )
    .await?;
    Ok(())
}

async fn assert_sorted_root_reader(
    storage: &data_mover::HDFSStorage,
) -> Result<(), Box<dyn std::error::Error>> {
    let context = ReadContext {
        match_expr: std::sync::Arc::new(None),
        exclude_expr: std::sync::Arc::new(None),
        current_depth: 0,
        max_depth: 0,
        apply_filter: true,
        include_tags: false,
        is_versioned: false,
    };
    let result = storage
        .read_dir_sorted("", &DirHandle::Hdfs(std::path::PathBuf::new()), &context)
        .await?;
    assert_eq!(result.files.len(), 1);
    assert_eq!(result.files[0].get_name(), "root.txt");
    assert_eq!(result.subdirs.len(), 2);
    assert_eq!(result.subdirs[0].entry.get_name(), "并列");
    assert_eq!(result.subdirs[1].entry.get_name(), "甲");
    Ok(())
}

async fn collect_walkdir2_snapshot(
    storage: &StorageEnum,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let receiver = storage.walkdir_2(None, None, None, None, 3, false).await?;
    let mut snapshot = Vec::new();
    let mut done = 0;
    while let Some(event) = receiver.next().await {
        match event {
            NdxEvent::Page(page) => {
                let files = page
                    .files
                    .iter()
                    .map(|entry| {
                        format!(
                            "{}:{}",
                            entry.ndx,
                            entry.entry.get_relative_path().display()
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                let subdirs = page
                    .subdirs
                    .iter()
                    .map(|entry| {
                        format!(
                            "{}:{}",
                            entry.ndx,
                            entry.entry.get_relative_path().display()
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                snapshot.push(format!(
                    "{}|{}|{}|{}|{}",
                    page.dir_path, page.ndx_start, files, subdirs, page.gap_ndx
                ));
            }
            NdxEvent::Error { path, reason } => {
                return Err(format!("walkdir_2 failed at {path}: {reason}").into());
            }
            NdxEvent::Done => done += 1,
        }
    }
    assert_eq!(done, 1);
    Ok(snapshot)
}

fn assert_clone_send_sync<T: Clone + Send + Sync>() {}
fn assert_send<T: Send>() {}

fn assert_send_value<T: Send>(_: T) {}

fn assert_upstream_status_shape(status: hdfs_native::client::FileStatus) {
    let hdfs_native::client::FileStatus {
        path,
        length,
        isdir,
        permission,
        owner,
        group,
        modification_time,
        access_time,
        replication,
        blocksize,
    } = status;
    drop((
        path,
        length,
        isdir,
        permission,
        owner,
        group,
        modification_time,
        access_time,
        replication,
        blocksize,
    ));
}

async fn assert_upstream_file_method_contract(
    reader: &FileReader,
    writer: &mut FileWriter,
) -> hdfs_native::Result<()> {
    let _: bytes::Bytes = reader.read_range(0, 1).await?;
    let _: usize = Box::pin(writer.write_bytes(bytes::Bytes::from_static(b"contract"))).await?;
    let _: () = Box::pin(writer.close()).await?;
    Ok(())
}

#[test]
fn async_types_meet_streaming_adapter_ownership_contract() {
    assert_clone_send_sync::<Client>();
    assert_send::<FileReader>();
    assert_send::<FileWriter>();
    assert_clone_send_sync::<data_mover::hdfs::HDFSFileHandle>();
    assert_send::<data_mover::error::StorageError>();
    assert_clone_send_sync::<tokio_util::sync::CancellationToken>();

    let client = ClientBuilder::new()
        .with_url("hdfs://127.0.0.1:9000")
        .with_user("data-mover-contract")
        .with_config([("hadoop.security.authentication", "simple")])
        .build()
        .unwrap_or_else(|error| panic!("explicit Simple client must build: {error}"));
    assert_send_value(client.list_status_iter("/", false));
    let _ = assert_upstream_status_shape;
    let _ = assert_upstream_file_method_contract;

    let options = WriteOptions::default()
        .block_size(1_048_576)
        .replication(1)
        .permission(0o640)
        .overwrite(true)
        .create_parent(true);
    assert_send_value(client.get_file_info("/contract"));
    assert_send_value(client.list_status("/contract", false));
    assert_send_value(client.read("/contract"));
    assert_send_value(client.create("/contract", options));
    assert_send_value(client.append("/contract"));
    assert_send_value(client.mkdirs("/contract", 0o750, true));
    assert_send_value(client.rename("/contract", "/renamed", true));
    assert_send_value(client.delete("/contract", true));
    assert_send_value(client.set_times("/contract", 1, 2));
    assert_send_value(client.set_owner("/contract", Some("owner"), Some("group")));
    assert_send_value(client.set_permission("/contract", 0o640));
}

#[tokio::test]
async fn client_can_be_built_from_explicit_inputs_only() -> Result<(), Box<dyn std::error::Error>> {
    let client = ClientBuilder::new()
        .with_url("hdfs://127.0.0.1:9000")
        .with_user("data-mover-contract")
        .with_config_dir("/path/that/does/not/contain/hadoop/config")
        .with_config([("hadoop.security.authentication", "simple")])
        .with_io_runtime(tokio::runtime::Handle::current())
        .build()?;

    assert_clone_send_sync::<Client>();
    drop(client);
    Ok(())
}

#[test]
fn simple_location_contract_is_platform_independent() -> Result<(), Box<dyn std::error::Error>> {
    let parsed = data_mover::HdfsLocation::parse(
        "hdfs://migration%20user@127.0.0.1:9000/warehouse/%E7%9B%AE%E5%BD%95/",
    )?;
    assert_eq!(parsed.user(), "migration user");
    assert_eq!(parsed.endpoint(), "hdfs://127.0.0.1:9000");
    assert_eq!(parsed.root(), "/warehouse/目录");

    let kerberos = HdfsConfig {
        config_dir: None,
        overrides: std::collections::HashMap::from([(
            "hadoop.security.authentication".to_string(),
            "kerberos".to_string(),
        )]),
        ..Default::default()
    };
    assert!(
        data_mover::HdfsLocation::parse_configured(
            "hdfs://migration@127.0.0.1:9000/warehouse",
            &kerberos,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn hdfs_lab_root_validation_rejects_broad_or_escaped_cleanup_targets() {
    for accepted in [
        "hdfs://hdfs@127.0.0.1:9000/tmp/data-mover-nightly/nightly-42-1/hdfs",
        "hdfs://hdfs@127.0.0.1:9000/tmp/data-mover-release/release-v1.2_3/hdfs",
    ] {
        assert!(
            validated_hdfs_lab_root(accepted).is_ok(),
            "rejected {accepted}"
        );
    }
    for rejected in [
        "hdfs://hdfs@127.0.0.1:9000/",
        "hdfs://hdfs@127.0.0.1:9000/tmp/data-mover-nightly",
        "hdfs://hdfs@127.0.0.1:9000/tmp/data-mover-nightly/nightly-x",
        "hdfs://hdfs@127.0.0.1:9000/tmp//data-mover-nightly/nightly-x/hdfs",
        "hdfs://hdfs@127.0.0.1:9000/tmp/data-mover-nightly/release-x/hdfs",
        "hdfs://hdfs@127.0.0.1:9000/tmp/data-mover-nightly/manual-x/hdfs",
        "hdfs://hdfs@127.0.0.1:9000/tmp/data-mover-nightly/nightly-%2E%2E/hdfs",
        "hdfs://hdfs:secret@127.0.0.1:9000/tmp/data-mover-nightly/nightly-x/hdfs",
    ] {
        let Err(error) = validated_hdfs_lab_root(rejected) else {
            panic!("unsafe HDFS lab root was accepted: {rejected}");
        };
        assert!(!error.contains("secret"));
    }
    assert!(!valid_hdfs_lab_run_id("nightly-"));
    assert!(!valid_hdfs_lab_run_id("manual-1"));
    assert!(!valid_hdfs_lab_run_id("nightly-a/b"));
    assert!(!valid_hdfs_lab_run_id(&format!(
        "nightly-{}",
        "x".repeat(81)
    )));
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_reads_root_status_without_mutation() -> Result<(), Box<dyn std::error::Error>>
{
    let location = std::env::var("LAB_HDFS_LOCATION")?;
    let (client, parsed) = build_hdfs_client(&location, &hdfs_lab_config())?;

    let root = client.get_file_info(parsed.root()).await?;
    assert!(root.isdir, "HDFS root must be a directory");
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_confines_independent_roots_and_cleanup()
-> Result<(), Box<dyn std::error::Error>> {
    let first = create_storage(
        &hdfs_lab_location("isolation-a")?,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    let second = create_storage(
        &hdfs_lab_location("isolation-b")?,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    let (StorageEnum::HDFS(first), StorageEnum::HDFS(second)) = (&first, &second) else {
        return Err("isolation fixture did not create HDFS storage".into());
    };
    create_hdfs_file(first, "sentinel", bytes::Bytes::from_static(b"first")).await?;
    create_hdfs_file(second, "sentinel", bytes::Bytes::from_static(b"second")).await?;
    first.delete_storage_root().await?;
    assert!(matches!(
        first.delete_storage_root().await,
        Err(data_mover::error::StorageError::FileNotFound(_))
    ));
    let second_sentinel = second.open_file(std::path::Path::new("sentinel")).await?;
    assert_eq!(
        second.read_at(&second_sentinel, 0, 16).await?,
        b"second"[..]
    );
    second.delete_storage_root().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_cleanup_confined_run_root() -> Result<(), Box<dyn std::error::Error>> {
    let root = hdfs_lab_root()?;
    let (client, _) = build_hdfs_client(&std::env::var("LAB_HDFS_RUN_ROOT")?, &hdfs_lab_config())?;
    for _ in 0..2 {
        match client.delete(root.root(), true).await {
            Ok(_) | Err(hdfs_native::HdfsError::FileNotFound(_)) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_validates_factory_root_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let unique = hdfs_lab_location("factory")?;
    let config = hdfs_lab_config();
    let missing = create_storage(
        &unique,
        CreateStorageOptions {
            ensure_dir: false,
            backend: BackendConfig::Hdfs(config.clone()),
            ..Default::default()
        },
    )
    .await;
    assert!(missing.is_err(), "missing root must be rejected");

    let storage = create_storage(
        &unique,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(config),
            ..Default::default()
        },
    )
    .await?;
    let StorageEnum::HDFS(storage) = storage else {
        return Err("factory did not return HDFS storage".into());
    };
    let status = storage
        .client()
        .get_file_info(storage.location().root())
        .await?;
    assert!(status.isdir);
    assert!(
        storage
            .client()
            .delete(storage.location().root(), true)
            .await?
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_maps_file_and_directory_entries() -> Result<(), Box<dyn std::error::Error>> {
    let location = hdfs_lab_location("entries")?;
    let storage = create_storage(
        &location,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    let StorageEnum::HDFS(hdfs) = &storage else {
        return Err("factory did not return HDFS storage".into());
    };
    let dir_path = hdfs.resolve_path(std::path::Path::new("目录"))?;
    hdfs.client().mkdirs(&dir_path, 0o750, true).await?;
    let file_path = hdfs.resolve_path(std::path::Path::new("目录/file.txt"))?;
    let mut writer = hdfs
        .client()
        .create(&file_path, hdfs_native::WriteOptions::default())
        .await?;
    Box::pin(writer.close()).await?;

    let root = storage.get_metadata(std::path::Path::new("")).await?;
    let dir = storage.get_metadata(std::path::Path::new("目录")).await?;
    let file = storage
        .get_metadata(std::path::Path::new("目录/file.txt"))
        .await?;
    assert!(root.get_is_dir());
    assert_eq!(root.get_relative_path(), std::path::Path::new(""));
    assert!(dir.get_is_dir());
    assert_eq!(dir.get_relative_path(), std::path::Path::new("目录"));
    assert!(!file.get_is_dir());
    assert_eq!(
        file.get_relative_path(),
        std::path::Path::new("目录/file.txt")
    );
    assert_eq!(file.get_extension(), Some("txt"));
    let missing = storage
        .get_metadata(std::path::Path::new("目录/missing.txt"))
        .await;
    assert!(matches!(
        missing,
        Err(data_mover::error::StorageError::FileNotFound(_))
    ));

    assert!(hdfs.client().delete(hdfs.location().root(), true).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_lists_exactly_one_directory() -> Result<(), Box<dyn std::error::Error>> {
    let location = hdfs_lab_location("listing")?;
    let storage = create_storage(
        &location,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    let StorageEnum::HDFS(hdfs) = storage else {
        return Err("factory did not return HDFS storage".into());
    };
    let empty_path = hdfs.resolve_path(std::path::Path::new("空目录"))?;
    let nested_path = hdfs.resolve_path(std::path::Path::new("混合/nested"))?;
    hdfs.client().mkdirs(&empty_path, 0o755, true).await?;
    hdfs.client().mkdirs(&nested_path, 0o755, true).await?;
    let file_path = hdfs.resolve_path(std::path::Path::new("混合/file.txt"))?;
    let mut writer = hdfs
        .client()
        .create(&file_path, hdfs_native::WriteOptions::default())
        .await?;
    Box::pin(writer.close()).await?;
    hdfs.create_dir_all(std::path::Path::new("新建/嵌套"), 0o750)
        .await?;
    hdfs.create_dir_all(std::path::Path::new("新建/嵌套"), 0o750)
        .await?;
    let created = hdfs.get_metadata(std::path::Path::new("新建/嵌套")).await?;
    assert!(created.is_dir);
    assert_eq!(created.mode, 0o750);
    assert!(
        hdfs.create_dir_all(std::path::Path::new("混合/file.txt"), 0o755)
            .await
            .is_err()
    );

    assert!(
        hdfs.list_directory(std::path::Path::new("空目录"))
            .await?
            .is_empty()
    );
    let mut children = hdfs.list_directory(std::path::Path::new("混合")).await?;
    children.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    assert_eq!(children.len(), 2);
    assert_eq!(
        children[0].relative_path,
        std::path::Path::new("混合/file.txt")
    );
    assert!(!children[0].is_dir);
    assert_eq!(
        children[1].relative_path,
        std::path::Path::new("混合/nested")
    );
    assert!(children[1].is_dir);
    assert!(
        hdfs.list_directory(std::path::Path::new("混合/file.txt"))
            .await
            .is_err()
    );

    assert!(hdfs.client().delete(hdfs.location().root(), true).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_deletes_files_and_directories_safely() -> Result<(), Box<dyn std::error::Error>>
{
    let location = hdfs_lab_location("delete")?;
    let storage = create_storage(
        &location,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    let StorageEnum::HDFS(hdfs) = &storage else {
        return Err("factory did not return HDFS storage".into());
    };
    hdfs.create_dir_all(std::path::Path::new("目录/嵌套"), 0o750)
        .await?;
    create_empty_hdfs_files(hdfs, &["file.txt", "目录/嵌套/child.txt"]).await?;

    let file = storage
        .get_metadata(std::path::Path::new("file.txt"))
        .await?;
    storage.delete_file(&file).await?;
    assert!(matches!(
        storage
            .get_metadata(std::path::Path::new("file.txt"))
            .await,
        Err(data_mover::error::StorageError::FileNotFound(path)) if path == "file.txt"
    ));
    assert!(matches!(
        storage.delete_file(&file).await,
        Err(data_mover::error::StorageError::FileNotFound(path)) if path == "file.txt"
    ));
    let directory = storage.get_metadata(std::path::Path::new("目录")).await?;
    assert!(storage.delete_file(&directory).await.is_err());
    storage.delete_dir_all(&directory).await?;
    assert!(
        storage
            .get_metadata(std::path::Path::new("目录"))
            .await
            .is_err()
    );
    assert!(hdfs.delete_dir_all(std::path::Path::new("")).await.is_err());
    hdfs.delete_storage_root().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_renames_files_and_directories_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let location = hdfs_lab_location("rename")?;
    let storage = create_storage(
        &location,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    let StorageEnum::HDFS(hdfs) = &storage else {
        return Err("factory did not return HDFS storage".into());
    };
    hdfs.create_dir_all(std::path::Path::new("源目录/嵌套"), 0o750)
        .await?;
    Box::pin(create_hdfs_file(
        hdfs,
        "源目录/嵌套/data.bin",
        bytes::Bytes::from_static(b"rename-data"),
    ))
    .await?;
    storage
        .rename(
            std::path::Path::new("源目录"),
            std::path::Path::new("源目录"),
        )
        .await?;
    assert!(
        storage
            .get_metadata(std::path::Path::new("源目录/嵌套/data.bin"))
            .await?
            .get_is_regular_file()
    );
    storage
        .rename(
            std::path::Path::new("源目录/嵌套/data.bin"),
            std::path::Path::new("新父目录/renamed.bin"),
        )
        .await?;
    assert!(
        storage
            .get_metadata(std::path::Path::new("源目录/嵌套/data.bin"))
            .await
            .is_err()
    );
    let handle = hdfs
        .open_file(std::path::Path::new("新父目录/renamed.bin"))
        .await?;
    assert_eq!(hdfs.read_at(&handle, 0, 64).await?, b"rename-data"[..]);
    Box::pin(assert_hdfs_overwrite_rename(&storage, hdfs)).await?;
    storage
        .rename(
            std::path::Path::new("源目录"),
            std::path::Path::new("已移动/目录"),
        )
        .await?;
    assert!(
        storage
            .get_metadata(std::path::Path::new("源目录"))
            .await
            .is_err()
    );
    assert!(
        storage
            .get_metadata(std::path::Path::new("已移动/目录/嵌套"))
            .await?
            .get_is_dir()
    );
    assert!(
        hdfs.rename(
            std::path::Path::new("已移动"),
            std::path::Path::new("已移动/子目录")
        )
        .await
        .is_err()
    );
    assert!(
        hdfs.rename(
            std::path::Path::new("missing"),
            std::path::Path::new("still-missing")
        )
        .await
        .is_err()
    );
    assert!(
        hdfs.rename(std::path::Path::new(""), std::path::Path::new("root"))
            .await
            .is_err()
    );
    hdfs.delete_storage_root().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_appends_only_a_validated_contiguous_tail()
-> Result<(), Box<dyn std::error::Error>> {
    let location = hdfs_lab_location("append")?;
    let storage = create_storage(
        &location,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    let StorageEnum::HDFS(hdfs) = &storage else {
        return Err("factory did not return HDFS storage".into());
    };
    let data = deterministic_data();
    let split = 1_200_000;
    Box::pin(append_hdfs_tail(hdfs, &data, split)).await?;
    let handle = hdfs.open_file(std::path::Path::new("append.bin")).await?;
    assert_eq!(
        hdfs.read_at(&handle, 0, u64::try_from(data.len())?).await?,
        data
    );
    let final_size = u64::try_from(data.len())?;
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    drop(sender);
    hdfs.append_stream(
        receiver,
        std::path::Path::new("append.bin"),
        final_size,
        final_size,
    )
    .await?;
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    drop(sender);
    assert!(
        hdfs.append_stream(
            receiver,
            std::path::Path::new("append.bin"),
            u64::try_from(split)?,
            u64::try_from(data.len())?
        )
        .await
        .is_err()
    );
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    sender
        .send(data_mover::DataChunk {
            offset: final_size + 1,
            data: bytes::Bytes::from_static(b"x"),
        })
        .await?;
    drop(sender);
    assert!(
        hdfs.append_stream(
            receiver,
            std::path::Path::new("append.bin"),
            final_size,
            final_size + 2
        )
        .await
        .is_err()
    );
    assert_eq!(
        hdfs.get_metadata(std::path::Path::new("append.bin"))
            .await?
            .size,
        final_size
    );
    hdfs.delete_storage_root().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_prepares_persistent_hdfs_tail_resume_state()
-> Result<(), Box<dyn std::error::Error>> {
    let location = hdfs_lab_location("resume")?;
    let storage = create_storage(
        &location,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    let StorageEnum::HDFS(hdfs) = &storage else {
        return Err("factory did not return HDFS storage".into());
    };
    create_hdfs_file(
        hdfs,
        "最终.bin",
        bytes::Bytes::from_static(b"old-final-data!!"),
    )
    .await?;
    let entry = storage
        .get_metadata(std::path::Path::new("最终.bin"))
        .await?;
    let part = std::path::Path::new("临时/最终.bin.part");

    let (missing, handle) = StorageEnum::resume_prepare(&storage, &entry, part, true).await?;
    assert_eq!(missing, vec![(0, 16)]);
    assert_hdfs_resume_handle(handle, "临时/最终.bin.part", 0, 16)?;
    create_hdfs_file(
        hdfs,
        "临时/最终.bin.part",
        bytes::Bytes::from_static(b"0123"),
    )
    .await?;
    let (missing, handle) = StorageEnum::resume_prepare(&storage, &entry, part, true).await?;
    assert_eq!(missing, vec![(4, 16)]);
    assert_hdfs_resume_handle(handle, "临时/最终.bin.part", 4, 16)?;
    let legacy_fixture =
        r#"{"Hdfs":{"part_path":"临时/最终.bin.part","prefix_len":4,"expected_size":16}}"#;
    let handle: StreamHandle = serde_json::from_str(legacy_fixture)?;
    assert_hdfs_resume_handle(handle.clone(), "临时/最终.bin.part", 4, 16)?;
    Box::pin(complete_and_commit_hdfs_resume(
        &storage, hdfs, &entry, &handle,
    ))
    .await?;
    create_hdfs_file(
        hdfs,
        "临时/最终.bin.part",
        bytes::Bytes::from_static(b"0123456789abcdefx"),
    )
    .await?;
    let (missing, handle) = StorageEnum::resume_prepare(&storage, &entry, part, true).await?;
    assert_eq!(missing, vec![(0, 16)]);
    assert_hdfs_resume_handle(handle, "临时/最终.bin.part", 0, 16)?;
    assert_eq!(hdfs.get_metadata(part).await?.size, 0);
    create_hdfs_file(
        hdfs,
        "临时/最终.bin.part",
        bytes::Bytes::from_static(b"0123"),
    )
    .await?;
    let (missing, handle) = StorageEnum::resume_prepare(&storage, &entry, part, false).await?;
    assert_eq!(missing, vec![(0, 16)]);
    assert_hdfs_resume_handle(handle, "临时/最终.bin.part", 0, 16)?;
    hdfs.delete_file(part).await?;
    hdfs.create_dir_all(part, 0o755).await?;
    assert!(
        StorageEnum::resume_prepare(&storage, &entry, part, true)
            .await
            .is_err()
    );
    assert_eq!(
        storage
            .get_metadata(std::path::Path::new("最终.bin"))
            .await?
            .get_size(),
        16
    );

    Box::pin(assert_stable_hdfs_resume_binding(hdfs, &entry)).await?;
    Box::pin(assert_hdfs_staged_resume_modes(hdfs, &entry)).await?;
    Box::pin(assert_zero_byte_staged_commit(hdfs)).await?;
    Box::pin(assert_common_recoverable_hdfs_copy(&storage, hdfs)).await?;
    Box::pin(assert_cancelled_hdfs_partial_disposition(&storage, hdfs)).await?;
    Box::pin(assert_public_copy_preserves_mid_transfer_cancellation(
        &storage, hdfs,
    ))
    .await?;
    Box::pin(assert_changed_source_is_not_published(&storage, hdfs)).await?;
    hdfs.delete_storage_root().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_streams_recursive_scan_with_depth_limit()
-> Result<(), Box<dyn std::error::Error>> {
    let location = hdfs_lab_location("scan")?;
    let storage = create_storage(
        &location,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    let StorageEnum::HDFS(hdfs) = storage else {
        return Err("factory did not return HDFS storage".into());
    };
    for directory in ["甲", "甲/乙", "甲/乙/丙", "并列"] {
        let path = hdfs.resolve_path(std::path::Path::new(directory))?;
        hdfs.client().mkdirs(&path, 0o755, true).await?;
    }
    create_empty_hdfs_files(
        &hdfs,
        &["root.txt", "甲/a.txt", "甲/乙/b.txt", "甲/乙/丙/c.txt"],
    )
    .await?;
    assert_sorted_root_reader(&hdfs).await?;

    let receiver = hdfs.scan_recursive(None, Some(2), 3)?;
    let mut paths = Vec::new();
    while let Some(event) = receiver.next().await {
        match event {
            HdfsScanEvent::Entry(entry) => paths.push(entry.relative_path),
            HdfsScanEvent::Error { path, error } => {
                return Err(format!("scan failed at {}: {error}", path.display()).into());
            }
        }
    }
    paths.sort();
    assert_eq!(
        paths,
        vec![
            std::path::PathBuf::from("root.txt"),
            std::path::PathBuf::from("并列"),
            std::path::PathBuf::from("甲"),
            std::path::PathBuf::from("甲/a.txt"),
            std::path::PathBuf::from("甲/乙"),
        ]
    );

    let common = StorageEnum::HDFS(hdfs.clone());
    let receiver = common
        .walkdir(
            None,
            WalkOptions {
                depth: Some(2),
                match_expressions: Some(data_mover::filter::parse_filter_expression(
                    "extension == \"txt\"",
                )?),
                concurrency: 3,
                ..Default::default()
            },
        )
        .await?;
    let mut filtered = Vec::new();
    while let Some(message) = receiver.next().await {
        match message {
            StorageEntryMessage::Scanned(entry) => {
                filtered.push(entry.get_relative_path().to_path_buf());
            }
            StorageEntryMessage::Error { path, reason, .. } => {
                return Err(format!("common walk failed at {}: {reason}", path.display()).into());
            }
            other => return Err(format!("unexpected common walk event: {other:?}").into()),
        }
    }
    filtered.sort();
    assert_eq!(
        filtered,
        vec![
            std::path::PathBuf::from("root.txt"),
            std::path::PathBuf::from("甲/a.txt"),
        ]
    );
    let first = collect_walkdir2_snapshot(&common).await?;
    let second = collect_walkdir2_snapshot(&common).await?;
    assert_eq!(first, second);
    assert_eq!(first.len(), 4);

    assert!(hdfs.client().delete(hdfs.location().root(), true).await?);
    Ok(())
}

fn deterministic_data() -> bytes::Bytes {
    bytes::Bytes::from(
        (0..2_500_000_u32)
            .map(|index| (index.wrapping_mul(31) % 251) as u8)
            .collect::<Vec<_>>(),
    )
}

async fn assert_hdfs_to_local_resume_failure_and_deletion(
    source: &StorageEnum,
    destination: &StorageEnum,
    entry: &data_mover::EntryEnum,
    local_root: &std::path::Path,
    data: &bytes::Bytes,
    split: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let relative = std::path::Path::new("split-resume.bin");
    let part = std::path::Path::new("split-resume.bin.part");
    tokio::fs::write(local_root.join(relative), b"protected-final").await?;
    let mut corrupt = data.to_vec();
    corrupt[0] ^= 0xff;
    tokio::fs::write(local_root.join(part), corrupt).await?;
    let result = Box::pin(StorageEnum::copy_file_resumable(
        source,
        destination,
        entry,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: false,
            ..Default::default()
        },
        resume_context("split-resume.bin.part", vec![]),
    ))
    .await;
    assert!(result.is_err());
    assert_eq!(
        tokio::fs::read(local_root.join(relative)).await?,
        b"protected-final"
    );
    assert!(source.get_metadata(relative).await.is_ok());

    tokio::fs::write(local_root.join(part), &data[..split]).await?;
    Box::pin(StorageEnum::copy_file_resumable(
        source,
        destination,
        entry,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: false,
            ..Default::default()
        },
        resume_context(
            "split-resume.bin.part",
            vec![(u64::try_from(split)?, u64::try_from(data.len())?)],
        ),
    ))
    .await?;
    assert_eq!(tokio::fs::read(local_root.join(relative)).await?, *data);
    assert!(source.get_metadata(relative).await.is_err());
    assert!(tokio::fs::metadata(local_root.join(part)).await.is_err());
    Ok(())
}

async fn assert_hdfs_to_local_split_resume(
    source: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
    data: &bytes::Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let relative = std::path::Path::new("split-resume.bin");
    create_hdfs_file(hdfs, "split-resume.bin", data.clone()).await?;
    hdfs.set_permission(relative, 0o635).await?;
    let source_path = hdfs.resolve_path(relative)?;
    hdfs.client()
        .set_times(&source_path, 1_760_000_000_321, 1_650_000_000_123)
        .await?;
    let entry = source.get_metadata(relative).await?;

    let local_root = std::env::temp_dir().join(format!(
        "data-mover-hdfs-local-resume-{}",
        std::process::id()
    ));
    let _ = tokio::fs::remove_dir_all(&local_root).await;
    tokio::fs::create_dir_all(&local_root).await?;
    let destination = create_storage(
        local_root.to_str().ok_or("invalid local resume path")?,
        CreateStorageOptions::new(Some(1_048_576), true),
    )
    .await?;
    tokio::fs::write(local_root.join(relative), b"old-final").await?;
    let part = std::path::Path::new("split-resume.bin.part");
    let split = 1_150_000_usize;
    tokio::fs::write(local_root.join(part), &data[..split]).await?;

    let (missing, handle) = StorageEnum::resume_prepare(&destination, &entry, part, true).await?;
    assert_eq!(
        missing,
        vec![(u64::try_from(split)?, u64::try_from(data.len())?)]
    );
    let encoded = serde_json::to_vec(&handle)?;
    let handle: StreamHandle = serde_json::from_slice(&encoded)?;
    assert_eq!(
        handle,
        StreamHandle::Nas {
            part_path: part.into()
        }
    );

    let callbacks = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let callback_values = callbacks.clone();
    let callback: data_mover::CommitCallback = std::sync::Arc::new(move |offset, length| {
        if let Ok(mut values) = callback_values.lock() {
            values.push((offset, length));
        }
    });
    let bytes = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (receiver, reader) =
        StorageEnum::read_chunk_stream(source, &entry, Some(missing), None, false, 2);
    StorageEnum::write_chunk_stream(
        &destination,
        &entry,
        receiver,
        &handle,
        Some(bytes.clone()),
        callback,
    )
    .await?;
    assert!(reader.await??.is_none());
    assert_eq!(
        bytes.load(std::sync::atomic::Ordering::Relaxed),
        u64::try_from(data.len() - split)?
    );
    assert!(
        !callbacks
            .lock()
            .map_err(|_| "callbacks poisoned")?
            .is_empty()
    );
    assert_eq!(
        source
            .compute_hash(relative, u64::try_from(data.len())?)
            .await?,
        destination
            .compute_hash(part, u64::try_from(data.len())?)
            .await?
    );

    let (exact, exact_handle) =
        StorageEnum::resume_prepare(&destination, &entry, part, true).await?;
    assert!(exact.is_empty());
    assert_eq!(exact_handle, handle);
    StorageEnum::commit_chunk_stream(&destination, &entry, u64::try_from(data.len())?, handle)
        .await?;
    assert_eq!(tokio::fs::read(local_root.join(relative)).await?, *data);
    assert!(tokio::fs::metadata(local_root.join(part)).await.is_err());
    let metadata = tokio::fs::metadata(local_root.join(relative)).await?;
    assert_eq!(local_mode(&metadata), 0o635);
    assert_eq!(local_mtime(&metadata), 1_760_000_000);
    assert!(source.get_metadata(relative).await.is_ok());

    Box::pin(assert_hdfs_to_local_resume_failure_and_deletion(
        source,
        &destination,
        &entry,
        &local_root,
        data,
        split,
    ))
    .await?;
    tokio::fs::remove_dir_all(local_root).await?;
    Ok(())
}

async fn write_read_fixture(
    hdfs: &data_mover::HDFSStorage,
    data: &bytes::Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = hdfs.resolve_path(std::path::Path::new("multi-block.bin"))?;
    let mut writer = hdfs
        .client()
        .create(
            &path,
            hdfs_native::WriteOptions::default().block_size(1_048_576),
        )
        .await?;
    assert_eq!(
        Box::pin(writer.write_bytes(data.clone())).await?,
        data.len()
    );
    Box::pin(writer.close()).await?;
    Ok(())
}

async fn assert_hdfs_local_integrity(
    source: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
    data: &bytes::Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let relative = std::path::Path::new("integrity.bin");
    create_hdfs_file(hdfs, "integrity.bin", data.clone()).await?;
    hdfs.set_permission(relative, 0o624).await?;
    let resolved = hdfs.resolve_path(relative)?;
    hdfs.client()
        .set_times(&resolved, 1_797_000_000_555, 1_600_000_000_000)
        .await?;
    let entry = source.get_metadata(relative).await?;

    let local_root = hdfs_lab_local_path("integrity")?;
    let _ = tokio::fs::remove_dir_all(&local_root).await;
    tokio::fs::create_dir_all(&local_root).await?;
    tokio::fs::write(local_root.join(relative), data).await?;
    set_local_mode(&local_root.join(relative), 0o624).await?;
    filetime::set_file_mtime(
        local_root.join(relative),
        filetime::FileTime::from_unix_time(1_797_000_000, 555_000_000),
    )?;
    let local = create_storage(
        local_root.to_str().ok_or("invalid integrity local root")?,
        CreateStorageOptions::default(),
    )
    .await?;
    for mode in [IntegrityCheckMode::Quick, IntegrityCheckMode::Full] {
        IntegrityCheck::check_with_source_entry(source, &local, &entry, mode, None).await?;
    }

    let corrupt_offset = 1_234_567_usize;
    let mut corrupt = data.to_vec();
    corrupt[corrupt_offset] ^= 0xff;
    tokio::fs::write(local_root.join(relative), corrupt).await?;
    let result = IntegrityCheck::check_with_source_entry(
        source,
        &local,
        &entry,
        IntegrityCheckMode::Full,
        None,
    )
    .await;
    assert!(matches!(
        result,
        Err(data_mover::error::StorageError::MismatchData(fields))
            if fields == vec![MismatchDataField::Content {
                offset: u64::try_from(corrupt_offset)?,
            }]
    ));
    tokio::fs::remove_dir_all(local_root).await?;
    Ok(())
}

async fn assert_hdfs_hdfs_integrity(
    source: &StorageEnum,
    destination_location: &str,
    data: &bytes::Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let relative = std::path::Path::new("integrity.bin");
    let entry = source.get_metadata(relative).await?;
    let destination = create_storage(
        destination_location,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    StorageEnum::copy_file(
        source,
        &destination,
        &entry,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: true,
            ..Default::default()
        },
    )
    .await?;
    for mode in [IntegrityCheckMode::Quick, IntegrityCheckMode::Full] {
        IntegrityCheck::check_path(source, &destination, relative, mode, None).await?;
    }
    let StorageEnum::HDFS(destination_hdfs) = &destination else {
        return Err("integrity destination is not HDFS".into());
    };
    let corrupt_offset = 987_654_usize;
    let mut corrupt = data.to_vec();
    corrupt[corrupt_offset] ^= 0xff;
    create_hdfs_file(
        destination_hdfs,
        "integrity.bin",
        bytes::Bytes::from(corrupt),
    )
    .await?;
    let result = IntegrityCheck::check_path(
        source,
        &destination,
        relative,
        IntegrityCheckMode::Full,
        None,
    )
    .await;
    assert!(matches!(
        result,
        Err(data_mover::error::StorageError::MismatchData(fields))
            if fields == vec![MismatchDataField::Content {
                offset: u64::try_from(corrupt_offset)?,
            }]
    ));
    destination_hdfs.delete_storage_root().await?;
    Ok(())
}

async fn assert_read_contract(
    storage: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
    data: &bytes::Bytes,
) -> Result<data_mover::EntryEnum, Box<dyn std::error::Error>> {
    let handle = hdfs
        .open_file(std::path::Path::new("multi-block.bin"))
        .await?;
    assert_eq!(hdfs.read_at(&handle, 0, 17).await?.as_ref(), &data[..17]);
    assert_eq!(
        hdfs.read_at(&handle, 1_100_000, 97).await?.as_ref(),
        &data[1_100_000..1_100_097]
    );
    let tail = hdfs.read_at(&handle, 2_499_950, 500).await?;
    assert_eq!(tail.as_ref(), &data[2_499_950..]);
    let later = hdfs.read_at(&handle, 2_000_000, 64).await?;
    let earlier = hdfs.read_at(&handle, 500_000, 64).await?;
    assert_eq!(later.as_ref(), &data[2_000_000..2_000_064]);
    assert_eq!(earlier.as_ref(), &data[500_000..500_064]);

    let entry = storage
        .get_metadata(std::path::Path::new("multi-block.bin"))
        .await?;
    let (mut receiver, task) = StorageEnum::read_chunk_stream(storage, &entry, None, None, true, 4);
    let mut streamed = Vec::new();
    while let Some(chunk) = receiver.recv().await {
        assert_eq!(usize::try_from(chunk.offset)?, streamed.len());
        streamed.extend_from_slice(&chunk.data);
    }
    assert!(task.await??.is_some());
    assert_eq!(streamed.as_slice(), data.as_ref());
    let intervals = vec![(2_000_000, 2_000_064), (500_000, 500_064)];
    let (mut receiver, task) =
        StorageEnum::read_chunk_stream(storage, &entry, Some(intervals), None, false, 2);
    let mut chunks = Vec::new();
    while let Some(chunk) = receiver.recv().await {
        chunks.push((chunk.offset, chunk.data));
    }
    assert!(task.await??.is_none());
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].0, 2_000_000);
    assert_eq!(chunks[0].1.as_ref(), &data[2_000_000..2_000_064]);
    assert_eq!(chunks[1].0, 500_000);
    assert_eq!(chunks[1].1.as_ref(), &data[500_000..500_064]);
    Ok(entry)
}

async fn assert_shuffled_write(
    hdfs: &data_mover::HDFSStorage,
    data: &bytes::Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let writer = hdfs
        .clone()
        .with_transfer_concurrency(TransferConcurrency::new(4, 4)?);
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    sender
        .send(data_mover::DataChunk {
            offset: 1_000_000,
            data: data.slice(1_000_000..2_000_000),
        })
        .await?;
    sender
        .send(data_mover::DataChunk {
            offset: 0,
            data: data.slice(..1_000_000),
        })
        .await?;
    sender
        .send(data_mover::DataChunk {
            offset: 2_000_000,
            data: data.slice(2_000_000..),
        })
        .await?;
    drop(sender);
    assert_eq!(
        writer
            .write_stream(
                receiver,
                std::path::Path::new("shuffled.bin"),
                u64::try_from(data.len())?,
                0o640,
                None,
            )
            .await?,
        u64::try_from(data.len())?
    );
    let shuffled = writer
        .open_file(std::path::Path::new("shuffled.bin"))
        .await?;
    assert_eq!(
        writer
            .read_at(&shuffled, 0, u64::try_from(data.len())?)
            .await?,
        data
    );
    Ok(())
}

async fn copy_and_verify_hdfs_destination(
    location: &str,
    source: &StorageEnum,
    data: &bytes::Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let StorageEnum::HDFS(source_hdfs) = source else {
        return Err("copy source is not HDFS".into());
    };
    let source_path = source_hdfs.resolve_path(std::path::Path::new("multi-block.bin"))?;
    source_hdfs
        .client()
        .set_times(&source_path, 1_740_000_000_789, 1_640_000_000_456)
        .await?;
    source_hdfs
        .set_permission(std::path::Path::new("multi-block.bin"), 0o604)
        .await?;
    source_hdfs
        .set_owner_group(
            std::path::Path::new("multi-block.bin"),
            Some("fresh-source-owner"),
            Some("fresh-source-group"),
        )
        .await?;
    let entry = source
        .get_metadata(std::path::Path::new("multi-block.bin"))
        .await?;
    let destination_location = format!("{location}-copy");
    let destination = create_storage(
        &destination_location,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    let StorageEnum::HDFS(destination_hdfs) = &destination else {
        return Err("factory did not return HDFS destination".into());
    };
    Box::pin(create_hdfs_file(
        destination_hdfs,
        "multi-block.bin",
        bytes::Bytes::from_static(b"old-visible-content"),
    ))
    .await?;
    StorageEnum::copy_file(
        source,
        &destination,
        &entry,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: false,
            ..Default::default()
        },
    )
    .await?;
    let copied_metadata = destination_hdfs
        .get_metadata(std::path::Path::new("multi-block.bin"))
        .await?;
    assert_eq!(copied_metadata.mode, 0o604);
    assert_eq!(copied_metadata.mtime, 1_740_000_000_789_000_000);
    assert_eq!(copied_metadata.owner, "fresh-source-owner");
    assert_eq!(copied_metadata.group, "fresh-source-group");
    assert_ne!(copied_metadata.atime, entry.get_atime());
    let copied_handle = destination_hdfs
        .open_file(std::path::Path::new("multi-block.bin"))
        .await?;
    let copied = destination_hdfs
        .read_at(&copied_handle, 0, u64::try_from(data.len())?)
        .await?;
    assert_eq!(copied, data);
    let children = destination_hdfs
        .list_directory(std::path::Path::new(""))
        .await?;
    assert_eq!(children.len(), 1);
    assert_eq!(
        children[0].relative_path,
        std::path::Path::new("multi-block.bin")
    );
    assert!(
        destination_hdfs
            .client()
            .delete(destination_hdfs.location().root(), true)
            .await?
    );
    Ok(())
}

async fn copy_and_verify_local_source(
    location: &str,
    data: &bytes::Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let local_root = hdfs_lab_local_path("source")?;
    tokio::fs::create_dir_all(&local_root).await?;
    let source_path = local_root.join("local-source.bin");
    tokio::fs::write(&source_path, data).await?;
    set_local_mode(&source_path, 0o647).await?;
    filetime::set_file_times(
        &source_path,
        filetime::FileTime::from_unix_time(1_620_000_000, 222_333_444),
        filetime::FileTime::from_unix_time(1_760_000_000, 654_987_321),
    )?;
    let source = create_storage(
        local_root.to_str().ok_or("invalid local fixture path")?,
        CreateStorageOptions {
            block_size: Some(1_048_576),
            ensure_dir: false,
            backend: BackendConfig::Default,
        },
    )
    .await?;
    let entry = source
        .get_metadata(std::path::Path::new("local-source.bin"))
        .await?;
    let destination = create_storage(
        &format!("{location}-local-copy"),
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    StorageEnum::copy_file(
        &source,
        &destination,
        &entry,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: false,
            ..Default::default()
        },
    )
    .await?;
    let StorageEnum::HDFS(hdfs) = &destination else {
        return Err("factory did not return HDFS destination".into());
    };
    let copied_metadata = hdfs
        .get_metadata(std::path::Path::new("local-source.bin"))
        .await?;
    assert_eq!(copied_metadata.mode, 0o647);
    assert_eq!(copied_metadata.mtime, 1_760_000_000_654_000_000);
    assert_ne!(copied_metadata.atime, entry.get_atime());
    assert_eq!(
        copied_metadata.owner,
        hdfs_short_user(hdfs.location().user())
    );
    if let data_mover::EntryEnum::NAS(source_entry) = &entry {
        assert_ne!(
            copied_metadata.owner,
            source_entry.uid.unwrap_or_default().to_string()
        );
        assert_ne!(
            copied_metadata.group,
            source_entry.gid.unwrap_or_default().to_string()
        );
    }
    assert!(tokio::fs::metadata(&source_path).await.is_err());
    let handle = hdfs
        .open_file(std::path::Path::new("local-source.bin"))
        .await?;
    assert_eq!(
        hdfs.read_at(&handle, 0, u64::try_from(data.len())?).await?,
        *data
    );
    Box::pin(assert_failed_fresh_copy_is_isolated(
        &local_root,
        &source,
        &destination,
        hdfs,
        data,
    ))
    .await?;
    Box::pin(assert_nas_metadata_failure_retains_source(
        &local_root,
        &source,
        &destination,
        hdfs,
    ))
    .await?;
    assert!(hdfs.client().delete(hdfs.location().root(), true).await?);
    tokio::fs::remove_dir_all(local_root).await?;
    Ok(())
}

async fn copy_and_verify_local_destination(
    source: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
    data: &bytes::Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let relative = std::path::Path::new("hdfs-to-local.bin");
    create_hdfs_file(hdfs, "hdfs-to-local.bin", data.clone()).await?;
    let resolved = hdfs.resolve_path(relative)?;
    hdfs.client()
        .set_times(&resolved, 1_770_000_000_876, 1_615_000_000_432)
        .await?;
    hdfs.set_permission(relative, 0o625).await?;
    hdfs.set_owner_group(relative, Some("not-a-local-uid"), Some("not-a-local-gid"))
        .await?;
    let entry = source.get_metadata(relative).await?;

    let local_root = hdfs_lab_local_path("destination")?;
    tokio::fs::create_dir_all(&local_root).await?;
    let local_path = local_root.join(relative);
    tokio::fs::write(&local_path, b"different-existing-data").await?;
    let initial = tokio::fs::metadata(&local_path).await?;
    let (numeric_owner, numeric_group) = local_owner(&initial);
    let destination = create_storage(
        local_root.to_str().ok_or("invalid HDFS-to-Local path")?,
        CreateStorageOptions::new(Some(1_048_576), false),
    )
    .await?;
    StorageEnum::copy_file(
        source,
        &destination,
        &entry,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: false,
            ..Default::default()
        },
    )
    .await?;
    let copied = destination.get_metadata(relative).await?;
    assert_eq!(copied.get_mode().map(|mode| mode & 0o7777), Some(0o625));
    assert_eq!(copied.get_mtime(), 1_770_000_000_876_000_000);
    assert_ne!(copied.get_atime(), entry.get_atime());
    assert_eq!(copied.get_uid(), Some(numeric_owner));
    assert_eq!(copied.get_gid(), Some(numeric_group));
    assert_eq!(tokio::fs::read(&local_path).await?, *data);
    assert!(source.get_metadata(relative).await.is_err());

    let small_path = std::path::Path::new("hdfs-to-local-small.bin");
    create_hdfs_file(
        hdfs,
        "hdfs-to-local-small.bin",
        bytes::Bytes::from_static(b"small-hdfs-source"),
    )
    .await?;
    let resolved_small = hdfs.resolve_path(small_path)?;
    hdfs.client()
        .set_times(&resolved_small, 1_775_000_000_123, 1_616_000_000_000)
        .await?;
    hdfs.set_permission(small_path, 0o642).await?;
    let small_entry = source.get_metadata(small_path).await?;
    StorageEnum::copy_file(
        source,
        &destination,
        &small_entry,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: true,
            ..Default::default()
        },
    )
    .await?;
    let small_copy = destination.get_metadata(small_path).await?;
    assert_eq!(small_copy.get_mode().map(|mode| mode & 0o7777), Some(0o642));
    assert_eq!(small_copy.get_mtime(), 1_775_000_000_123_000_000);
    assert_eq!(std::fs::read_dir(&local_root)?.count(), 2);
    tokio::fs::remove_dir_all(local_root).await?;
    Ok(())
}

async fn assert_failed_fresh_copy_is_isolated(
    local_root: &std::path::Path,
    source: &StorageEnum,
    destination: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
    data: &bytes::Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let source_path = local_root.join("failure.bin");
    tokio::fs::write(&source_path, data).await?;
    let entry = source
        .get_metadata(std::path::Path::new("failure.bin"))
        .await?;
    Box::pin(create_hdfs_file(
        hdfs,
        "failure.bin",
        bytes::Bytes::from_static(b"previous-final"),
    ))
    .await?;
    tokio::fs::OpenOptions::new()
        .write(true)
        .open(&source_path)
        .await?
        .set_len(1_100_000)
        .await?;
    assert!(
        StorageEnum::copy_file(
            source,
            destination,
            &entry,
            CopyOptions {
                enable_integrity_check: true,
                is_source_reserved: true,
                ..Default::default()
            },
        )
        .await
        .is_err()
    );
    let handle = hdfs.open_file(std::path::Path::new("failure.bin")).await?;
    assert_eq!(hdfs.read_at(&handle, 0, 64).await?, b"previous-final"[..]);
    tokio::fs::write(local_root.join("empty.bin"), []).await?;
    let empty = source
        .get_metadata(std::path::Path::new("empty.bin"))
        .await?;
    Box::pin(StorageEnum::copy_file(
        source,
        destination,
        &empty,
        CopyOptions {
            is_source_reserved: true,
            ..Default::default()
        },
    ))
    .await?;
    assert_eq!(
        destination
            .get_metadata(std::path::Path::new("empty.bin"))
            .await?
            .get_size(),
        0
    );
    let recoverable_partials = hdfs
        .list_directory(std::path::Path::new(""))
        .await?
        .into_iter()
        .filter(|entry| {
            entry.name.contains("data-mover-")
                && std::path::Path::new(&entry.name)
                    .extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("part"))
        })
        .collect::<Vec<_>>();
    assert_eq!(recoverable_partials.len(), 1);
    assert_eq!(recoverable_partials[0].size, 1_100_000);
    Ok(())
}

async fn assert_nas_metadata_failure_retains_source(
    local_root: &std::path::Path,
    source: &StorageEnum,
    destination: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
) -> Result<(), Box<dyn std::error::Error>> {
    let relative = std::path::Path::new("metadata-failure.bin");
    let source_path = local_root.join(relative);
    let payload = bytes::Bytes::from_static(b"verified-before-invalid-metadata");
    tokio::fs::write(&source_path, &payload).await?;
    let mut entry = source.get_metadata(relative).await?;
    let data_mover::EntryEnum::NAS(nas_entry) = &mut entry else {
        return Err("local metadata fixture did not produce NASEntry".into());
    };
    nas_entry.mtime = -1;
    let result = StorageEnum::copy_file(
        source,
        destination,
        &entry,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: false,
            ..Default::default()
        },
    )
    .await;
    assert!(result.is_err());
    assert!(tokio::fs::metadata(&source_path).await.is_ok());
    assert!(hdfs.get_metadata(relative).await.is_err());
    let mut identity_hasher = blake3::Hasher::new();
    identity_hasher.update(b"data-mover:hdfs-default-copy-identity:v1\0");
    identity_hasher.update(b"local\0");
    identity_hasher.update(local_root.to_string_lossy().as_bytes());
    identity_hasher.update(b"\0metadata-failure.bin");
    let identity_digest = identity_hasher.finalize().to_hex();
    let request = data_mover::hdfs_transfer_request(
        &entry,
        &format!("default-copy-{}", &identity_digest[..32]),
        relative.to_path_buf(),
    )?;
    assert_eq!(
        hdfs.get_metadata(request.partial_path()).await?.size,
        u64::try_from(payload.len())?
    );
    Ok(())
}

fn resume_context(part: &str, missing_intervals: Vec<(u64, u64)>) -> ResumeContext {
    ResumeContext {
        part_relative_path: std::path::PathBuf::from(part),
        missing_intervals,
        on_committed: std::sync::Arc::new(|_, _| {}),
    }
}

async fn recover_overlong_hdfs_resume_part(
    source: &StorageEnum,
    destination: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
    entry: &data_mover::EntryEnum,
    data: &bytes::Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut overlong = data.to_vec();
    overlong.push(0xff);
    create_hdfs_file(hdfs, "payload-overlong.part", bytes::Bytes::from(overlong)).await?;
    Box::pin(StorageEnum::copy_file_resumable(
        source,
        destination,
        entry,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: true,
            ..Default::default()
        },
        resume_context(
            "payload-overlong.part",
            vec![(0, u64::try_from(data.len())?)],
        ),
    ))
    .await?;
    assert!(
        destination
            .get_metadata(std::path::Path::new("payload-overlong.part"))
            .await
            .is_err()
    );
    assert!(
        source
            .get_metadata(std::path::Path::new("payload.bin"))
            .await
            .is_ok()
    );
    Ok(())
}

async fn assert_corrupt_hdfs_resume_isolated(
    source: &StorageEnum,
    destination: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
    source_path: &std::path::Path,
    data: &bytes::Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    tokio::fs::write(source_path, data).await?;
    let entry = source
        .get_metadata(std::path::Path::new("payload.bin"))
        .await?;
    create_hdfs_file(
        hdfs,
        "payload.bin",
        bytes::Bytes::from_static(b"preserved-final"),
    )
    .await?;
    let mut corrupt = data.to_vec();
    corrupt[0] ^= 0xff;
    create_hdfs_file(hdfs, "payload.bin.part", bytes::Bytes::from(corrupt)).await?;
    assert!(
        Box::pin(StorageEnum::copy_file_resumable(
            source,
            destination,
            &entry,
            CopyOptions {
                enable_integrity_check: true,
                is_source_reserved: false,
                ..Default::default()
            },
            resume_context("payload.bin.part", vec![]),
        ))
        .await
        .is_err()
    );
    assert!(tokio::fs::metadata(source_path).await.is_ok());
    let handle = hdfs.open_file(std::path::Path::new("payload.bin")).await?;
    assert_eq!(hdfs.read_at(&handle, 0, 64).await?, b"preserved-final"[..]);
    assert!(
        destination
            .get_metadata(std::path::Path::new("payload.bin.part"))
            .await
            .is_err()
    );
    Box::pin(recover_overlong_hdfs_resume_part(
        source,
        destination,
        hdfs,
        &entry,
        data,
    ))
    .await?;
    Ok(())
}

async fn configure_local_nas_metadata(
    path: &std::path::Path,
    mode: u32,
    atime: filetime::FileTime,
    mtime: filetime::FileTime,
) -> Result<(), Box<dyn std::error::Error>> {
    set_local_mode(path, mode).await?;
    filetime::set_file_times(path, atime, mtime)?;
    Ok(())
}

async fn assert_local_resume_metadata(
    hdfs: &data_mover::HDFSStorage,
    entry: &data_mover::EntryEnum,
) -> Result<(), Box<dyn std::error::Error>> {
    let copied = hdfs
        .get_metadata(std::path::Path::new("payload.bin"))
        .await?;
    assert_eq!(copied.mode, 0o631);
    assert_eq!(copied.mtime, 1_750_000_000_987_000_000);
    assert_ne!(copied.atime, entry.get_atime());
    assert_eq!(copied.owner, "resume-destination-owner");
    assert_eq!(copied.group, "resume-destination-group");
    Ok(())
}

async fn assert_local_to_hdfs_resume(
    location: &str,
    data: &bytes::Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let local_root = hdfs_lab_local_path("resume")?;
    tokio::fs::create_dir_all(&local_root).await?;
    let source_path = local_root.join("payload.bin");
    tokio::fs::write(&source_path, data).await?;
    configure_local_nas_metadata(
        &source_path,
        0o631,
        filetime::FileTime::from_unix_time(1_630_000_000, 123_456_789),
        filetime::FileTime::from_unix_time(1_750_000_000, 987_654_321),
    )
    .await?;
    let source = create_storage(
        local_root.to_str().ok_or("invalid local resume path")?,
        CreateStorageOptions::new(Some(1_048_576), false),
    )
    .await?;
    let entry = source
        .get_metadata(std::path::Path::new("payload.bin"))
        .await?;
    let destination = create_storage(
        location,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    let StorageEnum::HDFS(hdfs) = &destination else {
        return Err("resume destination is not HDFS".into());
    };
    let split = 1_100_000;
    create_hdfs_file(hdfs, "payload.bin", bytes::Bytes::from_static(b"old-final")).await?;
    create_hdfs_file(hdfs, "payload.bin.part", data.slice(..split)).await?;
    hdfs.set_owner_group(
        std::path::Path::new("payload.bin.part"),
        Some("resume-destination-owner"),
        Some("resume-destination-group"),
    )
    .await?;
    let part_path = hdfs.resolve_path(std::path::Path::new("payload.bin.part"))?;
    hdfs.client()
        .set_times(&part_path, 1_600_000_000_000, 1_610_000_000_456)
        .await?;
    assert!(
        Box::pin(StorageEnum::copy_file_resumable(
            &source,
            &destination,
            &entry,
            CopyOptions {
                is_source_reserved: true,
                ..Default::default()
            },
            resume_context("payload.bin.part", vec![(0, 1)]),
        ))
        .await
        .is_err()
    );
    Box::pin(StorageEnum::copy_file_resumable(
        &source,
        &destination,
        &entry,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: false,
            ..Default::default()
        },
        resume_context(
            "payload.bin.part",
            vec![(u64::try_from(split)?, u64::try_from(data.len())?)],
        ),
    ))
    .await?;
    assert!(tokio::fs::metadata(&source_path).await.is_err());
    Box::pin(assert_local_resume_metadata(hdfs, &entry)).await?;
    let handle = hdfs.open_file(std::path::Path::new("payload.bin")).await?;
    assert_eq!(
        hdfs.read_at(&handle, 0, u64::try_from(data.len())?).await?,
        *data
    );
    assert!(
        destination
            .get_metadata(std::path::Path::new("payload.bin.part"))
            .await
            .is_err()
    );

    Box::pin(assert_corrupt_hdfs_resume_isolated(
        &source,
        &destination,
        hdfs,
        &source_path,
        data,
    ))
    .await?;
    hdfs.delete_storage_root().await?;
    tokio::fs::remove_dir_all(local_root).await?;
    Ok(())
}

async fn assert_hdfs_to_hdfs_resume(
    source_location: &str,
    destination_location: &str,
    data: &bytes::Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let options = || CreateStorageOptions {
        ensure_dir: true,
        backend: BackendConfig::Hdfs(hdfs_lab_config()),
        ..Default::default()
    };
    let source = create_storage(source_location, options()).await?;
    let destination = create_storage(destination_location, options()).await?;
    let StorageEnum::HDFS(source_hdfs) = &source else {
        return Err("resume source is not HDFS".into());
    };
    let StorageEnum::HDFS(destination_hdfs) = &destination else {
        return Err("resume destination is not HDFS".into());
    };
    create_hdfs_file(source_hdfs, "hdfs-source.bin", data.clone()).await?;
    let source_path = source_hdfs.resolve_path(std::path::Path::new("hdfs-source.bin"))?;
    source_hdfs
        .client()
        .set_times(&source_path, 1_730_000_000_123, 1_650_000_000_456)
        .await?;
    source_hdfs
        .set_permission(std::path::Path::new("hdfs-source.bin"), 0o620)
        .await?;
    source_hdfs
        .set_owner_group(
            std::path::Path::new("hdfs-source.bin"),
            Some("resume-source-owner"),
            Some("resume-source-group"),
        )
        .await?;
    let entry = source
        .get_metadata(std::path::Path::new("hdfs-source.bin"))
        .await?;
    let fresh_part = std::path::Path::new("fresh-options.part");
    let (fresh_missing, _) =
        StorageEnum::resume_prepare(&destination, &entry, fresh_part, false).await?;
    assert_eq!(fresh_missing, vec![(0, u64::try_from(data.len())?)]);
    let fresh_metadata = destination_hdfs.get_metadata(fresh_part).await?;
    assert_eq!(
        fresh_metadata.mode,
        entry.get_mode().ok_or("missing HDFS mode")? & 0o7777
    );
    let data_mover::EntryEnum::HDFS(source_entry) = &entry else {
        return Err("HDFS source did not produce an HDFS entry".into());
    };
    assert_eq!(fresh_metadata.replication, source_entry.replication);
    destination_hdfs.delete_file(fresh_part).await?;
    let split = 1_300_000;
    create_hdfs_file(
        destination_hdfs,
        "hdfs-source.bin.part",
        data.slice(..split),
    )
    .await?;
    Box::pin(StorageEnum::copy_file_resumable(
        &source,
        &destination,
        &entry,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: true,
            ..Default::default()
        },
        resume_context(
            "hdfs-source.bin.part",
            vec![(u64::try_from(split)?, u64::try_from(data.len())?)],
        ),
    ))
    .await?;
    let handle = destination_hdfs
        .open_file(std::path::Path::new("hdfs-source.bin"))
        .await?;
    assert_eq!(
        destination_hdfs
            .read_at(&handle, 0, u64::try_from(data.len())?)
            .await?,
        *data
    );
    let copied = destination_hdfs
        .get_metadata(std::path::Path::new("hdfs-source.bin"))
        .await?;
    assert_eq!(copied.mode, 0o620);
    assert_eq!(copied.mtime, 1_730_000_000_123_000_000);
    assert_eq!(copied.owner, "resume-source-owner");
    assert_eq!(copied.group, "resume-source-group");
    assert_ne!(copied.atime, entry.get_atime());
    assert!(
        source
            .get_metadata(std::path::Path::new("hdfs-source.bin"))
            .await
            .is_ok()
    );
    source_hdfs.delete_storage_root().await?;
    destination_hdfs.delete_storage_root().await?;
    Ok(())
}

async fn assert_hdfs_metadata_mutations(
    hdfs: &data_mover::HDFSStorage,
    admin_client: &Client,
    outside: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file_path = hdfs.resolve_path(std::path::Path::new("file.bin"))?;
    let directory_path = hdfs.resolve_path(std::path::Path::new("directory"))?;
    let initial_atime_ms = 1_700_000_001_456_u64;
    admin_client
        .set_times(&file_path, 1_600_000_000_000, initial_atime_ms)
        .await?;
    admin_client
        .set_times(&directory_path, 1_600_000_000_000, initial_atime_ms)
        .await?;
    hdfs.set_permission(std::path::Path::new("file.bin"), 0o640)
        .await?;
    hdfs.set_permission(std::path::Path::new("directory"), 0o750)
        .await?;
    hdfs.set_mtime(std::path::Path::new("file.bin"), 1_710_000_000_123_999_999)
        .await?;
    hdfs.set_mtime(std::path::Path::new("directory"), 1_720_000_000_456_999_999)
        .await?;
    hdfs.set_owner_group(
        std::path::Path::new("file.bin"),
        Some("data-mover-owner"),
        Some("data-mover-group"),
    )
    .await?;
    hdfs.set_owner_group(
        std::path::Path::new("directory"),
        Some("data-mover-dir-owner"),
        Some("data-mover-dir-group"),
    )
    .await?;
    hdfs.set_owner_group(
        std::path::Path::new("file.bin"),
        Some("data-mover-final-owner"),
        None,
    )
    .await?;
    hdfs.set_owner_group(
        std::path::Path::new("directory"),
        None,
        Some("data-mover-final-group"),
    )
    .await?;
    hdfs.set_owner_group(std::path::Path::new("file.bin"), Some(""), None)
        .await?;

    let file = hdfs.get_metadata(std::path::Path::new("file.bin")).await?;
    assert_eq!(file.mode, 0o640);
    assert_eq!(file.mtime, 1_710_000_000_123_000_000);
    assert_eq!(file.atime, i64::try_from(initial_atime_ms)? * 1_000_000);
    assert_eq!(file.owner, "data-mover-final-owner");
    assert_eq!(file.group, "data-mover-group");
    let directory = hdfs.get_metadata(std::path::Path::new("directory")).await?;
    assert_eq!(directory.mode, 0o750);
    assert_eq!(directory.mtime, 1_720_000_000_456_000_000);
    assert_eq!(
        directory.atime,
        i64::try_from(initial_atime_ms)? * 1_000_000
    );
    assert_eq!(directory.owner, "data-mover-dir-owner");
    assert_eq!(directory.group, "data-mover-final-group");
    assert!(
        hdfs.set_permission(std::path::Path::new("missing"), 0o600)
            .await
            .is_err()
    );
    assert!(
        hdfs.set_permission(
            std::path::Path::new("../metadata-outside-placeholder"),
            0o600
        )
        .await
        .is_err()
    );
    assert_eq!(admin_client.get_file_info(outside).await?.permission, 0o755);
    Ok(())
}

async fn seed_s3_metadata_objects(
    location: &str,
    data: &bytes::Bytes,
) -> Result<(std::path::PathBuf, StorageEnum), Box<dyn std::error::Error>> {
    let local_root = hdfs_lab_local_path("s3")?;
    tokio::fs::create_dir_all(&local_root).await?;
    let local = create_storage(
        local_root.to_str().ok_or("invalid S3 seed path")?,
        CreateStorageOptions::new(Some(1_048_576), false),
    )
    .await?;
    let s3 = create_storage(location, CreateStorageOptions::default()).await?;
    for name in ["ordinary.bin", "resume.bin"] {
        tokio::fs::write(local_root.join(name), data).await?;
        let entry = local.get_metadata(std::path::Path::new(name)).await?;
        StorageEnum::copy_file(
            &local,
            &s3,
            &entry,
            CopyOptions {
                is_source_reserved: true,
                ..Default::default()
            },
        )
        .await?;
    }
    Ok((local_root, s3))
}

async fn assert_s3_to_hdfs_metadata_copies(
    source: &StorageEnum,
    destination: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
    data: &bytes::Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let ordinary_path = std::path::Path::new("ordinary.bin");
    let ordinary = source.get_metadata(ordinary_path).await?;
    StorageEnum::copy_file(
        source,
        destination,
        &ordinary,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: false,
            ..Default::default()
        },
    )
    .await?;
    let copied = hdfs.get_metadata(ordinary_path).await?;
    assert_eq!(copied.mtime, ordinary.get_mtime() / 1_000_000 * 1_000_000);
    assert_eq!(copied.mode, 0o644);
    assert_eq!(copied.owner, hdfs_short_user(hdfs.location().user()));
    assert!(source.get_metadata(ordinary_path).await.is_err());

    let resume_path = std::path::Path::new("resume.bin");
    let part_path = std::path::Path::new("resume.bin.part");
    let resume_entry = source.get_metadata(resume_path).await?;
    let split = 1_100_000_usize;
    create_hdfs_file(hdfs, "resume.bin.part", data.slice(..split)).await?;
    hdfs.set_permission(part_path, 0o601).await?;
    hdfs.set_owner_group(
        part_path,
        Some("s3-resume-destination-owner"),
        Some("s3-resume-destination-group"),
    )
    .await?;
    let resolved_part = hdfs.resolve_path(part_path)?;
    hdfs.client()
        .set_times(&resolved_part, 1_600_000_000_000, 1_610_000_000_456)
        .await?;
    Box::pin(StorageEnum::copy_file_resumable(
        source,
        destination,
        &resume_entry,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: false,
            ..Default::default()
        },
        resume_context(
            "resume.bin.part",
            vec![(u64::try_from(split)?, u64::try_from(data.len())?)],
        ),
    ))
    .await?;
    let resumed = hdfs.get_metadata(resume_path).await?;
    assert_eq!(
        resumed.mtime,
        resume_entry.get_mtime() / 1_000_000 * 1_000_000
    );
    assert_eq!(resumed.mode, 0o601);
    assert_eq!(resumed.owner, "s3-resume-destination-owner");
    assert_eq!(resumed.group, "s3-resume-destination-group");
    assert_ne!(resumed.atime, resume_entry.get_atime());
    assert!(source.get_metadata(resume_path).await.is_err());
    assert!(destination.get_metadata(part_path).await.is_err());
    Ok(())
}

async fn assert_hdfs_to_s3_ordinary_copies(
    source: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
    destination: &StorageEnum,
    data: &bytes::Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let small_path = std::path::Path::new("小文件-源.bin");
    let small_data = bytes::Bytes::from_static(b"small HDFS to S3 fixture");
    create_hdfs_file(hdfs, "小文件-源.bin", small_data.clone()).await?;
    let resolved_small = hdfs.resolve_path(small_path)?;
    hdfs.client()
        .set_times(&resolved_small, 1_780_000_000_111, 1_600_000_000_000)
        .await?;
    let small_entry = source.get_metadata(small_path).await?;
    StorageEnum::copy_file(
        source,
        destination,
        &small_entry,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: true,
            ..Default::default()
        },
    )
    .await?;

    let large_path = std::path::Path::new("多块-源.bin");
    let large_data = bytes::Bytes::from(data.as_ref().repeat(3));
    create_hdfs_file(hdfs, "多块-源.bin", large_data.clone()).await?;
    let resolved_large = hdfs.resolve_path(large_path)?;
    hdfs.client()
        .set_times(&resolved_large, 1_790_000_000_222, 1_600_000_000_000)
        .await?;
    let large_entry = source.get_metadata(large_path).await?;
    StorageEnum::copy_file(
        source,
        destination,
        &large_entry,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: false,
            ..Default::default()
        },
    )
    .await?;

    for (entry, expected) in [(&small_entry, &small_data), (&large_entry, &large_data)] {
        let copied = destination.get_metadata(entry.get_relative_path()).await?;
        assert_eq!(copied.get_size(), u64::try_from(expected.len())?);
        let data_mover::EntryEnum::S3(s3_entry) = &copied else {
            return Err("HDFS-to-S3 copy did not produce S3Entry".into());
        };
        assert!(s3_entry.tags.as_ref().is_none_or(Vec::is_empty));
        assert_eq!(
            StorageEnum::read_file_from(destination, &copied, copied.get_size()).await?,
            **expected
        );
    }
    assert!(source.get_metadata(small_path).await.is_ok());
    assert!(source.get_metadata(large_path).await.is_err());
    Ok(())
}

async fn write_split_s3_resume_ranges(
    source: &StorageEnum,
    destination: &StorageEnum,
    entry: &data_mover::EntryEnum,
    intervals: Vec<(u64, u64)>,
    handle: &StreamHandle,
) -> Result<u64, Box<dyn std::error::Error>> {
    let committed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let committed_bytes = committed.clone();
    let callback: data_mover::CommitCallback = std::sync::Arc::new(move |_, length| {
        committed_bytes.fetch_add(length, std::sync::atomic::Ordering::Relaxed);
    });
    let counted = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (receiver, reader) =
        StorageEnum::read_chunk_stream(source, entry, Some(intervals), None, false, 2);
    StorageEnum::write_chunk_stream(
        destination,
        entry,
        receiver,
        handle,
        Some(counted.clone()),
        callback,
    )
    .await?;
    assert!(reader.await??.is_none());
    let counted = counted.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        committed.load(std::sync::atomic::Ordering::Relaxed),
        counted
    );
    Ok(counted)
}

async fn assert_hdfs_to_s3_cancel_and_deletion(
    source: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
    destination: &StorageEnum,
    data: &bytes::Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let cancelled_path = std::path::Path::new("续传-取消.bin");
    create_hdfs_file(hdfs, "续传-取消.bin", data.clone()).await?;
    let cancelled_entry = source.get_metadata(cancelled_path).await?;
    let token = tokio_util::sync::CancellationToken::new();
    token.cancel();
    let result = Box::pin(StorageEnum::copy_file_resumable(
        source,
        destination,
        &cancelled_entry,
        CopyOptions {
            cancel: Some(token),
            is_source_reserved: false,
            ..Default::default()
        },
        resume_context("ignored-for-s3.part", vec![(1, 2)]),
    ))
    .await;
    assert!(matches!(
        result,
        Err(data_mover::error::StorageError::Cancelled)
    ));
    assert!(source.get_metadata(cancelled_path).await.is_ok());
    assert!(destination.get_metadata(cancelled_path).await.is_err());

    let movable_path = std::path::Path::new("续传-删除源.bin");
    create_hdfs_file(hdfs, "续传-删除源.bin", data.clone()).await?;
    let resolved = hdfs.resolve_path(movable_path)?;
    hdfs.client()
        .set_times(&resolved, 1_796_000_000_444, 1_600_000_000_000)
        .await?;
    let movable = source.get_metadata(movable_path).await?;
    Box::pin(StorageEnum::copy_file_resumable(
        source,
        destination,
        &movable,
        CopyOptions {
            enable_integrity_check: true,
            is_source_reserved: false,
            ..Default::default()
        },
        resume_context("ignored-for-s3.part", vec![(1, 2)]),
    ))
    .await?;
    assert!(source.get_metadata(movable_path).await.is_err());
    let copied = destination.get_metadata(movable_path).await?;
    assert_eq!(
        StorageEnum::read_file_from(destination, &copied, u64::try_from(data.len())?).await?,
        *data
    );
    Ok(())
}

async fn assert_hdfs_to_s3_resumable_copies(
    source: &StorageEnum,
    hdfs: &data_mover::HDFSStorage,
    destination: &StorageEnum,
    data: &bytes::Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let relative = std::path::Path::new("续传-源.bin");
    let payload = bytes::Bytes::from(data.as_ref().repeat(3));
    create_hdfs_file(hdfs, "续传-源.bin", payload.clone()).await?;
    let resolved = hdfs.resolve_path(relative)?;
    hdfs.client()
        .set_times(&resolved, 1_795_000_000_333, 1_600_000_000_000)
        .await?;
    hdfs.set_permission(relative, 0o617).await?;
    let entry = source.get_metadata(relative).await?;
    let size = u64::try_from(payload.len())?;

    let (fresh_missing, fresh_handle) =
        StorageEnum::resume_prepare(destination, &entry, relative, true).await?;
    assert_eq!(fresh_missing, vec![(0, size)]);
    let encoded = serde_json::to_vec(&fresh_handle)?;
    let fresh_handle: StreamHandle = serde_json::from_slice(&encoded)?;
    let StreamHandle::S3 { part_size, .. } = fresh_handle else {
        return Err("S3 resume did not return StreamHandle::S3".into());
    };
    assert!(part_size < size);
    let first = vec![(0, part_size)];
    assert_eq!(
        Box::pin(write_split_s3_resume_ranges(
            source,
            destination,
            &entry,
            first,
            &fresh_handle,
        ))
        .await?,
        part_size
    );

    let (remaining, resumed_handle) =
        StorageEnum::resume_prepare(destination, &entry, relative, true).await?;
    assert_eq!(remaining, vec![(part_size, size)]);
    assert_eq!(resumed_handle, fresh_handle);
    let encoded = serde_json::to_vec(&resumed_handle)?;
    let resumed_handle: StreamHandle = serde_json::from_slice(&encoded)?;
    assert_eq!(
        Box::pin(write_split_s3_resume_ranges(
            source,
            destination,
            &entry,
            remaining,
            &resumed_handle,
        ))
        .await?,
        size - part_size
    );
    let (exact, exact_handle) =
        StorageEnum::resume_prepare(destination, &entry, relative, true).await?;
    assert!(exact.is_empty());
    assert_eq!(exact_handle, resumed_handle);
    StorageEnum::commit_chunk_stream(destination, &entry, size, exact_handle).await?;
    assert_eq!(
        source.compute_hash(relative, size).await?,
        destination.compute_hash(relative, size).await?
    );
    let copied = destination.get_metadata(relative).await?;
    let data_mover::EntryEnum::S3(copied) = copied else {
        return Err("resumed destination did not produce S3Entry".into());
    };
    assert!(copied.tags.as_ref().is_none_or(Vec::is_empty));
    assert!(source.get_metadata(relative).await.is_ok());

    Box::pin(assert_hdfs_to_s3_cancel_and_deletion(
        source,
        hdfs,
        destination,
        data,
    ))
    .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_resumes_cross_protocol_copies_safely() -> Result<(), Box<dyn std::error::Error>>
{
    let local_destination = hdfs_lab_location("resume-local")?;
    let hdfs_source = hdfs_lab_location("resume-source")?;
    let hdfs_destination = hdfs_lab_location("resume-hdfs")?;
    let data = deterministic_data();
    Box::pin(assert_local_to_hdfs_resume(&local_destination, &data)).await?;
    Box::pin(assert_hdfs_to_hdfs_resume(
        &hdfs_source,
        &hdfs_destination,
        &data,
    ))
    .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_mutates_hdfs_native_metadata_safely() -> Result<(), Box<dyn std::error::Error>>
{
    let location = hdfs_lab_location("metadata")?;
    let outside = hdfs_lab_path("metadata-outside")?;
    let root = hdfs_lab_root()?;
    let (admin_client, _) = build_hdfs_client(
        &format!(
            "hdfs://{}@{}{}/admin-client",
            percent_encoding::utf8_percent_encode(root.user(), percent_encoding::NON_ALPHANUMERIC),
            root.endpoint()
                .strip_prefix("hdfs://")
                .ok_or("invalid HDFS endpoint")?,
            root.root()
        ),
        &hdfs_lab_config(),
    )?;
    let storage = create_storage(
        &location,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    let StorageEnum::HDFS(hdfs) = &storage else {
        return Err("factory did not return HDFS storage".into());
    };
    hdfs.create_dir_all(std::path::Path::new("directory"), 0o755)
        .await?;
    create_hdfs_file(hdfs, "file.bin", bytes::Bytes::from_static(b"metadata")).await?;
    admin_client.mkdirs(&outside, 0o755, true).await?;

    Box::pin(assert_hdfs_metadata_mutations(
        hdfs,
        &admin_client,
        &outside,
    ))
    .await?;
    let mut entry = hdfs.get_metadata(std::path::Path::new("file.bin")).await?;
    let expected_owner = entry.owner.clone();
    let expected_group = entry.group.clone();
    entry.mode = 0o612;
    entry.mtime = 1_725_000_000_321_999_999;
    entry.owner.clear();
    entry.group.clear();
    storage
        .set_entry_metadata(&data_mover::EntryEnum::HDFS(entry))
        .await?;
    let updated = hdfs.get_metadata(std::path::Path::new("file.bin")).await?;
    assert_eq!(updated.mode, 0o612);
    assert_eq!(updated.mtime, 1_725_000_000_321_000_000);
    assert_eq!(updated.owner, expected_owner);
    assert_eq!(updated.group, expected_group);
    let s3_entry = data_mover::EntryEnum::S3(data_mover::S3Entry {
        name: "file.bin".to_string(),
        relative_path: "file.bin".to_string(),
        extension: Some("bin".to_string()),
        size: 8,
        mtime: 1_735_000_000_654_999_999,
        tags: Some(vec![data_mover::Tag {
            key: "not-hdfs-metadata".to_string(),
            value: "ignored".to_string(),
        }]),
        version_id: Some("not-an-hdfs-version".to_string()),
        is_latest: true,
        is_delete_marker: false,
        version_count: Some(2),
        is_dir: false,
    });
    storage.set_entry_metadata(&s3_entry).await?;
    let after_s3 = hdfs.get_metadata(std::path::Path::new("file.bin")).await?;
    assert_eq!(after_s3.mtime, 1_735_000_000_654_000_000);
    assert_eq!(after_s3.mode, updated.mode);
    assert_eq!(after_s3.atime, updated.atime);
    assert_eq!(after_s3.owner, updated.owner);
    assert_eq!(after_s3.group, updated.group);
    let mut invalid_s3 = s3_entry;
    let data_mover::EntryEnum::S3(invalid_entry) = &mut invalid_s3 else {
        return Err("S3 metadata fixture changed shape".into());
    };
    invalid_entry.mtime = -1;
    assert!(storage.set_entry_metadata(&invalid_s3).await.is_err());
    assert_eq!(
        hdfs.get_metadata(std::path::Path::new("file.bin"))
            .await?
            .mtime,
        after_s3.mtime
    );

    hdfs.delete_storage_root().await?;
    assert!(admin_client.delete(&outside, true).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_storage_enum_metadata_and_progress_delete()
-> Result<(), Box<dyn std::error::Error>> {
    let location = hdfs_lab_location("enum-operations")?;
    let storage = create_storage(
        &location,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    let StorageEnum::HDFS(hdfs) = &storage else {
        return Err("factory did not return HDFS storage".into());
    };
    hdfs.create_dir_all(std::path::Path::new("delete-me/nested"), 0o755)
        .await?;
    let file_path = format!("{}/empty", hdfs.location().root().trim_end_matches('/'));
    let mut writer = hdfs
        .client()
        .create(&file_path, WriteOptions::default().permission(0o600))
        .await?;
    Box::pin(writer.close()).await?;

    storage
        .set_metadata(
            std::path::Path::new("empty"),
            Some(1_725_000_000_123_999_999),
            Some(1_735_000_000_456_999_999),
            None,
            None,
            Some(0o640),
        )
        .await?;
    let metadata = hdfs.get_metadata(std::path::Path::new("empty")).await?;
    assert_eq!(metadata.atime, 1_725_000_000_123_000_000);
    assert_eq!(metadata.mtime, 1_735_000_000_456_000_000);
    assert_eq!(metadata.mode, 0o640);
    assert!(
        storage
            .set_metadata(
                std::path::Path::new("empty"),
                None,
                None,
                Some(1000),
                None,
                None,
            )
            .await
            .is_err()
    );

    let events =
        storage.delete_dir_all_with_progress(Some(std::path::Path::new("delete-me")), 4)?;
    let event = events.next().await.ok_or("missing HDFS delete event")?;
    assert_eq!(event.relative_path, std::path::Path::new("delete-me"));
    assert!(event.is_dir);
    assert_eq!(event.error, None);
    assert!(events.next().await.is_none());
    assert!(matches!(
        hdfs.get_metadata(std::path::Path::new("delete-me")).await,
        Err(data_mover::error::StorageError::FileNotFound(_))
    ));

    hdfs.delete_storage_root().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_storage_enum_server_time_and_tar_write()
-> Result<(), Box<dyn std::error::Error>> {
    let location = hdfs_lab_location("enum-streams")?;
    let storage = create_storage(
        &location,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    let StorageEnum::HDFS(hdfs) = &storage else {
        return Err("factory did not return HDFS storage".into());
    };

    assert!(storage.probe_server_time().await?.is_some());
    StorageEnum::pack_files_to_tar(
        &storage,
        &storage,
        &[],
        std::path::Path::new("empty.tar"),
        1024,
        0,
        data_mover::TarPackOptions::default(),
    )
    .await?;
    assert_eq!(
        hdfs.get_metadata(std::path::Path::new("empty.tar"))
            .await?
            .size,
        1024
    );

    hdfs.delete_storage_root().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_retains_hdfs_source_when_copy_metadata_fails()
-> Result<(), Box<dyn std::error::Error>> {
    let source_location = hdfs_lab_location("metadata-failure-source")?;
    let destination_location = hdfs_lab_location("metadata-failure-dest")?;
    let source_root = data_mover::HdfsLocation::parse(&source_location)?
        .root()
        .to_string();
    let destination_root = data_mover::HdfsLocation::parse(&destination_location)?
        .root()
        .to_string();
    let (admin_client, _) = build_hdfs_client(
        &hdfs_lab_location("metadata-failure-admin")?,
        &hdfs_lab_config(),
    )?;
    let source = create_storage(
        &source_location,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    let destination = create_storage(
        &destination_location,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    let StorageEnum::HDFS(source_hdfs) = &source else {
        return Err("metadata failure source is not HDFS".into());
    };
    let StorageEnum::HDFS(destination_hdfs) = &destination else {
        return Err("metadata failure destination is not HDFS".into());
    };
    let payload = bytes::Bytes::from_static(b"committed-before-metadata-failure");
    create_hdfs_file(source_hdfs, "failure.bin", payload.clone()).await?;
    let mut entry = source
        .get_metadata(std::path::Path::new("failure.bin"))
        .await?;
    let data_mover::EntryEnum::HDFS(hdfs_entry) = &mut entry else {
        return Err("metadata failure fixture is not HDFS".into());
    };
    // The copy commits data before applying metadata. A negative HDFS mtime is
    // rejected deterministically, independent of the authenticated user's
    // superuser status, so this remains valid for both Simple and Kerberos labs.
    hdfs_entry.mtime = -1;
    let result = Box::pin(StorageEnum::copy_file_resumable(
        &source,
        &destination,
        &entry,
        CopyOptions {
            is_source_reserved: false,
            enable_integrity_check: true,
            ..Default::default()
        },
        resume_context("failure.bin.part", vec![(0, u64::try_from(payload.len())?)]),
    ))
    .await;
    assert!(
        matches!(
            &result,
            Err(data_mover::error::StorageError::ConfigError(_))
        ),
        "unexpected metadata failure result: {result:?}"
    );
    assert!(
        source
            .get_metadata(std::path::Path::new("failure.bin"))
            .await
            .is_ok()
    );
    let committed = destination_hdfs
        .open_file(std::path::Path::new("failure.bin"))
        .await?;
    assert_eq!(destination_hdfs.read_at(&committed, 0, 64).await?, payload);
    assert!(admin_client.delete(&source_root, true).await?);
    assert!(admin_client.delete(&destination_root, true).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab S3 and HDFS clusters"]
async fn nightly_lab_copies_between_s3_and_hdfs() -> Result<(), Box<dyn std::error::Error>> {
    let s3_location = std::env::var("LAB_S3_HDFS_LOCATION")?;
    let hdfs_location = hdfs_lab_location("s3-metadata")?;
    let data = deterministic_data();
    let (local_root, source) = seed_s3_metadata_objects(&s3_location, &data).await?;
    let destination = create_storage(
        &hdfs_location,
        CreateStorageOptions {
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
            ..Default::default()
        },
    )
    .await?;
    let StorageEnum::HDFS(hdfs) = &destination else {
        return Err("S3 metadata destination is not HDFS".into());
    };
    Box::pin(assert_s3_to_hdfs_metadata_copies(
        &source,
        &destination,
        hdfs,
        &data,
    ))
    .await?;
    Box::pin(assert_hdfs_to_s3_ordinary_copies(
        &destination,
        hdfs,
        &source,
        &data,
    ))
    .await?;
    Box::pin(assert_hdfs_to_s3_resumable_copies(
        &destination,
        hdfs,
        &source,
        &data,
    ))
    .await?;
    hdfs.delete_storage_root().await?;
    tokio::fs::remove_dir_all(local_root).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_reads_positional_ranges_and_common_stream()
-> Result<(), Box<dyn std::error::Error>> {
    let location = hdfs_lab_location("read")?;
    let storage = create_storage(
        &location,
        CreateStorageOptions {
            block_size: Some(1_048_576),
            ensure_dir: true,
            backend: BackendConfig::Hdfs(hdfs_lab_config()),
        },
    )
    .await?;
    let StorageEnum::HDFS(hdfs) = &storage else {
        return Err("factory did not return HDFS storage".into());
    };
    let data = deterministic_data();
    write_read_fixture(hdfs, &data).await?;
    Box::pin(assert_hdfs_local_integrity(&storage, hdfs, &data)).await?;
    Box::pin(assert_hdfs_hdfs_integrity(
        &storage,
        &format!("{location}-integrity-destination"),
        &data,
    ))
    .await?;
    assert_read_contract(&storage, hdfs, &data).await?;
    assert_shuffled_write(hdfs, &data).await?;
    Box::pin(copy_and_verify_local_destination(&storage, hdfs, &data)).await?;
    Box::pin(assert_hdfs_to_local_split_resume(&storage, hdfs, &data)).await?;
    copy_and_verify_hdfs_destination(&location, &storage, &data).await?;
    assert!(matches!(
        storage
            .get_metadata(std::path::Path::new("multi-block.bin"))
            .await,
        Err(data_mover::error::StorageError::FileNotFound(_))
    ));
    copy_and_verify_local_source(&location, &data).await?;
    assert!(hdfs.client().delete(hdfs.location().root(), true).await?);
    Ok(())
}
