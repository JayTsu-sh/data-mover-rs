use async_trait::async_trait;
use data_mover::model::{
    EntryOperationFailure, FailureClass, ObservationMode, ObservationPlan, ObservedEntry,
    Operation, StoragePath, Transience,
};
use data_mover::storage::{
    BackendCapabilities, BackendConfig, ByteStream, CapabilityAvailability, LocalBackendConfig,
    ReadRequest, ReadSource, SourceDescriptor, Storage, StorageBuilder, StorageRoleFailure,
    UnsupportedReason, connect_backend,
};
use data_mover::tar_pack::{TarFailure, TarRequest, calculate_tar_size, pack};
use data_mover::transfer::{TransferOutcome, TransferPolicy, TransferSide};
use futures::StreamExt as _;
use std::error::Error as _;
use std::num::NonZeroUsize;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;
use std::result::Result as StdResult;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[path = "role_tar/policy.rs"]
mod policy;

async fn local(path: &std::path::Path) -> Result<Storage> {
    let slots = NonZeroUsize::new(2).ok_or("nonzero")?;
    Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
        rename_context: None,
        read_chunk_bytes: None,
        write_chunk_bytes: None,
        root: path.into(),
        read_concurrency: slots,
        write_concurrency: slots,
    }))
    .await?)
}

async fn entry(storage: &Storage, name: &str) -> Result<ObservedEntry> {
    use data_mover::traversal::{
        StorageTraversalSource, TraversalItem, TraversalRequest, TraversalSource,
    };
    let mut session = StorageTraversalSource::new(storage)?.traverse(
        TraversalRequest::new(StoragePath::root()).observation_plan(
            ObservationPlan::default()
                .with_ownership_mode(ObservationMode::InlineOnly)
                .with_timestamps(ObservationMode::InlineOnly),
        ),
    );
    let mut found = None;
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) if entry.path().as_str() == name => found = Some(*entry),
            TraversalItem::Entry(_) => {}
            other => return Err(format!("unexpected traversal item: {other:?}").into()),
        }
    }
    session.finish().await?;
    found.ok_or_else(|| "archive entry missing".into())
}

#[tokio::test]
async fn archive_streams_files_headers_and_padding_with_small_chunk_limits() -> Result {
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    std::fs::create_dir(source_root.path().join("dir"))?;
    let payload: Vec<u8> = (0..8193)
        .map(|value| u8::try_from(value % 251).unwrap_or(0))
        .collect();
    std::fs::write(source_root.path().join("dir/data"), &payload)?;
    // Keep this layout-only test on ordinary ustar headers. Fractional mtime is covered
    // separately by `archive_preserves_positive_fractional_mtime_in_pax`.
    let integral_mtime = filetime::FileTime::from_unix_time(1_700_000_000, 0);
    filetime::set_file_mtime(source_root.path().join("dir"), integral_mtime)?;
    filetime::set_file_mtime(source_root.path().join("dir/data"), integral_mtime)?;
    let source = local(source_root.path()).await?;
    let destination = local(destination_root.path()).await?;
    let entries = vec![
        entry(&source, "dir").await?,
        entry(&source, "dir/data").await?,
    ];
    let expected_size = calculate_tar_size(&entries)?;
    let outcome = pack(TarRequest {
        source,
        destination,
        entries: entries.into_iter().map(std::sync::Arc::new).collect(),
        final_path: StoragePath::new("result.tar")?,

        cancel: CancellationToken::new(),
        source_qos: None,
        transfer_policy: TransferPolicy::AtomicReplace,
    })
    .await?;
    let archive = std::fs::read(destination_root.path().join("result.tar"))?;
    assert_eq!(archive.len() as u64, expected_size);
    assert_eq!(outcome.transferred_bytes, expected_size);
    assert_eq!(&archive[..4], b"dir/");
    assert_eq!(archive[156], b'5');
    assert_eq!(&archive[512..520], b"dir/data");
    #[cfg(unix)]
    for (offset, name) in [(0, "dir"), (512, "dir/data")] {
        let mode = std::str::from_utf8(&archive[offset + 100..offset + 107])?;
        assert_eq!(
            u32::from_str_radix(mode, 8)?,
            std::fs::metadata(source_root.path().join(name))?
                .permissions()
                .mode()
                & 0o7777,
            "tar mode must contain only the actual permission bits"
        );
    }
    assert_eq!(&archive[1024..1024 + payload.len()], &payload);
    assert!(
        archive[1024 + payload.len()..]
            .iter()
            .all(|byte| *byte == 0)
    );
    assert_eq!(outcome.blake3, Some(*blake3::hash(&archive).as_bytes()));
    assert_eq!(std::fs::read_dir(destination_root.path())?.count(), 1);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn archive_records_a_link_without_reading_its_target() -> Result {
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    std::os::unix::fs::symlink("missing", source_root.path().join("link"))?;
    filetime::set_symlink_file_times(
        source_root.path().join("link"),
        filetime::FileTime::from_unix_time(1_700_000_000, 0),
        filetime::FileTime::from_unix_time(1_700_000_000, 0),
    )?;
    let source = local(source_root.path()).await?;
    let entries = vec![entry(&source, "link").await?];
    pack(TarRequest {
        source,
        destination: local(destination_root.path()).await?,
        entries: entries.into(),
        final_path: StoragePath::new("link.tar")?,

        cancel: CancellationToken::new(),
        source_qos: None,
        transfer_policy: TransferPolicy::AtomicReplace,
    })
    .await?;
    let archive = std::fs::read(destination_root.path().join("link.tar"))?;
    assert_eq!(archive.len(), 1536);
    assert_eq!(archive[156], b'2');
    assert_eq!(&archive[157..164], b"missing");
    Ok(())
}

#[tokio::test]
async fn archive_preserves_long_names_using_pax_headers() -> Result {
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    let name = "long-name-".repeat(16);
    std::fs::write(source_root.path().join(&name), b"payload")?;
    let source = local(source_root.path()).await?;
    let entries = vec![entry(&source, &name).await?];
    let expected = calculate_tar_size(&entries)?;
    pack(TarRequest {
        source,
        destination: local(destination_root.path()).await?,
        entries: entries.into(),
        final_path: StoragePath::new("long.tar")?,

        cancel: CancellationToken::new(),
        source_qos: None,
        transfer_policy: TransferPolicy::AtomicReplace,
    })
    .await?;
    let archive = std::fs::read(destination_root.path().join("long.tar"))?;
    assert_eq!(archive.len() as u64, expected);
    assert_eq!(archive[156], b'x');
    let record_end = archive[512..]
        .iter()
        .position(|byte| *byte == b'\n')
        .ok_or("missing PAX record")?
        + 513;
    let record = std::str::from_utf8(&archive[512..record_end])?;
    let (length, body) = record.split_once(' ').ok_or("malformed record")?;
    assert_eq!(length.parse::<usize>()?, record.len());
    assert_eq!(body, format!("path={name}\n"));
    assert_eq!(&archive[1536..1543], b"payload");
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn archive_preserves_long_non_utf8_link_targets_in_binary_pax() -> Result {
    use std::os::unix::ffi::OsStrExt;

    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    let mut target = vec![b'x'; 100];
    target.push(0xff);
    std::os::unix::fs::symlink(
        std::ffi::OsStr::from_bytes(&target),
        source_root.path().join("link"),
    )?;

    let source = local(source_root.path()).await?;
    let entries = vec![entry(&source, "link").await?];
    let expected_size = calculate_tar_size(&entries)?;
    pack(TarRequest {
        source,
        destination: local(destination_root.path()).await?,
        entries: entries.into(),
        final_path: StoragePath::new("binary-link.tar")?,

        cancel: CancellationToken::new(),
        source_qos: None,
        transfer_policy: TransferPolicy::AtomicReplace,
    })
    .await?;

    let archive = std::fs::read(destination_root.path().join("binary-link.tar"))?;
    assert_eq!(archive.len() as u64, expected_size);
    assert_eq!(archive[156], b'x');
    let payload_size = usize::from_str_radix(
        std::str::from_utf8(&archive[124..135])?.trim_start_matches('0'),
        8,
    )?;
    let pax = &archive[512..512 + payload_size];
    assert!(
        pax.windows(b"hdrcharset=BINARY\n".len())
            .any(|w| w == b"hdrcharset=BINARY\n")
    );
    let mut expected_linkpath = b"linkpath=".to_vec();
    expected_linkpath.extend_from_slice(&target);
    expected_linkpath.push(b'\n');
    assert!(
        pax.windows(expected_linkpath.len())
            .any(|w| w == expected_linkpath)
    );
    let pax_block_size = payload_size.div_ceil(512) * 512;
    let link_header = &archive[512 + pax_block_size..512 + pax_block_size + 512];
    assert_eq!(link_header[156], b'2');
    assert_eq!(&link_header[157..257], &[0; 100]);
    Ok(())
}

#[tokio::test]
async fn archive_preserves_positive_fractional_mtime_in_pax() -> Result {
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    let source_path = source_root.path().join("fractional-time");
    std::fs::write(&source_path, b"payload")?;
    filetime::set_file_mtime(
        &source_path,
        filetime::FileTime::from_unix_time(1_700_000_000, 123_456_789),
    )?;

    let source = local(source_root.path()).await?;
    let entries = vec![entry(&source, "fractional-time").await?];
    let expected_size = calculate_tar_size(&entries)?;
    pack(TarRequest {
        source,
        destination: local(destination_root.path()).await?,
        entries: entries.into(),
        final_path: StoragePath::new("fractional.tar")?,

        cancel: CancellationToken::new(),
        source_qos: None,
        transfer_policy: TransferPolicy::AtomicReplace,
    })
    .await?;

    let archive = std::fs::read(destination_root.path().join("fractional.tar"))?;
    assert_eq!(archive.len() as u64, expected_size);
    assert_eq!(archive[156], b'x');
    let payload_size = usize::from_str_radix(
        std::str::from_utf8(&archive[124..135])?.trim_start_matches('0'),
        8,
    )?;
    let pax = &archive[512..512 + payload_size];
    assert!(
        pax.windows(b"mtime=1700000000.123456789\n".len())
            .any(|w| w == b"mtime=1700000000.123456789\n")
    );
    Ok(())
}

#[tokio::test]
async fn archive_request_shares_the_manifest_and_each_observation() -> Result {
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"payload")?;
    let source = local(source_root.path()).await?;
    let entries: Arc<[Arc<ObservedEntry>]> = vec![Arc::new(entry(&source, "file").await?)].into();
    let request = TarRequest {
        source,
        destination: local(destination_root.path()).await?,
        entries: Arc::clone(&entries),
        final_path: StoragePath::new("shared.tar")?,
        cancel: CancellationToken::new(),
        source_qos: None,
        transfer_policy: TransferPolicy::AtomicReplace,
    };
    let cloned = request.clone();
    assert!(Arc::ptr_eq(&cloned.entries, &entries));
    assert_eq!(Arc::strong_count(&entries[0]), 1);
    drop(cloned);
    pack(request).await?;
    assert_eq!(Arc::strong_count(&entries[0]), 1);
    assert!(
        std::fs::read(destination_root.path().join("shared.tar"))?
            .windows(7)
            .any(|bytes| bytes == b"payload")
    );
    Ok(())
}

async fn stale_archive_source(original: &[u8], changed: &[u8]) -> Result {
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    let file = source_root.path().join("file");
    std::fs::write(&file, original)?;
    filetime::set_file_mtime(&file, filetime::FileTime::from_unix_time(1_700_000_000, 0))?;
    std::fs::write(destination_root.path().join("result.tar"), b"published")?;
    let source = local(source_root.path()).await?;
    let observed = entry(&source, "file").await?;
    std::fs::write(&file, changed)?;
    filetime::set_file_mtime(&file, filetime::FileTime::from_unix_time(1_700_000_010, 0))?;
    let result = pack(TarRequest {
        source,
        destination: local(destination_root.path()).await?,
        entries: vec![observed].into(),
        final_path: StoragePath::new("result.tar")?,
        cancel: CancellationToken::new(),
        source_qos: None,
        transfer_policy: TransferPolicy::AtomicReplace,
    })
    .await;
    assert_refused_archive(result)?;
    assert_eq!(std::fs::read_dir(destination_root.path())?.count(), 1);
    assert_eq!(
        std::fs::read(destination_root.path().join("result.tar"))?,
        b"published"
    );
    assert_eq!(std::fs::read(file)?, changed);
    Ok(())
}

#[tokio::test]
async fn archive_refuses_a_stale_size_after_in_place_growth() -> Result {
    stale_archive_source(b"old", b"changed after traversal").await
}

#[tokio::test]
async fn archive_refuses_a_stale_mtime_after_same_size_change() -> Result {
    stale_archive_source(b"old", b"new").await
}

#[tokio::test]
async fn archive_refuses_an_observed_empty_file_that_gained_content() -> Result {
    stale_archive_source(b"", b"new").await
}

struct ChangeAfterFirstChunk {
    inner: Arc<dyn ReadSource>,
    path: PathBuf,
}

#[async_trait]
impl ReadSource for ChangeAfterFirstChunk {
    fn maximum_read_chunk_bytes(&self) -> usize {
        512
    }
    async fn describe(
        &self,
        path: &StoragePath,
    ) -> StdResult<SourceDescriptor, StorageRoleFailure> {
        self.inner.describe(path).await
    }
    async fn read(&self, request: ReadRequest) -> StdResult<ByteStream, StorageRoleFailure> {
        let input = self.inner.read(request).await?;
        let mut file = Some(self.path.clone());
        Ok(Box::pin(input.map(move |item| {
            let bytes = item?;
            if let Some(file) = file.take() {
                std::fs::write(&file, vec![b'B'; 1025]).map_err(|_| mutation_failure())?;
                filetime::set_file_mtime(
                    &file,
                    filetime::FileTime::from_unix_time(1_700_000_010, 0),
                )
                .map_err(|_| mutation_failure())?;
            }
            Ok(bytes)
        })))
    }
}

fn mutation_failure() -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            StoragePath::root(),
            Operation::Read,
            FailureClass::Internal,
            Transience::Permanent,
            "test source mutation failed",
        )
        .unwrap_or_else(|_| unreachable!("static diagnostic")),
    )
}

#[tokio::test]
async fn archive_refuses_source_changes_during_payload_reads() -> Result {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    let file = source_root.path().join("file");
    std::fs::write(&file, vec![b'A'; 1025])?;
    filetime::set_file_mtime(&file, filetime::FileTime::from_unix_time(1_700_000_000, 0))?;
    std::fs::write(target_root.path().join("result.tar"), b"published")?;
    let source = local(source_root.path()).await?;
    let observed = entry(&source, "file").await?;
    let absent =
        CapabilityAvailability::Unsupported(UnsupportedReason::new("read-only mutation probe")?);
    let capabilities = BackendCapabilities::new(
        CapabilityAvailability::Supported,
        absent.clone(),
        absent.clone(),
        absent,
    );
    let wrapped = StorageBuilder::new(source.identity().clone(), capabilities)
        .read_source(Arc::new(ChangeAfterFirstChunk {
            inner: source.read_source()?,
            path: file.clone(),
        }))
        .build()?;
    let result = pack(TarRequest {
        source: wrapped,
        destination: local(target_root.path()).await?,
        entries: vec![observed].into(),
        final_path: StoragePath::new("result.tar")?,
        cancel: CancellationToken::new(),
        source_qos: None,
        transfer_policy: TransferPolicy::AtomicReplace,
    })
    .await;
    assert!(
        result.is_err(),
        "source mutation during archive read was published"
    );
    assert_refused_archive(result)?;
    assert_eq!(std::fs::read_dir(target_root.path())?.count(), 1);
    assert_eq!(
        std::fs::read(target_root.path().join("result.tar"))?,
        b"published"
    );
    assert_eq!(std::fs::read(file)?, vec![b'B'; 1025]);
    Ok(())
}

fn assert_refused_archive(result: StdResult<TransferOutcome, TarFailure>) -> Result {
    let Err(TarFailure::Transfer(failure)) = result else {
        return Err("stale source accepted or its typed failure lost".into());
    };
    assert!(!failure.final_destination_changed());
    assert_eq!(failure.side(), TransferSide::Source);
    assert!(!failure.has_pending_cleanup());
    assert!(failure.cleanup_failure().is_none());
    assert!(matches!(
        failure.source().and_then(|cause| cause.downcast_ref::<StorageRoleFailure>()),
        Some(StorageRoleFailure::Entry(cause)) if cause.operation() == Operation::Read
            && cause.class() == FailureClass::Conflict
    ));
    Ok(())
}
