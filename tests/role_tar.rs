use data_mover::model::{ObservationMode, ObservationPlan, ObservedEntry, StoragePath};
use data_mover::storage::{BackendConfig, LocalBackendConfig, Storage, connect_backend};
use data_mover::tar_pack::{TarRequest, calculate_tar_size, pack};
use std::num::NonZeroUsize;
use tokio_util::sync::CancellationToken;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

async fn local(path: &std::path::Path) -> Result<Storage> {
    let slots = NonZeroUsize::new(2).ok_or("nonzero")?;
    Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
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
        entries,
        final_path: StoragePath::new("result.tar")?,

        cancel: CancellationToken::new(),
        source_qos: None,
    })
    .await?;
    let archive = std::fs::read(destination_root.path().join("result.tar"))?;
    assert_eq!(archive.len() as u64, expected_size);
    assert_eq!(outcome.transferred_bytes, expected_size);
    assert_eq!(&archive[..4], b"dir/");
    assert_eq!(archive[156], b'5');
    assert_eq!(&archive[512..520], b"dir/data");
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
        entries,
        final_path: StoragePath::new("link.tar")?,

        cancel: CancellationToken::new(),
        source_qos: None,
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
        entries,
        final_path: StoragePath::new("long.tar")?,

        cancel: CancellationToken::new(),
        source_qos: None,
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
        entries,
        final_path: StoragePath::new("binary-link.tar")?,

        cancel: CancellationToken::new(),
        source_qos: None,
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
        entries,
        final_path: StoragePath::new("fractional.tar")?,

        cancel: CancellationToken::new(),
        source_qos: None,
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
