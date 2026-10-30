use super::*;
use crate::{
    metadata::{
        AclTarget, MetadataPlanRequest, MetadataPolicies, MetadataTarget, OwnershipTarget,
        TimestampTargetCapability, ValueTarget, compile_metadata_plan,
    },
    model::{EntryKind, IdentityStrength, MetadataObservations, SourceIdentity},
    storage::{LocalBackendConfig, backends::local::connect_backend},
};
use std::{
    error::Error,
    fs::{File, metadata, read, write},
    num::NonZeroUsize,
    os::unix::fs::MetadataExt,
};
use tempfile::TempDir;
type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn plan() -> TestResult<MetadataPlan> {
    Ok(compile_metadata_plan(&MetadataPlanRequest {
        observations: &MetadataObservations::default(),
        target: MetadataTarget {
            acl: AclTarget::NotApplicable,
            xattrs: ValueTarget::NotApplicable,
            tags: ValueTarget::NotApplicable,
            ownership_mode: OwnershipTarget::NotApplicable,
            timestamps: TimestampTargetCapability::NotApplicable,
        },
        policies: MetadataPolicies::default(),
        principal_mapper: None,
    })?)
}

fn storage(root: &TempDir) -> TestResult<Storage> {
    Ok(connect_backend(LocalBackendConfig {
        root: root.path().to_path_buf(),
        rename_context: None,
        read_chunk_bytes: None,
        write_chunk_bytes: None,
        read_concurrency: NonZeroUsize::MIN,
        write_concurrency: NonZeroUsize::MIN,
    })?)
}

async fn new_session(root: &TempDir, size: usize) -> TestResult<DirectPatchSession> {
    let storage = storage(root)?;
    let source = SourceDescriptor::new(
        StoragePath::new("file")?,
        EntryKind::File,
        Some(size as u64),
        SourceIdentity::new(
            storage.identity().clone(),
            IdentityStrength::PathScoped,
            b"remote-file",
        )?,
    );
    Ok(DirectPatchSession::prepare(storage, source, plan()?).await?)
}

async fn prepare(root: &TempDir, payload: &[u8]) -> TestResult<DirectPatchSession> {
    let mut session = new_session(root, payload.len()).await?;
    let chunks = payload
        .chunks(4)
        .enumerate()
        .map(|(index, data)| DirectPatchChunk {
            offset: (index * 4) as u64,
            data: Bytes::copy_from_slice(data),
            unchanged_basis: false,
        })
        .collect::<Vec<_>>();
    let mut input: DirectPatchInput = Box::pin(stream::iter(chunks));
    session.receive(&mut input).await?;
    Ok(session)
}

#[tokio::test]
async fn session_writes_only_differences_and_consumes_moving_basis_before_changes() -> TestResult {
    for (old, new, expected) in [
        (&b"AAAABBBBCCCC"[..], &b"AAAAXXXXCCCC"[..], 4),
        (&b"AAAABBBBCCCC"[..], &b"BBBBAAAACCCC"[..], 8),
        (&b"AAAABBBB"[..], &b"AAAABBBBCCCC"[..], 4),
        (&b"AAAABBBBCCCC"[..], &b"AAAA"[..], 0),
    ] {
        let root = tempfile::tempdir()?;
        write(root.path().join("file"), old)?;
        let inode = metadata(root.path().join("file"))?.ino();
        let session = prepare(&root, new).await?;
        assert_eq!(read(root.path().join("file"))?, old);
        assert_eq!(session.complete().await?, expected);
        assert_eq!(read(root.path().join("file"))?, new);
        assert_eq!(metadata(root.path().join("file"))?.ino(), inode);
    }
    Ok(())
}

#[tokio::test]
async fn aborted_or_stale_sessions_leave_basis_untouched() -> TestResult {
    let root = tempfile::tempdir()?;
    write(root.path().join("file"), b"AAAABBBB")?;
    let session = prepare(&root, b"AAAAXXXX").await?;
    drop(session);
    assert_eq!(read(root.path().join("file"))?, b"AAAABBBB");
    let session = prepare(&root, b"AAAAXXXX").await?;
    write(root.path().join("file"), b"EXTERNAL")?;
    assert!(matches!(
        session.complete().await,
        Err(DirectPatchError::BasisChanged(_))
    ));
    assert_eq!(read(root.path().join("file"))?, b"EXTERNAL");
    Ok(())
}

#[tokio::test]
async fn incomplete_input_cannot_truncate_or_publish() -> TestResult {
    let root = tempfile::tempdir()?;
    write(root.path().join("file"), b"AAAABBBB")?;
    let session = new_session(&root, 8).await?;
    assert!(matches!(
        session.complete().await,
        Err(DirectPatchError::InvalidInput(_))
    ));
    let mut session = new_session(&root, 8).await?;
    let mut input: DirectPatchInput = Box::pin(stream::iter([DirectPatchChunk {
        offset: 0,
        data: Bytes::from_static(b"XXXX"),
        unchanged_basis: false,
    }]));
    assert!(session.receive(&mut input).await.is_err());
    assert!(matches!(
        session.complete().await,
        Err(DirectPatchError::InvalidInput(_))
    ));
    assert_eq!(read(root.path().join("file"))?, b"AAAABBBB");
    Ok(())
}

#[tokio::test]
async fn invalid_input_cannot_be_cleared_by_a_later_empty_receive() -> TestResult {
    let root = tempfile::tempdir()?;
    write(root.path().join("file"), b"AAAABBBB")?;
    let mut session = new_session(&root, 8).await?;
    let mut bad: DirectPatchInput = Box::pin(stream::iter([DirectPatchChunk {
        offset: 1,
        data: Bytes::from_static(b"XXXXXXXX"),
        unchanged_basis: false,
    }]));
    assert!(session.receive(&mut bad).await.is_err());
    let mut empty: DirectPatchInput = Box::pin(stream::empty());
    assert!(session.receive(&mut empty).await.is_err());
    assert!(matches!(
        session.complete().await,
        Err(DirectPatchError::InvalidInput(_))
    ));
    assert_eq!(read(root.path().join("file"))?, b"AAAABBBB");
    Ok(())
}

#[tokio::test]
#[ignore = "uses a 20 GiB sparse fixture; explicit native large-file acceptance"]
async fn twenty_gib_basis_with_ten_mib_changes_writes_only_ten_mib() -> TestResult {
    const SIZE: u64 = 20 * 1024 * 1024 * 1024;
    const CHANGED: u64 = 10 * 1024 * 1024;
    let root = tempfile::tempdir()?;
    let path = root.path().join("file");
    File::create(&path)?.set_len(SIZE)?;
    let inode = metadata(&path)?.ino();
    let mut session = new_session(&root, usize::try_from(SIZE)?).await?;
    let mut input = large_input(SIZE, CHANGED)?;
    session.receive(&mut input).await?;
    assert_eq!(metadata(&path)?.blocks(), 0);
    assert_eq!(session.progress().bytes, SIZE);
    assert_eq!(session.complete().await?, CHANGED);
    let after = metadata(&path)?;
    assert_eq!(after.ino(), inode);
    assert_eq!(after.len(), SIZE);
    assert!(after.blocks() * 512 < CHANGED * 4);
    Ok(())
}

fn large_input(size: u64, changed_size: u64) -> TestResult<DirectPatchInput> {
    const CHUNK: u64 = 1024 * 1024;
    let zeros = Bytes::from(vec![0; usize::try_from(CHUNK)?]);
    let changed = Bytes::from(vec![0x47; usize::try_from(CHUNK)?]);
    let input: DirectPatchInput = Box::pin(stream::unfold(0u64, move |offset| {
        let zeros = zeros.clone();
        let changed = changed.clone();
        async move {
            if offset == size {
                return None;
            }
            let different = (size / 2..size / 2 + changed_size).contains(&offset);
            Some((
                DirectPatchChunk {
                    offset,
                    data: if different { changed } else { zeros },
                    unchanged_basis: !different,
                },
                offset + CHUNK,
            ))
        }
    }));
    Ok(input)
}
