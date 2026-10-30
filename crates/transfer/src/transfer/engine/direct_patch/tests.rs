use super::*;
use crate::{
    metadata::{
        AclTarget, MetadataPlanRequest, MetadataPolicies, MetadataTarget, OwnershipTarget,
        TimestampTargetCapability, ValueTarget, compile_metadata_plan,
    },
    model::{
        BackendIdentity, BackendKind, EntryKind, IdentityStrength, MetadataObservations,
        SourceIdentity,
    },
    storage::{LocalBackendConfig, backends::local::connect_backend},
};
use std::{
    error::Error,
    fs::{metadata, read, write},
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

fn source(size: usize) -> TestResult<SourceDescriptor> {
    Ok(SourceDescriptor::new(
        StoragePath::new("file")?,
        EntryKind::File,
        Some(size as u64),
        SourceIdentity::new(
            BackendIdentity::new(BackendKind::Local, "remote-test")?,
            IdentityStrength::PathScoped,
            b"remote-file",
        )?,
    ))
}

async fn prepare(root: &TempDir, payload: &[u8]) -> TestResult<DirectPatchSession> {
    let mut session =
        DirectPatchSession::prepare(storage(root)?, source(payload.len())?, plan()?).await?;
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
    let session = DirectPatchSession::prepare(storage(&root)?, source(8)?, plan()?).await?;
    assert!(matches!(
        session.complete().await,
        Err(DirectPatchError::InvalidInput(_))
    ));
    let mut session = DirectPatchSession::prepare(storage(&root)?, source(8)?, plan()?).await?;
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
