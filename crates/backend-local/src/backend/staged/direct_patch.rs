//! 原地稀疏补丁只写变化区间；完整接收和 basis 依赖排序由调用方先完成。
use super::{LocalStagedDestination, failure, io_failure, write_all_at};
use crate::model::{FailureClass, Operation, StoragePath};
use crate::storage::{
    PositionedByteStream, PositionedChunk, PreparedStage, StorageRoleFailure, WriteEvidence,
};
use futures::StreamExt;
use std::{fs::File, os::unix::fs::FileExt, sync::Arc};
use tokio::task::spawn_blocking;
use tokio_util::sync::CancellationToken;

pub(super) async fn apply(
    destination: &LocalStagedDestination,
    stage: &PreparedStage,
    mut input: PositionedByteStream,
    final_size: u64,
    cancel: CancellationToken,
) -> Result<WriteEvidence, StorageRoleFailure> {
    let path = stage.final_destination().path();
    if !stage.direct() {
        return Err(failure(path, Operation::Write, FailureClass::InvalidInput));
    }
    let file = destination
        .open_stage_file_for(stage, Operation::Write, false)
        .await?;
    let (mut end, mut written) = (0, 0);
    while let Some(chunk) = input.next().await {
        if cancel.is_cancelled() {
            return Err(failure(path, Operation::Write, FailureClass::Cancelled));
        }
        let chunk = chunk?;
        let next = chunk
            .offset
            .checked_add(chunk.data.len() as u64)
            .filter(|next| *next <= final_size && chunk.offset >= end && !chunk.data.is_empty())
            .ok_or_else(|| failure(path, Operation::Write, FailureClass::InvalidInput))?;
        written += write_piece(file.clone(), chunk, path).await?;
        end = next;
    }
    if cancel.is_cancelled() {
        return Err(failure(path, Operation::Write, FailureClass::Cancelled));
    }
    let result = spawn_blocking(move || file.set_len(final_size))
        .await
        .map_err(|_| failure(path, Operation::Write, FailureClass::Internal))?;
    result.map_err(|error| io_failure(path, Operation::Write, &error))?;
    Ok(WriteEvidence {
        persisted_bytes: written,
    })
}

async fn write_piece(
    file: Arc<File>,
    chunk: PositionedChunk,
    path: &StoragePath,
) -> Result<u64, StorageRoleFailure> {
    spawn_blocking(move || {
        write_all_at(&chunk.data, chunk.offset, |bytes, offset| {
            file.write_at(bytes, offset)
        })
    })
    .await
    .map_err(|_| failure(path, Operation::Write, FailureClass::Internal))?
    .map_err(|error| io_failure(path, Operation::Write, &error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{BackendConnectError, Storage};
    use crate::storage::{FinalDestination, LocalBackendConfig, PrepareRequest, SourceDescriptor};
    use crate::{
        backend::connect_backend,
        model::{BackendIdentity, BackendKind, EntryKind, IdentityStrength, SourceIdentity},
    };
    use bytes::Bytes;
    use futures::stream;
    use std::error::Error;
    use std::os::unix::fs::MetadataExt;
    use std::{
        fs::{metadata, read, write},
        num::NonZeroUsize,
    };
    use tempfile::TempDir;

    fn storage(root: &TempDir) -> Result<Storage, BackendConnectError> {
        connect_backend(LocalBackendConfig {
            root: root.path().to_path_buf(),
            rename_context: None,
            read_chunk_bytes: None,
            write_chunk_bytes: None,
            read_concurrency: NonZeroUsize::MIN,
            write_concurrency: NonZeroUsize::MIN,
        })
    }

    #[tokio::test]
    async fn sparse_patch_preserves_untouched_bytes_inode_and_exact_write_count()
    -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        write(root.path().join("file"), b"AAAABBBBCCCC")?;
        let inode = metadata(root.path().join("file"))?.ino();
        let storage = storage(&root)?;
        let destination = storage.staged_destination()?;
        assert!(destination.supports_sparse_patch());
        let path = StoragePath::new("file")?;
        let identity = SourceIdentity::new(
            BackendIdentity::new(BackendKind::Local, "remote-source")?,
            IdentityStrength::PathScoped,
            b"remote-file",
        )?;
        let stage = destination
            .prepare_direct(
                PrepareRequest {
                    final_destination: FinalDestination::new(path.clone()),
                    source: SourceDescriptor::new(path, EntryKind::File, Some(12), identity),
                    recovery_binding: [0; 32],
                },
                CancellationToken::new(),
            )
            .await?;
        let input = stream::iter([Ok(PositionedChunk {
            offset: 4,
            data: Bytes::from_static(b"XXXX"),
        })]);
        let written = destination
            .patch_direct(&stage, Box::pin(input), 12, CancellationToken::new())
            .await?;
        assert_eq!(written.persisted_bytes, 4);
        assert_eq!(read(root.path().join("file"))?, b"AAAAXXXXCCCC");
        assert_eq!(metadata(root.path().join("file"))?.ino(), inode);
        let written = destination
            .patch_direct(
                &stage,
                Box::pin(stream::empty()),
                4,
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(written.persisted_bytes, 0);
        assert_eq!(read(root.path().join("file"))?, b"AAAA");
        Ok(())
    }
}
