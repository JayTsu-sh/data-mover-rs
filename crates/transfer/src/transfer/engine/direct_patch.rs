//! Direct 增量会话：先消费所有 basis 依赖，只暂存变化区间，完整验证后才原地改写。
use crate::metadata::MetadataPlan;
use crate::{
    model::{EntryKind, EntryOperationFailure, FailureClass, Operation, StoragePath, Transience},
    storage::{
        FinalDestination, PositionedByteStream, PositionedChunk, PrepareRequest, PreparedStage,
        PublishRequest, ReadRequest, SourceDescriptor, StagedDestination, Storage,
        StorageRoleFailure, VerifyRequest,
    },
};
use blake3::Hasher;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt, stream};
use std::{
    fs::File as StdFile,
    io,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
};
use tempfile::{NamedTempFile, TempPath};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    task::spawn_blocking,
};
use tokio_util::sync::CancellationToken;
mod error;
pub use error::DirectPatchError;
type PatchResult<T> = std::result::Result<T, DirectPatchError>;
fn operation(message: &'static str) -> DirectPatchError {
    DirectPatchError::InvalidInput(message)
}
fn role_failure(message: &'static str) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            StoragePath::root(),
            Operation::Read,
            FailureClass::Internal,
            Transience::Permanent,
            message,
        )
        .unwrap_or_else(|_| unreachable!("static diagnostic is valid")),
    )
}
pub type DirectPatchInput = Pin<Box<dyn Stream<Item = DirectPatchChunk> + Send>>;

/// `unchanged_basis` 只可用于已从同位置旧 basis 读取的字节，不能接受外部未验证的跳过提示。
pub struct DirectPatchChunk {
    pub offset: u64,
    pub data: Bytes,
    pub unchanged_basis: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct DirectPatchProgress {
    pub bytes: u64,
    pub blake3: [u8; 32],
    pub ended: bool,
}

/// 持有 Direct 增量输入、basis 版本与待提交补丁；不提供事务回滚。
pub struct DirectPatchSession {
    storage: Storage,
    path: StoragePath,
    source: SourceDescriptor,
    basis: Option<SourceDescriptor>,
    plan: MetadataPlan,
    spool: File,
    _temp: Option<TempPath>,
    bytes: u64,
    hash: Hasher,
    ended: bool,
    failed: bool,
    changed_bytes: u64,
}

impl DirectPatchSession {
    /// 固定目标 basis 描述，创建有界补丁暂存。
    ///
    /// # Errors
    /// 不支持的角色、无效源描述、basis 描述或暂存创建失败时返回 typed error。
    pub async fn prepare(
        storage: Storage,
        source: SourceDescriptor,
        plan: MetadataPlan,
    ) -> PatchResult<Self> {
        if source.kind != EntryKind::File || source.size.is_none() {
            return Err(operation(
                "Direct patch requires a regular file with a known size",
            ));
        }
        let destination = storage.staged_destination()?;
        if !destination.supports_direct() || !destination.supports_sparse_patch() {
            return Err(DirectPatchError::UnsupportedDestination(storage.kind()));
        }
        let path = source.path.clone();
        let basis = describe(&storage, &path).await?;
        let (file, temp) = spawn_blocking(create_spool).await??;
        Ok(Self {
            storage,
            path,
            source,
            basis,
            plan,
            spool: File::from_std(file),
            _temp: temp,
            bytes: 0,
            hash: Hasher::new(),
            ended: false,
            failed: false,
            changed_bytes: 0,
        })
    }

    /// 消费完整重建输入，只写入临时补丁，不改目标。
    ///
    /// # Errors
    /// 输入长度、顺序、basis 读取或临时文件 IO 失败时返回原始类型；失败后须丢弃会话。
    pub async fn receive(&mut self, input: &mut DirectPatchInput) -> PatchResult<()> {
        if self.failed {
            return Err(operation(
                "Direct input previously failed; discard this session",
            ));
        }
        let result = self.receive_inner(input).await;
        self.failed = result.is_err();
        result
    }

    async fn receive_inner(&mut self, input: &mut DirectPatchInput) -> PatchResult<()> {
        if self.ended {
            return Err(operation("Direct input was already completed"));
        }
        while let Some(input) = input.next().await {
            self.receive_chunk(input).await?;
        }
        self.ended = true;
        if Some(self.bytes) != self.source.size {
            return Err(operation("Direct payload length differs from source"));
        }
        self.spool.flush().await?;
        Ok(())
    }

    async fn receive_chunk(&mut self, input: DirectPatchChunk) -> PatchResult<()> {
        let DirectPatchChunk {
            offset,
            data,
            unchanged_basis,
        } = input;
        if offset != self.bytes {
            return Err(operation("non-contiguous Direct payload"));
        }
        self.bytes = self
            .bytes
            .checked_add(data.len() as u64)
            .ok_or_else(|| operation("Direct payload overflow"))?;
        if self.bytes > self.source.size.unwrap_or(0) {
            return Err(operation("Direct payload exceeds source size"));
        }
        self.hash.update(&data);
        let same_position = unchanged_basis
            && self
                .basis
                .as_ref()
                .and_then(|b| b.size)
                .is_some_and(|size| self.bytes <= size);
        if same_position || self.equals_basis(offset, &data).await? {
            return Ok(());
        }
        for (index, piece) in data.chunks(1024 * 1024).enumerate() {
            self.spool
                .write_all(&(offset + (index * 1024 * 1024) as u64).to_le_bytes())
                .await?;
            self.spool
                .write_all(&(piece.len() as u64).to_le_bytes())
                .await?;
            self.spool.write_all(piece).await?;
        }
        self.changed_bytes += data.len() as u64;
        Ok(())
    }

    async fn equals_basis(&self, offset: u64, expected: &[u8]) -> PatchResult<bool> {
        let Some(basis) = &self.basis else {
            return Ok(false);
        };
        if basis
            .size
            .is_none_or(|size| offset + expected.len() as u64 > size)
        {
            return Ok(false);
        }
        let source = self.storage.read_source()?;
        let mut input = source
            .read(ReadRequest {
                path: self.path.clone(),
                range: Some(offset..offset + expected.len() as u64),
                expected_source: Some(basis.source_identity.clone()),
                maximum_chunk_bytes: source.maximum_read_chunk_bytes(),
                read_inflight: source.read_concurrency(),
                read_budget: None,
                cancel: CancellationToken::new(),
                source_qos: None,
                version: basis.version.clone(),
            })
            .await?;
        let mut bytes = BytesMut::with_capacity(expected.len());
        while let Some(chunk) = input.next().await {
            bytes.extend_from_slice(&chunk?);
        }
        Ok(bytes.as_ref() == expected)
    }

    /// 返回已接收输入的完整摘要和结束状态。
    #[must_use]
    pub fn progress(&self) -> DirectPatchProgress {
        DirectPatchProgress {
            bytes: self.bytes,
            blake3: *self.hash.finalize().as_bytes(),
            ended: self.ended,
        }
    }

    /// 在调用方确认源端证据后复核 basis 并应用补丁，校验后更新元数据及完成发布。
    ///
    /// # Errors
    /// basis 变化、输入未完成或后端写入/校验/元数据/发布失败时返回错误。
    /// 写入开始后的失败可能留下部分更新；调用方须重新生成 basis 签名后重试。
    pub async fn complete(mut self) -> PatchResult<u64> {
        if self.failed || !self.ended || Some(self.bytes) != self.source.size {
            return Err(operation(
                "Direct patch requires a complete verified payload",
            ));
        }
        self.validate_basis().await?;
        let destination = self.storage.staged_destination()?;
        let stage = self.prepare_stage(&*destination).await?;
        self.spool.rewind().await?;
        let spool_error = Arc::new(Mutex::new(None));
        let input = spool_input(self.spool, spool_error.clone());
        let written = destination
            .patch_direct(&stage, input, self.bytes, CancellationToken::new())
            .await;
        if let Some(error) = spool_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            return Err(error);
        }
        let written = written?;
        finish_patch(
            &self.plan,
            &*destination,
            &stage,
            self.bytes,
            *self.hash.finalize().as_bytes(),
        )
        .await?;
        tracing::info!(
            logical_bytes = self.bytes,
            written_bytes = written.persisted_bytes,
            patch_bytes = self.changed_bytes,
            "Direct sparse patch completed"
        );
        Ok(written.persisted_bytes)
    }

    async fn validate_basis(&self) -> PatchResult<()> {
        let current = describe(&self.storage, &self.path).await?;
        let unchanged = match (&self.basis, &current) {
            (None, None) => true,
            (Some(old), Some(now)) => {
                old.source_identity == now.source_identity
                    && old.content_version == now.content_version
                    && old.size == now.size
            }
            _ => false,
        };
        if unchanged {
            Ok(())
        } else {
            Err(DirectPatchError::BasisChanged(self.path.as_str().into()))
        }
    }

    async fn prepare_stage(
        &self,
        destination: &dyn StagedDestination,
    ) -> PatchResult<PreparedStage> {
        Ok(destination
            .prepare_direct(
                PrepareRequest {
                    final_destination: FinalDestination::new(self.path.clone())
                        .with_private_creation(self.plan.carries_mode()),
                    source: self.source.clone(),
                    recovery_binding: [0; 32],
                },
                CancellationToken::new(),
            )
            .await?)
    }
}

async fn describe(storage: &Storage, path: &StoragePath) -> PatchResult<Option<SourceDescriptor>> {
    match storage.read_source()?.describe(path).await {
        Ok(value) => Ok(Some(value)),
        Err(error) => {
            let error = DirectPatchError::from(error);
            if matches!(&error, DirectPatchError::Role(StorageRoleFailure::Entry(failure)) if failure.class() == FailureClass::NotFound)
            {
                Ok(None)
            } else {
                Err(error)
            }
        }
    }
}

fn create_spool() -> io::Result<(StdFile, Option<TempPath>)> {
    let (file, temp) = NamedTempFile::new()?.into_parts();
    #[cfg(unix)]
    {
        temp.close()?;
        Ok((file, None))
    }
    #[cfg(not(unix))]
    {
        Ok((file, Some(temp)))
    }
}

fn spool_input(file: File, errors: Arc<Mutex<Option<DirectPatchError>>>) -> PositionedByteStream {
    Box::pin(stream::try_unfold(file, read_record).map(move |chunk| {
        chunk.map_err(|error| {
            *errors.lock().unwrap_or_else(PoisonError::into_inner) = Some(error);
            role_failure("Direct patch spool could not be read")
        })
    }))
}

async fn read_record(mut file: File) -> PatchResult<Option<(PositionedChunk, File)>> {
    let mut header = [0; 16];
    if file.read(&mut header[..1]).await? == 0 {
        return Ok(None);
    }
    file.read_exact(&mut header[1..]).await?;
    let mut start = [0; 8];
    start.copy_from_slice(&header[..8]);
    let mut size = [0; 8];
    size.copy_from_slice(&header[8..]);
    let size = usize::try_from(u64::from_le_bytes(size))?;
    if size > 1024 * 1024 {
        return Err(operation("corrupt Direct patch spool length"));
    }
    let mut data = vec![0; size];
    file.read_exact(&mut data).await?;
    Ok(Some((
        PositionedChunk {
            offset: u64::from_le_bytes(start),
            data: data.into(),
        },
        file,
    )))
}

async fn finish_patch(
    plan: &MetadataPlan,
    destination: &dyn StagedDestination,
    stage: &PreparedStage,
    size: u64,
    hash: [u8; 32],
) -> PatchResult<()> {
    destination
        .verify(
            stage,
            VerifyRequest {
                expected_size: size,
                expected_blake3: hash,
                cancel: CancellationToken::new(),
                published: None,
            },
        )
        .await?;
    plan.apply_to_stage(destination, stage, CancellationToken::new())
        .await?;
    destination
        .publish(
            stage,
            PublishRequest {
                expected_size: size,
                expected_blake3: Some(hash),
                cancel: CancellationToken::new(),
            },
        )
        .await?;
    Ok(())
}

#[cfg(all(test, unix))]
mod tests;
