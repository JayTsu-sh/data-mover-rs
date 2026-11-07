//! S3 原生复制：Direct 单请求在填充阶段完成 CopyObject，之后才应用元数据。
//! 其他策略的单请求在发布时发送；分段复制由 `native_final` 填充，发布时完成上传。

use tokio_util::sync::CancellationToken;

use crate::model::{EntryKind, FailureClass, Operation, Transience};
use crate::storage::{
    NativeStageEvidence, NativeStageFailure, PreparedStage, SourceDescriptor, StorageRoleFailure,
    WriteEvidence,
};

use super::super::source::{cancelled, classified_entry};
use super::super::{S3NativeCopyEvidence, S3NativeCopySource, S3Protocol};
use super::{S3StagedDestination, at_destination, foreign, native_final, single};

pub(super) struct NativeFillFailure {
    pub(super) error: StorageRoleFailure,
    pub(super) bytes: u64,
    pub(super) requests: u64,
}

impl<P: S3Protocol + 'static> S3StagedDestination<P> {
    // 当前仅启用普通对象的单请求路径；超过 Direct 单 PUT 阈值仍走原有流式实现。
    pub(in crate::backend) fn direct_native_eligible(&self, source: &SourceDescriptor) -> bool {
        source.kind == EntryKind::File
            && !source.path.as_str().is_empty()
            && !source.path.names_directory_object()
            && source
                .size
                .is_some_and(|size| size <= self.native.single_max)
            && matches!(self.single_put_size(&source.path, source.size), Ok(Some(_)))
    }

    pub(in crate::backend) async fn fill_native(
        &self,
        stage: &PreparedStage,
        source: S3NativeCopySource,
        cancel: CancellationToken,
    ) -> Result<NativeStageEvidence, NativeStageFailure> {
        match self.fill_native_stage(stage, &source, &cancel).await {
            // `bytes` counts only this attempt's copies. Recovered islands also belong to the
            // complete staged object, beyond the contiguous `write_offset` prefix.
            Ok(copy) => Ok(NativeStageEvidence {
                write: WriteEvidence {
                    persisted_bytes: source.size,
                },
                native_bytes: copy.bytes,
                native_requests: copy.requests,
                single_operation: single::of(stage).is_some(),
            }),
            Err(failure) => Err(NativeStageFailure {
                error: failure.error,
                native_bytes: failure.bytes,
                native_requests: failure.requests,
            }),
        }
    }

    /// A native copy goes to the final key: only a stage prepared at the destination can take it.
    async fn fill_native_stage(
        &self,
        stage: &PreparedStage,
        source: &S3NativeCopySource,
        cancel: &CancellationToken,
    ) -> Result<S3NativeCopyEvidence, NativeFillFailure> {
        self.validate(stage)
            .map_err(|error| native_role_failure(error, 0))?;
        if stage.direct() {
            return self.fill_direct_native(stage, source, cancel).await;
        }
        if let Some(upload) = at_destination::of(stage) {
            return self.fill_native_upload(stage, upload, source, cancel).await;
        }
        match single::of(stage) {
            Some(single) if stage.at_destination() => fill_native_single(stage, single, source),
            // Refused like every other role method refuses such a stage.
            _ => Err(native_role_failure(foreign(stage, Operation::Write), 0)),
        }
    }

    async fn fill_direct_native(
        &self,
        stage: &PreparedStage,
        source: &S3NativeCopySource,
        cancel: &CancellationToken,
    ) -> Result<S3NativeCopyEvidence, NativeFillFailure> {
        let path = stage.final_destination().path();
        if cancel.is_cancelled() {
            return Err(native_role_failure(cancelled(path, Operation::Write), 0));
        }
        let single = single::of(stage)
            .ok_or_else(|| native_role_failure(foreign(stage, Operation::Write), 0))?;
        validate_native_single(stage, single, source)?;
        // Direct 先完成数据写入，再应用元数据；中途取消时最终 key 可能已变化。
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                Err(native_role_failure(cancelled(path, Operation::Write), 1))
            }
            result = native_final::send_copy(self, path, single, source, single.expected_size()) => {
                result.map_err(|failure| native_role_failure(failure.error, 1))
            }
        }?;
        // 仅在最终数据已确认后记录完整字节，失败或取消不能把源绑定当作写入进度。
        single.set_native_source(source.clone());
        Ok(S3NativeCopyEvidence {
            bytes: source.size,
            requests: 1,
        })
    }
}

/// A stage for one `CopyObject` to the final key: publication sends it, so the fill only records
/// the source. The copy (its bytes and its one request) is counted here, the only place the engine
/// takes native counts from — so a copy refused at publication still reports them.
fn fill_native_single(
    stage: &PreparedStage,
    single: &single::SingleStage,
    source: &S3NativeCopySource,
) -> Result<S3NativeCopyEvidence, NativeFillFailure> {
    validate_native_single(stage, single, source)?;
    single.set_native_source(source.clone());
    Ok(S3NativeCopyEvidence {
        bytes: source.size,
        requests: 1,
    })
}

fn validate_native_single(
    stage: &PreparedStage,
    single: &single::SingleStage,
    source: &S3NativeCopySource,
) -> Result<(), NativeFillFailure> {
    if single.expected_size() != source.size {
        return Err(native_role_failure(
            classified_entry(
                stage.final_destination().path(),
                Operation::Write,
                FailureClass::InvalidInput,
                Transience::Permanent,
                "the native S3 source differs from the prepared size",
            ),
            0,
        ));
    }
    Ok(())
}

pub(super) fn native_role_failure(error: StorageRoleFailure, requests: u64) -> NativeFillFailure {
    NativeFillFailure {
        error,
        bytes: 0,
        requests,
    }
}
