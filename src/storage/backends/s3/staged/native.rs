use tokio_util::sync::CancellationToken;

use crate::model::Operation;
use crate::storage::{
    NativeStageEvidence, NativeStageFailure, PreparedStage, StorageRoleFailure, WriteEvidence,
};

use super::super::source::{cancelled, entry, role_failure};
use super::super::{
    S3_NATIVE_COPY_SINGLE_MAX, S3NativeCopyEvidence, S3NativeCopySource, S3Protocol,
};
use super::{S3StagedDestination, cleanup_result};

struct NativeFillFailure {
    error: StorageRoleFailure,
    bytes: u64,
    requests: u64,
}

impl<P: S3Protocol + 'static> S3StagedDestination<P> {
    pub(in crate::storage::backends::s3) async fn fill_native(
        &self,
        stage: &PreparedStage,
        source: S3NativeCopySource,
        cancel: CancellationToken,
    ) -> Result<NativeStageEvidence, NativeStageFailure> {
        match self.fill_native_stage(stage, &source, &cancel).await {
            Ok(copy) => Ok(NativeStageEvidence {
                write: WriteEvidence {
                    persisted_bytes: copy.bytes,
                },
                native_bytes: copy.bytes,
                native_requests: copy.requests,
            }),
            Err(failure) => Err(NativeStageFailure {
                error: failure.error,
                native_bytes: failure.bytes,
                native_requests: failure.requests,
            }),
        }
    }

    async fn fill_native_stage(
        &self,
        stage: &PreparedStage,
        source: &S3NativeCopySource,
        cancel: &CancellationToken,
    ) -> Result<S3NativeCopyEvidence, NativeFillFailure> {
        let baseline = u64::from(source.size > S3_NATIVE_COPY_SINGLE_MAX);
        let (key, upload_id) = self
            .prepare_native_ownership(stage, source, cancel, baseline)
            .await?;
        let copy = self
            .invoke_native_copy(stage, source, &key, upload_id.as_deref(), cancel, baseline)
            .await?;
        self.complete_native_state(stage, source, copy).await
    }

    async fn prepare_native_ownership(
        &self,
        stage: &PreparedStage,
        source: &S3NativeCopySource,
        cancel: &CancellationToken,
        baseline: u64,
    ) -> Result<(String, Option<String>), NativeFillFailure> {
        let map = |error| native_role_failure(error, baseline);
        let key = self.validate(stage).map_err(map)?;
        let mut record = self
            .stage_state(stage, Operation::Prepare)
            .await
            .map_err(map)?;
        if cancel.is_cancelled() {
            return Err(map(cancelled(
                stage.final_destination.path(),
                Operation::Write,
            )));
        }
        let multipart = source.size > S3_NATIVE_COPY_SINGLE_MAX;
        if !multipart {
            cleanup_result(
                stage.final_destination.path(),
                self.protocol.abort_multipart(&key, &record.upload_id).await,
            )
            .map_err(map)?;
            record.completed = true;
        }
        let upload_id = multipart.then(|| record.upload_id.clone());
        self.states
            .lock()
            .await
            .insert(stage.token.to_vec(), record);
        Ok((key, upload_id))
    }

    async fn invoke_native_copy(
        &self,
        stage: &PreparedStage,
        source: &S3NativeCopySource,
        key: &str,
        upload_id: Option<&str>,
        cancel: &CancellationToken,
        baseline: u64,
    ) -> Result<S3NativeCopyEvidence, NativeFillFailure> {
        self.protocol
            .native_copy(source, key, upload_id, cancel)
            .await
            .map(|copy| S3NativeCopyEvidence {
                bytes: copy.bytes,
                requests: baseline + copy.requests,
            })
            .map_err(|failure| NativeFillFailure {
                error: role_failure(
                    stage.final_destination.path(),
                    Operation::Write,
                    failure.error,
                ),
                bytes: failure.bytes,
                requests: baseline + failure.requests,
            })
    }

    async fn complete_native_state(
        &self,
        stage: &PreparedStage,
        source: &S3NativeCopySource,
        copy: S3NativeCopyEvidence,
    ) -> Result<S3NativeCopyEvidence, NativeFillFailure> {
        if copy.bytes != source.size {
            return Err(NativeFillFailure {
                error: entry(
                    stage.final_destination.path(),
                    Operation::Write,
                    "native S3 copy reported an unexpected size",
                ),
                bytes: copy.bytes,
                requests: copy.requests,
            });
        }
        let mut record = self
            .stage_state(stage, Operation::Write)
            .await
            .map_err(|error| NativeFillFailure {
                error,
                bytes: copy.bytes,
                requests: copy.requests,
            })?;
        record.persisted = copy.bytes;
        record.completed = true;
        self.states
            .lock()
            .await
            .insert(stage.token.to_vec(), record);
        Ok(copy)
    }
}

fn native_role_failure(error: StorageRoleFailure, requests: u64) -> NativeFillFailure {
    NativeFillFailure {
        error,
        bytes: 0,
        requests,
    }
}
