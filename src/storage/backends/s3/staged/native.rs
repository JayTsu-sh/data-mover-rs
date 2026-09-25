//! Filling a stage prepared for a native S3→S3 copy (ADR-0006 C18): an upload on the final key
//! is filled with `UploadPartCopy` ([`native_final`](super::native_final)); a single stage only
//! records the source, and publication sends one `CopyObject` to the final key.

use tokio_util::sync::CancellationToken;

use crate::model::{FailureClass, Operation, Transience};
use crate::storage::{
    NativeStageEvidence, NativeStageFailure, PreparedStage, StorageRoleFailure, WriteEvidence,
};

use super::super::source::classified_entry;
use super::super::{S3NativeCopyEvidence, S3NativeCopySource, S3Protocol};
use super::{S3StagedDestination, at_destination, foreign, single};

pub(super) struct NativeFillFailure {
    pub(super) error: StorageRoleFailure,
    pub(super) bytes: u64,
    pub(super) requests: u64,
}

impl<P: S3Protocol + 'static> S3StagedDestination<P> {
    pub(in crate::storage::backends::s3) async fn fill_native(
        &self,
        stage: &PreparedStage,
        source: S3NativeCopySource,
        cancel: CancellationToken,
        operations: usize,
    ) -> Result<NativeStageEvidence, NativeStageFailure> {
        match self
            .fill_native_stage(stage, &source, (&cancel, operations))
            .await
        {
            // A resumed upload already held `write_offset` bytes; `bytes` is what this copy added.
            Ok(copy) => Ok(NativeStageEvidence {
                write: WriteEvidence {
                    persisted_bytes: stage.write_offset + copy.bytes,
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

    /// A native copy goes to the final key: only a stage prepared at the destination can take it.
    async fn fill_native_stage(
        &self,
        stage: &PreparedStage,
        source: &S3NativeCopySource,
        (cancel, operations): (&CancellationToken, usize),
    ) -> Result<S3NativeCopyEvidence, NativeFillFailure> {
        self.validate(stage)
            .map_err(|error| native_role_failure(error, 0))?;
        if let Some(upload) = at_destination::of(stage) {
            return self
                .fill_native_upload(stage, upload, source, (cancel, operations))
                .await;
        }
        match single::of(stage) {
            Some(single) if stage.at_destination => fill_native_single(stage, single, source),
            // Refused like every other role method refuses such a stage.
            _ => Err(native_role_failure(foreign(stage, Operation::Write), 0)),
        }
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
    if single.expected_size() != source.size {
        return Err(native_role_failure(
            classified_entry(
                stage.final_destination.path(),
                Operation::Write,
                FailureClass::InvalidInput,
                Transience::Permanent,
                "the native S3 source differs from the prepared size",
            ),
            0,
        ));
    }
    single.set_native_source(source.clone());
    Ok(S3NativeCopyEvidence {
        bytes: source.size,
        requests: 1,
    })
}

pub(super) fn native_role_failure(error: StorageRoleFailure, requests: u64) -> NativeFillFailure {
    NativeFillFailure {
        error,
        bytes: 0,
        requests,
    }
}
