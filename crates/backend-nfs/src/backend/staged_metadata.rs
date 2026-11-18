//! 同一 NFS 目标上的批量属性应用；一次核验归属，保留逐 mutation 的失败位置。
use tokio_util::sync::CancellationToken;

use super::{NfsStagedDestinationAdapter, direct::mark_metadata, entry_failure};
use crate::model::{FailureClass, Operation, Transience};
use crate::storage::{MetadataMutation, PreparedStage, StagedMetadataApplicationFailure};

impl NfsStagedDestinationAdapter {
    pub(super) async fn apply_batch(
        &self,
        stage: &PreparedStage,
        mutations: Vec<MetadataMutation>,
        cancel: CancellationToken,
    ) -> Result<(), StagedMetadataApplicationFailure> {
        if !mutations.is_empty() {
            mark_metadata(stage);
        }
        if cancel.is_cancelled() {
            return Err(StagedMetadataApplicationFailure {
                failed_index: 0,
                completed: 0,
                error: None,
            });
        }
        let total = mutations.len();
        let path = self.validate(stage).map_err(|error| {
            StagedMetadataApplicationFailure::whole_batch(total, 0, Some(error))
        })?;
        if stage.recovered_publication().is_some() || mutations.is_empty() {
            return Ok(());
        }
        if stage.direct() {
            self.confirm_direct(stage).await.map_err(|error| {
                StagedMetadataApplicationFailure::whole_batch(total, 0, Some(error))
            })?;
        }
        let metadata = self.metadata.as_ref().ok_or_else(|| {
            StagedMetadataApplicationFailure::whole_batch(
                total,
                0,
                Some(entry_failure(
                    &path,
                    Operation::Metadata,
                    FailureClass::Unsupported,
                    Transience::Permanent,
                )),
            )
        })?;
        for (completed, mutation) in mutations.into_iter().enumerate() {
            if cancel.is_cancelled() {
                return Err(StagedMetadataApplicationFailure {
                    failed_index: completed,
                    completed,
                    error: None,
                });
            }
            if let Err(error) = metadata.apply(&path, mutation, cancel.clone()).await {
                return Err(StagedMetadataApplicationFailure {
                    failed_index: completed,
                    completed,
                    error: Some(error),
                });
            }
        }
        Ok(())
    }
}
