//! Re-observed S3 part coverage and the source evidence that must precede its dispatch.
//!
//! Armed v2 roots have strict release semantics: any held owned payload version or cleanup
//! error leaves typed pending cleanup after publication and verification, never a hide-only
//! success with raw H retained in history. Legacy publication
//! continues to use ADR-0006 C17's warning-only cleanup in `at_destination::finish_published`.

use std::ops::Range;

use super::super::source::classified_entry;
use super::final_upload::FinalUpload;
use super::{S3Protocol, S3StagedDestination};
use crate::model::{FailureClass, Operation, Transience};
use crate::storage::checkpoint::{CheckpointData, insert_range, missing_ranges};
use crate::storage::range_hash::RangeHash;
use crate::storage::{PreparedStage, StorageRoleFailure};

pub(super) fn invalid(stage: &PreparedStage, diagnostic: &str) -> StorageRoleFailure {
    classified_entry(
        stage.final_destination.path(),
        Operation::Prepare,
        FailureClass::Corruption,
        Transience::Permanent,
        diagnostic,
    )
}

pub(super) fn source_hash(
    stage: &PreparedStage,
    checkpoint: Option<&[u8]>,
) -> Result<Option<RangeHash>, StorageRoleFailure> {
    checkpoint
        .map(|bytes| {
            let data = CheckpointData::decode(bytes)
                .map_err(|_| invalid(stage, "invalid S3 hash checkpoint"))?;
            let hash = RangeHash::decode(&data.hash, data.size)
                .map_err(|_| invalid(stage, "invalid S3 source hash evidence"))?;
            if data.ranges.iter().any(|range| !hash.covers(range.clone())) {
                return Err(invalid(
                    stage,
                    "S3 checkpoint claims durable bytes without source hash evidence",
                ));
            }
            Ok(hash)
        })
        .transpose()
}

pub(super) fn part_range(upload: &FinalUpload, number: i32, size: u64) -> Range<u64> {
    let start = u64::try_from(number - 1).unwrap_or(0) * upload.part_size as u64;
    start..(start + upload.part_size as u64).min(size)
}

impl<P: S3Protocol + 'static> S3StagedDestination<P> {
    pub(super) async fn release_upload_checkpoint(
        &self,
        stage: &PreparedStage,
        upload: &FinalUpload,
    ) -> Result<(), StorageRoleFailure> {
        let _guard = stage.checkpoint_guard().await;
        let path = stage.final_destination.path();
        if upload.lock().checkpoint_released {
            return Ok(());
        }
        if upload.lock().published.is_none() {
            return Err(super::super::source::entry(
                path,
                Operation::Namespace,
                "S3 checkpoint release requires publication",
            ));
        }
        self.cleanup_hash_pointer(stage, upload).await?;
        upload.lock().checkpoint_released = true;
        Ok(())
    }

    pub(super) fn validate_checkpoint_receipts(
        stage: &PreparedStage,
        upload: &FinalUpload,
        extension: &[u8],
    ) -> Result<(), StorageRoleFailure> {
        let record = super::checkpoint_record::CheckpointRecord::decode(extension)
            .ok_or_else(|| invalid(stage, "invalid S3 checkpoint receipt record"))?;
        let data = stage
            .restored_checkpoint()
            .ok_or_else(|| invalid(stage, "missing S3 source hash checkpoint"))?;
        if !super::checkpoint_record::receipts_match(
            &record.receipts,
            upload,
            data.size,
            &data.ranges,
        ) {
            return Err(invalid(
                stage,
                "S3 ListParts changed a committed part receipt",
            ));
        }
        Ok(())
    }

    /// Replace D with exact `ListParts` facts, adopting islands only when anchored H covers them.
    /// This runs before nonce replacement and does not arm the stage.
    pub(super) fn restore_upload_ranges(
        stage: &PreparedStage,
        upload: &FinalUpload,
    ) -> Result<(), StorageRoleFailure> {
        let Some(mut data) = stage.restored_checkpoint() else {
            return Ok(());
        };
        if upload.expected_size != Some(data.size) {
            return Err(invalid(
                stage,
                "S3 checkpoint source length differs from multipart geometry",
            ));
        }
        let hash = RangeHash::decode(&data.hash, data.size)
            .map_err(|_| invalid(stage, "invalid S3 source hash evidence"))?;
        let mut observed = Vec::new();
        for (number, _) in &upload.lock().parts {
            let range = part_range(upload, *number, data.size);
            if !hash.covers(range.clone()) {
                return Err(invalid(
                    stage,
                    "S3 part exists beyond persisted source hash intent",
                ));
            }
            insert_range(&mut observed, range);
        }
        if data
            .ranges
            .iter()
            .any(|range| !missing_ranges(&observed, range.clone()).is_empty())
        {
            return Err(invalid(
                stage,
                "S3 ListParts lost previously durable checkpoint ranges",
            ));
        }
        data.ranges = observed;
        stage
            .restore_checkpoint(Some(data.encode()))
            .map_err(|_| invalid(stage, "invalid recovered S3 part coverage"))
    }
}
