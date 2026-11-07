//! The structured failure of one transfer attempt, and the staged state it retains.

use std::fmt;
use std::sync::Arc;

use super::negotiation;
use super::{TransferPhase, TransferSide};
use crate::metadata::role_failure_text;
use crate::model::{EntryOperationFailure, FailureClass, Operation, StoragePath, Transience};
use crate::storage::checkpoint::CheckpointPhase;
use crate::storage::{PreparedStage, SourceQosStats, StagedDestination, StorageRoleFailure};
use crate::transfer::TransferPolicy;

/// Structured failure from the unified transfer lifecycle.
#[derive(Debug)]
pub struct TransferFailure {
    phase: TransferPhase,
    pub(super) side: TransferSide,
    pub(super) message: &'static str,
    pub(super) role: Option<Box<StorageRoleFailure>>,
    metadata: Option<Box<crate::metadata::MetadataApplicationFailure>>,
    /// Why copied metadata was refused while planning, before anything was written.
    pub(super) refusal: Option<Box<crate::metadata::MetadataPlanError>>,
    stage_cleanup: Option<Box<FailedStage>>,
    cleanup_failure: Option<Box<StorageRoleFailure>>,
    pub(super) final_destination_changed: bool,
    source_qos: SourceQosStats,
}

struct FailedStage {
    destination: Arc<dyn StagedDestination>,
    stage: PreparedStage,
    disposition: StageCleanup,
}

#[derive(Clone, Copy)]
enum StageCleanup {
    Unpublished,
    AfterPublication,
}

impl fmt::Debug for FailedStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FailedStage(<opaque>)")
    }
}

impl TransferFailure {
    pub(super) fn role(phase: TransferPhase, side: TransferSide, role: StorageRoleFailure) -> Self {
        Self {
            phase,
            side,
            message: "storage role failed",
            role: Some(Box::new(role)),
            metadata: None,
            refusal: None,
            stage_cleanup: None,
            cleanup_failure: None,
            final_destination_changed: false,
            source_qos: SourceQosStats::default(),
        }
    }

    pub(super) fn orchestration(phase: TransferPhase, message: &'static str) -> Self {
        Self {
            phase,
            side: TransferSide::Orchestration,
            message,
            role: None,
            metadata: None,
            refusal: None,
            stage_cleanup: None,
            cleanup_failure: None,
            final_destination_changed: false,
            source_qos: SourceQosStats::default(),
        }
    }

    pub(super) fn capability(side: TransferSide, message: &'static str) -> Self {
        Self {
            phase: TransferPhase::Preflight,
            side,
            message,
            role: None,
            metadata: None,
            refusal: None,
            stage_cleanup: None,
            cleanup_failure: None,
            final_destination_changed: false,
            source_qos: SourceQosStats::default(),
        }
    }

    pub(super) fn metadata(error: crate::metadata::MetadataApplicationFailure) -> Self {
        Self {
            phase: TransferPhase::Metadata,
            side: TransferSide::Destination,
            message: "staged metadata application failed",
            role: None,
            metadata: Some(Box::new(error)),
            refusal: None,
            stage_cleanup: None,
            cleanup_failure: None,
            final_destination_changed: false,
            source_qos: SourceQosStats::default(),
        }
    }

    pub(super) fn with_stage(
        mut self,
        destination: Arc<dyn StagedDestination>,
        stage: PreparedStage,
    ) -> Self {
        if stage.direct() {
            self.final_destination_changed = true;
            return self;
        }
        self.stage_cleanup = Some(Box::new(FailedStage {
            destination,
            stage,
            disposition: StageCleanup::Unpublished,
        }));
        self
    }

    pub(super) fn with_committed_cleanup(
        mut self,
        destination: Arc<dyn StagedDestination>,
        stage: PreparedStage,
    ) -> Self {
        if stage.direct() {
            self.final_destination_changed = true;
            return self;
        }
        self.stage_cleanup = Some(Box::new(FailedStage {
            destination,
            stage,
            disposition: StageCleanup::AfterPublication,
        }));
        self
    }

    #[must_use]
    pub const fn phase(&self) -> TransferPhase {
        self.phase
    }

    #[must_use]
    pub const fn side(&self) -> TransferSide {
        self.side
    }

    /// Keep an independent destination error primary; only the original input cause is source-owned.
    pub(crate) fn attribute_source_input(mut self, cause: &StorageRoleFailure) -> Self {
        if self.role.as_deref() == Some(cause) {
            self.side = TransferSide::Source;
        }
        self
    }

    /// Whether the failed attempt retains unpublished state eligible for a later resume. `false`
    /// does not mean nothing can be resumed: a destination may keep its pointer from prepare
    /// (S3 since ADR-0006 C16), so an unrecoverable stage that is dropped instead of discarded
    /// may still be resumed by the next attempt. Discard it to start over.
    #[must_use]
    pub fn has_recoverable_stage(&self) -> bool {
        self.stage_cleanup.as_ref().is_some_and(|failed| {
            matches!(failed.disposition, StageCleanup::Unpublished)
                && failed.stage.recovery_enabled()
        })
    }

    /// Whether the failed attempt retains unpublished state that may be explicitly discarded.
    /// A resumed stage kept at the destination that failed verification is not retained: it was
    /// cleaned up in place, so its pointer cannot resume it again.
    #[must_use]
    pub fn has_unpublished_stage(&self) -> bool {
        self.stage_cleanup
            .as_ref()
            .is_some_and(|pending| matches!(pending.disposition, StageCleanup::Unpublished))
    }

    /// Whether publication committed — or may have: an S3 completion that failed without a
    /// definite refusal is reported so (ADR-0006 C15c) — and staged artifacts still require
    /// cleanup. Not proof that the new content is published.
    #[must_use]
    pub fn has_pending_cleanup(&self) -> bool {
        self.stage_cleanup
            .as_ref()
            .is_some_and(|pending| matches!(pending.disposition, StageCleanup::AfterPublication))
    }

    /// Whether publication committed — or may have (an S3 completion that failed without a
    /// definite refusal, ADR-0006 C15c) — or a direct attempt may have modified the final
    /// target. Not proof that the new content is published: retry according to the error's
    /// transience.
    #[must_use]
    pub const fn final_destination_changed(&self) -> bool {
        self.final_destination_changed
    }

    /// Actual source work charged before this attempt failed.
    #[must_use]
    pub const fn source_qos(&self) -> SourceQosStats {
        self.source_qos
    }

    /// Returns the metadata application failure, every failed family with it, and its partial family report.
    #[must_use]
    pub fn metadata_failure(&self) -> Option<&crate::metadata::MetadataApplicationFailure> {
        self.metadata.as_deref()
    }

    /// Why copied metadata was refused while planning — which family, under which policy, and
    /// which end lacks it — when that is what failed.
    #[must_use]
    pub const fn metadata_refusal(&self) -> Option<crate::metadata::MetadataPlanError> {
        match &self.refusal {
            Some(error) => Some(**error),
            None => None,
        }
    }

    pub(super) fn with_source_qos(mut self, source_qos: SourceQosStats) -> Self {
        self.source_qos = source_qos;
        self
    }

    /// Cleans only this failed attempt's owned artifacts, preserving its original typed cause.
    ///
    /// Checkpointed attempts retain unpublished recovery state. Other policies discard it;
    /// after possible publication only transient artifacts are removed. Cleanup never rolls
    /// back the final path. A cleanup error is retained alongside the original failure.
    #[must_use]
    pub async fn cleanup_for_policy(mut self, policy: TransferPolicy) -> Self {
        if policy == TransferPolicy::Checkpointed && self.has_unpublished_stage() {
            return self;
        }
        let Some(pending) = self.stage_cleanup.take() else {
            return self;
        };
        let result = match pending.disposition {
            StageCleanup::AfterPublication => cleanup_published(pending).await,
            StageCleanup::Unpublished => pending.destination.discard(pending.stage).await,
        };
        self.cleanup_failure = result.err().map(Box::new);
        self
    }

    /// Failure to release owned artifacts, separate from the transfer's original cause.
    #[must_use]
    pub fn cleanup_failure(&self) -> Option<&StorageRoleFailure> {
        self.cleanup_failure.as_deref()
    }

    /// Consumes this failure and discards its unpublished stage, whether recoverable or ephemeral.
    ///
    /// # Errors
    /// Returns a storage-role failure if no unpublished stage exists or cleanup fails.
    pub async fn discard_stage(mut self) -> Result<(), StorageRoleFailure> {
        let failed = self
            .stage_cleanup
            .take()
            .filter(|pending| matches!(pending.disposition, StageCleanup::Unpublished))
            .ok_or_else(|| source_failure(&StoragePath::root(), FailureClass::InvalidInput))?;
        failed.destination.discard(failed.stage).await
    }

    /// Consumes a failure after publication committed — or may have — and idempotently removes
    /// staged artifacts only.
    ///
    /// This never removes or rolls back the final destination. After an S3 completion that may
    /// not have committed (ADR-0006 C15c), cleanup aborts the upload: the final key then keeps
    /// whatever it held, so retry the transfer instead when the failure is transient.
    ///
    /// # Errors
    /// Returns a storage-role failure if no committed cleanup is pending or cleanup fails.
    pub async fn cleanup_published_stage(mut self) -> Result<(), StorageRoleFailure> {
        let pending = self
            .stage_cleanup
            .take()
            .filter(|pending| matches!(pending.disposition, StageCleanup::AfterPublication))
            .ok_or_else(|| source_failure(&StoragePath::root(), FailureClass::InvalidInput))?;
        cleanup_published(pending).await
    }
}

async fn cleanup_published(pending: Box<FailedStage>) -> Result<(), StorageRoleFailure> {
    if pending.stage.checkpoint_enabled()
        && pending.stage.checkpoint_phase() == CheckpointPhase::Published
    {
        // The payload already committed: release only the owned transient root. In
        // particular, an S3 Object Lock retry must not hide retained hash versions behind
        // another delete marker through the legacy discard path.
        pending
            .destination
            .release_hash_checkpoint(&pending.stage)
            .await
    } else {
        pending.destination.discard(pending.stage).await
    }
}

impl fmt::Display for TransferFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "transfer {:?} failed on {:?}: {}",
            self.phase, self.side, self.message
        )?;
        if self.phase == TransferPhase::Metadata {
            negotiation::write_metadata_detail(
                formatter,
                self.metadata_refusal(),
                self.metadata.as_deref(),
                self.role.as_deref(),
            )?;
        } else if let Some(role) = &self.role {
            write!(formatter, ": {}", role_failure_text(role))?;
        }
        if let Some(cleanup) = &self.cleanup_failure {
            write!(
                formatter,
                "; artifact cleanup failed: {}",
                role_failure_text(cleanup)
            )?;
        }
        Ok(())
    }
}

/// A metadata-phase failure has no `source()`: its Display already carries the refusal, the failed
/// write, or the failed read, with the storage diagnostic, and a chain printer would repeat them.
/// `metadata_refusal()` and `metadata_failure()` reach them as values.
impl std::error::Error for TransferFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if self.phase == TransferPhase::Metadata {
            return None;
        }
        self.role
            .as_ref()
            .map(|error| error.as_ref() as &(dyn std::error::Error + 'static))
            .or_else(|| {
                self.metadata
                    .as_ref()
                    .map(|error| error.as_ref() as &(dyn std::error::Error + 'static))
            })
    }
}

pub(super) fn source_failure(path: &StoragePath, class: FailureClass) -> StorageRoleFailure {
    let transience = if class == FailureClass::Cancelled {
        Transience::Transient
    } else {
        Transience::Permanent
    };
    let error = EntryOperationFailure::new(
        path.clone(),
        Operation::Read,
        class,
        transience,
        "transfer source stream failed",
    )
    .unwrap_or_else(|_| unreachable!("static diagnostic is valid"));
    StorageRoleFailure::Entry(error)
}

pub(super) fn failure_side(error: &StorageRoleFailure) -> TransferSide {
    match error {
        StorageRoleFailure::Entry(error) if error.operation() == Operation::Read => {
            TransferSide::Source
        }
        StorageRoleFailure::Session(error) if error.operation() == Operation::Read => {
            TransferSide::Source
        }
        StorageRoleFailure::Entry(_) | StorageRoleFailure::Session(_) => TransferSide::Destination,
    }
}

#[cfg(test)]
#[path = "failure_tests.rs"]
mod tests;
