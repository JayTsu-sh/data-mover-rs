//! Destination-resident recovery (ADR-0006): at prepare, what is found beside the final file decides
//! between resuming, cleaning up in place and starting fresh — with no state where data-mover runs.
//!
//! [`decide`] applies these rows top to bottom; the first that matches wins:
//!
//! | Found at the destination | Action |
//! |---|---|
//! | nothing | start fresh |
//! | anything, and a restart is requested | clean up in place, start from zero (`Requested`) |
//! | a pointer that does not decode | clean up, start from zero (`PointerCorrupt`) |
//! | a pointer without a stage (probably published) | clean up, start from zero (`PointerWithoutStage`) |
//! | a stage without a pointer | clean up, start from zero (`StageWithoutPointer`) |
//! | another transfer's pointer | clean up, start from zero (`OtherTransfer`) |
//! | this transfer, another binding (the source changed) | clean up, start from zero (`BindingChanged`) |
//! | a stage proving less than the pointer's prefix | clean up, start from zero (`StageBehindPointer`) |
//! | otherwise | resume from the durable prefix |
//!
//! The identity is checked before the binding because the binding hashes the identity: another
//! transfer's pointer differs in both.

use async_trait::async_trait;

use super::pointer::{DestinationPointer, MAX_POINTER_BYTES};
use super::{PrepareRequest, StorageRoleFailure};
use crate::model::StoragePath;

/// Whether prepare may resume what it finds at the destination.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ResumeMode {
    /// Resume an equal binding; clean up anything else.
    #[default]
    Discover,
    /// Clean up whatever is found and start from zero.
    Restart,
}

/// What prepare did with what it found at the destination, reported in the transfer outcome.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PrepareFact {
    /// Nothing was found.
    #[default]
    Fresh,
    /// A stage of this transfer was resumed; `bytes` were already at the destination (possibly
    /// 0, when only the stage itself — an S3 upload, say — is reused).
    Resumed { bytes: u64 },
    /// Something was found and cleaned up in place; the transfer started from zero.
    Restarted { reason: RestartReason },
}

impl PrepareFact {
    /// Bytes the destination already held and the transfer did not write again.
    #[must_use]
    pub const fn reused_bytes(self) -> u64 {
        match self {
            Self::Resumed { bytes } => bytes,
            Self::Fresh | Self::Restarted { .. } => 0,
        }
    }
}

/// Why prepare cleaned up instead of resuming.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestartReason {
    /// The caller asked for a restart, or the policy cannot resume.
    Requested,
    /// The same transfer, but the source changed since the stage was written.
    BindingChanged,
    /// The stage belongs to another transfer to the same final file.
    OtherTransfer,
    /// The pointer does not decode.
    PointerCorrupt,
    /// A pointer without a stage: the stage was most likely published already.
    PointerWithoutStage,
    /// A stage without a pointer: its binding is unknown.
    StageWithoutPointer,
    /// The stage holds less than the pointer's durable prefix.
    StageBehindPointer,
}

/// A prepare that looks at the destination first.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct DestinationPrepareRequest {
    /// The ordinary prepare inputs: final destination, source description and recovery binding.
    pub prepare: PrepareRequest,
    /// The transfer identity (`TransferIdentity` bytes), recorded in the pointer to tell another
    /// transfer from a changed source.
    pub transfer_identity: [u8; 32],
    /// Whether an equal binding found at the destination may be resumed.
    pub resume: ResumeMode,
    /// Whether a fresh stage writes its pointer from the start, so a crash can be resumed; `false`
    /// is the ephemeral prepare, whose pointer (if any) is written at its first deferred
    /// checkpoint. A resumed stage keeps its pointer either way.
    pub recoverable: bool,
}

impl DestinationPrepareRequest {
    /// A recoverable prepare that resumes what it can.
    #[must_use]
    pub const fn new(prepare: PrepareRequest, transfer_identity: [u8; 32]) -> Self {
        Self {
            prepare,
            transfer_identity,
            resume: ResumeMode::Discover,
            recoverable: true,
        }
    }

    /// Sets whether an equal binding may be resumed.
    #[must_use]
    pub const fn with_resume(mut self, resume: ResumeMode) -> Self {
        self.resume = resume;
        self
    }

    /// Sets whether a fresh stage writes its pointer from the start.
    #[must_use]
    pub const fn with_recoverable(mut self, recoverable: bool) -> Self {
        self.recoverable = recoverable;
        self
    }
}

/// The pointer as read from the destination.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FoundPointer {
    Absent,
    Corrupt,
    Present(DestinationPointer),
}

/// What prepare found beside the final file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Found {
    pub(crate) pointer: FoundPointer,
    /// The prefix the stage durably proves (see [`DestinationArtifacts::observe_stage`]), or
    /// `None` without a stage.
    pub(crate) stage_bytes: Option<u64>,
}

/// What to do about it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Decision {
    Fresh,
    Resume(ResumePoint),
    Clean { reason: RestartReason },
}

/// The decision table, top to bottom; the first matching row wins.
pub(crate) fn decide(found: &Found, request: &DestinationPrepareRequest) -> Decision {
    let anything = found.pointer != FoundPointer::Absent || found.stage_bytes.is_some();
    if !anything {
        return Decision::Fresh;
    }
    if request.resume == ResumeMode::Restart {
        return Decision::Clean {
            reason: RestartReason::Requested,
        };
    }
    let clean = |reason| Decision::Clean { reason };
    let pointer = match (&found.pointer, found.stage_bytes) {
        (FoundPointer::Corrupt, _) => return clean(RestartReason::PointerCorrupt),
        (FoundPointer::Present(_), None) => return clean(RestartReason::PointerWithoutStage),
        (FoundPointer::Absent, _) => return clean(RestartReason::StageWithoutPointer),
        (FoundPointer::Present(pointer), Some(_)) => pointer,
    };
    if pointer.transfer_identity != request.transfer_identity {
        return clean(RestartReason::OtherTransfer);
    }
    if pointer.binding != request.prepare.recovery_binding {
        return clean(RestartReason::BindingChanged);
    }
    let stage_bytes = found.stage_bytes.unwrap_or(0);
    let prefix = match pointer.durable_prefix {
        Some(prefix) if stage_bytes < prefix => return clean(RestartReason::StageBehindPointer),
        Some(prefix) => prefix,
        None => stage_bytes,
    };
    Decision::Resume(ResumePoint {
        prefix,
        pointer: pointer.clone(),
    })
}

/// The artifacts one final file's recovery looks at, as a destination backend exposes them.
/// Removals are idempotent and also remove the artifact's temporary.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "implemented by backends moving in ADR-0006 C8")
)]
#[async_trait]
pub(crate) trait DestinationArtifacts: Send + Sync {
    /// The pointer's bytes, read up to `limit` bytes (a longer one is corrupt); `None` if absent.
    async fn read_pointer(
        &self,
        final_path: &StoragePath,
        limit: usize,
    ) -> Result<Option<Vec<u8>>, StorageRoleFailure>;
    /// How many bytes the stage durably proves, or `None` if it is absent — never a file length
    /// that may be sparse or torn: a resume writes on from exactly this offset. Local, NFS and
    /// CIFS store their prefix in the pointer and only confirm the stage here; S3 sums the parts
    /// its upload lists; HDFS reports the visible length after lease recovery.
    async fn observe_stage(
        &self,
        final_path: &StoragePath,
    ) -> Result<Option<u64>, StorageRoleFailure>;
    async fn remove_pointer(&self, final_path: &StoragePath) -> Result<(), StorageRoleFailure>;
    async fn remove_stage(&self, final_path: &StoragePath) -> Result<(), StorageRoleFailure>;
}

/// Where a resume starts, and the pointer it continues. Stage bytes past `prefix` are not proven
/// and must be discarded before the writer continues from `prefix`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResumePoint {
    pub(crate) prefix: u64,
    pub(crate) pointer: DestinationPointer,
}

/// The outcome of looking: the fact to report and, when resuming, where from.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Discovery {
    pub(crate) fact: PrepareFact,
    pub(crate) resume: Option<ResumePoint>,
}

/// Looks at the destination and carries out the decision. A clean-up removes the pointer before
/// the stage, so a crash in between leaves "stage without pointer", which the table also cleans.
/// A leftover temporary beside an otherwise empty place is not looked at here; the reserved-name
/// cleanup removes it.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "called by backends moving in ADR-0006 C8")
)]
pub(crate) async fn discover(
    artifacts: &dyn DestinationArtifacts,
    request: &DestinationPrepareRequest,
) -> Result<Discovery, StorageRoleFailure> {
    let final_path = request.prepare.final_destination.path();
    let pointer = match artifacts
        .read_pointer(final_path, MAX_POINTER_BYTES + 1)
        .await?
    {
        None => FoundPointer::Absent,
        Some(bytes) => {
            DestinationPointer::decode(&bytes).map_or(FoundPointer::Corrupt, FoundPointer::Present)
        }
    };
    let found = Found {
        pointer,
        stage_bytes: artifacts.observe_stage(final_path).await?,
    };
    match decide(&found, request) {
        Decision::Fresh => Ok(Discovery {
            fact: PrepareFact::Fresh,
            resume: None,
        }),
        Decision::Resume(point) => Ok(Discovery {
            fact: PrepareFact::Resumed {
                bytes: point.prefix,
            },
            resume: Some(point),
        }),
        Decision::Clean { reason } => {
            artifacts.remove_pointer(final_path).await?;
            artifacts.remove_stage(final_path).await?;
            Ok(Discovery {
                fact: PrepareFact::Restarted { reason },
                resume: None,
            })
        }
    }
}

#[cfg(test)]
#[path = "discovery_tests.rs"]
mod tests;
