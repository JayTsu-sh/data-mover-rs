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
//! | a stage longer than the source (when its size is known) | clean up, start from zero (`StageBeyondSource`) |
//! | a stage proving less than the pointer's prefix | clean up, start from zero (`StageBehindPointer`) |
//! | otherwise | resume from the durable prefix — or, for a backend that opts in (`continues_from_stage`), from the length the stage proves |
//!
//! The identity is checked before the binding because the binding hashes the identity: another
//! transfer's pointer differs in both.

use async_trait::async_trait;

use super::checkpoint::{CheckpointData, CheckpointPhase, insert_range};
use super::pointer::{DestinationPointer, MAX_POINTER_BYTES, is_checkpoint_pointer};
use super::range_hash::RangeHash;
use super::{PrepareRequest, StorageRoleFailure};
use crate::model::{EntryOperationFailure, FailureClass, Operation, StoragePath, Transience};

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
    /// The stage holds more than the source has: something other than this transfer wrote to it
    /// (the binding pins the source's size, and a writer stops there).
    StageBeyondSource,
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
    /// Whether a fresh stage is recoverable from the start (it writes its pointer at once, so a
    /// crash can be resumed); `false` is the ephemeral prepare, whose recovery turns on at its
    /// first deferred checkpoint. Its pointer is written there — or, on S3 with `Discover` and a
    /// size over the interval, already at prepare (ADR-0006 C16), so an attempt that fails before
    /// that checkpoint and is not discarded can still be resumed. A resumed stage keeps its
    /// pointer either way.
    pub recoverable: bool,
    /// Ordinary eligible Checkpointed transfer, never an expert or non-checkpointed request.
    pub(crate) checkpoint_hash: bool,
    /// Permit v2 discovery after planning changes without resuming legacy small-file state.
    pub(crate) legacy_restart: bool,
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
            checkpoint_hash: false,
            legacy_restart: false,
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

    pub(crate) const fn with_checkpoint_hash(mut self, enabled: bool) -> Self {
        self.checkpoint_hash = enabled;
        self
    }

    pub(crate) const fn with_legacy_restart(mut self, restart: bool) -> Self {
        self.legacy_restart = restart;
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
    /// Whether a resume continues from `stage_bytes` rather than from the pointer's prefix
    /// (see [`DestinationArtifacts::continues_from_stage`]).
    pub(crate) continues_from_stage: bool,
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
    if request.resume == ResumeMode::Restart
        || (request.legacy_restart
            && !matches!(&found.pointer,
            FoundPointer::Present(pointer) if pointer.checkpoint.is_some()))
    {
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
    if request
        .prepare
        .source
        .size
        .is_some_and(|size| stage_bytes > size)
    {
        return clean(RestartReason::StageBeyondSource);
    }
    let prefix = match pointer.durable_prefix {
        Some(prefix) if stage_bytes < prefix => return clean(RestartReason::StageBehindPointer),
        Some(_) if found.continues_from_stage => stage_bytes,
        Some(prefix) => prefix,
        None => stage_bytes,
    };
    Decision::Resume(ResumePoint {
        prefix,
        pointer: pointer.clone(),
        published: false,
    })
}

/// The artifacts one final file's recovery looks at, as a destination backend exposes them.
/// Removals are idempotent and also remove the artifact's temporary.
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
    /// Only queried for a matching hash-ready root whose stage has disappeared. This is a
    /// candidate, not proof: the engine must verify its bytes against the saved source hash.
    async fn observe_final(
        &self,
        _final_path: &StoragePath,
    ) -> Result<Option<u64>, StorageRoleFailure> {
        Ok(None)
    }
    async fn remove_pointer(&self, final_path: &StoragePath) -> Result<(), StorageRoleFailure>;
    async fn remove_stage(&self, final_path: &StoragePath) -> Result<(), StorageRoleFailure>;
    /// Whether a pointer that decodes is one this backend wrote. A refused one is cleaned up as
    /// corrupt: a backend whose `observe_stage` only confirms the stage refuses a pointer without
    /// a durable prefix, so a file length is never taken as the resume offset.
    fn accepts_pointer(&self, _pointer: &DestinationPointer) -> bool {
        true
    }
    /// Whether a resume continues from the length `observe_stage` proves rather than from the
    /// pointer's prefix. Only for a backend whose stage length is itself proven — HDFS after lease
    /// recovery, where bytes past the last hsync were acknowledged by the whole pipeline — and
    /// which cannot shorten the stage to the prefix. The pointer's prefix stays a lower bound.
    fn continues_from_stage(&self) -> bool {
        false
    }
}

/// Where a resume starts, and the pointer it continues. Stage bytes past `prefix` are not proven
/// and must be discarded before the writer continues from `prefix`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResumePoint {
    pub(crate) prefix: u64,
    pub(crate) pointer: DestinationPointer,
    pub(crate) published: bool,
}

/// The outcome of looking: the fact to report and, when resuming, where from.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Discovery {
    pub(crate) fact: PrepareFact,
    pub(crate) resume: Option<ResumePoint>,
}

/// Looks at the destination and carries out the decision. A clean-up removes the pointer before
/// the stage, so a crash in between leaves "stage without pointer", which the table also cleans.
/// A leftover temporary beside an otherwise empty place is not looked at here; the backend removes
/// it by name while it holds the final file (Local: at prepare, under its claim).
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
        Some(bytes) => match DestinationPointer::decode(&bytes) {
            Err(_) if is_checkpoint_pointer(&bytes) => return Err(checkpoint_refused(final_path)),
            Ok(pointer)
                if pointer.checkpoint.is_some()
                    && !request.checkpoint_hash
                    && request.resume == ResumeMode::Discover =>
            {
                return Err(checkpoint_refused(final_path));
            }
            Ok(pointer) if artifacts.accepts_pointer(&pointer) => FoundPointer::Present(pointer),
            Ok(pointer) if pointer.checkpoint.is_some() => {
                return Err(checkpoint_refused(final_path));
            }
            _ => FoundPointer::Corrupt,
        },
    };
    validate_hash_pointer(&pointer, final_path)?;
    if let FoundPointer::Present(pointer) = &pointer
        && pointer.checkpoint.is_some()
        && request.resume == ResumeMode::Discover
        && (pointer.transfer_identity != request.transfer_identity
            || pointer.binding != request.prepare.recovery_binding)
    {
        return Err(checkpoint_refused(final_path));
    }
    let found = Found {
        pointer,
        stage_bytes: artifacts.observe_stage(final_path).await?,
        continues_from_stage: artifacts.continues_from_stage(),
    };
    if let Some(published) = published_candidate(artifacts, request, &found).await? {
        return Ok(published);
    }
    match decide(&found, request) {
        Decision::Fresh => Ok(Discovery {
            fact: PrepareFact::Fresh,
            resume: None,
        }),
        Decision::Resume(point) => Ok(Discovery {
            fact: PrepareFact::Resumed {
                bytes: reused_bytes(&point),
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

fn reused_bytes(point: &ResumePoint) -> u64 {
    let Some(mut data) = point
        .pointer
        .checkpoint
        .as_deref()
        .and_then(|bytes| CheckpointData::decode(bytes).ok())
    else {
        return point.prefix;
    };
    // The append adapter may independently prove a tail longer than the saved pointer.
    // Sparse adapters retain all committed islands, not just the contiguous write offset.
    insert_range(&mut data.ranges, 0..point.prefix);
    data.ranges
        .iter()
        .map(|range| range.end - range.start)
        .sum()
}

fn validate_hash_pointer(
    pointer: &FoundPointer,
    path: &StoragePath,
) -> Result<(), StorageRoleFailure> {
    let FoundPointer::Present(pointer) = pointer else {
        return Ok(());
    };
    let Some(encoded) = &pointer.checkpoint else {
        return Ok(());
    };
    let data = CheckpointData::decode(encoded).map_err(|_| checkpoint_refused(path))?;
    let hash = RangeHash::decode(&data.hash, data.size).map_err(|_| checkpoint_refused(path))?;
    if pointer
        .durable_prefix
        .is_some_and(|prefix| prefix != data.prefix())
        || data.ranges.iter().any(|range| !hash.covers(range.clone()))
        || (data.phase != CheckpointPhase::Receiving
            && (!hash.covers(0..data.size) || data.prefix() != data.size))
    {
        return Err(checkpoint_refused(path));
    }
    Ok(())
}

async fn published_candidate(
    artifacts: &dyn DestinationArtifacts,
    request: &DestinationPrepareRequest,
    found: &Found,
) -> Result<Option<Discovery>, StorageRoleFailure> {
    let FoundPointer::Present(pointer) = &found.pointer else {
        return Ok(None);
    };
    if found.stage_bytes.is_some()
        || request.resume != ResumeMode::Discover
        || !request.checkpoint_hash
        || pointer.binding != request.prepare.recovery_binding
        || pointer.transfer_identity != request.transfer_identity
    {
        return Ok(None);
    }
    let Some(encoded) = &pointer.checkpoint else {
        return Ok(None);
    };
    let path = request.prepare.final_destination.path();
    let data = CheckpointData::decode(encoded).map_err(|_| checkpoint_refused(path))?;
    if data.phase == CheckpointPhase::Receiving
        || request.prepare.source.size != Some(data.size)
        || artifacts.observe_final(path).await? != Some(data.size)
    {
        return Err(checkpoint_refused(path));
    }
    Ok(Some(Discovery {
        fact: PrepareFact::Resumed { bytes: data.size },
        resume: Some(ResumePoint {
            prefix: data.size,
            pointer: pointer.clone(),
            published: true,
        }),
    }))
}

fn checkpoint_refused(path: &StoragePath) -> StorageRoleFailure {
    StorageRoleFailure::Entry(EntryOperationFailure::new(path.clone(), Operation::Prepare, FailureClass::Unsupported, Transience::Permanent, "checkpoint hash state is corrupt, unsupported, or incompatible with this recovery interface").unwrap_or_else(|_| unreachable!("static checkpoint diagnostic")))
}

#[cfg(test)]
#[path = "discovery_tests.rs"]
mod tests;
