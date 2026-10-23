use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;

use super::*;
use crate::model::{
    BackendKind, EntryKind, EntryOperationFailure, FailureClass, IdentityStrength, Operation,
    SourceIdentity, Transience,
};
use crate::storage::{FinalDestination, SourceDescriptor};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const BINDING: [u8; 32] = [1; 32];
const IDENTITY: [u8; 32] = [2; 32];
const PREFIX: u64 = 100;
const SOURCE_SIZE: u64 = 1000;

fn request(resume: ResumeMode) -> Result<DestinationPrepareRequest, Box<dyn std::error::Error>> {
    let path = StoragePath::new("dir/file.bin")?;
    let identity = SourceIdentity::new(
        crate::model::BackendIdentity::new(BackendKind::Local, "source")?,
        IdentityStrength::PathScoped,
        b"file",
    )?;
    Ok(DestinationPrepareRequest::new(
        PrepareRequest {
            final_destination: FinalDestination::new(path.clone()),
            source: SourceDescriptor::new(path, EntryKind::File, Some(SOURCE_SIZE), identity),
            recovery_binding: BINDING,
        },
        IDENTITY,
    )
    .with_resume(resume))
}

fn pointer(binding: [u8; 32], identity: [u8; 32], prefix: Option<u64>) -> DestinationPointer {
    DestinationPointer {
        binding,
        transfer_identity: identity,
        durable_prefix: prefix,
        extension: Bytes::new(),
        checkpoint: None,
    }
}

#[derive(Clone, Copy, Debug)]
enum PointerCase {
    Absent,
    Corrupt,
    Same,
    OtherBinding,
    OtherIdentity,
    /// Another transfer as it really looks: its binding hashes its identity, so both differ.
    OtherTransferBoth,
}

/// The module's table, row by row, as a second statement of it: it catches a row that goes
/// missing or moves, not a table that is wrong in both places.
fn expected(
    mode: ResumeMode,
    pointer: PointerCase,
    stage: Option<u64>,
    prefix: Option<u64>,
    continues: bool,
) -> Result<Option<u64>, RestartReason> {
    use PointerCase as P;
    use RestartReason as R;
    match (pointer, stage) {
        (P::Absent, None) => return Ok(None),
        _ if mode == ResumeMode::Restart => return Err(R::Requested),
        (P::Corrupt, _) => return Err(R::PointerCorrupt),
        (P::Same | P::OtherBinding | P::OtherIdentity | P::OtherTransferBoth, None) => {
            return Err(R::PointerWithoutStage);
        }
        (P::Absent, Some(_)) => return Err(R::StageWithoutPointer),
        (P::OtherIdentity | P::OtherTransferBoth, Some(_)) => return Err(R::OtherTransfer),
        (P::OtherBinding, Some(_)) => return Err(R::BindingChanged),
        (P::Same, Some(_)) => {}
    }
    let stage = stage.unwrap_or(0);
    if stage > SOURCE_SIZE {
        return Err(R::StageBeyondSource);
    }
    match prefix {
        Some(prefix) if stage < prefix => Err(R::StageBehindPointer),
        Some(_) if continues => Ok(Some(stage)),
        Some(prefix) => Ok(Some(prefix)),
        None => Ok(Some(stage)),
    }
}

#[test]
fn the_decision_table_matches_the_adr_in_every_combination() -> TestResult {
    for mode in [ResumeMode::Discover, ResumeMode::Restart] {
        let request = request(mode)?;
        for case in [
            PointerCase::Absent,
            PointerCase::Corrupt,
            PointerCase::Same,
            PointerCase::OtherBinding,
            PointerCase::OtherIdentity,
            PointerCase::OtherTransferBoth,
        ] {
            for stage in [
                None,
                Some(PREFIX - 1),
                Some(PREFIX),
                Some(PREFIX + 1),
                Some(SOURCE_SIZE),
                Some(SOURCE_SIZE + 1),
            ] {
                for (prefix, continues) in
                    [(Some(PREFIX), false), (Some(PREFIX), true), (None, false)]
                {
                    let found_pointer = match case {
                        PointerCase::Absent => FoundPointer::Absent,
                        PointerCase::Corrupt => FoundPointer::Corrupt,
                        PointerCase::Same => {
                            FoundPointer::Present(pointer(BINDING, IDENTITY, prefix))
                        }
                        PointerCase::OtherBinding => {
                            FoundPointer::Present(pointer([9; 32], IDENTITY, prefix))
                        }
                        PointerCase::OtherIdentity => {
                            FoundPointer::Present(pointer(BINDING, [9; 32], prefix))
                        }
                        PointerCase::OtherTransferBoth => {
                            FoundPointer::Present(pointer([8; 32], [9; 32], prefix))
                        }
                    };
                    let decision = decide(
                        &Found {
                            pointer: found_pointer,
                            stage_bytes: stage,
                            continues_from_stage: continues,
                        },
                        &request,
                    );
                    let got = match decision {
                        Decision::Fresh => Ok(None),
                        Decision::Resume(point) => Ok(Some(point.prefix)),
                        Decision::Clean { reason } => Err(reason),
                    };
                    assert_eq!(
                        got,
                        expected(mode, case, stage, prefix, continues),
                        "{mode:?} {case:?} stage={stage:?} prefix={prefix:?} continues={continues}"
                    );
                }
            }
        }
    }
    Ok(())
}

/// A source of unknown size never triggers the beyond-source row: the stage's length has nothing to
/// be compared with, so the old result stands.
#[test]
fn an_unknown_source_size_skips_the_beyond_source_row() -> TestResult {
    let mut request = request(ResumeMode::Discover)?;
    request.prepare.source.size = None;
    let decision = decide(
        &Found {
            pointer: FoundPointer::Present(pointer(BINDING, IDENTITY, Some(PREFIX))),
            stage_bytes: Some(SOURCE_SIZE + 1),
            continues_from_stage: false,
        },
        &request,
    );
    assert!(matches!(decision, Decision::Resume(point) if point.prefix == PREFIX));
    Ok(())
}

/// An in-memory destination: the pointer's bytes and the stage's length, a log of removals, and
/// an optional failing removal.
#[derive(Default)]
struct Memory {
    pointer: Mutex<Option<Vec<u8>>>,
    stage: Mutex<Option<u64>>,
    final_size: Option<u64>,
    removed: Mutex<Vec<&'static str>>,
    fail_stage_removal: bool,
    /// Refuses a pointer without a durable prefix, as a backend that only confirms its stage does.
    requires_prefix: bool,
}

#[async_trait]
impl DestinationArtifacts for Memory {
    async fn observe_final(&self, _path: &StoragePath) -> Result<Option<u64>, StorageRoleFailure> {
        Ok(self.final_size)
    }
    async fn read_pointer(
        &self,
        _final_path: &StoragePath,
        limit: usize,
    ) -> Result<Option<Vec<u8>>, StorageRoleFailure> {
        Ok(self
            .pointer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .map(|mut bytes| {
                bytes.truncate(limit);
                bytes
            }))
    }
    async fn observe_stage(
        &self,
        _final_path: &StoragePath,
    ) -> Result<Option<u64>, StorageRoleFailure> {
        Ok(*self
            .stage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner))
    }
    async fn remove_pointer(&self, _final_path: &StoragePath) -> Result<(), StorageRoleFailure> {
        self.removed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push("pointer");
        *self
            .pointer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        Ok(())
    }
    async fn remove_stage(&self, final_path: &StoragePath) -> Result<(), StorageRoleFailure> {
        if self.fail_stage_removal {
            return Err(StorageRoleFailure::Entry(
                EntryOperationFailure::new(
                    final_path.clone(),
                    Operation::Namespace,
                    FailureClass::PermissionDenied,
                    Transience::Permanent,
                    "refused",
                )
                .unwrap_or_else(|_| unreachable!("the static diagnostic is valid")),
            ));
        }
        self.removed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push("stage");
        *self
            .stage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        Ok(())
    }
    fn accepts_pointer(&self, pointer: &DestinationPointer) -> bool {
        !self.requires_prefix || pointer.durable_prefix.is_some()
    }
}

fn memory(pointer: Option<Vec<u8>>, stage: Option<u64>) -> Memory {
    Memory {
        pointer: Mutex::new(pointer),
        stage: Mutex::new(stage),
        ..Memory::default()
    }
}

fn hash_pointer(phase: CheckpointPhase) -> Result<DestinationPointer, Box<dyn std::error::Error>> {
    let mut hash = RangeHash::new(SOURCE_SIZE);
    hash.observe(0, &vec![42; usize::try_from(SOURCE_SIZE)?])?;
    let data = CheckpointData {
        size: SOURCE_SIZE,
        phase,
        hash: Bytes::from(hash.encode()?),
        ranges: std::iter::once(0..SOURCE_SIZE).collect(),
        ..CheckpointData::default()
    };
    Ok(DestinationPointer {
        checkpoint: Some(data.encode()),
        ..pointer(BINDING, IDENTITY, Some(SOURCE_SIZE))
    })
}

#[tokio::test]
async fn published_hash_root_is_reconciled_without_legacy_cleanup() -> TestResult {
    for phase in [CheckpointPhase::ReadyToPublish, CheckpointPhase::Published] {
        let written = hash_pointer(phase)?;
        let mut destination = memory(Some(written.encode().map_err(|_| "encode")?), None);
        destination.final_size = Some(SOURCE_SIZE);
        let found = discover(
            &destination,
            &request(ResumeMode::Discover)?.with_checkpoint_hash(true),
        )
        .await?;
        assert_eq!(found.fact, PrepareFact::Resumed { bytes: SOURCE_SIZE });
        assert!(found.resume.is_some_and(|point| point.published));
        assert!(
            destination
                .removed
                .lock()
                .map_err(|_| "poisoned")?
                .is_empty()
        );
    }
    Ok(())
}

#[tokio::test]
async fn hash_roots_fail_closed_on_interface_mismatch_or_missing_final() -> TestResult {
    let written = hash_pointer(CheckpointPhase::ReadyToPublish)?;
    for allowed in [false, true] {
        let destination = memory(Some(written.encode().map_err(|_| "encode")?), None);
        assert!(
            discover(
                &destination,
                &request(ResumeMode::Discover)?.with_checkpoint_hash(allowed)
            )
            .await
            .is_err()
        );
        assert!(
            destination
                .removed
                .lock()
                .map_err(|_| "poisoned")?
                .is_empty()
        );
    }
    let corrupt = DestinationPointer {
        checkpoint: Some(Bytes::from_static(b"not a hash checkpoint")),
        ..written
    };
    let destination = memory(
        Some(corrupt.encode().map_err(|_| "encode")?),
        Some(SOURCE_SIZE),
    );
    assert!(
        discover(
            &destination,
            &request(ResumeMode::Discover)?.with_checkpoint_hash(true)
        )
        .await
        .is_err()
    );
    assert!(
        destination
            .removed
            .lock()
            .map_err(|_| "poisoned")?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn a_changed_v2_binding_requires_an_explicit_restart() -> TestResult {
    for changed_identity in [false, true] {
        let mut written = hash_pointer(CheckpointPhase::Receiving)?;
        if changed_identity {
            written.transfer_identity = [7; 32];
        } else {
            written.binding = [7; 32];
        }
        let destination = memory(
            Some(written.encode().map_err(|_| "encode")?),
            Some(SOURCE_SIZE),
        );
        let request = request(ResumeMode::Discover)?.with_checkpoint_hash(true);
        assert!(discover(&destination, &request).await.is_err());
        assert!(
            destination
                .removed
                .lock()
                .map_err(|_| "poisoned")?
                .is_empty()
        );
        let restarted = discover(&destination, &request.with_resume(ResumeMode::Restart)).await?;
        assert_eq!(
            restarted.fact,
            PrepareFact::Restarted {
                reason: RestartReason::Requested
            }
        );
    }
    Ok(())
}

#[tokio::test]
async fn v2_reused_bytes_count_islands_without_moving_the_write_prefix() -> TestResult {
    let mut written = hash_pointer(CheckpointPhase::Receiving)?;
    let mut data = CheckpointData::decode(written.checkpoint.as_deref().ok_or("checkpoint")?)?;
    data.ranges = vec![0..17, 31..72];
    written.checkpoint = Some(data.encode());
    written.durable_prefix = Some(17);
    let destination = memory(Some(written.encode().map_err(|_| "encode")?), Some(72));
    let found = discover(
        &destination,
        &request(ResumeMode::Discover)?.with_checkpoint_hash(true),
    )
    .await?;
    assert_eq!(found.fact, PrepareFact::Resumed { bytes: 58 });
    assert_eq!(found.resume.ok_or("resume")?.prefix, 17);
    Ok(())
}

#[tokio::test]
async fn hash_only_discovery_preserves_the_original_legacy_restart_decision() -> TestResult {
    let legacy = pointer(BINDING, IDENTITY, Some(PREFIX));
    let destination = memory(Some(legacy.encode().map_err(|_| "encode")?), Some(PREFIX));
    let request = request(ResumeMode::Discover)?
        .with_checkpoint_hash(true)
        .with_legacy_restart(true);
    let found = discover(&destination, &request).await?;
    assert_eq!(
        found.fact,
        PrepareFact::Restarted {
            reason: RestartReason::Requested
        }
    );
    let v2 = hash_pointer(CheckpointPhase::Receiving)?;
    let destination = memory(Some(v2.encode().map_err(|_| "encode")?), Some(SOURCE_SIZE));
    let found = discover(&destination, &request).await?;
    assert_eq!(found.fact, PrepareFact::Resumed { bytes: SOURCE_SIZE });
    assert!(
        destination
            .removed
            .lock()
            .map_err(|_| "poisoned")?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn an_equal_binding_resumes_from_the_recorded_prefix() -> TestResult {
    let written = pointer(BINDING, IDENTITY, Some(PREFIX));
    let destination = memory(
        Some(written.encode().map_err(|_| "encode")?),
        Some(PREFIX + 7),
    );
    let found = discover(&destination, &request(ResumeMode::Discover)?).await?;
    assert_eq!(found.fact, PrepareFact::Resumed { bytes: PREFIX });
    assert_eq!(
        found.resume,
        Some(ResumePoint {
            prefix: PREFIX,
            pointer: written,
            published: false,
        })
    );
    assert!(
        destination
            .removed
            .lock()
            .map_err(|_| "poisoned")?
            .is_empty()
    );
    Ok(())
}

/// A pointer without a durable prefix (S3's, whose `ListParts` is the durable record) resumes from
/// what the stage proves, without the backend opting into `continues_from_stage`.
#[tokio::test]
async fn a_pointer_without_a_prefix_resumes_from_the_proven_stage() -> TestResult {
    let written = pointer(BINDING, IDENTITY, None);
    let destination = memory(Some(written.encode().map_err(|_| "encode")?), Some(PREFIX));
    assert!(!destination.continues_from_stage());
    let found = discover(&destination, &request(ResumeMode::Discover)?).await?;
    assert_eq!(found.fact, PrepareFact::Resumed { bytes: PREFIX });
    assert_eq!(
        found.resume,
        Some(ResumePoint {
            prefix: PREFIX,
            pointer: written,
            published: false,
        })
    );
    Ok(())
}

/// A clean-up removes the pointer before the stage: a crash between the two leaves a stage
/// without a pointer, which the table cleans next time.
#[tokio::test]
async fn a_clean_up_removes_the_pointer_first_then_the_stage() -> TestResult {
    let other = pointer([9; 32], IDENTITY, Some(PREFIX));
    let destination = memory(Some(other.encode().map_err(|_| "encode")?), Some(PREFIX));
    let found = discover(&destination, &request(ResumeMode::Discover)?).await?;
    assert_eq!(
        found.fact,
        PrepareFact::Restarted {
            reason: RestartReason::BindingChanged
        }
    );
    assert_eq!(found.resume, None);
    assert_eq!(
        *destination.removed.lock().map_err(|_| "poisoned")?,
        ["pointer", "stage"]
    );
    Ok(())
}

/// Bytes that do not decode — including a pointer longer than any real one — are corrupt.
#[tokio::test]
async fn an_undecodable_or_oversized_pointer_is_corrupt() -> TestResult {
    for bytes in [b"garbage".to_vec(), vec![0; MAX_POINTER_BYTES + 10]] {
        let destination = memory(Some(bytes), Some(PREFIX));
        let found = discover(&destination, &request(ResumeMode::Discover)?).await?;
        assert_eq!(
            found.fact,
            PrepareFact::Restarted {
                reason: RestartReason::PointerCorrupt
            }
        );
    }
    Ok(())
}

/// A clean-up whose stage removal fails fails the prepare — after the pointer is gone, which is
/// the state a crash between the two removals leaves. The next prepare finds a stage without a
/// pointer and cleans it.
#[tokio::test]
async fn an_interrupted_clean_up_is_finished_by_the_next_prepare() -> TestResult {
    let other = pointer([9; 32], IDENTITY, Some(PREFIX));
    let mut destination = Memory {
        fail_stage_removal: true,
        ..memory(Some(other.encode().map_err(|_| "encode")?), Some(PREFIX))
    };
    let result = discover(&destination, &request(ResumeMode::Discover)?).await;
    assert!(matches!(result, Err(StorageRoleFailure::Entry(_))));
    assert!(
        destination
            .pointer
            .lock()
            .map_err(|_| "poisoned")?
            .is_none()
    );
    assert_eq!(
        *destination.stage.lock().map_err(|_| "poisoned")?,
        Some(PREFIX)
    );

    destination.fail_stage_removal = false;
    let found = discover(&destination, &request(ResumeMode::Discover)?).await?;
    assert_eq!(
        found.fact,
        PrepareFact::Restarted {
            reason: RestartReason::StageWithoutPointer
        }
    );
    assert_eq!(*destination.stage.lock().map_err(|_| "poisoned")?, None);
    Ok(())
}

/// A pointer that decodes but that the backend refuses is cleaned up like a corrupt one — here, a
/// pointer without a prefix, which would otherwise resume from the stage's length.
#[tokio::test]
async fn a_refused_pointer_is_cleaned_as_corrupt() -> TestResult {
    let without_prefix = pointer(BINDING, IDENTITY, None);
    let destination = Memory {
        requires_prefix: true,
        ..memory(
            Some(without_prefix.encode().map_err(|_| "encode")?),
            Some(PREFIX),
        )
    };
    let found = discover(&destination, &request(ResumeMode::Discover)?).await?;
    assert_eq!(
        found.fact,
        PrepareFact::Restarted {
            reason: RestartReason::PointerCorrupt
        }
    );
    assert_eq!(found.resume, None);
    assert_eq!(
        *destination.removed.lock().map_err(|_| "poisoned")?,
        ["pointer", "stage"]
    );
    Ok(())
}

#[test]
fn only_a_resume_reuses_bytes() {
    assert_eq!(PrepareFact::Fresh.reused_bytes(), 0);
    assert_eq!(PrepareFact::Resumed { bytes: 5 }.reused_bytes(), 5);
    assert_eq!(
        PrepareFact::Restarted {
            reason: RestartReason::Requested
        }
        .reused_bytes(),
        0
    );
}
