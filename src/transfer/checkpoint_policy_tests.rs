//! Mode isolation at ordinary/expert seams using real Local durability and recording roles.

mod fresh_process;
mod metadata_seal;

use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use futures::SinkExt as _;
use tokio_util::sync::CancellationToken;

use crate::model::{
    EntryOperationFailure, FailureClass, ObservedEntry, Operation, StoragePath, Transience,
};
use crate::storage::backends::local::{test_destination_storage_with_role, test_source_storage};
use crate::storage::checkpoint::CheckpointPhase;
use crate::storage::{
    BackendCapabilities, ByteStream, CapabilityAvailability, CheckpointObservation,
    DestinationPrepareRequest, PositionedByteStream, PrepareRequest, PreparedStage,
    PublicationEvidence, PublicationFailure, PublishRequest, ReadRequest, ReadSource,
    SourceDescriptor, StagedDestination, Storage, StorageRoleFailure, UnsupportedReason,
    VerificationEvidence, VerificationPoint, VerifyRequest, WriteEvidence,
};
use crate::transfer::{
    EffectiveRecovery, ExpertDestinationRequest, ExpertDestinationSession,
    ExpertDestinationTransferred, ExpertSourceEvidence, ExpertSourceRequest, ExpertSourceSession,
    PrepareFact, ReadBackVerification, TransferOutcome, TransferPhase, TransferPolicy,
    TransferRequest, transfer,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const CHUNK: usize = 64 * 1024;
const INTERVAL: u64 = 128 * 1024;
const PATH: &str = "payload.bin";

#[derive(Clone, Debug, Eq, PartialEq)]
enum Call {
    OrderedRead(Option<Range<u64>>),
    PositionedRead,
    OrderedWrite,
    PositionedWrite,
    CheckpointedWrite,
    PersistHash,
    HashPhase(CheckpointPhase),
    ReleaseHash,
    Verify,
    VerifyPublished,
    Publish,
    ArmedOnOrderedPath,
}

#[derive(Default)]
struct Calls(Mutex<Vec<Call>>);

impl Calls {
    fn record(&self, call: Call) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(call);
    }

    fn snapshot(&self) -> Vec<Call> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn clear(&self) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

struct RecordingSource {
    inner: Arc<dyn ReadSource>,
    calls: Arc<Calls>,
}

#[async_trait]
impl ReadSource for RecordingSource {
    fn maximum_read_chunk_bytes(&self) -> usize {
        self.inner.maximum_read_chunk_bytes()
    }

    fn read_concurrency(&self) -> usize {
        self.inner.read_concurrency()
    }

    fn supports_read_budget(&self) -> bool {
        self.inner.supports_read_budget()
    }

    fn supports_positioned_read(&self) -> bool {
        self.inner.supports_positioned_read()
    }

    async fn describe(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.inner.describe(path).await
    }

    async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        self.calls.record(Call::OrderedRead(request.range.clone()));
        self.inner.read(request).await
    }

    async fn read_positioned(
        &self,
        request: ReadRequest,
    ) -> Result<PositionedByteStream, StorageRoleFailure> {
        self.calls.record(Call::PositionedRead);
        self.inner.read_positioned(request).await
    }
}

struct RecordingDestination {
    inner: Arc<dyn StagedDestination>,
    calls: Arc<Calls>,
    resumable: bool,
    faults: Arc<Faults>,
}

#[derive(Default)]
struct Faults {
    ready: AtomicBool,
    published_record: AtomicBool,
    release: AtomicBool,
    published_verify: AtomicBool,
    verify_after: AtomicBool,
}

fn injected_failure(stage: &PreparedStage) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            stage.final_destination.path().clone(),
            Operation::Write,
            FailureClass::Connectivity,
            Transience::Transient,
            "injected publication failure",
        )
        .unwrap_or_else(|_| unreachable!("static test diagnostic")),
    )
}

#[async_trait]
impl StagedDestination for RecordingDestination {
    fn automatic_checkpoint_interval_bytes(&self) -> Option<u64> {
        self.inner.automatic_checkpoint_interval_bytes()
    }

    fn resumable_at_destination(&self) -> bool {
        self.resumable
    }

    fn supports_direct(&self) -> bool {
        self.inner.supports_direct()
    }

    fn supports_positioned_write(&self) -> bool {
        self.inner.supports_positioned_write()
    }

    fn supports_hash_checkpoint(&self) -> bool {
        self.inner.supports_hash_checkpoint()
    }

    fn stage_supports_hash_checkpoint(&self, stage: &PreparedStage) -> bool {
        self.inner.stage_supports_hash_checkpoint(stage)
    }

    fn requires_hash_intent(&self) -> bool {
        self.inner.requires_hash_intent()
    }

    async fn prepare_at_destination(
        &self,
        request: DestinationPrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        self.inner.prepare_at_destination(request).await
    }

    async fn prepare_direct(
        &self,
        request: PrepareRequest,
        cancel: CancellationToken,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        self.inner.prepare_direct(request, cancel).await
    }

    async fn write(
        &self,
        stage: &PreparedStage,
        input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        self.calls.record(Call::OrderedWrite);
        if stage.checkpoint_enabled() {
            self.calls.record(Call::ArmedOnOrderedPath);
        }
        self.inner.write(stage, input).await
    }

    async fn write_positioned(
        &self,
        stage: &PreparedStage,
        input: PositionedByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        self.calls.record(Call::PositionedWrite);
        self.inner.write_positioned(stage, input).await
    }

    async fn write_checkpointed(
        &self,
        stage: &PreparedStage,
        input: PositionedByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        self.calls.record(Call::CheckpointedWrite);
        self.inner.write_checkpointed(stage, input).await
    }

    async fn persist_hash_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<(), StorageRoleFailure> {
        self.calls.record(Call::PersistHash);
        self.calls.record(Call::HashPhase(stage.checkpoint_phase()));
        if stage.checkpoint_phase() == CheckpointPhase::ReadyToPublish
            && self.faults.ready.load(Ordering::SeqCst)
        {
            return Err(injected_failure(stage));
        }
        if stage.checkpoint_phase() == CheckpointPhase::Published
            && self.faults.published_record.load(Ordering::SeqCst)
        {
            return Err(injected_failure(stage));
        }
        self.inner.persist_hash_checkpoint(stage).await
    }

    async fn release_hash_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<(), StorageRoleFailure> {
        self.calls.record(Call::ReleaseHash);
        assert_eq!(stage.checkpoint_phase(), CheckpointPhase::Published);
        if self.faults.release.load(Ordering::SeqCst) {
            return Err(injected_failure(stage));
        }
        self.inner.release_hash_checkpoint(stage).await
    }

    async fn observe_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<CheckpointObservation, StorageRoleFailure> {
        self.inner.observe_checkpoint(stage).await
    }

    async fn verify(
        &self,
        stage: &PreparedStage,
        request: VerifyRequest,
    ) -> Result<VerificationEvidence, StorageRoleFailure> {
        self.calls.record(Call::Verify);
        if request.published.is_some() {
            self.calls.record(Call::VerifyPublished);
            if self.faults.published_verify.load(Ordering::SeqCst) {
                return Err(injected_failure(stage));
            }
        }
        self.inner.verify(stage, request).await
    }

    fn verification_point(&self, stage: &PreparedStage) -> VerificationPoint {
        if self.faults.verify_after.load(Ordering::SeqCst) {
            VerificationPoint::AfterPublish
        } else {
            self.inner.verification_point(stage)
        }
    }

    async fn publish(
        &self,
        stage: &PreparedStage,
        request: PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        self.calls.record(Call::Publish);
        if stage.checkpoint_enabled() {
            assert_eq!(stage.checkpoint_phase(), CheckpointPhase::ReadyToPublish);
        }
        let published = self.inner.publish(stage, request).await?;
        if self.faults.published_record.load(Ordering::SeqCst) {
            stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
        }
        Ok(published)
    }

    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        self.inner.discard(stage).await
    }
}

struct Fixture {
    _source_root: tempfile::TempDir,
    destination_root: tempfile::TempDir,
    source: Storage,
    destination: Storage,
    calls: Arc<Calls>,
    expected: Vec<u8>,
    faults: Arc<Faults>,
}

impl Fixture {
    fn new(size: usize, interval: u64, resumable: bool) -> TestResult<Self> {
        let source_root = tempfile::tempdir()?;
        let destination_root = tempfile::tempdir()?;
        let expected: Vec<u8> = (0..=250_u8).cycle().take(size).collect();
        std::fs::write(source_root.path().join(PATH), &expected)?;
        let (source, source_role) = test_source_storage(source_root.path(), "policy-source")?;
        assert_eq!(source_role.maximum_read_chunk_bytes(), CHUNK);
        let (destination, destination_role) = test_destination_storage_with_role(
            destination_root.path(),
            "checkpoint-policy-destination",
        )?;
        destination_role.set_automatic_checkpoint_interval(interval);
        let calls = Arc::new(Calls::default());
        let faults = Arc::new(Faults::default());
        let source = source_storage(
            &source,
            Arc::new(RecordingSource {
                inner: source_role,
                calls: Arc::clone(&calls),
            }),
        )?;
        let destination = destination_storage(
            &destination,
            Arc::new(RecordingDestination {
                inner: destination_role,
                calls: Arc::clone(&calls),
                resumable,
                faults: Arc::clone(&faults),
            }),
        )?;
        Ok(Self {
            _source_root: source_root,
            destination_root,
            source,
            destination,
            calls,
            expected,
            faults,
        })
    }

    fn request(&self, policy: TransferPolicy) -> TestResult<TransferRequest> {
        Ok(TransferRequest::new(
            self.source.clone(),
            StoragePath::new(PATH)?,
            self.destination.clone(),
            CancellationToken::new(),
        )
        .with_transfer_policy(policy))
    }

    async fn run(&self, policy: TransferPolicy) -> TestResult<TransferOutcome> {
        Ok(
            tokio::time::timeout(Duration::from_secs(10), transfer(self.request(policy)?))
                .await??,
        )
    }

    fn assert_published(&self) -> TestResult {
        assert_eq!(
            std::fs::read(self.destination_root.path().join(PATH))?,
            self.expected
        );
        let names: Vec<_> = std::fs::read_dir(self.destination_root.path())?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<Result<_, _>>()?;
        assert_eq!(
            names,
            [std::ffi::OsString::from(PATH)],
            "leftover transfer artifacts"
        );
        Ok(())
    }

    async fn expert_write(
        &self,
    ) -> TestResult<(ExpertDestinationTransferred, ExpertSourceEvidence)> {
        let descriptor = self
            .source
            .read_source()?
            .describe(&StoragePath::new(PATH)?)
            .await?;
        let observation = ObservedEntry::new(
            descriptor.path,
            descriptor.kind,
            descriptor.size,
            None,
            descriptor.source_identity,
        )?;
        let source = ExpertSourceSession::open(ExpertSourceRequest::new(
            self.source.clone(),
            observation.clone(),
            CancellationToken::new(),
        ))
        .await?;
        let destination = ExpertDestinationSession::prepare(ExpertDestinationRequest::new(
            observation,
            source.offer().maximum_chunk_bytes,
            self.destination.clone(),
            StoragePath::new(PATH)?,
            CancellationToken::new(),
        ))
        .await?;
        transport_expert(source, destination).await
    }
}

async fn transport_expert(
    source: ExpertSourceSession,
    destination: ExpertDestinationSession,
) -> TestResult<(ExpertDestinationTransferred, ExpertSourceEvidence)> {
    let mut payload = source.stream_from(destination.write_offset())?;
    let (mut sender, receiver) = futures::channel::mpsc::channel(1);
    let pump = async move {
        while let Some(chunk) = payload.next_chunk().await? {
            sender.send(Ok(chunk)).await?;
        }
        drop(sender);
        Ok::<_, Box<dyn std::error::Error>>(payload.finish().await?)
    };
    let (written, evidence) = tokio::time::timeout(Duration::from_secs(10), async {
        futures::join!(destination.write(Box::pin(receiver)), pump)
    })
    .await?;
    Ok((written?, evidence?))
}

fn source_storage(original: &Storage, source: Arc<dyn ReadSource>) -> TestResult<Storage> {
    let absent =
        CapabilityAvailability::Unsupported(UnsupportedReason::new("fixture role absent")?);
    Ok(Storage::connected(
        original.identity().clone(),
        BackendCapabilities::new(
            CapabilityAvailability::Supported,
            absent.clone(),
            absent.clone(),
            absent,
        ),
        Some(source),
        None,
        None,
        None,
        None,
    )?)
}

fn destination_storage(
    original: &Storage,
    destination: Arc<dyn StagedDestination>,
) -> TestResult<Storage> {
    let absent =
        CapabilityAvailability::Unsupported(UnsupportedReason::new("fixture role absent")?);
    Ok(Storage::connected(
        original.identity().clone(),
        BackendCapabilities::new(
            absent.clone(),
            CapabilityAvailability::Supported,
            absent.clone(),
            absent,
        ),
        None,
        Some(destination),
        None,
        None,
        None,
    )?)
}

fn assert_original_ordered_path(calls: &Calls) {
    let calls = calls.snapshot();
    assert!(
        calls
            .iter()
            .any(|call| matches!(call, Call::OrderedRead(_))),
        "{calls:?}"
    );
    assert!(calls.contains(&Call::OrderedWrite), "{calls:?}");
    for forbidden in [
        Call::PositionedRead,
        Call::PositionedWrite,
        Call::CheckpointedWrite,
        Call::PersistHash,
        Call::ReleaseHash,
        Call::ArmedOnOrderedPath,
    ] {
        assert!(
            !calls.contains(&forbidden),
            "unexpected {forbidden:?}: {calls:?}"
        );
    }
}

#[tokio::test]
async fn atomic_replace_keeps_ordered_delivery_without_hash_metadata() -> TestResult {
    let fixture = Fixture::new(4 * CHUNK + 17, INTERVAL, true)?;
    let outcome = fixture.run(TransferPolicy::AtomicReplace).await?;
    assert_eq!(outcome.recovery, EffectiveRecovery::Disabled);
    assert_original_ordered_path(&fixture.calls);
    fixture.assert_published()
}

#[cfg(unix)]
#[tokio::test]
async fn direct_keeps_ordered_delivery_without_hash_metadata() -> TestResult {
    let fixture = Fixture::new(4 * CHUNK + 17, INTERVAL, true)?;
    let outcome = fixture.run(TransferPolicy::Direct).await?;
    assert_eq!(outcome.recovery, EffectiveRecovery::Disabled);
    assert_original_ordered_path(&fixture.calls);
    fixture.assert_published()
}

#[tokio::test]
async fn one_source_chunk_never_arms_hash_checkpoints_even_above_interval() -> TestResult {
    let fixture = Fixture::new(CHUNK, (CHUNK / 2) as u64, true)?;
    let outcome = fixture.run(TransferPolicy::Checkpointed).await?;
    assert_eq!(
        outcome.recovery,
        EffectiveRecovery::SkippedSingleSourceChunk
    );
    assert_original_ordered_path(&fixture.calls);
    fixture.assert_published()
}

#[tokio::test]
async fn below_or_exactly_one_interval_preserves_the_ordered_path() -> TestResult {
    for size in [CHUNK + 1, 2 * CHUNK - 1, 2 * CHUNK] {
        let fixture = Fixture::new(size, INTERVAL, true)?;
        let outcome = fixture.run(TransferPolicy::Checkpointed).await?;
        assert_eq!(
            outcome.recovery,
            EffectiveRecovery::SkippedBelowCheckpointThreshold
        );
        assert_original_ordered_path(&fixture.calls);
        fixture.assert_published()?;
    }
    Ok(())
}

#[tokio::test]
async fn a_nonresumable_destination_never_arms_hash_checkpoints() -> TestResult {
    let fixture = Fixture::new(4 * CHUNK + 17, INTERVAL, false)?;
    let outcome = fixture.run(TransferPolicy::Checkpointed).await?;
    assert_eq!(
        outcome.recovery,
        EffectiveRecovery::UnsupportedByDestination
    );
    assert_original_ordered_path(&fixture.calls);
    fixture.assert_published()
}

#[tokio::test]
async fn eligible_checkpointed_copy_uses_positioned_delivery_and_releases_hash_state() -> TestResult
{
    let fixture = Fixture::new(2 * CHUNK + 1, INTERVAL, true)?;
    let outcome = fixture.run(TransferPolicy::Checkpointed).await?;
    assert_eq!(outcome.recovery, EffectiveRecovery::Checkpointed);
    let calls = fixture.calls.snapshot();
    assert!(calls.contains(&Call::PositionedRead), "{calls:?}");
    assert!(calls.contains(&Call::CheckpointedWrite), "{calls:?}");
    assert!(calls.contains(&Call::PersistHash), "{calls:?}");
    assert!(!calls.contains(&Call::OrderedWrite), "{calls:?}");
    assert!(
        !calls
            .iter()
            .any(|call| matches!(call, Call::OrderedRead(_))),
        "{calls:?}"
    );
    assert_eq!(
        calls
            .iter()
            .filter(|call| **call == Call::ReleaseHash)
            .count(),
        1,
        "{calls:?}"
    );
    let released = calls
        .iter()
        .position(|call| *call == Call::ReleaseHash)
        .ok_or("no release")?;
    for completed in [Call::Verify, Call::Publish] {
        let at = calls
            .iter()
            .position(|call| *call == completed)
            .ok_or("missing completion")?;
        assert!(at < released, "released before {completed:?}: {calls:?}");
    }
    assert_eq!(
        outcome.blake3,
        Some(*blake3::hash(&fixture.expected).as_bytes())
    );
    fixture.assert_published()
}

#[tokio::test]
async fn disabling_readback_does_not_disable_eligible_source_hash_recovery() -> TestResult {
    let fixture = Fixture::new(4 * CHUNK + 17, INTERVAL, true)?;
    let request = fixture
        .request(TransferPolicy::Checkpointed)?
        .with_read_back_verification(ReadBackVerification::Disabled);
    let outcome = tokio::time::timeout(Duration::from_secs(10), transfer(request)).await??;
    let calls = fixture.calls.snapshot();
    assert!(calls.contains(&Call::PositionedRead), "{calls:?}");
    assert!(calls.contains(&Call::CheckpointedWrite), "{calls:?}");
    assert!(calls.contains(&Call::PersistHash), "{calls:?}");
    assert!(calls.contains(&Call::ReleaseHash), "{calls:?}");
    assert!(!calls.contains(&Call::Verify), "{calls:?}");
    assert_eq!(outcome.blake3, None);
    assert_eq!(outcome.read_back, ReadBackVerification::Disabled);
    fixture.assert_published()
}

#[tokio::test]
async fn expert_checkpointed_copy_preserves_the_ordered_transport() -> TestResult {
    let fixture = Fixture::new(4 * CHUNK + 17, INTERVAL, true)?;
    let (written, evidence) = fixture.expert_write().await?;
    let outcome = written.complete(evidence).await?;
    assert_eq!(
        outcome.blake3,
        Some(*blake3::hash(&fixture.expected).as_bytes())
    );
    assert_original_ordered_path(&fixture.calls);
    fixture.assert_published()
}

#[tokio::test]
async fn expert_resume_preserves_its_full_ordered_source_hash_pass() -> TestResult {
    let fixture = Fixture::new(4 * CHUNK + 17, INTERVAL, true)?;
    let (written, _) = fixture.expert_write().await?;
    drop(written); // process exit after writes, before verification/publication
    fixture.calls.clear();
    let (written, evidence) = fixture.expert_write().await?;
    let outcome = written.complete(evidence).await?;
    assert_eq!(
        outcome.prepare,
        PrepareFact::Resumed {
            bytes: fixture.expected.len() as u64
        }
    );
    assert_eq!(outcome.reused_bytes, fixture.expected.len() as u64);
    assert_original_ordered_path(&fixture.calls);
    assert!(
        fixture
            .calls
            .snapshot()
            .contains(&Call::OrderedRead(Some(0..fixture.expected.len() as u64)))
    );
    fixture.assert_published()
}

#[tokio::test]
async fn failing_to_persist_ready_never_publishes_the_stage() -> TestResult {
    let fixture = Fixture::new(4 * CHUNK + 17, INTERVAL, true)?;
    fixture.faults.ready.store(true, Ordering::SeqCst);
    let failure = transfer(fixture.request(TransferPolicy::Checkpointed)?)
        .await
        .err()
        .ok_or("expected ready failure")?;
    assert_eq!(failure.phase(), TransferPhase::Publish);
    assert!(failure.has_unpublished_stage());
    assert!(!failure.final_destination_changed());
    let calls = fixture.calls.snapshot();
    assert!(
        calls.contains(&Call::HashPhase(CheckpointPhase::ReadyToPublish)),
        "{calls:?}"
    );
    assert!(!calls.contains(&Call::Publish), "{calls:?}");
    assert!(!calls.contains(&Call::ReleaseHash), "{calls:?}");
    assert!(!fixture.destination_root.path().join(PATH).exists());
    failure.discard_stage().await?;
    Ok(())
}

#[tokio::test]
async fn postpublication_persistence_and_release_failures_keep_cleanup() -> TestResult {
    for fail_record in [false, true] {
        let fixture = Fixture::new(4 * CHUNK + 17, INTERVAL, true)?;
        fixture.faults.release.store(!fail_record, Ordering::SeqCst);
        fixture
            .faults
            .published_record
            .store(fail_record, Ordering::SeqCst);
        let failure = transfer(fixture.request(TransferPolicy::Checkpointed)?)
            .await
            .err()
            .ok_or("expected postpublication failure")?;
        assert_eq!(failure.phase(), TransferPhase::Publish);
        assert!(failure.final_destination_changed());
        assert!(failure.has_pending_cleanup());
        assert!(!failure.has_unpublished_stage());
        let calls = fixture.calls.snapshot();
        assert!(calls.contains(&Call::Publish), "{calls:?}");
        assert_eq!(
            calls.contains(&Call::ReleaseHash),
            !fail_record,
            "{calls:?}"
        );
        if fail_record {
            assert!(
                calls.contains(&Call::HashPhase(CheckpointPhase::Published)),
                "{calls:?}"
            );
        }
        fixture.faults.release.store(false, Ordering::SeqCst);
        fixture
            .faults
            .published_record
            .store(false, Ordering::SeqCst);
        failure.cleanup_published_stage().await?;
        fixture.assert_published()?;
    }
    Ok(())
}

async fn leave_unverified_final(fixture: &Fixture) -> TestResult {
    fixture.faults.verify_after.store(true, Ordering::SeqCst);
    fixture
        .faults
        .published_verify
        .store(true, Ordering::SeqCst);
    let failure = transfer(fixture.request(TransferPolicy::Checkpointed)?)
        .await
        .err()
        .ok_or("expected published verification failure")?;
    assert!(failure.final_destination_changed());
    assert!(failure.has_pending_cleanup());
    drop(failure);
    fixture.calls.clear();
    Ok(())
}

#[tokio::test]
async fn recovered_final_is_verified_even_when_readback_is_disabled() -> TestResult {
    let fixture = Fixture::new(4 * CHUNK + 17, INTERVAL, true)?;
    leave_unverified_final(&fixture).await?;
    let request = fixture
        .request(TransferPolicy::Checkpointed)?
        .with_read_back_verification(ReadBackVerification::Disabled);
    let failure = transfer(request.clone())
        .await
        .err()
        .ok_or("recovery must verify the final")?;
    assert_eq!(failure.phase(), TransferPhase::Verify);
    assert!(failure.has_pending_cleanup());
    assert!(failure.final_destination_changed());
    let calls = fixture.calls.snapshot();
    assert!(calls.contains(&Call::VerifyPublished), "{calls:?}");
    assert!(
        !calls.iter().any(|call| matches!(
            call,
            Call::OrderedRead(_) | Call::PositionedRead | Call::Publish | Call::ReleaseHash
        )),
        "{calls:?}"
    );
    drop(failure);
    fixture.calls.clear();
    fixture
        .faults
        .published_verify
        .store(false, Ordering::SeqCst);
    let outcome = transfer(request).await?;
    assert_eq!(outcome.blake3, None);
    assert_eq!(outcome.read_back, ReadBackVerification::Disabled);
    let calls = fixture.calls.snapshot();
    assert_eq!(
        calls
            .iter()
            .filter(|call| **call == Call::VerifyPublished)
            .count(),
        1,
        "{calls:?}"
    );
    assert!(
        !calls.iter().any(|call| matches!(
            call,
            Call::OrderedRead(_) | Call::PositionedRead | Call::Publish
        )),
        "{calls:?}"
    );
    assert!(calls.contains(&Call::ReleaseHash), "{calls:?}");
    fixture.assert_published()
}
