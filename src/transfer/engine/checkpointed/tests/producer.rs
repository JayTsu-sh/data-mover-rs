use super::*;

async fn run(
    source: &mocks::Source,
    destination: &mocks::Destination,
    stage: &PreparedStage,
) -> Result<TransferEvidence, TransferFailure> {
    let cancel = CancellationToken::new();
    let context = context(source, destination, stage, &cancel);
    let recovered = state::initialize(&context)?;
    let (sender, receiver) = channel::channel(cancel.clone(), source.descriptor.path.clone());
    let (produced, written) = tokio::join!(
        super::super::producer::produce(&context, recovered, Some(sender)),
        async {
            destination
                .write_checkpointed(stage, receiver)
                .await
                .map_err(|error| {
                    TransferFailure::role(TransferPhase::Transfer, TransferSide::Destination, error)
                })
        }
    );
    let (digest, write) = settle(produced, written)?;
    state::finish(&context, write, digest).await
}

#[tokio::test]
async fn positioned_and_ordered_resume_read_only_missing_d_and_hash_only_missing_h() -> TestResult {
    for positioned in [true, false] {
        for intent in [true, false] {
            let (mut source, mut destination, mut stage) = fixture(73);
            source.positioned = positioned;
            destination.intent = intent;
            stage.write_offset = 17;
            restore(&stage, &source, &[0..40, 49..57], vec![0..17, 49..57]);
            let evidence = run(&source, &destination, &stage).await?;
            assert_eq!(
                evidence.source_blake3,
                Some(*blake3::hash(&source.data).as_bytes())
            );
            for (actual, request) in source.reads.lock().unwrap().iter() {
                assert_eq!(*actual, positioned);
                let range = request.range.clone().unwrap();
                assert!(range.start >= 17 && (range.end <= 49 || range.start >= 57));
                assert!(range.end - range.start <= 8);
                assert_eq!(
                    request.expected_source.as_ref(),
                    Some(&source.descriptor.source_identity)
                );
                assert_eq!(request.version, source.descriptor.version);
            }
            let snapshots = destination.snapshots.lock().unwrap();
            assert_eq!(snapshots.last().unwrap().ranges, vec![0..73]);
            assert_eq!(snapshots.last().unwrap().phase, CheckpointPhase::Receiving);
            if !intent {
                assert_eq!(snapshots.len(), 1);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn final_persistence_enables_deferred_recovery_before_observation() -> TestResult {
    let (source, mut destination, stage) = fixture(73);
    destination.require_persist_to_observe = true;
    stage
        .recovery_enabled
        .store(false, std::sync::atomic::Ordering::Release);
    let evidence = run(&source, &destination, &stage).await?;
    assert_eq!(evidence.checkpoint.durable_prefix, 73);
    assert!(stage.recovery_enabled());
    let snapshots = destination.snapshots.lock().unwrap();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].ranges, vec![0..73]);
    assert_eq!(snapshots[0].phase, CheckpointPhase::Receiving);
    assert_eq!(
        RangeHash::decode(&snapshots[0].hash, 73)?.finalize()?,
        *blake3::hash(&source.data).as_bytes()
    );
    Ok(())
}

#[tokio::test]
async fn failed_final_persistence_does_not_attempt_checkpoint_observation() {
    let (source, mut destination, stage) = fixture(73);
    destination.require_persist_to_observe = true;
    destination.anchor = mocks::AnchorBehavior::Refuse;
    stage
        .recovery_enabled
        .store(false, std::sync::atomic::Ordering::Release);
    assert!(run(&source, &destination, &stage).await.is_err());
    assert_eq!(
        destination
            .observations
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert!(!stage.recovery_enabled());
    assert!(destination.snapshots.lock().unwrap().is_empty());
}

#[tokio::test]
async fn malformed_source_completions_never_succeed_or_hang() {
    for fault in [
        mocks::Fault::Short,
        mocks::Fault::Duplicate,
        mocks::Fault::Empty,
        mocks::Fault::Outside,
        mocks::Fault::Oversized,
        mocks::Fault::Error,
    ] {
        for intent in [false, true] {
            let (mut source, mut destination, stage) = fixture(24);
            source.fault = fault;
            destination.intent = intent;
            assert!(
                tokio::time::timeout(Duration::from_secs(2), run(&source, &destination, &stage))
                    .await
                    .unwrap()
                    .is_err()
            );
        }
    }
}

#[tokio::test]
async fn non_intent_completion_is_released_before_the_window_finishes() -> TestResult {
    let (mut source, destination, stage) = fixture(24);
    source.fault = mocks::Fault::PendingAfterFirst;
    let cancel = CancellationToken::new();
    let context = context(&source, &destination, &stage, &cancel);
    let recovered = state::initialize(&context)?;
    let (sender, mut receiver) = channel::channel(cancel.clone(), source.descriptor.path.clone());
    let produced = super::super::producer::produce(&context, recovered, Some(sender));
    let consume = async {
        let chunk = tokio::time::timeout(Duration::from_secs(2), receiver.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(chunk.offset, 4);
        assert!(destination.snapshots.lock().unwrap().is_empty());
        drop(receiver);
    };
    let (result, ()) = tokio::join!(produced, consume);
    assert!(result.is_err());
    Ok(())
}

#[tokio::test]
async fn failed_hash_anchor_exposes_no_adoptable_payload() {
    let (source, mut destination, stage) = fixture(24);
    destination.intent = true;
    destination.anchor = mocks::AnchorBehavior::Refuse;
    assert!(run(&source, &destination, &stage).await.is_err());
    assert!(destination.chunks.lock().unwrap().is_empty());
}

#[tokio::test]
async fn closed_receiver_interrupts_pending_source_open_and_hash_anchor() {
    for pending_anchor in [false, true] {
        let (mut source, mut destination, stage) = fixture(24);
        source.fault = mocks::Fault::PendingOpen;
        destination.intent = pending_anchor;
        destination.anchor = if pending_anchor {
            mocks::AnchorBehavior::Pending
        } else {
            mocks::AnchorBehavior::Succeed
        };
        let cancel = CancellationToken::new();
        let context = context(&source, &destination, &stage, &cancel);
        let recovered = state::initialize(&context).unwrap();
        let (sender, receiver) = channel::channel(cancel.clone(), source.descriptor.path.clone());
        drop(receiver);
        assert!(
            tokio::time::timeout(
                Duration::from_secs(2),
                super::super::producer::produce(&context, recovered, Some(sender))
            )
            .await
            .unwrap()
            .is_err()
        );
    }
}

#[tokio::test]
async fn hash_only_resumes_missing_h_without_writes_or_promoting_d() -> TestResult {
    let (source, destination, mut stage) = fixture(73);
    stage.write_offset = 17;
    restore(&stage, &source, &[0..40, 49..57], vec![0..17]);
    let cancel = CancellationToken::new();
    let context = context(&source, &destination, &stage, &cancel);
    let recovered = state::initialize(&context)?;
    let digest = super::super::producer::produce(&context, recovered, None).await?;
    assert_eq!(digest, *blake3::hash(&source.data).as_bytes());
    assert!(destination.chunks.lock().unwrap().is_empty());
    for (_, request) in source.reads.lock().unwrap().iter() {
        let range = request.range.clone().unwrap();
        assert!(range.start >= 40 && (range.end <= 49 || range.start >= 57));
    }
    for saved in destination.snapshots.lock().unwrap().iter() {
        assert_eq!(saved.ranges, vec![0..17]);
        assert_eq!(saved.phase, CheckpointPhase::Receiving);
    }
    Ok(())
}

#[test]
fn adopted_tail_without_h_fails_before_arming_or_mutating_the_record() {
    let (source, destination, mut stage) = fixture(73);
    restore(&stage, &source, &[0..17], vec![0..17]);
    let before = stage.checkpoint_snapshot().unwrap();
    stage.write_offset = 18;
    let cancel = CancellationToken::new();
    assert!(state::initialize(&context(&source, &destination, &stage, &cancel)).is_err());
    assert!(!stage.checkpoint_enabled());
    assert_eq!(stage.checkpoint_snapshot().unwrap(), before);
}

#[test]
fn outer_limit_includes_range_records_and_the_actual_envelope() {
    let limit = crate::storage::checkpoint::MAX_CHECKPOINT_BYTES;
    let overhead = CheckpointData::default().encode().len();
    assert!(state::capacity(limit - overhead - 160_000, 10_000).is_ok());
    assert!(state::capacity(limit - overhead - 160_000 + 1, 10_000).is_err());
    assert!(state::capacity(0, usize::MAX).is_err());
}
