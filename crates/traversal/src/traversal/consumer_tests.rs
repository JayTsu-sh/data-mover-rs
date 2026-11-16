use crate::{AssertTestError, AssertTestValue};

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::FutureExt;
use tokio::sync::mpsc;

use super::*;
use crate::model::{EntryKind, IdentityStrength, SourceIdentity, StoragePath};
use crate::storage::backends::local::test_identity;
use crate::traversal::{
    ObservationFailure, ScanTotals, TraversalCompletion, TraversalOutcome, TraversalRequest,
    TraversalSession, TraversalSource,
};

fn diagnostic() -> TraversalItem {
    TraversalItem::ObservationFailure(Box::new(ObservationFailure {
        path: StoragePath::new("known").assert_value("diagnostic fixture path"),
        family: crate::model::MetadataFamily::Acl,
        version: crate::model::SourceVersion::Current,
        class: crate::model::FailureClass::PermissionDenied,
        transience: crate::model::Transience::Permanent,
    }))
}

#[tokio::test]
async fn diagnostic_normal_delivery_and_cancel_fallback_are_exclusive_with_full_output() {
    for normal_first in [false, true] {
        let (mut session, state, sender, completion) = pair(1, 1, 1);
        let item = diagnostic();
        state.record_observation(vec![item.clone()], None);
        sender
            .send_one(item.clone())
            .await
            .assert_value("capacity one diagnostic output");
        let mut feedback = Vec::new();
        if normal_first {
            assert_eq!(session.next_item(&mut feedback).await, Some(item.clone()));
        }
        // A producer already waiting to forward its copy cannot publish it after the cutoff.
        let mut late = Box::pin(sender.send_one(item.clone()));
        assert!(futures::poll!(&mut late).is_pending());
        completed(completion);
        session.cancel.cancel();
        let expected = (!normal_first).then_some(item.clone());
        assert_eq!(session.next_item(&mut feedback).await, expected);
        assert!(late.await.is_err());
        state.record_observation(vec![item], Some(TraversalTerminalFailure::Internal));
        assert!(session.next_item(&mut feedback).await.is_none());
        assert!(state.ledger.lock().diagnostics.is_empty());
        assert!(
            state.failure().is_none(),
            "late producer cannot change a sealed terminal"
        );
        assert_eq!(session.finish().await, Ok(TraversalOutcome::Cancelled));
    }
}

#[tokio::test]
async fn producer_drop_drains_registered_diagnostic_before_internal_terminal() {
    let (mut session, state, sender, completion) = pair(1, 1, 1);
    let item = diagnostic();
    state.record_observation(vec![item.clone()], None);
    drop(sender);
    drop(completion);
    let mut feedback = Vec::new();
    assert_eq!(session.next_item(&mut feedback).await, Some(item));
    assert!(session.next_item(&mut feedback).await.is_none());
    assert_eq!(
        session.finish().await,
        Err(TraversalTerminalFailure::Internal)
    );
    assert!(state.ledger.lock().diagnostics.is_empty());
}

type Completion = oneshot::Sender<Result<TraversalOutcome, TraversalTerminalFailure>>;

fn request(window: usize) -> TraversalRequest {
    let mut request = TraversalRequest::new(StoragePath::root());
    request.consumer_completion_budget = Some(NonZeroUsize::MIN);
    request.consumer_max_inflight =
        NonZeroUsize::new(window).assert_value("consumer contract fixture");
    request
}

fn pair(
    window: usize,
    output: usize,
    execution: u64,
) -> (
    TraversalSession,
    Arc<ConsumerState>,
    channel::ItemSender,
    Completion,
) {
    let (sender, receiver) =
        channel::channel(NonZeroUsize::new(output).assert_value("consumer contract fixture"));
    let (completion, result) = oneshot::channel();
    let (session, consumer) =
        TraversalSession::controlled(receiver, result, &request(window), execution);
    (session, consumer, sender, completion)
}

fn entry(index: usize) -> Box<ObservedEntry> {
    let name = format!("file-{index}");
    Box::new(
        ObservedEntry::new(
            StoragePath::new(name.clone()).assert_value("consumer contract fixture"),
            EntryKind::File,
            Some(1),
            None,
            SourceIdentity::new(
                test_identity("consumer-mock"),
                IdentityStrength::StableWithinBackend,
                name.as_bytes(),
            )
            .assert_value("consumer contract fixture"),
        )
        .assert_value("consumer contract fixture"),
    )
}

fn id(item: TraversalItem) -> [u8; 32] {
    match item {
        TraversalItem::ConsumerEntry(work) | TraversalItem::SubtreeReady(work) => work.0,
        other => panic!("expected consumer phase: {other:?}"),
    }
}

fn completed(completion: Completion) {
    completion
        .send(Ok(TraversalOutcome::Completed(TraversalCompletion::new(
            ScanTotals::default(),
        ))))
        .assert_value("consumer contract fixture");
}

#[tokio::test]
async fn feedback_survives_unpolled_cancellation_and_commits_before_first_await() {
    let (mut session, state, sender, completion) = pair(1, 1, 1);
    let mut result = state
        .send(&sender, entry(0), false)
        .await
        .assert_value("consumer contract fixture");
    let mut feedback = Vec::new();
    let key = id(session
        .next_item(&mut feedback)
        .await
        .assert_value("consumer contract fixture"));
    feedback.push((key, true));
    drop(session.next_item(&mut feedback));
    assert_eq!(feedback, [(key, true)]);
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let mut future = Box::pin(session.next_item(&mut feedback));
    assert!(futures::poll!(&mut future).is_pending());
    drop(future);
    assert!(feedback.is_empty());
    assert!(state.ledger.lock().pending.is_empty());
    // End the pinned borrow without repolling; committed state is session-owned.
    drop(sender);
    completed(completion);
    assert_eq!(result.await, Ok(true));
}

#[tokio::test]
async fn full_output_cannot_delay_feedback_and_taken_output_has_no_later_await() {
    let (mut session, state, sender, completion) = pair(4, 1, 1);
    let mut feedback = Vec::new();
    let first = state
        .send(&sender, entry(0), false)
        .await
        .assert_value("consumer contract fixture");
    let first_id = id(session
        .next_item(&mut feedback)
        .await
        .assert_value("consumer contract fixture"));
    assert!(session.next_item(&mut feedback).now_or_never().is_none()); // Return held batch room.
    let second = state
        .send(&sender, entry(1), false)
        .await
        .assert_value("consumer contract fixture");
    let blocked = state.send(&sender, entry(2), false);
    tokio::pin!(blocked);
    assert!(futures::poll!(&mut blocked).is_pending());
    feedback.push((first_id, true));
    let second_id = id(session
        .next_item(&mut feedback)
        .now_or_never()
        .assert_value("consumer contract fixture")
        .assert_value("consumer contract fixture"));
    assert!(feedback.is_empty());
    assert_eq!(first.await, Ok(true));
    assert_ne!(first_id, second_id);
    drop(second);
    completed(completion);
}

#[tokio::test]
async fn unissued_cross_session_phase_duplicate_and_late_feedback_cannot_settle_work() {
    let (mut session, state, sender, completion) = pair(4, 3, 7);
    let (mut other, other_state, other_sender, other_completion) = pair(1, 1, 7);
    let mut first = state
        .send(&sender, entry(0), false)
        .await
        .assert_value("consumer contract fixture");
    let mut final_result = state
        .send(&sender, entry(0), true)
        .await
        .assert_value("consumer contract fixture");
    let mut foreign = other_state
        .send(&other_sender, entry(0), false)
        .await
        .assert_value("consumer contract fixture");
    let mut feedback = Vec::new();
    let first_id = id(session
        .next_item(&mut feedback)
        .await
        .assert_value("consumer contract fixture"));
    let final_id = *state
        .ledger
        .lock()
        .pending
        .keys()
        .find(|key| **key != first_id)
        .assert_value("consumer contract fixture");
    let foreign_id = id(other
        .next_item(&mut feedback)
        .await
        .assert_value("consumer contract fixture"));
    assert_ne!(first_id, final_id);
    assert_ne!(first_id, foreign_id);
    feedback.extend([(final_id, true), (foreign_id, false), ([0; 32], false)]);
    assert_eq!(
        id(session
            .next_item(&mut feedback)
            .await
            .assert_value("consumer contract fixture")),
        final_id
    );
    assert!(matches!(
        final_result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        foreign.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    feedback.extend([(first_id, true), (first_id, false)]);
    assert!(session.next_item(&mut feedback).now_or_never().is_none());
    assert_eq!(first.try_recv(), Ok(true));
    assert!(state.failure().is_none());
    feedback.extend([(first_id, false), (final_id, true)]);
    assert!(session.next_item(&mut feedback).now_or_never().is_none());
    assert_eq!(final_result.await, Ok(true));
    assert!(state.failure().is_none());
    drop(sender);
    completed(completion);
    assert!(session.next_item(&mut feedback).await.is_none());
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Completed(_))
    ));
    drop(other);
    drop(other_sender);
    drop(other_completion);
    assert!(foreign.await.is_err());
}

#[tokio::test]
async fn producer_disappearing_with_unsettled_work_ends_without_waiting_for_feedback() {
    let (mut session, state, sender, completion) = pair(1, 1, 1);
    let result = state
        .send(&sender, entry(0), false)
        .await
        .assert_value("consumer contract fixture");
    let mut feedback = Vec::new();
    assert!(session.next_item(&mut feedback).await.is_some());
    drop(sender);
    drop(completion);
    let eof = tokio::time::timeout(Duration::from_secs(3), session.next_item(&mut feedback)).await;
    assert!(
        eof.is_ok(),
        "producer disappeared, but EOF still waits for consumer ACK"
    );
    assert!(eof.assert_value("consumer contract fixture").is_none());
    assert_eq!(
        session.finish().await,
        Err(TraversalTerminalFailure::Internal)
    );
    assert!(result.await.is_err());
}

#[tokio::test]
async fn producer_panic_and_typed_error_release_pending_without_feedback() {
    for panic in [false, true] {
        let (mut session, state, sender, completion) = pair(1, 1, 1);
        let task = tokio::spawn(async move {
            let completion_guard = completion;
            let _result = state
                .send(&sender, entry(0), false)
                .await
                .assert_value("consumer contract fixture");
            assert!(!panic, "deliberate controlled producer panic");
            completion_guard
                .send(Err(
                    TraversalTerminalFailure::ConsumerCompletionBudgetExceeded {
                        limit: 1,
                        required: 2,
                    },
                ))
                .assert_value("consumer contract fixture");
        });
        let mut feedback = Vec::new();
        assert!(session.next_item(&mut feedback).await.is_some());
        let joined = task.await;
        if panic {
            assert!(
                joined
                    .assert_error("expected controlled producer panic")
                    .is_panic()
            );
        } else {
            joined.assert_value("consumer contract fixture");
        }
        assert!(
            tokio::time::timeout(Duration::from_secs(3), session.next_item(&mut feedback))
                .await
                .assert_value("consumer contract fixture")
                .is_none()
        );
        let expected = if panic {
            TraversalTerminalFailure::Internal
        } else {
            TraversalTerminalFailure::ConsumerCompletionBudgetExceeded {
                limit: 1,
                required: 2,
            }
        };
        assert_eq!(session.finish().await, Err(expected));
    }
}

#[tokio::test]
async fn eof_before_success_evidence_waits_and_can_flush_feedback_or_cancel() {
    for cancel in [false, true] {
        let (mut session, state, sender, completion) = pair(1, 1, 1);
        let result = state
            .send(&sender, entry(0), false)
            .await
            .assert_value("consumer contract fixture");
        let mut feedback = Vec::new();
        let key = id(session
            .next_item(&mut feedback)
            .await
            .assert_value("consumer contract fixture"));
        drop(sender);
        assert!(
            session.next_item(&mut feedback).now_or_never().is_none(),
            "Pending terminal evidence is not producer failure"
        );
        completed(completion);
        assert!(
            session.next_item(&mut feedback).now_or_never().is_none(),
            "success evidence still requires consumer settlement"
        );
        if cancel {
            session.cancel.cancel();
        } else {
            feedback.push((key, true));
        }
        assert!(session.next_item(&mut feedback).await.is_none());
        if cancel {
            assert_eq!(session.finish().await, Ok(TraversalOutcome::Cancelled));
            assert!(result.await.is_err());
        } else {
            assert!(matches!(
                session.finish().await,
                Ok(TraversalOutcome::Completed(_))
            ));
            assert_eq!(result.await, Ok(true));
        }
    }
}

#[tokio::test]
async fn source_fatal_at_eof_preserves_typed_cause_and_releases_unsettled_work() {
    let (mut session, state, sender, completion) = pair(1, 1, 1);
    let result = state
        .send(&sender, entry(0), false)
        .await
        .assert_value("consumer contract fixture");
    let mut feedback = Vec::new();
    assert!(session.next_item(&mut feedback).await.is_some());
    let failure = TraversalTerminalFailure::ConsumerSource(Box::new(
        crate::model::EntryOperationFailure::new(
            StoragePath::root(),
            crate::model::Operation::Traverse,
            crate::model::FailureClass::PermissionDenied,
            crate::model::Transience::Permanent,
            "source failed before derived cancel",
        )
        .assert_value("consumer contract fixture"),
    ));
    state.record_failure(failure.clone());
    drop(sender);
    assert!(
        session
            .next_item(&mut feedback)
            .now_or_never()
            .assert_value("consumer contract fixture")
            .is_none()
    );
    session.cancel.cancel();
    assert_eq!(session.finish().await, Err(failure));
    assert!(result.await.is_err());
    drop(completion);
}

#[tokio::test]
async fn eof_cannot_succeed_with_unsettled_work_and_false_is_terminal() {
    for success in [true, false] {
        let (mut session, state, sender, completion) = pair(1, 1, 1);
        let result = state
            .send(&sender, entry(0), false)
            .await
            .assert_value("consumer contract fixture");
        let mut feedback = Vec::new();
        let key = id(session
            .next_item(&mut feedback)
            .await
            .assert_value("consumer contract fixture"));
        drop(sender);
        completed(completion);
        assert!(session.next_item(&mut feedback).now_or_never().is_none());
        feedback.push((key, success));
        assert!(session.next_item(&mut feedback).await.is_none());
        assert_eq!(result.await, Ok(success));
        match session.finish().await {
            Ok(TraversalOutcome::Completed(_)) => assert!(success),
            Err(TraversalTerminalFailure::ConsumerFailed) => assert!(!success),
            other => panic!("unexpected completion: {other:?}"),
        }
    }
}

#[tokio::test]
async fn oversized_feedback_fails_before_processing_any_confirmation() {
    let (mut session, state, sender, completion) = pair(1, 1, 1);
    let result = state
        .send(&sender, entry(0), false)
        .await
        .assert_value("consumer contract fixture");
    let mut feedback = Vec::new();
    let key = id(session
        .next_item(&mut feedback)
        .await
        .assert_value("consumer contract fixture"));
    feedback.extend([(key, true), (key, true)]);
    assert!(session.next_item(&mut feedback).await.is_none());
    assert!(feedback.is_empty());
    assert!(result.await.is_err());
    assert_eq!(
        session.finish().await,
        Err(TraversalTerminalFailure::ConsumerFeedbackLimitExceeded { limit: 1 })
    );
    drop(completion);
}

#[tokio::test]
async fn close_and_cancel_release_window_output_and_ack_waiters() {
    for cancel in [false, true] {
        let (mut session, state, sender, completion) = pair(1, 1, 1);
        let result = state
            .send(&sender, entry(0), false)
            .await
            .assert_value("consumer contract fixture");
        let blocked = state.send(&sender, entry(1), false);
        tokio::pin!(blocked);
        assert!(futures::poll!(&mut blocked).is_pending());
        if cancel {
            session.cancel.cancel();
            assert!(session.next_item(&mut Vec::new()).await.is_none());
            completed(completion);
            assert_eq!(session.finish().await, Ok(TraversalOutcome::Cancelled));
        } else {
            drop(session);
            drop(completion);
        }
        assert!(blocked.await.is_err());
        assert!(result.await.is_err());
        assert!(state.ledger.lock().pending.is_empty());
    }
}

struct SourceMock {
    work: usize,
    state: Mutex<Option<Arc<ConsumerState>>>,
}

impl TraversalSource for SourceMock {
    fn traverse(&self, request: TraversalRequest) -> TraversalSession {
        let (sender, receiver) = channel::channel(NonZeroUsize::MIN);
        let (completion, result) = oneshot::channel();
        let (session, state) = TraversalSession::controlled(receiver, result, &request, 1);
        *self.state.lock() = Some(state.clone());
        let work = self.work;
        tokio::spawn(async move {
            for index in 0..work {
                if state.send(&sender, entry(index), false).await.is_err() {
                    return;
                }
            }
            drop(sender);
            completed(completion);
        });
        session
    }
}

#[tokio::test]
async fn source_mock_window_128_workers_16_output_and_results_one_keep_progressing() {
    tokio::time::timeout(Duration::from_secs(10), mock_pipeline())
        .await
        .assert_value("consumer contract fixture");
}

async fn mock_pipeline() {
    const WINDOW: usize = 128;
    const WORK: usize = 4096;
    let source = SourceMock {
        work: WORK,
        state: Mutex::new(None),
    };
    let mut session = source.traverse(request(WINDOW));
    let state = source
        .state
        .lock()
        .clone()
        .assert_value("consumer contract fixture");
    let (work_tx, work_rx) = mpsc::channel::<[u8; 32]>(1);
    let work_rx = Arc::new(tokio::sync::Mutex::new(work_rx));
    let (results_tx, mut results_rx) = mpsc::channel(1);
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let release = CancellationToken::new();
    let started = Arc::new(Notify::new());
    let mut workers = Vec::new();
    for _ in 0..16 {
        let (queue, results, active, peak, release) = (
            work_rx.clone(),
            results_tx.clone(),
            active.clone(),
            peak.clone(),
            release.clone(),
        );
        let started = started.clone();
        workers.push(tokio::spawn(async move {
            loop {
                let Some(key) = queue.lock().await.recv().await else {
                    break;
                };
                let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(count, Ordering::SeqCst);
                started.notify_one();
                release.cancelled().await;
                active.fetch_sub(1, Ordering::SeqCst);
                if results.send((key, true)).await.is_err() {
                    break;
                }
            }
        }));
    }
    drop(results_tx);
    let mut feedback = Vec::new();
    let mut pending = VecDeque::new();
    let mut acknowledged = 0;
    let mut reached_window = false;
    loop {
        let unsettled = state.ledger.lock().pending.len();
        assert!(unsettled <= WINDOW);
        assert!(pending.len() <= WINDOW);
        if unsettled == WINDOW && active.load(Ordering::SeqCst) == 16 {
            reached_window = true;
            release.cancel();
        }
        let receive = pending.len() < WINDOW || !feedback.is_empty();
        tokio::select! {
            biased;
            item = session.next_item(&mut feedback), if receive => match item {
                Some(item) => pending.push_back(id(item)),
                None => break,
            },
            result = results_rx.recv() => {
                feedback.push(result.assert_value("consumer contract fixture")); acknowledged += 1;
            },
            () = started.notified() => {},
            permit = work_tx.reserve(), if !pending.is_empty() => {
                permit.assert_value("consumer contract fixture").send(pending.pop_front().assert_value("consumer contract fixture"));
            },
        }
    }
    assert!(reached_window);
    assert_eq!(acknowledged, WORK);
    assert_eq!(peak.load(Ordering::SeqCst), 16);
    assert!(pending.is_empty());
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Completed(_))
    ));
    drop(work_tx);
    for task in workers {
        task.await.assert_value("consumer contract fixture");
    }
}

#[tokio::test]
async fn actor_failure_notification_cannot_steal_session_settlement_wakeup() {
    let (mut session, state, sender, _completion) = pair(1, 1, 1);
    let receipt = state
        .send(&sender, entry(0), false)
        .await
        .assert_value("issued work");
    let mut feedback = Vec::new();
    let work = id(session
        .next_item(&mut feedback)
        .await
        .assert_value("issued work"));
    let mut settled = Box::pin(state.settled());
    let mut failed = Box::pin(state.failed());
    assert!(futures::poll!(&mut settled).is_pending());
    assert!(futures::poll!(&mut failed).is_pending());
    feedback.push((work, true));
    state.submit(&mut feedback);
    assert!(receipt.await.assert_value("receipt"));
    assert!(futures::poll!(&mut settled).is_ready());
    assert!(futures::poll!(&mut failed).is_pending());
    state.record_failure(TraversalTerminalFailure::Internal);
    assert!(futures::poll!(&mut failed).is_ready());
    // A later actor waiter observes the cached cause without requiring another notification.
    assert!(state.failed().now_or_never().is_some());
}
