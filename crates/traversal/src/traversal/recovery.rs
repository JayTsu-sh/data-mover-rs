//! Reconnect remote traversals within a caller-selected window and resume safe positions.

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;

use super::channel;
use super::scan::{RecoveryBudget, ScanFailureSignal};
use super::{
    Checkpoint, DEFAULT_MAX_BUFFERED_ITEMS, RemoteScanFailure, RemoteScanFailureKind, Resumable,
    ScanSetupError, StorageTraversalSource, TraversalItem, TraversalOutcome, TraversalProducer,
    TraversalRequest, TraversalResume, TraversalSession, TraversalTerminalFailure,
};
use crate::model::{BackendSessionFailure, FailureClass, Transience};
use crate::storage::{BackendConnectError, Storage, StorageRoleFailure};

mod attempt;
mod counters;
use counters::RecoveryCounters;
pub use counters::TraversalRecoveryCounters;
mod probe;
use probe::{Probe, ProbeContext};
#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[path = "../recovery_tests.rs"]
mod tests;

/// Opens a fresh connection with the same endpoint identity and listing semantics.
pub type TraversalReconnect =
    Arc<dyn Fn() -> BoxFuture<'static, Result<Storage, BackendConnectError>> + Send + Sync>;

/// Caller-selected native-operation and reconnect budgets for a remote traversal.
/// Ordinary traversals do not enable this policy implicitly.
#[derive(Clone, Copy, Debug)]
pub struct TraversalRecoveryPolicy {
    /// 从检查点继续（允许重放），或从根重新开始并先交付 Resumed。
    pub resume: TraversalRecoveryResume,
    /// A native operation deadline; `None` preserves native protocol deadlines.
    pub operation_timeout: Option<Duration>,
    /// Wall-clock budget from the first detected fault through reconnection and
    /// verification of the original failed read. Verified recovery ends this
    /// deadline before healthy checkpoint rebuilding or consumer waits continue.
    pub recovery_window: Duration,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TraversalRecoveryResume {
    #[default]
    Checkpoint,
    RestartRoot,
}

/// Recovery activity; only `Failed` represents exhausted network recovery.
#[derive(Clone, Debug)]
pub enum TraversalRecoveryState {
    /// The faulting operation/path succeeded, or scanning has not encountered a fault.
    Running { recoveries: u64 },
    /// Native failure is being drained or a fresh connection is being tried.
    Recovering {
        first_failure: RemoteScanFailure,
        attempt: u64,
    },
    /// Recovery exhausted its window or could not safely resume.
    Failed { failure: RemoteScanFailure },
}

/// Out-of-band recovery activity and the final typed network failure.
#[derive(Clone)]
pub struct TraversalRecoveryStatus {
    counters: Arc<RecoveryCounters>,
    state: watch::Receiver<TraversalRecoveryState>,
    failure: ScanFailureSignal,
}

impl TraversalRecoveryStatus {
    /// Producer-owned cumulative facts, independent of coalesced watch notifications.
    #[must_use]
    pub fn counters(&self) -> TraversalRecoveryCounters {
        self.counters.snapshot()
    }

    /// Observes recovery independently of traversal output backpressure.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<TraversalRecoveryState> {
        self.state.clone()
    }

    /// Publishes only the first fault of a recovery window that ultimately fails.
    #[must_use]
    pub fn failure_signal(&self) -> ScanFailureSignal {
        self.failure.clone()
    }
}

/// Traverses with fresh connections and checkpoint recovery after native network failures.
/// The caller explicitly selects a recovery window; no scan budget is applied by default.
///
/// Active directories may be replayed. Delivered items keep their order and complete history
/// groups; completion totals describe the final resumed traversal, as for ordinary resume.
/// Checkpoint mode preserves the caller's checkpoint visibility and cancellation draining.
/// `RestartRoot` disables checkpoints and stops immediately on cancellation; each reconnection
/// emits Resumed before the restarted root's output so consumers can discard attempt state.
///
/// # Errors
/// Rejects invalid operation deadlines or unavailable traversal roles before starting.
pub fn recoverable_traversal(
    initial: &Storage,
    request: TraversalRequest,
    policy: TraversalRecoveryPolicy,
    reconnect: TraversalReconnect,
) -> Result<(TraversalSession, TraversalRecoveryStatus), ScanSetupError> {
    let (source, signal) = ScanFailureSignal::source(initial, policy.operation_timeout)?;
    let failure = ScanFailureSignal::new(initial.kind(), policy.operation_timeout);
    let (state, receiver) = watch::channel(TraversalRecoveryState::Running { recoveries: 0 });
    let (items, output) = channel::channel(DEFAULT_MAX_BUFFERED_ITEMS);
    let (completion, result) = oneshot::channel();
    let session = TraversalSession::new(
        output,
        result,
        request.cancel.clone(),
        policy.resume == TraversalRecoveryResume::Checkpoint && request.resumable.every().is_some(),
    );
    let producer = TraversalProducer {
        items,
        completion: Some(completion),
    };
    let counters = Arc::new(RecoveryCounters::default());
    let status = TraversalRecoveryStatus {
        counters: counters.clone(),
        state: receiver,
        failure: failure.clone(),
    };
    let runner = Recovery {
        counters,
        request,
        policy,
        reconnect,
        producer,
        state,
        failure,
        window: None,
        checkpoint: None,
        recoveries: 0,
        attempt: 1,
    };
    tokio::spawn(runner.run(source, signal));
    Ok((session, status))
}

struct Window {
    first: RemoteScanFailure,
    budget: RecoveryBudget,
    probe: Probe,
}

struct Recovery {
    counters: Arc<RecoveryCounters>,
    request: TraversalRequest,
    policy: TraversalRecoveryPolicy,
    reconnect: TraversalReconnect,
    producer: TraversalProducer,
    state: watch::Sender<TraversalRecoveryState>,
    failure: ScanFailureSignal,
    window: Option<Window>,
    checkpoint: Option<Checkpoint>,
    recoveries: u64,
    attempt: u64,
}

type Outcome = Result<TraversalOutcome, TraversalTerminalFailure>;

impl Recovery {
    async fn run(mut self, mut source: StorageTraversalSource, mut signal: ScanFailureSignal) {
        let outcome = loop {
            let result = self.attempt(source, &signal).await;
            if self.request.cancel.is_cancelled() {
                break Ok(TraversalOutcome::Cancelled);
            }
            if !self.retryable(&result, &signal) {
                break result;
            }
            match self.reconnect_source().await {
                Some(next) => {
                    (source, signal) = next;
                    if !self.begin_reconnected_attempt().await {
                        break Ok(TraversalOutcome::Cancelled);
                    }
                }
                None if self.request.cancel.is_cancelled() => {
                    break Ok(TraversalOutcome::Cancelled);
                }
                None => break self.exhausted(),
            }
        };
        self.producer.finish(outcome);
    }

    async fn begin_reconnected_attempt(&mut self) -> bool {
        self.attempt = self.attempt.saturating_add(1);
        let marker = TraversalItem::Resumed(Box::new(TraversalResume {
            attempt: self.attempt,
            checkpoint: if self.policy.resume == TraversalRecoveryResume::Checkpoint {
                self.checkpoint
                    .clone()
                    .or_else(|| self.request.resume_from.clone())
            } else {
                None
            },
        }));
        tokio::select! {
            biased;
            () = self.request.cancel.cancelled() => false,
            result = self.producer.send(marker) => result.is_ok(),
        }
    }

    fn request_for_attempt(&self) -> TraversalRequest {
        let mut request = self.request.clone();
        request.cancel = self.request.cancel.child_token();
        if self.policy.resume == TraversalRecoveryResume::RestartRoot {
            request.resumable = Resumable::Off;
            request.resume_from = None;
            return request;
        }
        request.resumable = match request.resumable {
            Resumable::Off => Resumable::on(),
            value => value,
        };
        if let Some(saved) = &self.checkpoint {
            request.resume_from = Some(saved.clone());
        }
        request
    }

    fn recovered(&mut self) {
        if let Some(window) = self.window.take() {
            window.budget.complete();
        }
        self.recoveries = self.recoveries.saturating_add(1);
        self.state.send_replace(TraversalRecoveryState::Running {
            recoveries: self.recoveries,
        });
    }

    fn start_window(&mut self, signal: &ScanFailureSignal) {
        if self.window.is_none()
            && let Some(first) = signal.failure()
        {
            self.counters.fault();
            let started = signal.failure_detected_at().unwrap_or_else(Instant::now);
            let budget = RecoveryBudget::new(started + self.policy.recovery_window);
            let probe = ProbeContext {
                counters: self.counters.clone(),
                first: first.clone(),
                target: signal.failure_target(),
                budget: budget.clone(),
                operation_timeout: self.policy.operation_timeout,
                reconnect: self.reconnect.clone(),
                cancel: self.request.cancel.child_token(),
                state: self.state.clone(),
                failure: self.failure.clone(),
                recoveries: self.recoveries.saturating_add(1),
            }
            .spawn();
            self.window = Some(Window {
                first,
                budget,
                probe,
            });
        }
    }

    fn retryable(&mut self, outcome: &Outcome, signal: &ScanFailureSignal) -> bool {
        self.start_window(signal);
        if self.window.is_none() {
            return false;
        }
        matches!(outcome, Err(TraversalTerminalFailure::Session(failure))
            if matches!(failure.class(), FailureClass::Connectivity | FailureClass::Timeout))
            || matches!(
                outcome,
                Ok(TraversalOutcome::Cancelled | TraversalOutcome::Completed(_))
            )
    }

    fn exhausted(&self) -> Outcome {
        let Some(window) = &self.window else {
            return Err(TraversalTerminalFailure::Internal);
        };
        self.failure.publish_failure(window.first.clone());
        self.state.send_replace(TraversalRecoveryState::Failed {
            failure: window.first.clone(),
        });
        Err(TraversalTerminalFailure::Session(final_failure(
            &window.first,
        )))
    }

    async fn reconnect_source(&mut self) -> Option<(StorageTraversalSource, ScanFailureSignal)> {
        let result = {
            let window = self.window.as_mut()?;
            tokio::select! {
                biased;
                () = self.request.cancel.cancelled() => return None,
                result = &mut window.probe.result => result.ok().flatten(),
            }
        };
        if result.is_some() {
            self.recovered();
        }
        result
    }
}

fn final_failure(failure: &RemoteScanFailure) -> BackendSessionFailure {
    if let Some(StorageRoleFailure::Session(source)) = &failure.source {
        return source.clone();
    }
    let class = match failure.kind {
        RemoteScanFailureKind::ConnectionLost => FailureClass::Connectivity,
        RemoteScanFailureKind::OperationTimedOut => FailureClass::Timeout,
    };
    BackendSessionFailure::new(
        failure.operation,
        class,
        Transience::Transient,
        failure.code(),
    )
    .unwrap_or_else(|_| unreachable!("static diagnostic is valid"))
}
