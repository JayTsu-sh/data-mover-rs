//! Reconnect remote scans within one recovery window and resume safe directory positions.

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;

use super::channel;
use super::scan::{RecoveryBudget, ScanFailureSignal};
use super::{
    Checkpoint, DEFAULT_MAX_BUFFERED_ITEMS, RemoteScanFailure, RemoteScanFailureKind, Resumable,
    ScanSetupError, StorageTraversalSource, TraversalOutcome, TraversalProducer, TraversalRequest,
    TraversalSession, TraversalTerminalFailure,
};
use crate::model::{BackendSessionFailure, FailureClass, Transience};
use crate::storage::{BackendConnectError, Storage, StorageRoleFailure};

mod attempt;
mod probe;
use probe::{Probe, ProbeContext};
#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[path = "../recovery_tests.rs"]
mod tests;

/// Opens a fresh connection with the same endpoint identity and listing semantics.
pub type ScanReconnect =
    Arc<dyn Fn() -> BoxFuture<'static, Result<Storage, BackendConnectError>> + Send + Sync>;

/// Independent native-operation and reconnect budgets for a remote scan.
#[derive(Clone, Copy, Debug)]
pub struct ScanRecoveryPolicy {
    /// A native operation deadline; `None` preserves native protocol deadlines.
    pub operation_timeout: Option<Duration>,
    /// Wall-clock budget from the first detected fault through reconnection and
    /// verification of the original failed read. Verified recovery ends this
    /// deadline before healthy checkpoint rebuilding or consumer waits continue.
    pub recovery_window: Duration,
}

impl Default for ScanRecoveryPolicy {
    fn default() -> Self {
        Self {
            operation_timeout: None,
            recovery_window: Duration::from_secs(3),
        }
    }
}

/// Recovery activity; only `Failed` represents exhausted network recovery.
#[derive(Clone, Debug)]
pub enum ScanRecoveryState {
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
pub struct ScanRecoveryStatus {
    state: watch::Receiver<ScanRecoveryState>,
    failure: ScanFailureSignal,
}

impl ScanRecoveryStatus {
    /// Observes recovery independently of traversal output backpressure.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<ScanRecoveryState> {
        self.state.clone()
    }

    /// Publishes only the first fault of a recovery window that ultimately fails.
    #[must_use]
    pub fn failure_signal(&self) -> ScanFailureSignal {
        self.failure.clone()
    }
}

/// Scans with fresh connections and checkpoint recovery after native network failures.
///
/// Active directories may be replayed. Delivered items keep their order and complete history
/// groups; completion totals describe the final resumed traversal, as for ordinary resume.
/// The caller's checkpoint visibility and cancellation-drain policy are preserved.
///
/// # Errors
/// Rejects invalid operation deadlines or unavailable traversal roles before starting.
pub fn recoverable_scan(
    initial: &Storage,
    request: TraversalRequest,
    policy: ScanRecoveryPolicy,
    reconnect: ScanReconnect,
) -> Result<(TraversalSession, ScanRecoveryStatus), ScanSetupError> {
    let (source, signal) = ScanFailureSignal::source(initial, policy.operation_timeout)?;
    let failure = ScanFailureSignal::new(initial.kind(), policy.operation_timeout);
    let (state, receiver) = watch::channel(ScanRecoveryState::Running { recoveries: 0 });
    let (items, output) = channel::channel(DEFAULT_MAX_BUFFERED_ITEMS);
    let (completion, result) = oneshot::channel();
    let session = TraversalSession::new(
        output,
        result,
        request.cancel.clone(),
        request.resumable.every().is_some(),
    );
    let producer = TraversalProducer {
        items,
        completion: Some(completion),
    };
    let status = ScanRecoveryStatus {
        state: receiver,
        failure: failure.clone(),
    };
    let runner = Recovery {
        request,
        policy,
        reconnect,
        producer,
        state,
        failure,
        window: None,
        checkpoint: None,
        recoveries: 0,
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
    request: TraversalRequest,
    policy: ScanRecoveryPolicy,
    reconnect: ScanReconnect,
    producer: TraversalProducer,
    state: watch::Sender<ScanRecoveryState>,
    failure: ScanFailureSignal,
    window: Option<Window>,
    checkpoint: Option<Checkpoint>,
    recoveries: u64,
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
                }
                None if self.request.cancel.is_cancelled() => {
                    break Ok(TraversalOutcome::Cancelled);
                }
                None => break self.exhausted(),
            }
        };
        self.producer.finish(outcome);
    }

    fn request_for_attempt(&self) -> TraversalRequest {
        let mut request = self.request.clone();
        request.cancel = self.request.cancel.child_token();
        request.resumable = match request.resumable {
            Resumable::Off => Resumable::on(),
            value => value,
        };
        if let Some(saved) = &self.checkpoint {
            request.resume_from = Some(saved.clone());
        }
        request
    }

    fn observe(&mut self, signal: &ScanFailureSignal) {
        self.start_window(signal);
        if signal.failure().is_some()
            && let Some(window) = &self.window
        {
            window.budget.reconnecting(true);
        }
    }

    fn recovered(&mut self) {
        if let Some(window) = self.window.take() {
            window.budget.complete();
        }
        self.recoveries = self.recoveries.saturating_add(1);
        self.state.send_replace(ScanRecoveryState::Running {
            recoveries: self.recoveries,
        });
    }

    fn start_window(&mut self, signal: &ScanFailureSignal) {
        if self.window.is_none()
            && let Some(first) = signal.failure()
        {
            let started = signal.failure_detected_at().unwrap_or_else(Instant::now);
            let elapsed = started.elapsed();
            let budget = RecoveryBudget::new(self.policy.recovery_window.saturating_sub(elapsed));
            let probe = ProbeContext {
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
        self.observe(signal);
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
        self.state.send_replace(ScanRecoveryState::Failed {
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
