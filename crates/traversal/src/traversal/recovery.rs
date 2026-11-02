//! Reconnect remote scans within one recovery window and resume safe directory positions.

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::{oneshot, watch};
use tokio::time::{Instant, sleep_until, timeout_at};

use super::channel;
use super::scan::{RetainedOperation, ScanFailureSignal};
use super::{
    Checkpoint, DEFAULT_MAX_BUFFERED_ITEMS, RemoteScanFailure, RemoteScanFailureKind, Resumable,
    ScanSetupError, StorageTraversalSource, TraversalOutcome, TraversalProducer, TraversalRequest,
    TraversalSession, TraversalTerminalFailure,
};
use crate::model::{BackendSessionFailure, FailureClass, Transience};
use crate::storage::{BackendConnectError, Storage, StorageRoleFailure};

mod attempt;
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
    /// Recovery work allowed from the first fault until its operation/path succeeds.
    /// Time blocked delivering output to the consumer does not consume this budget.
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
        restoring: false,
    };
    tokio::spawn(runner.run(source, signal));
    Ok((session, status))
}

struct Window {
    first: RemoteScanFailure,
    started: Instant,
    deadline: Instant,
    attempt: u64,
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
    restoring: bool,
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
        if let Some(window) = &self.window
            && self.restoring
            && signal.failure().is_none()
            && signal
                .recovered_at()
                .is_some_and(|at| at <= window.deadline)
        {
            self.restoring = false;
            self.window = None;
            self.recoveries = self.recoveries.saturating_add(1);
            self.state.send_replace(ScanRecoveryState::Running {
                recoveries: self.recoveries,
            });
        }
        self.start_window(signal);
    }

    fn start_window(&mut self, signal: &ScanFailureSignal) {
        if self.window.is_none()
            && let Some(first) = signal.failure()
        {
            let started = signal.failure_detected_at().unwrap_or_else(Instant::now);
            self.window = Some(Window {
                first,
                started,
                deadline: started + self.policy.recovery_window,
                attempt: 0,
            });
            self.publish_recovering();
        }
    }

    fn account_output_wait(&mut self, signal: &ScanFailureSignal, since: Instant) {
        self.start_window(signal);
        let now = Instant::now();
        if let Some(window) = &mut self.window {
            window.deadline += now.saturating_duration_since(since.max(window.started));
        }
        self.observe(signal);
    }

    fn publish_recovering(&self) {
        if let Some(window) = &self.window {
            self.state.send_replace(ScanRecoveryState::Recovering {
                first_failure: window.first.clone(),
                attempt: window.attempt,
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
        loop {
            let window = self.window.as_mut()?;
            if Instant::now() >= window.deadline {
                return None;
            }
            window.attempt = window.attempt.saturating_add(1);
            let deadline = window.deadline;
            self.publish_recovering();
            let mut work = RetainedOperation::new((self.reconnect)());
            let result = tokio::select! {
                biased;
                () = self.request.cancel.cancelled() => return None,
                result = timeout_at(deadline, work.wait()) => result,
            };
            if let Ok(Ok(storage)) = result
                && let Ok((source, signal)) =
                    ScanFailureSignal::source(&storage, self.policy.operation_timeout)
            {
                if let Some(window) = &self.window {
                    signal.watch_recovery(window.first.operation, window.first.path.clone());
                }
                return Some((source, signal));
            }
            tokio::select! {
                () = self.request.cancel.cancelled() => return None,
                () = sleep_until((Instant::now() + Duration::from_millis(25)).min(deadline)) => {}
            }
        }
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
