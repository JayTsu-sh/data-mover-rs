//! One bounded reconnect/target probe runs alongside safe draining of the old attempt.
use std::time::Duration;

use tokio::sync::{oneshot, watch};
use tokio::time::sleep;
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

use super::{StorageTraversalSource, TraversalReconnect, TraversalRecoveryState};
use crate::storage::Storage;
use crate::traversal::scan::{RecoveryBudget, RecoveryTarget, RetainedOperation};
use crate::traversal::{RemoteScanFailure, ScanFailureSignal};

type Ready = (StorageTraversalSource, ScanFailureSignal);

pub(super) struct Probe {
    pub(super) result: oneshot::Receiver<Option<Ready>>,
    _task: AbortOnDropHandle<()>,
}

pub(super) struct ProbeContext {
    pub(super) first: RemoteScanFailure,
    pub(super) target: Option<RecoveryTarget>,
    pub(super) budget: RecoveryBudget,
    pub(super) operation_timeout: Option<Duration>,
    pub(super) reconnect: TraversalReconnect,
    pub(super) cancel: CancellationToken,
    pub(super) state: watch::Sender<TraversalRecoveryState>,
    pub(super) failure: ScanFailureSignal,
    pub(super) recoveries: u64,
}

impl ProbeContext {
    pub(super) fn spawn(self) -> Probe {
        let (send, result) = oneshot::channel();
        let task = tokio::spawn(async move {
            let ready = self.run().await;
            if ready.is_some() && !self.budget.is_exhausted() {
                self.budget.complete();
                self.state.send_replace(TraversalRecoveryState::Running {
                    recoveries: self.recoveries,
                });
                let _ = send.send(ready);
            } else {
                self.budget.exhaust();
                if !self.cancel.is_cancelled() {
                    self.failure.publish_failure(self.first.clone());
                    self.state.send_replace(TraversalRecoveryState::Failed {
                        failure: self.first.clone(),
                    });
                }
                let _ = send.send(None);
            }
        });
        Probe {
            result,
            _task: AbortOnDropHandle::new(task),
        }
    }

    async fn run(&self) -> Option<Ready> {
        let mut attempt = 0_u64;
        loop {
            attempt = attempt.saturating_add(1);
            self.state.send_replace(TraversalRecoveryState::Recovering {
                first_failure: self.first.clone(),
                attempt,
            });
            tokio::select! {
                biased;
                () = self.cancel.cancelled() => return None,
                () = self.budget.exhausted() => return None,
                () = sleep(Duration::from_millis(25)) => {},
            }
            let mut connect = RetainedOperation::new((self.reconnect)());
            let storage = tokio::select! {
                biased;
                () = self.cancel.cancelled() => return None,
                () = self.budget.exhausted() => return None,
                result = connect.wait() => result,
            };
            if let Ok(storage) = storage
                && let Some(ready) = self.verify(storage).await
            {
                return Some(ready);
            }
        }
    }

    async fn verify(&self, storage: Storage) -> Option<Ready> {
        let target = self.target.clone()?;
        let (source, guard) = ScanFailureSignal::source(&storage, self.operation_timeout).ok()?;
        guard.watch_recovery(target.clone(), self.budget.clone());
        let verified = tokio::select! {
            biased;
            () = self.cancel.cancelled() => return None,
            () = self.budget.exhausted() => return None,
            result = target.probe(&storage, &guard, self.cancel.child_token()) => result,
        };
        if verified && guard.failure().is_none() && !self.budget.is_exhausted() {
            Some((source, guard))
        } else {
            None
        }
    }
}
