//! One owned child bundle at a time; blocked output never blocks actor receipt processing.
use std::sync::Arc;

use futures::FutureExt as _;
use futures::future::BoxFuture;
use tokio::sync::oneshot;

use super::arena::Key;
use super::page::Bundle;
use super::resources::{Charge, Held};
use crate::traversal::channel::ItemSender;
use crate::traversal::consumer::ConsumerState;
use crate::traversal::{TraversalItem, TraversalTerminalFailure};

#[derive(Clone, Copy)]
pub(super) enum Phase {
    Prepare,
    File,
    Finalize,
}

#[derive(Clone, Copy)]
pub(super) enum Target {
    Work(Key, Phase),
    Continue(Key),
    FinalStatistics(Key),
}

pub(super) struct Published {
    pub(super) target: Target,
    pub(super) receipt: Option<oneshot::Receiver<bool>>,
    pub(super) remainder: Option<Held<Bundle>>,
    pub(super) phase_charge: Option<Charge>,
}

pub(super) type Sending = BoxFuture<'static, Result<Published, TraversalTerminalFailure>>;

pub(super) fn send(
    state: Arc<ConsumerState>,
    output: ItemSender,
    mut bundle: Held<Bundle>,
    target: Target,
    phase_charge: Option<Charge>,
) -> Sending {
    async move {
        let stopped = || {
            state
                .failure()
                .unwrap_or(TraversalTerminalFailure::Internal)
        };
        while let Some(item) = bundle.value.pop() {
            match item {
                TraversalItem::Entry(entry) => {
                    let ready = matches!(target, Target::Work(_, Phase::Finalize));
                    let receipt = state
                        .send(&output, entry, ready)
                        .await
                        .map_err(|()| stopped())?;
                    let remainder = (!bundle.value.is_empty()).then_some(bundle);
                    return Ok(Published {
                        target,
                        receipt: Some(receipt),
                        remainder,
                        phase_charge,
                    });
                }
                diagnostic => tokio::select! {
                    biased;
                    () = state.stopped.cancelled() => return Err(stopped()),
                    result = output.send_one(diagnostic) => result.map_err(|_|stopped())?,
                },
            }
        }
        Ok(Published {
            target,
            receipt: None,
            remainder: None,
            phase_charge,
        })
    }
    .boxed()
}
