use futures::StreamExt as _;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::model::{FailureClass, StoragePath};
use crate::storage::{PositionedByteStream, PositionedChunk, StorageRoleFailure};

pub(super) type Sender = mpsc::Sender<Result<PositionedChunk, StorageRoleFailure>>;

pub(super) async fn closed(sender: Option<&Sender>) {
    match sender {
        Some(sender) => sender.closed().await,
        None => std::future::pending().await,
    }
}

pub(super) fn channel(
    cancel: CancellationToken,
    path: StoragePath,
) -> (Sender, PositionedByteStream) {
    let (sender, receiver) = mpsc::channel(4);
    let state = (receiver, cancel, path, false);
    let stream = futures::stream::unfold(state, |(mut receiver, cancel, path, done)| async move {
        if done {
            return None;
        }
        let item = tokio::select! {
            biased;
            () = cancel.cancelled() => Some(Err(super::super::failure::source_failure(&path, FailureClass::Cancelled))),
            item = receiver.recv() => item,
        };
        item.map(|item| {
            let terminal = item.is_err();
            (item, (receiver, cancel, path, terminal))
        })
    });
    (sender, stream.boxed())
}
