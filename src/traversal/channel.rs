//! The session's item channel, which moves items a batch at a time (P7 §12.5).
//!
//! A traversal produces its items a page at a time — an Unordered worker one listing page, the
//! Blocks driver whatever its reorder buffer released — so it hands them over the same way: one
//! channel operation, and at most one wake-up of the consumer, per batch rather than per item.
//! [`super::TraversalSession::next_item`] still hands out one item at a time.
//!
//! **Bounded in items, not batches.** A send first takes one permit per item from a semaphore of
//! `capacity` permits, and the receiver returns a batch's permits only once the consumer
//! has taken its last item. So the items sent and not yet handed to the consumer never exceed
//! `capacity`, whatever the batch sizes. A batch larger than the whole bound either goes
//! over in pieces of the bound ([`ItemSender::send`]: the Blocks driver, the only sender, so its
//! pieces stay in order), or whole ([`ItemSender::send_whole`]: it waits for the whole bound to be
//! free, takes all of it, and goes over alone).
//!
//! **A whole batch arrives whole.** It is one message, so no other sender's items land inside it:
//! the consumer sees its items together and in order. The Unordered workers, many senders, rely
//! on it to keep the segments of one key's history together (see `unordered::worker`).

use std::mem;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::vec;

use tokio::sync::{Semaphore, mpsc};

use super::TraversalItem;

/// The sending half. Clones share the bound.
#[derive(Clone)]
pub(crate) struct ItemSender {
    batches: mpsc::UnboundedSender<Vec<TraversalItem>>,
    room: Arc<Semaphore>,
    /// The bound: the most permits one batch takes.
    limit: usize,
}

/// The receiving half, owned by the session.
pub(crate) struct ItemReceiver {
    batches: mpsc::UnboundedReceiver<Vec<TraversalItem>>,
    room: Arc<Semaphore>,
    /// What is left of the batch taken last.
    current: vec::IntoIter<TraversalItem>,
    /// Permits of that batch, returned once it is used up.
    held: usize,
    /// The bound: the most permits one batch took.
    limit: usize,
}

/// A channel that holds at most `capacity` items sent and not yet taken.
pub(crate) fn channel(capacity: NonZeroUsize) -> (ItemSender, ItemReceiver) {
    // `acquire_many` takes a `u32`, and a semaphore holds at most `MAX_PERMITS`.
    let limit = capacity
        .get()
        .min(Semaphore::MAX_PERMITS)
        .min(usize::try_from(u32::MAX).unwrap_or(usize::MAX));
    let (sender, receiver) = mpsc::unbounded_channel();
    let room = Arc::new(Semaphore::new(limit));
    (
        ItemSender {
            batches: sender,
            room: Arc::clone(&room),
            limit,
        },
        ItemReceiver {
            batches: receiver,
            room,
            current: Vec::new().into_iter(),
            held: 0,
            limit,
        },
    )
}

impl ItemSender {
    /// Sends `items` in pieces of at most the bound, each waiting for room. Another sender's
    /// items may land between two pieces. Returns what was not sent once the receiver is gone.
    pub(crate) async fn send(
        &self,
        mut items: Vec<TraversalItem>,
    ) -> Result<(), Vec<TraversalItem>> {
        while items.len() > self.limit {
            let rest = items.split_off(self.limit);
            if let Err(mut unsent) = self.send_whole(items).await {
                unsent.extend(rest);
                return Err(unsent);
            }
            items = rest;
        }
        self.send_whole(items).await
    }

    /// Sends `items` as one batch, waiting for room — for all of the bound when the batch is
    /// larger. Returns them once the receiver is gone.
    pub(crate) async fn send_whole(
        &self,
        items: Vec<TraversalItem>,
    ) -> Result<(), Vec<TraversalItem>> {
        if items.is_empty() {
            return Ok(());
        }
        let charge = u32::try_from(items.len().min(self.limit)).unwrap_or(u32::MAX);
        let Ok(permit) = self.room.acquire_many(charge).await else {
            return Err(items);
        };
        // The receiver returns the permits once the consumer has taken the batch's last item.
        permit.forget();
        self.batches.send(items).map_err(|error| error.0)
    }

    /// Whether the receiver is gone.
    pub(crate) fn is_closed(&self) -> bool {
        self.batches.is_closed()
    }

    /// Sends one item; see [`Self::send`].
    pub(crate) async fn send_one(&self, item: TraversalItem) -> Result<(), TraversalItem> {
        self.send(vec![item])
            .await
            .map_err(|mut unsent| unsent.swap_remove(0))
    }
}

impl ItemReceiver {
    /// The next item, waiting for a batch when the one taken last is used up. `None` once every
    /// sender is gone and everything sent has been taken.
    pub(crate) async fn recv(&mut self) -> Option<TraversalItem> {
        loop {
            if let Some(item) = self.current.next() {
                return Some(item);
            }
            // The batch taken last is used up: its room is the producers' again.
            self.room.add_permits(mem::take(&mut self.held));
            let batch = self.batches.recv().await?;
            self.held = batch.len().min(self.limit);
            self.current = batch.into_iter();
        }
    }

    /// Stops the senders: a waiting one returns its items, and so does every later send.
    /// Whatever was sent already is dropped.
    pub(crate) fn close(&mut self) {
        self.room.close();
        self.batches.close();
        self.current = Vec::new().into_iter();
        while self.batches.try_recv().is_ok() {}
    }
}

impl Drop for ItemReceiver {
    fn drop(&mut self) {
        // A sender waiting for room would otherwise wait for a receiver that never comes back.
        self.room.close();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::model::{EntryOperationFailure, FailureClass, Operation, StoragePath, Transience};
    use crate::traversal::ScanFailure;

    fn item(name: &str) -> TraversalItem {
        TraversalItem::Failure(Box::new(ScanFailure::child(
            EntryOperationFailure::new(
                StoragePath::new(name).unwrap_or_else(|error| panic!("{error}")),
                Operation::Observe,
                FailureClass::PermissionDenied,
                Transience::Permanent,
                "denied",
            )
            .unwrap_or_else(|error| panic!("{error}")),
        )))
    }

    fn name(item: &TraversalItem) -> String {
        match item {
            TraversalItem::Failure(failure) => failure.path.as_str().to_owned(),
            other => panic!("unexpected {other:?}"),
        }
    }

    fn capacity(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap_or_else(|| unreachable!("tests pass nonzero"))
    }

    /// A batch larger than the bound goes over in pieces of the bound, in order.
    #[tokio::test]
    async fn a_large_batch_goes_over_in_pieces_of_the_bound() {
        let (sender, mut receiver) = channel(capacity(3));
        let task = tokio::spawn(async move {
            sender
                .send((0..7).map(|index| item(&format!("i{index}"))).collect())
                .await
                .map_err(|_| "receiver gone")
        });
        let mut names = Vec::new();
        while let Some(item) = receiver.recv().await {
            assert!(receiver.held <= 3);
            names.push(name(&item));
        }
        assert_eq!(names, ["i0", "i1", "i2", "i3", "i4", "i5", "i6"]);
        assert!(task.await.is_ok_and(|sent| sent.is_ok()));
    }

    /// Items come out one at a time, in order, across batches.
    #[tokio::test]
    async fn batches_are_taken_one_item_at_a_time_in_order() {
        let (sender, mut receiver) = channel(capacity(8));
        sender
            .send(vec![item("a"), item("b")])
            .await
            .unwrap_or_else(|_| panic!("receiver gone"));
        sender
            .send_one(item("c"))
            .await
            .unwrap_or_else(|_| panic!("receiver gone"));
        drop(sender);
        let mut names = Vec::new();
        while let Some(item) = receiver.recv().await {
            names.push(name(&item));
        }
        assert_eq!(names, ["a", "b", "c"]);
    }

    /// The bound is in items: a sender waits once `capacity` items are waiting and not yet
    /// handed out (the batch in hand counts until its last item is taken); a whole batch larger
    /// than the bound waits for all of it and goes over whole.
    #[tokio::test]
    async fn the_bound_counts_items_and_a_larger_batch_goes_over_whole() {
        let (sender, mut receiver) = channel(capacity(4));
        sender
            .send(vec![item("a"), item("b"), item("c")])
            .await
            .unwrap_or_else(|_| panic!("receiver gone"));
        let second = sender.clone();
        let small = tokio::spawn(async move {
            second
                .send(vec![item("d"), item("e")])
                .await
                .map_err(|_| "receiver gone")
        });
        let large = tokio::spawn(async move {
            sender
                .send_whole((0..7).map(|index| item(&format!("i{index}"))).collect())
                .await
                .map_err(|_| "receiver gone")
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!small.is_finished(), "3 + 2 items cannot fit a bound of 4");
        assert_eq!(
            receiver.recv().await.map(|item| name(&item)).as_deref(),
            Some("a")
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!small.is_finished(), "the batch in hand still counts");
        let mut names = Vec::new();
        while let Some(item) = receiver.recv().await {
            assert!(receiver.room.available_permits() + receiver.held <= 4);
            names.push(name(&item));
        }
        assert_eq!(names.len(), 11);
        // The large batch arrived whole: its items are consecutive and in order.
        let first = names.iter().position(|name| name == "i0");
        let run: Vec<&str> = names
            .iter()
            .skip(first.unwrap_or_default())
            .take(7)
            .map(String::as_str)
            .collect();
        assert_eq!(run, ["i0", "i1", "i2", "i3", "i4", "i5", "i6"]);
        assert!(small.await.is_ok_and(|sent| sent.is_ok()));
        assert!(large.await.is_ok_and(|sent| sent.is_ok()));
    }

    /// A sender waiting for room returns its items once the receiver is gone, rather than waiting
    /// for good.
    #[tokio::test]
    async fn a_waiting_sender_gets_its_items_back_when_the_receiver_goes() {
        let (sender, receiver) = channel(capacity(1));
        sender
            .send_one(item("held"))
            .await
            .unwrap_or_else(|_| panic!("receiver gone"));
        let task = tokio::spawn(async move { sender.send_one(item("waiting")).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(receiver);
        let returned = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap_or_else(|_| panic!("the sender kept waiting"))
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(returned.is_err_and(|item| name(&item) == "waiting"));
    }
}
