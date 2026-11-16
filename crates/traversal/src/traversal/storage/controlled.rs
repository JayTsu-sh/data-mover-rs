//! Consumer-completed subtrees, with independent snapshot, parked-page and work bounds.
//! Parent listings remain open: this scheduler never relists a released subtree.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};
use tokio::sync::{Semaphore, oneshot};

use super::statistics::DirectoryAccumulator;
use super::stream::Arrival;
use super::{DirectoryWork, Shared, StorageTraversalSource, root_work};
use crate::model::{BackendIdentity, EntryKind, EntrySnapshot, FailureClass, ObservedEntry};
use crate::storage::{ListRequest, ListingStream, StorageRoleFailure};
use crate::traversal::channel::{self, ItemSender};
use crate::traversal::consumer::ConsumerState;
use crate::traversal::{
    BlockCounts, ListingOutcome, Resumable, ScanFailure, ScanTotals, TraversalCompletion,
    TraversalConfigurationError, TraversalItem, TraversalOrder, TraversalOutcome, TraversalRequest,
    TraversalSession, TraversalTerminalFailure,
};

mod page;
pub(super) mod resources;
use page::Prepared;
use resources::{Charge, Held, PageCharge, Resources};
#[cfg(test)]
#[path = "controlled_tests.rs"]
mod tests;

type Terminal = Result<TraversalOutcome, TraversalTerminalFailure>;
type Acknowledged = (usize, Result<bool, oneshot::error::RecvError>);

pub(super) fn start(
    source: &StorageTraversalSource,
    request: TraversalRequest,
) -> TraversalSession {
    let refusal = if request.order != TraversalOrder::Unordered {
        Some(TraversalConfigurationError::ConsumerOrderUnsupported)
    } else if !matches!(request.resumable, Resumable::Off) || request.resume_from.is_some() {
        Some(TraversalConfigurationError::ConsumerResumeUnsupported)
    } else if source.namespace.capabilities().directory_objects {
        Some(TraversalConfigurationError::ConsumerCompletionUnsupported)
    } else {
        None
    };
    if let Some(error) = refusal {
        return super::refuse(&request, TraversalTerminalFailure::Configuration(error));
    }
    let requirements = match crate::traversal::configuration::validate(
        &request,
        source.metadata.observation_capabilities(),
    ) {
        Ok(requirements) => requirements,
        Err(error) => {
            return super::refuse(&request, TraversalTerminalFailure::Configuration(error));
        }
    };
    let (sender, receiver) = channel::channel(source.probe.output_capacity());
    let (completion, result) = oneshot::channel();
    let (session, consumer) = TraversalSession::controlled(receiver, result, &request, 1);
    let shared = Arc::new(Shared::new(
        source.namespace.clone(),
        source.metadata.clone(),
        request,
        requirements,
        None,
        source.probe.clone(),
    ));
    let mut runner = Runner::new(shared, consumer, sender, source.identity.clone());
    tokio::spawn(async move {
        let terminal = runner.run().await;
        let _ = completion.send(terminal);
    });
    session
}

/// `PolicyBoundary` closes only the selected request range, never physical EOF.
enum Boundary {
    Listing(ListingStream),
    PolicyBoundary,
}

struct Node {
    work: DirectoryWork,
    boundary: Boundary,
    original: Option<EntrySnapshot>,
    page: Prepared,
    pending: usize,
    counts: BlockCounts,
    statistics: Option<DirectoryAccumulator>,
    charge: Charge,
}

struct Runner {
    shared: Arc<Shared>,
    consumer: Arc<ConsumerState>,
    output: ItemSender,
    identity: BackendIdentity,
    nodes: Vec<Node>,
    acknowledgements: FuturesUnordered<BoxFuture<'static, Acknowledged>>,
    observations: Arc<Semaphore>,
    resources: Arc<Resources>,
    listing_limit: usize,
    page_hint: NonZeroUsize,
    totals: ScanTotals,
}

impl Runner {
    fn new(
        shared: Arc<Shared>,
        consumer: Arc<ConsumerState>,
        output: ItemSender,
        identity: BackendIdentity,
    ) -> Self {
        let operations = shared.request.max_inflight_operations.get().min(64);
        let completion_limit = shared
            .request
            .consumer_completion_budget
            .map_or(0, NonZeroUsize::get);
        let node_limit = (completion_limit / std::mem::size_of::<Node>()).max(1);
        Self {
            resources: Resources::new(completion_limit),
            listing_limit: shared
                .parked_listing_limit
                .map_or(node_limit, |n| node_limit.min(n)),
            page_hint: shared
                .request
                .consumer_max_inflight
                .min(crate::storage::listing::DEFAULT_PAGE_HINT),
            observations: Arc::new(Semaphore::new(operations)),
            shared,
            consumer,
            output,
            identity,
            nodes: Vec::new(),
            acknowledgements: FuturesUnordered::new(),
            totals: ScanTotals::default(),
        }
    }

    async fn run(&mut self) -> Terminal {
        let result = self.walk().await;
        if self.shared.request.cancel.is_cancelled()
            && self.consumer.failure().is_none()
            && matches!(result, Ok(()) | Err(TraversalTerminalFailure::Internal))
        {
            return Ok(TraversalOutcome::Cancelled);
        }
        if let Err(error) = &result {
            self.consumer.record_failure(error.clone()); // Before diagnostic backpressure/derived cancel.
            self.flush_diagnostics().await;
        }
        if let Some(failure) = self.consumer.failure() {
            return Err(failure);
        }
        result.map(|()| TraversalOutcome::Completed(TraversalCompletion::new(self.totals)))
    }

    async fn walk(&mut self) -> Result<(), TraversalTerminalFailure> {
        if self.consumer.stopped.is_cancelled() {
            return Ok(());
        }
        self.root().await?;
        while !self.nodes.is_empty() {
            if self.consumer.stopped.is_cancelled() {
                return Ok(());
            }
            self.drain_acknowledgements()?;
            let index = self.nodes.len() - 1;
            if let Some(failure) = self.nodes[index].page.terminal.take() {
                return Err(failure);
            }
            if let Some(Held { value, charge }) = self.nodes[index].page.items.pop_front() {
                self.item(index, value).await?;
                drop(charge);
            } else if let Some(Held { value, charge }) = self.nodes[index].page.below.pop() {
                self.push(value, None, Some(charge))?;
            } else if let Some(Held { value, charge }) = self.nodes[index].page.statistics.take() {
                self.emit(value).await?;
                drop(charge);
            } else {
                self.read_or_finish(index).await?;
            }
        }
        Ok(())
    }

    async fn root(&mut self) -> Result<(), TraversalTerminalFailure> {
        let namespace = self.shared.namespace.clone();
        let path = self.shared.request.root.clone();
        let (sender, receiver) = oneshot::channel();
        // Never abort an adapter operation while it owns a native handle.
        tokio::spawn(async move {
            let _ = sender.send(namespace.stat(&path).await);
        });
        let descriptor = tokio::select! {
            biased;
            () = self.consumer.stopped.cancelled() => return Ok(()),
            result = receiver => result.map_err(|_| TraversalTerminalFailure::Internal)?,
        }
        .map_err(source_failure)?;
        if descriptor.kind != EntryKind::Directory {
            return Err(TraversalTerminalFailure::ConsumerSource(Box::new(
                super::entry_failure(&descriptor.path, FailureClass::Unsupported),
            )));
        }
        // No root ACL observation/work: the caller owns copy-root metadata.
        self.push(root_work(&self.shared.request), None, None)
    }

    fn push(
        &mut self,
        work: DirectoryWork,
        original: Option<EntrySnapshot>,
        charge: Option<Charge>,
    ) -> Result<(), TraversalTerminalFailure> {
        let mut node = self.node(work, original, charge)?;
        self.open(&mut node)?;
        self.nodes.push(node);
        Ok(())
    }

    fn node(
        &mut self,
        work: DirectoryWork,
        original: Option<EntrySnapshot>,
        prior: Option<Charge>,
    ) -> Result<Node, TraversalTerminalFailure> {
        // Every root/hidden/policy node has a positive fixed charge. Visible work,
        // listing/statistics paths, descent hint and first raw value remain held together.
        let bytes = std::mem::size_of::<Node>()
            .saturating_add(resources::work_bytes(&work))
            .saturating_add(work.path.as_str().len().saturating_mul(2))
            .saturating_add(original.as_ref().map_or(0, |raw| raw.as_bytes().len()));
        let mut charge = match prior {
            Some(charge) => charge,
            None => self.resources.reserve(0)?,
        };
        charge.resize(bytes)?;
        let statistics = self
            .shared
            .request
            .directory_statistics
            .then(|| DirectoryAccumulator::new(work.path.clone()));
        Ok(Node {
            work,
            original,
            statistics,
            charge,
            boundary: Boundary::PolicyBoundary,
            page: Prepared::default(),
            pending: 0,
            counts: BlockCounts::default(),
        })
    }

    fn open(&mut self, node: &mut Node) -> Result<(), TraversalTerminalFailure> {
        self.check_listing_room()?;
        let work = &node.work;
        let stream = self.shared.namespace.list_for_traversal(
            ListRequest::new(work.path.clone(), self.consumer.stopped.child_token())
                .with_descent_hint(work.hint.clone())
                .with_page_hint(self.page_hint),
            self.shared
                .request
                .root_object_diagnostics_at(work.child_depth),
        );
        node.boundary = Boundary::Listing(stream);
        Ok(())
    }

    fn check_listing_room(&self) -> Result<(), TraversalTerminalFailure> {
        let parked = self
            .nodes
            .iter()
            .filter(|node| matches!(node.boundary, Boundary::Listing(_)))
            .count();
        if parked >= self.listing_limit {
            return Err(TraversalTerminalFailure::ConsumerListingLimitExceeded {
                limit: self.listing_limit,
            });
        }
        Ok(())
    }

    async fn item(
        &mut self,
        index: usize,
        item: TraversalItem,
    ) -> Result<(), TraversalTerminalFailure> {
        match item {
            TraversalItem::Entry(entry) if entry.kind() == EntryKind::Directory => {
                self.directory(index, entry).await
            }
            TraversalItem::Entry(entry) => {
                let charge = self.phase_charge()?;
                let result = self
                    .consumer
                    .send(&self.output, entry, false)
                    .await
                    .map_err(|()| self.stopped())?;
                self.nodes[index].pending += 1;
                self.acknowledgements.push(
                    async move {
                        let done = result.await;
                        drop(charge);
                        (index, done)
                    }
                    .boxed(),
                );
                Ok(())
            }
            TraversalItem::Failure(failure) => {
                let error =
                    TraversalTerminalFailure::ConsumerSource(Box::new(failure.failure.clone()));
                self.consumer.record_failure(error.clone());
                self.emit(TraversalItem::Failure(failure)).await?;
                Err(error)
            }
            other => self.emit(other).await,
        }
    }

    async fn directory(
        &mut self,
        index: usize,
        entry: Box<ObservedEntry>,
    ) -> Result<(), TraversalTerminalFailure> {
        let position = self.nodes[index]
            .page
            .below
            .iter()
            .position(|work| &work.value.path == entry.path());
        if position.is_some() {
            self.check_listing_room()?;
        }
        let snapshot = entry
            .try_encode_snapshot()
            .map_err(|_| TraversalTerminalFailure::Internal)?;
        let Held {
            value: work,
            charge,
        } = if let Some(position) = position {
            self.nodes[index].page.below.remove(position)
        } else {
            self.resources.hold(
                DirectoryWork {
                    path: entry.path().clone(),
                    child_depth: self.nodes[index].work.child_depth.saturating_add(1),
                    filter_children: false,
                    hint: None,
                },
                0,
            )?
        };
        let mut node = self.node(work, Some(snapshot), Some(charge))?;
        if position.is_none() {
            node.statistics = None;
        }
        let _phase = self.phase_charge()?;
        let result = self
            .consumer
            .send(&self.output, entry, false)
            .await
            .map_err(|()| self.stopped())?;
        self.wait_acknowledgement(result).await?; // Prepare must succeed before any descendant.
        if position.is_some() {
            self.open(&mut node)?;
        }
        self.nodes.push(node);
        Ok(())
    }

    async fn read_or_finish(&mut self, index: usize) -> Result<(), TraversalTerminalFailure> {
        let next = match &mut self.nodes[index].boundary {
            Boundary::Listing(stream) => tokio::select! {
                biased;
                () = self.consumer.stopped.cancelled() => return Ok(()),
                page = stream.next() => page,
            },
            Boundary::PolicyBoundary => None,
        };
        match next {
            Some(Ok(page)) => {
                let node = &mut self.nodes[index];
                node.page = page::prepare(
                    &self.shared,
                    &self.observations,
                    &self.consumer,
                    node,
                    page,
                    PageCharge::new(self.resources.reserve(0)?),
                )
                .await?;
                Ok(())
            }
            Some(Err(failure)) => self.listing_failed(index, failure).await,
            None => self.finish_node(index).await,
        }
    }

    async fn listing_failed(
        &mut self,
        index: usize,
        failure: StorageRoleFailure,
    ) -> Result<(), TraversalTerminalFailure> {
        match super::stream::failed(&self.nodes[index].work.path, failure) {
            Arrival::Failed(failure) => {
                let error = TraversalTerminalFailure::ConsumerSource(Box::new(failure.clone()));
                let node = &self.nodes[index];
                let item = TraversalItem::Failure(Box::new(ScanFailure::listing(
                    node.work.path.clone(),
                    failure,
                    node.counts.listed,
                )));
                self.consumer
                    .record_observation(vec![item.clone()], Some(error.clone()));
                self.emit(item).await?;
                Err(error)
            }
            Arrival::Terminal(failure) => Err(failure),
            Arrival::Page(_) | Arrival::End => Err(TraversalTerminalFailure::Internal),
        }
    }

    async fn finish_node(&mut self, index: usize) -> Result<(), TraversalTerminalFailure> {
        while self.nodes[index].pending != 0 {
            let done = tokio::select! {
                biased;
                () = self.consumer.stopped.cancelled() => return Err(self.stopped()),
                done = self.acknowledgements.next() => done.ok_or(TraversalTerminalFailure::Internal)?,
            };
            self.acknowledged(done)?;
        }
        let Node {
            work,
            boundary,
            original,
            counts,
            statistics,
            charge,
            ..
        } = self.nodes.pop().ok_or(TraversalTerminalFailure::Internal)?;
        drop(boundary); // Physical EOF or an explicit policy boundary; no stream is reopened.
        self.totals.absorb(&counts);
        if let Some(mut statistics) = statistics {
            self.emit(statistics.snapshot(&work.path, Some(ListingOutcome::Complete), false))
                .await?;
        }
        if let Some(original) = original {
            self.finalize(original).await?;
        }
        drop(charge);
        Ok(())
    }

    async fn finalize(&mut self, original: EntrySnapshot) -> Result<(), TraversalTerminalFailure> {
        let entry = ObservedEntry::decode_snapshot(original.as_bytes(), &self.identity)
            .map_err(|_| TraversalTerminalFailure::Internal)?;
        let entry_charge = self.resources.reserve(resources::entry_bytes(&entry))?;
        let _phase = self.phase_charge()?;
        let result = self
            .consumer
            .send(&self.output, Box::new(entry), true)
            .await
            .map_err(|()| self.stopped())?;
        drop(entry_charge); // Payload moved to the consumer; first raw remains charged.
        self.wait_acknowledgement(result).await?;
        Ok(())
    }

    fn phase_charge(&self) -> Result<Charge, TraversalTerminalFailure> {
        self.resources.reserve(
            std::mem::size_of::<Acknowledged>()
                + std::mem::size_of::<oneshot::Receiver<bool>>()
                + std::mem::size_of::<Charge>(),
        )
    }

    async fn wait_acknowledgement(
        &mut self,
        result: oneshot::Receiver<bool>,
    ) -> Result<(), TraversalTerminalFailure> {
        tokio::select! {
            biased;
            () = self.consumer.stopped.cancelled() => Err(self.stopped()),
            result = result => match result {
                Ok(true) => Ok(()), Ok(false) => Err(TraversalTerminalFailure::ConsumerFailed), Err(_) => Err(self.stopped()),
            },
        }
    }

    fn drain_acknowledgements(&mut self) -> Result<(), TraversalTerminalFailure> {
        while let Some(Some(done)) = self.acknowledgements.next().now_or_never() {
            self.acknowledged(done)?;
        }
        Ok(())
    }

    fn acknowledged(
        &mut self,
        (index, result): Acknowledged,
    ) -> Result<(), TraversalTerminalFailure> {
        match result {
            Ok(true) => {
                let node = self
                    .nodes
                    .get_mut(index)
                    .ok_or(TraversalTerminalFailure::Internal)?;
                node.pending = node
                    .pending
                    .checked_sub(1)
                    .ok_or(TraversalTerminalFailure::Internal)?;
                Ok(())
            }
            Ok(false) => Err(TraversalTerminalFailure::ConsumerFailed),
            Err(_) => Err(self.stopped()),
        }
    }

    fn stopped(&self) -> TraversalTerminalFailure {
        self.consumer
            .failure()
            .unwrap_or(TraversalTerminalFailure::Internal)
    }

    async fn emit(&mut self, item: TraversalItem) -> Result<(), TraversalTerminalFailure> {
        tokio::select! {
            biased;
            () = self.consumer.stopped.cancelled() => Err(self.stopped()),
            sent = self.output.send_one(item) => sent.map_err(|_| self.stopped()),
        }
    }

    async fn flush_diagnostics(&mut self) {
        let mut diagnostics = VecDeque::new();
        for node in &mut self.nodes {
            diagnostics.extend(node.page.items.drain(..).filter(|item| {
                matches!(
                    item.value,
                    TraversalItem::Failure(_) | TraversalItem::ObservationFailure(_)
                )
            }));
        }
        for Held { value, charge } in diagnostics {
            if self.emit(value).await.is_err() {
                break;
            }
            drop(charge);
        }
    }
}

fn source_failure(failure: StorageRoleFailure) -> TraversalTerminalFailure {
    match failure {
        StorageRoleFailure::Entry(failure) => {
            TraversalTerminalFailure::ConsumerSource(Box::new(failure))
        }
        StorageRoleFailure::Session(failure) => TraversalTerminalFailure::Session(failure),
    }
}
