//! Consumer-completed subtrees, with independent snapshot, parked-page and work bounds.
//! Parent listings remain open: this scheduler never relists a released subtree.

use std::num::NonZeroUsize;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
#[cfg(test)]
use futures::{FutureExt as _, StreamExt as _};
use tokio::sync::{Semaphore, oneshot};

use super::statistics::DirectoryAccumulator;
use super::{DirectoryWork, Shared, StorageTraversalSource, root_work};
use crate::model::{BackendIdentity, EntryKind, EntrySnapshot, FailureClass};
use crate::storage::{ListRequest, ListingStream, StorageRoleFailure};
use crate::traversal::channel::{self, ItemSender};
use crate::traversal::consumer::ConsumerState;
use crate::traversal::{
    BlockCounts, Resumable, ScanTotals, TraversalCompletion, TraversalConfigurationError,
    TraversalOrder, TraversalOutcome, TraversalRequest, TraversalSession, TraversalTerminalFailure,
};

mod admission;
mod arena;
mod io;
mod page;
mod publish;
pub(super) mod resources;
mod scheduler;
#[cfg(test)]
use crate::traversal::TraversalItem;
use arena::{Arena, Key, Stage};
use page::Prepared;
use publish::{Phase, Sending, Target};
#[cfg(test)]
use resources::PageCharge;
use resources::{Charge, Held, Resources};
#[cfg(test)]
#[path = "controlled_tests.rs"]
mod tests;

type Terminal = Result<TraversalOutcome, TraversalTerminalFailure>;
type Acknowledged = (Key, Phase, Result<bool, oneshot::error::RecvError>);

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
    final_credit: Option<Charge>,
    next_page_credit: usize,
    page: Prepared,
    counts: BlockCounts,
    statistics: Option<DirectoryAccumulator>,
    charge: Charge,
}

struct Runner {
    shared: Arc<Shared>,
    consumer: Arc<ConsumerState>,
    output: ItemSender,
    identity: BackendIdentity,
    nodes: Arena,
    acknowledgements: FuturesUnordered<BoxFuture<'static, Acknowledged>>,
    reads: FuturesUnordered<BoxFuture<'static, io::Read>>,
    sending: Option<Sending>,
    continuation: Option<(Target, Held<page::Bundle>)>,
    observations: Arc<Semaphore>,
    resources: Arc<Resources>,
    listing_limit: usize,
    listings: usize,
    read_limit: usize,
    page_hint: NonZeroUsize,
    blocked_error: Option<TraversalTerminalFailure>,
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
        let node_limit = (completion_limit / arena::storage_bytes()).max(1);
        Self {
            resources: Resources::new(completion_limit),
            listing_limit: shared
                .parked_listing_limit
                .map_or(node_limit, |n| node_limit.min(n)),
            listings: 0,
            read_limit: operations,
            page_hint: shared
                .request
                .consumer_max_inflight
                .min(crate::storage::listing::DEFAULT_PAGE_HINT),
            observations: Arc::new(Semaphore::new(operations)),
            shared,
            consumer,
            output,
            identity,
            nodes: Arena::default(),
            acknowledgements: FuturesUnordered::new(),
            reads: FuturesUnordered::new(),
            sending: None,
            continuation: None,
            blocked_error: None,
            totals: ScanTotals::default(),
        }
    }

    async fn run(&mut self) -> Terminal {
        let result = self.walk().await;
        if let Err(error) = &result {
            self.consumer.record_failure(error.clone());
        }
        if result.is_err() || self.consumer.stopped.is_cancelled() {
            // Seal the registered facts before dropping outer page/output futures. Their native
            // operations follow their existing cleanup paths; late results cannot add facts.
            self.consumer.close();
            self.sending = None;
            self.continuation = None;
            self.reads.clear();
            self.acknowledgements.clear();
            self.nodes = Arena::default();
        }
        if let Some(failure) = self.consumer.failure() {
            return Err(failure);
        }
        if self.shared.request.cancel.is_cancelled() || self.consumer.stopped.is_cancelled() {
            return Ok(TraversalOutcome::Cancelled);
        }
        result.map(|()| TraversalOutcome::Completed(TraversalCompletion::new(self.totals)))
    }

    async fn root(&mut self) -> Result<(), TraversalTerminalFailure> {
        let namespace = self.shared.namespace.clone();
        let path = self.shared.request.root.clone();
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let _ = sender.send(namespace.stat(&path, false).await);
        });
        let descriptor = tokio::select! {
            biased;
            ()=self.consumer.stopped.cancelled()=>return Ok(()),
            result=receiver=>result.map_err(|_|TraversalTerminalFailure::Internal)?,
        }
        .map_err(source_failure)?;
        if descriptor.kind != EntryKind::Directory {
            return Err(TraversalTerminalFailure::ConsumerSource(Box::new(
                super::entry_failure(&descriptor.path, FailureClass::Unsupported),
            )));
        }
        let node = self.node(root_work(&self.shared.request), None, None)?;
        let key = self.nodes.insert(None, node, Stage::Open)?;
        self.nodes.queue(key, false);
        Ok(())
    }

    fn node_bytes(work: &DirectoryWork, original: Option<&EntrySnapshot>) -> usize {
        arena::storage_bytes()
            .saturating_add(resources::work_bytes(work))
            .saturating_add(work.path.as_str().len().saturating_mul(2))
            .saturating_add(original.map_or(0, |raw| raw.as_bytes().len()))
    }

    fn node(
        &self,
        work: DirectoryWork,
        original: Option<EntrySnapshot>,
        prior: Option<Charge>,
    ) -> Result<Node, TraversalTerminalFailure> {
        let bytes = Self::node_bytes(&work, original.as_ref());
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
            final_credit: None,
            next_page_credit: std::mem::size_of::<crate::storage::ListingPage>(),
            statistics,
            charge,
            boundary: Boundary::PolicyBoundary,
            page: Prepared::default(),
            counts: BlockCounts::default(),
        })
    }

    fn open(&mut self, key: Key) -> Result<bool, TraversalTerminalFailure> {
        if self.listings >= self.listing_limit {
            self.park(
                key,
                TraversalTerminalFailure::ConsumerListingLimitExceeded {
                    limit: self.listing_limit,
                },
            );
            return Ok(false);
        }
        let live = self
            .nodes
            .get_mut(key)
            .ok_or(TraversalTerminalFailure::Internal)?;
        let node = live
            .node
            .as_mut()
            .ok_or(TraversalTerminalFailure::Internal)?;
        node.boundary = Boundary::Listing(
            self.shared.namespace.list_for_traversal(
                ListRequest::new(node.work.path.clone(), self.consumer.stopped.child_token())
                    .with_descent_hint(node.work.hint.clone())
                    .with_page_hint(self.page_hint),
                self.shared
                    .request
                    .root_object_diagnostics_at(node.work.child_depth),
            ),
        );
        self.listings += 1;
        live.stage = Stage::Ready;
        self.nodes.queue(key, false);
        Ok(true)
    }

    fn phase_bytes() -> usize {
        std::mem::size_of::<Acknowledged>()
            + std::mem::size_of::<oneshot::Receiver<bool>>()
            + std::mem::size_of::<Charge>()
    }

    fn phase_charge(&self) -> Result<Charge, TraversalTerminalFailure> {
        self.resources.reserve(Self::phase_bytes())
    }

    fn park(&mut self, key: Key, error: TraversalTerminalFailure) {
        self.blocked_error = Some(error);
        self.nodes.block(key);
    }
    fn stopped(&self) -> TraversalTerminalFailure {
        self.consumer
            .failure()
            .unwrap_or(TraversalTerminalFailure::Internal)
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
