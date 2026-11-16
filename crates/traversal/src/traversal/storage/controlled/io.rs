//! One page at a time per node; existing listing and observation lifetimes remain owned.
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::{FutureExt as _, StreamExt as _};
use tokio::sync::Semaphore;

use super::super::Shared;
use super::super::stream::{self, Arrival};
use super::arena::Key;
use super::resources::PageCharge;
use super::{Boundary, Node, source_failure};
use crate::storage::StorageRoleFailure;
use crate::traversal::consumer::ConsumerState;
use crate::traversal::{ScanFailure, TraversalItem, TraversalTerminalFailure};

pub(super) struct Read {
    pub(super) key: Key,
    pub(super) node: Box<Node>,
    pub(super) eof: bool,
    pub(super) result: Result<(), TraversalTerminalFailure>,
}

pub(super) fn page(
    key: Key,
    mut node: Box<Node>,
    shared: Arc<Shared>,
    budget: Arc<Semaphore>,
    consumer: Arc<ConsumerState>,
    charge: PageCharge,
) -> BoxFuture<'static, Read> {
    async move {
        let result = async {
            let next = match &mut node.boundary {
                Boundary::Listing(stream) => tokio::select! {
                    biased;
                    () = consumer.stopped.cancelled() => return Ok(false),
                    next = stream.next() => next,
                },
                Boundary::PolicyBoundary => return Ok(true),
            };
            match next {
                Some(Ok(page)) => {
                    node.page =
                        super::page::prepare(&shared, &budget, &consumer, &mut node, page, charge)
                            .await?;
                    Ok(false)
                }
                Some(Err(failure)) => Err(record_listing_failure(&consumer, &node, failure)),
                None => {
                    node.boundary = Boundary::PolicyBoundary;
                    Ok(true)
                }
            }
        }
        .await;
        let (eof, result) = match result {
            Ok(eof) => (eof, Ok(())),
            Err(error) => (false, Err(error)),
        };
        Read {
            key,
            node,
            eof,
            result,
        }
    }
    .boxed()
}

fn record_listing_failure(
    consumer: &ConsumerState,
    node: &Node,
    failure: StorageRoleFailure,
) -> TraversalTerminalFailure {
    let (error, diagnostics) = match stream::failed(&node.work.path, failure) {
        Arrival::Failed(failure) => {
            let error = source_failure(StorageRoleFailure::Entry(failure.clone()));
            let item = TraversalItem::Failure(Box::new(ScanFailure::listing(
                node.work.path.clone(),
                failure,
                node.counts.listed,
            )));
            (error, vec![item])
        }
        Arrival::Terminal(error) => (error, Vec::new()),
        _ => (TraversalTerminalFailure::Internal, Vec::new()),
    };
    consumer.record_observation(diagnostics, Some(error.clone()));
    error
}
