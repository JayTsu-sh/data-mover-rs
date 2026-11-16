//! Reuse ordinary page admission, observations, filtering, diagnostics and counts.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use tokio::sync::Semaphore;

use super::super::observe::{Listed, ObservationError};
use super::super::output::{ChildItem, tally_child};
use super::super::stream::{self, Batch, Child, Page, Slot};
use super::super::unordered::page_io;
use super::super::{Deferred, DirectoryWork, Shared, resolve};
use super::Node;
use super::resources::{self, Held, PageCharge};
use crate::model::{EntryKind, ObservedEntry};
use crate::storage::{ListingPage, SourceDescriptor};
use crate::traversal::consumer::ConsumerState;
use crate::traversal::{TraversalItem, TraversalTerminalFailure};

#[derive(Default)]
pub(super) struct Prepared {
    pub(super) items: VecDeque<Held<TraversalItem>>,
    pub(super) below: Vec<Held<DirectoryWork>>,
    pub(super) statistics: Option<Held<TraversalItem>>,
    pub(super) terminal: Option<TraversalTerminalFailure>,
    _storage: Option<PageCharge>,
}

#[derive(Default)]
struct Draft {
    below: Vec<DirectoryWork>,
    terminal: Option<TraversalTerminalFailure>,
}

enum Settled {
    Filtered(Vec<crate::traversal::ObservationFailure>),
    Observed(
        Result<Box<ObservedEntry>, ObservationError>,
        Option<Deferred>,
        EntryKind,
    ),
    Pending(Option<Deferred>, EntryKind),
}

pub(super) async fn prepare(
    shared: &Arc<Shared>,
    budget: &Arc<Semaphore>,
    consumer: &Arc<ConsumerState>,
    node: &mut Node,
    raw: ListingPage,
    charge: PageCharge,
) -> Result<Prepared, TraversalTerminalFailure> {
    let raw_bytes = resources::page_bytes(&raw);
    let Page {
        batch: Batch {
            failures,
            children,
            statistics,
        },
        slots,
        facts,
    } = stream::prepare(shared, &node.work, raw);
    facts.count_into(&mut node.counts);
    if let Some(counts) = node.statistics.as_mut() {
        counts.page(statistics);
    }
    let mut preparation = Preparation::new(shared, node, slots);
    for failure in failures {
        preparation.failure(failure);
    }
    // Preserve admitted facts before awaiting any additional observation. On success the
    // ordinary output copy retires them; cancellation/aggregation failure drains them.
    consumer.record_observation(
        preparation.items.clone(),
        preparation.prepared.terminal.clone(),
    );
    for child in &children {
        match child {
            Child::Filtered(failures) => consumer.record_observation(
                failures
                    .iter()
                    .cloned()
                    .map(|failure| TraversalItem::ObservationFailure(Box::new(failure)))
                    .collect(),
                None,
            ),
            Child::Observe {
                listed: Listed::Observed(result),
                ..
            } => {
                page_io::record_observation(consumer, result.as_ref().map(Box::as_ref));
            }
            Child::Observe {
                listed: Listed::Unobserved(_),
                ..
            } => {}
        }
    }
    let initial = preparation.bytes(&children).max(raw_bytes);
    // No extra observation starts until the acquired page and all inline facts fit.
    // Acquisition/inline preparation are a documented transient, not an allocator cap.
    charge.resize(initial)?;
    let settled = observe(
        shared,
        budget,
        consumer,
        children,
        preparation.prepared.terminal.as_ref(),
        &charge,
    )
    .await?;
    for child in settled {
        preparation.child(child)?;
    }
    preparation.finish(charge)
}

struct Preparation<'a> {
    shared: &'a Shared,
    node: &'a mut Node,
    prepared: Draft,
    hints: HashMap<u64, Option<crate::storage::DescentHint>>,
    items: Vec<TraversalItem>,
}

impl<'a> Preparation<'a> {
    fn new(shared: &'a Shared, node: &'a mut Node, slots: Vec<Slot>) -> Self {
        let mut result = Self {
            shared,
            node,
            prepared: Draft::default(),
            hints: HashMap::new(),
            items: Vec::new(),
        };
        for slot in slots {
            match slot {
                Slot::Descend(work) => result.prepared.below.push(work),
                Slot::Pending { ticket, hint, .. } => {
                    result.hints.insert(ticket, hint);
                }
            }
        }
        result
    }

    fn failure(&mut self, failure: crate::model::EntryOperationFailure) {
        self.prepared.terminal.get_or_insert_with(|| {
            TraversalTerminalFailure::ConsumerSource(Box::new(failure.clone()))
        });
        self.count(
            ChildItem::Item(TraversalItem::Failure(Box::new(
                crate::traversal::ScanFailure::child(failure),
            ))),
            None,
            None,
        );
    }

    fn child(&mut self, child: Settled) -> Result<(), TraversalTerminalFailure> {
        let (result, deferred, kind) = match child {
            Settled::Filtered(failures) => {
                self.count(ChildItem::ObservationFailures(failures), None, None);
                return Ok(());
            }
            Settled::Observed(result, deferred, kind) => (result, deferred, kind),
            Settled::Pending(_, _) => return Err(TraversalTerminalFailure::Internal),
        };
        match resolve(self.shared, result, deferred) {
            Ok(resolved) => self.resolved(resolved, deferred, kind),
            Err(error) => {
                self.prepared.terminal.get_or_insert(error.terminal);
                self.items.extend(
                    error
                        .observations
                        .into_iter()
                        .map(|failure| TraversalItem::ObservationFailure(Box::new(failure))),
                );
            }
        }
        Ok(())
    }

    fn resolved(
        &mut self,
        resolved: super::super::Resolved,
        deferred: Option<Deferred>,
        kind: EntryKind,
    ) {
        if let ChildItem::Item(TraversalItem::Failure(failure))
        | ChildItem::ObservedFailure { failure, .. } = &resolved.item
        {
            self.prepared.terminal.get_or_insert_with(|| {
                TraversalTerminalFailure::ConsumerSource(Box::new(failure.failure.clone()))
            });
        }
        if let Some(work) = resolved.below {
            let hint = deferred
                .and_then(|value| value.ticket)
                .and_then(|ticket| self.hints.remove(&ticket).flatten());
            self.prepared.below.push(work.with_hint(hint));
        }
        self.count(resolved.item, Some(kind), resolved.descent);
    }

    fn count(
        &mut self,
        item: ChildItem,
        kind: Option<EntryKind>,
        descent: Option<super::super::output::ChildDescent>,
    ) {
        if let Some(counts) = self.node.statistics.as_mut() {
            counts.child(&item, kind, descent);
        }
        tally_child(
            &mut self.node.counts,
            item,
            descent,
            self.shared.probe.history_segment(),
        )
        .push_into(&mut self.items);
    }

    fn bytes(&self, children: &VecDeque<Child>) -> usize {
        let storage = self
            .items
            .capacity()
            .saturating_mul(std::mem::size_of::<TraversalItem>())
            .saturating_add(
                self.prepared
                    .below
                    .capacity()
                    .saturating_mul(std::mem::size_of::<DirectoryWork>()),
            )
            .saturating_add(self.hints.capacity().saturating_mul(
                2 * std::mem::size_of::<(u64, Option<crate::storage::DescentHint>)>(),
            ))
            .saturating_add(
                children
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Child>()),
            )
            .saturating_add(children.len().saturating_mul(
                std::mem::size_of::<Settled>()
                    + std::mem::size_of::<(usize, Box<SourceDescriptor>)>(),
            ));
        let items = self.items.iter().map(resources::item_bytes);
        let below = self.prepared.below.iter().map(resources::work_bytes);
        let children = children.iter().map(|child| match child {
            Child::Filtered(failures) => failures
                .iter()
                .map(resources::observation_failure_bytes)
                .fold(0, usize::saturating_add),
            Child::Observe {
                listed: Listed::Observed(Ok(entry)),
                ..
            } => resources::entry_bytes(entry),
            Child::Observe {
                listed: Listed::Observed(Err(error)),
                ..
            } => resources::observation_error_bytes(error),
            Child::Observe {
                listed: Listed::Unobserved(entry),
                ..
            } => resources::descriptor_bytes(entry),
        });
        items
            .chain(below)
            .chain(children)
            .fold(storage, usize::saturating_add)
            .saturating_add(
                self.prepared
                    .terminal
                    .as_ref()
                    .map_or(0, resources::terminal_bytes),
            )
            .saturating_add(
                self.hints
                    .values()
                    .map(|hint| hint.as_ref().map_or(0, |hint| hint.as_bytes().len()))
                    .fold(0, usize::saturating_add),
            )
    }

    fn finish(self, charge: PageCharge) -> Result<Prepared, TraversalTerminalFailure> {
        let statistics = self
            .node
            .statistics
            .as_mut()
            .map(|counts| counts.snapshot(&self.node.work.path, None, false));
        let mut items = VecDeque::with_capacity(self.items.len());
        let mut below = Vec::with_capacity(self.prepared.below.len());
        let fixed = items
            .capacity()
            .saturating_mul(std::mem::size_of::<Held<TraversalItem>>())
            .saturating_add(
                below
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Held<DirectoryWork>>()),
            );
        let fixed = fixed.saturating_add(
            self.prepared
                .terminal
                .as_ref()
                .map_or(0, resources::terminal_bytes),
        );
        let payload = self
            .items
            .iter()
            .map(resources::item_bytes)
            .chain(self.prepared.below.iter().map(resources::work_bytes))
            .chain(statistics.iter().map(resources::item_bytes))
            .fold(fixed, usize::saturating_add);
        charge.resize(payload)?;
        for item in self.items {
            let bytes = resources::item_bytes(&item);
            items.push_back(charge.take(item, bytes)?);
        }
        for work in self.prepared.below {
            let bytes = resources::work_bytes(&work);
            below.push(charge.take(work, bytes)?);
        }
        let statistics = statistics
            .map(|item| {
                let bytes = resources::item_bytes(&item);
                charge.take(item, bytes)
            })
            .transpose()?;
        Ok(Prepared {
            items,
            below,
            statistics,
            terminal: self.prepared.terminal,
            _storage: Some(charge),
        })
    }
}

async fn observe(
    shared: &Arc<Shared>,
    budget: &Arc<Semaphore>,
    consumer: &Arc<ConsumerState>,
    children: VecDeque<Child>,
    known_failure: Option<&TraversalTerminalFailure>,
    charge: &PageCharge,
) -> Result<Vec<Settled>, TraversalTerminalFailure> {
    if let Some(failure) = known_failure {
        consumer.record_failure(failure.clone());
    }
    let mut settled = Vec::with_capacity(children.len());
    let mut jobs = Vec::new();
    for (place, child) in children.into_iter().enumerate() {
        match child {
            Child::Filtered(failures) => settled.push(Settled::Filtered(failures)),
            Child::Observe {
                listed: Listed::Observed(result),
                deferred,
                kind,
            } => settled.push(Settled::Observed(result, deferred, kind)),
            Child::Observe {
                listed: Listed::Unobserved(descriptor),
                deferred,
                kind,
            } => {
                if known_failure.is_some() {
                    continue;
                }
                settled.push(Settled::Pending(deferred, kind));
                jobs.push((place, descriptor));
            }
        }
    }
    if jobs.is_empty() {
        return Ok(settled);
    }
    let expected = jobs.len();
    let done = page_io::observe_page(
        shared,
        budget,
        &consumer.stopped,
        jobs,
        Some(Arc::clone(consumer)),
        Some(charge.clone()),
    )
    .await?
    .ok_or(TraversalTerminalFailure::Internal)?;
    if done.len() != expected {
        return Err(TraversalTerminalFailure::Internal);
    }
    for (place, result) in done {
        let slot = settled
            .get_mut(place)
            .ok_or(TraversalTerminalFailure::Internal)?;
        let Settled::Pending(deferred, kind) = slot else {
            return Err(TraversalTerminalFailure::Internal);
        };
        *slot = Settled::Observed(result, *deferred, *kind);
    }
    Ok(settled)
}
