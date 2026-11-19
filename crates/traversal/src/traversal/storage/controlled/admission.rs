//! Metered admission and publication; a deferred allocation never drops the owned page item.
use super::arena::{Key, Stage};
use super::page::Bundle;
use super::publish::{Phase, Target};
use super::{Charge, DirectoryWork, Held, Runner};
use crate::model::{EntryKind, ObservedEntry};
use crate::traversal::{TraversalItem, TraversalTerminalFailure};

impl Runner {
    pub(super) fn start_bundle(&mut self, key: Key) -> Result<bool, TraversalTerminalFailure> {
        if self.output_busy() {
            self.nodes.queue(key, false);
            return Ok(false);
        }
        let bundle = &self
            .nodes
            .get(key)
            .and_then(|live| live.node.as_ref())
            .and_then(|node| node.page.items.front())
            .ok_or(TraversalTerminalFailure::Internal)?
            .value;
        let entry = bundle.entry();
        if entry.is_some_and(|entry| entry.kind() == EntryKind::Directory) {
            return self.directory(key);
        }
        let phase = if entry.is_some() {
            match self.phase_charge() {
                Ok(charge) => Some(charge),
                Err(error) => {
                    self.park(key, error);
                    return Ok(false);
                }
            }
        } else {
            None
        };
        let live = self
            .nodes
            .get_mut(key)
            .ok_or(TraversalTerminalFailure::Internal)?;
        let bundle = live
            .node
            .as_mut()
            .and_then(|node| node.page.items.pop_front())
            .ok_or(TraversalTerminalFailure::Internal)?;
        live.bundles += 1;
        let target = if phase.is_some() {
            live.files += 1;
            Target::Work(key, Phase::File)
        } else {
            Target::Continue(key)
        };
        self.begin(bundle, target, phase);
        self.nodes.queue(key, false);
        Ok(true)
    }

    fn directory(&mut self, parent: Key) -> Result<bool, TraversalTerminalFailure> {
        if !self.allow_child(parent)? {
            return Ok(false);
        }
        let node = self
            .nodes
            .get(parent)
            .and_then(|live| live.node.as_ref())
            .ok_or(TraversalTerminalFailure::Internal)?;
        let bundle = &node
            .page
            .items
            .front()
            .ok_or(TraversalTerminalFailure::Internal)?
            .value;
        let entry = bundle.entry().ok_or(TraversalTerminalFailure::Internal)?;
        let snapshot = entry
            .try_encode_snapshot()
            .map_err(|_| TraversalTerminalFailure::Internal)?;
        let position = node
            .page
            .below
            .iter()
            .position(|work| &work.value.path == entry.path());
        let policy = DirectoryWork {
            path: entry.path().clone(),
            child_depth: node.work.child_depth.saturating_add(1),
            filter_children: false,
            hint: None,
        };
        let work = position.map_or(&policy, |index| &node.page.below[index].value);
        let credit = position.map_or(0, |index| node.page.below[index].charge.bytes());
        let final_bytes = super::resources::entry_bytes(entry).saturating_add(Self::phase_bytes());
        let bytes = Self::node_bytes(work, Some(&snapshot)).saturating_sub(credit);
        let phase = match self.phase_charge() {
            Ok(charge) => charge,
            Err(error) => {
                self.park(parent, error);
                return Ok(false);
            }
        };
        // Reserve the later decoded finalization payload/receipt before admitting the node.
        // This is byte credit, not a W phase permit: waiting directories cannot consume W.
        let final_credit = match self.resources.reserve(final_bytes) {
            Ok(charge) => charge,
            Err(error) => {
                self.park(parent, error);
                return Ok(false);
            }
        };
        let mut charge = match self.resources.reserve(bytes) {
            Ok(charge) => charge,
            Err(error) => {
                self.park(parent, error);
                return Ok(false);
            }
        };
        let live = self
            .nodes
            .get_mut(parent)
            .ok_or(TraversalTerminalFailure::Internal)?;
        let node = live
            .node
            .as_mut()
            .ok_or(TraversalTerminalFailure::Internal)?;
        let bundle = node
            .page
            .items
            .pop_front()
            .ok_or(TraversalTerminalFailure::Internal)?;
        let work = if let Some(position) = position {
            let Held { value, charge: old } = node.page.below.remove(position);
            charge.absorb(old);
            value
        } else {
            policy
        };
        live.bundles += 1;
        let mut child = self.node(work, Some(snapshot), Some(charge))?;
        child.final_credit = Some(final_credit);
        if position.is_none() {
            child.statistics = None;
        }
        let key = self.nodes.insert(Some(parent), child, Stage::Prepare)?;
        let live = self
            .nodes
            .get_mut(key)
            .ok_or(TraversalTerminalFailure::Internal)?;
        live.eof = position.is_none();
        live.prepares = bundle.value.entries();
        self.begin(bundle, Target::Work(key, Phase::Prepare), Some(phase));
        self.nodes.queue(parent, false);
        Ok(true)
    }

    pub(super) fn hidden(&mut self, parent: Key) -> Result<bool, TraversalTerminalFailure> {
        if !self.allow_child(parent)? {
            return Ok(false);
        }
        let work = self
            .nodes
            .get(parent)
            .and_then(|live| live.node.as_ref())
            .and_then(|node| node.page.below.last())
            .ok_or(TraversalTerminalFailure::Internal)?;
        let bytes = Self::node_bytes(&work.value, None).saturating_sub(work.charge.bytes());
        let mut charge = match self.resources.reserve(bytes) {
            Ok(charge) => charge,
            Err(error) => {
                self.park(parent, error);
                return Ok(false);
            }
        };
        let work = self
            .nodes
            .get_mut(parent)
            .and_then(|live| live.node.as_mut())
            .and_then(|node| node.page.below.pop())
            .ok_or(TraversalTerminalFailure::Internal)?;
        charge.absorb(work.charge);
        let node = self.node(work.value, None, Some(charge))?;
        let key = self.nodes.insert(Some(parent), node, Stage::Open)?;
        self.nodes.queue(parent, false);
        self.nodes.queue(key, false);
        Ok(true)
    }

    fn allow_child(&mut self, parent: Key) -> Result<bool, TraversalTerminalFailure> {
        let limit = self.shared.request.consumer_max_inflight.get();
        if self.nodes.can_add_child(parent, limit)? {
            return Ok(true);
        }
        // Keep the unconsumed page with its parent. Descendants remain runnable and
        // completing one can make room for the next sibling.
        self.nodes.queue(parent, false);
        Ok(false)
    }

    pub(super) fn finalize(&mut self, key: Key) -> Result<bool, TraversalTerminalFailure> {
        let node = self
            .nodes
            .get(key)
            .and_then(|live| live.node.as_ref())
            .ok_or(TraversalTerminalFailure::Internal)?;
        let Some(original) = node.original.as_ref() else {
            self.complete(key)?;
            return Ok(true);
        };
        if self.output_busy() {
            self.nodes.queue(key, true);
            return Ok(false);
        }
        let entry = ObservedEntry::decode_snapshot(original.as_bytes(), &self.identity)
            .map_err(|_| TraversalTerminalFailure::Internal)?;
        let needed = super::resources::entry_bytes(&entry).saturating_add(Self::phase_bytes());
        let live = self
            .nodes
            .get_mut(key)
            .ok_or(TraversalTerminalFailure::Internal)?;
        let node = live
            .node
            .as_mut()
            .ok_or(TraversalTerminalFailure::Internal)?;
        let credit = node
            .final_credit
            .as_mut()
            .ok_or(TraversalTerminalFailure::Internal)?;
        if let Err(error) = credit.resize(needed) {
            self.park(key, error);
            return Ok(false);
        }
        let mut charge = node
            .final_credit
            .take()
            .ok_or(TraversalTerminalFailure::Internal)?;
        let phase = charge.split(Self::phase_bytes())?;
        live.stage = Stage::FinalAcknowledgement;
        live.node
            .as_mut()
            .ok_or(TraversalTerminalFailure::Internal)?
            .original = None;
        let bundle = Held {
            value: Bundle::One(Some(TraversalItem::Entry(Box::new(entry)))),
            charge,
        };
        self.begin(bundle, Target::Work(key, Phase::Finalize), Some(phase));
        Ok(true)
    }

    pub(super) fn continue_bundle(&mut self) -> Result<bool, TraversalTerminalFailure> {
        let (target, bundle) = self
            .continuation
            .as_ref()
            .ok_or(TraversalTerminalFailure::Internal)?;
        let phase = if bundle.value.entry().is_some() {
            match self.phase_charge() {
                Ok(charge) => Some(charge),
                Err(error) => {
                    self.blocked_error = Some(error);
                    return Ok(false);
                }
            }
        } else {
            None
        };
        if let Target::Work(key, Phase::File) = *target
            && phase.is_some()
        {
            self.nodes
                .get_mut(key)
                .ok_or(TraversalTerminalFailure::Internal)?
                .files += 1;
        }
        let (target, bundle) = self
            .continuation
            .take()
            .ok_or(TraversalTerminalFailure::Internal)?;
        self.begin(bundle, target, phase);
        Ok(true)
    }

    fn begin(&mut self, bundle: Held<Bundle>, target: Target, phase: Option<Charge>) {
        self.sending = Some(super::publish::send(
            self.consumer.clone(),
            self.output.clone(),
            bundle,
            target,
            phase,
        ));
    }

    pub(super) fn send_item(&mut self, item: Held<TraversalItem>, target: Target) {
        self.begin(
            Held {
                value: Bundle::One(Some(item.value)),
                charge: item.charge,
            },
            target,
            None,
        );
    }
}
