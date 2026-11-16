//! Generation-scoped topology and deduplicated ready/blocked queues.
use std::collections::VecDeque;

use super::Node;
use crate::traversal::TraversalTerminalFailure;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Key {
    index: usize,
    generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Stage {
    Prepare,
    Open,
    Ready,
    Reading,
    Children,
    FinalStatistics,
    Finalize,
    FinalAcknowledgement,
}

pub(super) struct Live {
    pub(super) parent: Option<Key>,
    pub(super) node: Option<Box<Node>>,
    pub(super) stage: Stage,
    pub(super) files: usize,
    pub(super) prepares: usize,
    pub(super) bundles: usize,
    pub(super) children: usize,
    pub(super) eof: bool,
    queued: bool,
    blocked: bool,
}

struct Slot {
    generation: u64,
    live: Option<Live>,
}

#[derive(Default)]
pub(super) struct Arena {
    slots: Vec<Slot>,
    free: Vec<usize>,
    ready: VecDeque<Key>,
    blocked: Vec<Key>,
    count: usize,
}

impl Arena {
    pub(super) fn insert(
        &mut self,
        parent: Option<Key>,
        node: Node,
        stage: Stage,
    ) -> Result<Key, TraversalTerminalFailure> {
        let index = if let Some(index) = self.free.pop() {
            index
        } else {
            self.slots.push(Slot {
                generation: 0,
                live: None,
            });
            self.slots.len() - 1
        };
        let slot = &mut self.slots[index];
        slot.generation = slot
            .generation
            .checked_add(1)
            .ok_or(TraversalTerminalFailure::Internal)?;
        let key = Key {
            index,
            generation: slot.generation,
        };
        slot.live = Some(Live {
            parent,
            node: Some(Box::new(node)),
            stage,
            files: 0,
            prepares: 0,
            bundles: 0,
            children: 0,
            eof: false,
            queued: false,
            blocked: false,
        });
        self.count += 1;
        if let Some(parent) = parent {
            self.get_mut(parent)
                .ok_or(TraversalTerminalFailure::Internal)?
                .children += 1;
        }
        Ok(key)
    }

    pub(super) fn get(&self, key: Key) -> Option<&Live> {
        self.slots
            .get(key.index)
            .filter(|slot| slot.generation == key.generation)?
            .live
            .as_ref()
    }

    pub(super) fn get_mut(&mut self, key: Key) -> Option<&mut Live> {
        self.slots
            .get_mut(key.index)
            .filter(|slot| slot.generation == key.generation)?
            .live
            .as_mut()
    }

    pub(super) fn remove(&mut self, key: Key) -> Option<Live> {
        let slot = self
            .slots
            .get_mut(key.index)
            .filter(|slot| slot.generation == key.generation)?;
        let live = slot.live.take()?;
        self.count -= 1;
        self.free.push(key.index);
        Some(live)
    }

    pub(super) fn queue(&mut self, key: Key, priority: bool) {
        let Some(live) = self.get_mut(key) else {
            return;
        };
        if live.queued || live.blocked {
            return;
        }
        live.queued = true;
        if priority {
            self.ready.push_front(key);
        } else {
            self.ready.push_back(key);
        }
    }

    pub(super) fn next(&mut self) -> Option<Key> {
        while let Some(key) = self.ready.pop_front() {
            if let Some(live) = self.get_mut(key) {
                live.queued = false;
                return Some(key);
            }
        }
        None
    }

    pub(super) fn block(&mut self, key: Key) {
        if let Some(live) = self.get_mut(key) {
            if live.blocked {
                return;
            }
            live.blocked = true;
            self.blocked.push(key);
        }
    }

    pub(super) fn wake_blocked(&mut self) {
        let blocked = std::mem::take(&mut self.blocked);
        for key in blocked {
            if let Some(live) = self.get_mut(key) {
                live.blocked = false;
                self.queue(key, false);
            }
        }
    }

    pub(super) fn ready_len(&self) -> usize {
        self.ready.len()
    }
    pub(super) fn len(&self) -> usize {
        self.count
    }

    #[cfg(test)]
    pub(super) fn retained_capacity(&self) -> (usize, usize, usize, usize) {
        (
            self.slots.len(),
            self.slots.capacity(),
            self.ready.capacity(),
            self.free.capacity(),
        )
    }
}

pub(super) const fn storage_bytes() -> usize {
    // Node lives in the slot or an in-flight page future. Reserve topology/ready/free cells too.
    std::mem::size_of::<Node>() + std::mem::size_of::<Slot>() + 3 * std::mem::size_of::<Key>()
}
