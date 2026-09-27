//! Test builds only: the frontier invariant, checked at every gap between critical sections.
//!
//! Every slot lock goes through a [`SlotGuard`]. When a thread lets go of its last slot lock, it
//! takes every slot's lock and checks that the frontier (every stack and every `active`
//! directory) holds each directory pushed and not finished — the ground truth kept in
//! [`Unfinished`], updated inside the same critical sections as the stacks. That moment, no slot
//! locked by this thread, is exactly the state a checkpoint's snapshot of every slot can see. A
//! move that leaves one stack and registers somewhere in a *second* critical section (the P7 §14
//! fault "先出栈后登记") fails at the gap between the two, every time, not only when a concurrent
//! snapshot happens to land there: the check runs inside the faulty thread's own guard drop, so
//! nothing can register its directory before the check has looked. Correct code has no such gap, so concurrent tests never see a
//! false alarm: the check itself locks every slot, and every move holds its slots throughout.

use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::thread;

use parking_lot::{Mutex, MutexGuard};

use super::{Pool, SlotState};
use crate::traversal::storage::DirectoryWork;

thread_local! {
    /// Slot locks this thread holds, of any pool: pool operations are synchronous and never hold
    /// two pools' slots at once, so one count per thread is exact.
    static HELD: Cell<usize> = const { Cell::new(0) };
}

/// One locked slot. Dropping the last one this thread holds checks the frontier.
pub(super) struct SlotGuard<'a> {
    pool: &'a Pool,
    state: Option<MutexGuard<'a, SlotState>>,
}

impl<'a> SlotGuard<'a> {
    pub(super) fn new(pool: &'a Pool, state: MutexGuard<'a, SlotState>) -> Self {
        HELD.with(|held| held.set(held.get() + 1));
        Self {
            pool,
            state: Some(state),
        }
    }
}

impl Deref for SlotGuard<'_> {
    type Target = SlotState;

    fn deref(&self) -> &SlotState {
        self.state
            .as_deref()
            .unwrap_or_else(|| unreachable!("held until dropped"))
    }
}

impl DerefMut for SlotGuard<'_> {
    fn deref_mut(&mut self) -> &mut SlotState {
        self.state
            .as_deref_mut()
            .unwrap_or_else(|| unreachable!("held until dropped"))
    }
}

impl Drop for SlotGuard<'_> {
    fn drop(&mut self) {
        self.state = None;
        let held = HELD.with(|held| {
            let left = held.get().saturating_sub(1);
            held.set(left);
            left
        });
        if held == 0 && self.pool.checked && !thread::panicking() {
            self.pool.check_frontier();
        }
    }
}

/// Every directory pushed and not finished, by the address of its shared work (alive, so never
/// reused, while it is in here), with its path for the message.
pub(super) struct Unfinished(Mutex<HashMap<usize, String>>);

fn key(work: &Arc<DirectoryWork>) -> usize {
    Arc::as_ptr(work).addr()
}

impl Unfinished {
    pub(super) fn of(root: &Arc<DirectoryWork>) -> Self {
        Self(Mutex::new(HashMap::from([(
            key(root),
            root.path.as_str().to_owned(),
        )])))
    }

    /// Records directories just pushed, inside the push's critical section.
    pub(super) fn insert<'a>(&self, works: impl Iterator<Item = &'a Arc<DirectoryWork>>) {
        let mut unfinished = self.0.lock();
        for work in works {
            unfinished.insert(key(work), work.path.as_str().to_owned());
        }
    }

    /// Forgets a directory just finished, inside the finish's critical section.
    pub(super) fn remove(&self, work: Option<&Arc<DirectoryWork>>) {
        if let Some(work) = work {
            self.0.lock().remove(&key(work));
        }
    }
}

impl Pool {
    /// Asserts that every slot, locked at once, holds every unfinished directory.
    pub(super) fn check_frontier(&self) {
        let guards: Vec<_> = self.slots.iter().map(|slot| slot.state.lock()).collect();
        let held: HashSet<usize> = guards
            .iter()
            .flat_map(|state| state.stack.iter().chain(state.active.iter()))
            .map(key)
            .collect();
        let mut missing: Vec<String> = self
            .unfinished
            .0
            .lock()
            .iter()
            .filter(|(address, _)| !held.contains(address))
            .map(|(_, path)| path.clone())
            .collect();
        drop(guards);
        missing.sort();
        assert!(
            missing.is_empty(),
            "left the frontier while unfinished: {missing:?}"
        );
    }

    /// A pop split over two critical sections — the fault the check exists for. Only the test
    /// that proves the check catches it calls this.
    pub(super) fn pop_split(&self, me: usize) -> Option<Arc<DirectoryWork>> {
        let work = {
            let mut state = self.lock(me)?;
            let work = state.stack.pop_back()?;
            if let Some(slot) = self.slots.get(me) {
                slot.publish(&state);
            }
            work
        };
        self.lock(me)?.active = Some(Arc::clone(&work));
        Some(work)
    }
}
