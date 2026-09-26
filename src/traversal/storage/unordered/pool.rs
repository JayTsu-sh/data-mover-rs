//! The Unordered workers' stacks, and the one counter that says when the traversal is done.
//!
//! **Stacks.** Each worker owns one stack of directories still to list. It pushes the
//! subdirectories it finds onto its own stack and pops its own newest first (depth first, which
//! keeps the pending set as small as a depth-first walk's). A worker whose stack is empty steals
//! the *shallowest* directory any other worker holds — the front of that worker's stack, which is
//! the root of the largest subtree still waiting — so a steal hands over as much work as it can.
//!
//! **Who holds what (the frontier).** Every directory not yet finished is, at every moment, in at
//! least one worker's slot: on a stack, or as the slot's `active` directory, which it becomes the
//! moment it leaves a stack and stays until its listing has ended and every subdirectory it found
//! is on a stack. A pop takes the directory off the stack and makes it `active` under the same
//! lock; a steal holds both slots' locks (in index order, so two steals and a snapshot of every
//! slot can never deadlock) while it moves the directory from the victim's stack to its own
//! `active`. So a snapshot taken with every slot locked — what a resumable traversal records
//! (P7 §9.2) — never misses a subtree, and never needs the workers to pause.
//!
//! **Done.** `outstanding` counts directories pushed and not finished. A directory adds its
//! subdirectories before they become visible to anyone, and subtracts itself only after that, so
//! the count reaches zero exactly once, when nothing is left anywhere: the worker that takes it
//! there halts the traversal.
//!
//! **Idle workers** sleep on a [`Notify`]. A push wakes one only when some worker is idle, so a
//! busy traversal pays no wake-up per directory; a worker that stole from a stack with more left
//! wakes the next idle one, so a burst of work spreads without waking everyone at once.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering, fence};

use parking_lot::{Mutex, MutexGuard};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::super::DirectoryWork;
use crate::traversal::TraversalTerminalFailure;

/// No directory on the stack.
const EMPTY: usize = usize::MAX;

/// One worker's stack and the directory it is listing.
#[derive(Default)]
struct SlotState {
    /// Oldest (shallowest) at the front, newest at the back.
    stack: VecDeque<Arc<DirectoryWork>>,
    /// The directory being listed, from the moment it left a stack until it is finished.
    active: Option<Arc<DirectoryWork>>,
}

struct Slot {
    state: Mutex<SlotState>,
    /// Depth of the stack's front, or [`EMPTY`], published so a thief chooses its victim without
    /// taking every lock.
    front_depth: AtomicUsize,
}

impl Slot {
    fn new() -> Self {
        Self {
            state: Mutex::new(SlotState::default()),
            front_depth: AtomicUsize::new(EMPTY),
        }
    }

    /// Publishes the depth of the front, after the stack changed under `state`'s lock.
    fn publish(&self, state: &SlotState) {
        let depth = state.stack.front().map_or(EMPTY, |work| work.child_depth);
        self.front_depth.store(depth, Ordering::Relaxed);
    }
}

pub(super) struct Pool {
    slots: Box<[Slot]>,
    /// Directories pushed and not finished.
    outstanding: AtomicUsize,
    /// Workers waiting for work.
    idle: AtomicUsize,
    wake: Notify,
    /// Where the next steal starts looking, so thieves do not all start at the same victim.
    rotor: AtomicUsize,
    /// The first failure that ends the traversal.
    failure: Mutex<Option<TraversalTerminalFailure>>,
    /// Checks the frontier invariant at every move (tests only).
    #[cfg(test)]
    pub(super) checked: bool,
}

impl Pool {
    /// A pool of `workers` slots, the first holding `root`.
    pub(super) fn new(workers: usize, root: DirectoryWork) -> Self {
        let slots: Box<[Slot]> = (0..workers.max(1)).map(|_| Slot::new()).collect();
        if let Some(first) = slots.first() {
            let mut state = first.state.lock();
            state.stack.push_back(Arc::new(root));
            first.publish(&state);
        }
        Self {
            slots,
            outstanding: AtomicUsize::new(1),
            idle: AtomicUsize::new(0),
            wake: Notify::new(),
            rotor: AtomicUsize::new(0),
            failure: Mutex::new(None),
            #[cfg(test)]
            checked: true,
        }
    }

    pub(super) fn workers(&self) -> usize {
        self.slots.len()
    }

    /// The next directory for worker `me`: its own newest, else the shallowest another holds, else
    /// once some worker pushes one. `None` once the traversal halts.
    pub(super) async fn take(
        &self,
        me: usize,
        halt: &CancellationToken,
    ) -> Option<Arc<DirectoryWork>> {
        loop {
            if halt.is_cancelled() {
                return None;
            }
            if let Some(work) = self.pop_own(me).or_else(|| self.steal(me)) {
                return Some(work);
            }
            let notified = self.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            // Pairs with the fence in `push`: either this worker sees the push below, or the
            // pusher sees this worker idle and wakes it.
            self.idle.fetch_add(1, Ordering::SeqCst);
            fence(Ordering::SeqCst);
            let found = self.pop_own(me).or_else(|| self.steal(me));
            if found.is_none() {
                tokio::select! {
                    () = &mut notified => {}
                    () = halt.cancelled() => {}
                }
            }
            self.idle.fetch_sub(1, Ordering::SeqCst);
            if found.is_some() {
                return found;
            }
        }
    }

    /// Worker `me`'s newest directory, made its `active` one under the same lock.
    fn pop_own(&self, me: usize) -> Option<Arc<DirectoryWork>> {
        let slot = self.slots.get(me)?;
        let work = {
            let mut state = slot.state.lock();
            let work = state.stack.pop_back()?;
            state.active = Some(Arc::clone(&work));
            slot.publish(&state);
            work
        };
        self.check_moved(&work);
        Some(work)
    }

    /// The shallowest directory another worker holds, moved onto `me` as its `active` one with
    /// both slots locked.
    fn steal(&self, me: usize) -> Option<Arc<DirectoryWork>> {
        // A victim can be emptied between the scan and the lock; every retry saw a front that
        // was there, so the loop is bounded by the work that exists.
        loop {
            let victim = self.shallowest(me)?;
            let (mut mine, mut theirs) = self.lock_pair(me, victim)?;
            let Some(work) = theirs.stack.pop_front() else {
                continue;
            };
            mine.active = Some(Arc::clone(&work));
            let left = theirs.stack.len();
            if let Some(slot) = self.slots.get(victim) {
                slot.publish(&theirs);
            }
            drop(theirs);
            drop(mine);
            self.check_moved(&work);
            if left > 0 && self.idle.load(Ordering::SeqCst) > 0 {
                self.wake.notify_one();
            }
            return Some(work);
        }
    }

    /// The worker other than `me` whose stack front is shallowest, from the published depths.
    fn shallowest(&self, me: usize) -> Option<usize> {
        let workers = self.slots.len();
        let start = self.rotor.fetch_add(1, Ordering::Relaxed) % workers;
        let mut best: Option<(usize, usize)> = None;
        for offset in 0..workers {
            let index = (start + offset) % workers;
            if index == me {
                continue;
            }
            let depth = self
                .slots
                .get(index)
                .map_or(EMPTY, |slot| slot.front_depth.load(Ordering::Relaxed));
            if depth != EMPTY && best.is_none_or(|(_, shallowest)| depth < shallowest) {
                best = Some((index, depth));
            }
        }
        best.map(|(index, _)| index)
    }

    /// Locks `me`'s and `victim`'s slots, the lower index first; returns them as (mine, theirs).
    fn lock_pair(
        &self,
        me: usize,
        victim: usize,
    ) -> Option<(MutexGuard<'_, SlotState>, MutexGuard<'_, SlotState>)> {
        let (own, other) = (self.slots.get(me)?, self.slots.get(victim)?);
        Some(if me < victim {
            let mine = own.state.lock();
            (mine, other.state.lock())
        } else {
            let theirs = other.state.lock();
            (own.state.lock(), theirs)
        })
    }

    /// Puts the subdirectories worker `me` found onto its stack, counted before anyone can see
    /// them, and wakes an idle worker if there is one.
    pub(super) fn push(&self, me: usize, works: Vec<DirectoryWork>) {
        if works.is_empty() {
            return;
        }
        self.outstanding.fetch_add(works.len(), Ordering::SeqCst);
        if let Some(slot) = self.slots.get(me) {
            let mut state = slot.state.lock();
            #[cfg(test)]
            assert!(
                !self.checked || state.active.is_some(),
                "children pushed by a worker whose directory is not registered"
            );
            state.stack.extend(works.into_iter().map(Arc::new));
            slot.publish(&state);
        }
        // Pairs with the fence in `take`.
        fence(Ordering::SeqCst);
        if self.idle.load(Ordering::Relaxed) > 0 {
            self.wake.notify_one();
        }
    }

    /// Worker `me` finished its `active` directory: its listing ended and every subdirectory it
    /// found is pushed. Returns whether that was the last directory anywhere.
    pub(super) fn finish(&self, me: usize) -> bool {
        if let Some(slot) = self.slots.get(me) {
            slot.state.lock().active = None;
        }
        self.outstanding.fetch_sub(1, Ordering::AcqRel) == 1
    }

    /// Whether every directory pushed was finished.
    pub(super) fn drained(&self) -> bool {
        self.outstanding.load(Ordering::Acquire) == 0
    }

    /// Records the failure that ends the traversal; the first one wins.
    pub(super) fn fail(&self, failure: TraversalTerminalFailure) {
        self.failure.lock().get_or_insert(failure);
    }

    pub(super) fn failure(&self) -> Option<TraversalTerminalFailure> {
        self.failure.lock().clone()
    }

    /// Every directory not yet finished, as held by the slots with every slot locked at once, in
    /// index order (the order a steal takes two of them in): stacks and active directories. What
    /// a resumable traversal's checkpoint records (P7 §9.2); a directory moving between two slots
    /// may appear twice, never not at all.
    #[cfg(test)]
    pub(super) fn frontier(&self) -> Vec<Arc<DirectoryWork>> {
        let guards: Vec<_> = self.slots.iter().map(|slot| slot.state.lock()).collect();
        guards
            .iter()
            .flat_map(|state| state.stack.iter().chain(state.active.iter()))
            .cloned()
            .collect()
    }

    /// A directory that just left a stack must still be in the frontier.
    #[cfg(test)]
    fn check_moved(&self, work: &Arc<DirectoryWork>) {
        if self.checked {
            assert!(
                self.frontier().iter().any(|held| Arc::ptr_eq(held, work)),
                "{} left a stack without being registered",
                work.path
            );
        }
    }

    #[cfg(not(test))]
    #[allow(clippy::unused_self)]
    const fn check_moved(&self, _work: &Arc<DirectoryWork>) {}
}

#[cfg(test)]
mod tests;
