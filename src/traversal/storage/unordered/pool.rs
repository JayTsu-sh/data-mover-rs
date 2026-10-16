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
//! Under test every slot lock goes through `checked::SlotGuard`: each time a thread lets
//! go of its last slot lock, the frontier must hold every directory pushed and not finished, so
//! a move split over two critical sections fails at the gap between them, deterministically.
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

use parking_lot::Mutex;
#[cfg(not(test))]
use parking_lot::MutexGuard;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::super::DirectoryWork;
use crate::traversal::TraversalTerminalFailure;

#[cfg(test)]
mod checked;

#[cfg(test)]
use checked::{SlotGuard, Unfinished};

/// No directory on the stack.
const EMPTY: usize = usize::MAX;

/// A locked slot: the mutex guard itself, or, under test, a guard that checks the frontier once
/// its thread holds no slot lock any more.
#[cfg(not(test))]
type SlotGuard<'a> = MutexGuard<'a, SlotState>;

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
    /// Checks the frontier invariant every time a thread lets go of its last slot lock (tests
    /// only).
    #[cfg(test)]
    pub(super) checked: bool,
    /// Every directory pushed and not finished: what the frontier must hold (tests only).
    #[cfg(test)]
    unfinished: Unfinished,
}

impl Pool {
    /// A pool of `workers` slots, the first holding `seeds` — the traversal root, or a resumed
    /// traversal's frontier, shallowest first so thieves take the largest subtrees.
    pub(super) fn new(workers: usize, seeds: Vec<DirectoryWork>) -> Self {
        let slots: Box<[Slot]> = (0..workers.max(1)).map(|_| Slot::new()).collect();
        let count = seeds.len();
        let seeds: VecDeque<Arc<DirectoryWork>> = seeds.into_iter().map(Arc::new).collect();
        #[cfg(test)]
        let unfinished = Unfinished::of(seeds.iter());
        if let Some(first) = slots.first() {
            let mut state = first.state.lock();
            state.stack = seeds;
            first.publish(&state);
        }
        Self {
            slots,
            outstanding: AtomicUsize::new(count),
            idle: AtomicUsize::new(0),
            wake: Notify::new(),
            rotor: AtomicUsize::new(0),
            failure: Mutex::new(None),
            #[cfg(test)]
            checked: true,
            #[cfg(test)]
            unfinished,
        }
    }

    /// Locks slot `index`.
    fn lock(&self, index: usize) -> Option<SlotGuard<'_>> {
        let state = self.slots.get(index)?.state.lock();
        #[cfg(test)]
        let state = SlotGuard::new(self, state);
        Some(state)
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
        let mut state = self.lock(me)?;
        let work = state.stack.pop_back()?;
        state.active = Some(Arc::clone(&work));
        slot.publish(&state);
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
    fn lock_pair(&self, me: usize, victim: usize) -> Option<(SlotGuard<'_>, SlotGuard<'_>)> {
        Some(if me < victim {
            let mine = self.lock(me)?;
            (mine, self.lock(victim)?)
        } else {
            let theirs = self.lock(victim)?;
            (self.lock(me)?, theirs)
        })
    }

    /// Puts the subdirectories worker `me` found onto its stack, counted before anyone can see
    /// them, and wakes an idle worker if there is one.
    pub(super) fn push(&self, me: usize, works: Vec<DirectoryWork>) {
        if works.is_empty() {
            return;
        }
        self.outstanding.fetch_add(works.len(), Ordering::SeqCst);
        if let (Some(slot), Some(mut state)) = (self.slots.get(me), self.lock(me)) {
            #[cfg(test)]
            assert!(
                !self.checked || state.active.is_some(),
                "children pushed by a worker whose directory is not registered"
            );
            #[cfg(test)]
            let first = state.stack.len();
            state.stack.extend(works.into_iter().map(Arc::new));
            #[cfg(test)]
            self.unfinished.insert(state.stack.range(first..));
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
        if let Some(mut state) = self.lock(me) {
            let done = state.active.take();
            #[cfg(test)]
            self.unfinished.remove(done.as_ref());
            drop(done);
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

    /// Every directory not yet finished, as held by the slots with every slot locked at once (in
    /// index order, the order a steal takes two of them in): what a resumable traversal's
    /// checkpoint records (P7 §9.2). `None`, before anything is copied, when more than `cap` are
    /// held. The workers are held up only while the slots are copied, never paused.
    pub(super) fn snapshot(&self, cap: usize) -> Option<Vec<Arc<DirectoryWork>>> {
        let guards: Vec<_> = self.slots.iter().map(|slot| slot.state.lock()).collect();
        let held: usize = guards
            .iter()
            .map(|state| state.stack.len() + usize::from(state.active.is_some()))
            .sum();
        if held > cap {
            return None;
        }
        Some(
            guards
                .iter()
                .flat_map(|state| state.stack.iter().chain(state.active.iter()))
                .cloned()
                .collect(),
        )
    }

    /// Every directory not yet finished; see [`Self::snapshot`].
    #[cfg(test)]
    pub(super) fn frontier(&self) -> Vec<Arc<DirectoryWork>> {
        self.snapshot(usize::MAX).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests;
