//! The admission cursor: walks the tree depth-first and assigns every output slot its sequence.
//!
//! Output order is a sequence of *blocks*. A block is every child of one listed directory, in
//! listing order. The root's block comes first; after a directory's block, each subdirectory
//! it descends into contributes its own block, recursively, in listing order. So a directory's
//! children always follow the directory itself, and one listing's children are contiguous.
//!
//! Only the cursor allocates sequence numbers, and it does so in exactly that order. Listings
//! and observations may finish in any order; timing decides only *when* the cursor can take the
//! next step, never which sequence a result receives, and the reorder buffer releases results
//! strictly by sequence. That is what makes the output deterministic for a given tree.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use super::{
    DirectoryWork, ListingTask, ObservationTask, Runtime, State, admit, entry_failure,
    queue_failure,
};
use crate::model::{FailureClass, StoragePath};
use crate::storage::{NamespaceRequest, NamespaceResult, SourceDescriptor, StorageRoleFailure};
use crate::traversal::TraversalTerminalFailure;

/// Why the cursor stopped advancing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Step {
    /// Every block has been admitted.
    Done,
    /// The admission window is full.
    WindowFull,
    /// The next block's listing has not arrived yet.
    WaitListing,
    /// The next descend decision waits for a deferred observation.
    WaitObservation,
}

/// Where the cursor goes after a block, in listing order.
pub(super) enum Slot {
    Descend(DirectoryWork),
    /// A deferred decision, keyed by the sequence of the observation that settles it.
    Pending(u64),
}

/// One directory on the cursor's path from the root.
struct Frame {
    work: DirectoryWork,
    /// Children still to admit, in listing order; `None` until the listing is taken.
    children: Option<VecDeque<SourceDescriptor>>,
    /// Subdirectories to descend into once the block is admitted.
    slots: VecDeque<Slot>,
}

impl Frame {
    const fn new(work: DirectoryWork) -> Self {
        Self {
            work,
            children: None,
            slots: VecDeque::new(),
        }
    }
}

type ListingResult = Result<NamespaceResult, StorageRoleFailure>;

pub(super) struct Cursor {
    stack: Vec<Frame>,
    /// Deferred descend decisions, filled as their observations settle.
    decisions: HashMap<u64, Option<DirectoryWork>>,
    /// Listings started for frames the cursor has not taken yet: `None` while in flight.
    listings: HashMap<StoragePath, Option<ListingResult>>,
}

/// What to do with the top frame once its block is admitted.
enum Next {
    Pop,
    Descend(DirectoryWork),
    Skip,
    Wait,
}

impl Cursor {
    pub(super) fn new(root: DirectoryWork) -> Self {
        Self {
            stack: vec![Frame::new(root)],
            decisions: HashMap::new(),
            listings: HashMap::new(),
        }
    }

    /// Records a deferred observation's descend decision.
    pub(super) fn decide(&mut self, sequence: u64, descend: Option<DirectoryWork>) {
        self.decisions.insert(sequence, descend);
    }

    /// Stores a finished listing. A lost session ends the traversal at once.
    pub(super) fn listed(
        &mut self,
        path: StoragePath,
        result: ListingResult,
    ) -> Result<(), TraversalTerminalFailure> {
        if let Err(StorageRoleFailure::Session(error)) = result {
            return Err(TraversalTerminalFailure::Session(error));
        }
        self.listings.insert(path, Some(result));
        Ok(())
    }

    /// Starts the listing the cursor needs next, if it is not already running.
    pub(super) fn start_listings(&mut self, runtime: &Runtime<'_>, listings: &mut ListingTask) {
        let Some(frame) = self.stack.last() else {
            return;
        };
        if frame.children.is_some() || self.listings.contains_key(&frame.work.path) {
            return;
        }
        let path = frame.work.path.clone();
        self.listings.insert(path.clone(), None);
        let namespace = Arc::clone(runtime.namespace);
        listings.spawn(async move {
            let result = namespace
                .execute(NamespaceRequest::List(path.clone()))
                .await;
            (path, result)
        });
    }

    /// Admits as much as possible and reports what stopped it.
    pub(super) fn advance(
        &mut self,
        runtime: &Runtime<'_>,
        tasks: &mut ObservationTask,
        state: &mut State,
    ) -> Result<Step, TraversalTerminalFailure> {
        loop {
            let Some(frame) = self.stack.last_mut() else {
                return Ok(Step::Done);
            };
            if frame.children.is_none() {
                let Some(result) = self
                    .listings
                    .get_mut(&frame.work.path)
                    .and_then(Option::take)
                else {
                    return Ok(Step::WaitListing);
                };
                self.listings.remove(&frame.work.path);
                if !take_listing(frame, result, state)? {
                    self.stack.pop();
                }
                continue;
            }
            if let Some(children) = frame.children.as_mut()
                && !children.is_empty()
            {
                if state.admitted() >= runtime.request.max_inflight_operations.get() {
                    return Ok(Step::WindowFull);
                }
                let Some(descriptor) = children.pop_front() else {
                    continue;
                };
                admit(
                    runtime,
                    &frame.work,
                    descriptor,
                    &mut frame.slots,
                    tasks,
                    state,
                )?;
                continue;
            }
            match self.next_after_block() {
                Next::Pop => {
                    self.stack.pop();
                }
                Next::Descend(work) => self.stack.push(Frame::new(work)),
                Next::Skip => {}
                Next::Wait => return Ok(Step::WaitObservation),
            }
        }
    }

    /// Consumes the top frame's next slot once its whole block has been admitted.
    fn next_after_block(&mut self) -> Next {
        let Some(frame) = self.stack.last_mut() else {
            return Next::Pop;
        };
        match frame.slots.front() {
            None => Next::Pop,
            Some(Slot::Pending(sequence)) => {
                let Some(decision) = self.decisions.remove(sequence) else {
                    return Next::Wait;
                };
                frame.slots.pop_front();
                decision.map_or(Next::Skip, Next::Descend)
            }
            Some(Slot::Descend(_)) => match frame.slots.pop_front() {
                Some(Slot::Descend(work)) => Next::Descend(work),
                _ => Next::Skip,
            },
        }
    }
}

/// Turns a finished listing into the frame's block. Returns `false` when the directory could not
/// be listed, after queueing its failure in the block's place.
fn take_listing(
    frame: &mut Frame,
    result: ListingResult,
    state: &mut State,
) -> Result<bool, TraversalTerminalFailure> {
    match result.map(NamespaceResult::into_listing) {
        Ok(Some((entries, failures))) => {
            for failure in failures {
                queue_failure(state, failure)?;
            }
            frame.children = Some(entries.into());
            Ok(true)
        }
        Ok(None) => {
            queue_failure(
                state,
                entry_failure(&frame.work.path, FailureClass::Protocol),
            )?;
            Ok(false)
        }
        Err(StorageRoleFailure::Entry(error)) => {
            queue_failure(state, error)?;
            Ok(false)
        }
        Err(StorageRoleFailure::Session(error)) => Err(TraversalTerminalFailure::Session(error)),
    }
}
