//! Known errors in prepared pages survive a fatal sibling without admitting another entry.

use super::{Cursor, Ended, Listing};
use crate::model::{FailureClass, StoragePath};
use crate::traversal::ScanFailure;
use crate::traversal::storage::observe::Listed;
use crate::traversal::storage::output::Output;
use crate::traversal::storage::stream::Child;
use crate::traversal::storage::{Shared, resolve};

impl Cursor {
    pub(in crate::traversal::storage) fn remember_terminal_errors(
        &mut self,
        shared: &Shared,
        output: &mut Output,
    ) {
        for frame in &mut self.stack {
            for child in frame.children.drain(..) {
                remember_child(shared, child, output);
            }
            if let Some((_, listing)) = frame.listing.as_mut() {
                remember_listing(shared, &frame.work.path, listing, output);
            }
        }
        for (path, id) in &self.by_path {
            if let Some(listing) = self.started.get_mut(id) {
                remember_listing(shared, path, listing, output);
            }
        }
    }
}

fn remember_listing(
    shared: &Shared,
    directory: &StoragePath,
    listing: &mut Listing,
    output: &mut Output,
) {
    for batch in listing.pages.drain(..) {
        output.remember_terminal_failures(
            batch.failures.into_iter().map(ScanFailure::child).collect(),
        );
        for child in batch.children {
            remember_child(shared, child, output);
        }
    }
    if let Some(Ended::Failed(failure)) = listing.ended.take() {
        if failure.class() == FailureClass::NotFound
            && directory != &shared.request.root
            && listing.facts.listed == 0
        {
            return;
        }
        output.remember_terminal_failures(vec![ScanFailure::listing(
            directory.clone(),
            failure,
            listing.facts.listed,
        )]);
    }
}

fn remember_child(shared: &Shared, child: Child, output: &mut Output) {
    let result = match child {
        Child::Filtered(failures) => {
            output.remember_terminal_observations(failures);
            return;
        }
        Child::Observe {
            listed: Listed::Observed(result),
            ..
        } => result,
        Child::Observe {
            listed: Listed::Unobserved(_),
            ..
        } => return,
    };
    match resolve(shared, result, None) {
        Ok(resolved) => output.remember_terminal_item(resolved.item),
        Err(error) => output.remember_terminal_observations(error.observations),
    }
}
