//! Reading a whole listing into memory, for single-directory callers (recursive delete, the NDX
//! walk) and tests.

use std::num::NonZeroUsize;

use futures::StreamExt as _;

use super::{ListingPage, ListingStream, listing_failure};
use crate::model::{FailureClass, Transience};
use crate::storage::StorageRoleFailure;

/// Bounds on what [`collect_listing`] holds in memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CollectLimits {
    /// Most children and child failures one collected listing may hold; `None` is unbounded.
    pub max_children: Option<NonZeroUsize>,
}

impl CollectLimits {
    /// No bound: the whole directory is held, however large.
    pub const UNBOUNDED: Self = Self { max_children: None };

    /// At most `max_children` children and child failures.
    #[must_use]
    pub const fn at_most(max_children: NonZeroUsize) -> Self {
        Self {
            max_children: Some(max_children),
        }
    }
}

/// Reads a whole listing into one page: every child, every failure, the hidden count summed.
///
/// # Errors
/// Returns the listing's own failure, or a `Capacity` entry failure at the listed directory once
/// it holds more than `limits` allows; the stream is dropped then, which ends its producer.
pub async fn collect_listing(
    mut stream: ListingStream,
    limits: CollectLimits,
) -> Result<ListingPage, StorageRoleFailure> {
    let mut whole = ListingPage::default();
    while let Some(page) = stream.next().await {
        whole.absorb(page?);
        if let Some(max) = limits.max_children
            && whole.children.len() + whole.failures.len() > max.get()
        {
            return Err(StorageRoleFailure::Entry(listing_failure(
                stream.path(),
                FailureClass::Capacity,
                Transience::Permanent,
                "the directory holds more children than this caller collects",
            )));
        }
    }
    Ok(whole)
}
