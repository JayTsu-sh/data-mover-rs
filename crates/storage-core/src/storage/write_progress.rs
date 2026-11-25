//! Notifications from destination write, recovery and publication boundaries.

use std::{fmt, sync::Arc};

/// Destination progress, never caller-owned recovery authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteProgress {
    /// A complete write or multipart-part operation succeeded. Ranges may arrive out of order.
    /// This does not imply a contiguous prefix, crash durability, verification or publication.
    Confirmed { offset: u64, bytes: u64 },
    /// The destination has recorded this contiguous prefix for recovery. Notifications may
    /// repeat. A later process must still rediscover and validate destination-owned state.
    Checkpoint { durable_prefix: u64 },
    /// An ordinary transfer confirmed publication. Only bytes written by this request are
    /// counted. Later verification and cleanup can still fail.
    Published { newly_written_bytes: u64 },
}

/// A cheap optional destination callback. It may run concurrently on runtime or blocking
/// threads; it must return promptly and must not panic. Forward to a bounded/coalescing UI
/// channel if processing requires I/O. It cannot accept/reject writes or supply resume state.
#[derive(Clone)]
pub struct WriteProgressObserver(Arc<dyn Fn(WriteProgress) + Send + Sync>);

impl WriteProgressObserver {
    #[must_use]
    pub fn new(callback: impl Fn(WriteProgress) + Send + Sync + 'static) -> Self {
        Self(Arc::new(callback))
    }

    pub fn confirmed(&self, offset: u64, bytes: u64) {
        (self.0)(WriteProgress::Confirmed { offset, bytes });
    }

    pub fn checkpoint(&self, durable_prefix: u64) {
        (self.0)(WriteProgress::Checkpoint { durable_prefix });
    }

    pub fn published(&self, newly_written_bytes: u64) {
        (self.0)(WriteProgress::Published {
            newly_written_bytes,
        });
    }
}

impl fmt::Debug for WriteProgressObserver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WriteProgressObserver(..)")
    }
}
