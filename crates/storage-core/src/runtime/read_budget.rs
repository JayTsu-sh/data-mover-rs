//! Optional admission retained for low-level source requests.
use super::inflight::{InflightAdmission, InflightFailure, InflightRuntime};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Explicit admission context for low-level sources that prefetch payloads.
/// Ordinary transfers use backend windows and a bounded channel instead.
/// Reservations survive source completion until the ordered consumer takes the chunk.
#[derive(Clone)]
pub struct ReadBudget {
    runtime: InflightRuntime,
    ready: Arc<Mutex<BTreeMap<u64, InflightAdmission>>>,
}

impl std::fmt::Debug for ReadBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadBudget").finish_non_exhaustive()
    }
}

impl ReadBudget {
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn new(runtime: InflightRuntime) -> Self {
        Self {
            runtime,
            ready: Arc::default(),
        }
    }

    pub async fn reserve(
        &self,
        length: usize,
        wait: bool,
    ) -> Result<Option<InflightAdmission>, InflightFailure> {
        self.runtime.reserve_read(length, wait).await
    }

    pub fn ready(&self, offset: u64, admission: InflightAdmission) {
        self.ready
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(offset, admission);
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn take(&self, offset: u64) -> Option<InflightAdmission> {
        self.ready
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&offset)
    }
}
