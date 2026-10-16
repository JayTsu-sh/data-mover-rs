//! Shared admission for backend prefetch and the ordered producer queue.
use super::inflight::{InflightAdmission, InflightFailure, InflightRuntime};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Transfer-owned admission context for sources that prefetch payloads.
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
    pub(crate) fn new(runtime: InflightRuntime) -> Self {
        Self {
            runtime,
            ready: Arc::default(),
        }
    }

    pub(crate) async fn reserve(
        &self,
        length: usize,
        wait: bool,
    ) -> Result<Option<InflightAdmission>, InflightFailure> {
        self.runtime.reserve_read(length, wait).await
    }

    pub(crate) fn ready(&self, offset: u64, admission: InflightAdmission) {
        self.ready
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(offset, admission);
    }

    pub(crate) fn take(&self, offset: u64) -> Option<InflightAdmission> {
        self.ready
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&offset)
    }
}
