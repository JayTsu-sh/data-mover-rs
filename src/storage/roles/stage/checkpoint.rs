//! Checkpoint bookkeeping; only the ordinary eligible Checkpointed engine arms it.

use std::ops::Range;
use std::sync::atomic::Ordering;
use std::sync::{MutexGuard, PoisonError};

use bytes::Bytes;

use super::PreparedStage;
use crate::storage::checkpoint::{
    CheckpointData, CheckpointError, CheckpointPhase, StageCheckpoint, insert_range,
};

impl PreparedStage {
    fn checkpoint_state(&self) -> MutexGuard<'_, StageCheckpoint> {
        self.checkpoint
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn restore_checkpoint(&self, encoded: Option<Bytes>) -> Result<(), CheckpointError> {
        let Some(encoded) = encoded else {
            return Ok(());
        };
        let data = CheckpointData::decode(&encoded)?;
        let mut state = self.checkpoint_state();
        state.confirmed.clone_from(&data.ranges);
        state.data = data;
        state.restored = true;
        Ok(())
    }

    pub(crate) fn restored_checkpoint(&self) -> Option<CheckpointData> {
        let state = self.checkpoint_state();
        state.restored.then(|| state.data.clone())
    }

    pub(crate) fn enable_checkpoint(&self, size: u64, hash: Bytes) {
        let mut state = self.checkpoint_state();
        state.enabled = true;
        state.data.size = size;
        state.data.hash = hash;
        self.checkpoint_active.store(true, Ordering::Release);
    }

    pub(crate) fn checkpoint_enabled(&self) -> bool {
        self.checkpoint_active.load(Ordering::Acquire)
    }

    pub(crate) fn checkpoint_size(&self) -> u64 {
        self.checkpoint_state().data.size
    }

    pub(crate) fn checkpoint_window_bytes(&self) -> usize {
        self.checkpoint_window.load(Ordering::Acquire)
    }

    pub(crate) fn set_checkpoint_window(&self, bytes: usize) {
        self.checkpoint_window
            .store(bytes.max(1), Ordering::Release);
    }

    pub(crate) fn checkpoint_snapshot(&self) -> Option<Bytes> {
        let state = self.checkpoint_state();
        (state.enabled || state.restored).then(|| state.data.encode())
    }

    pub(crate) fn set_source_hash(&self, hash: Bytes) {
        let mut state = self.checkpoint_state();
        state.data.generation = state.data.generation.saturating_add(1);
        state.data.hash = hash;
    }

    pub(crate) fn checkpoint_ranges(&self) -> Vec<Range<u64>> {
        self.checkpoint_state().data.ranges.clone()
    }

    pub(crate) fn checkpoint_prefix(&self) -> u64 {
        self.checkpoint_state().data.prefix()
    }

    pub(crate) fn checkpoint_barrier(&self, prefix: u64) {
        let mut state = self.checkpoint_state();
        if state.enabled {
            let StageCheckpoint {
                data, confirmed, ..
            } = &mut *state;
            data.ranges.clone_from(confirmed);
            insert_range(&mut state.data.ranges, 0..prefix);
        }
    }

    pub(crate) fn checkpoint_confirmed_prefix(&self, prefix: u64) {
        let mut state = self.checkpoint_state();
        if state.enabled {
            state.data.ranges = if prefix == 0 {
                Vec::new()
            } else {
                std::iter::once(0..prefix).collect()
            };
        }
    }

    pub(crate) fn checkpoint_confirmed(&self, offset: u64, bytes: u64) {
        if !self.checkpoint_enabled() {
            return;
        }
        let mut state = self.checkpoint_state();
        if state.enabled
            && let Some(end) = offset.checked_add(bytes)
        {
            insert_range(&mut state.confirmed, offset..end);
        }
    }

    pub(crate) fn checkpoint_phase(&self) -> CheckpointPhase {
        self.checkpoint_state().data.phase
    }

    pub(crate) fn checkpoint_set_phase(&self, phase: CheckpointPhase) {
        self.checkpoint_state().data.phase = phase;
    }

    pub(crate) fn checkpoint_metadata_binding(&self) -> Option<[u8; 32]> {
        self.checkpoint_state().data.metadata_binding
    }

    /// A recovered final must retain its completed metadata plan. A surviving stage instead
    /// re-enters Receiving before any producer persistence and reapplies the current plan.
    pub(crate) fn checkpoint_bind_metadata(
        &self,
        binding: [u8; 32],
    ) -> Result<(), CheckpointError> {
        let mut state = self.checkpoint_state();
        if self.recovered_publication.is_some() {
            if state.data.phase == CheckpointPhase::Receiving
                || state.data.metadata_binding != Some(binding)
            {
                return Err(CheckpointError);
            }
        } else {
            state.data.phase = CheckpointPhase::Receiving;
        }
        state.data.metadata_binding = Some(binding);
        Ok(())
    }

    pub(crate) async fn checkpoint_guard(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.checkpoint_io.lock().await
    }
}
