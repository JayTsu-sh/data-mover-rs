#[cfg(test)]
use std::collections::HashMap;
use std::io;
#[cfg(test)]
use std::sync::atomic::Ordering;
#[cfg(test)]
use std::time::Duration;

#[derive(Default)]
pub(super) struct WriteProbe {
    #[cfg(test)]
    pub(super) checkpoint_prefixes: std::sync::Mutex<Vec<u64>>,
    #[cfg(test)]
    pub(super) final_data_sync_calls: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    pub(super) final_directory_sync_calls: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    pub(super) metadata_batch_calls: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    pub(super) metadata_sync_calls: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    pub(super) pause_checkpoint: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(super) checkpoint_started: tokio::sync::Notify,
    #[cfg(test)]
    pub(super) checkpoint_release: tokio::sync::Notify,
    #[cfg(test)]
    pub(super) automatic_interval: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    pub(super) delays: std::sync::Mutex<HashMap<u64, Duration>>,
    #[cfg(test)]
    pub(super) completion_order: std::sync::Mutex<Vec<u64>>,
    #[cfg(test)]
    pub(super) force_out_of_order: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(super) later_write_started: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(super) checkpoint_failure: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    pub(super) corrupt_before_verify: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(super) fail_after_publication_commit: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(super) fail_metadata_sync: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(super) replace_final_during_skip: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(super) slow_existing_verify: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(super) existing_verify_started: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(super) slow_discard_before_release: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(super) discard_contents_removed: std::sync::atomic::AtomicBool,
}

impl WriteProbe {
    #[cfg_attr(not(test), allow(clippy::unnecessary_wraps, clippy::unused_self))]
    pub(super) fn fail_checkpoint_at(&self, point: u64) -> io::Result<()> {
        #[cfg(test)]
        if self.checkpoint_failure.load(Ordering::SeqCst) == point {
            return Err(io::Error::other("injected checkpoint failure"));
        }
        #[cfg(not(test))]
        let _ = point;
        Ok(())
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    pub(super) fn before_write(&self, offset: u64) {
        #[cfg(test)]
        if offset == 0 && self.force_out_of_order.load(Ordering::SeqCst) {
            while !self.later_write_started.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
            while self
                .completion_order
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
            {
                std::thread::yield_now();
            }
        } else if offset > 0 {
            self.later_write_started.store(true, Ordering::SeqCst);
        }
        #[cfg(test)]
        if let Some(delay) = self
            .delays
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&offset)
            .copied()
        {
            std::thread::sleep(delay);
        }
        #[cfg(not(test))]
        let _ = offset;
    }

    /// Counts one metadata batch, and — when `durable` — the barrier it is due to end with. This
    /// counts intent: the barrier is skipped when nothing was applied or the batch was cancelled,
    /// so only an injected barrier failure proves that one ran.
    #[cfg_attr(not(test), allow(clippy::unused_self))]
    pub(super) fn record_metadata_batch(&self, durable: bool) {
        #[cfg(test)]
        {
            self.metadata_batch_calls.fetch_add(1, Ordering::SeqCst);
            if durable {
                self.metadata_sync_calls.fetch_add(1, Ordering::SeqCst);
            }
        }
        #[cfg(not(test))]
        let _ = durable;
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    pub(super) fn after_write(&self, offset: u64) {
        #[cfg(test)]
        self.completion_order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(offset);
        #[cfg(not(test))]
        let _ = offset;
    }
}

#[cfg(test)]
impl super::LocalStagedDestination {
    pub(crate) fn final_sync_counts(&self) -> (u64, u64) {
        (
            self.write_probe
                .final_data_sync_calls
                .load(Ordering::SeqCst),
            self.write_probe
                .final_directory_sync_calls
                .load(Ordering::SeqCst),
        )
    }

    pub(crate) fn metadata_batch_counts(&self) -> (u64, u64) {
        (
            self.write_probe.metadata_batch_calls.load(Ordering::SeqCst),
            self.write_probe.metadata_sync_calls.load(Ordering::SeqCst),
        )
    }
    pub(crate) fn corrupt_before_verify(&self) {
        self.write_probe
            .corrupt_before_verify
            .store(true, Ordering::SeqCst);
    }

    /// Fails the persistence barrier of every later metadata batch, after its mutations applied.
    pub(crate) fn fail_metadata_sync(&self) {
        self.write_probe
            .fail_metadata_sync
            .store(true, Ordering::SeqCst);
    }

    pub(crate) fn fail_after_publication_commit(&self) {
        self.write_probe
            .fail_after_publication_commit
            .store(true, Ordering::SeqCst);
    }

    pub(crate) fn replace_final_during_skip(&self) {
        self.write_probe
            .replace_final_during_skip
            .store(true, Ordering::SeqCst);
    }

    pub(crate) fn slow_existing_verify(&self) {
        self.write_probe
            .slow_existing_verify
            .store(true, Ordering::SeqCst);
    }

    pub(crate) fn existing_verify_started(&self) -> bool {
        self.write_probe
            .existing_verify_started
            .load(Ordering::SeqCst)
    }

    pub(crate) fn write_completion_count(&self) -> usize {
        self.write_probe
            .completion_order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    pub(crate) fn slow_discard_before_release(&self) {
        self.write_probe
            .slow_discard_before_release
            .store(true, Ordering::SeqCst);
    }

    pub(crate) fn discard_contents_removed(&self) -> bool {
        self.write_probe
            .discard_contents_removed
            .load(Ordering::SeqCst)
    }
}
