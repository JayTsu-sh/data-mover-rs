#[cfg(any(test, feature = "test-support"))]
use std::collections::HashMap;
use std::io;
#[cfg(any(test, feature = "test-support"))]
use std::sync::atomic::Ordering;
#[cfg(any(test, feature = "test-support"))]
use std::time::Duration;

#[derive(Default)]
pub(super) struct WriteProbe {
    #[cfg(any(test, feature = "test-support"))]
    pub(super) checkpoint_prefixes: std::sync::Mutex<Vec<u64>>,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) final_data_sync_calls: std::sync::atomic::AtomicU64,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) final_directory_sync_calls: std::sync::atomic::AtomicU64,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) metadata_batch_calls: std::sync::atomic::AtomicU64,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) metadata_sync_calls: std::sync::atomic::AtomicU64,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) pause_checkpoint: std::sync::atomic::AtomicBool,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) checkpoint_started: tokio::sync::Notify,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) checkpoint_release: tokio::sync::Notify,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) automatic_interval: std::sync::atomic::AtomicU64,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) delays: std::sync::Mutex<HashMap<u64, Duration>>,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) completion_order: std::sync::Mutex<Vec<u64>>,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) force_out_of_order: std::sync::atomic::AtomicBool,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) later_write_started: std::sync::atomic::AtomicBool,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) checkpoint_failure: std::sync::atomic::AtomicU64,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) corrupt_before_verify: std::sync::atomic::AtomicBool,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) fail_after_publication_commit: std::sync::atomic::AtomicBool,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) fail_metadata_sync: std::sync::atomic::AtomicBool,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) replace_final_during_skip: std::sync::atomic::AtomicBool,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) slow_existing_verify: std::sync::atomic::AtomicBool,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) existing_verify_started: std::sync::atomic::AtomicBool,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) slow_discard_before_release: std::sync::atomic::AtomicBool,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) discard_contents_removed: std::sync::atomic::AtomicBool,
}

impl WriteProbe {
    #[cfg_attr(not(test), allow(clippy::unnecessary_wraps, clippy::unused_self))]
    pub(super) fn fail_checkpoint_at(&self, point: u64) -> io::Result<()> {
        #[cfg(any(test, feature = "test-support"))]
        if self.checkpoint_failure.load(Ordering::SeqCst) == point {
            return Err(io::Error::other("injected checkpoint failure"));
        }
        #[cfg(not(any(test, feature = "test-support")))]
        let _ = point;
        Ok(())
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    pub(super) fn before_write(&self, offset: u64) {
        #[cfg(any(test, feature = "test-support"))]
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
        #[cfg(any(test, feature = "test-support"))]
        if let Some(delay) = self
            .delays
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&offset)
            .copied()
        {
            std::thread::sleep(delay);
        }
        #[cfg(not(any(test, feature = "test-support")))]
        let _ = offset;
    }

    /// Counts one metadata batch, and — when `durable` — the barrier it is due to end with. This
    /// counts intent: the barrier is skipped when the batch stops at a refusal or is cancelled, so
    /// only an injected barrier failure proves that one ran.
    #[cfg_attr(not(test), allow(clippy::unused_self))]
    pub(super) fn record_metadata_batch(&self, durable: bool) {
        #[cfg(any(test, feature = "test-support"))]
        {
            self.metadata_batch_calls.fetch_add(1, Ordering::SeqCst);
            if durable {
                self.metadata_sync_calls.fetch_add(1, Ordering::SeqCst);
            }
        }
        #[cfg(not(any(test, feature = "test-support")))]
        let _ = durable;
    }

    #[cfg_attr(not(test), allow(clippy::unused_self))]
    pub(super) fn after_write(&self, offset: u64) {
        #[cfg(any(test, feature = "test-support"))]
        self.completion_order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(offset);
        #[cfg(not(any(test, feature = "test-support")))]
        let _ = offset;
    }
}

#[cfg(any(test, feature = "test-support"))]
impl super::LocalStagedDestination {
    #[must_use]
    pub fn final_sync_counts(&self) -> (u64, u64) {
        (
            self.write_probe
                .final_data_sync_calls
                .load(Ordering::SeqCst),
            self.write_probe
                .final_directory_sync_calls
                .load(Ordering::SeqCst),
        )
    }

    #[must_use]
    pub fn metadata_batch_counts(&self) -> (u64, u64) {
        (
            self.write_probe.metadata_batch_calls.load(Ordering::SeqCst),
            self.write_probe.metadata_sync_calls.load(Ordering::SeqCst),
        )
    }
    pub fn corrupt_before_verify(&self) {
        self.write_probe
            .corrupt_before_verify
            .store(true, Ordering::SeqCst);
    }

    /// Fails the persistence barrier of every later metadata batch, after its mutations applied.
    pub fn fail_metadata_sync(&self) {
        self.write_probe
            .fail_metadata_sync
            .store(true, Ordering::SeqCst);
    }

    pub fn fail_after_publication_commit(&self) {
        self.write_probe
            .fail_after_publication_commit
            .store(true, Ordering::SeqCst);
    }

    pub fn replace_final_during_skip(&self) {
        self.write_probe
            .replace_final_during_skip
            .store(true, Ordering::SeqCst);
    }

    pub fn slow_existing_verify(&self) {
        self.write_probe
            .slow_existing_verify
            .store(true, Ordering::SeqCst);
    }

    #[must_use]
    pub fn existing_verify_started(&self) -> bool {
        self.write_probe
            .existing_verify_started
            .load(Ordering::SeqCst)
    }

    pub fn write_completion_count(&self) -> usize {
        self.write_probe
            .completion_order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    pub fn slow_discard_before_release(&self) {
        self.write_probe
            .slow_discard_before_release
            .store(true, Ordering::SeqCst);
    }

    #[must_use]
    pub fn discard_contents_removed(&self) -> bool {
        self.write_probe
            .discard_contents_removed
            .load(Ordering::SeqCst)
    }
}
