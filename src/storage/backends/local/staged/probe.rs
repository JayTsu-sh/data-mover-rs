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
    pub(super) replace_final_during_skip: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(super) slow_existing_verify: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(super) existing_verify_started: std::sync::atomic::AtomicBool,
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
