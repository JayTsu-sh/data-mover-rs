//! The prepared destination stage and its deferred checkpoint.

use std::fmt;

use bytes::Bytes;

use super::FinalDestination;
use crate::storage::PrepareFact;

/// Opaque linear prepared destination state bound to one backend and final destination.
pub struct PreparedStage {
    pub(crate) owner: crate::model::BackendIdentity,
    pub(crate) final_destination: FinalDestination,
    pub(crate) token: Bytes,
    pub(crate) recovery_binding: [u8; 32],
    pub(crate) write_offset: u64,
    pub(crate) recovery_enabled: std::sync::atomic::AtomicBool,
    pub(crate) deferred_checkpoint: Option<DeferredCheckpoint>,
    /// Whether the caller requires final publication persistence barriers.
    pub(crate) durable_publication: bool,
    /// Direct targets are already visible and must never enter stage cleanup or recovery.
    pub(crate) direct: bool,
    pub(crate) backend_state: Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>,
    pub(crate) claim: std::sync::Mutex<Option<std::fs::File>>,
    /// What prepare found at the destination and did about it.
    pub(crate) prepare_fact: PrepareFact,
    /// Whether this stage was prepared at the destination (`prepare_at_destination`, or marked so
    /// by the engine for a direct write) rather than built by hand, as backend unit tests do.
    pub(crate) at_destination: bool,
    /// An exclusivity lease held for as long as the stage lives (the engine's per-key guard).
    pub(crate) exclusive: Option<Box<dyn std::any::Any + Send + Sync>>,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StageBindingError;

#[allow(dead_code)]
impl PreparedStage {
    pub(crate) fn new(
        owner: crate::model::BackendIdentity,
        final_destination: FinalDestination,
        token: Bytes,
        recovery_binding: [u8; 32],
        write_offset: u64,
        claim: Option<std::fs::File>,
    ) -> Self {
        Self {
            owner,
            final_destination,
            token,
            recovery_binding,
            write_offset,
            recovery_enabled: std::sync::atomic::AtomicBool::new(true),
            deferred_checkpoint: None,
            durable_publication: true,
            direct: false,
            backend_state: None,
            claim: std::sync::Mutex::new(claim),
            prepare_fact: PrepareFact::Fresh,
            at_destination: false,
            exclusive: None,
        }
    }

    /// What prepare found at the destination and did about it: `Fresh`, `Resumed`, or
    /// `Restarted` with its reason.
    #[must_use]
    pub const fn prepare_fact(&self) -> PrepareFact {
        self.prepare_fact
    }

    /// Marks a stage prepared at the destination, with what its prepare found.
    pub(crate) fn mark_at_destination(&mut self, fact: PrepareFact) {
        self.at_destination = true;
        self.prepare_fact = fact;
    }

    pub(crate) fn disable_recovery(self) -> Self {
        self.recovery_enabled
            .store(false, std::sync::atomic::Ordering::Release);
        self
    }

    pub(crate) fn recovery_enabled(&self) -> bool {
        self.recovery_enabled
            .load(std::sync::atomic::Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) const fn recovery_binding(&self) -> [u8; 32] {
        self.recovery_binding
    }

    pub(crate) fn validate_owner(
        &self,
        owner: &crate::model::BackendIdentity,
    ) -> Result<(), StageBindingError> {
        if &self.owner == owner {
            Ok(())
        } else {
            Err(StageBindingError)
        }
    }

    pub(crate) fn release_claim(&self) {
        self.claim
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
}

impl fmt::Debug for PreparedStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedStage")
            .field("owner", &self.owner)
            .field("final_destination", &self.final_destination)
            .field("token", &"<redacted>")
            .field("recovery_binding", &"<redacted>")
            .field("write_offset", &self.write_offset)
            .field("recovery_enabled", &self.recovery_enabled())
            .field("claim", &"<exclusive-lock>")
            .field("deferred_checkpoint", &self.deferred_checkpoint.is_some())
            .field("durable_publication", &self.durable_publication)
            .field("direct", &self.direct)
            .field("backend_state", &"<opaque>")
            .field("prepare_fact", &self.prepare_fact)
            .field("at_destination", &self.at_destination)
            .field("exclusive", &self.exclusive.is_some())
            .finish()
    }
}

/// Automatic checkpoint spacing the engine arms for a stage; the destination writes its own
/// pointer when a checkpoint is due.
pub(crate) struct DeferredCheckpoint {
    pub(crate) interval_bytes: u64,
    pub(crate) source_size: u64,
}
