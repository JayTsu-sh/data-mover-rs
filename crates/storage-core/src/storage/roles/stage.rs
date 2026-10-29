//! The prepared destination stage and its deferred checkpoint.

use std::fmt;

use bytes::Bytes;

use super::FinalDestination;
use crate::storage::PrepareFact;

mod checkpoint;

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
    pub(crate) write_observer: Option<crate::storage::WriteProgressObserver>,
    pub(crate) checkpoint: std::sync::Mutex<crate::storage::checkpoint::StageCheckpoint>,
    pub(crate) checkpoint_io: tokio::sync::Mutex<()>,
    pub(crate) checkpoint_window: std::sync::atomic::AtomicUsize,
    pub(crate) checkpoint_active: std::sync::atomic::AtomicBool,
    /// A retained hash-ready root reconciled against the final name after publication.
    pub(crate) recovered_publication: Option<super::PublicationEvidence>,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StageBindingError;

#[allow(dead_code)]
impl PreparedStage {
    pub fn new(
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
            write_observer: None,
            checkpoint: std::sync::Mutex::default(),
            checkpoint_io: tokio::sync::Mutex::new(()),
            checkpoint_window: std::sync::atomic::AtomicUsize::new(32 * 1024 * 1024),
            checkpoint_active: std::sync::atomic::AtomicBool::new(false),
            recovered_publication: None,
        }
    }

    /// The final name bound when this stage was prepared.
    #[must_use]
    pub fn final_destination(&self) -> &FinalDestination {
        &self.final_destination
    }
    /// The opaque adapter token identifying this prepared state.
    #[must_use]
    pub fn token(&self) -> &Bytes {
        &self.token
    }
    /// The prefix from which sequential writing resumes.
    #[must_use]
    pub const fn write_offset(&self) -> u64 {
        self.write_offset
    }
    /// Whether publication must perform persistence barriers.
    #[must_use]
    pub fn durable_publication(&self) -> bool {
        self.durable_publication
    }
    /// Whether writes target the visible final name directly.
    #[must_use]
    pub const fn direct(&self) -> bool {
        self.direct
    }
    /// Whether preparation used destination-owned recovery discovery.
    #[must_use]
    pub const fn at_destination(&self) -> bool {
        self.at_destination
    }
    /// The adapter checkpoint policy negotiated during preparation.
    #[must_use]
    pub fn deferred_checkpoint(&self) -> &Option<DeferredCheckpoint> {
        &self.deferred_checkpoint
    }
    /// Opaque adapter-owned state retained for this stage.
    #[must_use]
    pub fn backend_state(&self) -> &Option<std::sync::Arc<dyn std::any::Any + Send + Sync>> {
        &self.backend_state
    }
    /// The optional progress observer attached by the engine.
    #[must_use]
    pub fn write_observer(&self) -> &Option<crate::storage::WriteProgressObserver> {
        &self.write_observer
    }
    /// Publication evidence reconciled from a previous attempt.
    #[must_use]
    pub fn recovered_publication(&self) -> &Option<super::PublicationEvidence> {
        &self.recovered_publication
    }
    /// Record write offset while preparing the destination stage.
    pub fn set_write_offset(&mut self, value: u64) {
        self.write_offset = value;
    }
    /// Record deferred checkpoint while preparing the destination stage.
    pub fn set_deferred_checkpoint(&mut self, value: Option<DeferredCheckpoint>) {
        self.deferred_checkpoint = value;
    }
    /// Record durable publication while preparing the destination stage.
    pub fn set_durable_publication(&mut self, value: bool) {
        self.durable_publication = value;
    }
    /// Mark whether preparation targets the visible final name directly.
    pub fn set_direct(&mut self, value: bool) {
        self.direct = value;
    }
    /// Record backend state while preparing the destination stage.
    pub fn set_backend_state(
        &mut self,
        value: Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>,
    ) {
        self.backend_state = value;
    }
    /// Record prepare fact while preparing the destination stage.
    pub fn set_prepare_fact(&mut self, value: PrepareFact) {
        self.prepare_fact = value;
    }
    /// Retain the engine exclusivity lease until this stage is dropped.
    pub fn set_exclusive(&mut self, value: Option<Box<dyn std::any::Any + Send + Sync>>) {
        self.exclusive = value;
    }
    /// Record write observer while preparing the destination stage.
    pub fn set_write_observer(&mut self, value: Option<crate::storage::WriteProgressObserver>) {
        self.write_observer = value;
    }
    /// Record recovered publication while preparing the destination stage.
    pub fn set_recovered_publication(&mut self, value: Option<super::PublicationEvidence>) {
        self.recovered_publication = value;
    }
    /// Transfer ownership of the backend claim for fenced removal.
    pub fn take_claim(&self) -> Option<std::fs::File> {
        self.claim
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
    /// Whether the stage still owns its exclusive filesystem claim.
    #[must_use]
    pub fn holds_claim(&self) -> bool {
        self.claim
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }
    /// Clone the held file descriptor for a backend identity check.
    /// # Errors
    /// Propagates a file-descriptor duplication failure.
    pub fn clone_claim(&self) -> std::io::Result<Option<std::fs::File>> {
        self.claim
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(std::fs::File::try_clone)
            .transpose()
    }
    /// Rebind an opaque token only for negative adapter contract tests.
    #[cfg(any(test, feature = "test-support"))]
    pub fn test_set_token(&mut self, token: Bytes) {
        self.token = token;
    }
    /// Retain the backend's exclusive filesystem claim until the stage releases it.
    pub fn retain_claim(&self, claim: std::fs::File) {
        *self
            .claim
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(claim);
    }
    /// Enable recovery after the destination has established its durable state.
    pub fn set_recovery_enabled(&self, value: bool) {
        self.recovery_enabled
            .store(value, std::sync::atomic::Ordering::Release);
    }
    /// What prepare found at the destination and did about it: `Fresh`, `Resumed`, or
    /// `Restarted` with its reason.
    #[must_use]
    pub const fn prepare_fact(&self) -> PrepareFact {
        self.prepare_fact
    }

    pub fn confirmed_write(&self, offset: u64, bytes: u64) {
        self.checkpoint_confirmed(offset, bytes);
        if let Some(observer) = &self.write_observer {
            observer.confirmed(offset, bytes);
        }
    }

    pub fn checkpoint_recorded(&self, durable_prefix: u64) {
        if let Some(observer) = &self.write_observer {
            observer.checkpoint(durable_prefix);
        }
    }

    /// Marks a stage prepared at the destination, with what its prepare found.
    pub fn mark_at_destination(&mut self, fact: PrepareFact) {
        self.at_destination = true;
        self.prepare_fact = fact;
    }

    #[must_use]
    pub fn disable_recovery(self) -> Self {
        self.recovery_enabled
            .store(false, std::sync::atomic::Ordering::Release);
        self
    }

    pub fn recovery_enabled(&self) -> bool {
        self.recovery_enabled
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub const fn recovery_binding(&self) -> [u8; 32] {
        self.recovery_binding
    }

    pub fn validate_owner(
        &self,
        owner: &crate::model::BackendIdentity,
    ) -> Result<(), StageBindingError> {
        if &self.owner == owner {
            Ok(())
        } else {
            Err(StageBindingError)
        }
    }

    pub fn release_claim(&self) {
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
            .field("write_observer", &self.write_observer.is_some())
            .finish_non_exhaustive()
    }
}

/// Automatic checkpoint spacing the engine arms for a stage; the destination writes its own
/// pointer when a checkpoint is due.
pub struct DeferredCheckpoint {
    pub interval_bytes: u64,
    pub source_size: u64,
}
