//! The prepared destination stage and its deferred checkpoint registration.

use std::fmt;

use async_trait::async_trait;
use bytes::Bytes;

use super::{FinalDestination, RecoveryIdentity, StorageRoleFailure};

/// Opaque linear prepared destination state bound to one backend and final destination.
pub struct PreparedStage {
    pub(crate) owner: crate::model::BackendIdentity,
    pub(crate) final_destination: FinalDestination,
    pub(crate) token: Bytes,
    pub(crate) recovery_binding: [u8; 32],
    pub(crate) write_offset: u64,
    pub(crate) recovery_enabled: std::sync::atomic::AtomicBool,
    pub(crate) registration_owned: std::sync::atomic::AtomicBool,
    pub(crate) deferred_checkpoint: Option<DeferredCheckpoint>,
    /// Whether the caller requires final publication persistence barriers.
    pub(crate) durable_publication: bool,
    /// Direct targets are already visible and must never enter stage cleanup or recovery.
    pub(crate) direct: bool,
    pub(crate) backend_state: Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>,
    pub(crate) claim: std::sync::Mutex<Option<std::fs::File>>,
    pub(crate) recovery_lease: std::sync::Mutex<Option<std::sync::Arc<std::fs::File>>>,
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
            registration_owned: std::sync::atomic::AtomicBool::new(true),
            deferred_checkpoint: None,
            durable_publication: true,
            direct: false,
            backend_state: None,
            claim: std::sync::Mutex::new(claim),
            recovery_lease: std::sync::Mutex::new(None),
        }
    }

    pub(crate) fn disable_recovery(self) -> Self {
        self.recovery_enabled
            .store(false, std::sync::atomic::Ordering::Release);
        self.registration_owned
            .store(false, std::sync::atomic::Ordering::Release);
        self
    }

    pub(crate) fn recovery_enabled(&self) -> bool {
        self.recovery_enabled
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn owns_recovery_registration(&self) -> bool {
        self.registration_owned
            .load(std::sync::atomic::Ordering::Acquire)
    }

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

    pub(crate) fn retain_recovery_lease(&self, lease: std::sync::Arc<std::fs::File>) {
        *self
            .recovery_lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(lease);
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
            .field("recovery_lease", &"<exclusive-lock>")
            .field("registration_owned", &self.owns_recovery_registration())
            .field("deferred_checkpoint", &self.deferred_checkpoint.is_some())
            .field("durable_publication", &self.durable_publication)
            .field("direct", &self.direct)
            .field("backend_state", &"<opaque>")
            .finish()
    }
}

pub(crate) struct DeferredCheckpoint {
    pub(crate) interval_bytes: u64,
    pub(crate) source_size: u64,
    pub(crate) registration: std::sync::Arc<dyn CheckpointRegistration>,
}

#[async_trait]
pub(crate) trait CheckpointRegistration: Send + Sync {
    async fn register(
        &self,
        stage: &PreparedStage,
        identity: RecoveryIdentity,
    ) -> Result<(), StorageRoleFailure>;
}
