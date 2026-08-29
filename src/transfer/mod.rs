//! Streaming transfer requests, recovery policy, state, and outcomes.

#[allow(dead_code)]
mod engine;
mod model;

pub use crate::storage::ExistingDestinationPolicy;
pub use crate::storage::RecoveryIdentity;
pub use engine::{TransferFailure, TransferOutcome, transfer};
pub use model::{
    InflightLimits, RecoveryPolicy, TransferIdentity, TransferRequest, TransferValueError,
};

#[cfg(test)]
mod tests;
