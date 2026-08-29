//! Streaming transfer requests, recovery policy, state, and outcomes.

#[allow(dead_code)]
mod engine;
mod model;

pub use crate::storage::ExistingDestinationPolicy;
pub use engine::{TransferFailure, TransferOutcome, transfer};
pub use model::{InflightLimits, TransferIdentity, TransferRequest, TransferValueError};

#[cfg(test)]
mod tests;
