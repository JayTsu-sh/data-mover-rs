//! Streaming transfer requests, recovery policy, state, and outcomes.

#[allow(dead_code)]
mod engine;
mod model;

pub use crate::storage::ExistingDestinationPolicy;
pub use crate::storage::RecoveryIdentity;
pub use crate::storage::{SourceQosGroup, SourceQosPolicy, SourceQosStats, SourceQosValueError};
pub use engine::{TransferFailure, TransferOutcome, transfer};
pub use model::{
    InflightLimits, PayloadShapingPolicy, RecoveryPolicy, TransferIdentity, TransferRequest,
    TransferValueError,
};

#[cfg(test)]
mod hdfs_tests;
#[cfg(test)]
mod s3_native_tests;
#[cfg(test)]
mod tests;
