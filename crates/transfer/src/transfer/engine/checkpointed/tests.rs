#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(clippy::cast_possible_truncation, clippy::single_range_in_vec_init)]

mod entry;
mod mocks;
mod producer;
mod settling;

use std::{ops::Range, sync::Mutex, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::{BackendIdentity, EntryKind, IdentityStrength, SourceIdentity, StoragePath};
use crate::storage::checkpoint::{CheckpointData, CheckpointPhase};
use crate::storage::range_hash::RangeHash;
use crate::storage::{
    ByteStream, DestinationPrepareRequest, FinalDestination, PositionedByteStream, PositionedChunk,
    PublicationEvidence, PublicationFailure, PublishRequest, ReadRequest, StorageRoleFailure,
    VerificationEvidence, VerifyRequest,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn fixture(size: usize) -> (mocks::Source, mocks::Destination, PreparedStage) {
    let owner = BackendIdentity::new("local".parse().unwrap(), "checkpointed-test").unwrap();
    let path = StoragePath::new("source").unwrap();
    let descriptor = SourceDescriptor::new(
        path,
        EntryKind::File,
        Some(size as u64),
        SourceIdentity::new(owner.clone(), IdentityStrength::PathScoped, b"source").unwrap(),
    );
    let source = mocks::Source::new(
        descriptor,
        Bytes::from((0..size).map(|i| (i % 251) as u8).collect::<Vec<_>>()),
    );
    let stage = PreparedStage::new(
        owner,
        FinalDestination::new(StoragePath::new("target").unwrap()),
        Bytes::new(),
        [1; 32],
        0,
        None,
    );
    (source, mocks::Destination::default(), stage)
}

fn restore(stage: &PreparedStage, source: &mocks::Source, h: &[Range<usize>], d: Vec<Range<u64>>) {
    let mut hash = RangeHash::new(source.data.len() as u64);
    for range in h {
        hash.observe(range.start as u64, &source.data[range.clone()])
            .unwrap();
    }
    let data = CheckpointData {
        size: source.data.len() as u64,
        hash: Bytes::from(hash.encode().unwrap()),
        ranges: d,
        ..CheckpointData::default()
    };
    stage.restore_checkpoint(Some(data.encode())).unwrap();
}

fn context<'a>(
    source: &'a mocks::Source,
    destination: &'a mocks::Destination,
    stage: &'a PreparedStage,
    cancel: &'a CancellationToken,
) -> Context<'a> {
    Context::new(
        cancel,
        source,
        destination,
        &source.descriptor,
        stage,
        source.data.len() as u64,
        None,
    )
    .unwrap()
}

fn request() -> TransferRequest {
    use crate::storage::backends::local::{test_destination_storage, test_source_storage};
    let root = std::path::Path::new("/tmp");
    let (source, _) = test_source_storage(root, "checkpointed-test-source").unwrap();
    let destination = test_destination_storage(root, "checkpointed-test-destination").unwrap();
    TransferRequest::new(
        source,
        StoragePath::new("source").unwrap(),
        destination,
        CancellationToken::new(),
    )
    .with_transfer_policy(TransferPolicy::Checkpointed)
}

fn plan(size: u64) -> TransferPlan {
    TransferPlan {
        data_path: super::super::TransferDataPath::Streaming,
        source_size: size,
        chunk_bytes: 4,
        recovery_enabled: true,
        effective_recovery: super::super::EffectiveRecovery::Checkpointed,
        automatic_interval: None,
    }
}
