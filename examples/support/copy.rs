//! Shared role-based validation for copy and resume examples.

use std::error::Error;

use data_mover::integrity::{IntegrityMode, IntegrityOptions, IntegrityRequest, compare};
use data_mover::model::{
    BackendKind, MetadataObservation, ObservationMode, ObservationPlan, ObservedEntry, StoragePath,
};
use data_mover::storage::ReadRequest;
use data_mover::storage::{Storage, create_directory_all};
use data_mover::transfer::InflightLimits;
use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

pub type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;

pub fn limits(chunk_bytes: u64) -> Result<InflightLimits> {
    let chunk = usize::try_from(chunk_bytes)?;
    Ok(InflightLimits::new(
        8,
        chunk.checked_mul(8).ok_or("inflight size overflow")?,
        8,
    )?)
}

pub async fn create_parent(storage: &Storage, path: &StoragePath) -> Result {
    if storage.kind() != BackendKind::S3
        && let Some((parent, _)) = path.as_str().rsplit_once('/')
        && !parent.is_empty()
    {
        create_directory_all(storage, &StoragePath::new(parent)?).await?;
    }
    Ok(())
}

pub async fn verify(source: &Storage, destination: &Storage, path: &StoragePath) -> Result<u64> {
    let report = compare(IntegrityRequest {
        source: source.clone(),
        source_path: path.clone(),
        destination: destination.clone(),
        destination_path: path.clone(),
        options: IntegrityOptions {
            mode: IntegrityMode::Content,
            permissions: true,
            ..IntegrityOptions::default()
        },
        cancel: CancellationToken::new(),
    })
    .await?;
    if !report.matches() {
        return Err(format!("copy verification differs: {:?}", report.differences()).into());
    }
    Ok(report.compared_bytes())
}

/// Records public source identity, size and modification time for the example's state guard.
pub async fn observe(source: &Storage, path: &StoragePath) -> Result<ObservedEntry> {
    let descriptor = source.read_source()?.describe(path).await?;
    let observations = source
        .metadata()?
        .observe_bound(
            path,
            &descriptor.source_identity,
            ObservationPlan::default().with_timestamps(ObservationMode::BestEffort),
        )
        .await?;
    let modified = match observations.timestamps() {
        MetadataObservation::Value { value, .. } => value.modified,
        MetadataObservation::Failed { .. } => {
            return Err("source timestamp observation failed".into());
        }
        _ => None,
    };
    Ok(ObservedEntry::new(
        descriptor.path,
        descriptor.kind,
        descriptor.size,
        modified,
        descriptor.source_identity,
    )?)
}

pub async fn hash(source: &Storage, path: &StoragePath) -> Result<(u64, String)> {
    let reader = source.read_source()?;
    let descriptor = reader.describe(path).await?;
    let expected = descriptor.size.ok_or("source size is unknown")?;
    let mut stream = reader
        .read(ReadRequest {
            path: path.clone(),
            range: Some(0..expected),
            expected_source: Some(descriptor.source_identity.clone()),
            maximum_chunk_bytes: 2 << 20,
            read_inflight: 4,
            read_budget: None,
            cancel: CancellationToken::new(),
            source_qos: None,
            version: descriptor.version().clone(),
        })
        .await?;
    let mut hasher = blake3::Hasher::new();
    let mut read = 0_u64;
    while let Some(chunk) = stream.next().await {
        let bytes = chunk?;
        read = read
            .checked_add(u64::try_from(bytes.len())?)
            .ok_or("read size overflow")?;
        hasher.update(&bytes);
    }
    if read != expected {
        return Err("source read length differs from described size".into());
    }
    Ok((read, hasher.finalize().to_hex().to_string()))
}
