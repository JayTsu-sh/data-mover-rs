//! Source `QoS` applied to the real S3 range-read pipeline.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

use super::{SourceQosGroup, SourceQosPolicy};
use crate::model::{SourceVersion, StoragePath};
use crate::storage::ReadRequest;
use crate::storage::backends::s3::{
    connect,
    tests::{MemoryS3, identity},
};

#[tokio::test(start_paused = true)]
async fn qos_caps_real_gets_and_paces_even_low_iops_limits()
-> Result<(), Box<dyn std::error::Error>> {
    const SIZE: usize = 16 * 1024 * 1024 + 137;
    for (cap, bandwidth, soft, hard, expected) in [
        (64 * 1024, None, 2, 2, 257),
        (64 * 1024, None, 2, 4, 257),
        (64 * 1024, None, 128, 128, 257),
        (8 * 1024 * 1024, None, 2, 2, 3),
        (8 * 1024 * 1024, Some(1024 * 1024), 2, 2, 3),
        (8 * 1024 * 1024, Some(8 * 1024 * 1024), 2, 2, 3),
        (8 * 1024 * 1024, Some(32 * 1024 * 1024), 2, 2, 3),
    ] {
        let protocol = Arc::new(MemoryS3::default());
        protocol
            .objects
            .lock()
            .await
            .insert("source".into(), Bytes::from(vec![42; SIZE]));
        let storage = connect(protocol.clone(), identity(), None)?;
        let source = storage.read_source()?;
        let descriptor = source.describe(&StoragePath::new("source")?).await?;
        let budget = SourceQosGroup::new(SourceQosPolicy::new(
            bandwidth.map(|rate| (rate, rate, Duration::ZERO)),
            cap,
            Some((soft, hard, Duration::from_millis(500))),
        )?)
        .transfer_budget();
        let started = tokio::time::Instant::now();
        let mut stream = source
            .read(ReadRequest {
                path: descriptor.path,
                range: Some(0..SIZE as u64),
                expected_source: Some(descriptor.source_identity),
                maximum_chunk_bytes: 8 * 1024 * 1024,
                read_inflight: 2,
                read_budget: None,
                cancel: CancellationToken::new(),
                source_qos: Some(budget.clone()),
                version: SourceVersion::Current,
            })
            .await?;
        let mut bytes = 0;
        let mut operations = 0_u32;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            assert!(chunk.len() as u64 <= cap);
            bytes += chunk.len();
            operations += 1;
            // Every real GET requires its own hard-rate slot, including the first.
            assert!(started.elapsed().as_secs_f64() >= f64::from(operations) / f64::from(hard));
            if let Some(rate) = bandwidth {
                assert!(
                    started.elapsed().as_nanos() * u128::from(rate)
                        >= bytes as u128 * 1_000_000_000
                );
            }
        }
        assert_eq!(bytes, SIZE);
        assert_eq!(operations, expected);
        assert_eq!(
            u64::try_from(protocol.range_observations.lock().await.len())?,
            u64::from(expected)
        );
        assert_eq!(budget.stats().source_read_operations, u64::from(expected));
        assert_eq!(budget.stats().client_streamed_shaped_bytes, SIZE as u64);
        println!(
            "cap={cap} bandwidth={bandwidth:?} iops={soft}/{hard} gets={operations} elapsed={:?}",
            started.elapsed()
        );
    }
    Ok(())
}
