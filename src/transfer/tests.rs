use std::io;
use std::path::{Path, PathBuf};

use super::engine::{TransferDataPath, TransferPhase, TransferSide, run_until_transferred};
use super::{InflightLimits, TransferIdentity, TransferRequest};
use crate::model::StoragePath;
use crate::storage::Storage;
use crate::storage::backends::local::{
    source::LocalReadSource, test_destination_storage, test_source_storage,
    test_unsupported_storage,
};

#[test]
fn transfer_inputs_reject_ambiguous_identity_and_unbounded_limits() {
    assert!(TransferIdentity::new("").is_err());
    assert!(TransferIdentity::new("logical-copy-7").is_ok());
    assert!(InflightLimits::new(0, 64 * 1024, 1).is_err());
    assert!(InflightLimits::new(2, 0, 1).is_err());
    assert!(InflightLimits::new(2, 64 * 1024, 0).is_err());
    assert!(InflightLimits::new(2, 64 * 1024, 1).is_ok());
}

#[tokio::test]
async fn local_transfer_reaches_durable_unpublished_state() -> Result<(), Box<dyn std::error::Error>>
{
    let source_root = TestRoot::new("source")?;
    let destination_root = TestRoot::new("destination")?;
    let payload = vec![0x5a; 160 * 1024 + 17];
    std::fs::write(source_root.path().join("source.bin"), &payload)?;
    std::fs::write(destination_root.path().join("final.bin"), b"old-final")?;
    let source = local_source(source_root.path())?;
    let destination = local_destination(destination_root.path())?;
    let request = TransferRequest::new(
        TransferIdentity::new("local-copy")?,
        source,
        StoragePath::new("source.bin")?,
        destination,
        StoragePath::new("final.bin")?,
        InflightLimits::new(2, 64 * 1024, 2)?,
        tokio_util::sync::CancellationToken::new(),
    );

    let transferred = run_until_transferred(request).await?;

    assert_eq!(transferred.data_path(), TransferDataPath::Streaming);
    assert_eq!(transferred.durable_prefix(), payload.len() as u64);
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        b"old-final"
    );
    transferred.discard().await?;
    Ok(())
}

#[tokio::test]
async fn cancellation_before_prepare_leaves_no_staged_or_final_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("cancel-source")?;
    let destination_root = TestRoot::new("cancel-destination")?;
    std::fs::write(source_root.path().join("source.bin"), b"payload")?;
    std::fs::write(destination_root.path().join("final.bin"), b"old-final")?;
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    let request = TransferRequest::new(
        TransferIdentity::new("cancelled-copy")?,
        local_source(source_root.path())?,
        StoragePath::new("source.bin")?,
        local_destination(destination_root.path())?,
        StoragePath::new("final.bin")?,
        InflightLimits::new(2, 64 * 1024, 2)?,
        cancel,
    );

    let Err(error) = run_until_transferred(request).await else {
        return Err("pre-cancelled transfer unexpectedly succeeded".into());
    };

    assert_eq!(error.phase(), TransferPhase::Preflight);
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        b"old-final"
    );
    assert!(!destination_root.path().join(".data-mover-staging").exists());
    Ok(())
}

#[tokio::test]
async fn preflight_refusal_happens_before_destination_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("preflight-source")?;
    let destination_root = TestRoot::new("preflight-destination")?;
    std::fs::write(source_root.path().join("source.bin"), b"payload")?;
    let destination = test_unsupported_storage("unsupported-destination")?;
    let request = TransferRequest::new(
        TransferIdentity::new("preflight-copy")?,
        local_source(source_root.path())?,
        StoragePath::new("source.bin")?,
        destination,
        StoragePath::new("final.bin")?,
        InflightLimits::new(1, 4096, 1)?,
        tokio_util::sync::CancellationToken::new(),
    );

    let Err(error) = run_until_transferred(request).await else {
        return Err("unsupported destination unexpectedly transferred".into());
    };
    assert_eq!(error.phase(), TransferPhase::Preflight);
    assert_eq!(error.side(), TransferSide::Destination);
    assert!(!destination_root.path().join(".data-mover-staging").exists());
    assert!(!destination_root.path().join("final.bin").exists());
    Ok(())
}

#[tokio::test]
async fn cancellation_during_describe_stops_before_prepare()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("describe-cancel-source")?;
    let destination_root = TestRoot::new("describe-cancel-destination")?;
    std::fs::write(source_root.path().join("source.bin"), b"payload")?;
    let (source, source_role) = test_source_storage(source_root.path(), "describe-source")?;
    source_role.delay_description("source.bin", std::time::Duration::from_millis(200));
    let cancel = tokio_util::sync::CancellationToken::new();
    let request = transfer_request(
        source,
        local_destination(destination_root.path())?,
        cancel.clone(),
    )?;
    let task = tokio::spawn(run_until_transferred(request));
    wait_for_description(&source_role).await?;
    cancel.cancel();

    let Err(error) = task.await? else {
        return Err("describe-cancelled transfer unexpectedly succeeded".into());
    };
    assert_eq!(error.phase(), TransferPhase::Describe);
    assert!(!destination_root.path().join(".data-mover-staging").exists());
    assert!(!destination_root.path().join("final.bin").exists());
    Ok(())
}

#[tokio::test]
async fn cancellation_during_source_read_preserves_unpublished_stage()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("active-cancel-source")?;
    let destination_root = TestRoot::new("active-cancel-destination")?;
    std::fs::write(
        source_root.path().join("source.bin"),
        vec![7_u8; 128 * 1024],
    )?;
    std::fs::write(destination_root.path().join("final.bin"), b"old-final")?;
    let (source, source_role) = test_source_storage(source_root.path(), "delayed-source")?;
    source_role.delay_reads(std::time::Duration::from_millis(200));
    let cancel = tokio_util::sync::CancellationToken::new();
    let request = transfer_request(
        source,
        local_destination(destination_root.path())?,
        cancel.clone(),
    )?;
    let task = tokio::spawn(run_until_transferred(request));
    wait_for_read(&source_role).await?;
    cancel.cancel();

    let result = task.await?;
    let Err(error) = result else {
        return Err("cancelled active transfer unexpectedly succeeded".into());
    };
    assert_eq!(error.phase(), TransferPhase::Transfer);
    assert_eq!(error.side(), TransferSide::Source);
    assert!(error.has_recoverable_stage());
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        b"old-final"
    );
    assert!(destination_root.path().join(".data-mover-staging").exists());
    error.discard_stage().await?;
    assert_eq!(staging_entry_count(destination_root.path())?, 0);
    Ok(())
}

fn staging_entry_count(root: &Path) -> io::Result<usize> {
    std::fs::read_dir(root.join(".data-mover-staging"))?
        .try_fold(0, |count, entry| entry.map(|_| count + 1))
}

fn transfer_request(
    source: Storage,
    destination: Storage,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<TransferRequest, Box<dyn std::error::Error>> {
    Ok(TransferRequest::new(
        TransferIdentity::new("active-cancel-copy")?,
        source,
        StoragePath::new("source.bin")?,
        destination,
        StoragePath::new("final.bin")?,
        InflightLimits::new(2, 64 * 1024, 2)?,
        cancel,
    ))
}

async fn wait_for_read(source: &LocalReadSource) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while source.read_call_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

async fn wait_for_description(source: &LocalReadSource) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !source.description_started() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

fn local_source(root: &Path) -> Result<Storage, Box<dyn std::error::Error>> {
    test_source_storage(root, "transfer-source").map(|(storage, _)| storage)
}

fn local_destination(root: &Path) -> Result<Storage, Box<dyn std::error::Error>> {
    test_destination_storage(root, "transfer-destination")
}

struct TestRoot(PathBuf);

impl TestRoot {
    fn new(label: &str) -> io::Result<Self> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "data-mover-transfer-{label}-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
