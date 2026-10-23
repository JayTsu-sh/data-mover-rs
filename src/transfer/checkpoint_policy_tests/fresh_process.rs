//! Resume real destination artifacts after a process exits with its stage still live.

use std::env;
use std::fs::{self, File};
use std::io::Read as _;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::process::Command;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::{CHUNK, INTERVAL, PATH, TestResult, destination_storage, source_storage};
use crate::model::{EntryOperationFailure, FailureClass, Operation, StoragePath, Transience};
use crate::storage::artifacts::{ArtifactKind, sibling_artifact};
use crate::storage::backends::local::{test_destination_storage_with_role, test_source_storage};
use crate::storage::checkpoint::{CheckpointData, CheckpointPhase};
use crate::storage::pointer::{DestinationPointer, MAX_POINTER_BYTES};
use crate::storage::range_hash::RangeHash;
use crate::storage::{
    ByteStream, PositionedByteStream, ReadRequest, ReadSource, SourceDescriptor, StorageRoleFailure,
};
use crate::transfer::engine::run_until_transferred;
use crate::transfer::{EffectiveRecovery, PrepareFact, TransferPolicy, TransferRequest, transfer};

const ROOT_ENV: &str = "DATA_MOVER_CHECKPOINT_POLICY_CHILD_ROOT";
const CHILD_TEST: &str =
    "transfer::checkpoint_policy_tests::fresh_process::child_leaves_complete_checkpoint_and_exits";
const CHILD_EXIT: i32 = 73;
const SIZE: usize = 4 * CHUNK + 17;
const ORIGINAL: &[u8] = b"the original final must survive the child";
const DEADLINE: Duration = Duration::from_secs(15);

struct ReadAudit {
    durable: Vec<Range<u64>>,
    size: u64,
    calls: AtomicUsize,
    forbidden: AtomicUsize,
}

impl ReadAudit {
    fn check(&self, request: &ReadRequest) -> Result<(), StorageRoleFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let range = request.range.clone().unwrap_or(0..self.size);
        if range.start < range.end
            && self
                .durable
                .iter()
                .any(|done| range.start < done.end && done.start < range.end)
        {
            self.forbidden.fetch_add(1, Ordering::SeqCst);
            return Err(StorageRoleFailure::Entry(
                EntryOperationFailure::new(
                    request.path.clone(),
                    Operation::Read,
                    FailureClass::Corruption,
                    Transience::Permanent,
                    "source READ overlaps fresh-process durable coverage",
                )
                .unwrap_or_else(|_| unreachable!("static read guard diagnostic")),
            ));
        }
        Ok(())
    }
}

struct GuardedSource {
    inner: Arc<dyn ReadSource>,
    audit: Arc<ReadAudit>,
}

#[async_trait]
impl ReadSource for GuardedSource {
    fn maximum_read_chunk_bytes(&self) -> usize {
        self.inner.maximum_read_chunk_bytes()
    }

    fn read_concurrency(&self) -> usize {
        self.inner.read_concurrency()
    }

    fn supports_read_budget(&self) -> bool {
        self.inner.supports_read_budget()
    }

    fn supports_positioned_read(&self) -> bool {
        self.inner.supports_positioned_read()
    }

    async fn describe(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.inner.describe(path).await
    }

    async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        self.audit.check(&request)?;
        self.inner.read(request).await
    }

    async fn read_positioned(
        &self,
        request: ReadRequest,
    ) -> Result<PositionedByteStream, StorageRoleFailure> {
        self.audit.check(&request)?;
        self.inner.read_positioned(request).await
    }
}

/// Every role is reconstructed in each process, with the same endpoint labels and roots.
fn request(root: &Path, audit: Option<Arc<ReadAudit>>) -> TestResult<TransferRequest> {
    let (source, source_role) =
        test_source_storage(&root.join("source"), "checkpoint-fresh-process-source")?;
    let reader: Arc<dyn ReadSource> = match audit {
        Some(audit) => Arc::new(GuardedSource {
            inner: source_role,
            audit,
        }),
        None => source_role,
    };
    let source = source_storage(&source, reader)?;
    let (destination, destination_role) = test_destination_storage_with_role(
        &root.join("destination"),
        "checkpoint-fresh-process-destination",
    )?;
    destination_role.set_automatic_checkpoint_interval(INTERVAL);
    let destination = destination_storage(&destination, destination_role)?;
    Ok(TransferRequest::new(
        source,
        StoragePath::new(PATH)?,
        destination,
        CancellationToken::new(),
    )
    .with_transfer_policy(TransferPolicy::Checkpointed))
}

fn checkpoint(root: &Path) -> TestResult<(DestinationPointer, CheckpointData)> {
    let pointer_path = sibling_artifact(&StoragePath::new(PATH)?, ArtifactKind::Pointer)
        .ok_or("missing deterministic pointer path")?;
    let mut encoded = Vec::new();
    File::open(root.join("destination").join(pointer_path.as_str()))?
        .take(MAX_POINTER_BYTES as u64 + 1)
        .read_to_end(&mut encoded)?;
    assert!(
        encoded.starts_with(b"DMDPTR02"),
        "child did not leave a v2 pointer"
    );
    let pointer = DestinationPointer::decode(&encoded)
        .map_err(|error| format!("invalid child pointer: {error:?}"))?;
    let data = CheckpointData::decode(
        pointer
            .checkpoint
            .as_deref()
            .ok_or("v2 pointer has no source hash checkpoint")?,
    )?;
    Ok((pointer, data))
}

async fn run_child(root: &Path) -> TestResult {
    let log_path = root.join("child.log");
    let log = File::create(&log_path)?;
    let mut child = Command::new(env::current_exe()?)
        .args(["--exact", CHILD_TEST, "--nocapture", "--test-threads=1"])
        .env(ROOT_ENV, root)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .kill_on_drop(true)
        .spawn()?;
    let status = match timeout(DEADLINE, child.wait()).await {
        Ok(status) => status?,
        Err(error) => {
            let _ = timeout(Duration::from_secs(2), child.kill()).await;
            return Err(format!(
                "checkpoint child timed out: {error}\n{}",
                fs::read_to_string(&log_path)?
            )
            .into());
        }
    };
    assert_eq!(
        status.code(),
        Some(CHILD_EXIT),
        "child did not exit at the checkpoint:\n{}",
        fs::read_to_string(log_path)?
    );
    Ok(())
}

/// An ordinary cargo test run does nothing here; only the parent's exact subprocess enters it.
#[tokio::test]
async fn child_leaves_complete_checkpoint_and_exits() -> TestResult {
    let Some(root) = env::var_os(ROOT_ENV) else {
        return Ok(());
    };
    let root = PathBuf::from(root);
    let transferred = timeout(DEADLINE, run_until_transferred(request(&root, None)?)).await??;
    assert_eq!(transferred.durable_prefix(), SIZE as u64);
    assert!(transferred.recovery_enabled());
    let (_, data) = checkpoint(&root)?;
    assert_eq!(data.phase, CheckpointPhase::Receiving);
    assert_eq!(data.ranges, vec![0..SIZE as u64]);
    assert_eq!(fs::read(root.join("destination").join(PATH))?, ORIGINAL);
    // Keep the stage, its claim and the runtime live. This deliberately bypasses their Drop.
    std::process::exit(CHILD_EXIT);
}

#[tokio::test]
async fn a_fresh_process_resumes_complete_durable_coverage_without_source_reads() -> TestResult {
    let root = tempfile::tempdir()?;
    fs::create_dir(root.path().join("source"))?;
    fs::create_dir(root.path().join("destination"))?;
    let expected: Vec<u8> = (0..=250_u8).cycle().take(SIZE).collect();
    fs::write(root.path().join("source").join(PATH), &expected)?;
    fs::write(root.path().join("destination").join(PATH), ORIGINAL)?;
    run_child(root.path()).await?;
    let (pointer, data) = checkpoint(root.path())?;
    assert_eq!(pointer.durable_prefix, Some(SIZE as u64));
    assert_eq!(data.size, SIZE as u64);
    assert_eq!(data.ranges, vec![0..SIZE as u64]);
    let expected_hash = *blake3::hash(&expected).as_bytes();
    assert_eq!(
        RangeHash::decode(&data.hash, data.size)?.finalize()?,
        expected_hash
    );
    let audit = Arc::new(ReadAudit {
        durable: data.ranges,
        size: data.size,
        calls: AtomicUsize::new(0),
        forbidden: AtomicUsize::new(0),
    });
    let request = request(root.path(), Some(Arc::clone(&audit)))?;
    assert_eq!(pointer.transfer_identity, *request.identity().as_bytes());
    let outcome = timeout(DEADLINE, transfer(request)).await??;
    assert_eq!(audit.forbidden.load(Ordering::SeqCst), 0);
    assert_eq!(
        audit.calls.load(Ordering::SeqCst),
        0,
        "complete D must need no source payload read"
    );
    assert_eq!(outcome.prepare, PrepareFact::Resumed { bytes: SIZE as u64 });
    assert_eq!(outcome.reused_bytes, SIZE as u64);
    assert_eq!(outcome.recovery, EffectiveRecovery::Checkpointed);
    assert_eq!(outcome.blake3, Some(expected_hash));
    assert_final_and_cleanup(root.path(), expected_hash)?;
    Ok(())
}

fn assert_final_and_cleanup(root: &Path, expected_hash: [u8; 32]) -> TestResult {
    let destination = root.join("destination");
    assert_eq!(
        *blake3::hash(&fs::read(destination.join(PATH))?).as_bytes(),
        expected_hash
    );
    let names: Vec<_> = fs::read_dir(destination)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<Result<_, _>>()?;
    assert_eq!(
        names,
        [std::ffi::OsString::from(PATH)],
        "resume left checkpoint artifacts"
    );
    Ok(())
}
