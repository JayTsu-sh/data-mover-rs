use std::sync::atomic::{AtomicU64, Ordering};

use futures::stream;

use super::*;
use crate::model::{BackendKind, EntryKind, IdentityStrength, SourceIdentity};
use crate::storage::{FinalDestination, SourceDescriptor};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct TestRoot(PathBuf);

impl TestRoot {
    async fn new() -> io::Result<Self> {
        let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "data-mover-local-stage-{}-{sequence}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&path).await?;
        Ok(Self(path))
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn ok<T, E: std::fmt::Display>(result: Result<T, E>) -> T {
    result.unwrap_or_else(|error| panic!("unexpected failure: {error}"))
}

fn identity(name: &str) -> BackendIdentity {
    ok(BackendIdentity::new(BackendKind::Local, name))
}

fn request(identity: &BackendIdentity, destination: &str) -> PrepareRequest {
    PrepareRequest {
        final_destination: FinalDestination::new(ok(StoragePath::new(destination))),
        source: SourceDescriptor {
            path: ok(StoragePath::new("source.bin")),
            kind: EntryKind::File,
            size: Some(9),
            source_identity: ok(SourceIdentity::new(
                identity.clone(),
                IdentityStrength::StableWithinBackend,
                b"source-file",
            )),
        },
        recovery_binding: [7; 32],
    }
}

fn bytes(items: &[&'static [u8]]) -> ByteStream {
    Box::pin(stream::iter(
        items
            .iter()
            .map(|item| Ok(Bytes::from_static(item)))
            .collect::<Vec<_>>(),
    ))
}

fn staging_is_empty(root: &Path) -> io::Result<bool> {
    let staging = root.join(STAGING_DIRECTORY);
    Ok(!staging.exists() || std::fs::read_dir(staging)?.next().is_none())
}

fn assert_out_of_order_completion(adapter: &LocalStagedDestination) {
    let completion_order = adapter
        .write_probe
        .completion_order
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert_eq!(completion_order.len(), 3);
    assert_ne!(completion_order[0], 0);
}

async fn assert_checkpoint_failure_rolls_back(point: u64) -> io::Result<()> {
    let root = TestRoot::new().await?;
    tokio::fs::write(root.0.join("final.bin"), b"original").await?;
    let backend = identity(&format!("checkpoint-failure-{point}"));
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 1));
    adapter
        .write_probe
        .checkpoint_failure
        .store(point, Ordering::SeqCst);

    assert!(
        adapter
            .prepare(request(&backend, "final.bin"))
            .await
            .is_err()
    );
    assert!(staging_is_empty(&root.0)?);
    assert_eq!(
        tokio::fs::read(root.0.join("final.bin")).await?,
        b"original"
    );
    Ok(())
}

#[tokio::test]
async fn prepare_rolls_back_checkpoint_failures_before_and_after_rename() -> io::Result<()> {
    assert_checkpoint_failure_rolls_back(1).await?;
    assert_checkpoint_failure_rolls_back(2).await
}

#[tokio::test]
async fn prepare_write_flush_checkpoint_and_discard_leave_final_unchanged() -> io::Result<()> {
    let root = TestRoot::new().await?;
    tokio::fs::write(root.0.join("final.bin"), b"original").await?;
    let backend = identity("local-stage-test");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 3));
    adapter
        .write_probe
        .force_out_of_order
        .store(true, Ordering::SeqCst);
    let stage = ok(adapter.prepare(request(&backend, "final.bin")).await);

    assert_eq!(
        tokio::fs::read(root.0.join("final.bin")).await?,
        b"original"
    );
    let evidence = ok(adapter
        .write(&stage, bytes(&[b"abc", b"def", b"ghi"]))
        .await);
    assert_eq!(evidence.persisted_bytes, 9);
    assert_out_of_order_completion(&adapter);
    assert_eq!(
        ok(adapter.observe_checkpoint(&stage).await).durable_prefix,
        9
    );
    let staged_path = adapter.stage_full_path(&stage, Operation::Verify);
    assert_eq!(tokio::fs::read(ok(staged_path)).await?, b"abcdefghi");
    assert_eq!(
        tokio::fs::read(root.0.join("final.bin")).await?,
        b"original"
    );

    let reconnected = ok(LocalStagedDestination::new(&root.0, backend, 1));
    assert_eq!(
        ok(reconnected.observe_checkpoint(&stage).await).durable_prefix,
        9
    );
    ok(reconnected.discard(stage).await);
    assert!(
        tokio::fs::read_dir(root.0.join(STAGING_DIRECTORY))
            .await?
            .next_entry()
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn discard_keeps_recovery_claim_until_owned_contents_are_removed() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("local-discard-recover-race-test");
    let adapter = Arc::new(ok(LocalStagedDestination::new(&root.0, backend.clone(), 1)));
    let prepare = request(&backend, "final.bin");
    let stage = ok(adapter.prepare(prepare.clone()).await);
    ok(adapter.write(&stage, bytes(&[b"partial"])).await);
    let recovery_identity = ok(adapter.recovery_identity(&stage).await);
    adapter.slow_discard_before_release();

    let discarding = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move { adapter.discard(stage).await })
    };
    while !adapter.discard_contents_removed() {
        tokio::task::yield_now().await;
    }

    let contender = ok(LocalStagedDestination::new(&root.0, backend, 1));
    let recovery = contender
        .recover(RecoverRequest {
            identity: recovery_identity,
            final_destination: prepare.final_destination,
            source: prepare.source,
            recovery_binding: prepare.recovery_binding,
        })
        .await;
    assert!(recovery.is_err());
    ok(discarding.await.map_err(io::Error::other)?);
    assert!(staging_is_empty(&root.0)?);
    Ok(())
}

#[tokio::test]
async fn restart_prepares_distinct_empty_state_without_touching_final() -> io::Result<()> {
    let root = TestRoot::new().await?;
    tokio::fs::write(root.0.join("final.bin"), b"keep").await?;
    let backend = identity("local-restart-test");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 2));
    let first = ok(adapter.prepare(request(&backend, "final.bin")).await);
    ok(adapter.write(&first, bytes(&[b"old"])).await);
    let restarted = ok(adapter.prepare(request(&backend, "final.bin")).await);

    assert_ne!(first.token, restarted.token);
    assert_eq!(
        ok(adapter.observe_checkpoint(&restarted).await).durable_prefix,
        0
    );
    assert_eq!(tokio::fs::read(root.0.join("final.bin")).await?, b"keep");
    ok(adapter.discard(first).await);
    ok(adapter.discard(restarted).await);
    Ok(())
}

#[tokio::test]
async fn reobserved_checkpoint_rejects_truncated_staged_content() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("local-checkpoint-truncation-test");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 2));
    let stage = ok(adapter.prepare(request(&backend, "final.bin")).await);
    ok(adapter.write(&stage, bytes(&[b"abcdef"])).await);
    let staged_path = ok(adapter.stage_full_path(&stage, Operation::Verify));
    tokio::fs::OpenOptions::new()
        .write(true)
        .open(staged_path)
        .await?
        .set_len(2)
        .await?;

    let reconnected = ok(LocalStagedDestination::new(&root.0, backend, 1));
    assert!(reconnected.observe_checkpoint(&stage).await.is_err());
    Ok(())
}

#[tokio::test]
async fn reobserved_checkpoint_rejects_tampered_record() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("local-checkpoint-tamper-test");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 1));
    let stage = ok(adapter.prepare(request(&backend, "final.bin")).await);
    ok(adapter.write(&stage, bytes(&[b"abc"])).await);
    let stage_path = ok(adapter.stage_full_path(&stage, Operation::Verify));
    let mut checkpoint_name = stage_path.file_name().unwrap_or_default().to_os_string();
    checkpoint_name.push(".checkpoint");
    let checkpoint_path = stage_path.with_file_name(checkpoint_name);
    let mut record = tokio::fs::read(&checkpoint_path).await?;
    record[10] ^= 0x80;
    tokio::fs::write(checkpoint_path, record).await?;

    let reconnected = ok(LocalStagedDestination::new(&root.0, backend, 1));
    assert!(reconnected.observe_checkpoint(&stage).await.is_err());
    Ok(())
}

#[tokio::test]
async fn entry_failure_and_cancellation_preserve_unpublished_stage() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("local-failure-test");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 1));
    adapter
        .write_probe
        .delays
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(0, Duration::from_millis(100));
    let stage = ok(adapter.prepare(request(&backend, "final.bin")).await);
    let failure = failure(
        &ok(StoragePath::new("source.bin")),
        Operation::Read,
        FailureClass::Cancelled,
    );
    let input: ByteStream = Box::pin(stream::iter(vec![
        Ok(Bytes::from_static(b"partial")),
        Err(failure),
    ]));

    let started = std::time::Instant::now();
    assert!(adapter.write(&stage, input).await.is_err());
    assert!(started.elapsed() >= Duration::from_millis(70));
    assert!(!root.0.join("final.bin").exists());
    assert!(ok(adapter.stage_full_path(&stage, Operation::Verify)).exists());
    assert_eq!(
        ok(adapter.observe_checkpoint(&stage).await).durable_prefix,
        7
    );
    let staged_path = ok(adapter.stage_full_path(&stage, Operation::Verify));
    let staged_len = tokio::fs::metadata(&staged_path).await?.len();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(tokio::fs::metadata(staged_path).await?.len(), staged_len);
    ok(adapter.discard(stage).await);
    Ok(())
}

#[tokio::test]
async fn paths_and_stage_ownership_are_confined_to_the_backend_root() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("local-confinement-test");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 1));
    assert!(
        adapter
            .prepare(request(&backend, "../escape.bin"))
            .await
            .is_err()
    );
    assert!(
        adapter
            .prepare(request(&backend, ".data-mover-staging/final.bin"))
            .await
            .is_err()
    );

    let foreign = PreparedStage::new(
        identity("foreign-local"),
        FinalDestination::new(ok(StoragePath::new("final.bin"))),
        Bytes::from_static(b".data-mover-staging/foreign.stage"),
        [0; 32],
        0,
        None,
    );
    assert!(adapter.observe_checkpoint(&foreign).await.is_err());

    tokio::fs::write(root.0.join("victim.bin"), b"keep").await?;
    let forged = PreparedStage::new(
        backend,
        FinalDestination::new(ok(StoragePath::new("victim.bin"))),
        Bytes::from_static(b"victim.bin"),
        [0; 32],
        0,
        None,
    );
    assert!(adapter.discard(forged).await.is_err());
    assert_eq!(tokio::fs::read(root.0.join("victim.bin")).await?, b"keep");
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn staging_symlink_cannot_escape_the_capability_root() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let outside = TestRoot::new().await?;
    std::os::unix::fs::symlink(&outside.0, root.0.join(STAGING_DIRECTORY))?;
    let backend = identity("local-symlink-confinement-test");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 1));

    assert!(
        adapter
            .prepare(request(&backend, "final.bin"))
            .await
            .is_err()
    );
    assert!(std::fs::read_dir(&outside.0)?.next().is_none());
    Ok(())
}
