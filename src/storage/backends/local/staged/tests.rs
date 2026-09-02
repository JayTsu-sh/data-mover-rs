use std::sync::atomic::{AtomicU64, Ordering};

use futures::{StreamExt as _, stream};

use super::*;
use crate::model::{BackendKind, EntryKind, IdentityStrength, SourceIdentity};
use crate::storage::{ExistingDestinationPolicy, FinalDestination, SourceDescriptor};

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
            backend_fact: None,
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

fn owned_bytes(items: Vec<Bytes>) -> ByteStream {
    Box::pin(stream::iter(items.into_iter().map(Ok)))
}

fn request_with_size(identity: &BackendIdentity, destination: &str, size: usize) -> PrepareRequest {
    let mut request = request(identity, destination);
    request.source.size = Some(size as u64);
    request
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
async fn write_submits_a_five_mib_input_as_one_piece() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("local-five-mib-write-test");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 2));
    let payload = Bytes::from(vec![0x5a; LOCAL_MAX_WRITE_CHUNK_BYTES]);
    let stage = ok(adapter
        .prepare(request_with_size(&backend, "final.bin", payload.len()))
        .await);

    let evidence = ok(adapter
        .write(&stage, owned_bytes(vec![payload.clone()]))
        .await);
    assert_eq!(evidence.persisted_bytes, payload.len() as u64);
    assert_eq!(
        *adapter
            .write_probe
            .completion_order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![0]
    );
    assert_eq!(
        tokio::fs::read(ok(adapter.stage_full_path(&stage, Operation::Verify))).await?,
        payload
    );
    Ok(())
}

#[tokio::test]
async fn write_splits_an_oversized_input_and_preserves_the_tail() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("local-oversized-write-test");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 2));
    let payload = Bytes::from(vec![0xa5; LOCAL_MAX_WRITE_CHUNK_BYTES + 17]);
    let stage = ok(adapter
        .prepare(request_with_size(&backend, "final.bin", payload.len()))
        .await);

    let evidence = ok(adapter
        .write(&stage, owned_bytes(vec![payload.clone()]))
        .await);
    assert_eq!(evidence.persisted_bytes, payload.len() as u64);
    let mut offsets = adapter
        .write_probe
        .completion_order
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    offsets.sort_unstable();
    assert_eq!(offsets, vec![0, LOCAL_MAX_WRITE_CHUNK_BYTES as u64]);
    assert_eq!(
        tokio::fs::read(ok(adapter.stage_full_path(&stage, Operation::Verify))).await?,
        payload
    );
    Ok(())
}

#[test]
fn positional_write_retries_interrupted_and_completes_short_writes() {
    let mut calls = Vec::new();
    let mut interrupted = true;
    let written = ok(write_all_at(b"abcdef", 11, |remaining, offset| {
        calls.push((offset, remaining.len()));
        if interrupted {
            interrupted = false;
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        Ok(remaining.len().min(2))
    }));

    assert_eq!(written, 6);
    assert_eq!(calls, vec![(11, 6), (11, 6), (13, 4), (15, 2)]);
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
            claim_token: [1; 32],
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
async fn recoverable_write_advances_a_durable_prefix_before_input_ends() -> io::Result<()> {
    const CHECKPOINT_BYTES: usize = 4 * 64 * 1024;

    let root = TestRoot::new().await?;
    let backend = identity("local-periodic-checkpoint-test");
    let adapter = Arc::new(ok(LocalStagedDestination::new(&root.0, backend.clone(), 2)));
    adapter
        .write_probe
        .force_out_of_order
        .store(true, Ordering::SeqCst);
    let prepare = request_with_size(&backend, "final.bin", CHECKPOINT_BYTES + 1);
    let stage = Arc::new(ok(adapter.prepare(prepare.clone()).await));
    let recovery_identity = ok(adapter.recovery_identity(&stage).await);
    let input: ByteStream = Box::pin(
        stream::iter((0..4).map(|_| Ok(Bytes::from(vec![0x63; 64 * 1024]))))
            .chain(stream::pending::<Result<Bytes, StorageRoleFailure>>()),
    );
    let writing = {
        let adapter = Arc::clone(&adapter);
        let stage = Arc::clone(&stage);
        tokio::spawn(async move { adapter.write(&stage, input).await })
    };

    let durable_prefix = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let prefix = ok(adapter.observe_checkpoint(&stage).await).durable_prefix;
            if prefix > 0 {
                break prefix;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(io::Error::other)?;

    assert_eq!(durable_prefix, CHECKPOINT_BYTES as u64);
    writing.abort();
    let _ = writing.await;
    let stage = Arc::into_inner(stage).ok_or_else(|| io::Error::other("stage still shared"))?;
    drop(stage);
    drop(adapter);

    let reconnected = ok(LocalStagedDestination::new(&root.0, backend, 1));
    let recovered = ok(reconnected
        .recover(RecoverRequest {
            identity: recovery_identity,
            final_destination: prepare.final_destination,
            source: prepare.source,
            recovery_binding: prepare.recovery_binding,
            claim_token: [3; 32],
        })
        .await);
    assert_eq!(
        ok(reconnected.observe_checkpoint(&recovered).await).durable_prefix,
        CHECKPOINT_BYTES as u64
    );
    ok(reconnected.discard(recovered).await);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publishing_one_path_does_not_block_preparing_another_path() -> io::Result<()> {
    const EXISTING_BYTES: u64 = 64 * 1024 * 1024;

    let root = TestRoot::new().await?;
    let existing = std::fs::File::create(root.0.join("slow-existing.bin"))?;
    existing.set_len(EXISTING_BYTES)?;
    drop(existing);

    let backend = identity("local-independent-lifecycle-test");
    let adapter = Arc::new(ok(LocalStagedDestination::new(&root.0, backend.clone(), 1)));
    let slow_stage = ok(adapter
        .prepare_ephemeral(request_with_size(
            &backend,
            "slow-existing.bin",
            EXISTING_BYTES as usize,
        ))
        .await);
    adapter.slow_existing_verify();

    let publishing = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move {
            adapter
                .publish(
                    &slow_stage,
                    PublishRequest {
                        policy: ExistingDestinationPolicy::VerifyOrSkip,
                        expected_size: EXISTING_BYTES,
                        expected_blake3: [0xff; 32],
                        cancel: tokio_util::sync::CancellationToken::new(),
                    },
                )
                .await
        })
    };

    let observation_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while !adapter.existing_verify_started() {
        assert!(
            tokio::time::Instant::now() < observation_deadline,
            "existing-destination verification did not start"
        );
        tokio::task::yield_now().await;
    }

    let independent = tokio::time::timeout(
        Duration::from_millis(100),
        adapter.prepare_ephemeral(request(&backend, "independent.bin")),
    )
    .await;
    let publication = publishing.await.map_err(io::Error::other)?;
    assert!(
        publication.is_err(),
        "the deliberately wrong digest was accepted"
    );

    let independent = independent.map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "publishing one final destination serialized preparation for another destination",
        )
    })?;
    let independent = independent.map_err(io::Error::other)?;
    ok(adapter.discard(independent).await);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_fail_if_exists_publications_commit_exactly_one_complete_file() -> io::Result<()>
{
    let root = TestRoot::new().await?;
    let backend = identity("local-concurrent-create-publication-test");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 1));
    let left_payload = Bytes::from_static(b"left-complete");
    let right_payload = Bytes::from_static(b"right-complete");

    let left = ok(adapter
        .prepare_ephemeral(request_with_size(
            &backend,
            "shared-final.bin",
            left_payload.len(),
        ))
        .await);
    let right = ok(adapter
        .prepare_ephemeral(request_with_size(
            &backend,
            "shared-final.bin",
            right_payload.len(),
        ))
        .await);
    ok(adapter
        .write(&left, owned_bytes(vec![left_payload.clone()]))
        .await);
    ok(adapter
        .write(&right, owned_bytes(vec![right_payload.clone()]))
        .await);

    let publish_request = |payload: &Bytes| PublishRequest {
        policy: ExistingDestinationPolicy::FailIfExists,
        expected_size: payload.len() as u64,
        expected_blake3: *blake3::hash(payload).as_bytes(),
        cancel: tokio_util::sync::CancellationToken::new(),
    };
    let (left_result, right_result) = tokio::join!(
        adapter.publish(&left, publish_request(&left_payload)),
        adapter.publish(&right, publish_request(&right_payload)),
    );

    assert_eq!(
        usize::from(left_result.is_ok()) + usize::from(right_result.is_ok()),
        1
    );
    if left_result.is_err() {
        ok(adapter.discard(left).await);
    }
    if right_result.is_err() {
        ok(adapter.discard(right).await);
    }
    let final_content = tokio::fs::read(root.0.join("shared-final.bin")).await?;
    assert!(final_content == left_payload || final_content == right_payload);
    assert!(staging_is_empty(&root.0)?);
    Ok(())
}
