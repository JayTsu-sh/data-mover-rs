use std::sync::atomic::{AtomicU64, Ordering};

use futures::stream;

use super::*;
use crate::model::{BackendKind, EntryKind, IdentityStrength, SourceIdentity, SourceVersion};
use crate::storage::ListingFacts;
use crate::storage::{FinalDestination, PrepareFact, RestartReason, ResumeMode, SourceDescriptor};

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
            content_version: None,
            inline_timestamps: None,
            inline_mode: None,
            version: SourceVersion::Current,
            listing: ListingFacts::default(),
        },
        recovery_binding: [7; 32],
    }
}

/// A fresh stage: whatever an earlier stage of the file left is cleaned up first.
async fn prepare_stage(
    adapter: &LocalStagedDestination,
    request: PrepareRequest,
) -> Result<PreparedStage, StorageRoleFailure> {
    adapter
        .prepare_at_destination(at_destination(request, ResumeMode::Restart, true))
        .await
}

/// A fresh stage whose first pointer waits for a checkpoint.
async fn prepare_ephemeral_stage(
    adapter: &LocalStagedDestination,
    request: PrepareRequest,
) -> Result<PreparedStage, StorageRoleFailure> {
    adapter
        .prepare_at_destination(at_destination(request, ResumeMode::Restart, false))
        .await
}

/// Resumes what an earlier stage of the file left, as a new process would.
async fn resume_stage(
    adapter: &LocalStagedDestination,
    request: PrepareRequest,
) -> Result<PreparedStage, StorageRoleFailure> {
    adapter
        .prepare_at_destination(at_destination(request, ResumeMode::Discover, true))
        .await
}

fn at_destination(
    request: PrepareRequest,
    resume: ResumeMode,
    recoverable: bool,
) -> DestinationPrepareRequest {
    DestinationPrepareRequest::new(request, [3; 32])
        .with_resume(resume)
        .with_recoverable(recoverable)
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
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(".data-mover-")
        {
            return Ok(false);
        }
        if entry.file_type()?.is_dir() && !staging_is_empty(&entry.path())? {
            return Ok(false);
        }
    }
    Ok(true)
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
        prepare_stage(&adapter, request(&backend, "final.bin"))
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
    let stage = ok(prepare_stage(&adapter, request(&backend, "final.bin")).await);

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
    assert!(staging_is_empty(&root.0)?);
    Ok(())
}

#[tokio::test]
async fn write_submits_an_eight_mib_input_as_one_piece() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("local-eight-mib-write-test");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 2));
    let payload = Bytes::from(vec![0x5a; LOCAL_MAX_WRITE_CHUNK_BYTES]);
    let stage = ok(prepare_stage(
        &adapter,
        request_with_size(&backend, "final.bin", payload.len()),
    )
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
async fn write_ceiling_does_not_change_automatic_checkpoint_interval() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let adapter = ok(LocalStagedDestination::new(
        &root.0,
        identity("local-independent-checkpoint-interval"),
        1,
    ));
    assert_eq!(LOCAL_MAX_WRITE_CHUNK_BYTES, 8 * 1024 * 1024);
    assert_eq!(
        adapter.automatic_checkpoint_interval_bytes(),
        Some(64 * 1024 * 1024)
    );
    Ok(())
}

#[tokio::test]
async fn write_splits_an_oversized_input_and_preserves_the_tail() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("local-oversized-write-test");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 2));
    let payload = Bytes::from(vec![0xa5; LOCAL_MAX_WRITE_CHUNK_BYTES + 17]);
    let stage = ok(prepare_stage(
        &adapter,
        request_with_size(&backend, "final.bin", payload.len()),
    )
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
async fn discard_keeps_the_claim_until_owned_contents_are_removed() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("local-discard-recover-race-test");
    let adapter = Arc::new(ok(LocalStagedDestination::new(&root.0, backend.clone(), 1)));
    let prepare = request(&backend, "final.bin");
    let stage = ok(prepare_stage(&adapter, prepare.clone()).await);
    ok(adapter.write(&stage, bytes(&[b"partial"])).await);
    adapter.slow_discard_before_release();

    let discarding = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move { adapter.discard(stage).await })
    };
    while !adapter.discard_contents_removed() {
        tokio::task::yield_now().await;
    }

    // Contents are gone but the claim is still held: a contender is refused, not handed a
    // half-removed stage.
    let contender = ok(LocalStagedDestination::new(&root.0, backend, 1));
    let refused = resume_stage(&contender, prepare.clone()).await;
    assert!(matches!(
        refused,
        Err(StorageRoleFailure::Entry(ref error))
            if error.class() == FailureClass::Conflict
                && error.transience() == Transience::Transient
    ));
    ok(discarding.await.map_err(io::Error::other)?);
    assert!(staging_is_empty(&root.0)?);
    let after = ok(resume_stage(&contender, prepare).await);
    assert_eq!(after.prepare_fact, PrepareFact::Fresh);
    ok(contender.discard(after).await);
    Ok(())
}

#[tokio::test]
async fn restart_cleans_the_earlier_stage_without_touching_final() -> io::Result<()> {
    let root = TestRoot::new().await?;
    tokio::fs::write(root.0.join("final.bin"), b"keep").await?;
    let backend = identity("local-restart-test");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 2));
    let first = ok(prepare_stage(&adapter, request(&backend, "final.bin")).await);
    ok(adapter.write(&first, bytes(&[b"old"])).await);
    drop(first);
    let restarted = ok(prepare_stage(&adapter, request(&backend, "final.bin")).await);

    assert_eq!(
        restarted.prepare_fact,
        PrepareFact::Restarted {
            reason: RestartReason::Requested
        }
    );
    assert_eq!(
        ok(adapter.observe_checkpoint(&restarted).await).durable_prefix,
        0
    );
    let staged = ok(adapter.stage_full_path(&restarted, Operation::Verify));
    assert_eq!(tokio::fs::metadata(staged).await?.len(), 0);
    assert_eq!(tokio::fs::read(root.0.join("final.bin")).await?, b"keep");
    ok(adapter.discard(restarted).await);
    Ok(())
}

#[tokio::test]
async fn reobserved_checkpoint_rejects_truncated_staged_content() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("local-checkpoint-truncation-test");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 2));
    let stage = ok(prepare_stage(&adapter, request(&backend, "final.bin")).await);
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
    let stage = ok(prepare_stage(&adapter, request(&backend, "final.bin")).await);
    ok(adapter.write(&stage, bytes(&[b"abc"])).await);
    let checkpoint_path = root.0.join(crate::storage::artifacts::artifact_name(
        "final.bin",
        ArtifactKind::Pointer,
    ));
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
    let stage = ok(prepare_stage(&adapter, request(&backend, "final.bin")).await);
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
    let stage = Arc::new(ok(prepare_stage(&adapter, prepare.clone()).await));
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
    let recovered = ok(resume_stage(&reconnected, prepare).await);
    assert_eq!(
        recovered.prepare_fact,
        PrepareFact::Resumed {
            bytes: CHECKPOINT_BYTES as u64
        }
    );
    assert_eq!(
        ok(reconnected.observe_checkpoint(&recovered).await).durable_prefix,
        CHECKPOINT_BYTES as u64
    );
    ok(reconnected.discard(recovered).await);
    Ok(())
}

#[test]
fn only_the_deterministic_stage_name_is_accepted() {
    let deterministic = crate::storage::artifacts::artifact_name("final.bin", ArtifactKind::Stage);
    let random = ".data-mover-0123456789abcdef-0123456789abcdef0123456789abcdef.stage";
    for (token, at_destination, accepted) in [
        // The random name stages had before ADR-0006 C8, even for a stage kept at the destination.
        (random, true, false),
        (".data-mover-staging/old.stage", true, false),
        // The deterministic name, only for a stage prepared at the destination.
        (deterministic.as_str(), false, false),
        (deterministic.as_str(), true, true),
    ] {
        let mut stage = PreparedStage::new(
            identity("legacy-name-test"),
            FinalDestination::new(ok(StoragePath::new("final.bin"))),
            Bytes::copy_from_slice(token.as_bytes()),
            [0; 32],
            0,
            None,
        );
        if at_destination {
            stage.mark_at_destination(PrepareFact::Fresh);
        }
        assert_eq!(
            LocalStagedDestination::stage_relative(&stage, Operation::Prepare).is_ok(),
            accepted,
            "{token} at_destination={at_destination}"
        );
    }
}

#[tokio::test]
async fn paths_and_stage_ownership_are_confined_to_the_backend_root() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("local-confinement-test");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 1));
    assert!(
        prepare_stage(&adapter, request(&backend, "../escape.bin"))
            .await
            .is_err()
    );
    assert!(
        prepare_stage(&adapter, request(&backend, ".data-mover-staging/final.bin"))
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
    std::os::unix::fs::symlink(&outside.0, root.0.join("subdir"))?;
    let backend = identity("local-symlink-confinement-test");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 1));

    assert!(
        prepare_stage(&adapter, request(&backend, "subdir/final.bin"))
            .await
            .is_err()
    );
    assert!(std::fs::read_dir(&outside.0)?.next().is_none());
    Ok(())
}
#[tokio::test]
async fn cancelled_publication_preserves_destination_and_stage() -> io::Result<()> {
    for existing in [true, false] {
        let root = TestRoot::new().await?;
        let final_path = root.0.join("final.bin");
        if existing {
            tokio::fs::write(&final_path, b"original").await?;
        }
        let backend = identity("local-cancelled-publication-test");
        let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 1));
        let payload = Bytes::from_static(b"new bytes");
        let stage = ok(prepare_ephemeral_stage(
            &adapter,
            request_with_size(&backend, "final.bin", payload.len()),
        )
        .await);
        ok(adapter
            .write(&stage, owned_bytes(vec![payload.clone()]))
            .await);
        let cancel = tokio_util::sync::CancellationToken::new();
        cancel.cancel();

        let failure = adapter
            .publish(
                &stage,
                PublishRequest {
                    expected_size: payload.len() as u64,
                    expected_blake3: Some(*blake3::hash(&payload).as_bytes()),
                    cancel,
                },
            )
            .await
            .err()
            .ok_or_else(|| io::Error::other("cancelled publication must not commit"))?;
        assert!(matches!(
            failure.error,
            StorageRoleFailure::Entry(ref error) if error.class() == FailureClass::Cancelled
        ));
        assert!(!failure.final_destination_changed);
        if existing {
            assert_eq!(tokio::fs::read(&final_path).await?, b"original");
        } else {
            assert!(!final_path.exists());
        }
        let stage_name = ok(adapter.stage_name(&stage, Operation::Publish));
        assert_eq!(tokio::fs::read(root.0.join(stage_name)).await?, payload);
        ok(adapter.discard(stage).await);
    }
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_source_chunk_splits_into_concurrent_destination_writes() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("single-source-concurrent-write");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 2));
    let payload = Bytes::from(vec![0xa7; LOCAL_MAX_WRITE_CHUNK_BYTES + 4096]);
    let stage = ok(prepare_ephemeral_stage(
        &adapter,
        request_with_size(&backend, "final.bin", payload.len()),
    )
    .await);
    adapter
        .write_probe
        .force_out_of_order
        .store(true, Ordering::SeqCst);
    let evidence = ok(adapter.write_single(&stage, payload.clone()).await);
    assert_eq!(evidence.persisted_bytes, payload.len() as u64);
    assert_eq!(
        *adapter
            .write_probe
            .completion_order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![LOCAL_MAX_WRITE_CHUNK_BYTES as u64, 0]
    );
    assert_eq!(
        tokio::fs::read(ok(adapter.stage_full_path(&stage, Operation::Verify))).await?,
        payload
    );
    assert!(!root.0.join("final.bin").exists());
    ok(adapter.discard(stage).await);
    Ok(())
}

#[tokio::test]
async fn checkpoint_persistence_overlaps_the_next_write_window() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("checkpoint-overlap");
    let adapter = Arc::new(ok(LocalStagedDestination::new(&root.0, backend.clone(), 2)));
    let stage = ok(prepare_stage(&adapter, request(&backend, "final.bin")).await);
    let probe = Arc::clone(&adapter.write_probe);
    probe.pause_checkpoint.store(true, Ordering::SeqCst);
    let writing = Arc::clone(&adapter);
    let writer = tokio::spawn(async move {
        let input = stream::iter((0..5).map(|_| Ok(Bytes::from(vec![42; 64 * 1024]))));
        let result = writing.write(&stage, Box::pin(input)).await;
        (stage, result)
    });
    tokio::time::timeout(Duration::from_secs(3), probe.checkpoint_started.notified()).await?;
    let advanced = tokio::time::timeout(Duration::from_secs(1), async {
        while adapter.write_completion_count() < 5 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    probe.checkpoint_release.notify_one();
    let (stage, result) = writer.await?;
    assert_eq!(ok(result).persisted_bytes, 5 * 64 * 1024);
    assert!(
        advanced.is_ok(),
        "checkpoint persistence stalled subsequent writes"
    );
    assert_eq!(
        tokio::fs::read(
            adapter
                .stage_full_path(&stage, Operation::Read)
                .map_err(io::Error::other)?
        )
        .await?,
        vec![42; 5 * 64 * 1024]
    );
    assert_eq!(
        ok(adapter.observe_checkpoint(&stage).await).durable_prefix,
        5 * 64 * 1024
    );
    ok(adapter.discard(stage).await);
    Ok(())
}

#[tokio::test]
async fn nested_stages_recover_in_the_parent() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("nested-stage-recovery");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 2));
    let prepare = request(&backend, "nested/deep/final.bin");
    let stage = ok(prepare_stage(&adapter, prepare.clone()).await);
    let stage_path = ok(adapter.stage_full_path(&stage, Operation::Read));
    assert_eq!(
        stage_path.parent(),
        Some(root.0.join("nested/deep").as_path())
    );
    assert!(!root.0.join(".data-mover-staging").exists());
    ok(adapter
        .write(&stage, owned_bytes(vec![Bytes::from_static(b"new-bytes")]))
        .await);
    drop(stage);
    let recovered = ok(resume_stage(&adapter, prepare).await);
    assert_eq!(recovered.write_offset, 9);
    ok(adapter
        .publish(
            &recovered,
            PublishRequest {
                expected_size: 9,
                expected_blake3: Some(*blake3::hash(b"new-bytes").as_bytes()),
                cancel: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await
        .map_err(|error| error.error));
    assert_eq!(
        std::fs::read(root.0.join("nested/deep/final.bin"))?,
        b"new-bytes"
    );
    assert!(staging_is_empty(&root.0)?);
    Ok(())
}

#[tokio::test]
async fn publication_resolves_a_replaced_parent_and_preserves_existing_entries() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("replaced-publication-parent");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 2));
    let stage = ok(prepare_ephemeral_stage(&adapter, request(&backend, "nested/final.bin")).await);
    ok(adapter
        .write(&stage, owned_bytes(vec![Bytes::from_static(b"new-bytes")]))
        .await);
    tokio::fs::rename(root.0.join("nested"), root.0.join("moved")).await?;
    tokio::fs::create_dir(root.0.join("nested")).await?;
    tokio::fs::write(root.0.join("nested/unrelated.bin"), b"keep me").await?;
    ok(adapter
        .publish(
            &stage,
            PublishRequest {
                expected_size: 9,
                expected_blake3: Some(*blake3::hash(b"new-bytes").as_bytes()),
                cancel: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await
        .map_err(|error| error.error));
    assert_eq!(
        tokio::fs::read(root.0.join("nested/final.bin")).await?,
        b"new-bytes"
    );
    assert_eq!(
        tokio::fs::read(root.0.join("nested/unrelated.bin")).await?,
        b"keep me"
    );
    assert!(
        tokio::fs::read_dir(root.0.join("moved"))
            .await?
            .next_entry()
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn repeated_writes_normalize_the_previous_tail() -> io::Result<()> {
    for single in [false, true] {
        let root = TestRoot::new().await?;
        let backend = identity("repeated-stage-length");
        let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 2));
        let stage = ok(prepare_ephemeral_stage(&adapter, request(&backend, "final.bin")).await);
        for payload in [b"long payload".as_slice(), b"new", b""] {
            let data = Bytes::copy_from_slice(payload);
            if single {
                ok(adapter.write_single(&stage, data).await);
            } else {
                ok(adapter.write(&stage, owned_bytes(vec![data])).await);
            }
            assert_eq!(
                tokio::fs::read(
                    adapter
                        .stage_full_path(&stage, Operation::Write)
                        .map_err(io::Error::other)?
                )
                .await?,
                payload
            );
        }
        ok(adapter.discard(stage).await);
    }
    Ok(())
}

#[tokio::test]
async fn recovered_stage_with_no_remaining_input_removes_uncheckpointed_tail() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("recovered-stage-tail");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 2));
    let stage = ok(prepare_stage(&adapter, request(&backend, "final.bin")).await);
    ok(adapter.write(&stage, bytes(&[b"abc"])).await);
    let path = ok(adapter.stage_full_path(&stage, Operation::Write));
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)?
        // A torn tail past the durable prefix, still within the source's nine bytes.
        .set_len(9)?;
    drop(stage);
    let recovered = ok(resume_stage(&adapter, request(&backend, "final.bin")).await);
    // Resuming already drops what lies past the durable prefix; writing nothing keeps it so.
    assert_eq!(tokio::fs::metadata(&path).await?.len(), 3);
    ok(adapter.write(&recovered, bytes(&[])).await);
    assert_eq!(tokio::fs::read(path).await?, b"abc");
    ok(adapter.discard(recovered).await);
    Ok(())
}

#[tokio::test]
async fn failed_first_write_does_not_skip_length_normalization_on_retry() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("failed-stage-retry");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 2));
    let stage = ok(prepare_ephemeral_stage(&adapter, request(&backend, "final.bin")).await);
    let error = failure(
        stage.final_destination.path(),
        Operation::Write,
        FailureClass::Cancelled,
    );
    let input = Box::pin(stream::iter([
        Ok(Bytes::from_static(b"partial")),
        Err(error),
    ]));
    assert!(adapter.write(&stage, input).await.is_err());
    ok(adapter.write(&stage, bytes(&[])).await);
    assert_eq!(
        tokio::fs::metadata(ok(adapter.stage_full_path(&stage, Operation::Write)))
            .await?
            .len(),
        0
    );
    ok(adapter.discard(stage).await);
    Ok(())
}

#[tokio::test]
async fn cached_stage_rejects_a_rebound_token() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("cached-stage-binding");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 2));
    let mut first = ok(prepare_ephemeral_stage(&adapter, request(&backend, "final.bin")).await);
    let second = ok(prepare_ephemeral_stage(&adapter, request(&backend, "other.bin")).await);
    let original = first.token.clone();
    first.token = second.token.clone();
    assert!(
        adapter
            .write_single(&first, Bytes::from_static(b"wrong"))
            .await
            .is_err()
    );
    assert_eq!(
        tokio::fs::metadata(ok(adapter.stage_full_path(&second, Operation::Write)))
            .await?
            .len(),
        0
    );
    first.token = original;
    ok(adapter.discard(first).await);
    ok(adapter.discard(second).await);
    Ok(())
}

#[tokio::test]
async fn positioned_write_stores_later_chunk_before_receiving_prefix() -> io::Result<()> {
    use crate::storage::PositionedChunk;
    let root = TestRoot::new().await?;
    let backend = identity("positioned-write");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 2));
    let stage = ok(prepare_stage(&adapter, request_with_size(&backend, "final.bin", 8)).await);
    let path = ok(adapter.stage_full_path(&stage, Operation::Read));
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let input = Box::pin(stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|item| (item, receiver))
    }));
    let produce = async {
        ok(sender
            .send(Ok(PositionedChunk {
                offset: 4,
                data: Bytes::from_static(b"efgh"),
            }))
            .await);
        let observed = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let bytes = tokio::fs::read(&path).await?;
                if bytes.len() == 8 && &bytes[4..] == b"efgh" {
                    return Ok::<_, io::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        // Always release the consumer, even if the positioned write did not progress.
        ok(sender
            .send(Ok(PositionedChunk {
                offset: 0,
                data: Bytes::from_static(b"abcd"),
            }))
            .await);
        drop(sender);
        observed??;
        Ok::<_, io::Error>(())
    };
    let (result, produced) = tokio::join!(adapter.write_positioned(&stage, input), produce);
    produced?;
    assert_eq!(ok(result).persisted_bytes, 8);
    assert_eq!(tokio::fs::read(&path).await?, b"abcdefgh");
    assert_eq!(
        ok(adapter.observe_checkpoint(&stage).await).durable_prefix,
        8
    );
    ok(adapter.discard(stage).await);
    Ok(())
}

#[tokio::test]
async fn positioned_cancel_preserves_only_contiguous_prefix() -> io::Result<()> {
    use crate::storage::PositionedChunk;
    let root = TestRoot::new().await?;
    let backend = identity("positioned-cancel");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 3));
    let stage = ok(prepare_stage(&adapter, request_with_size(&backend, "final.bin", 12)).await);
    let cancelled = failure(
        stage.final_destination.path(),
        Operation::Read,
        FailureClass::Cancelled,
    );
    let input = Box::pin(stream::iter(vec![
        Ok(PositionedChunk {
            offset: 8,
            data: Bytes::from_static(b"ijkl"),
        }),
        Ok(PositionedChunk {
            offset: 0,
            data: Bytes::from_static(b"abcd"),
        }),
        Err(cancelled),
    ]));
    let error = adapter.write_positioned(&stage, input).await;
    assert!(
        matches!(error, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Cancelled)
    );
    assert_eq!(
        ok(adapter.observe_checkpoint(&stage).await).durable_prefix,
        4
    );
    assert_eq!(
        tokio::fs::read(ok(adapter.stage_full_path(&stage, Operation::Read))).await?,
        b"abcd"
    );
    ok(adapter.discard(stage).await);
    Ok(())
}

#[tokio::test]
async fn positioned_eof_with_hole_is_corruption_not_checkpointed_length() -> io::Result<()> {
    use crate::storage::PositionedChunk;
    let root = TestRoot::new().await?;
    let backend = identity("positioned-hole");
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 2));
    let stage = ok(prepare_stage(&adapter, request_with_size(&backend, "final.bin", 8)).await);
    let input = Box::pin(stream::iter(vec![Ok(PositionedChunk {
        offset: 4,
        data: Bytes::from_static(b"efgh"),
    })]));
    assert!(
        matches!(adapter.write_positioned(&stage, input).await, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Corruption)
    );
    assert_eq!(
        ok(adapter.observe_checkpoint(&stage).await).durable_prefix,
        0
    );
    assert_eq!(
        tokio::fs::metadata(ok(adapter.stage_full_path(&stage, Operation::Read)))
            .await?
            .len(),
        0
    );
    ok(adapter.discard(stage).await);
    Ok(())
}

#[tokio::test]
async fn positioned_checkpoint_persistence_overlaps_the_next_write_window() -> io::Result<()> {
    let root = TestRoot::new().await?;
    let backend = identity("positioned-checkpoint-overlap");
    let adapter = Arc::new(ok(LocalStagedDestination::new(&root.0, backend.clone(), 2)));
    let stage = ok(prepare_stage(&adapter, request(&backend, "final.bin")).await);
    let probe = Arc::clone(&adapter.write_probe);
    probe.pause_checkpoint.store(true, Ordering::SeqCst);
    let writing = Arc::clone(&adapter);
    let writer = tokio::spawn(async move {
        let input = stream::iter((0..5).map(|index| {
            Ok(crate::storage::PositionedChunk {
                offset: index * 64 * 1024,
                data: Bytes::from(vec![42; 64 * 1024]),
            })
        }));
        let result = writing.write_positioned(&stage, Box::pin(input)).await;
        (stage, result)
    });
    tokio::time::timeout(Duration::from_secs(3), probe.checkpoint_started.notified()).await?;
    let advanced = tokio::time::timeout(Duration::from_secs(1), async {
        while adapter.write_completion_count() < 5 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    probe.checkpoint_release.notify_one();
    let (stage, result) = writer.await?;
    assert_eq!(ok(result).persisted_bytes, 5 * 64 * 1024);
    assert!(
        advanced.is_ok(),
        "checkpoint persistence stalled subsequent writes"
    );
    assert_eq!(
        tokio::fs::read(
            adapter
                .stage_full_path(&stage, Operation::Read)
                .map_err(io::Error::other)?
        )
        .await?,
        vec![42; 5 * 64 * 1024]
    );
    assert_eq!(
        ok(adapter.observe_checkpoint(&stage).await).durable_prefix,
        5 * 64 * 1024
    );
    ok(adapter.discard(stage).await);
    Ok(())
}

#[tokio::test]
async fn positioned_checkpoint_distinguishes_final_crossing_from_earlier_boundary() -> io::Result<()>
{
    use crate::storage::{DeferredCheckpoint, PositionedChunk};
    for (size, offsets, expected) in [(8, vec![4, 0], 0), (12, vec![8, 4, 0], 1)] {
        let root = TestRoot::new().await?;
        let backend = identity("positioned-checkpoint-boundaries");
        let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 3));
        let mut stage = ok(prepare_ephemeral_stage(
            &adapter,
            request_with_size(&backend, "final.bin", size),
        )
        .await);
        stage.deferred_checkpoint = Some(DeferredCheckpoint {
            interval_bytes: 6,
            source_size: size as u64,
        });
        let input = Box::pin(stream::iter(offsets.into_iter().map(|offset| {
            Ok(PositionedChunk {
                offset,
                data: Bytes::from_static(b"abcd"),
            })
        })));
        assert_eq!(
            ok(adapter.write_positioned(&stage, input).await).persisted_bytes,
            size as u64
        );
        // The pointer is the only record: a checkpoint shows as a pointer write.
        assert_eq!(
            observed_checkpoint_prefixes(&adapter).is_empty(),
            expected == 0
        );
        assert_eq!(stage.recovery_enabled(), expected != 0);
        ok(adapter.discard(stage).await);
    }
    Ok(())
}

async fn send_positioned_then_wait_for_next_poll(
    sender: &tokio::sync::mpsc::Sender<Result<crate::storage::PositionedChunk, StorageRoleFailure>>,
    polled: &mut tokio::sync::mpsc::UnboundedReceiver<()>,
    offset: u64,
    length: usize,
) -> io::Result<()> {
    sender
        .send(Ok(crate::storage::PositionedChunk {
            offset,
            data: Bytes::from(vec![42; length]),
        }))
        .await
        .map_err(io::Error::other)?;
    tokio::time::timeout(Duration::from_secs(3), polled.recv())
        .await?
        .ok_or_else(|| io::Error::other("positioned writer stopped polling"))
}

async fn wait_for_durable_prefix(
    adapter: &LocalStagedDestination,
    stage: &PreparedStage,
    prefix: u64,
) -> io::Result<()> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if stage.recovery_enabled()
                && adapter
                    .observe_checkpoint(stage)
                    .await
                    .is_ok_and(|value| value.durable_prefix == prefix)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

fn observed_checkpoint_prefixes(adapter: &LocalStagedDestination) -> Vec<u64> {
    adapter
        .write_probe
        .checkpoint_prefixes
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

#[tokio::test]
async fn positioned_sparse_completion_crosses_multiple_intervals_once() -> io::Result<()> {
    use crate::storage::DeferredCheckpoint;
    let root = TestRoot::new().await?;
    let backend = identity("positioned-sparse-threshold");
    // Depth one makes the next input poll an acknowledgement barrier for the prior write.
    let adapter = ok(LocalStagedDestination::new(&root.0, backend.clone(), 1));
    let mut stage =
        ok(prepare_ephemeral_stage(&adapter, request_with_size(&backend, "final.bin", 24)).await);
    stage.deferred_checkpoint = Some(DeferredCheckpoint {
        interval_bytes: 4,
        source_size: 24,
    });
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let (poll_sender, mut polled) = tokio::sync::mpsc::unbounded_channel();
    let input = Box::pin(stream::unfold(
        (receiver, poll_sender),
        |(mut receiver, poll_sender)| async move {
            let _ = poll_sender.send(());
            receiver
                .recv()
                .await
                .map(|item| (item, (receiver, poll_sender)))
        },
    ));
    let produce = async {
        tokio::time::timeout(Duration::from_secs(3), polled.recv()).await?;
        send_positioned_then_wait_for_next_poll(&sender, &mut polled, 16, 4).await?;
        send_positioned_then_wait_for_next_poll(&sender, &mut polled, 4, 8).await?;
        assert!(!stage.recovery_enabled());
        assert!(observed_checkpoint_prefixes(&adapter).is_empty());
        // Filling 0..4 joins 4..12. The sparse 16..20 suffix is still ineligible.
        send_positioned_then_wait_for_next_poll(&sender, &mut polled, 0, 4).await?;
        wait_for_durable_prefix(&adapter, &stage, 12).await?;
        assert_eq!(observed_checkpoint_prefixes(&adapter), [12]);
        assert_eq!(
            tokio::fs::metadata(ok(adapter.stage_full_path(&stage, Operation::Read)))
                .await?
                .len(),
            20
        );
        // Acknowledged prefix 14 must not trigger a catch-up checkpoint at 4 or 8.
        send_positioned_then_wait_for_next_poll(&sender, &mut polled, 12, 2).await?;
        assert_eq!(
            ok(adapter.observe_checkpoint(&stage).await).durable_prefix,
            12
        );
        // Crossing the new threshold 16 connects the already-written suffix to 20.
        send_positioned_then_wait_for_next_poll(&sender, &mut polled, 14, 2).await?;
        wait_for_durable_prefix(&adapter, &stage, 20).await?;
        assert_eq!(observed_checkpoint_prefixes(&adapter), [12, 20]);
        send_positioned_then_wait_for_next_poll(&sender, &mut polled, 20, 4).await?;
        drop(sender);
        Ok::<_, io::Error>(())
    };
    let (written, produced) = tokio::join!(adapter.write_positioned(&stage, input), produce);
    produced?;
    assert_eq!(ok(written).persisted_bytes, 24);
    // Two periodic checkpoints plus the final checkpoint, no redundant threshold replay.
    assert_eq!(observed_checkpoint_prefixes(&adapter), [12, 20, 24]);
    assert_eq!(
        tokio::fs::read(ok(adapter.stage_full_path(&stage, Operation::Read))).await?,
        vec![42; 24]
    );
    ok(adapter.discard(stage).await);
    Ok(())
}
