//! Optional HDFS contracts exercised through the storage roles and transfer engine.
//! Raw HDFS protocol calls seed/read fixtures only; focused backend contracts live in
//! `hdfs_native_contract`.
use std::collections::BTreeSet;
use std::env;
use std::num::NonZeroUsize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use data_mover::model::{EntryOperationFailure, FailureClass, Operation, StoragePath, Transience};
use data_mover::storage::{
    BackendConfig, ByteStream, DestinationPrepareRequest, FinalDestination, HdfsBackendConfig,
    PreflightPolicy, PrepareFact, PrepareRequest, PublishRequest, ResumeMode, SourceDescriptor,
    Storage, StorageRoleFailure, VerifyRequest, connect_backend,
};
use data_mover::storage::{HdfsConfig, HdfsKerberosCredentials, HdfsLocation, build_hdfs_client};
use data_mover::transfer::{InflightLimits, TransferIdentity, TransferRequest, transfer};
use data_mover::traversal::{
    Checkpoint, Resumable, StorageTraversalSource, TraversalItem, TraversalOrder, TraversalOutcome,
    TraversalRequest, TraversalSource as _,
};
use hdfs_native::{Client, WriteOptions};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use tokio_util::sync::CancellationToken;

mod common;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn lab_config() -> HdfsConfig {
    HdfsConfig {
        config_dir: env::var_os("LAB_HDFS_CONFIG_DIR").map(Into::into),
        kerberos_credentials: env::var_os("LAB_HDFS_KEYTAB").map(|keytab| {
            HdfsKerberosCredentials {
                keytab: Some(keytab.into()),
                ..Default::default()
            }
        }),
        ..Default::default()
    }
}

fn lab_location(case: &str) -> TestResult<String> {
    let root = HdfsLocation::parse_configured(&env::var("LAB_HDFS_RUN_ROOT")?, &lab_config())?;
    let endpoint = root
        .endpoint()
        .strip_prefix("hdfs://")
        .ok_or("invalid endpoint")?;
    let user = utf8_percent_encode(root.user(), NON_ALPHANUMERIC);
    let run = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(format!(
        "hdfs://{user}@{endpoint}{}/{case}-{run}",
        root.root()
    ))
}

async fn connect_hdfs(location: &str) -> TestResult<Storage> {
    Ok(connect_backend(BackendConfig::Hdfs(HdfsBackendConfig {
        location: location.to_owned(),
        client: lab_config(),
        block_size: None,
        ensure_dir: true,
    }))
    .await?)
}

fn raw_client(location: &str) -> TestResult<(Client, HdfsLocation)> {
    Ok(build_hdfs_client(location, &lab_config())?)
}

async fn create_file(client: &Client, root: &HdfsLocation, path: &str, data: Bytes) -> TestResult {
    let full_path = format!("{}/{path}", root.root().trim_end_matches('/'));
    let mut writer = client
        .create(
            &full_path,
            WriteOptions::default().overwrite(true).create_parent(true),
        )
        .await?;
    Box::pin(writer.write_bytes(data)).await?;
    Box::pin(writer.close()).await?;
    Ok(())
}

async fn read_file(client: &Client, root: &HdfsLocation, path: &str) -> TestResult<Bytes> {
    let full_path = format!("/{}/{}", root.root().trim_matches('/'), path);
    let size = client.get_file_info(&full_path).await?.length;
    Ok(client.read(&full_path).await?.read_range(0, size).await?)
}

async fn cleanup(storage: &Storage) -> TestResult {
    storage
        .namespace(&PreflightPolicy::production())?
        .delete_subtree(&StoragePath::root(), true)
        .await?;
    Ok(())
}

async fn observes_nested_source(storage: &Storage) -> TestResult<bool> {
    let traversal = StorageTraversalSource::new(storage)?;
    let slots = NonZeroUsize::new(2).ok_or("invalid inflight")?;
    let mut session = traversal.traverse(
        TraversalRequest::new(StoragePath::root())
            .max_inflight_operations(slots)
            .max_buffered_items(slots)
            .cancel(CancellationToken::new()),
    );
    let mut observed = false;
    while let Some(item) = session.next_item().await {
        if let TraversalItem::Entry(entry) = item {
            observed |= entry.path().as_str() == "nested/source.bin";
        }
    }
    session.finish().await?;
    Ok(observed)
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn architecture_roles_traverse_stream_verify_and_overwrite() -> TestResult {
    let source_location = lab_location("architecture-source")?;
    let destination_location = lab_location("architecture-destination")?;
    let source = connect_hdfs(&source_location).await?;
    let destination = connect_hdfs(&destination_location).await?;
    let (source_client, source_root) = raw_client(&source_location)?;
    let (destination_client, destination_root) = raw_client(&destination_location)?;
    let payload = Bytes::from(vec![0x5a; 1024 * 1024 + 31]);
    create_file(
        &source_client,
        &source_root,
        "nested/source.bin",
        payload.clone(),
    )
    .await?;
    create_file(
        &destination_client,
        &destination_root,
        "published/final.bin",
        Bytes::from_static(b"old"),
    )
    .await?;
    let outcome = transfer(
        TransferRequest::new(
            source.clone(),
            StoragePath::new("nested/source.bin")?,
            destination.clone(),
            StoragePath::new("published/final.bin")?,
            InflightLimits::new(2, 128 * 1024, 2)?,
            CancellationToken::new(),
        )
        .with_identity_override(TransferIdentity::from_label("hdfs-architecture-contract")?),
    )
    .await?;
    assert_eq!(outcome.blake3, Some(*blake3::hash(&payload).as_bytes()));
    assert!(observes_nested_source(&source).await?);
    assert_eq!(
        read_file(
            &destination_client,
            &destination_root,
            "published/final.bin"
        )
        .await?,
        payload
    );
    cleanup(&source).await?;
    cleanup(&destination).await?;
    Ok(())
}

/// The old public chunk callback allowed arbitrary offsets. The replacement contract exercises
/// exact multi-block content through bounded role transfer at the native writer boundary; the
/// staged HDFS writer's out-of-order completion logic is covered by its focused unit tests.
#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn native_writer_crosses_block_boundary_through_bounded_transfer() -> TestResult {
    let source_location = lab_location("architecture-writer-source")?;
    let destination_location = lab_location("architecture-writer-destination")?;
    let source = connect_hdfs(&source_location).await?;
    let destination = connect_hdfs(&destination_location).await?;
    let (source_client, source_root) = raw_client(&source_location)?;
    let (destination_client, destination_root) = raw_client(&destination_location)?;
    let payload = Bytes::from(vec![0x3c; 3 * 1024 * 1024 + 257]);
    create_file(&source_client, &source_root, "source.bin", payload.clone()).await?;
    let outcome = transfer(TransferRequest::new(
        source.clone(),
        StoragePath::new("source.bin")?,
        destination.clone(),
        StoragePath::new("reordered.bin")?,
        InflightLimits::new(2, 1024 * 1024, 3)?,
        CancellationToken::new(),
    ))
    .await?;
    assert_eq!(outcome.blake3, Some(*blake3::hash(&payload).as_bytes()));
    assert_eq!(
        read_file(&destination_client, &destination_root, "reordered.bin").await?,
        payload
    );
    cleanup(&source).await?;
    cleanup(&destination).await?;
    Ok(())
}

fn interrupted_input(prefix: Bytes) -> TestResult<ByteStream> {
    let failure = EntryOperationFailure::new(
        StoragePath::new("source")?,
        Operation::Read,
        FailureClass::Cancelled,
        Transience::Transient,
        "injected HDFS recovery interruption",
    )?;
    Ok(Box::pin(futures::stream::iter([
        Ok(prefix),
        Err(StorageRoleFailure::Entry(failure)),
    ])))
}

fn resume_request(descriptor: &SourceDescriptor) -> TestResult<DestinationPrepareRequest> {
    Ok(DestinationPrepareRequest::new(
        PrepareRequest {
            final_destination: FinalDestination::new(StoragePath::new("final.bin")?),
            source: descriptor.clone(),
            recovery_binding: [0x34; 32],
        },
        [0x51; 32],
    ))
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn architecture_stage_recovers_durable_prefix_after_reconnect() -> TestResult {
    let location = lab_location("architecture-recovery")?;
    let backend = connect_hdfs(&location).await?;
    let (client, root) = raw_client(&location)?;
    let payload = Bytes::from(vec![0x6d; 2 * 1024 * 1024]);
    create_file(&client, &root, "source.bin", payload.clone()).await?;
    let descriptor = interrupt_stage(&backend, &payload).await?;
    drop(backend);
    let reconnected = connect_hdfs(&location).await?;
    let roles = reconnected.clone();
    let staged = roles.staged_destination(&PreflightPolicy::production())?;
    let resumed = staged
        .prepare_at_destination(resume_request(&descriptor)?)
        .await?;
    assert_eq!(
        resumed.prepare_fact(),
        PrepareFact::Resumed { bytes: 1024 * 1024 }
    );
    staged
        .write(
            &resumed,
            Box::pin(futures::stream::iter([Ok(payload.slice(1024 * 1024..))])),
        )
        .await?;
    let digest = *blake3::hash(&payload).as_bytes();
    staged
        .verify(
            &resumed,
            VerifyRequest {
                expected_size: payload.len() as u64,
                expected_blake3: digest,
                cancel: CancellationToken::new(),
                published: None,
            },
        )
        .await?;
    staged
        .publish(
            &resumed,
            PublishRequest {
                expected_size: payload.len() as u64,
                expected_blake3: Some(digest),
                cancel: CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(read_file(&client, &root, "final.bin").await?, payload);
    assert_no_artifacts(&reconnected).await?;
    cleanup(&reconnected).await?;
    Ok(())
}

async fn interrupt_stage(backend: &Storage, payload: &Bytes) -> TestResult<SourceDescriptor> {
    let descriptor = backend
        .read_source(&PreflightPolicy::production())?
        .describe(&StoragePath::new("source.bin")?)
        .await?;
    let staged = backend.staged_destination(&PreflightPolicy::production())?;
    let stage = staged
        .prepare_at_destination(resume_request(&descriptor)?.with_resume(ResumeMode::Restart))
        .await?;
    assert!(!matches!(stage.prepare_fact(), PrepareFact::Resumed { .. }));
    assert!(
        staged
            .write(&stage, interrupted_input(payload.slice(..1024 * 1024))?)
            .await
            .is_err()
    );
    assert_eq!(
        staged.observe_checkpoint(&stage).await?.durable_prefix,
        1024 * 1024
    );
    drop(stage);
    Ok(descriptor)
}

async fn assert_no_artifacts(storage: &Storage) -> TestResult {
    let traversal = StorageTraversalSource::new(storage)?;
    let mut session = traversal
        .traverse(TraversalRequest::new(StoragePath::root()).cancel(CancellationToken::new()));
    while let Some(item) = session.next_item().await {
        if let TraversalItem::Entry(entry) = item {
            assert!(
                !entry.path().as_str().starts_with(".data-mover-"),
                "artifact left: {}",
                entry.path()
            );
        }
    }
    session.finish().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn architecture_traversal_resumes_after_cancel() -> TestResult {
    let location = lab_location("traversal-resume")?;
    let hdfs = connect_hdfs(&location).await?;
    let (client, root) = raw_client(&location)?;
    let checked = async {
        seed_resume_tree(&client, &root).await?;
        let source = StorageTraversalSource::new(&hdfs)?;
        for order in [TraversalOrder::Unordered, TraversalOrder::Blocks] {
            resume_chain(&source, order).await?;
            resume_from_part_way(&source, order).await?;
        }
        TestResult::Ok(())
    }
    .await;
    let cleanup_result = cleanup(&hdfs).await;
    checked?;
    cleanup_result?;
    Ok(())
}

async fn seed_resume_tree(client: &Client, root: &HdfsLocation) -> TestResult {
    for top in 0..6 {
        for sub in 0..3 {
            for file in 0..8 {
                create_file(
                    client,
                    root,
                    &format!("a{top}/s{sub}/f{file}"),
                    Bytes::from_static(b"x"),
                )
                .await?;
            }
        }
    }
    create_file(
        client,
        root,
        &format!("chain{}/leaf", "/c".repeat(10)),
        Bytes::new(),
    )
    .await?;
    for file in 0..60 {
        create_file(client, root, &format!("wide/w{file:02}"), Bytes::new()).await?;
    }
    Ok(())
}

type Traversed = (Vec<String>, Vec<(usize, Checkpoint)>, TraversalOutcome);

async fn traverse_resumable(
    source: &StorageTraversalSource,
    request: TraversalRequest,
    cancel_after: Option<usize>,
) -> TestResult<Traversed> {
    let cancel = request.cancel.clone();
    let mut session = source.traverse(request);
    let (mut entries, mut checkpoints) = (Vec::new(), Vec::new());
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => entries.push(entry.path().as_str().to_owned()),
            TraversalItem::Checkpoint(checkpoint) => checkpoints.push((entries.len(), *checkpoint)),
            _ => {}
        }
        if cancel_after == Some(entries.len()) {
            cancel.cancel();
        }
    }
    Ok((entries, checkpoints, session.finish().await?))
}

fn traversal_request(order: TraversalOrder) -> TestResult<TraversalRequest> {
    let slots = NonZeroUsize::new(8).ok_or("zero")?;
    Ok(TraversalRequest::new(StoragePath::root())
        .order(order)
        .max_inflight_operations(slots)
        .max_buffered_items(slots))
}

async fn resume_chain(source: &StorageTraversalSource, order: TraversalOrder) -> TestResult {
    let (full, _, _) = traverse_resumable(source, traversal_request(order)?, None).await?;
    let full: BTreeSet<String> = full.into_iter().collect();
    let (mut delivered, mut from, mut rounds) = (Vec::new(), None::<Checkpoint>, 0);
    loop {
        rounds += 1;
        assert!(rounds < 100, "{order:?}: no progress");
        let mut request = traversal_request(order)?.resumable(Resumable::on());
        if let Some(checkpoint) = from.take() {
            request = request.resume_from(checkpoint);
        }
        let (entries, checkpoints, outcome) = traverse_resumable(source, request, Some(90)).await?;
        delivered.extend(entries);
        if matches!(outcome, TraversalOutcome::Completed(_)) {
            break;
        }
        from = Some(checkpoints.last().ok_or("no final checkpoint")?.1.clone());
    }
    let union: BTreeSet<String> = delivered.iter().cloned().collect();
    assert_eq!(union, full, "{order:?}");
    Ok(())
}

async fn resume_from_part_way(
    source: &StorageTraversalSource,
    order: TraversalOrder,
) -> TestResult {
    let every = Resumable::On {
        every: Duration::ZERO,
    };
    let (full, checkpoints, _) =
        traverse_resumable(source, traversal_request(order)?.resumable(every), None).await?;
    let all: BTreeSet<String> = full.iter().cloned().collect();
    let step = (checkpoints.len() / 8).max(1);
    for (at, checkpoint) in checkpoints.iter().step_by(step) {
        let request = traversal_request(order)?.resume_from(checkpoint.clone());
        let (resumed, _, outcome) = traverse_resumable(source, request, None).await?;
        assert!(matches!(outcome, TraversalOutcome::Completed(_)));
        let union: BTreeSet<String> = full[..*at].iter().chain(&resumed).cloned().collect();
        assert_eq!(
            union, all,
            "{order:?} from the checkpoint after {at} entries"
        );
    }
    Ok(())
}
