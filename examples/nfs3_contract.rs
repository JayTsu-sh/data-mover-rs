use std::error::Error;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use data_mover::model::{
    BackendIdentity, BackendKind, FailureClass, MetadataObservation, ObservationMode,
    ObservationPlan, OwnershipMode, StoragePath, Transience,
};
use data_mover::storage::{
    ByteStream, ExistingDestinationPolicy, FinalDestination, MetadataMutation, PreflightPolicy,
    PrepareRequest, PublishRequest, RecoverRequest, Storage, StorageRoleFailure, VerifyRequest,
};
use data_mover::transfer::{
    InflightLimits, SourceQosGroup, SourceQosPolicy, TransferIdentity, TransferRequest, transfer,
};
use data_mover::traversal::{
    StorageTraversalSource, TraversalItem, TraversalOrder, TraversalOutcome, TraversalRequest,
    TraversalSource,
};
use futures::stream;
use tokio_util::sync::CancellationToken;

type ContractResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Debug, Parser)]
#[command(about = "Run an ArchitectureReady NFS dialect contract against two real roots")]
struct Args {
    #[arg(long, value_enum, default_value_t = ContractDialect::Nfs3)]
    dialect: ContractDialect,
    #[arg(long)]
    source: String,
    #[arg(long)]
    destination: String,
    #[arg(long, requires = "seed_root")]
    seed_mount: Option<String>,
    #[arg(long, requires = "seed_mount")]
    seed_root: Option<String>,
    #[arg(long, requires = "stale_go_file")]
    stale_ready_file: Option<PathBuf>,
    #[arg(long, requires = "stale_ready_file")]
    stale_go_file: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum ContractDialect {
    Nfs3,
    Nfs40,
}

async fn validate_stale_retry(
    source: &Storage,
    ready: Option<&Path>,
    go: Option<&Path>,
) -> ContractResult {
    let (Some(ready), Some(go)) = (ready, go) else {
        return Ok(());
    };
    let metadata = source.metadata(&PreflightPolicy::production())?;
    let stale_path = path("stale/fixture.bin")?;
    metadata
        .observe(
            &stale_path,
            ObservationPlan::default().with_ownership_mode(ObservationMode::InlineOnly),
        )
        .await?;
    tokio::fs::write(ready, b"ready\n").await?;
    for _ in 0..3_000 {
        if tokio::fs::try_exists(go).await? {
            metadata
                .apply(
                    &stale_path,
                    MetadataMutation::NumericOwnership(OwnershipMode {
                        uid: 0,
                        gid: 0,
                        mode: 0o644,
                    }),
                    CancellationToken::new(),
                )
                .await?;
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err("stale-handle mutation barrier timed out".into())
}

fn path(value: &str) -> ContractResult<StoragePath> {
    Ok(StoragePath::new(value)?)
}

fn identity(id: &str) -> ContractResult<BackendIdentity> {
    Ok(BackendIdentity::new(BackendKind::Nfs, id)?)
}

fn nonzero(value: usize) -> ContractResult<NonZeroUsize> {
    NonZeroUsize::new(value)
        .ok_or_else(|| std::io::Error::other("contract bound must be nonzero").into())
}

async fn seed_v40_fixture(mount_url: &str, root: &str) -> ContractResult {
    let mount = nfs_rs::parse_url_and_mount(mount_url).await?;
    let mut prefix = String::new();
    for component in root.trim_matches('/').split('/') {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(component);
        if mount.lookup_path(&prefix).await.is_err() {
            mount.mkdir_path(&prefix, 0o700).await?;
        }
    }
    for relative in ["dir", "stale"] {
        let directory = format!("{root}/{relative}");
        if mount.lookup_path(&directory).await.is_err() {
            mount.mkdir_path(&directory, 0o700).await?;
        }
    }
    for (relative, payload) in [
        (
            "fixture.bin",
            b"architecture-ready-nfs40-fixture".as_slice(),
        ),
        ("dir/nested.bin", b"nested".as_slice()),
        ("stale/fixture.bin", b"stale-handle-fixture".as_slice()),
    ] {
        let file = format!("{root}/{relative}");
        let _ = mount.remove_path(&file).await;
        let created = mount.create_path(&file, Some(0o600)).await?;
        let count = mount
            .write(created.fh.clone(), 0, Bytes::copy_from_slice(payload))
            .await?;
        if usize::try_from(count)? != payload.len() {
            return Err("NFSv4.0 fixture write was short".into());
        }
        mount.commit(created.fh.clone(), 0, count).await?;
        mount.close(created.fh).await?;
    }
    let link = format!("{root}/fixture.link");
    let _ = mount.remove_path(&link).await;
    mount.symlink_path("fixture.bin", &link).await?;
    mount.umount().await?;
    Ok(())
}

async fn validate_v40_stale_retry(source: &Storage, mount_url: &str, root: &str) -> ContractResult {
    let metadata = source.metadata(&PreflightPolicy::production())?;
    let stale = path("stale/fixture.bin")?;
    metadata
        .observe(
            &stale,
            ObservationPlan::default().with_ownership_mode(ObservationMode::InlineOnly),
        )
        .await?;

    let mount = nfs_rs::parse_url_and_mount(mount_url).await?;
    let native = format!("{root}/stale/fixture.bin");
    let old = mount.lookup_path(&native).await?;
    mount.remove_path(&native).await?;
    let replacement = mount.create_path(&native, Some(0o600)).await?;
    let payload = Bytes::from_static(b"stale-handle-fixture");
    let count = mount.write(replacement.fh.clone(), 0, payload).await?;
    mount.commit(replacement.fh.clone(), 0, count).await?;
    mount.close(replacement.fh).await?;
    let fresh = mount.lookup_path(&native).await?;
    if old.attr.as_ref().map(|attr| attr.fileid) == fresh.attr.as_ref().map(|attr| attr.fileid) {
        return Err("DXN stale fixture did not replace the file identity".into());
    }
    mount.umount().await?;

    metadata
        .apply(
            &stale,
            MetadataMutation::NumericOwnership(OwnershipMode {
                uid: 0,
                gid: 0,
                mode: 0o644,
            }),
            CancellationToken::new(),
        )
        .await?;
    Ok(())
}

async fn connect_destination(url: &str) -> ContractResult<Storage> {
    Ok(data_mover::nfs::create_nfs_role_storage(
        url,
        Some(64 * 1024),
        true,
        identity("nfs3-contract-destination")?,
    )
    .await?)
}

async fn validate_traversal(source: &Storage, dialect: ContractDialect) -> ContractResult {
    let traversal = StorageTraversalSource::new(source)?;
    let acl_mode = match dialect {
        ContractDialect::Nfs3 => ObservationMode::Required,
        ContractDialect::Nfs40 => ObservationMode::BestEffort,
    };
    let mut session = traversal.traverse(TraversalRequest {
        root: StoragePath::root(),
        order: TraversalOrder::Admission,
        max_inflight_operations: nonzero(4)?,
        max_buffered_items: nonzero(2)?,
        observation_plan: ObservationPlan::default()
            .with_acl(acl_mode)
            .with_xattrs(ObservationMode::Required)
            .with_ownership_mode(ObservationMode::InlineOnly)
            .with_timestamps(ObservationMode::InlineOnly),
        cancel: CancellationToken::new(),
    });
    let mut saw_fixture = false;
    let mut saw_link = false;
    while let Some(item) = session.next_item().await {
        let TraversalItem::Entry(entry) = item else {
            return Err("fixture traversal returned an entry failure".into());
        };
        saw_fixture |= entry.path().as_str() == "fixture.bin";
        saw_link |= entry.path().as_str() == "fixture.link" && entry.symlink_target().is_some();
        if entry.path().as_str() == "fixture.bin" {
            match dialect {
                ContractDialect::Nfs3 => assert!(matches!(
                    entry.metadata().acl(),
                    MetadataObservation::Unsupported
                )),
                ContractDialect::Nfs40 => {
                    if !matches!(entry.metadata().acl(), MetadataObservation::Value { .. }) {
                        return Err(format!(
                            "DXN NFSv4.0 GETACL observation was not typed unsupported: {:?}",
                            entry.metadata().acl()
                        )
                        .into());
                    }
                }
            }
            assert!(matches!(
                entry.metadata().xattrs(),
                MetadataObservation::Unsupported
            ));
        }
    }
    assert!(saw_fixture && saw_link);
    assert!(matches!(
        session.finish().await?,
        TraversalOutcome::Completed(_)
    ));
    Ok(())
}

fn assert_acl_set_unsupported<T>(result: Result<T, StorageRoleFailure>) -> ContractResult {
    match result {
        Err(StorageRoleFailure::Entry(error))
            if error.class() == FailureClass::Unsupported
                && error.transience() == Transience::Permanent =>
        {
            Ok(())
        }
        Ok(_) => Err("DXN NFSv4.0 unexpectedly accepted an ACL operation".into()),
        Err(error) => Err(format!("unexpected DXN NFSv4.0 ACL result: {error:?}").into()),
    }
}

async fn validate_acl_negative(source: &Storage, dialect: ContractDialect) -> ContractResult {
    if dialect == ContractDialect::Nfs3 {
        return Ok(());
    }
    let metadata = source.metadata(&PreflightPolicy::production())?;
    let fixture = path("fixture.bin")?;
    let observed = metadata
        .observe(
            &fixture,
            ObservationPlan::default().with_acl(ObservationMode::Required),
        )
        .await;
    let acl_to_set = match observed {
        Ok(observations) => match observations.acl() {
            MetadataObservation::Value { value, .. } => value.clone(),
            other => return Err(format!("unexpected DXN GETACL observation: {other:?}").into()),
        },
        Err(error) => return Err(format!("unexpected DXN GETACL failure: {error:?}").into()),
    };
    assert_acl_set_unsupported(
        metadata
            .apply(
                &fixture,
                MetadataMutation::Acl(acl_to_set),
                CancellationToken::new(),
            )
            .await,
    )?;
    Ok(())
}

async fn validate_streaming_copy(source: &Storage, destination: &Storage) -> ContractResult {
    let outcome = transfer(
        TransferRequest::new(
            TransferIdentity::new("nfs3-contract-stream-copy")?,
            source.clone(),
            path("fixture.bin")?,
            destination.clone(),
            path("copied.bin")?,
            InflightLimits::new(4, 256 * 1024, 4)?,
            CancellationToken::new(),
        )
        .with_source_qos(SourceQosGroup::new(SourceQosPolicy::new(
            Some((1024 * 1024, 1024 * 1024, Duration::ZERO)),
            64 * 1024,
            Some((100, 100, Duration::ZERO)),
        )?))
        .with_existing_destination_policy(ExistingDestinationPolicy::Overwrite),
    )
    .await?;
    assert!(outcome.transferred_bytes > 0);
    assert_eq!(
        outcome.source_qos.client_streamed_shaped_bytes,
        outcome.transferred_bytes
    );
    assert!(outcome.source_qos.source_read_operations > 0);
    Ok(())
}

async fn validate_recovery(source: &Storage, destination: Storage, url: &str) -> ContractResult {
    let policy = PreflightPolicy::production();
    let descriptor = source
        .read_source(&policy)?
        .describe(&path("fixture.bin")?)
        .await?;
    let destination_role = destination.staged_destination(&policy)?;
    let recovery_binding = [42; 32];
    let stage = destination_role
        .prepare(PrepareRequest {
            final_destination: FinalDestination::new(path("recovered.bin")?),
            source: descriptor.clone(),
            recovery_binding,
        })
        .await?;
    let payload = b"nfs3 durable recovery contract";
    let partial: ByteStream = Box::pin(stream::iter([Ok(Bytes::copy_from_slice(&payload[..11]))]));
    destination_role.write(&stage, partial).await?;
    let recovery_identity = destination_role.recovery_identity(&stage).await?;
    drop(destination_role);
    drop(destination);

    let first = connect_destination(url).await?;
    let second = connect_destination(url).await?;
    let first_role = first.staged_destination(&policy)?;
    let second_role = second.staged_destination(&policy)?;
    let first_request = RecoverRequest {
        identity: recovery_identity.clone(),
        final_destination: FinalDestination::new(path("recovered.bin")?),
        source: descriptor.clone(),
        recovery_binding,
        claim_token: [1; 32],
    };
    let second_request = RecoverRequest {
        identity: recovery_identity,
        final_destination: FinalDestination::new(path("recovered.bin")?),
        source: descriptor,
        recovery_binding,
        claim_token: [2; 32],
    };
    let (first_result, second_result) = tokio::join!(
        first_role.recover(first_request),
        second_role.recover(second_request)
    );
    let (recovered_role, recovered, loser) = match (first_result, second_result) {
        (Ok(stage), Err(error)) => (first_role, stage, error),
        (Err(error), Ok(stage)) => (second_role, stage, error),
        (left, right) => {
            return Err(format!(
                "exactly one real NFS recovery claim must succeed: {left:?}, {right:?}"
            )
            .into());
        }
    };
    assert!(
        matches!(
            &loser,
        StorageRoleFailure::Entry(error)
            if matches!(error.class(), FailureClass::Conflict | FailureClass::NotFound)
        ),
        "unexpected losing recovery claim: {loser:?}"
    );
    let remainder: ByteStream =
        Box::pin(stream::iter([Ok(Bytes::copy_from_slice(&payload[11..]))]));
    recovered_role.write(&recovered, remainder).await?;
    let hash = *blake3::hash(payload).as_bytes();
    recovered_role
        .verify(
            &recovered,
            VerifyRequest {
                expected_size: payload.len() as u64,
                expected_blake3: hash,
                cancel: CancellationToken::new(),
            },
        )
        .await?;
    recovered_role
        .publish(
            &recovered,
            PublishRequest {
                policy: ExistingDestinationPolicy::Overwrite,
                expected_size: payload.len() as u64,
                expected_blake3: hash,
                cancel: CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    Ok(())
}

#[tokio::main]
async fn main() -> ContractResult {
    let args = Args::parse();
    if let (Some(mount), Some(root)) = (&args.seed_mount, &args.seed_root) {
        seed_v40_fixture(mount, root).await?;
    }
    let source = data_mover::nfs::create_nfs_role_storage(
        &args.source,
        Some(64 * 1024),
        false,
        identity("nfs3-contract-source")?,
    )
    .await?;
    let destination = data_mover::nfs::create_nfs_role_storage(
        &args.destination,
        Some(64 * 1024),
        true,
        identity("nfs3-contract-destination")?,
    )
    .await?;
    validate_stale_retry(
        &source,
        args.stale_ready_file.as_deref(),
        args.stale_go_file.as_deref(),
    )
    .await?;
    if args.dialect == ContractDialect::Nfs40 {
        let mount = args
            .seed_mount
            .as_deref()
            .ok_or("NFSv4.0 contract requires --seed-mount")?;
        let root = args
            .seed_root
            .as_deref()
            .ok_or("NFSv4.0 contract requires --seed-root")?;
        validate_v40_stale_retry(&source, mount, root).await?;
    }
    validate_traversal(&source, args.dialect).await?;
    validate_acl_negative(&source, args.dialect).await?;
    validate_streaming_copy(&source, &destination).await?;
    validate_recovery(&source, destination, &args.destination).await?;
    println!(
        "{} passed",
        match args.dialect {
            ContractDialect::Nfs3 => "DM-NFS3-CONTRACT",
            ContractDialect::Nfs40 => "DM-NFS40-CONTRACT",
        }
    );
    Ok(())
}
