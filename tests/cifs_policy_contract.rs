//! Optional real SMB policy tests, confined to unique file names.
use bytes::Bytes;
use data_mover::model::{BackendIdentity, BackendKind, StoragePath};
use data_mover::storage::{
    BackendConfig, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy, LocalBackendConfig,
    Storage, connect_backend,
};
use data_mover::transfer::{
    InflightLimits, ReadBackVerification, TransferIdentity, TransferOutcome, TransferPolicy,
    TransferRequest, transfer,
};
use futures::TryStreamExt as _;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an explicitly configured writable CIFS share"]
async fn real_share_checkpointed_and_atomic_replace_roundtrip() -> Result {
    let server = std::env::var("CIFS_REAL_SERVER")?;
    let share_name = std::env::var("CIFS_REAL_SHARE")?;
    let client = smb_client();
    let share = client
        .connect_share(
            &smb_domain::ShareTarget::new(&server, &share_name)?,
            smb_domain::Credentials::ntlm(raw_username()?, std::env::var("CIFS_REAL_PASS")?),
        )
        .await?;
    let remote = connect_remote(&server, &share_name).await?;
    let second_server = std::env::var("CIFS_REAL_SECOND_SERVER").unwrap_or(server);
    let destination = connect_remote(&second_server, &share_name).await?;
    let root = tempfile::tempdir()?;
    let local = connect_local(root.path()).await?;
    let id = uuid::Uuid::new_v4().simple().to_string();
    let sizes = std::env::var("CIFS_POLICY_TEST_BYTES").map_or_else(
        |_| Ok(vec![4096, 2 * 1024 * 1024 + 1, 64 * 1024 * 1024 + 1]),
        |v| {
            v.split(',')
                .map(str::parse::<usize>)
                .collect::<std::result::Result<Vec<_>, _>>()
        },
    )?;
    let result = async {
        for policy in [TransferPolicy::Checkpointed, TransferPolicy::AtomicReplace] {
            for &size in &sizes {
                let name = format!("dm-cifs-policy-{id}-{policy:?}-{size}");
                let mut payload = vec![0; size];
                blake3::Hasher::new()
                    .update(name.as_bytes())
                    .finalize_xof()
                    .fill(&mut payload);
                let payload = Bytes::from(payload);
                std::fs::write(root.path().join("source"), &payload)?;
                filetime::set_file_mtime(
                    root.path().join("source"),
                    filetime::FileTime::from_unix_time(1_700_000_001, 123_456_700),
                )?;
                let copied =
                    roundtrip(&local, &remote, &destination, &name, &payload, policy).await;
                let cleanup = cleanup_case(&share, &name).await;
                copied?;
                cleanup?;
            }
        }
        Ok(())
    }
    .await;
    let _ = client.close().await;
    result
}

async fn connect_remote(server: &str, share: &str) -> Result<Storage> {
    Ok(connect_backend(BackendConfig::Cifs(CifsBackendConfig {
        signing_policy: CifsSigningPolicy::default(),
        guest_policy: guest_policy(),
        server: server.to_owned(),
        share: share.to_owned(),
        root: None,
        username: std::env::var("CIFS_REAL_USER")?,
        password: std::env::var("CIFS_REAL_PASS")?,
        identity: BackendIdentity::new(BackendKind::Cifs, format!("{server}/{share}"))?,
    }))
    .await?)
}

async fn connect_local(root: &std::path::Path) -> Result<Storage> {
    Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: root.to_path_buf(),
        identity: BackendIdentity::new(BackendKind::Local, root.to_string_lossy())?,
        read_concurrency: std::num::NonZeroUsize::new(8).ok_or("invalid depth")?,
        write_concurrency: std::num::NonZeroUsize::new(8).ok_or("invalid depth")?,
    }))
    .await?)
}

async fn copy(
    source: &Storage,
    from: &str,
    destination: &Storage,
    to: &str,
    policy: TransferPolicy,
    read_back: ReadBackVerification,
) -> Result<TransferOutcome> {
    let started = std::time::Instant::now();
    let copied = transfer(
        TransferRequest::new(
            TransferIdentity::new(format!("{from}-{to}"))?,
            source.clone(),
            StoragePath::new(from)?,
            destination.clone(),
            StoragePath::new(to)?,
            InflightLimits::new(8, 16 * 1024 * 1024, 8)?,
            tokio_util::sync::CancellationToken::new(),
        )
        .with_transfer_policy(policy)
        .with_read_back_verification(read_back),
    )
    .await?;
    eprintln!(
        "CIFS case {policy:?} destination={to} elapsed_ms={} recovery={:?}",
        started.elapsed().as_millis(),
        copied.recovery
    );
    Ok(copied)
}

async fn roundtrip(
    local: &Storage,
    remote: &Storage,
    destination: &Storage,
    name: &str,
    payload: &Bytes,
    policy: TransferPolicy,
) -> Result {
    let expected = Some(*blake3::hash(payload).as_bytes());
    let target = format!("{name}-copied");
    let copied = copy(
        local,
        "source",
        remote,
        name,
        policy,
        ReadBackVerification::Enabled,
    )
    .await?;
    assert_eq!(copied.blake3, expected);
    assert_mtime(remote, name).await?;
    for read_back in [
        ReadBackVerification::Disabled,
        ReadBackVerification::Enabled,
    ] {
        let network = copy(remote, name, destination, &target, policy, read_back).await?;
        if policy == TransferPolicy::Checkpointed && payload.len() > 64 * 1024 * 1024 {
            assert_eq!(
                network.recovery,
                data_mover::transfer::EffectiveRecovery::Checkpointed
            );
        }
        assert_eq!(
            network.blake3,
            if read_back == ReadBackVerification::Enabled {
                expected
            } else {
                None
            }
        );
        assert_mtime(destination, &target).await?;
    }
    let back = copy(
        destination,
        &target,
        local,
        "returned",
        TransferPolicy::AtomicReplace,
        ReadBackVerification::Enabled,
    )
    .await?;
    assert_eq!(back.blake3, expected);
    assert_mtime(local, "returned").await?;
    Ok(())
}

async fn cleanup_case(share: &smb_domain::Share, name: &str) -> Result {
    let names = [name.to_owned(), format!("{name}-copied")];
    let prefixes: Vec<_> = names
        .iter()
        .map(|path| {
            format!(
                ".data-mover-{}-",
                &blake3::hash(path.as_bytes()).to_hex()[..16]
            )
        })
        .collect();
    let directory = share
        .open_directory(
            &smb_domain::SharePath::new(".")?,
            smb_domain::DirectoryOpenOptions::open_existing(),
        )
        .await?;
    let entries = directory.entries("*").try_collect::<Vec<_>>().await;
    let closed = directory.close().await;
    let entries = entries?;
    confirmed(closed?)?;
    for entry in entries {
        if names.iter().any(|name| name == entry.name())
            || prefixes
                .iter()
                .any(|prefix| entry.name().starts_with(prefix))
        {
            delete_file(share, entry.name()).await?;
        }
    }
    Ok(())
}

async fn delete_file(share: &smb_domain::Share, path: &str) -> Result {
    let file = share
        .open_file(
            &smb_domain::SharePath::new(path)?,
            smb_domain::FileOpenOptions::open_existing(),
        )
        .await?;
    let deleted = file.delete().await;
    let closed = file.close().await;
    deleted?;
    confirmed(closed?)
}

fn confirmed(outcome: smb_domain::CloseOutcome) -> Result {
    match outcome {
        smb_domain::CloseOutcome::Confirmed | smb_domain::CloseOutcome::AlreadyClosed => Ok(()),
        smb_domain::CloseOutcome::OutcomeUnknown => Err("SMB close failed".into()),
    }
}

async fn assert_mtime(storage: &Storage, path: &str) -> Result {
    use data_mover::model::{MetadataObservation, ObservationMode, ObservationPlan};
    use data_mover::storage::PreflightPolicy;
    let metadata = storage
        .metadata(&PreflightPolicy::production())?
        .observe(
            &StoragePath::new(path)?,
            ObservationPlan::default().with_timestamps(ObservationMode::Required),
        )
        .await?;
    let MetadataObservation::Value { value, .. } = metadata.timestamps() else {
        return Err("missing timestamps".into());
    };
    assert_eq!(
        value
            .modified
            .map(data_mover::model::StorageTimestamp::unix_nanos),
        Some(1_700_000_001_123_456_700),
        "mtime changed for {path}"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires an explicitly configured writable CIFS share"]
async fn real_directory_mtime_apply_preserves_creation_time() -> Result {
    let server = std::env::var("CIFS_REAL_SERVER")?;
    let name = std::env::var("CIFS_REAL_SHARE")?;
    let client = smb_client();
    let share = client
        .connect_share(
            &smb_domain::ShareTarget::new(&server, &name)?,
            smb_domain::Credentials::ntlm(raw_username()?, std::env::var("CIFS_REAL_PASS")?),
        )
        .await?;
    let path = format!("dm-cifs-metadata-dir-{}", uuid::Uuid::new_v4().simple());
    let directory = share
        .open_directory(
            &smb_domain::SharePath::new(&path)?,
            smb_domain::DirectoryOpenOptions::create_new(),
        )
        .await?;
    let result = verify_directory_metadata(&server, &name, &path, &directory).await;
    let deleted = directory.delete().await;
    let closed = directory.close().await;
    let _ = client.close().await;
    result?;
    deleted?;
    confirmed(closed?)
}

async fn verify_directory_metadata(
    server: &str,
    share: &str,
    path: &str,
    directory: &smb_domain::Directory,
) -> Result {
    use data_mover::model::{StorageTimestamp, TimePrecision, TimestampMetadata};
    use data_mover::storage::{MetadataMutation, PreflightPolicy};
    let before = directory.metadata().await?;
    let storage = connect_remote(server, share).await?;
    storage
        .metadata(&PreflightPolicy::production())?
        .apply(
            &StoragePath::new(path)?,
            MetadataMutation::Timestamps(TimestampMetadata {
                accessed: None,
                modified: Some(StorageTimestamp::new(
                    1_700_000_001_123_456_700,
                    TimePrecision::HundredNanoseconds,
                )?),
                created: None,
            }),
            tokio_util::sync::CancellationToken::new(),
        )
        .await?;
    assert_mtime(&storage, path).await?;
    let after = directory.metadata().await?;
    assert_eq!(before.created(), after.created());
    assert_eq!(before.accessed(), after.accessed());
    Ok(())
}

/// `CIFS_REAL_GUEST_POLICY=allow-unsigned` opts the real-share tests into guest sessions.
fn guest_policy() -> CifsGuestPolicy {
    if std::env::var("CIFS_REAL_GUEST_POLICY").is_ok_and(|value| value == "allow-unsigned") {
        CifsGuestPolicy::AllowUnsigned
    } else {
        CifsGuestPolicy::Deny
    }
}

fn smb_client() -> smb_domain::Client {
    smb_domain::Client::with_policies(
        smb_domain::SigningPolicy::WhenRequired,
        match guest_policy() {
            CifsGuestPolicy::AllowUnsigned => smb_domain::GuestPolicy::AllowUnsigned,
            CifsGuestPolicy::Deny => smb_domain::GuestPolicy::Deny,
        },
    )
}

/// Raw smb-domain connections in these tests mirror the factory: an empty username under
/// `allow-unsigned` becomes the placeholder identity that reaches the server's guest mapping.
fn raw_username() -> std::result::Result<String, std::env::VarError> {
    std::env::var("CIFS_REAL_USER").map(|user| raw_username_of(&user))
}

fn raw_username_of(user: &str) -> String {
    if user.is_empty() && guest_policy() == CifsGuestPolicy::AllowUnsigned {
        "anonymous".to_owned()
    } else {
        user.to_owned()
    }
}
