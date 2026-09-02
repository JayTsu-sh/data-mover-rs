use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt as _;
use futures::TryStreamExt as _;
use futures::stream;

use super::namespace::{CifsNamespace, CifsNamespaceProtocol};
use super::source::{CifsReadCursor, CifsReadSource, CifsSourceFacts, CifsSourceProtocol};
use crate::model::{BackendIdentity, BackendKind, EntryKind, StoragePath};
use crate::storage::{Namespace, NamespaceRequest, NamespaceResult, ReadRequest, ReadSource};
use crate::storage::{SourceQosGroup, SourceQosPolicy};

const REAL_RECOVERY_BINDING: [u8; 32] = [11; 32];
const REAL_RECOVERY_CLAIM: [u8; 32] = [12; 32];

struct MemoryCifs {
    payload: Bytes,
    reads: Arc<Mutex<Vec<(u64, usize)>>>,
    described_identity: Bytes,
    opened_identity: Bytes,
    closes: Arc<AtomicUsize>,
}

struct MemoryCursor {
    payload: Bytes,
    reads: Arc<Mutex<Vec<(u64, usize)>>>,
    closes: Arc<AtomicUsize>,
}

#[async_trait]
impl CifsReadCursor for MemoryCursor {
    fn maximum_read_chunk(&self) -> u32 {
        4
    }

    async fn read_at(&mut self, offset: u64, count: u32) -> smb_domain::Result<Bytes> {
        let count = usize::try_from(count)?;
        self.reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((offset, count));
        let start = usize::try_from(offset)?;
        Ok(self.payload.slice(start..start + count))
    }

    async fn close(self: Box<Self>) -> smb_domain::Result<()> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl CifsSourceProtocol for MemoryCifs {
    async fn describe(&self, _path: &StoragePath) -> smb_domain::Result<CifsSourceFacts> {
        Ok(CifsSourceFacts {
            kind: EntryKind::File,
            size: self.payload.len() as u64,
            identity: self.described_identity.clone(),
            maximum_read_chunk: 4,
        })
    }

    async fn open(
        &self,
        _path: &StoragePath,
    ) -> smb_domain::Result<(Box<dyn CifsReadCursor>, CifsSourceFacts)> {
        Ok((
            Box::new(MemoryCursor {
                payload: self.payload.clone(),
                reads: Arc::clone(&self.reads),
                closes: Arc::clone(&self.closes),
            }),
            CifsSourceFacts {
                kind: EntryKind::File,
                size: self.payload.len() as u64,
                identity: self.opened_identity.clone(),
                maximum_read_chunk: 4,
            },
        ))
    }
}

#[async_trait]
impl CifsNamespaceProtocol for MemoryCifs {
    async fn list(
        &self,
        path: &StoragePath,
    ) -> smb_domain::Result<Vec<(StoragePath, CifsSourceFacts)>> {
        Ok(vec![
            (
                StoragePath::new(format!("{}/child.bin", path.as_str()))
                    .map_err(|_| smb_domain::Error::InvalidArgument("invalid path".into()))?,
                CifsSourceFacts {
                    kind: EntryKind::File,
                    size: 10,
                    identity: Bytes::from_static(b"child-file"),
                    maximum_read_chunk: 4,
                },
            ),
            (
                StoragePath::new(format!("{}/nested", path.as_str()))
                    .map_err(|_| smb_domain::Error::InvalidArgument("invalid path".into()))?,
                CifsSourceFacts {
                    kind: EntryKind::Directory,
                    size: 0,
                    identity: Bytes::from_static(b"child-directory"),
                    maximum_read_chunk: u32::MAX,
                },
            ),
        ])
    }
}

#[tokio::test]
async fn source_stream_honours_negotiated_chunks_without_short_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryCifs {
        payload: Bytes::from_static(b"abcdefghij"),
        reads: Arc::new(Mutex::new(Vec::new())),
        described_identity: Bytes::from_static(b"file-identity"),
        opened_identity: Bytes::from_static(b"file-identity"),
        closes: Arc::new(AtomicUsize::new(0)),
    });
    let source = CifsReadSource::new(
        Arc::clone(&protocol),
        BackendIdentity::new(BackendKind::Cifs, "test-share")?,
    );
    let path = StoragePath::new("file.bin")?;
    let descriptor = source.describe(&path).await?;
    let mut stream = source
        .read(ReadRequest {
            path,
            range: None,
            expected_source: Some(descriptor.source_identity),
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
        })
        .await?;
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next().await.transpose()? {
        chunks.push(chunk);
    }
    assert_eq!(
        chunks,
        [
            Bytes::from_static(b"abcd"),
            Bytes::from_static(b"efgh"),
            Bytes::from_static(b"ij")
        ]
    );
    assert_eq!(
        *protocol
            .reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        [(0, 4), (4, 4), (8, 2)]
    );
    assert_eq!(protocol.closes.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn active_source_cancellation_stops_before_another_read_and_closes()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryCifs {
        payload: Bytes::from_static(b"abcdefgh"),
        reads: Arc::new(Mutex::new(Vec::new())),
        described_identity: Bytes::from_static(b"file-identity"),
        opened_identity: Bytes::from_static(b"file-identity"),
        closes: Arc::new(AtomicUsize::new(0)),
    });
    let source = CifsReadSource::new(
        Arc::clone(&protocol),
        BackendIdentity::new(BackendKind::Cifs, "test-share")?,
    );
    let cancel = tokio_util::sync::CancellationToken::new();
    let mut stream = source
        .read(ReadRequest {
            path: StoragePath::new("file.bin")?,
            range: None,
            expected_source: None,
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            cancel: cancel.clone(),
            source_qos: None,
        })
        .await?;
    assert!(stream.next().await.transpose()?.is_some());
    cancel.cancel();
    assert!(stream.next().await.transpose().is_err());
    assert_eq!(
        *protocol
            .reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        [(0, 4)]
    );
    assert_eq!(protocol.closes.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn opened_identity_change_fails_before_read_and_closes_resource()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryCifs {
        payload: Bytes::from_static(b"payload"),
        reads: Arc::new(Mutex::new(Vec::new())),
        described_identity: Bytes::from_static(b"observed"),
        opened_identity: Bytes::from_static(b"replaced"),
        closes: Arc::new(AtomicUsize::new(0)),
    });
    let source = CifsReadSource::new(
        Arc::clone(&protocol),
        BackendIdentity::new(BackendKind::Cifs, "test-share")?,
    );
    let path = StoragePath::new("file.bin")?;
    let descriptor = source.describe(&path).await?;
    let result = source
        .read(ReadRequest {
            path,
            range: None,
            expected_source: Some(descriptor.source_identity),
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
        })
        .await;
    assert!(matches!(
        result,
        Err(crate::storage::StorageRoleFailure::Entry(error))
            if error.class() == crate::model::FailureClass::Conflict
    ));
    assert!(
        protocol
            .reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
    assert_eq!(protocol.closes.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn source_qos_limits_each_real_read_and_accounts_only_source_io()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryCifs {
        payload: Bytes::from_static(b"abcdefghij"),
        reads: Arc::new(Mutex::new(Vec::new())),
        described_identity: Bytes::from_static(b"stable"),
        opened_identity: Bytes::from_static(b"stable"),
        closes: Arc::new(AtomicUsize::new(0)),
    });
    let source = CifsReadSource::new(
        Arc::clone(&protocol),
        BackendIdentity::new(BackendKind::Cifs, "test-share")?,
    );
    let path = StoragePath::new("file.bin")?;
    let descriptor = source.describe(&path).await?;
    let budget = SourceQosGroup::new(SourceQosPolicy::new(None, 3, None)?).transfer_budget();
    let mut stream = source
        .read(ReadRequest {
            path,
            range: None,
            expected_source: Some(descriptor.source_identity),
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: Some(budget.clone()),
        })
        .await?;
    while stream.next().await.transpose()?.is_some() {}

    assert_eq!(
        *protocol
            .reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        [(0, 3), (3, 3), (6, 3), (9, 1)]
    );
    let stats = budget.stats();
    assert_eq!(stats.client_streamed_shaped_bytes, 10);
    assert_eq!(stats.source_read_operations, 4);
    assert_eq!(stats.native_bytes, 0);
    Ok(())
}

#[tokio::test]
async fn namespace_list_returns_neutral_child_descriptors() -> Result<(), Box<dyn std::error::Error>>
{
    let protocol = Arc::new(MemoryCifs {
        payload: Bytes::new(),
        reads: Arc::new(Mutex::new(Vec::new())),
        described_identity: Bytes::from_static(b"directory"),
        opened_identity: Bytes::from_static(b"directory"),
        closes: Arc::new(AtomicUsize::new(0)),
    });
    let namespace = CifsNamespace::new(
        protocol,
        BackendIdentity::new(BackendKind::Cifs, "test-share")?,
    );
    let result = namespace
        .execute(NamespaceRequest::List(StoragePath::new("root")?))
        .await?;
    let NamespaceResult::Entries(entries) = result else {
        return Err("list did not return entries".into());
    };
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].path.as_str(), "root/child.bin");
    assert_eq!(entries[0].kind, EntryKind::File);
    assert_eq!(entries[1].path.as_str(), "root/nested");
    assert_eq!(entries[1].kind, EntryKind::Directory);
    Ok(())
}

#[tokio::test]
#[ignore = "requires an explicitly configured real CIFS share"]
async fn real_share_exercises_domain_roles_without_wire_api()
-> Result<(), Box<dyn std::error::Error>> {
    let server = std::env::var("CIFS_REAL_SERVER")?;
    let share_name = std::env::var("CIFS_REAL_SHARE")?;
    let username = std::env::var("CIFS_REAL_USER")?;
    let password = std::env::var("CIFS_REAL_PASS")?;
    let root = std::env::var("CIFS_REAL_ROOT").ok();
    let client = smb_domain::Client::new();
    let target = smb_domain::ShareTarget::new(&server, &share_name)?;
    let share = client
        .connect_share(&target, smb_domain::Credentials::ntlm(username, password))
        .await?;
    let storage = super::connect(
        share,
        root,
        BackendIdentity::new(BackendKind::Cifs, format!("{server}/{share_name}"))?,
    )?;
    let policy = crate::storage::PreflightPolicy::production();
    let namespace = storage.namespace(&policy)?;
    let NamespaceResult::Entries(entries) = namespace
        .execute(NamespaceRequest::List(StoragePath::root()))
        .await?
    else {
        return Err("real CIFS root did not return entries".into());
    };
    if let Some(file) = entries.iter().find(|entry| entry.kind == EntryKind::File) {
        let source = storage.read_source(&policy)?;
        let described = source.describe(&file.path).await?;
        let end = described.size.unwrap_or_default().min(4096);
        let mut stream = source
            .read(ReadRequest {
                path: file.path.clone(),
                range: Some(0..end),
                expected_source: Some(described.source_identity),
                maximum_chunk_bytes: 1024 * 1024,
                read_inflight: 4,
                cancel: tokio_util::sync::CancellationToken::new(),
                source_qos: None,
            })
            .await?;
        let mut observed = 0_u64;
        while let Some(chunk) = stream.next().await.transpose()? {
            observed += chunk.len() as u64;
        }
        assert_eq!(observed, end);
    }
    let metadata = storage.metadata(&policy)?;
    let _ = metadata
        .observe(
            &StoragePath::root(),
            crate::model::ObservationPlan::default()
                .with_acl(crate::model::ObservationMode::BestEffort),
        )
        .await?;
    if std::env::var("CIFS_EXPECT_READ_ONLY").as_deref() == Ok("true") {
        let destination = storage.staged_destination(&policy)?;
        let source_identity = crate::model::SourceIdentity::new(
            storage.identity().clone(),
            crate::model::IdentityStrength::PathScoped,
            b"real-negative-source",
        )?;
        let result = destination
            .prepare(crate::storage::PrepareRequest {
                final_destination: crate::storage::FinalDestination::new(StoragePath::new(
                    "data-mover-read-only-probe.bin",
                )?),
                source: crate::storage::SourceDescriptor::new(
                    StoragePath::new("source")?,
                    EntryKind::File,
                    Some(0),
                    source_identity,
                ),
                recovery_binding: [1; 32],
            })
            .await;
        assert!(matches!(
            result,
            Err(crate::storage::StorageRoleFailure::Entry(error))
                if error.class() == crate::model::FailureClass::PermissionDenied
        ));
    }
    let _ = client.close().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires an explicitly configured writable CIFS share on two LIFs"]
async fn real_share_recovers_durable_prefix_across_lifs() -> Result<(), Box<dyn std::error::Error>>
{
    let config = RealCifsConfig::from_env()?;
    let fixture = RecoveryFixture::new(&config.identity)?;
    let first = config.connect(&config.first_server).await?;
    let second = config.connect(&config.second_server).await?;
    let policy = crate::storage::PreflightPolicy::production();
    let recovery = write_durable_prefix(&first.storage, &policy, &fixture).await;
    first.close().await;
    let result = match recovery {
        Ok(identity) => run_recovered_half(&second.storage, &policy, &fixture, identity).await,
        Err(error) => Err(error),
    };
    let cleanup = cleanup_real_fixture(&second.share, config.root.as_deref(), &fixture).await;
    second.close().await;
    result?;
    cleanup
}

struct RealCifsConfig {
    first_server: String,
    second_server: String,
    share_name: String,
    username: String,
    password: String,
    root: Option<String>,
    identity: BackendIdentity,
}

impl RealCifsConfig {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let share_name = std::env::var("CIFS_REAL_SHARE")?;
        Ok(Self {
            first_server: std::env::var("CIFS_REAL_SERVER")?,
            second_server: std::env::var("CIFS_REAL_SECOND_SERVER")?,
            username: std::env::var("CIFS_REAL_USER")?,
            password: std::env::var("CIFS_REAL_PASS")?,
            root: std::env::var("CIFS_REAL_ROOT").ok(),
            identity: BackendIdentity::new(BackendKind::Cifs, format!("fas2750/{share_name}"))?,
            share_name,
        })
    }

    async fn connect(&self, server: &str) -> Result<RealConnection, Box<dyn std::error::Error>> {
        let client = smb_domain::Client::new();
        let share = client
            .connect_share(
                &smb_domain::ShareTarget::new(server, &self.share_name)?,
                smb_domain::Credentials::ntlm(self.username.clone(), self.password.clone()),
            )
            .await?;
        let storage = super::connect(share.clone(), self.root.clone(), self.identity.clone())?;
        Ok(RealConnection {
            client,
            share,
            storage,
        })
    }
}

struct RealConnection {
    client: smb_domain::Client,
    share: smb_domain::Share,
    storage: crate::storage::Storage,
}

impl RealConnection {
    async fn close(self) {
        drop(self.storage);
        let _ = self.client.close().await;
    }
}

struct RecoveryFixture {
    final_path: StoragePath,
    payload: Bytes,
    split: usize,
    source: crate::storage::SourceDescriptor,
}

impl RecoveryFixture {
    fn new(identity: &BackendIdentity) -> Result<Self, Box<dyn std::error::Error>> {
        let payload = Bytes::from_static(b"durable-prefix-across-fas2750-lifs");
        Ok(Self {
            final_path: StoragePath::new(format!(
                "data-mover-recovery-{}.bin",
                uuid::Uuid::new_v4().simple()
            ))?,
            split: 16,
            source: real_source_descriptor(identity, payload.len() as u64)?,
            payload,
        })
    }
}

fn real_source_descriptor(
    identity: &BackendIdentity,
    size: u64,
) -> Result<crate::storage::SourceDescriptor, Box<dyn std::error::Error>> {
    Ok(crate::storage::SourceDescriptor::new(
        StoragePath::new("synthetic-source")?,
        EntryKind::File,
        Some(size),
        crate::model::SourceIdentity::new(
            identity.clone(),
            crate::model::IdentityStrength::PathScoped,
            b"fas2750-recovery-source-v1",
        )?,
    ))
}

async fn write_durable_prefix(
    storage: &crate::storage::Storage,
    policy: &crate::storage::PreflightPolicy,
    fixture: &RecoveryFixture,
) -> Result<crate::storage::RecoveryIdentity, Box<dyn std::error::Error>> {
    let destination = storage.staged_destination(policy)?;
    let stage = destination
        .prepare(crate::storage::PrepareRequest {
            final_destination: crate::storage::FinalDestination::new(fixture.final_path.clone()),
            source: fixture.source.clone(),
            recovery_binding: REAL_RECOVERY_BINDING,
        })
        .await?;
    destination
        .write(
            &stage,
            Box::pin(stream::iter(vec![Ok(fixture
                .payload
                .slice(..fixture.split))])),
        )
        .await?;
    assert_eq!(
        destination.observe_checkpoint(&stage).await?.durable_prefix,
        fixture.split as u64
    );
    Ok(destination.recovery_identity(&stage).await?)
}

async fn run_recovered_half(
    storage: &crate::storage::Storage,
    policy: &crate::storage::PreflightPolicy,
    fixture: &RecoveryFixture,
    identity: crate::storage::RecoveryIdentity,
) -> Result<(), Box<dyn std::error::Error>> {
    let destination = storage.staged_destination(policy)?;
    let stage = destination
        .recover(crate::storage::RecoverRequest {
            identity,
            final_destination: crate::storage::FinalDestination::new(fixture.final_path.clone()),
            source: fixture.source.clone(),
            recovery_binding: REAL_RECOVERY_BINDING,
            claim_token: REAL_RECOVERY_CLAIM,
        })
        .await?;
    assert_eq!(stage.write_offset, fixture.split as u64);
    destination
        .write(
            &stage,
            Box::pin(stream::iter(vec![Ok(fixture
                .payload
                .slice(fixture.split..))])),
        )
        .await?;
    verify_and_publish(destination.as_ref(), &stage, fixture).await?;
    assert_real_payload(storage, policy, &fixture.final_path, &fixture.payload).await?;
    round_trip_acl(storage, policy, &fixture.final_path).await?;
    assert_real_failure_isolation(storage, policy, fixture).await
}

async fn assert_real_payload(
    storage: &crate::storage::Storage,
    policy: &crate::storage::PreflightPolicy,
    path: &StoragePath,
    expected: &Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = storage.read_source(policy)?;
    let descriptor = source.describe(path).await?;
    let mut stream = source
        .read(ReadRequest {
            path: path.clone(),
            range: None,
            expected_source: Some(descriptor.source_identity),
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            cancel: tokio_util::sync::CancellationToken::new(),
            source_qos: None,
        })
        .await?;
    let mut actual = Vec::new();
    while let Some(chunk) = stream.next().await.transpose()? {
        actual.extend_from_slice(&chunk);
    }
    assert_eq!(actual, expected.as_ref());
    Ok(())
}

async fn verify_and_publish(
    destination: &dyn crate::storage::StagedDestination,
    stage: &crate::storage::PreparedStage,
    fixture: &RecoveryFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let hash = *blake3::hash(&fixture.payload).as_bytes();
    destination
        .verify(
            stage,
            crate::storage::VerifyRequest {
                expected_size: fixture.payload.len() as u64,
                expected_blake3: hash,
                cancel: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await?;
    destination
        .publish(
            stage,
            crate::storage::PublishRequest {
                policy: crate::storage::ExistingDestinationPolicy::FailIfExists,
                expected_size: fixture.payload.len() as u64,
                expected_blake3: hash,
                cancel: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    Ok(())
}

async fn assert_real_failure_isolation(
    storage: &crate::storage::Storage,
    policy: &crate::storage::PreflightPolicy,
    fixture: &RecoveryFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = storage.read_source(policy)?;
    let missing = source
        .describe(&StoragePath::new("data-mover-missing-entry")?)
        .await;
    assert!(matches!(
        missing,
        Err(crate::storage::StorageRoleFailure::Entry(error))
            if error.class() == crate::model::FailureClass::NotFound
    ));
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    let cancelled = source
        .read(ReadRequest {
            path: fixture.final_path.clone(),
            range: None,
            expected_source: None,
            maximum_chunk_bytes: 1024 * 1024,
            read_inflight: 4,
            cancel,
            source_qos: None,
        })
        .await;
    assert!(matches!(
        cancelled,
        Err(crate::storage::StorageRoleFailure::Entry(error))
            if error.class() == crate::model::FailureClass::Cancelled
    ));
    assert_real_payload(storage, policy, &fixture.final_path, &fixture.payload).await
}

async fn round_trip_acl(
    storage: &crate::storage::Storage,
    policy: &crate::storage::PreflightPolicy,
    path: &StoragePath,
) -> Result<(), Box<dyn std::error::Error>> {
    let metadata = storage.metadata(policy)?;
    let observed = match metadata
        .observe(
            path,
            crate::model::ObservationPlan::default()
                .with_acl(crate::model::ObservationMode::Required),
        )
        .await
    {
        Ok(observed) => observed,
        Err(crate::storage::StorageRoleFailure::Entry(error))
            if matches!(
                error.class(),
                crate::model::FailureClass::PermissionDenied
                    | crate::model::FailureClass::Unsupported
            ) =>
        {
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    if let crate::model::MetadataObservation::Value { value, .. } = observed.acl() {
        match metadata
            .apply(
                path,
                crate::storage::MetadataMutation::Acl(value.clone()),
                tokio_util::sync::CancellationToken::new(),
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(crate::storage::StorageRoleFailure::Entry(error))
                if matches!(
                    error.class(),
                    crate::model::FailureClass::PermissionDenied
                        | crate::model::FailureClass::Unsupported
                ) =>
            {
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    } else {
        return Err("FAS2750 ACL observation did not return a value".into());
    }
}

async fn cleanup_real_fixture(
    share: &smb_domain::Share,
    root: Option<&str>,
    fixture: &RecoveryFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    delete_real_path_if_present(share, root, fixture.final_path.as_str()).await?;
    let directory = match share
        .open_directory(
            &real_share_path(root, ".data-mover-staging")?,
            smb_domain::DirectoryOpenOptions::open_existing(),
        )
        .await
    {
        Ok(directory) => directory,
        Err(error) if real_not_found(&error) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let entries = directory.entries("*").try_collect::<Vec<_>>().await;
    close_real_directory(directory).await?;
    let digest = blake3::hash(fixture.final_path.as_str().as_bytes()).to_hex();
    let prefix = &digest[..16];
    for entry in entries? {
        if entry.name().starts_with(prefix) {
            let path = format!(".data-mover-staging/{}", entry.name());
            delete_real_path_if_present(share, root, &path).await?;
        }
    }
    Ok(())
}

async fn delete_real_path_if_present(
    share: &smb_domain::Share,
    root: Option<&str>,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = match share
        .open_file(
            &real_share_path(root, path)?,
            smb_domain::FileOpenOptions::open_existing(),
        )
        .await
    {
        Ok(file) => file,
        Err(error) if real_not_found(&error) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    file.delete().await?;
    close_real_file(file).await
}

fn real_share_path(
    root: Option<&str>,
    path: &str,
) -> Result<smb_domain::SharePath, Box<dyn std::error::Error>> {
    let relative = path.replace('/', "\\");
    let value = root.filter(|value| !value.is_empty()).map_or_else(
        || relative.clone(),
        |root| format!("{}\\{relative}", root.replace('/', "\\")),
    );
    Ok(smb_domain::SharePath::new(value)?)
}

async fn close_real_file(file: smb_domain::File) -> Result<(), Box<dyn std::error::Error>> {
    require_real_close(file.close().await?)
}

async fn close_real_directory(
    directory: smb_domain::Directory,
) -> Result<(), Box<dyn std::error::Error>> {
    require_real_close(directory.close().await?)
}

fn require_real_close(outcome: smb_domain::CloseOutcome) -> Result<(), Box<dyn std::error::Error>> {
    match outcome {
        smb_domain::CloseOutcome::Confirmed | smb_domain::CloseOutcome::AlreadyClosed => Ok(()),
        smb_domain::CloseOutcome::OutcomeUnknown => Err("cleanup close outcome unknown".into()),
    }
}

fn real_not_found(error: &smb_domain::Error) -> bool {
    match error {
        smb_domain::Error::NotFound(_) => true,
        smb_domain::Error::ReceivedErrorMessage(status, _)
        | smb_domain::Error::UnexpectedMessageStatus(status) => matches!(
            smb_domain::protocol::Status::try_from(*status),
            Ok(smb_domain::protocol::Status::ObjectNameNotFound
                | smb_domain::protocol::Status::ObjectPathNotFound)
        ),
        _ => false,
    }
}
