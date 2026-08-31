use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt as _;
use futures::stream;

use super::namespace::{CifsNamespace, CifsNamespaceProtocol};
use super::source::{CifsReadCursor, CifsReadSource, CifsSourceFacts, CifsSourceProtocol};
use crate::model::{BackendIdentity, BackendKind, EntryKind, StoragePath};
use crate::storage::{Namespace, NamespaceRequest, NamespaceResult, ReadRequest, ReadSource};
use crate::storage::{SourceQosGroup, SourceQosPolicy};

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
                },
            ),
            (
                StoragePath::new(format!("{}/nested", path.as_str()))
                    .map_err(|_| smb_domain::Error::InvalidArgument("invalid path".into()))?,
                CifsSourceFacts {
                    kind: EntryKind::Directory,
                    size: 0,
                    identity: Bytes::from_static(b"child-directory"),
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
    let storage = crate::cifs::create_cifs_role_storage(
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
    let first_server = std::env::var("CIFS_REAL_SERVER")?;
    let second_server = std::env::var("CIFS_REAL_SECOND_SERVER")?;
    let share_name = std::env::var("CIFS_REAL_SHARE")?;
    let username = std::env::var("CIFS_REAL_USER")?;
    let password = std::env::var("CIFS_REAL_PASS")?;
    let root = std::env::var("CIFS_REAL_ROOT").ok();
    let cleanup_root = root.clone();
    let identity = BackendIdentity::new(BackendKind::Cifs, format!("fas2750/{share_name}"))?;
    let final_path = StoragePath::new(format!(
        "data-mover-recovery-{}.bin",
        uuid::Uuid::new_v4().simple()
    ))?;
    let payload = Bytes::from_static(b"durable-prefix-across-fas2750-lifs");
    let split = 16_usize;

    let first_client = smb_domain::Client::new();
    let first_share = first_client
        .connect_share(
            &smb_domain::ShareTarget::new(&first_server, &share_name)?,
            smb_domain::Credentials::ntlm(username.clone(), password.clone()),
        )
        .await?;
    let first_storage =
        crate::cifs::create_cifs_role_storage(first_share, root.clone(), identity.clone())?;
    let policy = crate::storage::PreflightPolicy::production();
    let destination = first_storage.staged_destination(&policy)?;
    let source = real_source_descriptor(&identity, payload.len() as u64)?;
    let stage = destination
        .prepare(crate::storage::PrepareRequest {
            final_destination: crate::storage::FinalDestination::new(final_path.clone()),
            source: source.clone(),
            recovery_binding: [11; 32],
        })
        .await?;
    destination
        .write(
            &stage,
            Box::pin(stream::iter(vec![Ok(payload.slice(..split))])),
        )
        .await?;
    assert_eq!(
        destination.observe_checkpoint(&stage).await?.durable_prefix,
        split as u64
    );
    let recovery_identity = destination.recovery_identity(&stage).await?;
    drop(first_storage);
    let _ = first_client.close().await;

    let second_client = smb_domain::Client::new();
    let second_share = second_client
        .connect_share(
            &smb_domain::ShareTarget::new(&second_server, &share_name)?,
            smb_domain::Credentials::ntlm(username, password),
        )
        .await?;
    let cleanup_share = second_share.clone();
    let second_storage = crate::cifs::create_cifs_role_storage(second_share, root, identity)?;
    complete_recovered_copy(
        &second_storage,
        &policy,
        source,
        final_path.clone(),
        recovery_identity,
        payload.clone(),
        split,
    )
    .await?;
    round_trip_acl(&second_storage, &policy, &final_path).await?;
    delete_real_file(&cleanup_share, cleanup_root.as_deref(), &final_path).await?;
    let _ = second_client.close().await;
    Ok(())
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

async fn complete_recovered_copy(
    storage: &crate::storage::Storage,
    policy: &crate::storage::PreflightPolicy,
    source: crate::storage::SourceDescriptor,
    final_path: StoragePath,
    recovery_identity: crate::storage::RecoveryIdentity,
    payload: Bytes,
    split: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let destination = storage.staged_destination(policy)?;
    let stage = destination
        .recover(crate::storage::RecoverRequest {
            identity: recovery_identity,
            final_destination: crate::storage::FinalDestination::new(final_path.clone()),
            source,
            recovery_binding: [11; 32],
            claim_token: [12; 32],
        })
        .await?;
    assert_eq!(stage.write_offset, split as u64);
    destination
        .write(
            &stage,
            Box::pin(stream::iter(vec![Ok(payload.slice(split..))])),
        )
        .await?;
    let hash = *blake3::hash(&payload).as_bytes();
    destination
        .verify(
            &stage,
            crate::storage::VerifyRequest {
                expected_size: payload.len() as u64,
                expected_blake3: hash,
                cancel: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await?;
    destination
        .publish(
            &stage,
            crate::storage::PublishRequest {
                policy: crate::storage::ExistingDestinationPolicy::FailIfExists,
                expected_size: payload.len() as u64,
                expected_blake3: hash,
                cancel: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    assert_real_payload(storage, policy, &final_path, &payload).await
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

async fn round_trip_acl(
    storage: &crate::storage::Storage,
    policy: &crate::storage::PreflightPolicy,
    path: &StoragePath,
) -> Result<(), Box<dyn std::error::Error>> {
    let metadata = storage.metadata(policy)?;
    let observed = metadata
        .observe(
            path,
            crate::model::ObservationPlan::default()
                .with_acl(crate::model::ObservationMode::Required),
        )
        .await?;
    if let crate::model::MetadataObservation::Value { value, .. } = observed.acl() {
        metadata
            .apply(
                path,
                crate::storage::MetadataMutation::Acl(value.clone()),
                tokio_util::sync::CancellationToken::new(),
            )
            .await?;
    } else {
        return Err("FAS2750 ACL observation did not return a value".into());
    }
    Ok(())
}

async fn delete_real_file(
    share: &smb_domain::Share,
    root: Option<&str>,
    path: &StoragePath,
) -> Result<(), Box<dyn std::error::Error>> {
    let relative = path.as_str().replace('/', "\\");
    let share_path = match root.filter(|value| !value.is_empty()) {
        Some(root) => format!("{}\\{relative}", root.replace('/', "\\")),
        None => relative,
    };
    let file = share
        .open_file(
            &smb_domain::SharePath::new(share_path)?,
            smb_domain::FileOpenOptions::open_existing(),
        )
        .await?;
    file.delete().await?;
    match file.close().await? {
        smb_domain::CloseOutcome::Confirmed | smb_domain::CloseOutcome::AlreadyClosed => Ok(()),
        smb_domain::CloseOutcome::OutcomeUnknown => Err("cleanup close outcome unknown".into()),
    }
}
