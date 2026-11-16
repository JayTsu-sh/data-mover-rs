//! 当前路径确认不能把缓存中的旧对象当作同名新对象。
#![allow(clippy::unwrap_used)]
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::{
    NamespaceReadCause, NamespaceReadFailure, NamespaceReadOutcome, NamespaceReadPolicy,
    read_namespace_entry,
};
use crate::model::{
    BackendIdentity, BackendKind, EntryIdentityKey, EntryKind, EntryOperationFailure, FailureClass,
    IdentityStrength, MetadataObservations, ObservationPlan, Operation, SourceIdentity,
    StoragePath, Transience,
};
use crate::storage::{
    BackendCapabilities, CapabilityAvailability, ListRequest, ListingStream, Metadata,
    MetadataMutation, Namespace, SourceDescriptor, Storage, StorageBuilder, StorageRoleFailure,
    UnsupportedReason,
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Names {
    backend: BackendIdentity,
    named_generation: AtomicU64,
    exists: AtomicBool,
    retry_once: AtomicBool,
    fresh_calls: AtomicU32,
    metadata_calls: AtomicU32,
    requests: Mutex<Vec<bool>>,
    stall: AtomicBool,
}

fn failure(path: &StoragePath, class: FailureClass) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            Operation::Observe,
            class,
            Transience::Transient,
            "injected current-name read",
        )
        .unwrap(),
    )
}

#[async_trait]
impl Namespace for Names {
    async fn stat(
        &self,
        path: &StoragePath,
        fresh: bool,
    ) -> std::result::Result<SourceDescriptor, StorageRoleFailure> {
        self.requests.lock().unwrap().push(fresh);
        let generation = if fresh {
            let attempt = self.fresh_calls.fetch_add(1, Ordering::AcqRel);
            if self.stall.load(Ordering::Acquire) {
                futures::future::pending::<()>().await;
            }
            if self.retry_once.load(Ordering::Acquire) && attempt == 0 {
                return Err(failure(path, FailureClass::Connectivity));
            }
            if !self.exists.load(Ordering::Acquire) {
                return Err(failure(path, FailureClass::NotFound));
            }
            let generation = self.named_generation.load(Ordering::Acquire);
            if path.as_str().is_empty() && generation != 0 {
                return Err(failure(path, FailureClass::Conflict));
            }
            generation
        } else {
            0
        };
        Ok(SourceDescriptor::new(
            path.clone(),
            EntryKind::Directory,
            None,
            SourceIdentity::new(
                self.backend.clone(),
                IdentityStrength::StableWithinBackend,
                generation.to_le_bytes(),
            )
            .map_err(|_| failure(path, FailureClass::Protocol))?,
        ))
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        ListingStream::from_pages(request.path, [])
    }
}

#[async_trait]
impl Metadata for Names {
    async fn observe(
        &self,
        _: &StoragePath,
        _: ObservationPlan,
    ) -> std::result::Result<MetadataObservations, StorageRoleFailure> {
        self.metadata_calls.fetch_add(1, Ordering::AcqRel);
        Ok(MetadataObservations::default())
    }
    async fn apply(
        &self,
        path: &StoragePath,
        _: MetadataMutation,
        _: CancellationToken,
    ) -> std::result::Result<(), StorageRoleFailure> {
        Err(failure(path, FailureClass::Unsupported))
    }
}

fn fixture() -> Result<(Storage, Arc<Names>)> {
    let backend = BackendIdentity::new(BackendKind::Local, "current-name-helper")?;
    let names = Arc::new(Names {
        backend: backend.clone(),
        named_generation: AtomicU64::new(0),
        exists: AtomicBool::new(true),
        retry_once: AtomicBool::new(false),
        fresh_calls: AtomicU32::new(0),
        metadata_calls: AtomicU32::new(0),
        requests: Mutex::new(Vec::new()),
        stall: AtomicBool::new(false),
    });
    let no = CapabilityAvailability::Unsupported(UnsupportedReason::new("unused")?);
    let storage = StorageBuilder::new(
        backend,
        BackendCapabilities::new(
            no.clone(),
            no,
            CapabilityAvailability::Supported,
            CapabilityAvailability::Supported,
        ),
    )
    .namespace(names.clone())
    .metadata(names.clone())
    .build()?;
    Ok((storage, names))
}

fn policy() -> NamespaceReadPolicy {
    NamespaceReadPolicy {
        operation_timeout: Duration::from_secs(1),
        recovery_window: Duration::from_secs(3),
        retry_interval: Duration::from_millis(100),
    }
}

fn identity(outcome: NamespaceReadOutcome) -> Option<EntryIdentityKey> {
    match outcome {
        NamespaceReadOutcome::Descriptor { descriptor, .. } => {
            descriptor.map(|entry| entry.source_identity.identity_key())
        }
        NamespaceReadOutcome::Observation { entry, .. } => {
            entry.map(|entry| entry.source_identity().identity_key())
        }
    }
}

async fn rebound(plan: Option<ObservationPlan>) -> Result {
    let (storage, names) = fixture()?;
    let path = StoragePath::new("parent/dir")?;
    let old = storage
        .namespace()?
        .stat(&path, false)
        .await?
        .source_identity
        .identity_key();
    names.named_generation.store(1, Ordering::Release);
    let outcome =
        read_namespace_entry(&storage, &path, plan, policy(), &CancellationToken::new()).await?;
    assert_ne!(
        identity(outcome),
        Some(old),
        "helper must resolve the current name, not reuse the live renamed-away object"
    );
    assert_eq!(*names.requests.lock().unwrap(), [false, true]);
    assert_eq!(
        names.metadata_calls.load(Ordering::Acquire),
        u32::from(plan.is_some())
    );
    Ok(())
}

#[tokio::test]
async fn current_name_helper_descriptor_branch_rechecks_a_warm_binding() -> Result {
    rebound(None).await
}

#[tokio::test]
async fn current_name_helper_observation_branch_rechecks_a_warm_binding() -> Result {
    rebound(Some(ObservationPlan::default())).await
}

#[tokio::test]
async fn current_name_helper_absence_is_confirmed_fresh_before_metadata() -> Result {
    for plan in [None, Some(ObservationPlan::default())] {
        let (storage, names) = fixture()?;
        let path = StoragePath::new("parent/dir")?;
        storage.namespace()?.stat(&path, false).await?;
        names.exists.store(false, Ordering::Release);
        let outcome =
            read_namespace_entry(&storage, &path, plan, policy(), &CancellationToken::new())
                .await?;
        assert!(identity(outcome).is_none());
        assert_eq!(*names.requests.lock().unwrap(), [false, true]);
        assert_eq!(names.metadata_calls.load(Ordering::Acquire), 0);
    }
    Ok(())
}

#[tokio::test]
async fn current_name_helper_root_conflict_is_not_hidden_or_rebound_by_either_branch() -> Result {
    for plan in [None, Some(ObservationPlan::default())] {
        let (storage, names) = fixture()?;
        names.named_generation.store(1, Ordering::Release);
        let result = read_namespace_entry(
            &storage,
            &StoragePath::root(),
            plan,
            policy(),
            &CancellationToken::new(),
        )
        .await;
        assert!(
            matches!(result, Err(NamespaceReadFailure::Operation { attempts: 1, last, .. })
            if matches!(*last, NamespaceReadCause::Role(StorageRoleFailure::Entry(ref error)) if error.class() == FailureClass::Conflict))
        );
        assert_eq!(names.metadata_calls.load(Ordering::Acquire), 0);
        assert_eq!(*names.requests.lock().unwrap(), [true]);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn current_name_helper_retries_keep_the_same_fresh_contract() -> Result {
    for plan in [None, Some(ObservationPlan::default())] {
        let (storage, names) = fixture()?;
        names.retry_once.store(true, Ordering::Release);
        let outcome = read_namespace_entry(
            &storage,
            &StoragePath::new("dir")?,
            plan,
            policy(),
            &CancellationToken::new(),
        )
        .await?;
        assert!(identity(outcome).is_some());
        assert_eq!(*names.requests.lock().unwrap(), [true, true]);
        assert_eq!(
            names.metadata_calls.load(Ordering::Acquire),
            u32::from(plan.is_some())
        );
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn current_name_helper_cancel_does_not_fall_back_to_the_old_descriptor() -> Result {
    let (storage, names) = fixture()?;
    names.stall.store(true, Ordering::Release);
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    let stop = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        signal.cancel();
    });
    let result =
        read_namespace_entry(&storage, &StoragePath::new("dir")?, None, policy(), &cancel).await;
    stop.await?;
    assert!(matches!(
        result,
        Err(NamespaceReadFailure::Cancelled { .. })
    ));
    assert_eq!(*names.requests.lock().unwrap(), [true]);
    assert_eq!(names.metadata_calls.load(Ordering::Acquire), 0);
    Ok(())
}
