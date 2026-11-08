//! 属性错误不能冒充路径缺失；stat 和属性读取共享同一次恢复窗口。
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

use super::{
    NamespaceReadCause, NamespaceReadFailure, NamespaceReadOutcome, NamespaceReadPolicy,
    read_namespace_entry,
};
use crate::model::{
    BackendIdentity, BackendKind, EntryKind, EntryOperationFailure, FailureClass, IdentityStrength,
    MetadataFamily, MetadataObservation, MetadataObservations, MetadataProvenance, ObjectTag,
    ObservationMode, ObservationPlan, Operation, SourceIdentity, StoragePath, Transience,
};
use crate::storage::{
    BackendCapabilities, CapabilityAvailability, EntryObservationFailure, ListRequest,
    ListingStream, Metadata, MetadataMutation, Namespace, SourceDescriptor, Storage,
    StorageBuilder, StorageRoleFailure, UnsupportedReason,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

enum Reply {
    Error(FailureClass),
    Recover,
    Stall,
    Facts(MetadataObservations),
}

struct Source {
    descriptor: SourceDescriptor,
    stat_calls: AtomicU32,
    metadata_calls: AtomicU32,
    first_stat_fails: bool,
    reply: Reply,
}

fn failure(path: &StoragePath, class: FailureClass) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            Operation::Metadata,
            class,
            Transience::Transient,
            "injected source read failure",
        )
        .unwrap_or_else(|error| panic!("{error}")),
    )
}

#[async_trait]
impl Namespace for Source {
    async fn stat(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        let call = self.stat_calls.fetch_add(1, Ordering::SeqCst);
        if self.first_stat_fails && call == 0 {
            return Err(failure(path, FailureClass::Connectivity));
        }
        Ok(self.descriptor.clone())
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        ListingStream::from_pages(request.path, [])
    }
}

#[async_trait]
impl Metadata for Source {
    async fn observe(
        &self,
        path: &StoragePath,
        _: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let call = self.metadata_calls.fetch_add(1, Ordering::SeqCst);
        match &self.reply {
            Reply::Error(class) => Err(failure(path, *class)),
            Reply::Recover if call == 0 => Err(failure(path, FailureClass::Connectivity)),
            Reply::Stall => {
                sleep(Duration::from_secs(100)).await;
                Err(failure(path, FailureClass::Timeout))
            }
            Reply::Facts(facts) => Ok(facts.clone()),
            Reply::Recover => Ok(MetadataObservations::default()),
        }
    }
    async fn apply(
        &self,
        path: &StoragePath,
        _: MetadataMutation,
        _: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        Err(failure(path, FailureClass::Unsupported))
    }
}

fn fixture(first_stat_fails: bool, reply: Reply) -> TestResult<(Storage, Arc<Source>)> {
    let backend = BackendIdentity::new(BackendKind::Local, "source-observation-read-test")?;
    let unavailable = CapabilityAvailability::Unsupported(UnsupportedReason::new("unused")?);
    let source = Arc::new(Source {
        descriptor: SourceDescriptor::new(
            StoragePath::new("file")?,
            EntryKind::File,
            Some(7),
            SourceIdentity::new(backend.clone(), IdentityStrength::PathScoped, b"file")?,
        ),
        stat_calls: AtomicU32::new(0),
        metadata_calls: AtomicU32::new(0),
        first_stat_fails,
        reply,
    });
    let storage = StorageBuilder::new(
        backend,
        BackendCapabilities::new(
            unavailable.clone(),
            unavailable,
            CapabilityAvailability::Supported,
            CapabilityAvailability::Supported,
        ),
    )
    .namespace(source.clone())
    .metadata(source.clone())
    .build()?;
    Ok((storage, source))
}

fn policy() -> NamespaceReadPolicy {
    NamespaceReadPolicy {
        operation_timeout: Duration::from_secs(1),
        recovery_window: Duration::from_secs(3),
        retry_interval: Duration::from_millis(100),
    }
}

fn plan(mode: ObservationMode) -> ObservationPlan {
    ObservationPlan::default().with_tags(mode)
}

fn facts(tags: MetadataObservation<Vec<ObjectTag>>) -> TestResult<MetadataObservations> {
    Ok(MetadataObservations::new(
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        tags,
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
    )?)
}

#[tokio::test(start_paused = true)]
async fn metadata_not_found_permission_and_authentication_are_failures_without_retry() -> TestResult
{
    for class in [
        FailureClass::NotFound,
        FailureClass::PermissionDenied,
        FailureClass::Authentication,
    ] {
        let (storage, source) = fixture(false, Reply::Error(class))?;
        let result = read_namespace_entry(
            &storage,
            &StoragePath::new("file")?,
            Some(plan(ObservationMode::Required)),
            policy(),
            &CancellationToken::new(),
        )
        .await;
        assert!(
            matches!(result, Err(NamespaceReadFailure::Operation { attempts: 1, last, .. })
            if matches!(*last, NamespaceReadCause::Observation(ref error)
                if matches!(error.as_ref(), EntryObservationFailure::Role(StorageRoleFailure::Entry(error)) if error.class() == class)))
        );
        assert_eq!(source.stat_calls.load(Ordering::SeqCst), 1);
        assert_eq!(source.metadata_calls.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn source_stat_and_metadata_share_one_window_and_cancel_stops_inflight_observation()
-> TestResult {
    let (storage, source) = fixture(true, Reply::Stall)?;
    let start = Instant::now();
    let result = read_namespace_entry(
        &storage,
        &StoragePath::new("file")?,
        Some(plan(ObservationMode::Required)),
        policy(),
        &CancellationToken::new(),
    )
    .await;
    assert!(
        matches!(result, Err(NamespaceReadFailure::Operation { first, last, .. })
        if matches!(*first, NamespaceReadCause::Role(_)) && matches!(*last, NamespaceReadCause::ObservationTimeout))
    );
    assert_eq!(start.elapsed(), Duration::from_secs(3));
    assert_eq!(source.stat_calls.load(Ordering::SeqCst), 4);
    assert_eq!(source.metadata_calls.load(Ordering::SeqCst), 3);
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(250)).await;
        signal.cancel();
    });
    let start = Instant::now();
    assert!(matches!(
        read_namespace_entry(
            &storage,
            &StoragePath::new("file")?,
            Some(plan(ObservationMode::Required)),
            policy(),
            &cancel
        )
        .await,
        Err(NamespaceReadFailure::Cancelled { .. })
    ));
    assert_eq!(start.elapsed(), Duration::from_millis(250));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn required_missing_or_failed_facts_are_never_successful_observations() -> TestResult {
    for (observation, class) in [
        (MetadataObservation::Unsupported, FailureClass::Unsupported),
        (
            MetadataObservation::NotRequested,
            FailureClass::InvalidInput,
        ),
        (
            MetadataObservation::Failed {
                class: FailureClass::PermissionDenied,
                transience: Transience::Transient,
            },
            FailureClass::PermissionDenied,
        ),
    ] {
        let (storage, source) = fixture(false, Reply::Facts(facts(observation)?))?;
        assert!(
            matches!(read_namespace_entry(&storage, &StoragePath::new("file")?, Some(plan(ObservationMode::Required)), policy(), &CancellationToken::new()).await,
            Err(NamespaceReadFailure::Operation { attempts: 1, last, .. })
                if matches!(*last, NamespaceReadCause::MissingMetadata(ref failures) if failures.len() == 1 && failures[0].family == MetadataFamily::Tags && failures[0].class == class))
        );
        assert_eq!(source.metadata_calls.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn recovered_read_returns_the_actual_facts_and_best_effort_keeps_data_readable() -> TestResult
{
    let (storage, source) = fixture(false, Reply::Recover)?;
    let result = read_namespace_entry(
        &storage,
        &StoragePath::new("file")?,
        Some(plan(ObservationMode::Omit)),
        policy(),
        &CancellationToken::new(),
    )
    .await?;
    let NamespaceReadOutcome::Observation { entry, attempts } = result else {
        return Err("expected observation".into());
    };
    assert_eq!(attempts, 2);
    assert_eq!(entry.ok_or("missing source")?.size(), Some(7));
    assert_eq!(source.metadata_calls.load(Ordering::SeqCst), 2);
    let tags = vec![ObjectTag::new("raw", "value")?];
    let (storage, _) = fixture(
        false,
        Reply::Facts(facts(MetadataObservation::Value {
            value: tags.clone(),
            provenance: MetadataProvenance::AdditionalCall,
        })?),
    )?;
    let result = read_namespace_entry(
        &storage,
        &StoragePath::new("file")?,
        Some(plan(ObservationMode::Required)),
        policy(),
        &CancellationToken::new(),
    )
    .await?;
    let NamespaceReadOutcome::Observation { entry, .. } = result else {
        return Err("expected observation".into());
    };
    assert_eq!(
        entry.ok_or("missing source")?.metadata().tags().value(),
        Some(&tags)
    );
    let (storage, source) = fixture(false, Reply::Error(FailureClass::Connectivity))?;
    let result = read_namespace_entry(
        &storage,
        &StoragePath::new("file")?,
        Some(plan(ObservationMode::BestEffort)),
        policy(),
        &CancellationToken::new(),
    )
    .await?;
    let NamespaceReadOutcome::Observation { entry, attempts } = result else {
        return Err("expected observation".into());
    };
    assert_eq!(attempts, 1);
    let entry = entry.ok_or("missing readable source")?;
    assert_eq!(entry.size(), Some(7));
    assert!(matches!(
        entry.metadata().tags(),
        MetadataObservation::Failed {
            class: FailureClass::Connectivity,
            ..
        }
    ));
    assert_eq!(source.stat_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn descriptor_only_read_does_not_acquire_or_request_metadata() -> TestResult {
    let (storage, source) = fixture(false, Reply::Stall)?;
    let start = Instant::now();
    let result = read_namespace_entry(
        &storage,
        &StoragePath::new("file")?,
        None,
        policy(),
        &CancellationToken::new(),
    )
    .await?;
    assert!(
        matches!(result, NamespaceReadOutcome::Descriptor { descriptor: Some(ref descriptor), attempts: 1 } if descriptor.size == Some(7))
    );
    assert_eq!(source.stat_calls.load(Ordering::SeqCst), 1);
    assert_eq!(source.metadata_calls.load(Ordering::SeqCst), 0);
    assert_eq!(start.elapsed(), Duration::ZERO);
    Ok(())
}
