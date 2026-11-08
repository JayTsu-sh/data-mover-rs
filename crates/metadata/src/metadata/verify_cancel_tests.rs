//! 取消必须终止在途只读校验，包括没有元数据 mutation 的 stat。

use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

use super::{
    MetadataPlan, MetadataPolicies, MetadataVerificationError, RefusalCause,
    compile_unavailable_plan,
};
use crate::model::{
    BackendIdentity, BackendKind, EntryKind, IdentityStrength, MetadataFamily,
    MetadataObservations, ObservationPlan, PermissionFacts, SourceIdentity, SourceVersion,
    StoragePath,
};
use crate::storage::{
    BackendCapabilities, ByteStream, CapabilityAvailability, CopiedMetadataObservation,
    ListRequest, ListingStream, Metadata, MetadataMutation, Namespace, ReadRequest, ReadSource,
    SourceDescriptor, Storage, StorageBuilder, StorageRoleFailure, UnsupportedReason,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadPhase {
    Stat,
    Current,
    Historical,
    Tags,
    Permissions,
}

struct StalledTarget {
    phase: ReadPhase,
    descriptor: SourceDescriptor,
    entered: AtomicUsize,
    active: AtomicUsize,
}

struct ActiveRead<'a>(&'a AtomicUsize);
impl Drop for ActiveRead<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl StalledTarget {
    async fn stall(&self, phase: ReadPhase) {
        if self.phase == phase {
            self.entered.fetch_add(1, Ordering::SeqCst);
            self.active.fetch_add(1, Ordering::SeqCst);
            let _active = ActiveRead(&self.active);
            pending::<()>().await;
        }
    }
}

#[async_trait]
impl Namespace for StalledTarget {
    async fn stat(&self, _: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.stall(ReadPhase::Stat).await;
        Ok(self.descriptor.clone())
    }
    fn list(&self, _: ListRequest) -> ListingStream {
        panic!("verification must not list")
    }
}

#[async_trait]
impl ReadSource for StalledTarget {
    async fn describe(&self, _: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.stall(ReadPhase::Current).await;
        Ok(self.descriptor.clone())
    }
    async fn describe_version(
        &self,
        _: &StoragePath,
        _: &SourceVersion,
    ) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.stall(ReadPhase::Historical).await;
        Ok(self.descriptor.clone())
    }
    async fn read(&self, _: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        panic!("quick verification must not read data")
    }
}

#[async_trait]
impl Metadata for StalledTarget {
    async fn observe(
        &self,
        _: &StoragePath,
        _: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        self.stall(ReadPhase::Tags).await;
        Ok(MetadataObservations::default())
    }
    async fn observe_copy_bound_version(
        &self,
        path: &StoragePath,
        _: &SourceIdentity,
        _: &SourceVersion,
        plan: ObservationPlan,
    ) -> Result<CopiedMetadataObservation, StorageRoleFailure> {
        Ok(CopiedMetadataObservation {
            observations: self.observe(path, plan).await?,
            mode_without_ownership: None,
            owner_names_unmapped: false,
        })
    }
    async fn permissions(
        &self,
        _: &SourceDescriptor,
    ) -> Result<PermissionFacts, StorageRoleFailure> {
        self.stall(ReadPhase::Permissions).await;
        Ok(PermissionFacts::default())
    }
    async fn apply(
        &self,
        _: &StoragePath,
        _: MetadataMutation,
        _: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        panic!("verification must not mutate metadata")
    }
}

fn fixture(phase: ReadPhase) -> TestResult<(Storage, Arc<StalledTarget>, MetadataPlan)> {
    let backend = BackendIdentity::new(BackendKind::Local, "verification-cancel-test")?;
    let mut descriptor = SourceDescriptor::new(
        StoragePath::new("file")?,
        EntryKind::File,
        Some(7),
        SourceIdentity::new(backend.clone(), IdentityStrength::PathScoped, b"file")?,
    );
    descriptor.version = SourceVersion::Id("v1".into());
    let role = Arc::new(StalledTarget {
        phase,
        descriptor,
        entered: AtomicUsize::new(0),
        active: AtomicUsize::new(0),
    });
    let capabilities = BackendCapabilities::new(
        CapabilityAvailability::Supported,
        CapabilityAvailability::Unsupported(UnsupportedReason::new("not needed")?),
        CapabilityAvailability::Supported,
        CapabilityAvailability::Supported,
    );
    let storage = StorageBuilder::new(backend, capabilities)
        .namespace(role.clone())
        .read_source(role.clone())
        .metadata(role.clone())
        .build()?;
    let mut plan = compile_unavailable_plan(
        MetadataPolicies::default(),
        RefusalCause::DestinationCannotStore,
    );
    match phase {
        ReadPhase::Tags => plan
            .mutations
            .push((MetadataFamily::Tags, MetadataMutation::Tags(Vec::new()))),
        ReadPhase::Permissions => plan
            .mutations
            .push((MetadataFamily::OwnershipMode, MetadataMutation::Mode(0o644))),
        _ => {}
    }
    Ok((storage, role, plan))
}

#[tokio::test(start_paused = true)]
async fn cancellation_drops_each_in_flight_verification_read() -> TestResult {
    for phase in [
        ReadPhase::Stat,
        ReadPhase::Current,
        ReadPhase::Historical,
        ReadPhase::Tags,
        ReadPhase::Permissions,
    ] {
        let (storage, role, plan) = fixture(phase)?;
        let cancel = CancellationToken::new();
        let cancelling = cancel.clone();
        let watcher = tokio::spawn(async move {
            sleep(Duration::from_millis(250)).await;
            cancelling.cancel();
        });
        let version = if phase == ReadPhase::Historical {
            SourceVersion::Id("v1".into())
        } else {
            SourceVersion::Current
        };
        let expected = (phase == ReadPhase::Current).then_some("v1");
        let start = Instant::now();
        let result = timeout(
            Duration::from_secs(1),
            plan.verify_published(&storage, &role.descriptor.path, &version, expected, cancel),
        )
        .await;
        watcher.await?;
        assert!(
            matches!(result, Ok(Err(MetadataVerificationError::Cancelled))),
            "phase={phase:?}, result={result:?}"
        );
        assert_eq!(start.elapsed(), Duration::from_millis(250));
        assert_eq!(role.entered.load(Ordering::SeqCst), 1);
        assert_eq!(role.active.load(Ordering::SeqCst), 0);
    }
    Ok(())
}
