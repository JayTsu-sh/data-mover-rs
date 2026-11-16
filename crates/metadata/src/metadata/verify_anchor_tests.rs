//! 有身份锚点的目录最终核验：真实 role 调用顺序；不冒充协议现场验证。
#![allow(clippy::unwrap_used)]

use std::{
    collections::VecDeque,
    future::pending,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use super::{
    MetadataPlan, MetadataPolicies, MetadataVerificationError, RefusalCause,
    compile_unavailable_plan,
};
use crate::model::{
    BackendIdentity, BackendKind, BackendSessionFailure, EntryKind, EntryOperationFailure,
    FailureClass, IdentityStrength, MetadataFamily, MetadataObservation, MetadataObservationParts,
    MetadataObservations, MetadataProvenance, ObservationMode, ObservationPlan, Operation,
    OwnershipMode, PermissionFacts, PrincipalIdentity, SourceIdentity, SourceVersion, StoragePath,
    Transience,
};
use crate::storage::{
    BackendCapabilities, CapabilityAvailability, CopiedMetadataObservation, InlineOwnership,
    ListRequest, ListingStream, Metadata, MetadataMutation, Namespace, SourceDescriptor, Storage,
    StorageBuilder, StorageRoleFailure, UnsupportedReason,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Call {
    Stat(bool),
    Opaque,
    Permissions,
}

struct DirectoryTarget {
    ordinary: Mutex<SourceDescriptor>,
    fresh: Mutex<VecDeque<SourceDescriptor>>,
    actual: SourceDescriptor,
    calls: Mutex<Vec<Call>>,
    bound: Mutex<Vec<SourceIdentity>>,
    reject_bound: AtomicBool,
    fresh_failure: Mutex<Option<FailureClass>>,
    permission_override: Mutex<Option<PermissionFacts>>,
    stall: Mutex<Option<Call>>,
    active: AtomicUsize,
    entered: AtomicUsize,
}

struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl DirectoryTarget {
    async fn enter(&self, call: Call) {
        self.calls.lock().unwrap().push(call);
        let stalled = *self.stall.lock().unwrap() == Some(call);
        if stalled {
            self.entered.fetch_add(1, Ordering::SeqCst);
            self.active.fetch_add(1, Ordering::SeqCst);
            let _active = Active(&self.active);
            pending::<()>().await;
        }
    }
    fn observed(descriptor: &SourceDescriptor, plan: ObservationPlan) -> MetadataObservations {
        let ownership_mode = match descriptor.inline_ownership {
            Some(InlineOwnership::Known(value))
                if plan.ownership_mode() != ObservationMode::Omit =>
            {
                MetadataObservation::Value {
                    value,
                    provenance: MetadataProvenance::Inline,
                }
            }
            _ => MetadataObservation::NotRequested,
        };
        MetadataObservationParts {
            ownership_mode,
            xattrs: if plan.xattrs() == ObservationMode::Omit {
                MetadataObservation::NotRequested
            } else {
                MetadataObservation::Value {
                    value: Vec::new(),
                    provenance: MetadataProvenance::AdditionalCall,
                }
            },
            ..MetadataObservationParts::default()
        }
        .build()
        .unwrap()
    }
    fn trace(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl Namespace for DirectoryTarget {
    async fn stat(
        &self,
        _: &StoragePath,
        fresh: bool,
    ) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.enter(Call::Stat(fresh)).await;
        if fresh {
            if let Some(class) = *self.fresh_failure.lock().unwrap() {
                return Err(if class == FailureClass::Connectivity {
                    StorageRoleFailure::Session(
                        BackendSessionFailure::new(
                            Operation::Observe,
                            class,
                            Transience::Transient,
                            "target connection lost",
                        )
                        .unwrap(),
                    )
                } else {
                    StorageRoleFailure::Entry(
                        EntryOperationFailure::new(
                            self.actual.path.clone(),
                            Operation::Observe,
                            class,
                            Transience::Permanent,
                            "fresh path observation refused",
                        )
                        .unwrap(),
                    )
                });
            }
            Ok(self
                .fresh
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| self.actual.clone()))
        } else {
            Ok(self.ordinary.lock().unwrap().clone())
        }
    }
    fn list(&self, _: ListRequest) -> ListingStream {
        panic!("verification must not list")
    }
}

#[async_trait]
impl Metadata for DirectoryTarget {
    fn observe_listed(
        &self,
        descriptor: &SourceDescriptor,
        plan: ObservationPlan,
    ) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
        if plan.xattrs() != ObservationMode::Omit
            || (plan.ownership_mode() != ObservationMode::Omit
                && !matches!(descriptor.inline_ownership, Some(InlineOwnership::Known(_))))
        {
            return None;
        }
        Some(Ok(Self::observed(descriptor, plan)))
    }
    async fn observe(
        &self,
        _: &StoragePath,
        _: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        panic!("verification must bind opaque metadata to identity")
    }
    async fn observe_copy_bound_version(
        &self,
        path: &StoragePath,
        expected: &SourceIdentity,
        version: &SourceVersion,
        plan: ObservationPlan,
    ) -> Result<CopiedMetadataObservation, StorageRoleFailure> {
        self.enter(Call::Opaque).await;
        self.bound.lock().unwrap().push(expected.clone());
        assert_eq!(version, &SourceVersion::Current);
        if self.reject_bound.load(Ordering::SeqCst)
            || expected.identity_key() != self.actual.source_identity.identity_key()
        {
            return Err(StorageRoleFailure::Entry(
                EntryOperationFailure::new(
                    path.clone(),
                    Operation::Observe,
                    FailureClass::Conflict,
                    Transience::Permanent,
                    "bound opaque read observed a replacement",
                )
                .unwrap(),
            ));
        }
        Ok(CopiedMetadataObservation {
            observations: Self::observed(&self.actual, plan),
            mode_without_ownership: None,
            owner_names_unmapped: false,
        })
    }
    async fn permissions(
        &self,
        _: &SourceDescriptor,
    ) -> Result<PermissionFacts, StorageRoleFailure> {
        self.enter(Call::Permissions).await;
        if let Some(value) = self.permission_override.lock().unwrap().as_ref() {
            return Ok(value.clone());
        }
        Ok(PermissionFacts {
            owner: Some(PrincipalIdentity::Numeric(1)),
            group: Some(PrincipalIdentity::Numeric(2)),
            mode: Some(0o751),
        })
    }
    async fn apply(
        &self,
        _: &StoragePath,
        _: MetadataMutation,
        _: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        panic!("verification must not mutate")
    }
}

fn fixture(
    kind: BackendKind,
    opaque: bool,
) -> TestResult<(
    Storage,
    Arc<DirectoryTarget>,
    SourceDescriptor,
    MetadataPlan,
)> {
    let backend = BackendIdentity::new(kind, "directory-verification-test")?;
    let identity = SourceIdentity::new(
        backend.clone(),
        IdentityStrength::StableWithinBackend,
        b"directory-handle",
    )?;
    let anchor = SourceDescriptor::new(
        StoragePath::new("dir")?,
        EntryKind::Directory,
        None,
        identity,
    )
    .with_inline_ownership(InlineOwnership::Known(OwnershipMode {
        uid: 1,
        gid: 2,
        mode: 0o700,
    }));
    let mut actual = anchor.clone();
    actual.inline_ownership = Some(InlineOwnership::Known(OwnershipMode {
        uid: 1,
        gid: 2,
        mode: 0o751,
    }));
    actual.content_version = Some(Bytes::from_static(b"fresh-after-metadata"));
    let mut ordinary = actual.clone();
    ordinary.content_version = Some(Bytes::from_static(b"ordinary-stat"));
    let role = Arc::new(DirectoryTarget {
        ordinary: Mutex::new(ordinary),
        fresh: Mutex::new(VecDeque::from([actual.clone()])),
        actual,
        calls: Mutex::new(Vec::new()),
        bound: Mutex::new(Vec::new()),
        reject_bound: AtomicBool::new(false),
        fresh_failure: Mutex::new(None),
        permission_override: Mutex::new(None),
        stall: Mutex::new(None),
        active: AtomicUsize::new(0),
        entered: AtomicUsize::new(0),
    });
    let unavailable = CapabilityAvailability::Unsupported(UnsupportedReason::new("not needed")?);
    let capabilities = BackendCapabilities::new(
        unavailable.clone(),
        unavailable,
        CapabilityAvailability::Supported,
        CapabilityAvailability::Supported,
    );
    let storage = StorageBuilder::new(backend, capabilities)
        .namespace(role.clone())
        .metadata(role.clone())
        .build()?;
    let mut plan = compile_unavailable_plan(
        MetadataPolicies::default(),
        RefusalCause::DestinationCannotStore,
    );
    plan.mutations.push(if opaque {
        (MetadataFamily::Xattrs, MetadataMutation::Xattrs(Vec::new()))
    } else {
        (
            MetadataFamily::OwnershipMode,
            MetadataMutation::NumericOwnership(OwnershipMode {
                uid: 1,
                gid: 2,
                mode: 0o751,
            }),
        )
    });
    Ok((storage, role, anchor, plan))
}

async fn anchored(
    storage: &Storage,
    anchor: &SourceDescriptor,
    plan: &MetadataPlan,
) -> Result<SourceDescriptor, MetadataVerificationError> {
    plan.verify_published(
        storage,
        &anchor.path,
        &SourceVersion::Current,
        None,
        Some(anchor),
        CancellationToken::new(),
    )
    .await
}

fn replaced(mut descriptor: SourceDescriptor) -> SourceDescriptor {
    descriptor.source_identity = SourceIdentity::new(
        descriptor.source_identity.backend().clone(),
        IdentityStrength::StableWithinBackend,
        b"replacement-handle",
    )
    .unwrap();
    descriptor
}

fn numeric_permissions(mode: u32) -> PermissionFacts {
    PermissionFacts {
        owner: Some(PrincipalIdentity::Numeric(1)),
        group: Some(PrincipalIdentity::Numeric(2)),
        mode: Some(mode),
    }
}

fn authoritative(anchor: &mut SourceDescriptor, role: &DirectoryTarget) {
    anchor.permissions = Some(Box::new(numeric_permissions(0o700)));
    role.fresh.lock().unwrap().front_mut().unwrap().permissions =
        Some(Box::new(numeric_permissions(0o751)));
}

#[tokio::test]
async fn provided_permissions_use_only_actual_final_fresh_descriptor() -> TestResult {
    for kind in [BackendKind::Local, BackendKind::Nfs] {
        let (storage, role, mut anchor, plan) = fixture(kind, false)?;
        authoritative(&mut anchor, &role);
        let actual = anchored(&storage, &anchor, &plan).await?;
        assert_eq!(
            actual,
            role.actual
                .clone()
                .with_permissions(numeric_permissions(0o751))
        );
        assert_eq!(role.trace(), [Call::Stat(true)]);
        assert!(role.bound.lock().unwrap().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn none_retains_ordinary_descriptor_and_original_permission_reads() -> TestResult {
    let (storage, role, anchor, plan) = fixture(BackendKind::Local, false)?;
    let descriptor = plan
        .verify_published(
            &storage,
            &anchor.path,
            &SourceVersion::Current,
            None,
            None,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        descriptor.content_version,
        Some(Bytes::from_static(b"ordinary-stat"))
    );
    assert_eq!(role.trace(), [Call::Stat(false), Call::Permissions]);
    Ok(())
}

#[tokio::test]
async fn replaced_fresh_identity_is_refused_before_metadata_mismatch() -> TestResult {
    let (storage, role, mut anchor, plan) = fixture(BackendKind::Local, false)?;
    authoritative(&mut anchor, &role);
    let mut changed = replaced(role.actual.clone());
    changed.inline_ownership = Some(InlineOwnership::Known(OwnershipMode {
        uid: 99,
        gid: 99,
        mode: 0,
    }));
    changed.permissions = Some(Box::new(PermissionFacts {
        owner: Some(PrincipalIdentity::Numeric(99)),
        group: Some(PrincipalIdentity::Numeric(99)),
        mode: Some(0),
    }));
    *role.fresh.lock().unwrap() = VecDeque::from([changed]);
    assert!(matches!(
        anchored(&storage, &anchor, &plan).await,
        Err(MetadataVerificationError::VersionChanged { .. })
    ));
    assert_eq!(role.trace(), [Call::Stat(true)]);
    Ok(())
}

#[tokio::test]
async fn opaque_facts_are_bound_before_the_actual_last_fresh() -> TestResult {
    let (storage, role, anchor, plan) = fixture(BackendKind::Nfs, true)?;
    let actual = anchored(&storage, &anchor, &plan).await?;
    assert_eq!(actual, role.actual);
    assert_eq!(
        role.trace(),
        [Call::Stat(false), Call::Opaque, Call::Stat(true)]
    );
    assert_eq!(*role.bound.lock().unwrap(), [anchor.source_identity]);
    Ok(())
}

#[tokio::test]
async fn changed_initial_opaque_identity_is_rejected_even_if_the_final_path_changes_back()
-> TestResult {
    let (storage, role, anchor, plan) = fixture(BackendKind::Local, true)?;
    *role.ordinary.lock().unwrap() = replaced(role.actual.clone());
    assert!(matches!(
        anchored(&storage, &anchor, &plan).await,
        Err(MetadataVerificationError::VersionChanged { .. })
    ));
    assert_eq!(role.trace(), [Call::Stat(false)]);
    assert!(role.bound.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn opaque_replacement_error_survives_even_if_the_path_would_change_back() -> TestResult {
    let (storage, role, anchor, plan) = fixture(BackendKind::Local, true)?;
    role.reject_bound.store(true, Ordering::SeqCst);
    let result = anchored(&storage, &anchor, &plan).await;
    assert!(
        matches!(result, Err(MetadataVerificationError::Read(StorageRoleFailure::Entry(error))) if error.class() == FailureClass::Conflict)
    );
    assert_eq!(role.trace(), [Call::Stat(false), Call::Opaque]);
    Ok(())
}

#[tokio::test]
async fn inline_information_loss_falls_back_and_freshens_again_after_all_reads() -> TestResult {
    let (storage, role, mut anchor, plan) = fixture(BackendKind::Local, false)?;
    authoritative(&mut anchor, &role);
    let mut degraded = role.actual.clone();
    degraded.inline_ownership = None;
    degraded.permissions = None;
    *role.fresh.lock().unwrap() = VecDeque::from([degraded, role.actual.clone()]);
    let result = anchored(&storage, &anchor, &plan).await?;
    assert_eq!(result, role.actual);
    assert_eq!(
        role.trace(),
        [
            Call::Stat(true),
            Call::Opaque,
            Call::Permissions,
            Call::Stat(true)
        ]
    );
    Ok(())
}

#[tokio::test]
async fn existing_partial_permissions_are_never_completed_from_inline_ownership() -> TestResult {
    let (storage, role, mut anchor, plan) = fixture(BackendKind::Local, false)?;
    let permissions = Some(Box::new(PermissionFacts {
        mode: Some(0o751),
        ..PermissionFacts::default()
    }));
    anchor.permissions = permissions.clone();
    role.ordinary.lock().unwrap().permissions = permissions.clone();
    role.fresh.lock().unwrap().front_mut().unwrap().permissions = permissions;
    assert!(matches!(
        anchored(&storage, &anchor, &plan).await,
        Err(MetadataVerificationError::Mismatch { .. })
    ));
    assert_eq!(role.trace(), [Call::Stat(false), Call::Stat(true)]);
    Ok(())
}

#[tokio::test]
async fn fresh_unsupported_and_session_failure_are_never_downgraded_to_ordinary_stat() -> TestResult
{
    for class in [FailureClass::Unsupported, FailureClass::Connectivity] {
        let (storage, role, mut anchor, plan) = fixture(BackendKind::Nfs, false)?;
        authoritative(&mut anchor, &role);
        *role.fresh_failure.lock().unwrap() = Some(class);
        let result = anchored(&storage, &anchor, &plan).await;
        match result {
            Err(MetadataVerificationError::Read(StorageRoleFailure::Entry(error))) => {
                assert_eq!(error.class(), class);
            }
            Err(MetadataVerificationError::Read(StorageRoleFailure::Session(error))) => {
                assert_eq!(error.class(), class);
            }
            other => panic!("expected original typed fresh failure: {other:?}"),
        }
        assert_eq!(role.trace(), [Call::Stat(true)]);
    }
    Ok(())
}

#[tokio::test]
async fn empty_plan_still_freshens_with_anchor_without_requiring_a_metadata_role() -> TestResult {
    let (_, role, anchor, mut plan) = fixture(BackendKind::Local, false)?;
    plan.mutations.clear();
    let unavailable =
        CapabilityAvailability::Unsupported(UnsupportedReason::new("no metadata needed")?);
    let storage = StorageBuilder::new(
        anchor.source_identity.backend().clone(),
        BackendCapabilities::new(
            unavailable.clone(),
            unavailable.clone(),
            CapabilityAvailability::Supported,
            unavailable,
        ),
    )
    .namespace(role.clone())
    .build()?;
    assert_eq!(anchored(&storage, &anchor, &plan).await?, role.actual);
    assert_eq!(role.trace(), [Call::Stat(true)]);
    role.calls.lock().unwrap().clear();
    *role.fresh.lock().unwrap() = VecDeque::from([replaced(role.actual.clone())]);
    assert!(matches!(
        anchored(&storage, &anchor, &plan).await,
        Err(MetadataVerificationError::VersionChanged { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn invalid_anchor_combinations_are_rejected_before_any_read() -> TestResult {
    let (storage, role, anchor, plan) = fixture(BackendKind::Local, false)?;
    let path = anchor.path.clone();
    for (version, expected, changed) in [
        (SourceVersion::Id("v1".into()), None, anchor.clone()),
        (SourceVersion::Current, Some("v1"), anchor.clone()),
        (SourceVersion::Current, None, {
            let mut value = anchor.clone();
            value.path = StoragePath::new("other")?;
            value
        }),
        (SourceVersion::Current, None, {
            let mut value = anchor.clone();
            value.kind = EntryKind::File;
            value
        }),
        (SourceVersion::Current, None, {
            let mut value = anchor.clone();
            value.version = SourceVersion::Id("v1".into());
            value
        }),
        (SourceVersion::Current, None, {
            let mut value = anchor.clone();
            value.source_identity = SourceIdentity::new(
                value.source_identity.backend().clone(),
                IdentityStrength::PathScoped,
                b"dir",
            )?;
            value
        }),
    ] {
        assert!(matches!(
            plan.verify_published(
                &storage,
                &path,
                &version,
                expected,
                Some(&changed),
                CancellationToken::new()
            )
            .await,
            Err(MetadataVerificationError::VersionChanged { .. })
        ));
        assert!(role.trace().is_empty());
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancellation_drops_each_anchored_read_and_prevents_the_next_one() -> TestResult {
    for phase in [Call::Stat(true), Call::Opaque, Call::Permissions] {
        let (storage, role, mut anchor, plan) = fixture(BackendKind::Local, phase == Call::Opaque)?;
        if phase == Call::Stat(true) {
            authoritative(&mut anchor, &role);
        }
        if phase == Call::Permissions {
            anchor.inline_ownership = None;
            role.ordinary.lock().unwrap().inline_ownership = None;
        }
        *role.stall.lock().unwrap() = Some(phase);
        let cancel = CancellationToken::new();
        let cancelling = cancel.clone();
        let watcher = tokio::spawn(async move {
            sleep(Duration::from_millis(250)).await;
            cancelling.cancel();
        });
        let result = timeout(
            Duration::from_secs(1),
            plan.verify_published(
                &storage,
                &anchor.path,
                &SourceVersion::Current,
                None,
                Some(&anchor),
                cancel,
            ),
        )
        .await;
        watcher.await?;
        assert!(
            matches!(result, Ok(Err(MetadataVerificationError::Cancelled))),
            "phase={phase:?} result={result:?}"
        );
        assert_eq!(role.entered.load(Ordering::SeqCst), 1);
        assert_eq!(role.active.load(Ordering::SeqCst), 0);
        assert_eq!(role.trace().last(), Some(&phase));
    }
    Ok(())
}

#[tokio::test]
async fn known_ownership_without_permissions_uses_the_complete_fallback() -> TestResult {
    for kind in [BackendKind::Local, BackendKind::Nfs] {
        let (storage, role, anchor, plan) = fixture(kind, false)?;
        assert!(anchor.permissions.is_none());
        assert_eq!(anchored(&storage, &anchor, &plan).await?, role.actual);
        assert_eq!(
            role.trace(),
            [Call::Stat(false), Call::Permissions, Call::Stat(true)]
        );
    }
    Ok(())
}

#[tokio::test]
async fn known_inline_ownership_never_bypasses_overridden_permission_facts() -> TestResult {
    let (storage, role, anchor, plan) = fixture(BackendKind::Local, false)?;
    *role.permission_override.lock().unwrap() = Some(PermissionFacts {
        owner: Some(PrincipalIdentity::Named("independent-owner".into())),
        group: Some(PrincipalIdentity::Named("independent-group".into())),
        mode: Some(0o751),
    });
    assert!(matches!(
        anchored(&storage, &anchor, &plan).await,
        Err(MetadataVerificationError::Mismatch { .. })
    ));
    assert_eq!(
        role.trace(),
        [Call::Stat(false), Call::Permissions, Call::Stat(true)]
    );
    Ok(())
}
