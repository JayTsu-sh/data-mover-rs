#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use super::*;
use crate::integrity::AclComparisonScope;
use crate::model::acl::{NfsAce, PosixAcl, PosixEntry, decode_nfs4, encode_nfs4, encode_posix};
use crate::model::{
    AclEncoding, AclMetadata, EntryKind, MetadataObservation, MetadataObservations,
    MetadataProvenance, ObservationMode, ObservationPlan, OwnershipMode, Transience,
};
use crate::storage::{
    ByteStream, Metadata, MetadataMutation, PermissionFacts, PrincipalIdentity, ReadRequest,
    ReadSource, SourceDescriptor, StorageRoleFailure,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

const OWNER: OwnershipMode = OwnershipMode {
    uid: 42000,
    gid: 42001,
    mode: 0o640,
};
const BASE: u32 = 0x12_0080;

type AclObservation = MetadataObservation<AclMetadata>;

#[derive(Clone)]
struct Side {
    acl: AclObservation,
    owner: Option<OwnershipMode>,
    kind: EntryKind,
}

impl Side {
    fn new(acl: AclMetadata) -> Self {
        Self {
            acl: value(acl),
            owner: Some(OWNER),
            kind: EntryKind::File,
        }
    }
}

struct NumericAclBackend {
    inner: TimelessBackend,
    side: Side,
}

#[async_trait::async_trait]
impl ReadSource for NumericAclBackend {
    async fn describe(
        &self,
        path: &StoragePath,
    ) -> std::result::Result<SourceDescriptor, StorageRoleFailure> {
        let mut entry = self.inner.describe(path).await?;
        entry.kind = self.side.kind;
        if entry.kind == EntryKind::Directory {
            entry.size = None;
        }
        Ok(entry)
    }
    async fn read(
        &self,
        request: ReadRequest,
    ) -> std::result::Result<ByteStream, StorageRoleFailure> {
        self.inner.read(request).await
    }
}

#[async_trait::async_trait]
impl Metadata for NumericAclBackend {
    async fn permissions(
        &self,
        _: &SourceDescriptor,
    ) -> std::result::Result<PermissionFacts, StorageRoleFailure> {
        Ok(match self.side.owner {
            Some(owner) => PermissionFacts {
                owner: Some(PrincipalIdentity::Numeric(owner.uid)),
                group: Some(PrincipalIdentity::Numeric(owner.gid)),
                mode: Some(owner.mode),
            },
            None => PermissionFacts {
                owner: Some(PrincipalIdentity::Named("alice@example".into())),
                group: Some(PrincipalIdentity::Named("staff@example".into())),
                mode: Some(0o640),
            },
        })
    }
    async fn observe(
        &self,
        _: &StoragePath,
        plan: ObservationPlan,
    ) -> std::result::Result<MetadataObservations, StorageRoleFailure> {
        Ok(MetadataObservations::new(
            if plan.acl() == ObservationMode::Omit {
                MetadataObservation::NotRequested
            } else {
                self.side.acl.clone()
            },
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            if plan.ownership_mode() == ObservationMode::Omit {
                MetadataObservation::NotRequested
            } else {
                self.side
                    .owner
                    .map_or(MetadataObservation::Unsupported, value)
            },
            if plan.timestamps() == ObservationMode::Omit {
                MetadataObservation::NotRequested
            } else {
                MetadataObservation::NotApplicable
            },
        )
        .unwrap())
    }
    async fn apply(
        &self,
        _: &StoragePath,
        _: MetadataMutation,
        _: CancellationToken,
    ) -> std::result::Result<(), StorageRoleFailure> {
        panic!("integrity comparison must never mutate metadata")
    }
}

fn value<T>(value: T) -> MetadataObservation<T> {
    MetadataObservation::Value {
        value,
        provenance: MetadataProvenance::AdditionalCall,
    }
}

fn connected(name: &str, side: Side) -> Result<Storage> {
    let identity = BackendIdentity::new(BackendKind::Nfs, name)?;
    let backend = Arc::new(NumericAclBackend {
        inner: TimelessBackend {
            identity: identity.clone(),
            payload: bytes::Bytes::from_static(b"same"),
            timestamp: None,
            read_limits: None,
            acl: side.acl.clone(),
            xattrs: MetadataObservation::NotRequested,
        },
        side,
    });
    let yes = crate::storage::CapabilityAvailability::Supported;
    let no = crate::storage::CapabilityAvailability::Unsupported(
        crate::storage::UnsupportedReason::new("read-only integrity fixture")?,
    );
    Ok(Storage::connected(
        identity,
        crate::storage::BackendCapabilities::new(yes.clone(), no.clone(), no, yes),
        Some(backend.clone()),
        None,
        None,
        Some(backend),
        None,
    )?)
}

fn request(source: Side, destination: Side, plan: IntegrityPlan) -> Result<IntegrityRequest> {
    Ok(IntegrityRequest {
        source: connected("strict-acl-source", source)?,
        source_path: StoragePath::new("object")?,
        destination: connected("strict-acl-destination", destination)?,
        destination_path: StoragePath::new("object")?,
        plan,
        options: IntegrityOptions::default(),
        cancel: CancellationToken::new(),
    })
}

fn posix(named_permissions: u32, default: bool) -> AclMetadata {
    let entry = |tag, id, permissions| PosixEntry {
        tag,
        id,
        permissions,
    };
    let access = vec![
        entry(1, u32::MAX, 6),
        entry(2, 42421, named_permissions),
        entry(4, u32::MAX, 4),
        entry(8, 42422, 4),
        entry(16, u32::MAX, 4),
        entry(32, u32::MAX, 0),
    ];
    encode_posix(&PosixAcl {
        default: if default { access.clone() } else { vec![] },
        access,
    })
    .unwrap()
}

/// An independent expected ACL, not produced by the mapper being checked by `compare()`.
fn nfs4(named_permissions: u32) -> AclMetadata {
    let ace = |who: &str, flags, permissions| NfsAce {
        ace_type: 0,
        flags,
        permissions,
        who: who.into(),
    };
    encode_nfs4(&[
        ace("OWNER@", 0, 0x16_0187),
        ace("42421", 0, BASE | named_permissions),
        ace("GROUP@", 0x40, BASE | 1),
        ace("42422", 0x40, BASE | 1),
        ace("EVERYONE@", 0, BASE),
    ])
    .unwrap()
}

fn native_plan(source: &AclMetadata, destination: &AclMetadata) -> IntegrityPlan {
    if source.encoding() == destination.encoding() {
        IntegrityPlan::default().with_required_acl()
    } else {
        IntegrityPlan::default().with_acl_equivalence()
    }
}

fn assert_acl_not_ignored(report: &crate::integrity::IntegrityReport) {
    assert!(
        !report
            .ignored()
            .iter()
            .any(|item| matches!(item, IntegrityIgnored::Acl { .. })),
        "{report:?}"
    );
}

fn assert_acl_failed(report: &crate::integrity::IntegrityReport) {
    assert!(!report.matches(), "{report:?}");
    assert_eq!(report.acl_scope(), None, "{report:?}");
    assert!(
        report.differences().iter().any(|difference| matches!(
            difference,
            IntegrityDifference::Acl { .. } | IntegrityDifference::AclUnverified { .. }
        )),
        "{report:?}"
    );
    assert_acl_not_ignored(report);
}

#[tokio::test]
async fn all_four_acl_combinations_pass_only_with_complete_equal_acl_evidence() -> Result {
    for source in [posix(7, false), nfs4(1)] {
        for destination in [posix(7, false), nfs4(1)] {
            let plan = native_plan(&source, &destination);
            let scope = if source.encoding() == destination.encoding() {
                AclComparisonScope::NativeConfiguration {
                    encoding: source.encoding(),
                }
            } else {
                AclComparisonScope::CurrentRegularFileAccess
            };
            let report = compare(request(
                Side::new(source.clone()),
                Side::new(destination),
                plan,
            )?)
            .await?;
            assert!(report.matches(), "{report:?}");
            assert_eq!(report.acl_scope(), Some(scope));
            assert_acl_not_ignored(&report);
        }
    }
    Ok(())
}

#[tokio::test]
async fn all_four_combinations_detect_named_permission_loss() -> Result {
    for source in [posix(7, false), nfs4(1)] {
        for destination in [posix(0, false), nfs4(0)] {
            let plan = native_plan(&source, &destination);
            assert_acl_failed(
                &compare(request(
                    Side::new(source.clone()),
                    Side::new(destination),
                    plan,
                )?)
                .await?,
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_posix_keeps_latent_permissions_and_directory_defaults_significant() -> Result {
    let mut source = Side::new(posix(7, true));
    source.kind = EntryKind::Directory;
    let plan = IntegrityPlan::default().with_required_acl();
    assert!(
        compare(request(source.clone(), source.clone(), plan)?)
            .await?
            .matches()
    );
    for acl in [posix(4, true), posix(7, false)] {
        let mut destination = Side::new(acl);
        destination.kind = EntryKind::Directory;
        assert_acl_failed(&compare(request(source.clone(), destination, plan)?).await?);
    }
    Ok(())
}

#[tokio::test]
async fn native_nfs4_requires_ordered_full_ace_preservation() -> Result {
    let mut aces = decode_nfs4(&nfs4(1))?;
    aces.push(NfsAce {
        ace_type: 2,
        flags: 0x10,
        permissions: 0x2_0000,
        who: "alice@example".into(),
    });
    let source = Side::new(encode_nfs4(&aces)?);
    let plan = IntegrityPlan::default().with_required_acl();
    assert!(
        compare(request(source.clone(), source.clone(), plan)?)
            .await?
            .matches()
    );
    aces.swap(0, 1);
    assert_acl_failed(&compare(request(source, Side::new(encode_nfs4(&aces)?), plan)?).await?);
    Ok(())
}

#[tokio::test]
async fn basic_mode_difference_is_accepted_only_after_cross_model_acl_proof() -> Result {
    for reverse in [false, true] {
        let (a, b) = if reverse {
            (nfs4(1), posix(7, false))
        } else {
            (posix(7, false), nfs4(1))
        };
        let source = Side::new(a);
        let mut destination = Side::new(b);
        destination.owner = Some(OwnershipMode {
            mode: 0o600,
            ..OWNER
        });
        let equivalent = IntegrityPlan::default().with_acl_equivalence();
        let report = compare(request(source.clone(), destination.clone(), equivalent)?).await?;
        assert!(report.matches(), "{report:?}");
        assert_eq!(
            report.acl_scope(),
            Some(AclComparisonScope::CurrentRegularFileAccess)
        );
        assert_acl_not_ignored(&report);
        let unqualified = compare(request(
            source,
            destination,
            IntegrityPlan::default().with_required_acl(),
        )?)
        .await?;
        assert_acl_failed(&unqualified);
        assert!(
            unqualified
                .differences()
                .iter()
                .any(|item| matches!(item, IntegrityDifference::Permissions { .. }))
        );
    }
    Ok(())
}

#[tokio::test]
async fn failed_acl_proof_and_same_model_comparisons_still_check_all_mode_bits() -> Result {
    for destination_acl in [nfs4(0), posix(7, false)] {
        let source = Side::new(posix(7, false));
        let mut destination = Side::new(destination_acl);
        destination.owner = Some(OwnershipMode {
            mode: 0o600,
            ..OWNER
        });
        let report = compare(request(
            source,
            destination,
            IntegrityPlan::default().with_acl_equivalence(),
        )?)
        .await?;
        assert!(!report.matches());
        assert!(
            report
                .differences()
                .iter()
                .any(|item| matches!(item, IntegrityDifference::Permissions { .. })),
            "{report:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn successful_cross_acl_proof_never_hides_special_bits_or_owner_group_changes() -> Result {
    for owner in [
        OwnershipMode {
            mode: 0o4600,
            ..OWNER
        },
        OwnershipMode {
            mode: 0o2600,
            ..OWNER
        },
        OwnershipMode {
            mode: 0o1600,
            ..OWNER
        },
        OwnershipMode {
            uid: OWNER.uid + 1,
            ..OWNER
        },
        OwnershipMode {
            gid: OWNER.gid + 1,
            ..OWNER
        },
    ] {
        let mut destination = Side::new(nfs4(1));
        destination.owner = Some(owner);
        let report = compare(request(
            Side::new(posix(7, false)),
            destination,
            IntegrityPlan::default().with_acl_equivalence(),
        )?)
        .await?;
        assert!(!report.matches());
        assert!(
            report
                .differences()
                .iter()
                .any(|item| matches!(item, IntegrityDifference::Permissions { .. })),
            "{report:?}"
        );
        assert_acl_not_ignored(&report);
    }
    Ok(())
}

#[tokio::test]
async fn missing_or_unsupported_required_acl_is_a_difference_on_either_side() -> Result {
    for plan in [
        IntegrityPlan::default().with_required_acl(),
        IntegrityPlan::default().with_acl_equivalence(),
    ] {
        for observation in [
            MetadataObservation::NotRequested,
            MetadataObservation::NotApplicable,
            MetadataObservation::Unsupported,
        ] {
            for source_missing in [true, false] {
                let mut missing = Side::new(posix(7, false));
                missing.acl = observation.clone();
                let present = Side::new(posix(7, false));
                let (source, destination) = if source_missing {
                    (missing, present)
                } else {
                    (present, missing)
                };
                assert_acl_failed(&compare(request(source, destination, plan)?).await?);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn identical_malformed_native_payloads_never_pass_required_acl_validation() -> Result {
    for encoding in [AclEncoding::Posix, AclEncoding::NfsV4] {
        let bad = Side::new(AclMetadata::new(encoding, vec![1, 2])?);
        for plan in [
            IntegrityPlan::default().with_required_acl(),
            IntegrityPlan::default().with_acl_equivalence(),
        ] {
            assert_acl_failed(&compare(request(bad.clone(), bad.clone(), plan)?).await?);
        }
    }
    Ok(())
}

#[tokio::test]
async fn failed_required_acl_observation_is_attributed_to_the_failing_side() -> Result {
    for (source_failed, side) in [
        (true, IntegritySide::Source),
        (false, IntegritySide::Destination),
    ] {
        let mut failed = Side::new(posix(7, false));
        failed.acl = MetadataObservation::Failed {
            class: FailureClass::PermissionDenied,
            transience: Transience::Permanent,
        };
        let good = Side::new(posix(7, false));
        let (source, destination) = if source_failed {
            (failed, good)
        } else {
            (good, failed)
        };
        let error = compare(request(
            source,
            destination,
            IntegrityPlan::default().with_required_acl(),
        )?)
        .await
        .expect_err("a failed ACL read must not become an ignored ACL");
        assert_eq!(error.side, side);
        assert!(
            matches!(error.failure, StorageRoleFailure::Entry(ref error) if error.class() == FailureClass::PermissionDenied)
        );
    }
    Ok(())
}

#[tokio::test]
async fn cross_model_comparison_rejects_nonnumeric_owner_and_acl_principals() -> Result {
    for reverse in [false, true] {
        let (a, b) = if reverse {
            (nfs4(1), posix(7, false))
        } else {
            (posix(7, false), nfs4(1))
        };
        let mut source = Side::new(a);
        source.owner = None;
        assert_acl_failed(
            &compare(request(
                source,
                Side::new(b),
                IntegrityPlan::default().with_acl_equivalence(),
            )?)
            .await?,
        );
    }
    let mut entries = decode_nfs4(&nfs4(1))?;
    entries
        .iter_mut()
        .find(|entry| entry.who == "42421")
        .unwrap()
        .who = "alice@example".into();
    assert_acl_failed(
        &compare(request(
            Side::new(encode_nfs4(&entries)?),
            Side::new(posix(7, false)),
            IntegrityPlan::default().with_acl_equivalence(),
        )?)
        .await?,
    );
    Ok(())
}

#[tokio::test]
async fn cross_model_directory_acl_is_rejected_in_both_directions() -> Result {
    for reverse in [false, true] {
        let (a, b) = if reverse {
            (nfs4(1), posix(7, false))
        } else {
            (posix(7, false), nfs4(1))
        };
        let mut source = Side::new(a);
        source.kind = EntryKind::Directory;
        let mut destination = Side::new(b);
        destination.kind = EntryKind::Directory;
        assert_acl_failed(
            &compare(request(
                source,
                destination,
                IntegrityPlan::default().with_acl_equivalence(),
            )?)
            .await?,
        );
    }
    Ok(())
}
