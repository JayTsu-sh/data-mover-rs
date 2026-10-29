//! Strict ACL planning must prove a mapping or refuse before any writes.
use super::*;
use crate::model::{AclMetadata, MetadataObservation, MetadataProvenance};

fn observations(acl: MetadataObservation<AclMetadata>) -> MetadataObservations {
    crate::model::MetadataObservationParts {
        acl,
        ownership_mode: MetadataObservation::Value {
            value: OwnershipMode {
                uid: 1000,
                gid: 100,
                mode: 0o100_640,
            },
            provenance: MetadataProvenance::Inline,
        },
        ..crate::model::MetadataObservationParts::default()
    }
    .build()
    .unwrap_or_else(|error| panic!("{error}"))
}

fn basic_acl() -> Result<AclMetadata, crate::model::acl::AclError> {
    use crate::model::acl::{PosixAcl, PosixEntry, encode_posix};
    encode_posix(&PosixAcl {
        access: vec![
            PosixEntry {
                tag: 1,
                id: u32::MAX,
                permissions: 6,
            },
            PosixEntry {
                tag: 4,
                id: u32::MAX,
                permissions: 4,
            },
            PosixEntry {
                tag: 32,
                id: u32::MAX,
                permissions: 0,
            },
        ],
        default: Vec::new(),
    })
}

fn partial_plan(
    source: &MetadataObservations,
    ownership: OwnershipTarget,
) -> Result<MetadataPlan, MetadataPlanError> {
    compile_copied_metadata_plan(
        &MetadataPlanRequest {
            observations: source,
            target: MetadataTarget {
                acl: AclTarget::Encoding(AclEncoding::Posix),
                xattrs: ValueTarget::Unsupported,
                tags: ValueTarget::Unsupported,
                ownership_mode: ownership,
                timestamps: TimestampTargetCapability::Unsupported,
            },
            policies: MetadataPolicies::default()
                .with_acl(MetadataPolicy::BestEffort)
                .with_ownership_mode(MetadataPolicy::AllowKnownLoss),
            principal_mapper: None,
        },
        None,
        false,
        true,
    )
}

fn nfs_with_extra(extra: u32) -> Result<AclMetadata, Box<dyn std::error::Error>> {
    use crate::model::acl::{decode_nfs4, encode_nfs4, map_acl};
    let owner = OwnershipMode {
        uid: 1000,
        gid: 100,
        mode: 0o100_640,
    };
    let value = map_acl(&basic_acl()?, AclEncoding::NfsV4, owner)?;
    let mut entries = decode_nfs4(&value)?;
    entries.last_mut().ok_or("missing EVERYONE@")?.permissions |= extra;
    Ok(encode_nfs4(&entries)?)
}

#[tokio::test]
async fn partial_acl_report_survives_application_and_checkpoint_binds_omissions()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::model::acl::{AclCopyIssueKind, AclIssueLocation};
    let source = |extra| -> Result<_, Box<dyn std::error::Error>> {
        Ok(observations(MetadataObservation::Value {
            value: nfs_with_extra(extra)?,
            provenance: MetadataProvenance::Inline,
        }))
    };
    let source_a = source(0x4_0000)?;
    let plan = partial_plan(&source_a, OwnershipTarget::Numeric)?;
    let details = plan.acl_copy_report().ok_or("missing ACL report")?;
    assert!(!details.conservative_fallback);
    assert!(details.issues.iter().any(|issue| matches!(issue.location, AclIssueLocation::NfsAce { .. })
        && matches!(issue.kind, AclCopyIssueKind::UnsupportedRights { rights } if rights & 0x4_0000 != 0)));
    let source_b = source(0x4_0000 | 0x8)?;
    let different_omissions = partial_plan(&source_b, OwnershipTarget::Numeric)?;
    assert_ne!(
        plan.checkpoint_binding(),
        different_omissions.checkpoint_binding()
    );
    let target = AcceptMetadata;
    let report = plan
        .apply(
            &target,
            &StoragePath::new("file")?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(report.acl_copy_report(), Some(details));
    assert!(
        report
            .outcomes()
            .iter()
            .any(|v| v.family == MetadataFamily::Acl
                && v.outcome == ApplicationOutcome::AppliedPartialAcl)
    );
    Ok(())
}

#[tokio::test]
async fn partial_without_preserved_ownership_applies_explicit_deny_all()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::model::acl::{AclCopyIssueKind, decode_posix};
    let source = observations(MetadataObservation::Value {
        value: nfs_with_extra(0)?,
        provenance: MetadataProvenance::Inline,
    });
    let plan = partial_plan(&source, OwnershipTarget::ModeOnly)?;
    let details = plan.acl_copy_report().ok_or("missing ACL report")?;
    assert!(details.conservative_fallback);
    assert!(
        details
            .issues
            .iter()
            .any(|issue| matches!(issue.kind, AclCopyIssueKind::OwnershipNotPreserved))
    );
    let source_acl = source.acl().value().ok_or("missing source ACL")?;
    let original_count = crate::model::acl::decode_nfs4(source_acl)?.len();
    assert_eq!(
        details
            .issues
            .iter()
            .filter(|issue| matches!(issue.kind, AclCopyIssueKind::UncopiedEntry { .. }))
            .count(),
        original_count
    );
    let acl = plan
        .mutations
        .iter()
        .find_map(|(_, mutation)| match mutation {
            MetadataMutation::Acl(acl) => Some(acl),
            _ => None,
        })
        .ok_or("missing conservative ACL")?;
    assert!(
        decode_posix(acl)?
            .access
            .iter()
            .all(|entry| entry.permissions == 0)
    );
    let report = plan
        .apply(
            &AcceptMetadata,
            &StoragePath::new("file")?,
            CancellationToken::new(),
        )
        .await?;
    assert!(
        report
            .outcomes()
            .iter()
            .any(|v| v.family == MetadataFamily::Acl
                && v.outcome == ApplicationOutcome::AppliedConservativeAcl)
    );
    Ok(())
}

struct AcceptMetadata;

#[async_trait::async_trait]
impl Metadata for AcceptMetadata {
    async fn observe(
        &self,
        _: &StoragePath,
        _: crate::model::ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        Ok(MetadataObservations::default())
    }
    async fn apply(
        &self,
        _: &StoragePath,
        _: MetadataMutation,
        _: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        Ok(())
    }
}

#[test]
fn windows_native_acl_is_not_refused_for_absent_unix_ownership()
-> Result<(), Box<dyn std::error::Error>> {
    let acl = AclMetadata::new(AclEncoding::WindowsSecurityDescriptor, vec![1, 2, 3])?;
    let source = crate::model::MetadataObservationParts {
        acl: MetadataObservation::Value {
            value: acl.clone(),
            provenance: MetadataProvenance::Inline,
        },
        ownership_mode: MetadataObservation::NotApplicable,
        ..crate::model::MetadataObservationParts::default()
    }
    .build()
    .unwrap_or_else(|error| panic!("{error}"));
    let plan = compile_copied_metadata_plan(
        &MetadataPlanRequest {
            observations: &source,
            target: MetadataTarget {
                acl: AclTarget::Encoding(AclEncoding::WindowsSecurityDescriptor),
                xattrs: ValueTarget::Unsupported,
                tags: ValueTarget::Unsupported,
                ownership_mode: OwnershipTarget::NotApplicable,
                timestamps: TimestampTargetCapability::Unsupported,
            },
            policies: MetadataPolicies::default().with_acl(MetadataPolicy::BestEffort),
            principal_mapper: None,
        },
        None,
        false,
        true,
    )?;
    assert!(
        plan.mutations
            .iter()
            .any(|(_, m)| matches!(m, MetadataMutation::Acl(value) if value == &acl))
    );
    assert!(
        !plan
            .acl_copy_report()
            .ok_or("missing report")?
            .conservative_fallback
    );
    Ok(())
}

#[test]
fn malformed_acl_is_reported_even_when_ownership_cannot_be_preserved()
-> Result<(), Box<dyn std::error::Error>> {
    let source = observations(MetadataObservation::Value {
        value: AclMetadata::new(AclEncoding::NfsV4, vec![1, 2, 3])?,
        provenance: MetadataProvenance::Inline,
    });
    let error = partial_plan(&source, OwnershipTarget::ModeOnly).unwrap_err();
    assert!(matches!(
        error.cause(),
        RefusalCause::AclMappingRejected { .. }
    ));
    Ok(())
}

#[test]
fn recovery_report_retains_acl_issues_without_claiming_a_new_write()
-> Result<(), Box<dyn std::error::Error>> {
    let source = observations(MetadataObservation::Value {
        value: nfs_with_extra(0x4_0000)?,
        provenance: MetadataProvenance::Inline,
    });
    let plan = partial_plan(&source, OwnershipTarget::Numeric)?;
    let report = plan
        .recovered_acl_report()
        .ok_or("missing recovered report")?;
    assert_eq!(report.acl_copy_report(), plan.acl_copy_report());
    assert!(
        report
            .outcomes()
            .iter()
            .any(|v| v.family == MetadataFamily::Acl
                && v.outcome == ApplicationOutcome::RecoveredFromCheckpoint)
    );
    Ok(())
}

#[test]
fn unsupported_windows_projection_uses_a_protected_empty_dacl()
-> Result<(), Box<dyn std::error::Error>> {
    let source = observations(MetadataObservation::Value {
        value: nfs_with_extra(0)?,
        provenance: MetadataProvenance::Inline,
    });
    let plan = compile_copied_metadata_plan(
        &MetadataPlanRequest {
            observations: &source,
            target: MetadataTarget {
                acl: AclTarget::Encoding(AclEncoding::WindowsSecurityDescriptor),
                xattrs: ValueTarget::Unsupported,
                tags: ValueTarget::Unsupported,
                ownership_mode: OwnershipTarget::NotApplicable,
                timestamps: TimestampTargetCapability::Unsupported,
            },
            policies: MetadataPolicies::default().with_acl(MetadataPolicy::BestEffort),
            principal_mapper: None,
        },
        None,
        false,
        true,
    )?;
    let acl = plan
        .mutations
        .iter()
        .find_map(|(_, mutation)| match mutation {
            MetadataMutation::Acl(acl) => Some(acl),
            _ => None,
        })
        .ok_or("missing fallback descriptor")?;
    let bytes = acl.access().ok_or("missing DACL")?;
    assert_eq!(&bytes[2..4], &0x9004_u16.to_le_bytes());
    assert_eq!(&bytes[16..20], &20_u32.to_le_bytes());
    assert_eq!(&bytes[24..26], &0_u16.to_le_bytes());
    assert!(
        plan.acl_copy_report()
            .ok_or("missing report")?
            .conservative_fallback
    );
    Ok(())
}

#[test]
fn fallback_reports_the_source_mode_when_posix_acl_is_implicit()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::model::acl::AclCopyIssueKind;
    let source = observations(MetadataObservation::Value {
        value: AclMetadata::new_posix(None, None)?,
        provenance: MetadataProvenance::Inline,
    });
    let plan = partial_plan(&source, OwnershipTarget::ModeOnly)?;
    let report = plan.acl_copy_report().ok_or("missing ACL report")?;
    assert!(report.issues.iter().any(|issue| matches!(
        issue.kind,
        AclCopyIssueKind::UncopiedMode {
            mode: Some(0o100_640)
        }
    )));
    Ok(())
}

struct RejectAcl;

#[async_trait::async_trait]
impl Metadata for RejectAcl {
    async fn observe(
        &self,
        _: &StoragePath,
        _: crate::model::ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        Ok(MetadataObservations::default())
    }
    async fn apply(
        &self,
        path: &StoragePath,
        _: MetadataMutation,
        _: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        Err(StorageRoleFailure::Entry(
            crate::model::EntryOperationFailure::new(
                path.clone(),
                crate::model::Operation::Metadata,
                crate::model::FailureClass::Protocol,
                crate::model::Transience::Permanent,
                "ACL SET readback mismatch",
            )
            .unwrap_or_else(|error| panic!("invalid failure: {error}")),
        ))
    }
}

#[tokio::test]
async fn failed_acl_application_preserves_omissions() -> Result<(), Box<dyn std::error::Error>> {
    let source = observations(MetadataObservation::Value {
        value: nfs_with_extra(0)?,
        provenance: MetadataProvenance::Inline,
    });
    let plan = partial_plan(&source, OwnershipTarget::ModeOnly)?;
    let failure = plan
        .apply(
            &RejectAcl,
            &StoragePath::new("file")?,
            CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("unexpected success")?;
    assert!(
        failure
            .failures()
            .iter()
            .any(|f| f.family() == MetadataFamily::Acl)
    );
    assert!(
        failure
            .report()
            .outcomes()
            .iter()
            .any(|o| o.family == MetadataFamily::Acl && o.outcome == ApplicationOutcome::Failed)
    );
    assert!(
        failure
            .report()
            .acl_copy_report()
            .is_some_and(|acl| acl.conservative_fallback && !acl.issues.is_empty())
    );
    Ok(())
}
