//! Strict ACL planning must prove a mapping or refuse before any writes.
use super::*;
use crate::model::{AclMetadata, MetadataObservation, MetadataProvenance};

fn observations(acl: MetadataObservation<AclMetadata>) -> MetadataObservations {
    MetadataObservations {
        acl,
        ownership_mode: MetadataObservation::Value {
            value: OwnershipMode {
                uid: 1000,
                gid: 100,
                mode: 0o100_640,
            },
            provenance: MetadataProvenance::Inline,
        },
        ..MetadataObservations::default()
    }
}

fn plan(
    observations: &MetadataObservations,
    target: AclTarget,
) -> Result<MetadataPlan, MetadataPlanError> {
    compile_metadata_plan(&MetadataPlanRequest {
        observations,
        target: MetadataTarget {
            acl: target,
            xattrs: ValueTarget::Unsupported,
            tags: ValueTarget::Unsupported,
            ownership_mode: OwnershipTarget::Numeric,
            timestamps: TimestampTargetCapability::Unsupported,
        },
        policies: MetadataPolicies::default().with_acl(MetadataPolicy::RequireEquivalent),
        principal_mapper: None,
    })
}

#[test]
fn strict_acl_never_skips_missing_source() {
    for acl in [
        MetadataObservation::Unsupported,
        MetadataObservation::NotRequested,
        MetadataObservation::NotApplicable,
    ] {
        assert!(plan(&observations(acl), AclTarget::Encoding(AclEncoding::NfsV4)).is_err());
    }
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

#[test]
fn strict_acl_maps_numeric_regular_file_both_directions() -> Result<(), Box<dyn std::error::Error>>
{
    let source = observations(MetadataObservation::Value {
        value: basic_acl()?,
        provenance: MetadataProvenance::Inline,
    });
    let mapped = plan(&source, AclTarget::Encoding(AclEncoding::NfsV4))?;
    assert_eq!(
        mapped
            .mappings()
            .iter()
            .find(|v| v.family == MetadataFamily::Acl)
            .map(|v| &v.decision),
        Some(&MappingDecision::Equivalent)
    );
    let destination = mapped
        .mutations
        .iter()
        .find_map(|(_, m)| match m {
            MetadataMutation::Acl(acl) => Some(acl.clone()),
            _ => None,
        })
        .ok_or("mapped ACL mutation is absent")?;
    let source = observations(MetadataObservation::Value {
        value: destination,
        provenance: MetadataProvenance::Inline,
    });
    assert!(plan(&source, AclTarget::Encoding(AclEncoding::Posix)).is_ok());
    assert!(plan(&source, AclTarget::Unsupported).is_err());
    Ok(())
}
