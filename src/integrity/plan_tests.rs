use super::*;
use crate::model::{
    AclMetadata, ExtendedAttribute, MetadataObservation, MetadataProvenance, Transience,
};

fn observed<T>(value: T) -> MetadataObservation<T> {
    MetadataObservation::Value {
        value,
        provenance: MetadataProvenance::AdditionalCall,
    }
}

fn request(
    source_acl: MetadataObservation<AclMetadata>,
    destination_acl: MetadataObservation<AclMetadata>,
    source_xattrs: MetadataObservation<Vec<ExtendedAttribute>>,
    destination_xattrs: MetadataObservation<Vec<ExtendedAttribute>>,
) -> Result<IntegrityRequest> {
    Ok(IntegrityRequest {
        source: described_storage_with_optional(
            "dxn-nfs4-source",
            b"same",
            None,
            None,
            source_acl,
            source_xattrs,
            BackendKind::Nfs,
        )?,
        source_path: StoragePath::new("object.bin")?,
        destination: described_storage_with_optional(
            "dxn-nfs4-destination",
            b"same",
            None,
            None,
            destination_acl,
            destination_xattrs,
            BackendKind::Nfs,
        )?,
        destination_path: StoragePath::new("object.bin")?,
        plan: IntegrityPlan::default(),
        options: IntegrityOptions::default(),
        cancel: tokio_util::sync::CancellationToken::new(),
    })
}

fn xattr(name: &[u8], value: &[u8]) -> Result<ExtendedAttribute> {
    Ok(ExtendedAttribute::new(name.to_vec(), value.to_vec())?)
}

#[tokio::test]
async fn omitted_optional_families_are_not_observed_even_on_nfs4() -> Result {
    let failed_acl = MetadataObservation::Failed {
        class: FailureClass::PermissionDenied,
        transience: Transience::Permanent,
    };
    let failed_xattrs = MetadataObservation::Failed {
        class: FailureClass::PermissionDenied,
        transience: Transience::Permanent,
    };
    let mut request = request(
        failed_acl.clone(),
        failed_acl,
        failed_xattrs.clone(),
        failed_xattrs,
    )?;
    let report = compare(request.clone()).await?;
    assert!(report.matches());
    assert!(!report.ignored().contains(&IntegrityIgnored::Xattrs));
    assert!(
        !report
            .ignored()
            .iter()
            .any(|item| matches!(item, IntegrityIgnored::Acl { .. }))
    );

    request.plan.acl = true;
    let Err(failure) = compare(request.clone()).await else {
        panic!("requested failed ACL observation must fail on its side");
    };
    assert_eq!(failure.side, IntegritySide::Source);

    request.plan.acl = false;
    request.plan.xattrs = true;
    let Err(failure) = compare(request).await else {
        panic!("requested failed xattr observation must fail on its side");
    };
    assert_eq!(failure.side, IntegritySide::Source);
    Ok(())
}

#[tokio::test]
async fn unsupported_nfs4_optional_families_are_reported_as_ignored() -> Result {
    let mut request = request(
        MetadataObservation::Unsupported,
        MetadataObservation::Unsupported,
        MetadataObservation::Unsupported,
        MetadataObservation::Unsupported,
    )?;
    request.plan.acl = true;
    request.plan.xattrs = true;
    let report = compare(request).await?;
    assert!(report.matches());
    assert!(report.ignored().contains(&IntegrityIgnored::Acl {
        source: None,
        destination: None,
    }));
    assert!(report.ignored().contains(&IntegrityIgnored::Xattrs));
    Ok(())
}

#[tokio::test]
async fn xattrs_compare_without_relying_on_listing_order() -> Result {
    let attrs = || {
        Ok::<_, Box<dyn std::error::Error>>(vec![
            xattr(b"user.alpha", b"one")?,
            xattr(b"user.beta", b"two")?,
        ])
    };
    let mut destination_attrs = attrs()?;
    destination_attrs.reverse();
    let mut same_request = request(
        MetadataObservation::Unsupported,
        MetadataObservation::Unsupported,
        observed(attrs()?),
        observed(destination_attrs),
    )?;
    same_request.plan.xattrs = true;
    assert!(compare(same_request).await?.matches());

    let mut different_request = request(
        MetadataObservation::Unsupported,
        MetadataObservation::Unsupported,
        observed(attrs()?),
        observed(vec![xattr(b"user.alpha", b"changed")?]),
    )?;
    different_request.plan.xattrs = true;
    assert_eq!(
        compare(different_request).await?.differences(),
        &[IntegrityDifference::Xattrs]
    );
    Ok(())
}
