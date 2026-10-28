use tokio_util::sync::CancellationToken;

use super::super::source::cancelled;
use super::{NfsStagedDestinationAdapter, failure};
use crate::model::acl::{compare_acl, equivalent_native};
use crate::model::{
    AclMetadata, FailureClass, MetadataObservation, MetadataObservations, ObservationMode,
    ObservationPlan, OwnershipMode, Transience,
};
use crate::storage::{MetadataMutation, PreparedStage, StorageRoleFailure};

/// A recovered rename skipped application, so check the final object's ACL before acceptance.
pub(super) async fn validate_recovered_metadata(
    adapter: &NfsStagedDestinationAdapter,
    stage: &PreparedStage,
    mutations: Vec<MetadataMutation>,
    cancel: CancellationToken,
) -> Result<(), StorageRoleFailure> {
    adapter.validate(stage)?;
    let path = stage.final_destination.path();
    let expected_owner = mutations.iter().rev().find_map(|mutation| match mutation {
        MetadataMutation::NumericOwnership(owner) => Some(*owner),
        _ => None,
    });
    for mutation in mutations {
        let MetadataMutation::Acl(expected) = mutation else {
            continue;
        };
        if cancel.is_cancelled() {
            return Err(cancelled(path));
        }
        let metadata = adapter
            .metadata
            .as_ref()
            .ok_or_else(|| failure(path, FailureClass::Unsupported, Transience::Permanent))?;
        let observed = metadata
            .observe(
                path,
                ObservationPlan::default()
                    .with_acl(ObservationMode::Required)
                    .with_ownership_mode(ObservationMode::BestEffort),
            )
            .await?;
        let matches = recovered_acl_matches(&expected, &observed, expected_owner);
        if !matches {
            return Err(failure(path, FailureClass::Protocol, Transience::Permanent));
        }
    }
    Ok(())
}

fn recovered_acl_matches(
    expected: &AclMetadata,
    observed: &MetadataObservations,
    expected_owner: Option<OwnershipMode>,
) -> bool {
    let MetadataObservation::Value { value, .. } = observed.acl() else {
        return false;
    };
    if let MetadataObservation::Value { value: owner, .. } = observed.ownership_mode() {
        if expected_owner.is_some_and(|expected| {
            expected.uid != owner.uid
                || expected.gid != owner.gid
                || (expected.mode & 0o7000) != (owner.mode & 0o7000)
        }) {
            return false;
        }
        compare_acl(
            expected,
            value,
            expected_owner.unwrap_or(*owner),
            *owner,
            true,
        )
        .unwrap_or(false)
    } else {
        expected_owner.is_none() && equivalent_native(expected, value).unwrap_or(false)
    }
}
