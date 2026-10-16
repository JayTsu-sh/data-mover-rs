//! A writer that may not give a file to its source owner carries the mode alone and says why.

use super::*;

fn setuid_source() -> MetadataObservations {
    MetadataObservations {
        ownership_mode: value(OwnershipMode {
            uid: 0,
            gid: 0,
            mode: 0o106_755,
        }),
        ..exact_observations()
    }
}

fn not_permitted(policy: MetadataPolicy) -> Result<MetadataPlan, MetadataPlanError> {
    compile_metadata_plan(&MetadataPlanRequest {
        observations: &setuid_source(),
        target: MetadataTarget {
            ownership_mode: OwnershipTarget::NotPermitted,
            ..exact_target()
        },
        policies: all_exact().with_ownership_mode(policy),
        principal_mapper: None,
    })
}

/// The file stays owned by the writer, so a setuid or setgid bit would make someone else's program
/// run as the writer: both go, the permission bits stay.
#[test]
fn the_mode_is_carried_without_its_set_id_bits_and_the_loss_is_named() {
    let plan = not_permitted(MetadataPolicy::AllowKnownLoss).unwrap();
    assert_eq!(
        plan.mutations
            .iter()
            .find(|(family, _)| *family == MetadataFamily::OwnershipMode)
            .map(|(_, mutation)| mutation.clone()),
        Some(MetadataMutation::Mode(0o755))
    );
    assert!(plan.loss_report().losses().contains(&(
        MetadataFamily::OwnershipMode,
        SemanticLoss::OwnerAndGroupNotPermitted
    )));
    assert_eq!(plan.skipped(), [], "a loss, not a skip");
}

#[test]
fn an_exact_policy_refuses_the_loss_by_name() {
    let error = not_permitted(MetadataPolicy::RequireExact).unwrap_err();
    assert_eq!(
        error.cause(),
        RefusalCause::LossRejected(SemanticLoss::OwnerAndGroupNotPermitted)
    );
    assert!(
        error
            .to_string()
            .contains("may not give the file this owner or group")
    );
}

/// A source without a numeric owner already carries its mode alone; a destination that could not
/// have set the owner anyway changes nothing about that.
#[test]
fn a_source_without_an_owner_keeps_its_own_loss() {
    let plan = compile_copied_metadata_plan(
        &MetadataPlanRequest {
            observations: &MetadataObservations::default(),
            target: MetadataTarget {
                ownership_mode: OwnershipTarget::NotPermitted,
                ..exact_target()
            },
            policies: MetadataPolicies::default()
                .with_ownership_mode(MetadataPolicy::AllowKnownLoss),
            principal_mapper: None,
        },
        Some(0o100_640),
        false,
    )
    .unwrap();
    assert_eq!(
        plan.mutations,
        [(MetadataFamily::OwnershipMode, MetadataMutation::Mode(0o640))]
    );
    assert_eq!(
        plan.loss_report().losses(),
        &[(
            MetadataFamily::OwnershipMode,
            SemanticLoss::OwnerAndGroupDropped
        )]
    );
}
