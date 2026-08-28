use data_mover::storage::{
    BackendCapabilities, Capability, CapabilityAvailability, PreflightPolicy, UnsupportedReason,
    ValidationGate,
};

#[test]
fn production_preflight_allows_supported_and_rejects_unsupported()
-> Result<(), Box<dyn std::error::Error>> {
    let capabilities = BackendCapabilities::new(
        CapabilityAvailability::Supported,
        CapabilityAvailability::Unsupported(UnsupportedReason::new("read-only endpoint")?),
        CapabilityAvailability::Supported,
        CapabilityAvailability::Supported,
    );
    let policy = PreflightPolicy::production();
    assert!(
        capabilities
            .preflight(Capability::ReadSource, &policy)
            .is_ok()
    );
    let Err(error) = capabilities.preflight(Capability::StagedDestination, &policy) else {
        panic!("unsupported capability must fail before lending");
    };
    assert_eq!(error.capability(), Capability::StagedDestination);
    assert!(matches!(
        error.availability(),
        CapabilityAvailability::Unsupported(_)
    ));
    Ok(())
}

#[test]
fn uncertified_capability_is_disabled_for_public_production_policy()
-> Result<(), Box<dyn std::error::Error>> {
    let required = ValidationGate::new("DM-CIFS-CONTRACT")?;
    let capabilities = BackendCapabilities::new(
        CapabilityAvailability::Supported,
        CapabilityAvailability::Uncertified(required.clone()),
        CapabilityAvailability::Supported,
        CapabilityAvailability::Supported,
    );
    let Err(production) = capabilities.preflight(
        Capability::StagedDestination,
        &PreflightPolicy::production(),
    ) else {
        panic!("production cannot use uncertified behavior");
    };
    assert!(matches!(
        production.availability(),
        CapabilityAvailability::Uncertified(_)
    ));

    Ok(())
}

#[test]
fn validation_fact_values_reject_blank_or_secret_leaking_debug()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(ValidationGate::new("").is_err());
    assert!(UnsupportedReason::new(" ").is_err());
    let reason = UnsupportedReason::new("token=secret")?;
    assert!(!format!("{reason:?}").contains("secret"));
    Ok(())
}
