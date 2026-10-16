use data_mover::storage::{
    BackendCapabilities, Capability, CapabilityAvailability, UnsupportedReason,
};

#[test]
fn preflight_allows_supported_and_rejects_unsupported() -> Result<(), Box<dyn std::error::Error>> {
    let supported = CapabilityAvailability::Supported;
    let unsupported =
        CapabilityAvailability::Unsupported(UnsupportedReason::new("read-only endpoint")?);
    for availability in [supported, unsupported] {
        let capabilities = BackendCapabilities::new(
            availability.clone(),
            availability.clone(),
            availability.clone(),
            availability.clone(),
        );
        for capability in [
            Capability::ReadSource,
            Capability::StagedDestination,
            Capability::Namespace,
            Capability::Metadata,
        ] {
            match &availability {
                CapabilityAvailability::Supported => {
                    assert!(capabilities.preflight(capability).is_ok());
                }
                CapabilityAvailability::Unsupported(_) => {
                    let Err(error) = capabilities.preflight(capability) else {
                        return Err(format!(
                            "unsupported capability {capability:?} passed preflight"
                        )
                        .into());
                    };
                    assert_eq!(error.capability(), capability);
                    assert_eq!(error.availability(), &availability);
                }
            }
        }
    }
    Ok(())
}

#[test]
fn unsupported_reason_values_reject_blank_or_secret_leaking_debug()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(UnsupportedReason::new(" ").is_err());
    let reason = UnsupportedReason::new("token=secret")?;
    assert!(!format!("{reason:?}").contains("secret"));
    Ok(())
}
