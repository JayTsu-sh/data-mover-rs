use std::fmt;

/// A failure to construct a capability fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityValueError(&'static str);

impl fmt::Display for CapabilityValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for CapabilityValueError {}

fn required(value: impl Into<String>) -> Result<String, CapabilityValueError> {
    let value = value.into();
    if value.trim().is_empty() || value.contains('\0') {
        Err(CapabilityValueError(
            "capability fact must be non-blank and contain no NUL",
        ))
    } else {
        Ok(value)
    }
}

/// A durable acceptance-gate name.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct ValidationGate(String);

impl ValidationGate {
    /// Creates a named validation gate.
    ///
    /// # Errors
    /// Returns an error for blank or NUL-containing names.
    pub fn new(value: impl Into<String>) -> Result<Self, CapabilityValueError> {
        required(value).map(Self)
    }

    /// Returns the gate name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ValidationGate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ValidationGate")
            .field(&self.0)
            .finish()
    }
}

/// A backend-provided explanation for an unsupported role.
#[derive(Clone, Eq, PartialEq)]
pub struct UnsupportedReason(String);

impl UnsupportedReason {
    /// Creates a reason that adapters must redact before supplying.
    ///
    /// # Errors
    /// Returns an error for blank or NUL-containing values.
    pub fn new(value: impl Into<String>) -> Result<Self, CapabilityValueError> {
        required(value).map(Self)
    }

    /// Returns the adapter-provided reason.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for UnsupportedReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UnsupportedReason(<redacted>)")
    }
}

/// Availability of one role on one connected backend instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CapabilityAvailability {
    Supported,
    Unsupported(UnsupportedReason),
    Uncertified(ValidationGate),
}

/// The four public storage roles.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Capability {
    ReadSource,
    StagedDestination,
    Namespace,
    Metadata,
}

/// Immutable instance-specific role facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendCapabilities {
    read_source: CapabilityAvailability,
    staged_destination: CapabilityAvailability,
    namespace: CapabilityAvailability,
    metadata: CapabilityAvailability,
}

impl BackendCapabilities {
    #[must_use]
    pub const fn new(
        read_source: CapabilityAvailability,
        staged_destination: CapabilityAvailability,
        namespace: CapabilityAvailability,
        metadata: CapabilityAvailability,
    ) -> Self {
        Self {
            read_source,
            staged_destination,
            namespace,
            metadata,
        }
    }

    #[must_use]
    pub const fn availability(&self, capability: Capability) -> &CapabilityAvailability {
        match capability {
            Capability::ReadSource => &self.read_source,
            Capability::StagedDestination => &self.staged_destination,
            Capability::Namespace => &self.namespace,
            Capability::Metadata => &self.metadata,
        }
    }

    /// Validates role availability before callers can cause backend side effects.
    ///
    /// # Errors
    /// Returns the instance capability fact when the role is unsupported or the required
    /// validation gate is not active.
    pub fn preflight(
        &self,
        capability: Capability,
        policy: &PreflightPolicy,
    ) -> Result<(), CapabilityUnavailable> {
        let availability = self.availability(capability);
        let allowed = match availability {
            CapabilityAvailability::Supported => true,
            CapabilityAvailability::Unsupported(_) => false,
            CapabilityAvailability::Uncertified(required) => policy.gate.as_ref() == Some(required),
        };
        if allowed {
            Ok(())
        } else {
            Err(CapabilityUnavailable {
                capability,
                availability: availability.clone(),
            })
        }
    }
}

/// Preflight authority. Production has no validation override.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PreflightPolicy {
    gate: Option<ValidationGate>,
}

impl PreflightPolicy {
    #[must_use]
    pub const fn production() -> Self {
        Self { gate: None }
    }
    #[must_use]
    #[allow(dead_code)]
    pub(crate) const fn validation(gate: ValidationGate) -> Self {
        Self { gate: Some(gate) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_validation_authority_requires_exact_gate() -> Result<(), Box<dyn std::error::Error>>
    {
        let required = ValidationGate::new("DM-CIFS-CONTRACT")?;
        let capabilities = BackendCapabilities::new(
            CapabilityAvailability::Supported,
            CapabilityAvailability::Uncertified(required.clone()),
            CapabilityAvailability::Supported,
            CapabilityAvailability::Supported,
        );
        assert!(
            capabilities
                .preflight(
                    Capability::StagedDestination,
                    &PreflightPolicy::production()
                )
                .is_err()
        );
        assert!(
            capabilities
                .preflight(
                    Capability::StagedDestination,
                    &PreflightPolicy::validation(required)
                )
                .is_ok()
        );
        Ok(())
    }
}

/// Typed refusal returned before a role is lent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityUnavailable {
    capability: Capability,
    availability: CapabilityAvailability,
}

impl CapabilityUnavailable {
    pub(crate) fn missing_role(capability: Capability) -> Self {
        Self {
            capability,
            availability: CapabilityAvailability::Unsupported(UnsupportedReason(
                "connected backend did not provide its declared role".to_owned(),
            )),
        }
    }
    #[must_use]
    pub const fn capability(&self) -> Capability {
        self.capability
    }
    #[must_use]
    pub const fn availability(&self) -> &CapabilityAvailability {
        &self.availability
    }
}

impl fmt::Display for CapabilityUnavailable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "capability {:?} is unavailable", self.capability)
    }
}

impl std::error::Error for CapabilityUnavailable {}
