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
    /// Returns the instance capability fact when the role is unsupported.
    pub fn preflight(&self, capability: Capability) -> Result<(), CapabilityUnavailable> {
        let availability = self.availability(capability);
        if matches!(availability, CapabilityAvailability::Supported) {
            Ok(())
        } else {
            Err(CapabilityUnavailable {
                capability,
                availability: availability.clone(),
            })
        }
    }
}

/// Typed refusal returned before a role is lent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityUnavailable {
    capability: Capability,
    availability: CapabilityAvailability,
}

impl CapabilityUnavailable {
    #[must_use]
    pub fn missing_role(capability: Capability) -> Self {
        Self {
            capability,
            availability: CapabilityAvailability::Unsupported(UnsupportedReason(
                "connected backend did not provide its declared role".to_owned(),
            )),
        }
    }
    /// A role that is lent but cannot serve the operation asked of it, for `reason`.
    #[must_use]
    pub fn unsupported(capability: Capability, reason: &str) -> Self {
        Self {
            capability,
            availability: CapabilityAvailability::Unsupported(UnsupportedReason(reason.to_owned())),
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
