//! Permission facts for comparison, without inventing mappings between principal namespaces.

/// A principal as the backend reports it. Numeric and named identities are not interchangeable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrincipalIdentity {
    Numeric(u32),
    Named(String),
}

/// Observed permission facts. `None` means the backend cannot supply that fact.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PermissionFacts {
    pub owner: Option<PrincipalIdentity>,
    pub group: Option<PrincipalIdentity>,
    pub mode: Option<u32>,
}
