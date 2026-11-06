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

impl PermissionFacts {
    pub(crate) fn validate(&self) -> Result<(), crate::ModelValueError> {
        for principal in [&self.owner, &self.group].into_iter().flatten() {
            if let PrincipalIdentity::Named(name) = principal
                && (name.trim().is_empty()
                    || name.contains('\0')
                    || name.len() > crate::MAX_MODEL_FIELD_BYTES)
            {
                return Err(crate::ModelValueError::new(
                    "permission_principal",
                    "must be nonempty, bounded and contain no NUL",
                ));
            }
        }
        if self.mode.is_some_and(|mode| mode & !0o7777 != 0) {
            return Err(crate::ModelValueError::new(
                "permission_mode",
                "must contain permission bits only",
            ));
        }
        Ok(())
    }
}
