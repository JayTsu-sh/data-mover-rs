use crate::model::NfsVersion;

/// Immutable facts captured from one connected NFS instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NfsInstanceFacts {
    pub dialect: NfsVersion,
    pub max_read_size: u32,
    pub max_write_size: u32,
    pub acl: bool,
    pub xattrs: bool,
    pub stable_writes: bool,
}

impl NfsInstanceFacts {
    pub(crate) fn validate(self) -> Result<Self, NfsFactsError> {
        if self.max_read_size == 0 || self.max_write_size == 0 || !self.stable_writes {
            return Err(NfsFactsError);
        }
        if self.dialect == NfsVersion::V3 && (self.acl || self.xattrs) {
            return Err(NfsFactsError);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NfsFactsError;

impl std::fmt::Display for NfsFactsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("invalid negotiated NFS facts")
    }
}

impl std::error::Error for NfsFactsError {}

pub(crate) trait NfsFactsProvider: Send + Sync {
    fn instance_facts(&self) -> Result<NfsInstanceFacts, NfsFactsError>;
}

/// Structured protocol result used before diagnostics are rendered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NfsFailureCode {
    StaleHandle,
    BadHandle,
    ConcurrentLookupMiss,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NfsRetryAction {
    InvalidateAndRetry,
    Return,
}

/// Bounded replay policy shared by NFS source, destination, namespace, and metadata roles.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NfsRetryPolicy {
    max_invalidations: u8,
}

impl NfsRetryPolicy {
    pub(crate) const fn new(max_invalidations: u8) -> Self {
        Self { max_invalidations }
    }

    pub(crate) const fn action(self, code: NfsFailureCode, attempts: u8) -> NfsRetryAction {
        if attempts >= self.max_invalidations {
            return NfsRetryAction::Return;
        }
        match code {
            NfsFailureCode::StaleHandle
            | NfsFailureCode::BadHandle
            | NfsFailureCode::ConcurrentLookupMiss => NfsRetryAction::InvalidateAndRetry,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_facts_reject_unevidenced_or_impossible_claims() {
        let valid = NfsInstanceFacts {
            dialect: NfsVersion::V3,
            max_read_size: 65_536,
            max_write_size: 65_536,
            acl: false,
            xattrs: false,
            stable_writes: true,
        };
        assert!(valid.validate().is_ok());
        assert!(NfsInstanceFacts { acl: true, ..valid }.validate().is_err());
        assert!(
            NfsInstanceFacts {
                max_read_size: 0,
                ..valid
            }
            .validate()
            .is_err()
        );
        assert!(
            NfsInstanceFacts {
                stable_writes: false,
                ..valid
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn retry_is_bounded_and_only_invalidates_safe_protocol_states() {
        let policy = NfsRetryPolicy::new(3);
        for code in [
            NfsFailureCode::StaleHandle,
            NfsFailureCode::BadHandle,
            NfsFailureCode::ConcurrentLookupMiss,
        ] {
            assert_eq!(policy.action(code, 0), NfsRetryAction::InvalidateAndRetry);
            assert_eq!(policy.action(code, 2), NfsRetryAction::InvalidateAndRetry);
            assert_eq!(policy.action(code, 3), NfsRetryAction::Return);
        }
    }

    #[test]
    fn dialects_remain_independent_facts() {
        assert_ne!(NfsVersion::V3, NfsVersion::V4_0);
        assert_ne!(NfsVersion::V4_0, NfsVersion::V4_1);
    }
}
