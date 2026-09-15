use super::source::NfsProtocolFailure;
use crate::model::{FailureClass, NfsVersion, Transience};

pub(crate) fn dialect(version: nfs_rs::NFSVersion) -> Result<NfsVersion, NfsDialectError> {
    match version {
        nfs_rs::NFSVersion::NFSv3 => Ok(NfsVersion::V3),
        nfs_rs::NFSVersion::NFSv4p0 => Ok(NfsVersion::V4_0),
        nfs_rs::NFSVersion::NFSv4p1 => Ok(NfsVersion::V4_1),
        #[allow(deprecated)]
        nfs_rs::NFSVersion::NFSv4 | nfs_rs::NFSVersion::NFSv4p2 | nfs_rs::NFSVersion::Unknown => {
            Err(NfsDialectError)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NfsDialectError;

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn classify_error(error: nfs_rs::NfsError) -> NfsProtocolFailure {
    if let Some(outcome) = error.operation_outcome() {
        let (class, transience) = match outcome.recovery {
            // The transport/session cannot currently serve requests. A retry is certified safe by
            // nfs-rs; a remount explicitly replaces that session. `role_failure` promotes both
            // Connectivity failures to session scope.
            nfs_rs::RecoveryAction::Retry | nfs_rs::RecoveryAction::Remount => {
                (FailureClass::Connectivity, Transience::Transient)
            }
            // Reopen remains entry-scoped because the mounted session is usable.
            nfs_rs::RecoveryAction::Reopen => (FailureClass::Protocol, Transience::Transient),
            // A replay-sensitive mutation may already have taken effect. Do not label it
            // transient: the recovery path must observe the staged object before resuming.
            nfs_rs::RecoveryAction::VerifyThenResume => {
                (FailureClass::Protocol, Transience::Unknown)
            }
            nfs_rs::RecoveryAction::DoNotRetry => (FailureClass::Protocol, Transience::Permanent),
        };
        return NfsProtocolFailure { class, transience };
    }
    let (class, transience) = match &error {
        nfs_rs::NfsError::Unsupported(_) => (FailureClass::Unsupported, Transience::Permanent),
        value if value.is_not_found() => (FailureClass::NotFound, Transience::Permanent),
        nfs_rs::NfsError::Nfs3(
            nfs_rs::Nfs3ErrorCode::NFS3ERR_ACCES | nfs_rs::Nfs3ErrorCode::NFS3ERR_PERM,
        )
        | nfs_rs::NfsError::Nfs4(
            nfs_rs::Nfs4ErrorCode::NFS4ERR_ACCESS | nfs_rs::Nfs4ErrorCode::NFS4ERR_PERM,
        ) => (FailureClass::PermissionDenied, Transience::Permanent),
        nfs_rs::NfsError::Nfs3(nfs_rs::Nfs3ErrorCode::NFS3ERR_NOSPC)
        | nfs_rs::NfsError::Nfs4(nfs_rs::Nfs4ErrorCode::NFS4ERR_NOSPC) => {
            (FailureClass::Capacity, Transience::Permanent)
        }
        nfs_rs::NfsError::InvalidInput(_) => (FailureClass::InvalidInput, Transience::Permanent),
        nfs_rs::NfsError::Io(io) if io.kind() == std::io::ErrorKind::PermissionDenied => {
            (FailureClass::PermissionDenied, Transience::Permanent)
        }
        nfs_rs::NfsError::Io(io) if io.kind() == std::io::ErrorKind::NotFound => {
            (FailureClass::NotFound, Transience::Permanent)
        }
        nfs_rs::NfsError::Io(io)
            if matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::WouldBlock
            ) =>
        {
            (FailureClass::Connectivity, Transience::Transient)
        }
        nfs_rs::NfsError::Io(_) | nfs_rs::NfsError::Rpc(_) => {
            (FailureClass::Connectivity, Transience::Unknown)
        }
        nfs_rs::NfsError::Nfs4(nfs_rs::Nfs4ErrorCode::NFS4ERR_DELAY) => {
            (FailureClass::Protocol, Transience::Transient)
        }
        _ => (FailureClass::Protocol, Transience::Unknown),
    };
    NfsProtocolFailure { class, transience }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_dialects_are_distinct_and_ambiguous_v4_is_rejected() {
        assert_eq!(dialect(nfs_rs::NFSVersion::NFSv4p0), Ok(NfsVersion::V4_0));
        #[allow(deprecated)]
        let ambiguous = dialect(nfs_rs::NFSVersion::NFSv4);
        assert_eq!(ambiguous, Err(NfsDialectError));
    }

    #[test]
    fn role_errors_preserve_actionable_taxonomy() {
        for (error, class, transience) in [
            (
                nfs_rs::NfsError::Unsupported("ACL unavailable".to_owned()),
                FailureClass::Unsupported,
                Transience::Permanent,
            ),
            (
                nfs_rs::NfsError::Nfs4(nfs_rs::Nfs4ErrorCode::NFS4ERR_ACCESS),
                FailureClass::PermissionDenied,
                Transience::Permanent,
            ),
            (
                nfs_rs::NfsError::Nfs4(nfs_rs::Nfs4ErrorCode::NFS4ERR_NOENT),
                FailureClass::NotFound,
                Transience::Permanent,
            ),
            (
                nfs_rs::NfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "connection reset",
                )),
                FailureClass::Connectivity,
                Transience::Transient,
            ),
        ] {
            let failure = classify_error(error);
            assert_eq!(failure.class, class);
            assert_eq!(failure.transience, transience);
        }
    }

    fn outcome_error(
        outcome: nfs_rs::OperationOutcome,
        operation_class: nfs_rs::OperationClass,
        recovery: nfs_rs::RecoveryAction,
    ) -> nfs_rs::NfsError {
        nfs_rs::NfsError::OperationOutcome(Box::new(nfs_rs::OperationOutcomeError::new(
            outcome,
            operation_class,
            recovery,
            nfs_rs::RequestContext {
                operation: "test".to_owned(),
                protocol: nfs_rs::NFSVersion::NFSv4p1,
                request_id: None,
            },
            nfs_rs::NfsError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "test timeout",
            )),
        )))
    }

    #[test]
    fn structured_outcomes_preserve_retry_and_verification_safety() {
        for (error, class, transience) in [
            (
                outcome_error(
                    nfs_rs::OperationOutcome::SafeToRetry,
                    nfs_rs::OperationClass::ReadOnly,
                    nfs_rs::RecoveryAction::Retry,
                ),
                FailureClass::Connectivity,
                Transience::Transient,
            ),
            (
                outcome_error(
                    nfs_rs::OperationOutcome::Uncertain,
                    nfs_rs::OperationClass::ReplaySensitive,
                    nfs_rs::RecoveryAction::VerifyThenResume,
                ),
                FailureClass::Protocol,
                Transience::Unknown,
            ),
            (
                outcome_error(
                    nfs_rs::OperationOutcome::Uncertain,
                    nfs_rs::OperationClass::SessionControl,
                    nfs_rs::RecoveryAction::Remount,
                ),
                FailureClass::Connectivity,
                Transience::Transient,
            ),
            (
                outcome_error(
                    nfs_rs::OperationOutcome::DefiniteFailure,
                    nfs_rs::OperationClass::ReplaySensitive,
                    nfs_rs::RecoveryAction::DoNotRetry,
                ),
                FailureClass::Protocol,
                Transience::Permanent,
            ),
        ] {
            let failure = classify_error(error);
            assert_eq!(failure.class, class);
            assert_eq!(failure.transience, transience);
        }
    }
}
