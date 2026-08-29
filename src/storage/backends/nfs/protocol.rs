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
}
