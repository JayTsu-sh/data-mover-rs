//! Typed SDK and response-body causes, independent of operation replay safety.
use std::error::Error as StdError;
use std::fmt::Debug;
use std::io::{Error as IoError, ErrorKind};

use aws_smithy_runtime_api::client::result::{ConnectorError, SdkError};
use hyper::Error as HttpError;

use crate::model::{FailureClass, Transience};
use crate::storage::backends::s3::S3ProtocolFailure;

pub(super) fn sdk_failure<E: StdError + 'static, R: Debug + 'static>(
    error: &SdkError<E, R>,
    diagnostic: &'static str,
) -> Option<S3ProtocolFailure> {
    let class = match error {
        SdkError::TimeoutError(_) => Some(FailureClass::Timeout),
        _ => cause_class(error),
    }?;
    Some(S3ProtocolFailure::session(
        class,
        Transience::Transient,
        diagnostic,
    ))
}

pub(super) fn body_failure<E: StdError + 'static>(error: &E) -> S3ProtocolFailure {
    // Preserve the existing lost-body result for opaque streaming errors.
    let class = cause_class(error).unwrap_or(FailureClass::Connectivity);
    S3ProtocolFailure::session(class, Transience::Transient, error.to_string())
}

pub(super) fn cause_class(error: &(dyn StdError + 'static)) -> Option<FailureClass> {
    let mut cause = Some(error);
    let mut interrupted = false;
    while let Some(error) = cause {
        if let Some(connector) = error.downcast_ref::<ConnectorError>() {
            if connector.is_timeout() {
                return Some(FailureClass::Timeout);
            }
            interrupted |= connector.is_io();
        }
        if let Some(io) = error.downcast_ref::<IoError>() {
            if io.kind() == ErrorKind::TimedOut {
                return Some(FailureClass::Timeout);
            }
            interrupted |= matches!(
                io.kind(),
                ErrorKind::BrokenPipe
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionRefused
                    | ErrorKind::NotConnected
                    | ErrorKind::UnexpectedEof
            );
        }
        if let Some(http) = error.downcast_ref::<HttpError>() {
            if http.is_timeout() {
                return Some(FailureClass::Timeout);
            }
            interrupted |= http.is_closed() || http.is_incomplete_message() || http.is_canceled();
        }
        cause = error.source();
    }
    interrupted.then_some(FailureClass::Connectivity)
}
