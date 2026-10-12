//! What `GetBucketVersioning` answered when a bucket was connected decides, once, whether its
//! listings give each key its history (P7 §10.1, Q26, Q-A): enabled, suspended or a refusal to
//! say lists histories; never versioned, a store without versioning, or a NAS bucket does not;
//! no answer at all refuses the role connection with a transient `Connectivity` failure.

use super::*;
use crate::model::{BackendSessionFailure, FailureClass, Operation, Transience};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn answered(answer: Option<S3VersioningAtConnect>, versioning: Versioning) -> Arc<MemoryS3> {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(versioning);
    if let Some(answer) = answer {
        protocol.answer_at_connect(answer);
    }
    protocol
}

fn histories(protocol: &Arc<MemoryS3>) -> TestResult<bool> {
    let storage = connect(protocol.clone(), identity(), None)?;
    Ok(storage.namespace()?.capabilities().histories)
}

#[test]
fn the_answer_at_connection_decides_whether_histories_are_listed() -> TestResult {
    use S3VersioningAtConnect::{Enabled, Never, NoVersioning, Refused, Suspended};
    let cases = [
        (None, Versioning::Enabled, true),
        (None, Versioning::Suspended, true),
        (None, Versioning::Off, false),
        (Some(Enabled), Versioning::Off, true),
        (Some(Suspended), Versioning::Off, true),
        // Q26: a refusal to say lists histories — correct on any bucket, only slower.
        (Some(Refused), Versioning::Off, true),
        (Some(Refused), Versioning::Enabled, true),
        // The answer at connection, not the bucket's versioning, decides.
        (Some(Never), Versioning::Enabled, false),
        (Some(NoVersioning), Versioning::Enabled, false),
    ];
    for (answer, versioning, expected) in cases {
        let protocol = answered(answer, versioning);
        assert_eq!(
            histories(&protocol)?,
            expected,
            "{answer:?} with {versioning:?}"
        );
    }
    Ok(())
}

/// No answer: whether the bucket keeps histories is unknown, and guessing would make two scans of
/// one bucket disagree, so the role connection fails — typed, transient — before any request.
#[test]
fn no_answer_refuses_the_role_connection_as_a_transient_connectivity_failure() {
    let protocol = answered(
        Some(S3VersioningAtConnect::Unreachable { status: Some(503) }),
        Versioning::Enabled,
    );
    let refused = connect(protocol, identity(), None);
    let Err(error) = refused else {
        panic!("a bucket whose versioning got no answer connected");
    };
    let failure = error
        .downcast_ref::<BackendSessionFailure>()
        .unwrap_or_else(|| panic!("untyped: {error}"));
    assert_eq!(failure.class(), FailureClass::Connectivity);
    assert_eq!(failure.transience(), Transience::Transient);
    assert_eq!(failure.operation(), Operation::Connect);
    assert!(failure.diagnostic().contains("HTTP 503"), "{failure:?}");
}

/// A NAS bucket never lists histories: its profile keeps no versions, whatever the answer was —
/// also none at all, which does not refuse its connection.
#[test]
fn a_nas_bucket_never_lists_histories_whatever_the_answer() -> TestResult {
    use S3VersioningAtConnect::{Enabled, Refused, Suspended, Unreachable};
    for answer in [Enabled, Suspended, Refused, Unreachable { status: None }] {
        let protocol = Arc::new(MemoryS3::nas_bucket());
        protocol.answer_at_connect(answer);
        assert!(!histories(&protocol)?, "{answer:?}");
    }
    Ok(())
}
