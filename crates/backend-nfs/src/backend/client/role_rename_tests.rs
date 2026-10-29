//! Tests of the NFS role's rename against scripted server answers (`rename_entry`).

use std::collections::VecDeque;
use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use nfs_rs::{Nfs3ErrorCode as V3, Nfs4ErrorCode as V4, NfsError};

use super::super::MAX_STALE_RETRIES;
use super::{RenameCalls, rename_entry};
use crate::model::{FailureClass, Transience};
use crate::storage::backends::nfs::source::NfsProtocolFailure;

/// A scripted answer; the last one of a queue repeats forever.
type Answer = fn() -> Result<(), NfsError>;
type ParentsAnswer = fn() -> Result<(), NfsProtocolFailure>;

/// Scripted answers for one rename, recording every call in order.
struct Script {
    parents: Mutex<VecDeque<ParentsAnswer>>,
    rename: Mutex<VecDeque<Answer>>,
    forget: ParentsAnswer,
    calls: Mutex<Vec<&'static str>>,
}

impl Script {
    fn new(rename: &[Answer]) -> Self {
        Self {
            parents: Mutex::new(VecDeque::from([(|| Ok(())) as ParentsAnswer])),
            rename: Mutex::new(rename.iter().copied().collect()),
            forget: || Ok(()),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn parents(self, answers: &[ParentsAnswer]) -> Self {
        *self.parents.lock().unwrap_or_else(PoisonError::into_inner) =
            answers.iter().copied().collect();
        self
    }

    fn record(&self, call: &'static str) {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(call);
    }

    fn calls(&self) -> Vec<&'static str> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

fn next<T: Copy>(queue: &Mutex<VecDeque<T>>) -> T {
    let mut queue = queue.lock().unwrap_or_else(PoisonError::into_inner);
    let answer = if queue.len() > 1 {
        queue.pop_front()
    } else {
        queue.front().copied()
    };
    answer.unwrap_or_else(|| panic!("no scripted answer"))
}

#[async_trait]
impl RenameCalls for Script {
    async fn parents(&self) -> Result<(Bytes, Bytes), NfsProtocolFailure> {
        self.record("parents");
        next(&self.parents)().map(|()| (Bytes::from_static(b"from"), Bytes::from_static(b"to")))
    }

    async fn rename(&self, _from_parent: Bytes, _to_parent: Bytes) -> Result<(), NfsError> {
        self.record("rename");
        next(&self.rename)()
    }

    async fn forget(&self) -> Result<(), NfsProtocolFailure> {
        self.record("forget");
        (self.forget)()
    }

    fn renamed(&self) {
        self.record("renamed");
    }
}

async fn failure_of(script: &Script) -> NfsProtocolFailure {
    match rename_entry(script).await {
        Err(failure) => failure,
        Ok(()) => panic!("expected the rename to fail"),
    }
}

#[tokio::test]
async fn a_rename_resolves_both_parents_and_forgets_the_renamed_names() {
    let script = Script::new(&[|| Ok(())]);
    assert!(rename_entry(&script).await.is_ok());
    assert_eq!(script.calls(), ["parents", "rename", "renamed"]);
}

/// A refused RENAME is classified from the server's answer, keeping its status — not the
/// `Protocol` / Unknown (retryable) the legacy string wrapping made of it — and is not sent
/// again.
#[tokio::test]
async fn a_refused_rename_is_classified_from_the_server_status() {
    for (answer, class, transience, status) in [
        (
            (|| Err(NfsError::Nfs3(V3::NFS3ERR_ACCES))) as Answer,
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "NFS3ERR_ACCES",
        ),
        (
            || Err(NfsError::Nfs4(V4::NFS4ERR_ACCESS)),
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "NFS4ERR_ACCESS",
        ),
        (
            || Err(NfsError::Nfs3(V3::NFS3ERR_PERM)),
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "NFS3ERR_PERM",
        ),
        (
            || Err(NfsError::Nfs4(V4::NFS4ERR_ROFS)),
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "NFS4ERR_ROFS",
        ),
        (
            || Err(NfsError::Nfs3(V3::NFS3ERR_XDEV)),
            FailureClass::Unsupported,
            Transience::Permanent,
            "NFS3ERR_XDEV",
        ),
        (
            || Err(NfsError::Nfs4(V4::NFS4ERR_NOTEMPTY)),
            FailureClass::Conflict,
            Transience::Permanent,
            "NFS4ERR_NOTEMPTY",
        ),
        (
            || Err(NfsError::Nfs3(V3::NFS3ERR_EXIST)),
            FailureClass::Conflict,
            Transience::Permanent,
            "NFS3ERR_EXIST",
        ),
        (
            || Err(NfsError::Nfs3(V3::NFS3ERR_NOTDIR)),
            FailureClass::Conflict,
            Transience::Permanent,
            "NFS3ERR_NOTDIR",
        ),
        (
            || Err(NfsError::Nfs4(V4::NFS4ERR_INVAL)),
            FailureClass::InvalidInput,
            Transience::Permanent,
            "NFS4ERR_INVAL",
        ),
        (
            || Err(NfsError::Nfs4(V4::NFS4ERR_DELAY)),
            FailureClass::Throttled,
            Transience::Transient,
            "NFS4ERR_DELAY",
        ),
    ] {
        let script = Script::new(&[answer]);
        let refused = failure_of(&script).await;
        assert_eq!((refused.class, refused.transience), (class, transience));
        assert!(
            refused
                .status
                .as_deref()
                .is_some_and(|named| named.starts_with(status)),
            "{:?}",
            refused.status
        );
        assert_eq!(script.calls(), ["parents", "rename"]);
    }
}

/// A parent the role cannot resolve is its LOOKUP's failure, as the role classified it; no
/// RENAME is sent.
#[tokio::test]
async fn a_refused_parent_lookup_is_the_failure() {
    let script = Script::new(&[|| Ok(())]).parents(&[|| {
        Err(NfsProtocolFailure::new(
            FailureClass::PermissionDenied,
            Transience::Permanent,
        ))
    }]);
    let refused = failure_of(&script).await;
    assert_eq!(refused.class, FailureClass::PermissionDenied);
    assert_eq!(script.calls(), ["parents"]);
}

/// A handle that no longer names what it did (`STALE`, or `NOENT` from a racing rename) is
/// forgotten and the rename sent again, at most `MAX_STALE_RETRIES` times; then the last answer
/// is classified (`NOENT` is `NotFound`, not `Protocol`).
#[tokio::test]
async fn stale_handles_are_forgotten_and_retried_a_bounded_number_of_times() {
    let recovered = Script::new(&[|| Err(NfsError::Nfs3(V3::NFS3ERR_STALE)), || Ok(())]);
    assert!(rename_entry(&recovered).await.is_ok());
    assert_eq!(
        recovered.calls(),
        [
            "parents", "rename", "forget", "parents", "rename", "renamed"
        ]
    );

    let gone = Script::new(&[|| Err(NfsError::Nfs4(V4::NFS4ERR_NOENT))]);
    let failure = failure_of(&gone).await;
    assert_eq!(
        (failure.class, failure.transience),
        (FailureClass::NotFound, Transience::Permanent)
    );
    let renames = gone
        .calls()
        .iter()
        .filter(|call| **call == "rename")
        .count();
    assert_eq!(renames, usize::from(MAX_STALE_RETRIES) + 1);
    assert!(!gone.calls().contains(&"renamed"));
}

/// Forgetting the cached handles fails (the root handle cannot be refreshed): that failure is the
/// rename's, and no RENAME is sent on handles known to be stale.
#[tokio::test]
async fn a_failed_forget_stops_the_rename() {
    let mut script = Script::new(&[|| Err(NfsError::Nfs3(V3::NFS3ERR_STALE))]);
    script.forget = || {
        Err(NfsProtocolFailure::new(
            FailureClass::Connectivity,
            Transience::Transient,
        ))
    };
    let failure = failure_of(&script).await;
    assert_eq!(failure.class, FailureClass::Connectivity);
    assert_eq!(script.calls(), ["parents", "rename", "forget"]);
}

/// A RENAME without an authoritative answer (the reply lost, nfs-rs wrapping it in its recovery
/// outcome) may have taken effect: it is classified by the recovery action and never sent again
/// here — the callers read back what is there.
#[tokio::test]
async fn an_uncertain_rename_is_not_sent_again() {
    let script = Script::new(&[|| {
        Err(NfsError::OperationOutcome(Box::new(
            nfs_rs::OperationOutcomeError::new(
                nfs_rs::OperationOutcome::Uncertain,
                nfs_rs::OperationClass::ReplaySensitive,
                nfs_rs::RecoveryAction::VerifyThenResume,
                nfs_rs::RequestContext {
                    operation: "RENAME".to_owned(),
                    protocol: nfs_rs::NFSVersion::NFSv3,
                    request_id: None,
                },
                NfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "reply lost",
                )),
            ),
        )))
    }]);
    let failure = failure_of(&script).await;
    assert_eq!(
        (failure.class, failure.transience),
        (FailureClass::Protocol, Transience::Unknown)
    );
    assert_eq!(script.calls(), ["parents", "rename"]);
}
