//! Tests of the NFS role's one-level directory creation against scripted server answers. How
//! `create_directory_all` drives it through the namespace adapter is tested in
//! `storage/backends/nfs/create_dir_tests.rs`.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use nfs_rs::{Nfs3ErrorCode as V3, Nfs4ErrorCode as V4, NfsError};

use super::super::MAX_STALE_RETRIES;
use super::{LevelCalls, create_level};
use crate::model::{FailureClass, Transience};
use crate::storage::backends::nfs::source::NfsProtocolFailure;

/// A scripted answer; the last one of a queue repeats forever.
type Answer<T> = fn() -> Result<T, NfsError>;

/// Scripted answers for one level, recording every call in order.
struct Script {
    parent: Option<fn() -> NfsProtocolFailure>,
    mkdir: Mutex<VecDeque<Answer<()>>>,
    /// Whether the name is a directory.
    lookup: Mutex<VecDeque<Answer<bool>>>,
    remove: Mutex<VecDeque<Answer<()>>>,
    calls: Mutex<Vec<&'static str>>,
}

impl Script {
    fn new(mkdir: &[Answer<()>]) -> Self {
        Self {
            parent: None,
            mkdir: Mutex::new(mkdir.iter().copied().collect()),
            lookup: Mutex::new(VecDeque::new()),
            remove: Mutex::new(VecDeque::new()),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn lookup(mut self, answers: &[Answer<bool>]) -> Self {
        self.lookup = Mutex::new(answers.iter().copied().collect());
        self
    }

    fn remove(mut self, answers: &[Answer<()>]) -> Self {
        self.remove = Mutex::new(answers.iter().copied().collect());
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

    fn count(&self, call: &str) -> usize {
        self.calls().iter().filter(|name| **name == call).count()
    }
}

fn next<T>(queue: &Mutex<VecDeque<Answer<T>>>) -> Result<T, NfsError> {
    let mut queue = queue.lock().unwrap_or_else(PoisonError::into_inner);
    let answer = if queue.len() > 1 {
        queue.pop_front()
    } else {
        queue.front().copied()
    };
    answer.unwrap_or_else(|| panic!("no scripted answer"))()
}

#[async_trait]
impl LevelCalls for Script {
    fn path(&self) -> &Path {
        Path::new("a/b")
    }

    async fn parent(&self) -> Result<Bytes, NfsProtocolFailure> {
        self.record("parent");
        self.parent
            .map_or(Ok(Bytes::from_static(b"parent")), |failure| Err(failure()))
    }

    async fn mkdir(&self, _parent: Bytes) -> Result<Bytes, NfsError> {
        self.record("mkdir");
        next(&self.mkdir).map(|()| Bytes::from_static(b"created"))
    }

    async fn lookup(&self, _parent: Bytes) -> Result<(Bytes, bool), NfsError> {
        self.record("lookup");
        next(&self.lookup).map(|directory| (Bytes::from_static(b"found"), directory))
    }

    async fn remove(&self, _parent: Bytes) -> Result<(), NfsError> {
        self.record("remove");
        next(&self.remove)
    }

    async fn forget(&self) -> Result<(), NfsProtocolFailure> {
        self.record("forget");
        Ok(())
    }

    fn remember(&self, _handle: &Bytes) {
        self.record("remember");
    }
}

fn exist() -> Result<(), NfsError> {
    Err(NfsError::Nfs3(V3::NFS3ERR_EXIST))
}

fn stale() -> Result<(), NfsError> {
    Err(NfsError::Nfs3(V3::NFS3ERR_STALE))
}

async fn failure_of(script: &Script) -> NfsProtocolFailure {
    match create_level(script).await {
        Ok(()) => panic!("expected a failure, calls {:?}", script.calls()),
        Err(failure) => failure,
    }
}

#[tokio::test]
async fn a_new_level_is_one_mkdir_and_is_cached() {
    let script = Script::new(&[|| Ok(())]);
    assert!(create_level(&script).await.is_ok());
    assert_eq!(script.calls(), ["parent", "mkdir", "remember"]);
}

#[tokio::test]
async fn an_existing_directory_is_success_in_either_dialect() {
    for answer in [exist, || Err(NfsError::Nfs4(V4::NFS4ERR_EXIST))] {
        let script = Script::new(&[answer]).lookup(&[|| Ok(true)]);
        assert!(create_level(&script).await.is_ok());
        assert_eq!(script.calls(), ["parent", "mkdir", "lookup", "remember"]);
    }
}

/// The legacy helper's rsync-style replacement, kept: a non-directory in the way is removed and
/// the directory created in its place.
#[tokio::test]
async fn a_non_directory_in_the_way_is_replaced_by_the_directory() {
    // A removal the server confirms, and one a concurrent task beat us to.
    for removed in [|| Ok(()), || Err(NfsError::Nfs3(V3::NFS3ERR_NOENT))] {
        let script = Script::new(&[exist, || Ok(())])
            .lookup(&[|| Ok(false)])
            .remove(&[removed]);
        assert!(create_level(&script).await.is_ok());
        assert_eq!(
            script.calls(),
            [
                "parent", "mkdir", "lookup", "remove", "parent", "mkdir", "remember"
            ]
        );
    }
}

#[tokio::test]
async fn a_refused_removal_is_reported_as_the_refusal() {
    let script = Script::new(&[exist])
        .lookup(&[|| Ok(false)])
        .remove(&[|| Err(NfsError::Nfs3(V3::NFS3ERR_ACCES))]);
    let failure = failure_of(&script).await;
    assert_eq!(failure.class, FailureClass::PermissionDenied);
    assert_eq!(failure.transience, Transience::Permanent);
    assert_eq!(script.calls(), ["parent", "mkdir", "lookup", "remove"]);
}

/// A non-directory that keeps coming back ends as a conflict once the retries run out.
#[tokio::test]
async fn a_non_directory_that_keeps_returning_is_a_conflict() {
    let script = Script::new(&[exist])
        .lookup(&[|| Ok(false)])
        .remove(&[|| Ok(())]);
    let failure = failure_of(&script).await;
    assert_eq!(failure.class, FailureClass::Conflict);
    assert_eq!(script.count("mkdir"), usize::from(MAX_STALE_RETRIES) + 1);
    // Never removed without a MKDIR left to put the directory in its place.
    assert_eq!(script.count("remove"), usize::from(MAX_STALE_RETRIES));
    assert_eq!(script.calls().last(), Some(&"lookup"));
}

/// A refused MKDIR is an entry `PermissionDenied` / Permanent naming the status — not the
/// `Protocol` / Unknown (retryable) the legacy string wrapping made of it — and costs nothing
/// more: whether the level already exists is `create_directory_all`'s question.
#[tokio::test]
async fn a_refused_mkdir_is_a_permanent_permission_failure_naming_the_status() {
    let refusals: [(Answer<()>, &str); 6] = [
        (|| Err(NfsError::Nfs3(V3::NFS3ERR_ACCES)), "NFS3ERR_ACCES"),
        (|| Err(NfsError::Nfs3(V3::NFS3ERR_PERM)), "NFS3ERR_PERM"),
        (|| Err(NfsError::Nfs3(V3::NFS3ERR_ROFS)), "NFS3ERR_ROFS"),
        (|| Err(NfsError::Nfs4(V4::NFS4ERR_ACCESS)), "NFS4ERR_ACCESS"),
        (|| Err(NfsError::Nfs4(V4::NFS4ERR_PERM)), "NFS4ERR_PERM"),
        (|| Err(NfsError::Nfs4(V4::NFS4ERR_ROFS)), "NFS4ERR_ROFS"),
    ];
    for (answer, status) in refusals {
        let script = Script::new(&[answer]);
        let failure = failure_of(&script).await;
        assert_eq!(failure.class, FailureClass::PermissionDenied, "{status}");
        assert_eq!(failure.transience, Transience::Permanent, "{status}");
        assert!(
            failure
                .status
                .as_deref()
                .is_some_and(|s| s.contains(status)),
            "{status}: {:?}",
            failure.status
        );
        assert_eq!(script.calls(), ["parent", "mkdir"], "{status}");
    }
}

#[tokio::test]
async fn other_answers_keep_their_own_class() {
    let answers: [(Answer<()>, FailureClass); 3] = [
        (
            || Err(NfsError::Nfs3(V3::NFS3ERR_NOSPC)),
            FailureClass::Capacity,
        ),
        (
            || Err(NfsError::Nfs3(V3::NFS3ERR_JUKEBOX)),
            FailureClass::Throttled,
        ),
        (
            || {
                Err(NfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "reset",
                )))
            },
            FailureClass::Connectivity,
        ),
    ];
    for (answer, class) in answers {
        let script = Script::new(&[answer]);
        assert_eq!(failure_of(&script).await.class, class);
        assert_eq!(script.calls(), ["parent", "mkdir"]);
    }
}

#[tokio::test]
async fn a_stale_parent_handle_is_forgotten_and_the_level_retried() {
    let stale_answers: [Answer<()>; 4] = [
        stale,
        || Err(NfsError::Nfs3(V3::NFS3ERR_BADHANDLE)),
        || Err(NfsError::Nfs4(V4::NFS4ERR_STALE)),
        || Err(NfsError::Nfs4(V4::NFS4ERR_BADHANDLE)),
    ];
    for answer in stale_answers {
        let script = Script::new(&[answer, || Ok(())]);
        assert!(create_level(&script).await.is_ok());
        assert_eq!(
            script.calls(),
            ["parent", "mkdir", "forget", "parent", "mkdir", "remember"]
        );
    }
}

/// A parent deleted after this process cached it: gone once its stale handle is forgotten, so
/// not permanent — the next attempt, with a clean cache, recreates it.
#[tokio::test]
async fn a_parent_gone_after_its_stale_handle_was_forgotten_is_transient() {
    struct Deleted(Script);
    #[async_trait]
    impl LevelCalls for Deleted {
        fn path(&self) -> &Path {
            self.0.path()
        }
        async fn parent(&self) -> Result<Bytes, NfsProtocolFailure> {
            let forgotten = self.0.count("forget") > 0;
            self.0.parent().await?;
            if forgotten {
                return Err(NfsProtocolFailure::new(
                    FailureClass::NotFound,
                    Transience::Permanent,
                ));
            }
            Ok(Bytes::from_static(b"parent"))
        }
        async fn mkdir(&self, parent: Bytes) -> Result<Bytes, NfsError> {
            self.0.mkdir(parent).await
        }
        async fn lookup(&self, parent: Bytes) -> Result<(Bytes, bool), NfsError> {
            LevelCalls::lookup(&self.0, parent).await
        }
        async fn remove(&self, parent: Bytes) -> Result<(), NfsError> {
            LevelCalls::remove(&self.0, parent).await
        }
        async fn forget(&self) -> Result<(), NfsProtocolFailure> {
            self.0.forget().await
        }
        fn remember(&self, handle: &Bytes) {
            self.0.remember(handle);
        }
    }

    let script = Deleted(Script::new(&[stale]));
    let failure = match create_level(&script).await {
        Ok(()) => panic!("expected a failure"),
        Err(failure) => failure,
    };
    assert_eq!(failure.class, FailureClass::NotFound);
    assert_eq!(failure.transience, Transience::Transient);
    assert_eq!(script.0.calls(), ["parent", "mkdir", "forget", "parent"]);
}

#[tokio::test]
async fn a_level_gone_between_mkdir_and_lookup_is_retried() {
    let script =
        Script::new(&[exist, || Ok(())]).lookup(&[|| Err(NfsError::Nfs3(V3::NFS3ERR_NOENT))]);
    assert!(create_level(&script).await.is_ok());
    assert_eq!(
        script.calls(),
        [
            "parent", "mkdir", "lookup", "forget", "parent", "mkdir", "remember"
        ]
    );
}

#[tokio::test]
async fn retries_are_bounded_and_report_the_last_cause() {
    let script = Script::new(&[stale]);
    let failure = failure_of(&script).await;
    assert_eq!(failure.class, FailureClass::Protocol);
    assert!(
        failure
            .status
            .as_deref()
            .is_some_and(|s| s.contains("NFS3ERR_STALE"))
    );
    assert_eq!(script.count("mkdir"), usize::from(MAX_STALE_RETRIES) + 1);
    assert_eq!(script.count("forget"), usize::from(MAX_STALE_RETRIES));
}

#[tokio::test]
async fn a_parent_that_cannot_be_resolved_stops_before_mkdir() {
    let mut script = Script::new(&[|| Ok(())]);
    script.parent = Some(|| NfsProtocolFailure::new(FailureClass::NotFound, Transience::Permanent));
    let failure = failure_of(&script).await;
    assert_eq!(failure.class, FailureClass::NotFound);
    // Nothing was forgotten: a parent missing from the start is permanent.
    assert_eq!(failure.transience, Transience::Permanent);
    assert_eq!(script.calls(), ["parent"]);
}
