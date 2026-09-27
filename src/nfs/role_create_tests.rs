//! Tests of what the NFS role's write paths create before they write, against scripted answers:
//! the levels of a directory path (`create_levels`), an exclusive file (`create_exclusive`), and
//! the classification of a failed parent lookup (`lookup_failure`).

use std::collections::VecDeque;
use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use nfs_rs::{Nfs3ErrorCode as V3, Nfs4ErrorCode as V4, NfsError};

use super::super::{LookupFailure, MAX_STALE_RETRIES};
use super::{FileCalls, Levels, create_exclusive, create_levels, lookup_failure};
use crate::error::StorageError;
use crate::model::{FailureClass, Transience};
use crate::storage::backends::nfs::source::NfsProtocolFailure;

fn failure(class: FailureClass, transience: Transience) -> NfsProtocolFailure {
    NfsProtocolFailure::new(class, transience)
}

fn denied() -> NfsProtocolFailure {
    failure(FailureClass::PermissionDenied, Transience::Permanent)
}

fn record(calls: &Mutex<Vec<String>>, call: String) {
    calls
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(call);
}

fn recorded(calls: &Mutex<Vec<String>>) -> Vec<String> {
    calls.lock().unwrap_or_else(PoisonError::into_inner).clone()
}

/// A path of `depth` levels, the first `cached` of them in the handle cache, with one scripted
/// answer per level for its creation and for its probe.
struct Path3 {
    cached: usize,
    create: [Result<(), NfsProtocolFailure>; 3],
    probe: [Result<bool, NfsProtocolFailure>; 3],
    calls: Mutex<Vec<String>>,
}

impl Path3 {
    fn new(cached: usize) -> Self {
        Self {
            cached,
            create: [Ok(()), Ok(()), Ok(())],
            probe: [Ok(true), Ok(true), Ok(true)],
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<String> {
        recorded(&self.calls)
    }
}

#[async_trait]
impl Levels for Path3 {
    fn depth(&self) -> usize {
        3
    }

    fn cached(&self) -> usize {
        self.cached
    }

    async fn create(&self, depth: usize) -> Result<(), NfsProtocolFailure> {
        record(&self.calls, format!("create {depth}"));
        self.create[depth - 1].clone()
    }

    async fn is_directory(&self, depth: usize) -> Result<bool, NfsProtocolFailure> {
        record(&self.calls, format!("probe {depth}"));
        self.probe[depth - 1].clone()
    }
}

#[tokio::test]
async fn every_level_below_the_cached_ones_is_created_in_order() {
    let path = Path3::new(1);
    assert!(create_levels(&path).await.is_ok());
    assert_eq!(path.calls(), ["create 2", "create 3"]);
    let cached = Path3::new(3);
    assert!(create_levels(&cached).await.is_ok());
    assert!(cached.calls().is_empty(), "a cached path costs no call");
}

/// DXN-2 answers MKDIR of an existing directory in a parent the caller may not write with
/// `ACCES`: that level exists, so the levels below it go on.
#[tokio::test]
async fn a_refused_level_that_is_a_directory_passes() {
    let mut path = Path3::new(0);
    path.create[0] = Err(denied());
    path.create[1] = Err(denied());
    assert!(create_levels(&path).await.is_ok());
    assert_eq!(
        path.calls(),
        ["create 1", "probe 1", "create 2", "probe 2", "create 3"]
    );
}

#[tokio::test]
async fn a_refused_level_that_is_missing_or_not_a_directory_stays_refused() {
    for probe in [
        Ok(false),
        Err(failure(FailureClass::NotFound, Transience::Permanent)),
        Err(failure(FailureClass::Protocol, Transience::Unknown)),
        Err(denied()),
    ] {
        let mut path = Path3::new(1);
        path.create[1] = Err(denied());
        path.probe[1] = probe;
        let refused = create_levels(&path).await.err();
        assert!(
            refused.as_ref().is_some_and(|refused| {
                refused.class == FailureClass::PermissionDenied
                    && refused.transience == Transience::Permanent
            }),
            "{:?}",
            refused.map(|refused| refused.class)
        );
        assert_eq!(path.calls(), ["create 2", "probe 2"]);
    }
}

/// A probe that fails beyond the level says nothing about it: reporting the refusal instead
/// would make a cancellation (R10), a lost session or a busy server look permanent.
#[tokio::test]
async fn a_probe_that_fails_beyond_the_level_reports_itself() {
    for class in [
        FailureClass::Cancelled,
        FailureClass::Connectivity,
        FailureClass::Authentication,
        FailureClass::Throttled,
    ] {
        let mut path = Path3::new(0);
        path.create[0] = Err(denied());
        path.probe[0] = Err(failure(class, Transience::Transient));
        let reported = create_levels(&path).await.err().map(|probe| probe.class);
        assert_eq!(reported, Some(class));
        assert_eq!(path.calls(), ["create 1", "probe 1"]);
    }
}

#[tokio::test]
async fn other_failures_stop_at_their_level_without_a_probe() {
    for class in [
        FailureClass::Conflict,
        FailureClass::NotFound,
        FailureClass::Connectivity,
    ] {
        let mut path = Path3::new(0);
        path.create[1] = Err(failure(class, Transience::Unknown));
        let reported = create_levels(&path)
            .await
            .err()
            .map(|failure| failure.class);
        assert_eq!(reported, Some(class));
        assert_eq!(path.calls(), ["create 1", "create 2"]);
    }
}

/// A scripted answer; the last one of a queue repeats forever.
type Answer<T> = fn() -> Result<T, NfsError>;
type RoleAnswer<T> = fn() -> Result<T, NfsProtocolFailure>;

/// Scripted answers, consumed in order.
type Queue<T, E> = Mutex<VecDeque<fn() -> Result<T, E>>>;

fn next<T, E>(queue: &Queue<T, E>) -> Result<T, E> {
    let mut queue = queue.lock().unwrap_or_else(PoisonError::into_inner);
    let answer = if queue.len() > 1 {
        queue.pop_front()
    } else {
        queue.front().copied()
    };
    answer.unwrap_or_else(|| panic!("no scripted answer"))()
}

/// Scripted answers for one exclusive file creation.
struct File {
    create_parents: Queue<(), NfsProtocolFailure>,
    parent: Queue<(), NfsProtocolFailure>,
    create: Queue<(), NfsError>,
    calls: Mutex<Vec<String>>,
}

impl File {
    fn new(create: &[Answer<()>]) -> Self {
        Self {
            create_parents: Mutex::new(VecDeque::from([(|| Ok(())) as RoleAnswer<()>])),
            parent: Mutex::new(VecDeque::from([(|| Ok(())) as RoleAnswer<()>])),
            create: Mutex::new(create.iter().copied().collect()),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn parent(self, answers: &[RoleAnswer<()>]) -> Self {
        *self.parent.lock().unwrap_or_else(PoisonError::into_inner) =
            answers.iter().copied().collect();
        self
    }

    fn create_parents(self, answers: &[RoleAnswer<()>]) -> Self {
        *self
            .create_parents
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = answers.iter().copied().collect();
        self
    }

    fn calls(&self) -> Vec<String> {
        recorded(&self.calls)
    }
}

#[async_trait]
impl FileCalls for File {
    async fn create_parents(&self) -> Result<(), NfsProtocolFailure> {
        record(&self.calls, "parents".to_owned());
        next(&self.create_parents)
    }

    async fn parent(&self) -> Result<Bytes, NfsProtocolFailure> {
        record(&self.calls, "parent".to_owned());
        next(&self.parent).map(|()| Bytes::from_static(b"parent"))
    }

    async fn create(&self, _parent: Bytes) -> Result<Bytes, NfsError> {
        record(&self.calls, "create".to_owned());
        next(&self.create).map(|()| Bytes::from_static(b"file"))
    }

    async fn forget(&self) -> Result<(), NfsProtocolFailure> {
        record(&self.calls, "forget".to_owned());
        Ok(())
    }

    async fn back_off(&self, attempt: u8) {
        record(&self.calls, format!("back off {attempt}"));
    }
}

fn stale() -> Result<(), NfsError> {
    Err(NfsError::Nfs3(V3::NFS3ERR_STALE))
}

fn gone() -> Result<(), NfsProtocolFailure> {
    Err(failure(FailureClass::NotFound, Transience::Permanent))
}

#[tokio::test]
async fn a_new_file_creates_its_parents_first() {
    let file = File::new(&[|| Ok(())]);
    let created = create_exclusive(&file).await;
    assert!(matches!(created, Ok(Some(_))));
    assert_eq!(file.calls(), ["parents", "parent", "create"]);
}

#[tokio::test]
async fn an_existing_file_is_the_typed_collision() {
    let file = File::new(&[|| Err(NfsError::Nfs4(V4::NFS4ERR_EXIST))]);
    assert!(matches!(create_exclusive(&file).await, Ok(None)));
}

/// A missing parent below a level the caller may not write: the refused MKDIR is the file's
/// failure, `PermissionDenied` / Permanent, and no CREATE is sent.
#[tokio::test]
async fn refused_parents_are_the_refusal() {
    let file = File::new(&[|| Ok(())]).create_parents(&[|| Err(denied())]);
    let refused = create_exclusive(&file).await.err();
    assert!(
        refused.is_some_and(|refused| refused.class == FailureClass::PermissionDenied
            && refused.transience == Transience::Permanent)
    );
    assert_eq!(file.calls(), ["parents"]);
}

/// A refused CREATE is classified from the server's answer, keeping its status — not the
/// `Protocol` / Unknown the legacy string wrapping made of it.
#[tokio::test]
async fn a_refused_create_is_a_permanent_permission_failure_naming_the_status() {
    for answer in [
        (|| Err(NfsError::Nfs3(V3::NFS3ERR_ACCES))) as Answer<()>,
        || Err(NfsError::Nfs4(V4::NFS4ERR_ACCESS)),
        || Err(NfsError::Nfs3(V3::NFS3ERR_ROFS)),
    ] {
        let file = File::new(&[answer]);
        let refused = create_exclusive(&file).await.err();
        let Some(refused) = refused else {
            panic!("expected a refusal");
        };
        assert_eq!(refused.class, FailureClass::PermissionDenied);
        assert_eq!(refused.transience, Transience::Permanent);
        assert!(
            refused
                .status
                .as_deref()
                .is_some_and(|status| status.contains("ERR_"))
        );
        assert_eq!(file.calls(), ["parents", "parent", "create"]);
    }
}

/// The parent's cached handle went stale and the parent is gone (deleted after this process
/// cached it): it is rebuilt once and the file created within the same call.
#[tokio::test]
async fn a_parent_deleted_after_it_was_cached_is_rebuilt_within_the_call() {
    let file = File::new(&[stale, || Ok(())]).parent(&[|| Ok(()), gone, || Ok(())]);
    assert!(matches!(create_exclusive(&file).await, Ok(Some(_))));
    assert_eq!(
        file.calls(),
        [
            "parents", "parent", "create", "forget", "parent", "parents", "parent", "create"
        ]
    );
}

#[tokio::test]
async fn a_parent_gone_again_after_its_rebuild_fails_without_a_second_rebuild() {
    let file = File::new(&[stale]).parent(&[|| Ok(()), gone]);
    let reported = create_exclusive(&file)
        .await
        .err()
        .map(|gone| (gone.class, gone.transience));
    // Not permanent: the cache is clean and a new attempt recreates the parent.
    assert_eq!(
        reported,
        Some((FailureClass::NotFound, Transience::Transient))
    );
    assert_eq!(
        file.calls(),
        [
            "parents", "parent", "create", "forget", "parent", "parents", "parent"
        ]
    );
}

/// A parent missing without a stale handle first is not rebuilt: `create_parents` just made it.
#[tokio::test]
async fn a_parent_missing_before_any_stale_handle_is_not_rebuilt() {
    let file = File::new(&[|| Ok(())]).parent(&[gone]);
    let reported = create_exclusive(&file)
        .await
        .err()
        .map(|gone| (gone.class, gone.transience));
    assert_eq!(
        reported,
        Some((FailureClass::NotFound, Transience::Permanent))
    );
    assert_eq!(file.calls(), ["parents", "parent"]);
}

#[tokio::test]
async fn stale_and_busy_retries_are_bounded_and_report_the_last_cause() {
    let file = File::new(&[stale]);
    let reported = create_exclusive(&file).await.err();
    assert!(reported.is_some_and(|stale| {
        stale
            .status
            .as_deref()
            .is_some_and(|status| status.contains("NFS3ERR_STALE"))
    }));
    let creates = file.calls().iter().filter(|call| *call == "create").count();
    assert_eq!(creates, usize::from(MAX_STALE_RETRIES) + 1);

    let busy = File::new(&[|| Err(NfsError::Nfs4(V4::NFS4ERR_DELAY)), || Ok(())]);
    assert!(matches!(create_exclusive(&busy).await, Ok(Some(_))));
    assert_eq!(
        busy.calls(),
        [
            "parents",
            "parent",
            "create",
            "back off 0",
            "parent",
            "create"
        ]
    );
}

/// The parent lookup before MKDIR / CREATE is classified from the LOOKUP's own error.
#[test]
fn a_failed_parent_lookup_keeps_the_role_classification() {
    let component = |error| LookupFailure::Component {
        dirname: "d".to_owned(),
        error,
    };
    for (error, class, transience) in [
        (
            NfsError::Nfs3(V3::NFS3ERR_ACCES),
            FailureClass::PermissionDenied,
            Transience::Permanent,
        ),
        (
            NfsError::Nfs4(V4::NFS4ERR_PERM),
            FailureClass::PermissionDenied,
            Transience::Permanent,
        ),
        (
            NfsError::Nfs3(V3::NFS3ERR_ROFS),
            FailureClass::PermissionDenied,
            Transience::Permanent,
        ),
        (
            NfsError::Nfs3(V3::NFS3ERR_NOENT),
            FailureClass::NotFound,
            Transience::Permanent,
        ),
        (
            NfsError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "reset",
            )),
            FailureClass::Connectivity,
            Transience::Transient,
        ),
    ] {
        let classified = lookup_failure(component(error));
        assert_eq!(
            (classified.class, classified.transience),
            (class, transience)
        );
    }
    let bad_path = lookup_failure(LookupFailure::Storage(StorageError::InvalidPath(
        "..".to_owned(),
    )));
    assert_eq!(bad_path.class, FailureClass::InvalidInput);
}
