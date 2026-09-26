//! Tests of the NFS role listing's READDIRPLUS reader and entry conversion.

use std::cell::Cell;
use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::{Mutex, PoisonError};

use futures::stream;
use nfs_rs::{Nfs3ErrorCode as V3, Nfs4ErrorCode as V4};

use super::super::{CACHE_WRITES, FType3, GLOBAL_CACHE, Time, role_metadata};
use super::*;
use crate::model::{FailureClass, Transience};

fn listed(name: &str, attr: Option<nfs_rs::Attr>, handle: &'static [u8]) -> RoleListedEntry {
    NFSStorage::role_listed_entry(
        PathBuf::from("dir").join(name),
        nfs_rs::ReaddirplusEntry {
            fileid: 1,
            file_name: name.to_owned(),
            attr,
            handle: Bytes::from_static(handle),
        },
        false,
    )
}

/// A child READDIRPLUS listed carries the ownership and timestamps the metadata role reads
/// from a GETATTR of the same attributes — through `NASEntry::from_nfs_attrs` (set-id bits
/// stripped for an owner it cannot map, owner strings mapped by `owner_id`) and
/// `role_observation` — for files, directories and symlinks with set-id bits, and for the
/// owner forms `NFSv3`, ONTAP and an LDAP-backed server send.
#[test]
fn a_listed_childs_ownership_and_times_are_what_a_getattr_reports() {
    let directory = StoragePath::new("dir").unwrap_or_else(|error| panic!("{error}"));
    let owners = [
        (false, "", "", 1000),
        (true, "1000", "1000", 1000),
        (true, "root@localdomain", "0", 0),
        (true, "alice@example.com", "1000", 65534),
        (true, "", "", 0),
    ];
    let mut checked = 0;
    for (type_, mode) in [
        (FType3::NF3REG, 0o106_755),
        (FType3::NF3DIR, 0o042_775),
        (FType3::NF3LNK, 0o120_777),
    ] {
        for (owners_are_strings, owner, group, parsed) in owners {
            let attrs = nfs_rs::Attr {
                type_: type_ as u32,
                file_mode: mode,
                uid: parsed,
                gid: 1000,
                atime: Time {
                    seconds: 1_700_000_000,
                    nseconds: 5,
                },
                mtime: Time {
                    seconds: 1_700_000_100,
                    nseconds: 6,
                },
                ctime: Time {
                    seconds: 1_700_000_200,
                    nseconds: 7,
                },
                owner: owner.to_owned(),
                owner_group: group.to_owned(),
                ..nfs_rs::Attr::default()
            };
            let child = NFSStorage::role_listed_entry(
                PathBuf::from("dir/x"),
                nfs_rs::ReaddirplusEntry {
                    fileid: 1,
                    file_name: "x".to_owned(),
                    attr: Some(attrs.clone()),
                    handle: Bytes::from_static(b"h"),
                },
                owners_are_strings,
            );
            let listed = role_child(&directory, &child)
                .unwrap_or_else(|_| panic!("a child with attributes is described"));
            let getattr = role_metadata::metadata_inline(
                Bytes::from_static(b"h"),
                &attrs,
                owners_are_strings,
            );
            let context = format!("{type_:?} {owner:?}/{group:?}");
            assert_eq!(
                listed.ownership,
                Some(inline_ownership(getattr.uid, getattr.gid, getattr.mode)),
                "{context}"
            );
            assert_eq!(
                listed.timestamps,
                Some(TimestampMetadata {
                    accessed: nfs_timestamp(getattr.atime),
                    modified: nfs_timestamp(getattr.mtime),
                    created: nfs_timestamp(getattr.ctime),
                }),
                "{context}"
            );
            assert_eq!(
                listed.kind == EntryKind::Symlink,
                getattr.symlink,
                "{context}"
            );
            checked += 1;
        }
    }
    assert_eq!(checked, 15);
}

fn regular() -> nfs_rs::Attr {
    nfs_rs::Attr {
        type_: FType3::NF3REG as u32,
        filesize: 3,
        ..nfs_rs::Attr::default()
    }
}

/// A READDIRPLUS entry without attributes, or without a file handle, cannot be described:
/// it becomes an entry failure at its own path (`Protocol`, retrying may or may not help),
/// while the entries beside it are still observed.
#[test]
fn an_entry_without_attributes_or_handle_fails_alone() {
    let directory = StoragePath::new("dir").unwrap_or_else(|error| panic!("{error}"));
    let children: Vec<NfsListedChild> = [
        listed("before", Some(regular()), b"h1"),
        listed("no-attrs", None, b"h2"),
        listed("no-handle", Some(regular()), b""),
        listed("after", Some(regular()), b"h3"),
    ]
    .iter()
    .map(|child| role_child(&directory, child))
    .collect();
    let observed: Vec<&str> = children
        .iter()
        .filter_map(|child| child.as_ref().ok())
        .map(|entry| entry.path.as_str())
        .collect();
    assert_eq!(observed, ["dir/before", "dir/after"]);
    let failed: Vec<(&str, &str)> = children
        .iter()
        .filter_map(|child| child.as_ref().err())
        .map(|child| (child.path.as_str(), child.reason))
        .collect();
    assert_eq!(
        failed,
        [
            ("dir/no-attrs", "the server omitted its attributes"),
            ("dir/no-handle", "the server returned no file handle"),
        ]
    );
    for child in children.iter().filter_map(|child| child.as_ref().err()) {
        assert_eq!(child.failure.class, FailureClass::Protocol);
        assert_eq!(child.failure.transience, Transience::Unknown);
    }
}

fn attrs(type_: FType3) -> nfs_rs::Attr {
    nfs_rs::Attr {
        type_: type_ as u32,
        ..nfs_rs::Attr::default()
    }
}

fn entry(name: &str, type_: FType3, handle: &'static [u8]) -> nfs_rs::ReaddirplusEntry {
    nfs_rs::ReaddirplusEntry {
        fileid: 1,
        file_name: name.to_owned(),
        attr: Some(attrs(type_)),
        handle: Bytes::from_static(handle),
    }
}

fn directory() -> StoragePath {
    StoragePath::new("dir").unwrap_or_else(|error| panic!("{error}"))
}

/// Pages of two children; `stop_after` makes the listing stop once that many were pushed.
#[derive(Default)]
struct Collected {
    children: Vec<NfsListedChild>,
    held: usize,
    hand_overs: usize,
    hidden: usize,
    stop_after: Option<usize>,
}

#[async_trait::async_trait]
impl NfsListingPages for Collected {
    fn push(&mut self, child: NfsListedChild) -> bool {
        self.children.push(child);
        self.held += 1;
        self.held >= 2
    }

    fn hide(&mut self) {
        self.hidden += 1;
    }

    async fn hand_over(&mut self) -> bool {
        self.held = 0;
        self.hand_overs += 1;
        true
    }

    fn stopping(&self) -> bool {
        self.stop_after
            .is_some_and(|limit| self.children.len() >= limit)
    }
}

/// Reading a directory puts no handle into the process-wide cache — not the listed
/// subdirectories' (the descent carries those as hints), not anything else. The counter is
/// per thread and this test's reader runs on its own thread.
#[tokio::test(flavor = "current_thread")]
async fn reading_a_directory_caches_no_handle() {
    CACHE_WRITES.with(|writes| writes.set(0));
    let entries = stream::iter(vec![
        Ok(entry("sub", FType3::NF3DIR, b"fh-sub")),
        Ok(entry("file", FType3::NF3REG, b"fh-file")),
        Ok(entry("other", FType3::NF3DIR, b"fh-other")),
    ]);
    let mut pages = Collected::default();
    let end = drain_readdirplus(
        &directory(),
        entries,
        &mut pages,
        NFSVersion::NFSv3,
        &NoLookup,
    )
    .await;
    assert!(matches!(end, Ok(NfsReadEnd::Complete)));
    assert_eq!(pages.children.len(), 3);
    assert_eq!(pages.hand_overs, 1);
    assert_eq!(
        CACHE_WRITES.with(Cell::get),
        0,
        "the listing cached a handle"
    );
    // The counter does see a write through the cache: the zero above is not vacuous.
    let key = (
        PathBuf::from("counted"),
        Bytes::from(uuid::Uuid::new_v4().as_bytes().to_vec()),
    );
    NFSStorage::cache_directory(key.clone(), &Bytes::from_static(b"fh"));
    assert_eq!(CACHE_WRITES.with(Cell::get), 1);
    GLOBAL_CACHE.invalidate(&key);
}

/// A listing told to stop takes no further entry, so nfs-rs sends no further request.
#[tokio::test]
async fn a_stopped_listing_takes_no_further_entry() {
    let taken = AtomicUsize::new(0);
    let entries =
        stream::iter((0..10).map(|_| Ok(entry("f", FType3::NF3REG, b"fh")))).inspect(|_| {
            taken.fetch_add(1, Ordering::SeqCst);
        });
    let mut pages = Collected {
        stop_after: Some(3),
        ..Collected::default()
    };
    let end = drain_readdirplus(
        &directory(),
        Box::pin(entries),
        &mut pages,
        NFSVersion::NFSv3,
        &NoLookup,
    )
    .await;
    assert!(matches!(end, Ok(NfsReadEnd::Stopped)));
    assert_eq!(taken.load(Ordering::SeqCst), 3);
}

/// A failure mid-directory ends the read with what reading anew could do about it, after the
/// entries before it were handed on.
#[tokio::test]
async fn a_failure_mid_directory_ends_the_read_after_the_entries_before_it() {
    let entries = stream::iter(vec![
        Ok(entry("a", FType3::NF3REG, b"fh-a")),
        Ok(entry("b", FType3::NF3REG, b"fh-b")),
        Ok(entry("c", FType3::NF3REG, b"fh-c")),
        Err(NfsError::Nfs3(nfs_rs::Nfs3ErrorCode::NFS3ERR_STALE)),
        Ok(entry("d", FType3::NF3REG, b"fh-d")),
    ]);
    let mut pages = Collected::default();
    let failure = drain_readdirplus(
        &directory(),
        entries,
        &mut pages,
        NFSVersion::NFSv3,
        &NoLookup,
    )
    .await;
    assert!(matches!(
        failure,
        Err(NfsReadFailure {
            restart: NfsRestart::Relookup,
            ..
        })
    ));
    assert_eq!(pages.children.len(), 3);
}

/// A [`ChildLookup`] no test expects to be asked.
struct NoLookup;

#[async_trait::async_trait]
impl ChildLookup for NoLookup {
    async fn lookup(&self, name: &str) -> nfs_rs::Result<nfs_rs::ObjRes> {
        panic!("unexpected LOOKUP of {name}")
    }

    async fn getattr(&self, _: Bytes) -> nfs_rs::Result<nfs_rs::Attr> {
        panic!("unexpected GETATTR")
    }
}

type Found = fn() -> nfs_rs::Result<nfs_rs::ObjRes>;
type Attributes = fn() -> nfs_rs::Result<nfs_rs::Attr>;

/// A [`ChildLookup`] that answers a LOOKUP with `found` and a GETATTR with `attributes`,
/// recording what it was asked.
struct Answering {
    found: Found,
    attributes: Attributes,
    asked: Mutex<Vec<String>>,
}

impl Answering {
    fn new(found: Found, attributes: Attributes) -> Self {
        Self {
            found,
            attributes,
            asked: Mutex::default(),
        }
    }

    fn asked(&self) -> Vec<String> {
        self.asked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[async_trait::async_trait]
impl ChildLookup for Answering {
    async fn lookup(&self, name: &str) -> nfs_rs::Result<nfs_rs::ObjRes> {
        self.asked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(format!("LOOKUP {name}"));
        (self.found)()
    }

    async fn getattr(&self, handle: Bytes) -> nfs_rs::Result<nfs_rs::Attr> {
        let handle = String::from_utf8_lossy(&handle).into_owned();
        self.asked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(format!("GETATTR {handle}"));
        (self.attributes)()
    }
}

/// What `drain_readdirplus` hands over for `entries`, read over `version`: the children, and how
/// many entries it hid.
async fn drained(
    version: NFSVersion,
    entries: Vec<nfs_rs::ReaddirplusEntry>,
    lookup: &dyn ChildLookup,
) -> (Vec<NfsListedChild>, usize) {
    let mut pages = Collected::default();
    let entries = stream::iter(entries.into_iter().map(Ok));
    let end = drain_readdirplus(&directory(), entries, &mut pages, version, lookup).await;
    assert!(matches!(end, Ok(NfsReadEnd::Complete)));
    (pages.children, pages.hidden)
}

/// The paths `children` describe, and those of the ones they could not describe.
fn outcome(children: &[NfsListedChild]) -> (Vec<&str>, Vec<&str>) {
    let observed = children.iter().filter_map(|child| child.as_ref().ok());
    let failed = children.iter().filter_map(|child| child.as_ref().err());
    (
        observed.map(|entry| entry.path.as_str()).collect(),
        failed.map(|child| child.path.as_str()).collect(),
    )
}

/// ONTAP's `NFSv3` READDIRPLUS entry for a volume's snapshot directory: a handle, no attributes.
fn ontap_snapshot() -> nfs_rs::ReaddirplusEntry {
    nfs_rs::ReaddirplusEntry {
        attr: None,
        ..entry(".snapshot", FType3::NF3DIR, b"fh-snap")
    }
}

#[allow(clippy::unnecessary_wraps)]
fn found(type_: FType3, attr: bool) -> nfs_rs::Result<nfs_rs::ObjRes> {
    Ok(nfs_rs::ObjRes {
        fh: Bytes::from_static(b"fh-looked-up"),
        attr: attr.then(|| attrs(type_)),
    })
}

fn no_getattr() -> nfs_rs::Result<nfs_rs::Attr> {
    panic!("unexpected GETATTR")
}

fn siblings_around(snapshot: nfs_rs::ReaddirplusEntry) -> Vec<nfs_rs::ReaddirplusEntry> {
    vec![
        entry("a", FType3::NF3REG, b"fh-a"),
        snapshot,
        entry("b", FType3::NF3DIR, b"fh-b"),
    ]
}

/// An `NFSv3` `.snapshot` listed without attributes that one LOOKUP shows to be a directory —
/// ONTAP's snapshot directory — is hidden (P7 C7b): not a child, not a failure, counted as
/// hidden; its siblings are handed over. A LOOKUP reply without attributes is completed with a
/// GETATTR of the handle it returned.
#[tokio::test]
async fn an_nfsv3_snapshot_directory_without_attributes_is_hidden() {
    let with_attributes = Answering::new(|| found(FType3::NF3DIR, true), no_getattr);
    let without = Answering::new(
        || found(FType3::NF3DIR, false),
        || Ok(attrs(FType3::NF3DIR)),
    );
    for (lookup, asked) in [
        (&with_attributes, vec!["LOOKUP .snapshot"]),
        (&without, vec!["LOOKUP .snapshot", "GETATTR fh-looked-up"]),
    ] {
        let (children, hidden) =
            drained(NFSVersion::NFSv3, siblings_around(ontap_snapshot()), lookup).await;
        assert_eq!(outcome(&children), (vec!["dir/a", "dir/b"], vec![]));
        assert_eq!(hidden, 1);
        assert_eq!(lookup.asked(), asked);
    }
}

/// When looking it up fails for the child's sake, the `.snapshot` is a failure of that child with
/// the lookup's own class and status — `NOENT` makes it `NotFound` (it vanished), a refused
/// LOOKUP `PermissionDenied`, a stale handle the LOOKUP returned (the GETATTR's) `Protocol` —
/// never silently dropped; its siblings are handed over.
#[tokio::test]
async fn a_snapshot_that_cannot_be_looked_up_fails_with_the_lookups_class() {
    let cases = [
        (
            Answering::new(|| Err(NfsError::Nfs3(V3::NFS3ERR_NOENT)), no_getattr),
            FailureClass::NotFound,
        ),
        (
            Answering::new(|| Err(NfsError::Nfs3(V3::NFS3ERR_ACCES)), no_getattr),
            FailureClass::PermissionDenied,
        ),
        (
            Answering::new(
                || found(FType3::NF3DIR, false),
                || Err(NfsError::Nfs3(V3::NFS3ERR_STALE)),
            ),
            FailureClass::Protocol,
        ),
    ];
    for (lookup, class) in cases {
        let (children, hidden) = drained(
            NFSVersion::NFSv3,
            siblings_around(ontap_snapshot()),
            &lookup,
        )
        .await;
        assert_eq!(
            outcome(&children),
            (vec!["dir/a", "dir/b"], vec!["dir/.snapshot"])
        );
        assert_eq!(hidden, 0);
        let failure = children[1].as_ref().err().map(|child| &child.failure);
        assert_eq!(failure.map(|failure| failure.class), Some(class));
        assert!(failure.is_some_and(|failure| failure.status.is_some()));
    }
}

/// A failed look that says more about the read than about the child ends the read the way a
/// READDIRPLUS failure would, after the siblings before it: a LOOKUP whose handle — the
/// directory's — went stale is resolved again, a transport failure nfs-rs certifies safe to
/// resend is resent, a busy server (here answering the GETATTR) is backed off.
#[tokio::test]
async fn a_lookup_failure_of_the_session_or_the_directory_ends_the_read() {
    let cases = [
        (
            Answering::new(|| Err(NfsError::Nfs3(V3::NFS3ERR_STALE)), no_getattr),
            NfsRestart::Relookup,
            FailureClass::Protocol,
        ),
        (
            Answering::new(
                || Err(wrapped(nfs_rs::RecoveryAction::Retry, timeout())),
                no_getattr,
            ),
            NfsRestart::Resend,
            FailureClass::Connectivity,
        ),
        (
            Answering::new(
                || found(FType3::NF3DIR, false),
                || Err(NfsError::Nfs3(V3::NFS3ERR_JUKEBOX)),
            ),
            NfsRestart::Backoff,
            FailureClass::Throttled,
        ),
    ];
    for (lookup, restart, class) in cases {
        let mut pages = Collected::default();
        let entries = stream::iter(siblings_around(ontap_snapshot()).into_iter().map(Ok));
        let end = drain_readdirplus(
            &directory(),
            entries,
            &mut pages,
            NFSVersion::NFSv3,
            &lookup,
        )
        .await;
        assert!(
            matches!(&end, Err(failure) if failure.restart == restart && failure.failure.class == class),
            "{restart:?} {class:?}"
        );
        assert_eq!(outcome(&pages.children), (vec!["dir/a"], vec![]));
        assert_eq!(pages.hidden, 0);
    }
}

/// A `.snapshot` the LOOKUP shows is not a directory is an ordinary child, described by what
/// the lookup returned (its attributes and its handle).
#[tokio::test]
async fn a_looked_up_snapshot_that_is_not_a_directory_is_an_ordinary_child() {
    let lookup = Answering::new(|| found(FType3::NF3REG, true), no_getattr);
    let (children, hidden) = drained(
        NFSVersion::NFSv3,
        siblings_around(ontap_snapshot()),
        &lookup,
    )
    .await;
    assert_eq!(
        outcome(&children),
        (vec!["dir/a", "dir/.snapshot", "dir/b"], vec![])
    );
    assert_eq!(hidden, 0);
    let listed = children[1].as_ref().ok();
    assert_eq!(listed.map(|child| child.kind), Some(EntryKind::File));
    assert_eq!(
        listed.map(|child| child.file_handle.as_ref()),
        Some(b"fh-looked-up".as_slice())
    );
}

/// Nothing else is looked up or hidden: a `.snapshot` READDIRPLUS describes (a real directory,
/// as DXN lists one) is an ordinary child, one with attributes but no handle fails as before, and
/// any other name without attributes — ONTAP's CIFS spelling `~snapshot` included — fails alone.
/// Over `NFSv4` even a `.snapshot` without attributes fails alone.
#[tokio::test]
async fn only_an_nfsv3_snapshot_without_attributes_is_looked_up() {
    let (described, hidden) = drained(
        NFSVersion::NFSv3,
        siblings_around(entry(".snapshot", FType3::NF3DIR, b"fh-real")),
        &NoLookup,
    )
    .await;
    assert_eq!(
        outcome(&described),
        (vec!["dir/a", "dir/.snapshot", "dir/b"], vec![])
    );
    assert!(matches!(&described[1], Ok(listed) if listed.kind == EntryKind::Directory));
    assert_eq!(hidden, 0);
    let undescribed = |name: &str| nfs_rs::ReaddirplusEntry {
        attr: None,
        ..entry(name, FType3::NF3DIR, b"fh")
    };
    let others = vec![
        undescribed("other"),
        undescribed("~snapshot"),
        undescribed(".snapshot2"),
        entry(".snapshot", FType3::NF3DIR, b""),
    ];
    let (failed, hidden) = drained(NFSVersion::NFSv3, others, &NoLookup).await;
    assert_eq!(
        outcome(&failed),
        (
            vec![],
            vec![
                "dir/other",
                "dir/~snapshot",
                "dir/.snapshot2",
                "dir/.snapshot"
            ]
        )
    );
    assert_eq!(hidden, 0);
    for version in [NFSVersion::NFSv4p0, NFSVersion::NFSv4p1] {
        let (v4, hidden) = drained(version, vec![ontap_snapshot()], &NoLookup).await;
        assert_eq!(outcome(&v4), (vec![], vec!["dir/.snapshot"]), "{version:?}");
        assert_eq!(hidden, 0);
    }
}

fn timeout() -> NfsError {
    NfsError::Io(io::Error::new(io::ErrorKind::TimedOut, "reply timeout"))
}

fn wrapped(recovery: nfs_rs::RecoveryAction, source: NfsError) -> NfsError {
    NfsError::OperationOutcome(Box::new(nfs_rs::OperationOutcomeError::new(
        nfs_rs::OperationOutcome::DefiniteFailure,
        nfs_rs::OperationClass::ReadOnly,
        recovery,
        nfs_rs::RequestContext {
            operation: "readdirplus".to_owned(),
            protocol: nfs_rs::NFSVersion::NFSv4p1,
            request_id: None,
        },
        source,
    )))
}

/// Every READDIRPLUS failure the listing tells apart, with what reading anew could do and the
/// class it gets.
fn read_failure_cases() -> Vec<(NfsError, NfsRestart, FailureClass)> {
    vec![
        (
            NfsError::Nfs3(V3::NFS3ERR_STALE),
            NfsRestart::Relookup,
            FailureClass::Protocol,
        ),
        (
            NfsError::Nfs3(V3::NFS3ERR_BADHANDLE),
            NfsRestart::Relookup,
            FailureClass::Protocol,
        ),
        (
            NfsError::Nfs4(V4::NFS4ERR_STALE),
            NfsRestart::Relookup,
            FailureClass::Protocol,
        ),
        (
            NfsError::Nfs4(V4::NFS4ERR_BADHANDLE),
            NfsRestart::Relookup,
            FailureClass::Protocol,
        ),
        (
            NfsError::Nfs4(V4::NFS4ERR_FHEXPIRED),
            NfsRestart::Relookup,
            FailureClass::Protocol,
        ),
        (
            NfsError::Nfs4(V4::NFS4ERR_DELAY),
            NfsRestart::Backoff,
            FailureClass::Throttled,
        ),
        (
            NfsError::Nfs4(V4::NFS4ERR_GRACE),
            NfsRestart::Backoff,
            FailureClass::Throttled,
        ),
        (
            NfsError::Nfs3(V3::NFS3ERR_JUKEBOX),
            NfsRestart::Backoff,
            FailureClass::Throttled,
        ),
        (
            NfsError::Nfs3(V3::NFS3ERR_ACCES),
            NfsRestart::Never,
            FailureClass::PermissionDenied,
        ),
        (
            NfsError::Nfs3(V3::NFS3ERR_BAD_COOKIE),
            NfsRestart::Never,
            FailureClass::Protocol,
        ),
        (
            wrapped(nfs_rs::RecoveryAction::Retry, timeout()),
            NfsRestart::Resend,
            FailureClass::Connectivity,
        ),
        (
            NfsError::Io(io::Error::new(io::ErrorKind::ConnectionReset, "reset")),
            NfsRestart::Resend,
            FailureClass::Connectivity,
        ),
        (
            wrapped(
                nfs_rs::RecoveryAction::Remount,
                NfsError::Nfs4(V4::NFS4ERR_BADSESSION),
            ),
            NfsRestart::Never,
            FailureClass::Connectivity,
        ),
        (
            wrapped(
                nfs_rs::RecoveryAction::DoNotRetry,
                NfsError::Nfs4(V4::NFS4ERR_STALE),
            ),
            NfsRestart::Relookup,
            FailureClass::Protocol,
        ),
        (
            wrapped(nfs_rs::RecoveryAction::Remount, timeout()),
            NfsRestart::Never,
            FailureClass::Connectivity,
        ),
    ]
}

/// A handle that no longer names the directory is resolved again, a busy server backed off, a
/// transport failure nfs-rs certifies safe to resend resent (only then: one it wants remounted is
/// not), anything else stands — looking through nfs-rs's recovery wrapper; the class is the one
/// every other NFS role failure gets.
#[test]
fn a_read_failure_says_what_reading_anew_could_do() {
    for (error, restart, class) in read_failure_cases() {
        let context = format!("{error:?}");
        let failure = read_failure(error);
        assert_eq!(failure.restart, restart, "{context}");
        assert_eq!(failure.failure.class, class, "{context}");
    }
}
