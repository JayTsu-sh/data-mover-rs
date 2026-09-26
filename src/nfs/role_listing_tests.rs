//! Tests of the NFS role listing's READDIRPLUS reader and entry conversion.

use std::cell::Cell;
use std::io;
use std::sync::atomic::AtomicUsize;

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
    stop_after: Option<usize>,
}

#[async_trait::async_trait]
impl NfsListingPages for Collected {
    fn push(&mut self, child: NfsListedChild) -> bool {
        self.children.push(child);
        self.held += 1;
        self.held >= 2
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
    let end = drain_readdirplus(&directory(), entries, &mut pages, false).await;
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
    let end = drain_readdirplus(&directory(), Box::pin(entries), &mut pages, false).await;
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
    let failure = drain_readdirplus(&directory(), entries, &mut pages, false).await;
    assert!(matches!(
        failure,
        Err(NfsReadFailure {
            restart: NfsRestart::Relookup,
            ..
        })
    ));
    assert_eq!(pages.children.len(), 3);
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
