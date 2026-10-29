//! Regression coverage for ordinary `NFSv3` children with optional READDIRPLUS facts omitted.

use std::result::Result;

use super::*;

fn listed_file(has_attr: bool, has_handle: bool) -> nfs_rs::ReaddirplusEntry {
    nfs_rs::ReaddirplusEntry {
        attr: has_attr.then(regular),
        handle: if has_handle {
            Bytes::from_static(b"fh-listed")
        } else {
            Bytes::new()
        },
        ..entry("file", FType3::NF3REG, b"unused")
    }
}

#[tokio::test]
async fn incomplete_v3_children_are_completed_with_the_fewest_requests() {
    for (has_attr, has_handle, lookup_attr, expected) in [
        (true, true, true, vec![]),
        (false, true, true, vec!["GETATTR fh-listed"]),
        (true, false, true, vec!["LOOKUP file"]),
        (false, false, true, vec!["LOOKUP file"]),
        (
            false,
            false,
            false,
            vec!["LOOKUP file", "GETATTR fh-looked-up"],
        ),
        (
            true,
            false,
            false,
            vec!["LOOKUP file", "GETATTR fh-looked-up"],
        ),
    ] {
        let lookup = Answering::new(
            if lookup_attr {
                || found(FType3::NF3REG, true)
            } else {
                || found(FType3::NF3REG, false)
            },
            || Ok(regular()),
        );
        let entry = listed_file(has_attr, has_handle);
        let (children, hidden) = drained(NFSVersion::NFSv3, siblings_around(entry), &lookup).await;
        assert_eq!(
            outcome(&children),
            (vec!["dir/a", "dir/file", "dir/b"], vec![])
        );
        assert_eq!(hidden, 0);
        assert_eq!(lookup.asked(), expected);
        let child = children[1]
            .as_ref()
            .unwrap_or_else(|_| panic!("completed child"));
        assert_eq!(
            child.file_handle.as_ref(),
            if has_handle {
                b"fh-listed".as_slice()
            } else {
                b"fh-looked-up".as_slice()
            }
        );
    }
}

/// LOOKUP can observe a replacement: the old attributes must not be paired with its new handle.
#[tokio::test]
async fn lookup_replaces_both_handle_and_attributes() {
    let lookup = Answering::new(|| found(FType3::NF3DIR, true), no_getattr);
    let (children, _) = drained(
        NFSVersion::NFSv3,
        vec![entry("replaced", FType3::NF3REG, b"")],
        &lookup,
    )
    .await;
    let child = children[0]
        .as_ref()
        .unwrap_or_else(|_| panic!("completed child"));
    assert_eq!(child.kind, EntryKind::Directory);
    assert_eq!(child.file_handle, Bytes::from_static(b"fh-looked-up"));
}

/// The export's configured sub-root is already represented by the directory handle. Only the
/// child's basename may reach LOOKUP, even several levels below that root (MR !58).
#[tokio::test]
async fn nested_listing_looks_up_only_the_child_name() {
    let directory = StoragePath::new("level/inner").unwrap_or_else(|e| panic!("{e}"));
    let lookup = Answering::new(|| found(FType3::NF3REG, false), || Ok(regular()));
    let mut pages = Collected::default();
    let entries = stream::iter([Ok(nfs_rs::ReaddirplusEntry {
        attr: None,
        ..entry("file", FType3::NF3REG, b"")
    })]);
    let end = drain_readdirplus(&directory, entries, &mut pages, NFSVersion::NFSv3, &lookup).await;
    assert!(matches!(end, Ok(NfsReadEnd::Complete)));
    assert_eq!(outcome(&pages.children), (vec!["level/inner/file"], vec![]));
    assert_eq!(lookup.asked(), ["LOOKUP file", "GETATTR fh-looked-up"]);
}

#[tokio::test]
async fn completion_preserves_child_failure_classes_and_siblings() {
    for (lookup, expected) in [
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
                || found(FType3::NF3REG, false),
                || Err(NfsError::Nfs3(V3::NFS3ERR_STALE)),
            ),
            FailureClass::Protocol,
        ),
    ] {
        let (children, _) = drained(
            NFSVersion::NFSv3,
            siblings_around(nfs_rs::ReaddirplusEntry {
                attr: None,
                ..entry("file", FType3::NF3REG, b"")
            }),
            &lookup,
        )
        .await;
        assert_eq!(
            outcome(&children),
            (vec!["dir/a", "dir/b"], vec!["dir/file"])
        );
        let failure = &children[1]
            .as_ref()
            .err()
            .unwrap_or_else(|| panic!("child failure"))
            .failure;
        assert_eq!(failure.class, expected);
        assert!(failure.status.is_some());
    }
}

#[tokio::test]
async fn completion_aborts_on_parent_staleness_transport_and_throttling() {
    for (lookup, restart) in [
        (
            Answering::new(|| Err(NfsError::Nfs3(V3::NFS3ERR_STALE)), no_getattr),
            NfsRestart::Relookup,
        ),
        (
            Answering::new(
                || Err(wrapped(nfs_rs::RecoveryAction::Retry, timeout())),
                no_getattr,
            ),
            NfsRestart::Resend,
        ),
        (
            Answering::new(
                || found(FType3::NF3REG, false),
                || Err(NfsError::Nfs3(V3::NFS3ERR_JUKEBOX)),
            ),
            NfsRestart::Backoff,
        ),
    ] {
        let entries = stream::iter(
            siblings_around(nfs_rs::ReaddirplusEntry {
                attr: None,
                ..entry("file", FType3::NF3REG, b"")
            })
            .into_iter()
            .map(Ok),
        );
        let mut pages = Collected::default();
        let end = drain_readdirplus(
            &directory(),
            entries,
            &mut pages,
            NFSVersion::NFSv3,
            &lookup,
        )
        .await;
        assert!(matches!(end, Err(failure) if failure.restart == restart));
        assert_eq!(outcome(&pages.children), (vec!["dir/a"], vec![]));
    }
}

#[tokio::test]
async fn completion_skips_hidden_invalid_replayed_and_stopped_children() {
    let incomplete = |name| nfs_rs::ReaddirplusEntry {
        attr: None,
        ..entry(name, FType3::NF3REG, b"")
    };
    let mut pages = Collected {
        taken: vec!["replayed"],
        ..Collected::default()
    };
    let entries = stream::iter([
        Ok(incomplete(".data-mover-test.stage")),
        Ok(incomplete("replayed")),
        Ok(incomplete("bad\u{fffd}name")),
        Ok(incomplete("../outside")),
    ]);
    let end = drain_readdirplus(
        &directory(),
        entries,
        &mut pages,
        NFSVersion::NFSv3,
        &NoLookup,
    )
    .await;
    assert!(matches!(end, Ok(NfsReadEnd::Complete)));
    assert_eq!(pages.hidden, 1);
    assert_eq!(pages.children.len(), 2);
    assert!(pages.children.iter().all(Result::is_err));

    let entries = stream::iter([Ok(incomplete("stopped"))]);
    let mut pages = Collected {
        stop_after: Some(0),
        ..Collected::default()
    };
    let end = drain_readdirplus(
        &directory(),
        entries,
        &mut pages,
        NFSVersion::NFSv3,
        &NoLookup,
    )
    .await;
    assert!(matches!(end, Ok(NfsReadEnd::Stopped)));
}

#[tokio::test]
async fn incomplete_v4_children_are_not_reinterpreted_as_v3_omissions() {
    for version in [NFSVersion::NFSv4p0, NFSVersion::NFSv4p1] {
        let (children, hidden) = drained(
            version,
            vec![nfs_rs::ReaddirplusEntry {
                attr: None,
                ..entry("file", FType3::NF3REG, b"")
            }],
            &NoLookup,
        )
        .await;
        assert_eq!(outcome(&children), (vec![], vec!["dir/file"]));
        assert_eq!(hidden, 0);
    }
}

#[tokio::test]
async fn a_listed_handle_failure_does_not_resolve_the_parent_again() {
    let lookup = Answering::new(
        || panic!("a listed child handle does not need LOOKUP"),
        || Err(NfsError::Nfs3(V3::NFS3ERR_STALE)),
    );
    let (children, _) = drained(
        NFSVersion::NFSv3,
        siblings_around(nfs_rs::ReaddirplusEntry {
            attr: None,
            ..entry("file", FType3::NF3REG, b"fh-listed")
        }),
        &lookup,
    )
    .await;
    assert_eq!(
        outcome(&children),
        (vec!["dir/a", "dir/b"], vec!["dir/file"])
    );
    assert_eq!(lookup.asked(), ["GETATTR fh-listed"]);
}

#[tokio::test]
async fn an_empty_lookup_handle_fails_without_sending_getattr() {
    let lookup = Answering::new(
        || {
            Ok(nfs_rs::ObjRes {
                fh: Bytes::new(),
                attr: None,
            })
        },
        no_getattr,
    );
    let (children, _) = drained(
        NFSVersion::NFSv3,
        vec![entry("file", FType3::NF3REG, b"")],
        &lookup,
    )
    .await;
    assert_eq!(outcome(&children), (vec![], vec!["dir/file"]));
    assert_eq!(lookup.asked(), ["LOOKUP file"]);
}
