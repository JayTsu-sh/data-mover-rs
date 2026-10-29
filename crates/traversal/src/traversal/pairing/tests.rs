#![allow(clippy::unwrap_used)]

use std::fs;
use std::path::Path;
use std::ptr;

use super::*;
use crate::model::{
    EntryKind, EntryOperationFailure, FailureClass, HistoryOrder, IdentityStrength, ObjectHistory,
    Operation, RecordKind, SourceIdentity, Transience, VersionRecord,
};
use crate::storage::backends::local::{test_identity, test_traversal_storage};
use crate::traversal::{
    BlockCounts, Checkpoint, ListingOutcome, ScanFailure, StorageTraversalSource, TraversalOrder,
    TraversalOutcome, TraversalRequest, TraversalSession, TraversalSource,
};

fn path(value: &str) -> StoragePath {
    StoragePath::new(value).unwrap()
}

fn identity(value: &str) -> SourceIdentity {
    SourceIdentity::new(
        test_identity("pairing-test"),
        IdentityStrength::PathScoped,
        value,
    )
    .unwrap()
}

fn entry(value: &str, kind: EntryKind) -> ObservedEntry {
    ObservedEntry::new(path(value), kind, Some(3), None, identity(value)).unwrap()
}

fn file(value: &str) -> ObservedEntry {
    entry(value, EntryKind::File)
}

fn options() -> BlockPairingOptions {
    BlockPairingOptions::new(path("logical"), path("left"), path("right"))
        .with_names(NameSemantics::EXACT, NameSemantics::EXACT)
}

fn block(directory: &str, entries: Vec<ObservedEntry>) -> Vec<TraversalItem> {
    let mut counts = BlockCounts::default();
    for entry in &entries {
        counts.count_emitted(entry);
        counts.listed += 1;
    }
    let mut items: Vec<_> = entries
        .into_iter()
        .map(|e| TraversalItem::Entry(Box::new(e)))
        .collect();
    items.push(TraversalItem::DirectoryListed(Box::new(
        DirectoryListed::new(path(directory), ListingOutcome::Complete, counts),
    )));
    items
}

fn marker(items: &mut [TraversalItem]) -> &mut DirectoryListed {
    match items.last_mut().unwrap() {
        TraversalItem::DirectoryListed(listed) => listed,
        _ => panic!("test boundary missing"),
    }
}

fn failure(value: &str) -> EntryOperationFailure {
    EntryOperationFailure::new(
        path(value),
        Operation::Observe,
        FailureClass::PermissionDenied,
        Transience::Permanent,
        "test failure",
    )
    .unwrap()
}

fn child_failure(items: &mut Vec<TraversalItem>, value: &str) {
    let listed = marker(items);
    listed.counts.listed += 1;
    listed.counts.failed_children += 1;
    items.insert(
        0,
        TraversalItem::Failure(Box::new(ScanFailure::child(failure(value)))),
    );
}

#[test]
fn maps_roots_pairs_unsorted_names_and_borrows_observations() {
    let left = block("left", vec![file("left/z"), file("left/a")]);
    let right = block("right", vec![file("right/b"), file("right/a")]);
    let mut paired = pair_directory_blocks(&left, &right, &options()).unwrap();
    paired.children.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(paired.logical_directory, path("logical"));
    assert_eq!(
        paired.children.iter().map(|p| p.status).collect::<Vec<_>>(),
        vec![
            PairingStatus::Matched,
            PairingStatus::OnlyRight,
            PairingStatus::OnlyLeft
        ]
    );
    assert_eq!(paired.children[0].key, ChildKey::Name("a".into()));
    let TraversalItem::Entry(original) = &left[1] else {
        panic!()
    };
    assert!(ptr::eq(
        paired.children[0].left[0].segments[0],
        original.as_ref()
    ));
}

#[test]
fn unknown_names_and_filters_suppress_absence() {
    let left = block("left", vec![file("left/A")]);
    let mut right = block("right", vec![file("right/a")]);
    let mut config = options();
    config.right_names = NameSemantics::UNKNOWN;
    let paired = pair_directory_blocks(&left, &right, &config).unwrap();
    assert!(
        paired
            .children
            .iter()
            .all(|p| p.status == PairingStatus::Undetermined(AbsenceBarrier::NameSemantics))
    );
    marker(&mut right).counts.filtered = 1;
    marker(&mut right).counts.listed += 1;
    let paired = pair_directory_blocks(&left, &right, &options()).unwrap();
    assert!(
        paired
            .children
            .iter()
            .all(|p| p.status == PairingStatus::Undetermined(AbsenceBarrier::FilteredChildren))
    );
}

#[test]
fn named_failure_blocks_its_name_and_unnamed_failure_blocks_every_absence() {
    let left = block("left", vec![file("left/a"), file("left/b")]);
    let mut right = block("right", vec![]);
    child_failure(&mut right, "right/a");
    let mut paired = pair_directory_blocks(&left, &right, &options()).unwrap();
    paired.children.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(
        paired.children[0].status,
        PairingStatus::Undetermined(AbsenceBarrier::ChildFailure)
    );
    assert_eq!(paired.children[1].status, PairingStatus::OnlyLeft);
    child_failure(&mut right, "right");
    let mut paired = pair_directory_blocks(&left, &right, &options()).unwrap();
    paired.children.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(
        paired.children[1].status,
        PairingStatus::Undetermined(AbsenceBarrier::UnnamedChildFailure)
    );
}

#[test]
fn interrupted_and_unreadable_blocks_never_establish_absence() {
    let left = block("left", vec![file("left/a")]);
    for entries in [vec![], vec![file("right/b")]] {
        let mut right = block("right", entries);
        let delivered = marker(&mut right).counts.listed;
        let failed = ScanFailure::listing(path("right"), failure("right"), delivered);
        marker(&mut right).outcome = failed.listing_outcome().unwrap();
        right.insert(right.len() - 1, TraversalItem::Failure(Box::new(failed)));
        let paired = pair_directory_blocks(&left, &right, &options()).unwrap();
        assert!(paired.children.iter().all(|p| p.status == PairingStatus::Undetermined(AbsenceBarrier::IncompleteListing)));
    }
}

#[test]
fn duplicates_and_kind_conflicts_are_retained() {
    let left = block("left", vec![file("left/a"), file("left/a")]);
    let right = block("right", vec![entry("right/a", EntryKind::Directory)]);
    let paired = pair_directory_blocks(&left, &right, &options()).unwrap();
    assert_eq!(paired.children[0].status, PairingStatus::Conflict);
    assert_eq!(paired.children[0].left.len(), 2);
    let left = block("left", vec![file("left/a")]);
    assert_eq!(
        pair_directory_blocks(&left, &right, &options())
            .unwrap()
            .children[0]
            .status,
        PairingStatus::Conflict
    );
}

#[test]
fn s3_file_prefix_and_own_directory_object_are_distinct() {
    let left = block(
        "left",
        vec![
            file("left/a"),
            entry("left/a", EntryKind::Directory),
            file("left/"),
        ],
    );
    let right = block(
        "right",
        vec![
            file("right/"),
            entry("right/a", EntryKind::Directory),
            file("right/a"),
        ],
    );
    let mut paired = pair_directory_blocks(&left, &right, &options()).unwrap();
    paired.children.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(paired.children.len(), 2);
    assert!(
        paired
            .children
            .iter()
            .all(|p| p.status == PairingStatus::Matched)
    );
    assert_eq!(paired.children[0].left.len(), 2);
    assert_eq!(paired.children[1].key, ChildKey::DirectoryObject);
}

fn history_entry(value: &str) -> ObservedEntry {
    let object = VersionRecord::from_listing(
        "v",
        false,
        RecordKind::Object {
            size: 3,
            etag: "e".into(),
            modified: None,
        },
        identity("version"),
    )
    .unwrap();
    let deleted = VersionRecord::from_listing(
        "d",
        true,
        RecordKind::DeleteMarker { modified: None },
        identity("delete-marker"),
    )
    .unwrap();
    let history = ObjectHistory::whole(vec![object, deleted], HistoryOrder::Server).unwrap();
    ObservedEntry::new(path(value), EntryKind::File, None, None, identity(value))
        .unwrap()
        .with_history(Some(Box::new(history)))
        .unwrap_or_else(|error| panic!("{error}"))
}

fn segmented_block(value: &str) -> Vec<TraversalItem> {
    let whole = history_entry(value);
    let mut items = block("left", vec![whole.clone()]);
    items.splice(
        0..1,
        whole
            .into_segments(1)
            .into_iter()
            .map(|e| TraversalItem::Entry(Box::new(e))),
    );
    items
}

#[test]
fn complete_history_segments_count_as_one_tombstoned_child() {
    let left = segmented_block("left/a");
    let right = block("right", vec![]);
    let paired = pair_directory_blocks(&left, &right, &options()).unwrap();
    let child = &paired.children[0];
    assert_eq!(child.status, PairingStatus::OnlyLeft);
    assert_eq!(child.left.len(), 1);
    assert_eq!(child.left[0].segments.len(), 2);
    assert!(
        child.left[0].segments[1]
            .history()
            .unwrap()
            .latest()
            .unwrap()
            .is_delete_marker()
    );
    assert_eq!(paired.left.counts.emitted, 1);
}

#[test]
fn history_gaps_reordering_and_missing_end_are_rejected() {
    let right = block("right", vec![]);
    for index in [0, 1] {
        let mut left = segmented_block("left/a");
        left.remove(index);
        assert!(pair_directory_blocks(&left, &right, &options()).is_err());
    }
    let mut left = segmented_block("left/a");
    left.swap(0, 1);
    assert!(pair_directory_blocks(&left, &right, &options()).is_err());
}

#[test]
fn boundaries_parent_mapping_and_accounting_are_checked() {
    let left = block("left", vec![file("left/a")]);
    let right = block("right", vec![]);
    let mut cases = vec![
        left[..1].to_vec(),
        block("left", vec![file("left/nested/a")]),
        block("elsewhere", vec![]),
    ];
    let mut trailing = left.clone();
    trailing.push(left[0].clone());
    cases.push(trailing);
    let mut wrong_counts = left.clone();
    marker(&mut wrong_counts).counts.files = 0;
    cases.push(wrong_counts);
    let mut balanced_wrong = left.clone();
    marker(&mut balanced_wrong).counts.bytes += 1;
    cases.push(balanced_wrong);
    for case in cases {
        assert_eq!(
            pair_directory_blocks(&case, &right, &options())
                .unwrap_err()
                .side,
            PairingSide::Left
        );
    }
}

#[test]
fn checkpoints_are_ignored_and_depth_counts_remain_visible() {
    let mut left = block("left", vec![entry("left/a", EntryKind::Directory)]);
    marker(&mut left).counts.truncated_dirs = 1;
    let checkpoint = TraversalItem::Checkpoint(Box::new(Checkpoint::from_bytes(vec![1].into())));
    left.insert(0, checkpoint.clone());
    left.push(checkpoint);
    let right = block("right", vec![]);
    let paired = pair_directory_blocks(&left, &right, &options()).unwrap();
    assert_eq!(paired.left.counts.truncated_dirs, 1);
    assert_eq!(paired.children[0].status, PairingStatus::OnlyLeft);
}

#[test]
fn path_mapping_preserves_parent_spelling_without_normalization() {
    for (left_parent, left_child, right_parent, right_child) in [
        ("", "a", "", "a"),
        ("left/", "left//a", "right/./", "right/.//a"),
    ] {
        let left = block(left_parent, vec![file(left_child)]);
        let right = block(right_parent, vec![file(right_child)]);
        let config = BlockPairingOptions::new(path(""), path(left_parent), path(right_parent));
        assert_eq!(
            pair_directory_blocks(&left, &right, &config)
                .unwrap()
                .children[0]
                .status,
            PairingStatus::Matched
        );
    }
}

fn local_session(root: &Path) -> TraversalSession {
    let (storage, _, _) = test_traversal_storage(root, "pairing-local").unwrap();
    StorageTraversalSource::new(&storage)
        .unwrap()
        .traverse(TraversalRequest::new(StoragePath::root()).order(TraversalOrder::Blocks))
}

async fn next_block(session: &mut TraversalSession) -> Vec<TraversalItem> {
    let mut items = Vec::new();
    while let Some(item) = session.next_item().await {
        let closed = matches!(item, TraversalItem::DirectoryListed(_));
        items.push(item);
        if closed {
            break;
        }
    }
    items
}

#[tokio::test]
async fn pairs_real_local_blocks_then_checks_both_session_completions() {
    let left_dir = tempfile::tempdir().unwrap();
    let right_dir = tempfile::tempdir().unwrap();
    for root in [left_dir.path(), right_dir.path()] {
        fs::create_dir(root.join("nested")).unwrap();
        fs::write(root.join("common"), b"same").unwrap();
        fs::write(root.join("nested/child"), b"same").unwrap();
    }
    fs::write(left_dir.path().join("left-only"), b"left").unwrap();
    fs::write(right_dir.path().join("right-only"), b"right").unwrap();
    let mut left = local_session(left_dir.path());
    let mut right = local_session(right_dir.path());
    for directory in ["", "nested"] {
        let lblock = next_block(&mut left).await;
        let rblock = next_block(&mut right).await;
        let config = BlockPairingOptions::new(path(directory), path(directory), path(directory))
            .with_names(NameSemantics::EXACT, NameSemantics::EXACT);
        let paired = pair_directory_blocks(&lblock, &rblock, &config).unwrap();
        assert_eq!(
            paired.children.len(),
            if directory.is_empty() { 4 } else { 1 }
        );
        for child in paired.children {
            let expected = match &child.key {
                ChildKey::Name(name) if name == "left-only" => PairingStatus::OnlyLeft,
                ChildKey::Name(name) if name == "right-only" => PairingStatus::OnlyRight,
                _ => PairingStatus::Matched,
            };
            assert_eq!(child.status, expected);
        }
    }
    assert!(left.next_item().await.is_none() && right.next_item().await.is_none());
    for outcome in [left.finish().await.unwrap(), right.finish().await.unwrap()] {
        let TraversalOutcome::Completed(completion) = outcome else {
            panic!("unexpected cancellation")
        };
        assert!(completion.totals.is_balanced());
        assert_eq!(completion.totals.listing_failures, 0);
        assert_eq!(completion.totals.failed_children, 0);
        assert_eq!(completion.totals.emitted, 4);
    }
}
