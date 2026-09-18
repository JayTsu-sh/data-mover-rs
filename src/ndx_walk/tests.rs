use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::{NdxWalkRequest, ndx_walk};
use crate::dir_tree::{DirPageResult, NdxEvent};
use crate::model::{
    BackendIdentity, BackendKind, EntryKind, EntryOperationFailure, FailureClass, IdentityStrength,
    ObservedEntry, Operation, SourceIdentity, StoragePath, StorageTimestamp, TimePrecision,
    TimestampMetadata, Transience,
};
use crate::storage::{
    BackendCapabilities, CapabilityAvailability, Namespace, NamespaceRequest, NamespaceResult,
    SourceDescriptor, Storage, StorageRoleFailure, UnsupportedReason,
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

const MODIFIED_NANOS: i128 = 1_700_000_000_000_000_000;

fn path(value: &str) -> Result<StoragePath> {
    Ok(StoragePath::new(value)?)
}

/// How one directory's listing behaves.
enum Listing {
    /// Children as `(name, kind)`, listed with inline timestamps.
    Entries(Vec<(&'static str, EntryKind)>),
    /// Children listed without inline timestamps, as every non-CIFS backend does today.
    WithoutTimestamps(Vec<(&'static str, EntryKind)>),
    /// One unreadable directory.
    EntryFailure,
    /// A broken session.
    SessionFailure,
    /// The backend reports the operation was cancelled.
    Cancelled,
    /// Children as `(name, kind)` plus one child the backend could not describe.
    Partial(Vec<(&'static str, EntryKind)>),
}

struct TreeNamespace {
    listings: HashMap<String, Listing>,
    listed: Mutex<Vec<String>>,
    non_list_requests: Mutex<Vec<String>>,
}

impl TreeNamespace {
    fn new(listings: Vec<(&str, Listing)>) -> Arc<Self> {
        Arc::new(Self {
            listings: listings
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
            listed: Mutex::new(Vec::new()),
            non_list_requests: Mutex::new(Vec::new()),
        })
    }

    fn listed(&self) -> Vec<String> {
        self.listed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn non_list_requests(&self) -> Vec<String> {
        self.non_list_requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

fn descriptor(full_path: &str, kind: EntryKind, timestamps: bool) -> Result<SourceDescriptor> {
    let descriptor = SourceDescriptor {
        path: path(full_path)?,
        kind,
        size: (kind == EntryKind::File).then_some(3),
        source_identity: SourceIdentity::new(
            BackendIdentity::new(BackendKind::Cifs, "ndx-walk-test")?,
            IdentityStrength::PathScoped,
            full_path.as_bytes(),
        )?,
        backend_fact: None,
        content_version: None,
        inline_timestamps: None,
        inline_mode: None,
    };
    if !timestamps {
        return Ok(descriptor);
    }
    let stamp = StorageTimestamp::new(MODIFIED_NANOS, TimePrecision::Nanoseconds)?;
    Ok(descriptor.with_inline_timestamps(TimestampMetadata {
        accessed: Some(stamp),
        modified: Some(stamp),
        created: Some(stamp),
    }))
}

fn children(
    parent: &str,
    entries: &[(&'static str, EntryKind)],
    timestamps: bool,
) -> Result<NamespaceResult> {
    let mut built = Vec::new();
    for (name, kind) in entries {
        let full = if parent.is_empty() {
            (*name).to_owned()
        } else {
            format!("{parent}/{name}")
        };
        built.push(descriptor(&full, *kind, timestamps)?);
    }
    Ok(NamespaceResult::Entries(built))
}

fn entry_failure(target: &StoragePath) -> StorageRoleFailure {
    entry_failure_with(target, FailureClass::PermissionDenied)
}

fn entry_failure_with(target: &StoragePath, class: FailureClass) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            target.clone(),
            Operation::Traverse,
            class,
            Transience::Permanent,
            "listing refused by the test namespace",
        )
        .unwrap_or_else(|error| panic!("{error}")),
    )
}

fn session_failure() -> StorageRoleFailure {
    StorageRoleFailure::Session(
        crate::model::BackendSessionFailure::new(
            Operation::Traverse,
            FailureClass::Connectivity,
            Transience::Transient,
            "test namespace session failed",
        )
        .unwrap_or_else(|error| panic!("{error}")),
    )
}

#[async_trait]
impl Namespace for TreeNamespace {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> std::result::Result<NamespaceResult, StorageRoleFailure> {
        let NamespaceRequest::List(target) = request else {
            self.non_list_requests
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(format!("{request:?}"));
            return Err(entry_failure(&StoragePath::root()));
        };
        let key = target.as_str().to_owned();
        self.listed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(key.clone());
        match self.listings.get(&key) {
            Some(Listing::Entries(entries)) => {
                children(&key, entries, true).map_err(|_| entry_failure(&target))
            }
            Some(Listing::WithoutTimestamps(entries)) => {
                children(&key, entries, false).map_err(|_| entry_failure(&target))
            }
            Some(Listing::EntryFailure) => Err(entry_failure(&target)),
            Some(Listing::SessionFailure) => Err(session_failure()),
            Some(Listing::Cancelled) => Err(entry_failure_with(&target, FailureClass::Cancelled)),
            Some(Listing::Partial(entries)) => {
                let Ok(NamespaceResult::Entries(described)) = children(&key, entries, true) else {
                    return Err(entry_failure(&target));
                };
                let StorageRoleFailure::Entry(failure) =
                    entry_failure_with(&target, FailureClass::Unsupported)
                else {
                    unreachable!("entry_failure_with builds an entry failure")
                };
                Ok(NamespaceResult::Listing {
                    entries: described,
                    failures: vec![failure],
                })
            }
            None => Ok(NamespaceResult::Entries(Vec::new())),
        }
    }
}

/// Only the namespace role is lent, which is all this adapter borrows.
fn namespace_only_capabilities() -> Result<BackendCapabilities> {
    let absent = CapabilityAvailability::Unsupported(UnsupportedReason::new(
        "not lent by the ndx_walk test storage",
    )?);
    Ok(BackendCapabilities::new(
        absent.clone(),
        absent.clone(),
        CapabilityAvailability::Supported,
        absent,
    ))
}

fn storage(namespace: Arc<TreeNamespace>) -> Result<Storage> {
    let capabilities = namespace_only_capabilities()?;
    Ok(Storage::connected(
        BackendIdentity::new(BackendKind::Cifs, "ndx-walk-test")?,
        capabilities,
        None,
        None,
        Some(namespace),
        None,
        None,
    )?)
}

fn request(root: StoragePath) -> Result<NdxWalkRequest> {
    Ok(NdxWalkRequest {
        root,
        max_depth: None,
        match_expressions: None,
        exclude_expressions: None,
        concurrency: NonZeroUsize::new(2).ok_or("concurrency must be non-zero")?,
        cancel: CancellationToken::new(),
    })
}

/// Drains the walk into its pages and errors, in emission order.
async fn drain(walk: &super::NdxWalkIterator) -> (Vec<DirPageResult<ObservedEntry>>, Vec<String>) {
    let mut pages = Vec::new();
    let mut errors = Vec::new();
    while let Some(event) = walk.next().await {
        match event {
            NdxEvent::Page(page) => pages.push(page),
            NdxEvent::Error { path, reason } => errors.push(format!("{path}: {reason}")),
            NdxEvent::Done => break,
        }
    }
    (pages, errors)
}

/// `a/{x.txt, deep/{y.txt}}` plus a sibling `b/`, enough to exercise DFS and gaps.
fn sample_tree() -> Vec<(&'static str, Listing)> {
    vec![
        (
            "",
            Listing::Entries(vec![
                ("b", EntryKind::Directory),
                ("a", EntryKind::Directory),
                ("root.txt", EntryKind::File),
            ]),
        ),
        (
            "a",
            Listing::Entries(vec![
                ("x.txt", EntryKind::File),
                ("deep", EntryKind::Directory),
            ]),
        ),
        ("a/deep", Listing::Entries(vec![("y.txt", EntryKind::File)])),
        ("b", Listing::Entries(vec![("z.txt", EntryKind::File)])),
    ]
}

fn page_names(page: &DirPageResult<ObservedEntry>) -> Vec<String> {
    page.files
        .iter()
        .chain(page.subdirs.iter())
        .map(|entry| name_of(&entry.entry))
        .collect()
}

/// Final path component of an emitted observation.
fn name_of(entry: &ObservedEntry) -> String {
    let value = entry.path().as_str();
    value
        .rsplit_once('/')
        .map_or(value, |(_, name)| name)
        .to_owned()
}

#[tokio::test]
async fn pages_arrive_depth_first_with_entries_sorted_by_name() -> Result {
    let namespace = TreeNamespace::new(sample_tree());
    let storage = storage(Arc::clone(&namespace))?;
    let (pages, errors) = drain(&ndx_walk(&storage, request(StoragePath::root())?)?).await;
    assert!(errors.is_empty(), "unexpected errors: {errors:?}");

    let visited: Vec<&str> = pages.iter().map(|page| page.dir_path.as_str()).collect();
    assert_eq!(
        visited,
        vec!["", "a", "a/deep", "b"],
        "subdirectories are visited depth-first in name order, not listing order"
    );
    assert_eq!(
        page_names(&pages[0]),
        vec!["root.txt", "a", "b"],
        "files sort before subdirectories, each group sorted by name"
    );
    assert_eq!(page_names(&pages[1]), vec!["x.txt", "deep"]);
    Ok(())
}

#[tokio::test]
async fn ndx_rises_monotonically_and_each_page_reserves_its_gap_slot() -> Result {
    let namespace = TreeNamespace::new(sample_tree());
    let storage = storage(Arc::clone(&namespace))?;
    let (pages, _) = drain(&ndx_walk(&storage, request(StoragePath::root())?)?).await;

    // Entry numbering is deliberately not dense: `allocate_gap_ndx` reserves one slot between
    // segments, which is what `gap_ndx` reports. Only monotonicity and the gap accounting are
    // invariants.
    let mut previous = -1;
    for page in &pages {
        let entries: Vec<i32> = page
            .files
            .iter()
            .chain(page.subdirs.iter())
            .map(|entry| entry.ndx)
            .collect();
        assert_eq!(
            page.ndx_start, entries[0],
            "ndx_start names the first entry of page '{}'",
            page.dir_path
        );
        for ndx in &entries {
            assert!(
                *ndx > previous,
                "NDX must rise across the DFS order, got {ndx} after {previous}"
            );
            previous = *ndx;
        }
        if page.gap_ndx >= 0 {
            assert!(
                page.gap_ndx > previous,
                "a page's gap slot follows its entries"
            );
            previous = page.gap_ndx;
        }
    }
    assert_eq!(
        pages.last().map(|page| page.gap_ndx),
        Some(-1),
        "the last page of the tree carries no gap"
    );
    assert!(
        pages[..pages.len() - 1]
            .iter()
            .all(|page| page.gap_ndx >= 0),
        "every earlier page reserves a gap slot"
    );
    Ok(())
}

#[tokio::test]
async fn one_list_per_directory_and_no_metadata_round_trips() -> Result {
    let namespace = TreeNamespace::new(sample_tree());
    let storage = storage(Arc::clone(&namespace))?;
    let (pages, _) = drain(&ndx_walk(&storage, request(StoragePath::root())?)?).await;
    assert_eq!(pages.len(), 4);

    let mut listed = namespace.listed();
    listed.sort();
    assert_eq!(
        listed,
        vec!["", "a", "a/deep", "b"],
        "each directory is listed exactly once"
    );
    assert!(
        namespace.non_list_requests().is_empty(),
        "timestamps come from the listing, so no per-entry observation is issued"
    );
    Ok(())
}

#[tokio::test]
async fn a_subtree_root_rebases_emitted_paths_and_still_lists_the_backend_path() -> Result {
    let namespace = TreeNamespace::new(sample_tree());
    let storage = storage(Arc::clone(&namespace))?;
    let (pages, errors) = drain(&ndx_walk(&storage, request(path("a")?)?)?).await;
    assert!(errors.is_empty(), "unexpected errors: {errors:?}");

    assert_eq!(
        namespace.listed(),
        vec!["a", "a/deep"],
        "listing targets stay backend-relative"
    );
    let emitted: Vec<String> = pages
        .iter()
        .flat_map(|page| page.files.iter().chain(page.subdirs.iter()))
        .map(|entry| entry.entry.path().as_str().to_owned())
        .collect();
    assert_eq!(
        emitted,
        vec!["a/x.txt", "a/deep", "a/deep/y.txt"],
        "an observation carries its backend-relative path, as `StorageTraversalSource` does"
    );
    assert_eq!(
        pages
            .iter()
            .map(|page| page.dir_path.clone())
            .collect::<Vec<_>>(),
        vec!["", "deep"],
        "the driver's own frame of reference stays relative to the traversal root, because it \
         derives depth from the separator count"
    );
    Ok(())
}

#[tokio::test]
async fn max_depth_limits_how_deep_directories_are_listed() -> Result {
    for (depth, expected) in [(Some(1), vec![""]), (Some(2), vec!["", "a", "b"])] {
        let namespace = TreeNamespace::new(sample_tree());
        let storage = storage(Arc::clone(&namespace))?;
        let mut walk_request = request(StoragePath::root())?;
        walk_request.max_depth = depth.and_then(NonZeroUsize::new);
        let (pages, _) = drain(&ndx_walk(&storage, walk_request)?).await;
        let visited: Vec<&str> = pages.iter().map(|page| page.dir_path.as_str()).collect();
        assert_eq!(
            visited, expected,
            "max_depth {depth:?} listed the wrong set"
        );
    }

    let namespace = TreeNamespace::new(sample_tree());
    let storage = storage(Arc::clone(&namespace))?;
    let (pages, _) = drain(&ndx_walk(&storage, request(StoragePath::root())?)?).await;
    assert_eq!(pages.len(), 4, "None means unlimited, not zero");
    Ok(())
}

#[tokio::test]
async fn depth_bounded_directories_are_emitted_as_entries_rather_than_dropped() -> Result {
    let namespace = TreeNamespace::new(sample_tree());
    let storage = storage(Arc::clone(&namespace))?;
    let mut walk_request = request(StoragePath::root())?;
    walk_request.max_depth = NonZeroUsize::new(1);
    let (pages, _) = drain(&ndx_walk(&storage, walk_request)?).await;
    assert_eq!(
        page_names(&pages[0]),
        vec!["a", "b", "root.txt"],
        "directories at the depth limit still appear, sorted among the files"
    );
    Ok(())
}

#[tokio::test]
async fn exclude_expression_hides_entries_but_still_descends_when_asked() -> Result {
    let namespace = TreeNamespace::new(sample_tree());
    let storage = storage(Arc::clone(&namespace))?;
    let mut walk_request = request(StoragePath::root())?;
    walk_request.exclude_expressions =
        Some(crate::filter::parse_filter_expression("name == \"x.txt\"")?);
    let (pages, _) = drain(&ndx_walk(&storage, walk_request)?).await;
    let emitted: Vec<String> = pages
        .iter()
        .flat_map(|page| page.files.iter().chain(page.subdirs.iter()))
        .map(|entry| name_of(&entry.entry))
        .collect();
    assert!(
        !emitted.contains(&"x.txt".to_owned()),
        "excluded entry must not be emitted: {emitted:?}"
    );
    assert!(
        emitted.contains(&"y.txt".to_owned()),
        "excluding one file must not prune its sibling subtree: {emitted:?}"
    );
    Ok(())
}

#[tokio::test]
async fn an_unreadable_directory_is_reported_and_the_rest_of_the_tree_continues() -> Result {
    let mut listings = sample_tree();
    listings.retain(|(key, _)| *key != "a");
    listings.push(("a", Listing::EntryFailure));
    let namespace = TreeNamespace::new(listings);
    let storage = storage(Arc::clone(&namespace))?;
    let (pages, errors) = drain(&ndx_walk(&storage, request(StoragePath::root())?)?).await;

    assert_eq!(errors.len(), 1, "one soft error for the one bad directory");
    assert!(
        errors[0].contains("failed to list 'a'"),
        "error names the directory: {errors:?}"
    );
    let visited: Vec<&str> = pages.iter().map(|page| page.dir_path.as_str()).collect();
    assert!(
        visited.contains(&"b"),
        "the sibling subtree still pages: {visited:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_listing_without_timestamps_is_refused_instead_of_reporting_the_epoch() -> Result {
    let namespace = TreeNamespace::new(vec![(
        "",
        Listing::WithoutTimestamps(vec![("only.txt", EntryKind::File)]),
    )]);
    let storage = storage(Arc::clone(&namespace))?;
    let (pages, errors) = drain(&ndx_walk(&storage, request(StoragePath::root())?)?).await;

    assert_eq!(errors.len(), 1, "one aggregated error, not one per entry");
    assert!(
        errors[0].contains("carry no inline modification time"),
        "error explains why the directory was refused: {errors:?}"
    );
    assert!(
        pages.is_empty(),
        "a refused directory emits no page at all, so nothing carries an epoch mtime: {pages:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_refused_directory_takes_its_whole_subtree_with_it() -> Result {
    let namespace = TreeNamespace::new(vec![
        (
            "",
            Listing::WithoutTimestamps(vec![("sub", EntryKind::Directory)]),
        ),
        ("sub", Listing::Entries(vec![("deep.txt", EntryKind::File)])),
    ]);
    let storage = storage(Arc::clone(&namespace))?;
    let (pages, errors) = drain(&ndx_walk(&storage, request(StoragePath::root())?)?).await;

    assert_eq!(errors.len(), 1);
    assert!(pages.is_empty(), "nothing is emitted: {pages:?}");
    assert_eq!(
        namespace.listed(),
        vec![""],
        "the subtree below a refused directory is never listed"
    );
    Ok(())
}

#[tokio::test]
async fn a_present_record_without_a_modification_time_is_refused_too() -> Result {
    struct UndatedNamespace;
    #[async_trait]
    impl Namespace for UndatedNamespace {
        async fn execute(
            &self,
            request: NamespaceRequest,
        ) -> std::result::Result<NamespaceResult, StorageRoleFailure> {
            let NamespaceRequest::List(target) = request else {
                return Err(entry_failure(&StoragePath::root()));
            };
            if !target.as_str().is_empty() {
                return Ok(NamespaceResult::Entries(Vec::new()));
            }
            let stamp = StorageTimestamp::new(MODIFIED_NANOS, TimePrecision::Nanoseconds)
                .unwrap_or_else(|error| panic!("{error}"));
            // The record is present, but the field transfers actually depend on is not.
            let descriptor = descriptor("only.txt", EntryKind::File, false)
                .unwrap_or_else(|error| panic!("{error}"))
                .with_inline_timestamps(TimestampMetadata {
                    accessed: Some(stamp),
                    modified: None,
                    created: Some(stamp),
                });
            Ok(NamespaceResult::Entries(vec![descriptor]))
        }
    }

    let storage = Storage::connected(
        BackendIdentity::new(BackendKind::Cifs, "ndx-walk-undated")?,
        namespace_only_capabilities()?,
        None,
        None,
        Some(Arc::new(UndatedNamespace)),
        None,
        None,
    )?;
    let (pages, errors) = drain(&ndx_walk(&storage, request(StoragePath::root())?)?).await;
    assert!(pages.is_empty(), "nothing is emitted: {pages:?}");
    assert_eq!(errors.len(), 1, "an absent modified field is refused too");
    Ok(())
}

#[tokio::test]
async fn a_broken_session_stops_the_walk_instead_of_listing_every_remaining_directory() -> Result {
    let mut listings = sample_tree();
    listings.retain(|(key, _)| *key != "a");
    listings.push(("a", Listing::SessionFailure));
    let namespace = TreeNamespace::new(listings);
    let storage = storage(Arc::clone(&namespace))?;
    let walk_request = request(StoragePath::root())?;
    let cancel = walk_request.cancel.clone();
    let (_, errors) = drain(&ndx_walk(&storage, walk_request)?).await;

    assert!(
        errors.iter().any(|error| error.contains("session failed")),
        "the session failure is reported: {errors:?}"
    );
    assert!(
        cancel.is_cancelled(),
        "a dead session ends the walk; the driver has no cancel channel, so the readers must \
         stop answering with backend calls"
    );
    let listed = namespace.listed();
    assert!(
        !listed.contains(&"a/deep".to_owned()),
        "no directory is listed after the session died: {listed:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_cancelled_listing_is_a_signal_that_stops_the_walk_without_looking_like_io_failure()
-> Result {
    let mut listings = sample_tree();
    listings.retain(|(key, _)| *key != "a");
    listings.push(("a", Listing::Cancelled));
    let namespace = TreeNamespace::new(listings);
    let storage = storage(Arc::clone(&namespace))?;
    let walk_request = request(StoragePath::root())?;
    let cancel = walk_request.cancel.clone();
    let (_, errors) = drain(&ndx_walk(&storage, walk_request)?).await;

    assert!(
        errors.iter().any(|error| error.contains("cancelled")),
        "cancellation is reported as cancellation, not as a listing failure: {errors:?}"
    );
    assert!(
        !errors.iter().any(|error| error.contains("failed to list")),
        "R10: a cancellation must not be dressed up as an I/O error: {errors:?}"
    );
    assert!(cancel.is_cancelled(), "the walk winds down");
    Ok(())
}

#[tokio::test]
async fn a_pre_cancelled_token_lists_nothing_at_all() -> Result {
    let namespace = TreeNamespace::new(sample_tree());
    let storage = storage(Arc::clone(&namespace))?;
    let mut walk_request = request(StoragePath::root())?;
    walk_request.cancel = CancellationToken::new();
    walk_request.cancel.cancel();
    let (pages, errors) = drain(&ndx_walk(&storage, walk_request)?).await;

    assert!(pages.is_empty(), "nothing is emitted: {pages:?}");
    assert!(!errors.is_empty(), "the cancellation is visible");
    assert!(
        namespace.listed().is_empty(),
        "not one backend listing is issued"
    );
    Ok(())
}

#[tokio::test]
async fn a_match_expression_admits_only_what_it_names() -> Result {
    let namespace = TreeNamespace::new(sample_tree());
    let storage = storage(Arc::clone(&namespace))?;
    let mut walk_request = request(StoragePath::root())?;
    walk_request.match_expressions =
        Some(crate::filter::parse_filter_expression("name == \"y.txt\"")?);
    let (pages, _) = drain(&ndx_walk(&storage, walk_request)?).await;
    let emitted: Vec<String> = pages
        .iter()
        .flat_map(|page| page.files.iter())
        .map(|entry| name_of(&entry.entry))
        .collect();
    assert_eq!(
        emitted,
        vec!["y.txt"],
        "only the named file is emitted, and it is nested so the walk had to descend"
    );
    Ok(())
}

#[tokio::test]
async fn a_filtered_out_directory_is_descended_without_appearing_in_its_parent_page() -> Result {
    let namespace = TreeNamespace::new(sample_tree());
    let storage = storage(Arc::clone(&namespace))?;
    let mut walk_request = request(StoragePath::root())?;
    walk_request.match_expressions =
        Some(crate::filter::parse_filter_expression("name == \"y.txt\"")?);
    let (pages, _) = drain(&ndx_walk(&storage, walk_request)?).await;

    let root_page = pages.iter().find(|page| page.dir_path.is_empty());
    assert!(
        root_page.is_none_or(|page| page
            .subdirs
            .iter()
            .all(|entry| name_of(&entry.entry) != "a")),
        "'a' does not match, so it is hidden from the page"
    );
    assert!(
        namespace.listed().contains(&"a/deep".to_owned()),
        "but it is still descended into, or 'y.txt' could never be found: {:?}",
        namespace.listed()
    );
    Ok(())
}

#[tokio::test]
async fn emitted_observations_carry_the_listed_facts_and_no_metadata_round_trip() -> Result {
    let namespace = TreeNamespace::new(vec![(
        "",
        Listing::Entries(vec![
            ("archive.tar.gz", EntryKind::File),
            ("nested", EntryKind::Directory),
        ]),
    )]);
    let storage = storage(Arc::clone(&namespace))?;
    let (pages, _) = drain(&ndx_walk(&storage, request(StoragePath::root())?)?).await;

    let file = &pages[0].files[0].entry;
    assert_eq!(file.path().as_str(), "archive.tar.gz");
    assert_eq!(file.kind(), EntryKind::File);
    assert_eq!(file.size(), Some(3));
    assert_eq!(
        file.modified().map(StorageTimestamp::unix_nanos),
        Some(MODIFIED_NANOS),
        "the modification time comes from the listing, not the epoch"
    );
    assert_eq!(
        file.metadata()
            .timestamps()
            .value()
            .and_then(|v| v.modified),
        file.modified(),
        "the observation's metadata carries the same listed timestamps"
    );
    assert!(
        namespace.non_list_requests().is_empty(),
        "building the observation issues no extra namespace request"
    );
    Ok(())
}

#[tokio::test]
async fn extension_conditions_use_path_extension_semantics() -> Result {
    let namespace = TreeNamespace::new(vec![(
        "",
        Listing::Entries(vec![
            ("archive.tar.gz", EntryKind::File),
            (".bashrc", EntryKind::File),
            ("plain", EntryKind::File),
        ]),
    )]);
    let storage = storage(Arc::clone(&namespace))?;
    let mut walk_request = request(StoragePath::root())?;
    walk_request.match_expressions = Some(crate::filter::parse_filter_expression(
        "extension == \"gz\"",
    )?);
    let (pages, _) = drain(&ndx_walk(&storage, walk_request)?).await;
    let emitted: Vec<String> = pages
        .iter()
        .flat_map(|page| page.files.iter())
        .map(|entry| name_of(&entry.entry))
        .collect();
    // `.bashrc` has a stem and no extension, and `plain` has neither: only the last component
    // after a dot counts, which is `Path::extension` semantics rather than the legacy CIFS
    // walker's `rsplit_once('.')` (that one reported `Some("bashrc")`).
    assert_eq!(emitted, vec!["archive.tar.gz"]);
    Ok(())
}

#[tokio::test]
async fn a_storage_without_a_namespace_role_is_refused_before_any_listing() -> Result {
    let absent =
        CapabilityAvailability::Unsupported(UnsupportedReason::new("S3 lends no namespace role")?);
    let capabilities =
        BackendCapabilities::new(absent.clone(), absent.clone(), absent.clone(), absent);
    let storage = Storage::connected(
        BackendIdentity::new(BackendKind::S3, "ndx-walk-no-namespace")?,
        capabilities,
        None,
        None,
        None,
        None,
        None,
    )?;
    assert!(
        ndx_walk(&storage, request(StoragePath::root())?).is_err(),
        "a storage without a namespace role is refused up front"
    );
    Ok(())
}

#[tokio::test]
async fn an_undescribable_child_is_reported_while_its_siblings_are_still_paged() -> Result {
    let namespace = TreeNamespace::new(vec![(
        "",
        Listing::Partial(vec![("ok.txt", EntryKind::File)]),
    )]);
    let storage = storage(Arc::clone(&namespace))?;
    let (pages, errors) = drain(&ndx_walk(&storage, request(StoragePath::root())?)?).await;
    assert_eq!(pages.len(), 1);
    assert_eq!(page_names(&pages[0]), ["ok.txt"]);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0].contains("failed to describe an entry"),
        "{errors:?}"
    );
    Ok(())
}
