//! What one listed entry costs the traversal: end to end through the driver, on a tree whose
//! listings are prebuilt so the fake backend itself costs next to nothing.
//!
//! Timing, not correctness, so ignored by default. Run it on an otherwise idle machine with
//!
//! ```text
//! cargo test --release --lib traversal::storage::tests::bench -- --ignored --nocapture
//! ```
//!
//! CPU time is read from `/proc/self/stat`, so the numbers are Linux-only; elsewhere the test
//! reports wall time alone.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::super::observe::{Listed, observe_listed};
use super::*;
use crate::model::{ObservationMode, StorageTimestamp, TimePrecision};

/// Listings built once, handed out by clone: the clone is what a real listing's construction
/// roughly costs, and it is the same before and after any change to the driver.
struct PrebuiltNamespace {
    listings: HashMap<StoragePath, Vec<SourceDescriptor>>,
}

#[async_trait]
impl Namespace for PrebuiltNamespace {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        let NamespaceRequest::List(directory) = request else {
            return Err(StorageRoleFailure::Entry(entry_failure(
                &StoragePath::root(),
                FailureClass::Unsupported,
            )));
        };
        Ok(NamespaceResult::Entries(
            self.listings.get(&directory).cloned().unwrap_or_default(),
        ))
    }
}

/// An NFS-shaped child: a 32-byte handle, the three READDIRPLUS timestamps and a mode.
fn nfs_like(
    identity: &BackendIdentity,
    value: &str,
    kind: EntryKind,
    seed: u64,
) -> SourceDescriptor {
    let mut handle = [0_u8; 32];
    handle[..8].copy_from_slice(&seed.to_le_bytes());
    handle[8..16].copy_from_slice(&seed.rotate_left(17).to_le_bytes());
    let time = StorageTimestamp::new(
        1_700_000_000_000_000_000 + i128::from(seed),
        TimePrecision::Nanoseconds,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    let mut descriptor = SourceDescriptor::new(
        path(value),
        kind,
        (kind == EntryKind::File).then_some(seed),
        SourceIdentity::new(
            identity.clone(),
            IdentityStrength::StableWithinBackend,
            handle,
        )
        .unwrap_or_else(|error| panic!("{error}")),
    )
    .with_inline_timestamps(TimestampMetadata {
        accessed: Some(time),
        modified: Some(time),
        created: Some(time),
    });
    descriptor.inline_mode = Some(0o644);
    descriptor
}

/// `directories` directories of `files` files each, under the root.
fn tree(directories: usize, files: usize) -> PrebuiltNamespace {
    let identity = BackendIdentity::new(Kind::Nfs, "bench.example:/export")
        .unwrap_or_else(|error| panic!("{error}"));
    let mut seed = 0_u64;
    let mut next = || {
        seed += 1;
        seed
    };
    let mut listings = HashMap::new();
    let mut root = Vec::with_capacity(directories);
    for directory in 0..directories {
        let name = format!("dir-{directory:05}");
        root.push(nfs_like(&identity, &name, EntryKind::Directory, next()));
        let children = (0..files)
            .map(|file| {
                nfs_like(
                    &identity,
                    &format!("{name}/file-{file:05}.dat"),
                    EntryKind::File,
                    next(),
                )
            })
            .collect();
        listings.insert(path(&name), children);
    }
    listings.insert(StoragePath::root(), root);
    PrebuiltNamespace { listings }
}

/// User plus system CPU time of this process so far, when the platform says.
fn process_cpu() -> Option<Duration> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // The command name may hold spaces, so fields are counted after its closing parenthesis;
    // utime and stime are fields 14 and 15, in USER_HZ (100 on every Linux).
    let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    let ticks = fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?;
    Some(Duration::from_millis(ticks * 10))
}

async fn run_once(namespace: &Arc<PrebuiltNamespace>, plan: ObservationPlan) -> u64 {
    let source = StorageTraversalSource::with_roles(
        Arc::clone(namespace) as Arc<dyn Namespace>,
        Arc::new(FakeMetadata),
    );
    let mut session = source.traverse(TraversalRequest {
        root: StoragePath::root(),
        order: crate::traversal::TraversalOrder::Admission,
        max_inflight_operations: NonZeroUsize::new(64)
            .unwrap_or_else(|| unreachable!("constant is nonzero")),
        max_buffered_items: NonZeroUsize::new(1024)
            .unwrap_or_else(|| unreachable!("constant is nonzero")),
        observation_plan: plan,
        cancel: tokio_util::sync::CancellationToken::new(),
        filter: None,
        max_depth: None,
        versions: TraversalVersions::Current,
    });
    let mut entries = 0_u64;
    while let Some(item) = session.next_item().await {
        if matches!(item, TraversalItem::Entry(_)) {
            entries += 1;
        }
    }
    session
        .finish()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    entries
}

/// End to end, the default plan: nothing but listings reaches the backend, so everything
/// measured is the traversal's own per-entry work plus the consumer's receive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "timing, not correctness; run by hand with --release"]
async fn traversal_cpu_per_entry() {
    measure("listing only", ObservationPlan::default()).await;
}

/// End to end, a plan the listing cannot answer: every child goes through the metadata role,
/// which here answers at once, so what is measured is the cost of routing a child through it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "timing, not correctness; run by hand with --release"]
async fn traversal_cpu_per_entry_through_the_metadata_role() {
    measure(
        "metadata role",
        ObservationPlan::default().with_ownership_mode(ObservationMode::InlineOnly),
    )
    .await;
}

async fn measure(label: &str, plan: ObservationPlan) {
    const DIRECTORIES: usize = 2_000;
    const FILES: usize = 100;
    let namespace = Arc::new(tree(DIRECTORIES, FILES));
    let expected = u64::try_from(DIRECTORIES * (FILES + 1)).unwrap_or(u64::MAX);
    assert_eq!(run_once(&namespace, plan).await, expected);
    for round in 0..3 {
        let (cpu, wall) = (process_cpu(), Instant::now());
        let entries = run_once(&namespace, plan).await;
        let wall = wall.elapsed();
        assert_eq!(entries, expected);
        let per_entry = |spent: Duration| spent.as_nanos() / u128::from(entries);
        match cpu.zip(process_cpu()) {
            Some((before, after)) => println!(
                "{label} round {round}: {entries} entries, wall {wall:?} ({} ns/entry), cpu {:?} ({} ns/entry)",
                per_entry(wall),
                after.saturating_sub(before),
                per_entry(after.saturating_sub(before)),
            ),
            None => println!(
                "{label} round {round}: {entries} entries, wall {wall:?} ({} ns/entry)",
                per_entry(wall)
            ),
        }
    }
}

/// One child's assembly alone — identity key, metadata observations, the entry and its box —
/// on one thread, with no driver around it. The design estimate is 0.5–1.5 µs per entry.
#[test]
#[ignore = "timing, not correctness; run by hand with --release"]
fn assembly_cost_per_entry() {
    const ENTRIES: usize = 200_000;
    let namespace = tree(ENTRIES / 100, 99);
    let descriptors: Vec<SourceDescriptor> = namespace.listings.into_values().flatten().collect();
    let plan = ObservationPlan::default();
    for round in 0..3 {
        let batch = descriptors.clone();
        let count = batch.len();
        let started = Instant::now();
        let mut assembled = Vec::with_capacity(count);
        for descriptor in batch {
            match observe_listed(&FakeMetadata, descriptor, plan) {
                Listed::Observed(Ok(entry)) => assembled.push(entry),
                _ => panic!("a plain file needs no I/O"),
            }
        }
        let spent = started.elapsed();
        assert_eq!(assembled.len(), count);
        println!(
            "round {round}: {count} entries assembled in {spent:?} ({} ns/entry)",
            spent.as_nanos() / count as u128
        );
    }
}
