//! Real Local-backend contract probe for independently mounted filesystems.
//! Run `before` and `after` in separate processes around a full unmount/mount.
//! The checkpoint must live outside the mount. No server configuration is changed.
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt::Write as _;
use std::fs;
use std::num::NonZeroUsize;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};

use clap::{Parser, ValueEnum};
use data_mover::model::{
    EntryKind, FailureClass, ObservationPlan, ObservedEntry, RenameContext, RenameEvidence,
    RenamePolicy, RenameScope, StoragePath,
};
use data_mover::storage::{
    BackendConfig, LocalBackendConfig, ReadRequest, Storage, StorageRoleFailure, connect_backend,
};
use data_mover::transfer::{TransferPhase, TransferPolicy, TransferRequest, transfer};
use data_mover::traversal::{
    StorageTraversalSource, TraversalItem, TraversalOutcome, TraversalRequest, TraversalSource,
};
use tokio_util::sync::CancellationToken;

type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Phase {
    Before,
    After,
}
#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    checkpoint: PathBuf,
    #[arg(long)]
    scope: String,
    #[arg(long, value_enum)]
    phase: Phase,
    /// Assert birth-time evidence exists (CIFS); otherwise assert it is absent (NFS).
    #[arg(long)]
    expect_evidence: bool,
    /// Only enable on an independently verified stable-inode mount, e.g. serverino.
    #[arg(long)]
    allow_heuristic: bool,
}

fn context(args: &Args) -> Result<RenameContext> {
    Ok(RenameContext::new(
        RenameScope::new(&args.scope)?,
        if args.allow_heuristic {
            RenamePolicy::AllowHeuristic
        } else {
            RenamePolicy::CandidatesOnly
        },
    ))
}
async fn local(root: &Path, context: RenameContext) -> Result<Storage> {
    Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: root.to_owned(),
        read_concurrency: NonZeroUsize::MIN,
        write_concurrency: NonZeroUsize::MIN,
        read_chunk_bytes: None,
        write_chunk_bytes: None,
        rename_context: Some(context),
    }))
    .await?)
}
fn hex(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(value, "{byte:02x}");
    }
    value
}
fn evidence(entry: &ObservedEntry) -> Option<String> {
    entry.rename_evidence().map(|value| hex(value.as_bytes()))
}
async fn scan(storage: &Storage) -> Result<BTreeMap<String, ObservedEntry>> {
    let source = StorageTraversalSource::new(storage)?;
    let mut session = source.traverse(TraversalRequest::new(StoragePath::root()));
    let mut entries = BTreeMap::new();
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => {
                entries.insert(entry.path().as_str().to_owned(), *entry);
            }
            TraversalItem::DirectoryListed(_) => {}
            other => return Err(format!("unexpected traversal result: {other:?}").into()),
        }
    }
    assert!(matches!(
        session.finish().await?,
        TraversalOutcome::Completed(_)
    ));
    Ok(entries)
}
// Example downstream consumer: count evidence across each complete scan before pairing.
// Duplicate tokens stay ambiguous even if a later entry happens to have another kind.
fn unique(
    entries: &BTreeMap<String, ObservedEntry>,
) -> HashMap<&RenameEvidence, Option<&ObservedEntry>> {
    let mut indexed = HashMap::new();
    for entry in entries.values() {
        if let Some(evidence) = entry.rename_evidence() {
            indexed
                .entry(evidence)
                .and_modify(|entry| *entry = None)
                .or_insert(Some(entry));
        }
    }
    indexed
}
fn agrees(
    old_kind: EntryKind,
    new_kind: EntryKind,
    old: &RenameEvidence,
    new: &RenameEvidence,
) -> bool {
    old_kind == new_kind && old == new
}
fn pairs<'a, 'b>(
    old_context: &RenameContext,
    new_context: &RenameContext,
    old: &'a BTreeMap<String, ObservedEntry>,
    new: &'b BTreeMap<String, ObservedEntry>,
) -> Vec<(&'a ObservedEntry, &'b ObservedEntry)> {
    if !old_context.permits_matching(new_context) {
        return Vec::new();
    }
    let new_index = unique(new);
    unique(old)
        .into_iter()
        .filter_map(|(evidence, old)| {
            let old = old?;
            let new = new_index.get(evidence).copied().flatten()?;
            (agrees(old.kind(), new.kind(), evidence, new.rename_evidence()?)
                && old.path() != new.path())
            .then_some((old, new))
        })
        .collect()
}
async fn consistent(storage: &Storage, name: &str, present: bool) -> Result<ObservedEntry> {
    let path = StoragePath::new(name)?;
    let listed = scan(storage)
        .await?
        .remove(name)
        .ok_or("file absent from scan")?;
    let stat = storage.namespace()?.stat(&path).await?;
    let described = storage.read_source()?.describe(&path).await?;
    assert_eq!(listed.rename_evidence(), stat.rename_evidence());
    assert_eq!(listed.rename_evidence(), described.rename_evidence());
    assert_eq!(listed.source_identity(), &stat.source_identity);
    assert_eq!(listed.source_identity(), &described.source_identity);
    assert_eq!(listed.rename_evidence().is_some(), present);
    let snapshot = listed.encode_snapshot();
    let restored = ObservedEntry::decode_snapshot(snapshot.as_bytes(), storage.identity())?;
    assert_eq!(restored.rename_evidence(), listed.rename_evidence());
    assert_eq!(restored.encode_snapshot().as_bytes(), snapshot.as_bytes());
    println!(
        "PASS stat/list/traversal/describe/snapshot path={name} evidence={:?}",
        evidence(&listed)
    );
    Ok(listed)
}
fn conflict<T>(result: &std::result::Result<T, StorageRoleFailure>) -> bool {
    matches!(result, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Conflict)
}
async fn runtime_replacement(storage: &Storage, root: &Path) -> Result {
    fs::write(root.join("replace.txt"), b"original")?;
    let path = StoragePath::new("replace.txt")?;
    let descriptor = storage.read_source()?.describe(&path).await?;
    fs::rename(root.join("replace.txt"), root.join("retained-original.txt"))?;
    fs::write(root.join("replace.txt"), b"replacement")?;
    let read = storage
        .read_source()?
        .read(ReadRequest {
            path: path.clone(),
            range: None,
            expected_source: Some(descriptor.source_identity.clone()),
            maximum_chunk_bytes: 4096,
            read_inflight: 1,
            read_budget: None,
            cancel: CancellationToken::new(),
            source_qos: None,
            version: descriptor.version().clone(),
        })
        .await;
    assert!(
        conflict(&read),
        "replacement read must reject stale identity"
    );
    let observed = storage
        .metadata()?
        .observe_bound(
            &path,
            &descriptor.source_identity,
            ObservationPlan::default(),
        )
        .await;
    assert!(
        conflict(&observed),
        "replacement metadata must reject stale identity"
    );
    println!("PASS runtime source replacement read + observe_bound");
    Ok(())
}
#[cfg(unix)]
fn same_inode(first: &Path, second: &Path) -> Result<bool> {
    let a = fs::metadata(first)?;
    let b = fs::metadata(second)?;
    Ok(a.dev() == b.dev() && a.ino() == b.ino())
}
#[cfg(not(unix))]
fn same_inode(_first: &Path, _second: &Path) -> Result<bool> {
    Ok(false)
}
async fn reject_direct_alias(source: Storage, target: Storage, original: &Path) -> Result {
    let result = transfer(
        TransferRequest::new(
            source,
            StoragePath::new("file.bin")?,
            target,
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Direct),
    )
    .await;
    let failure = result.err().ok_or("Direct alias must be rejected")?;
    assert_eq!(failure.phase(), TransferPhase::Prepare);
    let role = Error::source(&failure).and_then(|cause| cause.downcast_ref::<StorageRoleFailure>());
    assert!(
        matches!(role, Some(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Conflict)
    );
    assert_eq!(fs::read(original)?, b"keep source intact");
    println!("PASS Direct hardlink alias rejected before truncation");
    Ok(())
}
async fn aliases(args: &Args, ctx: &RenameContext) -> Result {
    let src = args.root.join("alias-source");
    let dst = args.root.join("alias-target");
    fs::create_dir(&src)?;
    fs::create_dir(&dst)?;
    fs::write(src.join("file.bin"), b"keep source intact")?;
    fs::hard_link(src.join("file.bin"), dst.join("file.bin"))?;
    let same = same_inode(&src.join("file.bin"), &dst.join("file.bin"))?;
    if args.allow_heuristic || !args.expect_evidence {
        assert!(same, "stable-inode profile must recognize hardlinks");
        reject_direct_alias(
            local(&src, *ctx).await?,
            local(&dst, *ctx).await?,
            &src.join("file.bin"),
        )
        .await?;
    } else {
        println!(
            "LIMITATION unstable-inode profile hardlink_same_inode={same}; Direct hardlink safety cannot be guaranteed by dev+ino"
        );
    }
    Ok(())
}
struct RenameCase {
    original_scan: BTreeMap<String, ObservedEntry>,
    renamed_scan: BTreeMap<String, ObservedEntry>,
    renamed: ObservedEntry,
}
async fn rename_case(args: &Args, ctx: &RenameContext, storage: &Storage) -> Result<RenameCase> {
    fs::write(args.root.join("original.txt"), b"before")?;
    let original = consistent(storage, "original.txt", args.expect_evidence).await?;
    let original_scan = scan(storage).await?;
    fs::create_dir(args.root.join("nested"))?;
    fs::rename(
        args.root.join("original.txt"),
        args.root.join("nested/renamed.txt"),
    )?;
    fs::write(
        args.root.join("nested/renamed.txt"),
        b"after rename with changed content",
    )?;
    let renamed = consistent(storage, "nested/renamed.txt", args.expect_evidence).await?;
    if args.allow_heuristic {
        assert_eq!(original.rename_evidence(), renamed.rename_evidence());
    }
    let renamed_scan = scan(storage).await?;
    let matched = pairs(ctx, ctx, &original_scan, &renamed_scan);
    assert_eq!(
        matched.len(),
        usize::from(args.allow_heuristic && args.expect_evidence)
    );
    if let Some((old, new)) = matched.first() {
        assert_ne!(old.size(), new.size());
    }
    println!("PASS unique same-kind rename candidates={}", matched.len());
    assert_ne!(original.size(), renamed.size());
    assert_eq!(
        fs::read(args.root.join("nested/renamed.txt"))?,
        b"after rename with changed content"
    );
    println!(
        "PASS rename + independent content change original={:?} renamed={:?}",
        evidence(&original),
        evidence(&renamed)
    );
    Ok(RenameCase {
        original_scan,
        renamed_scan,
        renamed,
    })
}
async fn copy_changed(args: &Args, ctx: &RenameContext) -> Result {
    let copy_root = args.root.join("content-copy");
    fs::create_dir(&copy_root)?;
    transfer(
        TransferRequest::new(
            local(&args.root.join("nested"), *ctx).await?,
            StoragePath::new("renamed.txt")?,
            local(&copy_root, *ctx).await?,
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Direct),
    )
    .await?;
    assert_eq!(
        fs::read(copy_root.join("renamed.txt"))?,
        b"after rename with changed content"
    );
    println!("PASS changed content independently transferred and read back");
    Ok(())
}
async fn reject_ambiguous(
    args: &Args,
    ctx: &RenameContext,
    storage: &Storage,
    case: &RenameCase,
) -> Result {
    fs::hard_link(
        args.root.join("nested/renamed.txt"),
        args.root.join("hardlink.txt"),
    )?;
    let entries = scan(storage).await?;
    let occurrences = entries
        .values()
        .filter(|entry| {
            case.renamed.rename_evidence().is_some()
                && entry.rename_evidence() == case.renamed.rename_evidence()
        })
        .count();
    if args.allow_heuristic {
        assert_eq!(occurrences, 2);
    }
    assert!(pairs(ctx, ctx, &case.original_scan, &entries).is_empty());
    println!(
        "PASS hardlink ambiguity falls back: candidate_count={occurrences} usable_unique={}",
        occurrences == 1 && args.allow_heuristic
    );
    // Exercise the consumer predicate with a colliding token and a different kind.
    if let Some(token) = case.renamed.rename_evidence() {
        assert!(!agrees(EntryKind::File, EntryKind::Directory, token, token));
        println!("PASS simulated same-token file/directory collision rejected");
    }
    let other = RenameContext::new(
        RenameScope::new(format!("{}-different-dataset", args.scope))?,
        RenamePolicy::AllowHeuristic,
    );
    assert!(!ctx.permits_matching(&other));
    assert!(pairs(ctx, &other, &case.original_scan, &case.renamed_scan).is_empty());
    assert_eq!(ctx.permits_matching(ctx), args.allow_heuristic);
    println!("PASS scope mismatch + policy gate + independent entry kind check");
    Ok(())
}
#[cfg(unix)]
fn device(path: &Path) -> Result<u64> {
    Ok(fs::metadata(path)?.dev())
}
#[cfg(not(unix))]
fn device(_path: &Path) -> Result<u64> {
    Ok(0)
}
async fn checkpoint(args: &Args, ctx: &RenameContext, storage: &Storage) -> Result {
    fs::write(args.root.join("remount.txt"), b"remount evidence")?;
    let remount = consistent(storage, "remount.txt", args.expect_evidence).await?;
    let dev = device(&args.root.join("remount.txt"))?;
    let checkpoint = serde_json::json!({"scope":args.scope,"evidence":evidence(&remount),
        "runtime":hex(remount.identity_key().as_bytes()),"dev":dev,"fingerprint":hex(&ctx.fingerprint())});
    fs::write(&args.checkpoint, serde_json::to_vec_pretty(&checkpoint)?)?;
    println!("PASS before checkpoint_saved dev={dev} all_handles_close_on_process_exit");
    Ok(())
}
async fn before(args: &Args, ctx: &RenameContext, storage: &Storage) -> Result {
    let case = rename_case(args, ctx, storage).await?;
    copy_changed(args, ctx).await?;
    reject_ambiguous(args, ctx, storage, &case).await?;
    runtime_replacement(storage, &args.root).await?;
    aliases(args, ctx).await?;
    checkpoint(args, ctx, storage).await
}
async fn after(args: &Args, ctx: &RenameContext, storage: &Storage) -> Result {
    let old: serde_json::Value = serde_json::from_slice(&fs::read(&args.checkpoint)?)?;
    assert_eq!(old["scope"].as_str(), Some(args.scope.as_str()));
    assert_eq!(
        old["fingerprint"],
        serde_json::json!(hex(&ctx.fingerprint()))
    );
    fs::rename(
        args.root.join("remount.txt"),
        args.root.join("remounted-renamed.txt"),
    )?;
    let entry = consistent(storage, "remounted-renamed.txt", args.expect_evidence).await?;
    let candidate_same = old["evidence"] == serde_json::json!(evidence(&entry));
    let dev = device(&args.root.join("remounted-renamed.txt"))?;
    if args.allow_heuristic {
        assert!(candidate_same);
    }
    let usable = ctx.permits_matching(ctx) && candidate_same && entry.rename_evidence().is_some();
    assert_eq!(usable, args.allow_heuristic && args.expect_evidence);
    println!(
        "PASS full-remount dev_before={} dev_after={dev} candidate_same={candidate_same} policy_allows={} usable={usable} runtime_key_same={}",
        old["dev"],
        ctx.permits_matching(ctx),
        old["runtime"] == serde_json::json!(hex(entry.identity_key().as_bytes()))
    );
    Ok(())
}
#[tokio::main]
async fn main() -> Result {
    let args = Args::parse();
    let ctx = context(&args)?;
    let storage = local(&args.root, ctx).await?;
    match args.phase {
        Phase::Before => before(&args, &ctx, &storage).await?,
        Phase::After => after(&args, &ctx, &storage).await?,
    }
    println!("RESULT phase={:?} PASS", args.phase);
    Ok(())
}
