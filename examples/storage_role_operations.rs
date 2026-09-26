//! Role-based entry point for the operations that have no `StorageEnum` equivalent:
//! filtered / depth-limited traversal, NDX paging, recursive delete, and cross-endpoint
//! integrity.
//!
//! Every backend here lends the namespace role, so all subcommands work on Local, NFS and CIFS.
//! S3 lends `Stat` / `List` only (ADR-0006 C22): `traverse` and `compare` work on it, `--versions
//! all` lists every stored version and delete marker, and `seed` writes objects without creating
//! directories; `ndx-walk`, `create-dir` and `delete-tree` refuse it before any I/O. Its URL
//! (with the prefix as the root) comes from `S3_LISTING_URL`.
//!
//! ```text
//! cargo run --example storage_role_operations -- traverse --backend local --root /tmp/tree \
//!     --match 'name == "*.log"' --max-depth 2
//! S3_LISTING_URL=s3://AK:SK@bucket.host:9000/prefix cargo run --example \
//!     storage_role_operations -- --backend s3 traverse --versions all --order blocks
//! cargo run --example storage_role_operations -- ndx-walk --backend cifs --path scratch \
//!     --entries
//! cargo run --example storage_role_operations -- delete-tree --backend cifs --path scratch/old
//! cargo run --example storage_role_operations -- compare --backend local --root /tmp/a \
//!     --other-root /tmp/b --path object.bin --content
//! ```
use std::fmt::Write as _;
use std::num::NonZeroUsize;
use std::path::Path;
use std::pin::pin;
use std::sync::Arc;

use clap::{Parser, Subcommand, ValueEnum};
use data_mover::DslTraversalFilter;
use data_mover::dir_tree::{NdxEntry, NdxEvent};
use data_mover::filter::parse_filter_expression;
use data_mover::integrity::{
    IntegrityMode, IntegrityOptions, IntegrityRequest, compare as compare_objects,
};
use data_mover::model::{
    BackendKind, HistoryOrder, MetadataObservation, ObservationMode, ObservationPlan,
    ObservedEntry, StoragePath, StorageTimestamp,
};
use data_mover::ndx_walk::{NdxWalkRequest, ndx_walk};
use data_mover::storage::{
    BackendConfig, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy, DeleteTreeItem,
    DeleteTreeRequest, LocalBackendConfig, NfsBackendConfig, S3BackendConfig, Storage,
    connect_backend, create_directory_all, delete_tree,
};
use data_mover::transfer::{InflightLimits, TransferIdentity, TransferRequest, transfer};
use data_mover::traversal::{
    BlockCounts, ScanTotals, StorageTraversalSource, TraversalItem, TraversalOrder,
    TraversalOutcome, TraversalRequest, TraversalSource as _, TraversalVersions,
};
use futures::stream::{self, StreamExt as _};
use tokio_util::sync::CancellationToken;

type Error = Box<dyn std::error::Error>;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Backend {
    Local,
    Cifs,
    Nfs,
    S3,
}

#[derive(Debug, Parser)]
#[command(about = "Exercise the role-based traversal, delete, and integrity operations")]
struct Args {
    #[arg(long, value_enum, default_value = "local")]
    backend: Backend,

    /// Local directory, or the NFS/CIFS sub-path used as the backend root (S3: the URL's
    /// prefix, so this stays empty).
    #[arg(long, default_value = "")]
    root: String,

    #[arg(long, default_value_t = 8)]
    concurrency: usize,

    #[command(subcommand)]
    command: Command,
}

/// How a traversal orders what it delivers.
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum OrderArg {
    /// No order but each directory's entry ahead of everything below it; no directory events.
    /// The fastest, and the default.
    Unordered,
    /// Each directory's children together in the order the backend listed them, then its
    /// subdirectories descended into by name bytes, so the same tree visits its directories in
    /// the same order on every backend.
    Blocks,
}

/// Which entries a traversal emits for a versioned store.
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum VersionsArg {
    /// Current objects only.
    Current,
    /// Every stored version and delete marker, oldest first per key (S3 only).
    All,
}

impl From<VersionsArg> for TraversalVersions {
    fn from(value: VersionsArg) -> Self {
        match value {
            VersionsArg::Current => Self::Current,
            VersionsArg::All => Self::All,
        }
    }
}

impl From<OrderArg> for TraversalOrder {
    fn from(value: OrderArg) -> Self {
        match value {
            OrderArg::Unordered => Self::Unordered,
            OrderArg::Blocks => Self::Blocks,
        }
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Stream a filtered, depth-limited traversal.
    Traverse {
        #[arg(long = "match")]
        match_expression: Option<String>,
        #[arg(long = "exclude")]
        exclude_expression: Option<String>,
        #[arg(long)]
        max_depth: Option<usize>,
        /// Unordered (the default), or blocks with directory events and a fixed descent order.
        #[arg(long, value_enum, default_value_t = OrderArg::Unordered)]
        order: OrderArg,
        /// Current objects only, or every stored version (S3).
        #[arg(long, value_enum, default_value_t = VersionsArg::Current)]
        versions: VersionsArg,
        /// Sub-path to start from, relative to the backend root.
        #[arg(long, default_value = "")]
        path: String,
        /// Count items instead of printing one line each, so a timing run measures the
        /// traversal rather than this example's formatting.
        #[arg(long)]
        quiet: bool,
        /// Also sum the size of every entry's encoded snapshot (what a caller persists per entry).
        #[arg(long)]
        snapshot_bytes: bool,
        /// Also print each entry's size (` size=<bytes>`, `-` for none) after its path, so a
        /// script sees a directory object's bytes (`File d/ size=5`) in the current listing too.
        /// A version entry already prints its size with its version.
        #[arg(long)]
        sizes: bool,
        /// Also print each entry's identity strength and the first 8 bytes of its identity key
        /// (hex), e.g. to see that two objects with the same content at two keys differ.
        #[arg(long)]
        identity: bool,
        /// Also ask for numeric ownership and print each entry's owner, group and mode (or why
        /// there is none). NFS and Local answer it from the listing itself.
        #[arg(long)]
        ownership: bool,
        /// Also ask for the ACL, best effort, and print what its observation was (not the ACL).
        /// Where the mount or filesystem can read ACLs this sends every entry through the
        /// metadata role, so a run with and one without `--acl` compare ownership read from the
        /// listing against a per-entry GETATTR/lstat; `acl=Value…` or `acl=Failed…` on the
        /// output shows the role really was asked (`Unsupported` could come from either).
        #[arg(long)]
        acl: bool,
    },
    /// Stream an NDX-paged depth-first traversal, printing one line per page.
    NdxWalk {
        #[arg(long = "match")]
        match_expression: Option<String>,
        #[arg(long = "exclude")]
        exclude_expression: Option<String>,
        #[arg(long)]
        max_depth: Option<usize>,
        /// Sub-path to start from, relative to the backend root.
        #[arg(long, default_value = "")]
        path: String,
        /// Also print every entry with its NDX and mode.
        #[arg(long)]
        entries: bool,
    },
    /// Write a small fixture tree under the root, so the other subcommands have something
    /// to work on. Each file is transferred from a local temporary directory, `--concurrency`
    /// transfers at a time.
    Seed {
        /// Sub-path under the root to populate.
        #[arg(long, default_value = "")]
        path: String,
        #[arg(long, default_value_t = 3)]
        files: usize,
        #[arg(long, default_value_t = 4096)]
        bytes: usize,
    },
    /// Create every missing component of a path, idempotently.
    CreateDir {
        #[arg(long)]
        path: String,
    },
    /// Delete a subtree, reporting progress per entry.
    DeleteTree {
        #[arg(long)]
        path: String,
        /// Also remove the directory named by `--path`.
        #[arg(long)]
        delete_root: bool,
    },
    /// Compare one object against a second endpoint of the same backend kind.
    Compare {
        #[arg(long)]
        other_root: String,
        #[arg(long)]
        path: String,
        /// Read both objects and compare BLAKE3 digests, not just metadata.
        #[arg(long)]
        content: bool,
    },
}

async fn connect(backend: Backend, root: &str, depth: usize) -> Result<Storage, Error> {
    let slots = NonZeroUsize::new(depth).ok_or("concurrency must be non-zero")?;
    let config = match backend {
        Backend::Local => BackendConfig::Local(LocalBackendConfig {
            root: root.into(),
            read_concurrency: slots,
            write_concurrency: slots,
        }),
        Backend::Nfs => BackendConfig::Nfs(NfsBackendConfig {
            url: std::env::var("NFS_REAL_URL")?,
            block_size: None,
            ensure_dir: false,
        }),
        Backend::Cifs => BackendConfig::Cifs(CifsBackendConfig {
            server: std::env::var("CIFS_REAL_SERVER")?,
            share: std::env::var("CIFS_REAL_SHARE")?,
            root: (!root.is_empty()).then(|| root.to_owned()),
            ensure_dir: true,
            username: std::env::var("CIFS_REAL_USER")?,
            password: std::env::var("CIFS_REAL_PASS")?,
            signing_policy: CifsSigningPolicy::default(),
            // Same opt-in the `cifs_*` tests use, so a guest share can be reached without
            // relaxing the default for everyone.
            guest_policy: if std::env::var("CIFS_REAL_GUEST_POLICY")
                .is_ok_and(|value| value == "allow-unsigned")
            {
                CifsGuestPolicy::AllowUnsigned
            } else {
                CifsGuestPolicy::default()
            },
        }),
        Backend::S3 => {
            if !root.is_empty() {
                return Err("S3 takes its root from the URL's prefix; leave --root empty".into());
            }
            BackendConfig::S3(S3BackendConfig {
                url: std::env::var("S3_LISTING_URL")?,
                block_size: None,
                single_put_threshold: None,
            })
        }
    };
    Ok(connect_backend(config).await?)
}

async fn traverse(storage: &Storage, args: &Args, command: &Command) -> Result<(), Error> {
    let Command::Traverse {
        quiet,
        snapshot_bytes,
        sizes,
        identity,
        ownership,
        acl,
        ..
    } = command
    else {
        unreachable!("dispatched by the caller")
    };
    let mut session =
        StorageTraversalSource::new(storage)?.traverse(traversal_request(args, command)?);
    let (mut entries, mut failures, mut snapshots) = (0_u64, 0_u64, 0_u64);
    while let Some(item) = session.next_item().await {
        match &item {
            TraversalItem::Entry(entry) => {
                entries += 1;
                if *snapshot_bytes {
                    snapshots += entry.encode_snapshot().as_bytes().len() as u64;
                }
                if !*quiet {
                    let identity = if *identity {
                        identity_of(entry)
                    } else {
                        String::new()
                    };
                    let mut owner = if *ownership {
                        ownership_of(entry)
                    } else {
                        String::new()
                    };
                    if *acl {
                        owner.push_str(&acl_of(entry));
                    }
                    let size = if *sizes && entry.version().is_none() {
                        entry
                            .size()
                            .map_or_else(|| " size=-".to_owned(), |size| format!(" size={size}"))
                    } else {
                        String::new()
                    };
                    println!(
                        "{:?} {}{size}{}{identity}{owner}",
                        entry.kind(),
                        entry.path(),
                        version_of(entry)
                    );
                }
            }
            TraversalItem::Failure(failure) => {
                failures += 1;
                eprintln!(
                    "failure scope={:?} {} {:?}",
                    failure.scope,
                    failure.path,
                    failure.failure.class()
                );
            }
            TraversalItem::DirectoryListed(listed) if !*quiet => {
                println!(
                    "listed {} {:?} {}",
                    listed.path,
                    listed.outcome,
                    block_counts(&listed.counts)
                );
            }
            // `TraversalItem` is `#[non_exhaustive]`: a later item kind must not break this.
            _ => {}
        }
    }
    let outcome = session.finish().await?;
    println!("entries={entries} failures={failures} outcome={outcome:?}");
    if let TraversalOutcome::Completed(completion) = &outcome {
        println!("{}", totals(&completion.totals));
    }
    if *snapshot_bytes {
        println!(
            "snapshot_bytes={snapshots} endpoint_bytes={}",
            storage.identity().stable_id().len()
        );
    }
    Ok(())
}

fn traversal_request(args: &Args, command: &Command) -> Result<TraversalRequest, Error> {
    let Command::Traverse {
        match_expression,
        exclude_expression,
        max_depth,
        order,
        versions,
        path,
        ownership,
        acl,
        ..
    } = command
    else {
        unreachable!("dispatched by the caller")
    };
    let mut plan = ObservationPlan::default().with_timestamps(ObservationMode::InlineOnly);
    if *ownership {
        plan = plan.with_ownership_mode(ObservationMode::InlineOnly);
    }
    if *acl {
        plan = plan.with_acl(ObservationMode::BestEffort);
    }
    let filter =
        DslTraversalFilter::parse(match_expression.as_deref(), exclude_expression.as_deref())?;
    let slots = NonZeroUsize::new(args.concurrency).ok_or("concurrency must be non-zero")?;
    let request = TraversalRequest::new(StoragePath::new(path.clone())?)
        .order(TraversalOrder::from(*order))
        .max_inflight_operations(slots)
        .max_buffered_items(slots)
        .observation_plan(plan)
        .max_depth(max_depth.and_then(NonZeroUsize::new))
        .versions(TraversalVersions::from(*versions));
    Ok(if filter.is_empty() {
        request
    } else {
        request.filter(Arc::new(filter))
    })
}

/// One directory's counts as `key=value` pairs, for scripts to parse.
fn block_counts(counts: &BlockCounts) -> String {
    format!(
        "balanced={} listed={} emitted={} filtered={} failed={} vanished={} hidden={} pruned={} \
         truncated={} bytes={} files={} dirs={} symlinks={} other={}",
        counts.is_balanced(),
        counts.listed,
        counts.emitted,
        counts.filtered,
        counts.failed_children,
        counts.vanished,
        counts.internal_hidden,
        counts.pruned_dirs,
        counts.truncated_dirs,
        counts.bytes,
        counts.files,
        counts.directories,
        counts.symlinks,
        counts.other
    )
}

/// The whole traversal's counts as `totals key=value …`, for scripts to parse.
fn totals(totals: &ScanTotals) -> String {
    format!(
        "totals balanced={} listed={} emitted={} filtered={} failed={} vanished={} hidden={} \
         listing_failures={} interrupted={} vanished_listings={} pruned={} truncated={} bytes={} \
         files={} dirs={} symlinks={} other={}",
        totals.is_balanced(),
        totals.listed,
        totals.emitted,
        totals.filtered,
        totals.failed_children,
        totals.vanished,
        totals.internal_hidden,
        totals.listing_failures,
        totals.interrupted_listings,
        totals.vanished_listings,
        totals.pruned_dirs,
        totals.truncated_dirs,
        totals.bytes,
        totals.files,
        totals.directories,
        totals.symlinks,
        totals.other
    )
}

/// ` version=<id|null> latest=<0|1> marker=<0|1> size=<n|->` for an entry of a traversal of
/// every version, then ` order=inferred` when its key's history was ordered by `LastModified`
/// rather than by the server's listing order; nothing otherwise.
fn version_of(entry: &ObservedEntry) -> String {
    entry.version().map_or_else(String::new, |version| {
        format!(
            " version={} latest={} marker={} size={}{}",
            version.id().unwrap_or("null"),
            u8::from(version.is_latest()),
            u8::from(version.is_delete_marker()),
            entry
                .size()
                .map_or_else(|| "-".to_string(), |size| size.to_string()),
            if version.order() == HistoryOrder::Inferred {
                " order=inferred"
            } else {
                ""
            }
        )
    })
}

/// ` owner=<uid>:<gid>:<mode, octal>/<provenance>`, or the observation that stands in for it.
fn ownership_of(entry: &ObservedEntry) -> String {
    match entry.metadata().ownership_mode() {
        MetadataObservation::Value { value, provenance } => format!(
            " owner={}:{}:{:o}/{provenance:?}",
            value.uid, value.gid, value.mode
        ),
        other => format!(" owner={other:?}"),
    }
}

/// ` acl=<what the observation was>`, without the ACL itself.
fn acl_of(entry: &ObservedEntry) -> String {
    let observed = match entry.metadata().acl() {
        MetadataObservation::Value { provenance, .. } => format!("Value/{provenance:?}"),
        MetadataObservation::NotRequested => "NotRequested".to_owned(),
        MetadataObservation::NotApplicable => "NotApplicable".to_owned(),
        MetadataObservation::Unsupported => "Unsupported".to_owned(),
        MetadataObservation::Failed { class, .. } => format!("Failed/{class:?}"),
    };
    format!(" acl={observed}")
}

/// ` identity=<strength>:<first 8 bytes of the identity key, hex>`.
fn identity_of(entry: &ObservedEntry) -> String {
    let key = entry.identity_key().as_bytes()[..8]
        .iter()
        .fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        });
    format!(" identity={:?}:{key}", entry.source_identity().strength())
}

async fn ndx_walk_pages(storage: &Storage, args: &Args, command: &Command) -> Result<(), Error> {
    let Command::NdxWalk {
        match_expression,
        exclude_expression,
        max_depth,
        path,
        entries: print_entries,
    } = command
    else {
        unreachable!("dispatched by the caller")
    };
    let walk = ndx_walk(
        storage,
        NdxWalkRequest {
            root: StoragePath::new(path.clone())?,
            max_depth: max_depth.and_then(NonZeroUsize::new),
            match_expressions: match_expression
                .as_deref()
                .map(parse_filter_expression)
                .transpose()?,
            exclude_expressions: exclude_expression
                .as_deref()
                .map(parse_filter_expression)
                .transpose()?,
            concurrency: NonZeroUsize::new(args.concurrency)
                .ok_or("concurrency must be non-zero")?,
            cancel: CancellationToken::new(),
        },
    )?;
    let (mut pages, mut counted, mut failures) = (0_u64, 0_u64, 0_u64);
    while let Some(event) = walk.next().await {
        match event {
            NdxEvent::Page(page) => {
                pages += 1;
                counted += page.files.len() as u64 + page.subdirs.len() as u64;
                println!(
                    "page dir='{}' ndx_start={} files={} subdirs={} gap={}",
                    page.dir_path,
                    page.ndx_start,
                    page.files.len(),
                    page.subdirs.len(),
                    page.gap_ndx
                );
                if *print_entries {
                    for entry in page.files.iter().chain(page.subdirs.iter()) {
                        print_ndx_entry(entry);
                    }
                }
            }
            NdxEvent::Error { path, reason } => {
                failures += 1;
                eprintln!("page error {path}: {reason}");
            }
            NdxEvent::Done => break,
        }
    }
    println!("pages={pages} entries={counted} errors={failures}");
    Ok(())
}

fn print_ndx_entry(entry: &NdxEntry<ObservedEntry>) {
    println!(
        "  ndx={} {} kind={:?} size={:?} modified={:?} identity={:?}",
        entry.ndx,
        entry.entry.path(),
        entry.entry.kind(),
        entry.entry.size(),
        entry.entry.modified().map(StorageTimestamp::unix_nanos),
        entry.entry.source_identity().strength()
    );
}

async fn seed(storage: &Storage, args: &Args, command: &Command) -> Result<(), Error> {
    let Command::Seed { path, files, bytes } = command else {
        unreachable!("dispatched by the caller")
    };
    // Staged prepare creates the file itself but not its parents; the role-layer helper
    // creates every missing level idempotently. S3 has no directories to create.
    if !path.is_empty() && storage.kind() != BackendKind::S3 {
        create_directory_all(storage, &StoragePath::new(path.clone())?).await?;
    }
    let local_root = tempfile::tempdir()?;
    let payload = vec![b'x'; *bytes];
    let slots = NonZeroUsize::new(args.concurrency).ok_or("concurrency must be non-zero")?;
    let source = connect(
        Backend::Local,
        &local_root.path().to_string_lossy(),
        args.concurrency,
    )
    .await?;
    let inflight = InflightLimits::new(slots.get(), (*bytes).max(1) * slots.get(), slots.get())?;
    let seeding = SeedFiles {
        source: &source,
        storage,
        path,
        local_root: local_root.path(),
        payload: &payload,
        inflight,
    };
    seeding.all(*files, slots.get()).await
}

/// Failed files after which `seed` stops trying the rest.
const MAX_SEED_FAILURES: usize = 100;

/// `error` and every error beneath it, `: `-separated.
fn chain(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(next) = cause {
        let _ = write!(text, ": {next}");
        cause = next.source();
    }
    text
}

/// What seeding one file needs.
struct SeedFiles<'a> {
    source: &'a Storage,
    storage: &'a Storage,
    path: &'a str,
    local_root: &'a Path,
    payload: &'a [u8],
    inflight: InflightLimits,
}

impl SeedFiles<'_> {
    /// Seeds `files` files, `concurrency` transfers at a time, so a wide directory seeds in
    /// reasonable time. A file that fails is reported and the rest still seeded, so a flaky server
    /// does not cut a large fixture short; any failure still fails the command, and a run where
    /// everything fails gives up after `MAX_SEED_FAILURES`.
    async fn all(&self, files: usize, concurrency: usize) -> Result<(), Error> {
        let mut seeded = pin!(
            stream::iter(0..files)
                .map(|index| self.one(index))
                .buffer_unordered(concurrency)
        );
        let mut failed = 0_usize;
        while let Some(result) = seeded.next().await {
            match result {
                Ok((destination, bytes)) => println!("seeded {destination} bytes={bytes}"),
                Err((destination, error)) => {
                    failed += 1;
                    eprintln!("seed failure {destination}: {}", chain(error.as_ref()));
                    if failed >= MAX_SEED_FAILURES {
                        return Err(format!("gave up after {failed} failed files").into());
                    }
                }
            }
        }
        if failed > 0 {
            return Err(format!("{failed} of {files} files failed to seed").into());
        }
        Ok(())
    }

    /// Writes `file-<index>.log` locally and transfers it below the root; a failure comes back
    /// with the destination it was for.
    async fn one(&self, index: usize) -> Result<(String, u64), (String, Error)> {
        let name = format!("file-{index}.log");
        let destination = if self.path.is_empty() {
            name.clone()
        } else {
            format!("{}/{name}", self.path)
        };
        self.transfer(index, name, &destination)
            .await
            .map(|bytes| (destination.clone(), bytes))
            .map_err(|error| (destination, error))
    }

    async fn transfer(&self, index: usize, name: String, destination: &str) -> Result<u64, Error> {
        std::fs::write(self.local_root.join(&name), self.payload)?;
        let outcome = transfer(
            TransferRequest::new(
                self.source.clone(),
                StoragePath::new(name)?,
                self.storage.clone(),
                StoragePath::new(destination)?,
                self.inflight,
                CancellationToken::new(),
            )
            .with_identity_override(TransferIdentity::from_label(format!(
                "seed-{}-{index}",
                std::process::id()
            ))?),
        )
        .await?;
        Ok(outcome.transferred_bytes)
    }
}

async fn make_directory(storage: &Storage, command: &Command) -> Result<(), Error> {
    let Command::CreateDir { path } = command else {
        unreachable!("dispatched by the caller")
    };
    create_directory_all(storage, &StoragePath::new(path.clone())?).await?;
    println!("created {path}");
    Ok(())
}

async fn remove_tree(storage: &Storage, args: &Args, command: &Command) -> Result<(), Error> {
    let Command::DeleteTree { path, delete_root } = command else {
        unreachable!("dispatched by the caller")
    };
    let slots = NonZeroUsize::new(args.concurrency).ok_or("concurrency must be non-zero")?;
    let mut session = delete_tree(
        storage,
        DeleteTreeRequest {
            root: StoragePath::new(path.clone())?,
            delete_root: *delete_root,
            max_inflight_operations: slots,
            max_buffered_items: slots,
            cancel: CancellationToken::new(),
        },
    )?;
    while let Some(item) = session.next_item().await {
        match item {
            DeleteTreeItem::Deleted { path, kind } => println!("deleted {kind:?} {path}"),
            DeleteTreeItem::EntryFailure(error) => {
                eprintln!(
                    "delete failure {} {:?}: {}",
                    error.path(),
                    error.class(),
                    error.diagnostic()
                );
            }
        }
    }
    println!("outcome={:?}", session.finish().await?);
    Ok(())
}

async fn compare(storage: Storage, args: &Args, command: &Command) -> Result<(), Error> {
    let Command::Compare {
        other_root,
        path,
        content,
    } = command
    else {
        unreachable!("dispatched by the caller")
    };
    let destination = connect(args.backend, other_root, args.concurrency).await?;
    let report = compare_objects(IntegrityRequest {
        source: storage,
        source_path: StoragePath::new(path.clone())?,
        destination,
        destination_path: StoragePath::new(path.clone())?,
        options: IntegrityOptions {
            mode: if *content {
                IntegrityMode::Content
            } else {
                IntegrityMode::Metadata
            },
            ..IntegrityOptions::default()
        },
        cancel: CancellationToken::new(),
    })
    .await?;
    if report.matches() {
        println!("match compared_bytes={}", report.compared_bytes());
    } else {
        for difference in report.differences() {
            println!("difference {difference:?}");
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let storage = connect(args.backend, &args.root, args.concurrency).await?;
    match &args.command {
        Command::Seed { .. } => seed(&storage, &args, &args.command).await,
        Command::Traverse { .. } => traverse(&storage, &args, &args.command).await,
        Command::NdxWalk { .. } => ndx_walk_pages(&storage, &args, &args.command).await,
        Command::CreateDir { .. } => make_directory(&storage, &args.command).await,
        Command::DeleteTree { .. } => remove_tree(&storage, &args, &args.command).await,
        Command::Compare { .. } => compare(storage, &args, &args.command).await,
    }
}
