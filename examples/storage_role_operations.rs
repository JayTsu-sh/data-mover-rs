//! Role-based entry point for the operations that have no `StorageEnum` equivalent:
//! filtered / depth-limited traversal, NDX paging, recursive delete, and cross-endpoint
//! integrity.
//!
//! Every backend here lends the namespace role, so all subcommands work on Local, NFS and CIFS.
//!
//! ```text
//! cargo run --example storage_role_operations -- traverse --backend local --root /tmp/tree \
//!     --match 'name == "*.log"' --max-depth 2
//! cargo run --example storage_role_operations -- ndx-walk --backend cifs --path scratch \
//!     --entries
//! cargo run --example storage_role_operations -- delete-tree --backend cifs --path scratch/old
//! cargo run --example storage_role_operations -- compare --backend local --root /tmp/a \
//!     --other-root /tmp/b --path object.bin --content
//! ```
use std::num::NonZeroUsize;
use std::sync::Arc;

use clap::{Parser, Subcommand, ValueEnum};
use data_mover::DslTraversalFilter;
use data_mover::dir_tree::{NdxEntry, NdxEvent};
use data_mover::filter::parse_filter_expression;
use data_mover::integrity::{
    IntegrityMode, IntegrityOptions, IntegrityRequest, compare as compare_objects,
};
use data_mover::model::{
    BackendIdentity, BackendKind, ObservationMode, ObservationPlan, ObservedEntry, StoragePath,
    StorageTimestamp,
};
use data_mover::ndx_walk::{NdxWalkRequest, ndx_walk};
use data_mover::storage::{
    BackendConfig, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy, DeleteTreeItem,
    DeleteTreeRequest, LocalBackendConfig, NfsBackendConfig, Storage, connect_backend,
    create_directory_all, delete_tree,
};
use data_mover::transfer::{InflightLimits, TransferIdentity, TransferRequest, transfer};
use data_mover::traversal::{
    StorageTraversalSource, TraversalItem, TraversalOrder, TraversalRequest, TraversalSource as _,
};
use tokio_util::sync::CancellationToken;

type Error = Box<dyn std::error::Error>;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Backend {
    Local,
    Cifs,
    Nfs,
}

#[derive(Debug, Parser)]
#[command(about = "Exercise the role-based traversal, delete, and integrity operations")]
struct Args {
    #[arg(long, value_enum, default_value = "local")]
    backend: Backend,

    /// Local directory, or the NFS/CIFS sub-path used as the backend root.
    #[arg(long, default_value = "")]
    root: String,

    #[arg(long, default_value_t = 8)]
    concurrency: usize,

    #[command(subcommand)]
    command: Command,
}

/// How a traversal orders each directory's children and the descent into them.
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum OrderArg {
    /// Whatever order the backend listed in, which differs between backends.
    Admission,
    /// Sorted by the bytes of the final path component, descent included, so the same tree
    /// gives the same sequence on every backend.
    NameBytes,
}

impl From<OrderArg> for TraversalOrder {
    fn from(value: OrderArg) -> Self {
        match value {
            OrderArg::Admission => Self::Admission,
            OrderArg::NameBytes => Self::NameBytes,
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
        /// Sibling and descent order.
        #[arg(long, value_enum, default_value_t = OrderArg::Admission)]
        order: OrderArg,
        /// Sub-path to start from, relative to the backend root.
        #[arg(long, default_value = "")]
        path: String,
        /// Count items instead of printing one line each, so a timing run measures the
        /// traversal rather than this example's formatting.
        #[arg(long)]
        quiet: bool,
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
    /// to work on. Each file is transferred from a local temporary directory.
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

async fn connect(backend: Backend, root: &str, side: &str, depth: usize) -> Result<Storage, Error> {
    let slots = NonZeroUsize::new(depth).ok_or("concurrency must be non-zero")?;
    let config = match backend {
        Backend::Local => BackendConfig::Local(LocalBackendConfig {
            root: root.into(),
            identity: BackendIdentity::new(BackendKind::Local, format!("{side}:{root}"))?,
            read_concurrency: slots,
            write_concurrency: slots,
        }),
        Backend::Nfs => BackendConfig::Nfs(NfsBackendConfig {
            url: std::env::var("NFS_REAL_URL")?,
            identity: BackendIdentity::new(BackendKind::Nfs, format!("{side}:{root}"))?,
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
            guest_policy: CifsGuestPolicy::default(),
            identity: BackendIdentity::new(BackendKind::Cifs, format!("{side}:{root}"))?,
        }),
    };
    Ok(connect_backend(config).await?)
}

async fn traverse(storage: &Storage, args: &Args, command: &Command) -> Result<(), Error> {
    let Command::Traverse {
        match_expression,
        exclude_expression,
        max_depth,
        order,
        path,
        quiet,
    } = command
    else {
        unreachable!("dispatched by the caller")
    };
    let filter =
        DslTraversalFilter::parse(match_expression.as_deref(), exclude_expression.as_deref())?;
    let slots = NonZeroUsize::new(args.concurrency).ok_or("concurrency must be non-zero")?;
    let request = TraversalRequest {
        root: StoragePath::new(path.clone())?,
        order: TraversalOrder::from(*order),
        max_inflight_operations: slots,
        max_buffered_items: slots,
        observation_plan: ObservationPlan::default().with_timestamps(ObservationMode::InlineOnly),
        cancel: CancellationToken::new(),
        filter: (!filter.is_empty()).then(|| Arc::new(filter) as Arc<_>),
        max_depth: max_depth.and_then(NonZeroUsize::new),
    };
    let mut session = StorageTraversalSource::new(storage)?.traverse(request);
    let (mut entries, mut failures) = (0_u64, 0_u64);
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => {
                entries += 1;
                if !*quiet {
                    println!("{:?} {}", entry.kind(), entry.path());
                }
            }
            TraversalItem::EntryFailure(error) => {
                failures += 1;
                eprintln!("entry failure {} {:?}", error.path(), error.class());
            }
            TraversalItem::DirectoryListed(listed) if !*quiet => {
                println!(
                    "listed {} {:?} order={:?} pruned={} truncated={}",
                    listed.path,
                    listed.listing,
                    listed.child_order,
                    listed.pruned_children,
                    listed.truncated_children
                );
            }
            TraversalItem::SubtreeComplete(complete)
                if !*quiet || complete.path.as_str() == path =>
            {
                println!(
                    "subtree {} exhaustive={} {:?}",
                    complete.path,
                    complete.summary.is_exhaustive(),
                    complete.summary
                );
            }
            // `TraversalItem` is `#[non_exhaustive]`: a later item kind must not break this.
            _ => {}
        }
    }
    let outcome = session.finish().await?;
    println!("entries={entries} failures={failures} outcome={outcome:?}");
    Ok(())
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
    // creates every missing level idempotently.
    if !path.is_empty() {
        create_directory_all(storage, &StoragePath::new(path.clone())?).await?;
    }
    let local_root = tempfile::tempdir()?;
    let payload = vec![b'x'; *bytes];
    let slots = NonZeroUsize::new(args.concurrency).ok_or("concurrency must be non-zero")?;
    let source = connect(
        Backend::Local,
        &local_root.path().to_string_lossy(),
        "seed",
        args.concurrency,
    )
    .await?;
    let inflight = InflightLimits::new(slots.get(), (*bytes).max(1) * slots.get(), slots.get())?;
    for index in 0..*files {
        let name = format!("file-{index}.log");
        std::fs::write(local_root.path().join(&name), &payload)?;
        let destination = if path.is_empty() {
            name.clone()
        } else {
            format!("{path}/{name}")
        };
        let outcome = transfer(TransferRequest::new(
            TransferIdentity::new(format!("seed-{}-{index}", std::process::id()))?,
            source.clone(),
            StoragePath::new(name)?,
            storage.clone(),
            StoragePath::new(&destination)?,
            inflight,
            CancellationToken::new(),
        ))
        .await?;
        println!("seeded {destination} bytes={}", outcome.transferred_bytes);
    }
    Ok(())
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
    let destination = connect(args.backend, other_root, "destination", args.concurrency).await?;
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
    let storage = connect(args.backend, &args.root, "source", args.concurrency).await?;
    match &args.command {
        Command::Seed { .. } => seed(&storage, &args, &args.command).await,
        Command::Traverse { .. } => traverse(&storage, &args, &args.command).await,
        Command::NdxWalk { .. } => ndx_walk_pages(&storage, &args, &args.command).await,
        Command::CreateDir { .. } => make_directory(&storage, &args.command).await,
        Command::DeleteTree { .. } => remove_tree(&storage, &args, &args.command).await,
        Command::Compare { .. } => compare(storage, &args, &args.command).await,
    }
}
