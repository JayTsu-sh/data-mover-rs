//! Role-based entry point for the operations that have no `StorageEnum` equivalent:
//! filtered / depth-limited traversal, recursive delete, and cross-endpoint integrity.
//!
//! Local lends no namespace role, so `delete-tree` needs a NAS backend; `traverse` and
//! `compare` work anywhere.
//!
//! ```text
//! cargo run --example storage_role_operations -- traverse --backend local --root /tmp/tree \
//!     --match 'name == "*.log"' --max-depth 2
//! cargo run --example storage_role_operations -- delete-tree --backend cifs --path scratch/old
//! cargo run --example storage_role_operations -- compare --backend local --root /tmp/a \
//!     --other-root /tmp/b --path object.bin --content
//! ```
use std::num::NonZeroUsize;
use std::sync::Arc;

use clap::{Parser, Subcommand, ValueEnum};
use data_mover::DslTraversalFilter;
use data_mover::integrity::{
    IntegrityMode, IntegrityOptions, IntegrityRequest, compare as compare_objects,
};
use data_mover::model::{
    BackendIdentity, BackendKind, ObservationMode, ObservationPlan, StoragePath,
};
use data_mover::storage::{
    BackendConfig, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy, DeleteTreeItem,
    DeleteTreeRequest, LocalBackendConfig, NfsBackendConfig, Storage, connect_backend, delete_tree,
};
use data_mover::traversal::{
    LocalTraversalSource, StorageTraversalSource, TraversalItem, TraversalOrder, TraversalRequest,
    TraversalSession, TraversalSource as _,
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
        /// Sub-path to start from, relative to the backend root.
        #[arg(long, default_value = "")]
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

/// Local lends no namespace role, so it uses its own sandboxed traversal; every other backend
/// composes the namespace and metadata roles.
fn open_traversal(
    storage: &Storage,
    args: &Args,
    request: TraversalRequest,
) -> Result<TraversalSession, Error> {
    Ok(match args.backend {
        Backend::Local => LocalTraversalSource::new(
            &args.root,
            BackendIdentity::new(BackendKind::Local, format!("source:{}", args.root))?,
        )?
        .traverse(request),
        Backend::Cifs | Backend::Nfs => StorageTraversalSource::new(storage)?.traverse(request),
    })
}

async fn traverse(storage: &Storage, args: &Args, command: &Command) -> Result<(), Error> {
    let Command::Traverse {
        match_expression,
        exclude_expression,
        max_depth,
        path,
    } = command
    else {
        unreachable!("dispatched by the caller")
    };
    let filter =
        DslTraversalFilter::parse(match_expression.as_deref(), exclude_expression.as_deref())?;
    let slots = NonZeroUsize::new(args.concurrency).ok_or("concurrency must be non-zero")?;
    let request = TraversalRequest {
        root: StoragePath::new(path.clone())?,
        order: TraversalOrder::Admission,
        max_inflight_operations: slots,
        max_buffered_items: slots,
        observation_plan: ObservationPlan::default().with_timestamps(ObservationMode::InlineOnly),
        cancel: CancellationToken::new(),
        filter: (!filter.is_empty()).then(|| Arc::new(filter) as Arc<_>),
        max_depth: max_depth.and_then(NonZeroUsize::new),
    };
    let mut session = open_traversal(storage, args, request)?;
    let (mut entries, mut failures) = (0_u64, 0_u64);
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => {
                entries += 1;
                println!("{:?} {}", entry.kind(), entry.path());
            }
            TraversalItem::EntryFailure(error) => {
                failures += 1;
                eprintln!("entry failure {} {:?}", error.path(), error.class());
            }
        }
    }
    let outcome = session.finish().await?;
    println!("entries={entries} failures={failures} outcome={outcome:?}");
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
                eprintln!("delete failure {} {:?}", error.path(), error.class());
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
        Command::Traverse { .. } => traverse(&storage, &args, &args.command).await,
        Command::DeleteTree { .. } => remove_tree(&storage, &args, &args.command).await,
        Command::Compare { .. } => compare(storage, &args, &args.command).await,
    }
}
