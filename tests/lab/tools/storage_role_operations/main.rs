//! Role-based entry point for traversal, directory operations and comparison:
//! filtered / depth-limited traversal, directory blocks, recursive delete, and cross-endpoint
//! integrity.
//!
//! Every backend here lends the namespace role, so all subcommands work on Local, NFS and CIFS.
//! S3 supports traversal, comparison and current-object deletion; on a
//! versioned bucket each key is one entry with its history, a `Record` line per version and
//! delete marker (`--current` prints the current objects only) — and `seed` writes objects
//! without creating directories; `create-dir` refuses virtual prefixes. Deletion retains stored
//! versions and delete markers. Its URL (with the prefix as the root) comes from `S3_LISTING_URL`.
//!
//! ```text
//! cargo run --example storage_role_operations -- --backend local --root /tmp/tree traverse \
//!     --match 'name == "*.log"' --max-depth 2
//! S3_LISTING_URL=s3://AK:SK@bucket.host:9000/prefix cargo run --example \
//!     storage_role_operations -- --backend s3 traverse --order blocks
//! cargo run --example storage_role_operations -- --backend cifs traverse --path scratch --order blocks
//! cargo run --example storage_role_operations -- --backend cifs delete-tree --path scratch/old
//! cargo run --example storage_role_operations -- --backend local --root /tmp/a compare \
//!     --other-root /tmp/b --path object.bin --content
//! ```
use std::fmt::Write as _;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::pin::pin;

use clap::{Parser, Subcommand, ValueEnum};
use data_mover::integrity::{
    IntegrityMode, IntegrityOptions, IntegrityRequest, compare as compare_objects,
};
use data_mover::model::{BackendKind, StoragePath};
use data_mover::storage::{
    BackendConfig, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy, DeleteTreeItem,
    DeleteTreeRequest, LocalBackendConfig, NfsBackendConfig, S3BackendConfig, Storage,
    connect_backend, create_directory_all, delete_tree,
};
use data_mover::transfer::{TransferIdentity, TransferRequest, transfer};
use data_mover::traversal::TraversalOrder;
use futures::stream::{self, StreamExt as _};
use tokio_util::sync::CancellationToken;

mod traverse;

use traverse::traverse;

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
        /// Print the current objects only: of a key with its history (a versioned S3 bucket), the
        /// entry when its latest record is a version — as a listing of current objects shows it,
        /// no history, no `Record` lines — and nothing for a key deleted now.
        #[arg(long)]
        current: bool,
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
        /// script sees a directory object's bytes (`File d/ size=5`). A key with its history
        /// prints its current object's size (`-` when deleted); its `Record` lines carry each
        /// record's.
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
        /// Emit a checkpoint about every this many seconds (P7 §9; 0 = at every chance), and one
        /// more as the last item when cancelled. Cancelling then drains what was produced.
        #[arg(long)]
        checkpoint_every: Option<f64>,
        /// Write each checkpoint to this file, replacing it atomically, after flushing every entry
        /// printed before it — so the file is never ahead of what was printed.
        #[arg(long)]
        checkpoint_out: Option<PathBuf>,
        /// Resume from the checkpoint in this file (written by `--checkpoint-out` of a traversal
        /// with the same backend, root, path, order, filter and depth).
        #[arg(long)]
        resume_from: Option<PathBuf>,
        /// Cancel once this many entries have been printed, then drain.
        #[arg(long)]
        cancel_after_entries: Option<u64>,
        /// Cancel after this many seconds, then drain.
        #[arg(long)]
        cancel_after_secs: Option<f64>,
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
            rename_context: None,
            read_chunk_bytes: None,
            write_chunk_bytes: None,
            root: root.into(),
            read_concurrency: slots,
            write_concurrency: slots,
        }),
        Backend::Nfs => BackendConfig::Nfs(NfsBackendConfig {
            transfer_concurrency: None,
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
    let seeding = SeedFiles {
        source: &source,
        storage,
        path,
        local_root: local_root.path(),
        payload: &payload,
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

    async fn transfer(&self, index: usize, _name: String, destination: &str) -> Result<u64, Error> {
        let local_path = self.local_root.join(destination);
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(local_path, self.payload)?;
        let outcome = transfer(
            TransferRequest::new(
                self.source.clone(),
                StoragePath::new(destination)?,
                self.storage.clone(),
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
        plan: data_mover::integrity::IntegrityPlan::default(),
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
        Command::CreateDir { .. } => make_directory(&storage, &args.command).await,
        Command::DeleteTree { .. } => remove_tree(&storage, &args, &args.command).await,
        Command::Compare { .. } => compare(storage, &args, &args.command).await,
    }
}
