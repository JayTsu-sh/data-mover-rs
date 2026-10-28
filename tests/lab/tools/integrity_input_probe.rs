//! Real-backend integrity probe. The caller seeds the same key in both endpoints.

use std::error::Error;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use data_mover::integrity::{
    IntegrityDifference, IntegrityIgnored, IntegrityInput, IntegrityMode, IntegrityOptions,
    IntegrityPlan, IntegrityRequest, compare,
};
use data_mover::model::{ObservedEntry, StoragePath};
use data_mover::storage::Storage;
use data_mover::traversal::{
    StorageTraversalSource, TraversalItem, TraversalRequest, TraversalSource as _,
};
use tokio_util::sync::CancellationToken;

#[path = "../../../examples/support/endpoint.rs"]
#[allow(dead_code)]
mod endpoint;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum InputKind {
    Path,
    Observed,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Expected {
    Match,
    Mismatch,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ExpectedAcl {
    Equal,
    Different,
    Ignored,
}

#[derive(Parser)]
#[allow(clippy::struct_excessive_bools)] // Independent command-line switches.
struct Args {
    /// Endpoint URL; `INTEGRITY_SOURCE_URL` is an alternative for credential-bearing URLs.
    #[arg(long)]
    source: Option<String>,
    /// Endpoint URL; `INTEGRITY_DESTINATION_URL` is an alternative for credential-bearing URLs.
    #[arg(long)]
    destination: Option<String>,
    /// One direct child of both endpoint roots.
    #[arg(long)]
    key: String,
    #[arg(long, value_enum)]
    input: InputKind,
    #[arg(long, value_enum)]
    expected: Expected,
    #[arg(long, value_enum, requires = "acl")]
    expect_acl: Option<ExpectedAcl>,
    /// Require both observed entries to carry an S3 version history.
    #[arg(long)]
    require_both_histories: bool,
    /// Require the source observed entry to carry an S3 version history.
    #[arg(long)]
    require_source_history: bool,
    /// Compare native ACLs when supported.
    #[arg(long)]
    acl: bool,
    /// Require complete native ACL observations.
    #[arg(long)]
    acl_required: bool,
    /// Assert a shared numeric identity/group namespace and compare regular-file access.
    #[arg(long)]
    acl_equivalent: bool,
    /// Compare extended attributes when supported.
    #[arg(long)]
    xattrs: bool,
    /// For a version-pinning probe, write <path>.ready after observation and wait for
    /// <path>.resume before comparing, so the caller can publish a newer version.
    #[arg(long)]
    pause_file: Option<PathBuf>,
}

async fn observed(storage: &Storage, key: &StoragePath) -> Result<ObservedEntry, Box<dyn Error>> {
    let request = TraversalRequest::new(StoragePath::root())
        .max_depth(NonZeroUsize::new(1))
        .cancel(CancellationToken::new());
    let mut session = StorageTraversalSource::new(storage)?.traverse(request);
    let mut found = None;
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) if entry.path() == key => {
                let previous = found.replace(*entry);
                if previous.is_some() {
                    return Err("duplicate observed entry".into());
                }
            }
            TraversalItem::Failure(failure) => {
                return Err(format!("traversal failure: {failure:?}").into());
            }
            _ => {}
        }
    }
    session.finish().await?;
    found.ok_or_else(|| format!("{} was not observed", key.as_str()).into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let key = StoragePath::new(&args.key)?;
    if key.as_str().contains('/') {
        return Err("--key must be one direct child of the endpoint root".into());
    }
    let source_url = args
        .source
        .or_else(|| std::env::var("INTEGRITY_SOURCE_URL").ok())
        .ok_or("--source or INTEGRITY_SOURCE_URL is required")?;
    let destination_url = args
        .destination
        .or_else(|| std::env::var("INTEGRITY_DESTINATION_URL").ok())
        .ok_or("--destination or INTEGRITY_DESTINATION_URL is required")?;
    let source = endpoint::connect(&source_url).await?;
    let destination = endpoint::connect(&destination_url).await?;
    let (source_input, destination_input) = match args.input {
        InputKind::Path => (
            IntegrityInput::Path(key.clone()),
            IntegrityInput::Path(key.clone()),
        ),
        InputKind::Observed => {
            let source_entry = observed(&source, &key).await?;
            let destination_entry = observed(&destination, &key).await?;
            if args.require_source_history && source_entry.history().is_none() {
                return Err("source observation has no version history".into());
            }
            if args.require_both_histories
                && (source_entry.history().is_none() || destination_entry.history().is_none())
            {
                return Err("both observations must have version histories".into());
            }
            println!(
                "observed source={:?} destination={:?}",
                source_entry.source_version(),
                destination_entry.source_version()
            );
            (source_entry.into(), destination_entry.into())
        }
    };
    if let Some(pause_file) = &args.pause_file {
        if !matches!(args.input, InputKind::Observed) {
            return Err("--pause-file requires --input observed".into());
        }
        let ready = pause_file.with_extension("ready");
        let resume = pause_file.with_extension("resume");
        std::fs::write(&ready, b"ready")?;
        tokio::time::timeout(Duration::from_mins(1), async {
            while !resume.exists() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await?;
    }
    let result = compare(IntegrityRequest {
        source,
        source_path: source_input,
        destination,
        destination_path: destination_input,
        plan: IntegrityPlan {
            acl: args.acl || args.acl_required || args.acl_equivalent,
            xattrs: args.xattrs,
            acl_required: args.acl_required || args.acl_equivalent,
            acl_equivalent: args.acl_equivalent,
        },
        options: IntegrityOptions {
            mode: IntegrityMode::Content,
            ..IntegrityOptions::default()
        },
        cancel: CancellationToken::new(),
    })
    .await?;
    let matches = result.matches();
    println!(
        "matches={matches} bytes={} differences={:?} ignored={:?} acl_scope={:?}",
        result.compared_bytes(),
        result.differences(),
        result.ignored(),
        result.acl_scope()
    );
    if matches != matches!(args.expected, Expected::Match) {
        return Err("integrity outcome differed from expectation".into());
    }
    if let Some(expected_acl) = args.expect_acl {
        check_acl(&result, expected_acl)?;
    }
    Ok(())
}

fn check_acl(
    result: &data_mover::integrity::IntegrityReport,
    expected_acl: ExpectedAcl,
) -> Result<(), Box<dyn Error>> {
    let different = result.differences().iter().any(|difference| {
        matches!(
            difference,
            IntegrityDifference::Acl { .. } | IntegrityDifference::AclUnverified { .. }
        )
    });
    let ignored = result
        .ignored()
        .iter()
        .any(|fact| matches!(fact, IntegrityIgnored::Acl { .. }));
    let correct = match expected_acl {
        ExpectedAcl::Equal => !different && !ignored,
        ExpectedAcl::Different => different && !ignored,
        ExpectedAcl::Ignored => ignored && !different,
    };
    if !correct {
        return Err("ACL outcome differed from expectation".into());
    }
    Ok(())
}
