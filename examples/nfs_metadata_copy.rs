//! Copies one file with the ACL / xattr policies given on the command line and checks what the
//! negotiation did with them, against a real share. See `.claude/docs/metadata-negotiation.md`.
//!
//! ```text
//! # NFSv3 cannot read an ACL, so asking for one skips it with the reason and still copies the file
//! cargo run --example nfs_metadata_copy -- \
//!     --source 'nfs://10.131.5.221/m1-source:/?uid=0&gid=0&noresvport=true' --source-path d000/f000 \
//!     --destination /tmp/dm-meta --destination-path f000 \
//!     --acl --expect copied --expect-acl unsupported
//! ```
//!
//! An endpoint starting with `nfs://` is an NFS URL; `cifs:<sub-path>` is that sub-path of the
//! share named by `CIFS_REAL_SERVER` / `CIFS_REAL_SHARE` / `CIFS_REAL_USER` / `CIFS_REAL_PASS`
//! (the e2e-cifs `.env`); anything else is a local directory.

use std::env;
use std::error::Error;
use std::fs;
use std::num::NonZeroUsize;
use std::process;

use clap::{Parser, ValueEnum};
use data_mover::metadata::{ApplicationOutcome, MetadataApplicationReport, MetadataFamily};
use data_mover::model::{BackendIdentity, BackendKind, FailureClass, StoragePath};
use data_mover::storage::{
    BackendConfig, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy, LocalBackendConfig,
    NfsBackendConfig, PreflightPolicy, Storage, StorageRoleFailure, connect_backend,
};
use data_mover::transfer::{
    CopiedMetadataRequest, InflightLimits, TransferIdentity, TransferOutcome, TransferRequest,
    transfer,
};
use nfs_rs::{AceFlags, AceMask, AceType, Acl, Mount, NfsAce};
use tokio_util::sync::CancellationToken;

type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;

#[derive(Debug, Parser)]
#[command(about = "Copy one file with optional ACL / xattr policies and check the outcome")]
struct Args {
    #[arg(long)]
    source: String,
    #[arg(long)]
    source_path: String,
    /// Write this many bytes to the source path first, so a fresh share needs no fixture.
    #[arg(long)]
    seed_bytes: Option<usize>,
    #[arg(long)]
    destination: String,
    #[arg(long)]
    destination_path: String,
    /// Ask for the ACL to be carried.
    #[arg(long)]
    acl: bool,
    /// Ask for extended attributes to be carried.
    #[arg(long)]
    xattrs: bool,
    /// Whether the copy has to succeed or has to be refused.
    #[arg(long, value_enum)]
    expect: Expect,
    /// The ACL outcome the report has to show, when the copy succeeds.
    #[arg(long, value_enum)]
    expect_acl: Option<Outcome>,
    /// The xattr outcome the report has to show, when the copy succeeds.
    #[arg(long, value_enum)]
    expect_xattrs: Option<Outcome>,
    /// Before the copy, give the (`NFSv4`) source an ACL no mode could produce, so that a
    /// destination ACL equal to it proves the ACL was carried rather than recomputed. The mark
    /// stays on the source, so it is only allowed on a file this run has just seeded.
    #[arg(long, value_enum, requires = "seed_bytes")]
    mark_acl: Option<Mark>,
    /// After the copy, read both ACLs back over raw NFS and require them to be equal and marked.
    /// Without a mark both would be rebuilt from the same copied mode and compare equal anyway.
    #[arg(long, requires = "mark_acl")]
    verify_acl: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mark {
    /// Adds read access for a principal nothing else on the share refers to. Proves the ACL was
    /// carried, but survives a later chmod on servers that preserve ACLs across one (ONTAP's
    /// `v4-acl-preserve`, on by default), so it cannot tell the application order apart.
    Principal,
    /// Grants `EVERYONE@` `WRITE_ACL`, which mode bits cannot express. A chmod rewrites the
    /// `EVERYONE@` entry even when the rest of the ACL is preserved, so this one is lost if the
    /// mode is applied after the ACL.
    EveryoneWriteAcl,
}

const MARK_PRINCIPAL: &str = "12345";
const EVERYONE: &str = "EVERYONE@";

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Expect {
    Copied,
    Refused,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Outcome {
    Applied,
    OmittedByPolicy,
    NotObserved,
    Unsupported,
    Failed,
}

impl From<ApplicationOutcome> for Outcome {
    fn from(value: ApplicationOutcome) -> Self {
        match value {
            ApplicationOutcome::Applied | ApplicationOutcome::PreservedByNativeTransfer => {
                Self::Applied
            }
            ApplicationOutcome::OmittedByPolicy => Self::OmittedByPolicy,
            ApplicationOutcome::NotObserved => Self::NotObserved,
            ApplicationOutcome::Unsupported => Self::Unsupported,
            ApplicationOutcome::Failed => Self::Failed,
        }
    }
}

fn env_var(name: &str) -> Result<String> {
    env::var(name).map_err(|_| format!("{name} is not set (source the e2e-cifs .env)").into())
}

async fn connect(endpoint: &str, side: &str) -> Result<Storage> {
    let slots = NonZeroUsize::new(4).ok_or("non-zero")?;
    let config = if endpoint.starts_with("nfs://") {
        BackendConfig::Nfs(NfsBackendConfig {
            url: endpoint.to_owned(),
            identity: BackendIdentity::new(BackendKind::Nfs, side)?,
            block_size: None,
            ensure_dir: true,
        })
    } else if let Some(root) = endpoint.strip_prefix("cifs:") {
        BackendConfig::Cifs(CifsBackendConfig {
            server: env_var("CIFS_REAL_SERVER")?,
            share: env_var("CIFS_REAL_SHARE")?,
            root: (!root.is_empty()).then(|| root.to_owned()),
            ensure_dir: true,
            username: env_var("CIFS_REAL_USER")?,
            password: env_var("CIFS_REAL_PASS")?,
            signing_policy: CifsSigningPolicy::default(),
            guest_policy: CifsGuestPolicy::default(),
            identity: BackendIdentity::new(BackendKind::Cifs, side)?,
        })
    } else {
        fs::create_dir_all(endpoint)?;
        BackendConfig::Local(LocalBackendConfig {
            root: endpoint.into(),
            identity: BackendIdentity::new(BackendKind::Local, side)?,
            read_concurrency: slots,
            write_concurrency: slots,
        })
    };
    Ok(connect_backend(config).await?)
}

fn request(
    source: &Storage,
    source_path: &str,
    destination: &Storage,
    destination_path: &str,
) -> Result<TransferRequest> {
    Ok(TransferRequest::new(
        TransferIdentity::new(format!("metadata-copy-{}", process::id()))?,
        source.clone(),
        StoragePath::new(source_path)?,
        destination.clone(),
        StoragePath::new(destination_path)?,
        InflightLimits::new(4, 4 << 20, 4)?,
        CancellationToken::new(),
    ))
}

/// Writes a fresh file, refusing to overwrite one: `--seed-bytes` pointed at a real fixture tree
/// would otherwise replace it with filler.
async fn seed(source: &Storage, path: &str, bytes: usize) -> Result {
    let described = source
        .read_source(&PreflightPolicy::production())?
        .describe(&StoragePath::new(path)?)
        .await;
    match described {
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => {}
        Err(error) => return Err(error.into()),
        Ok(_) => return Err(format!("{path} already exists; seed a new path").into()),
    }
    let local = tempfile::tempdir()?;
    fs::write(local.path().join("seed"), vec![b'x'; bytes])?;
    let local = connect(&local.path().to_string_lossy(), "seed").await?;
    transfer(request(&local, "seed", source, path)?).await?;
    println!("seeded {path} bytes={bytes}");
    Ok(())
}

/// Splits `nfs://host/export:/sub?query` into a URL nfs-rs can mount and the path of `relative`
/// under it. The `:` backend root is our convention, not nfs-rs's; like `parse_nfs_url` it is
/// looked for only after the host, so a port or an IPv6 host is not mistaken for it.
fn raw_location(endpoint: &str, relative: &str) -> Result<(String, String)> {
    let rest = endpoint
        .strip_prefix("nfs://")
        .ok_or("ACL marking and verification need an nfs:// endpoint")?;
    let (rest, query) = rest.split_once('?').unwrap_or((rest, ""));
    // The backend defaults to root when uid/gid are absent and nfs-rs to the caller, so a raw
    // mount without them could see a different ACL than the copy did.
    if !(query.contains("uid=") && query.contains("gid=")) {
        return Err("ACL marking and verification need explicit uid= and gid= in the URL".into());
    }
    let (host, path) = rest.split_once('/').ok_or("the NFS URL has no export")?;
    let (export, root) = path.split_once(':').unwrap_or((path, ""));
    let path = [root.trim_matches('/'), relative.trim_matches('/')]
        .iter()
        .filter(|part| !part.is_empty())
        .copied()
        .collect::<Vec<_>>()
        .join("/");
    Ok((format!("nfs://{host}/{export}?{query}"), path))
}

async fn raw_acl(endpoint: &str, relative: &str) -> Result<(Box<dyn Mount>, String, Acl)> {
    let (url, path) = raw_location(endpoint, relative)?;
    let mount = nfs_rs::parse_url_and_mount(&url).await?;
    let acl = mount.getacl_path(&path).await?;
    Ok((mount, path, acl))
}

/// The server may qualify the numeric principal with its `NFSv4` domain.
fn is_mark_principal(who: &str) -> bool {
    who == MARK_PRINCIPAL
        || who
            .strip_prefix(MARK_PRINCIPAL)
            .is_some_and(|rest| rest.starts_with('@'))
}

fn is_marked(mark: Mark, acl: &Acl) -> bool {
    acl.aces.iter().any(|ace| match mark {
        Mark::Principal => is_mark_principal(&ace.who),
        Mark::EveryoneWriteAcl => {
            ace.who == EVERYONE && ace.access_mask.0 & AceMask::WRITE_ACL != 0
        }
    })
}

async fn mark_acl(mark: Mark, endpoint: &str, relative: &str) -> Result {
    let (mount, path, mut acl) = raw_acl(endpoint, relative).await?;
    match mark {
        Mark::Principal => {
            acl.aces.retain(|ace| !is_mark_principal(&ace.who));
            acl.aces.push(NfsAce {
                ace_type: AceType::AccessAllowed,
                flags: AceFlags::default(),
                access_mask: AceMask(AceMask::READ_DATA),
                who: MARK_PRINCIPAL.to_owned(),
            });
        }
        Mark::EveryoneWriteAcl => {
            let everyone = acl
                .aces
                .iter_mut()
                .find(|ace| ace.who == EVERYONE && ace.ace_type == AceType::AccessAllowed)
                .ok_or("the source ACL has no EVERYONE@ allow entry to mark")?;
            everyone.access_mask.0 |= AceMask::WRITE_ACL;
        }
    }
    mount.setacl_path(&path, &acl).await?;
    let (_, _, marked) = raw_acl(endpoint, relative).await?;
    if !is_marked(mark, &marked) {
        return Err(format!("the server dropped the {mark:?} mark: {marked:?}").into());
    }
    println!("marked source ACL ({mark:?}): {} ACE(s)", marked.aces.len());
    Ok(())
}

async fn verify_acl(args: &Args, mark: Mark, reported: Option<Outcome>) -> Result {
    let (_, _, source) = raw_acl(&args.source, &args.source_path).await?;
    let (_, _, destination) = raw_acl(&args.destination, &args.destination_path).await?;
    for (side, acl) in [("source", &source), ("destination", &destination)] {
        for ace in &acl.aces {
            println!("{side} ACE {ace:?}");
        }
    }
    if !is_marked(mark, &destination) {
        return Err(format!("the destination lost the {mark:?} mark").into());
    }
    if source != destination {
        return Err("the destination ACL differs from the source ACL".into());
    }
    // The ACL arrived; a report that says otherwise is the engine misreporting.
    if reported != Some(Outcome::Applied) {
        return Err(format!("the ACL arrived but the report says {reported:?}").into());
    }
    println!("ACLs equal and marked: {} ACE(s)", source.aces.len());
    Ok(())
}

/// A copy onto itself would make the ACL comparison compare a file with itself.
fn same_file(args: &Args) -> bool {
    let nfs = |endpoint: &str, path: &str| raw_location(endpoint, path).ok();
    match (
        nfs(&args.source, &args.source_path),
        nfs(&args.destination, &args.destination_path),
    ) {
        (Some(source), Some(destination)) => source == destination,
        _ => args.source == args.destination && args.source_path == args.destination_path,
    }
}

fn check(family: &str, actual: Option<Outcome>, expected: Option<Outcome>) -> Result {
    match expected {
        Some(expected) if actual != Some(expected) => {
            Err(format!("{family}: expected {expected:?}, report says {actual:?}").into())
        }
        _ => Ok(()),
    }
}

fn print_report(report: &MetadataApplicationReport) {
    for value in report.outcomes() {
        println!("{:?}: {:?}", value.family, value.outcome);
    }
    for (family, loss) in report.loss_report().losses() {
        println!("loss {family:?}: {loss:?}");
    }
    for skip in report.skipped() {
        println!("{skip}");
    }
}

async fn copied(args: &Args, outcome: TransferOutcome) -> Result {
    let report = outcome
        .metadata
        .ok_or("the copy produced no metadata report")?;
    print_report(&report);
    let outcome_of = |family| {
        report
            .outcomes()
            .iter()
            .find(|value| value.family == family)
            .map(|value| Outcome::from(value.outcome))
    };
    check("acl", outcome_of(MetadataFamily::Acl), args.expect_acl)?;
    check(
        "xattrs",
        outcome_of(MetadataFamily::Xattrs),
        args.expect_xattrs,
    )?;
    println!("copied {} bytes", outcome.transferred_bytes);
    match args.mark_acl {
        Some(mark) if args.verify_acl => {
            verify_acl(args, mark, outcome_of(MetadataFamily::Acl)).await
        }
        _ => Ok(()),
    }
}

#[tokio::main]
async fn main() -> Result {
    let args = Args::parse();
    if args.expect == Expect::Refused
        && (args.verify_acl || args.expect_acl.is_some() || args.expect_xattrs.is_some())
    {
        return Err("--verify-acl / --expect-acl / --expect-xattrs need --expect copied".into());
    }
    if same_file(&args) {
        return Err("source and destination are the same file".into());
    }
    let source = connect(&args.source, "source").await?;
    let destination = connect(&args.destination, "destination").await?;
    if let Some(bytes) = args.seed_bytes {
        seed(&source, &args.source_path, bytes).await?;
    }
    if let Some(mark) = args.mark_acl {
        mark_acl(mark, &args.source, &args.source_path).await?;
    }
    let mut copied_metadata = CopiedMetadataRequest::default();
    if args.acl {
        copied_metadata = copied_metadata.with_acl();
    }
    if args.xattrs {
        copied_metadata = copied_metadata.with_xattrs();
    }
    let request = request(
        &source,
        &args.source_path,
        &destination,
        &args.destination_path,
    )?
    .with_copied_metadata(copied_metadata);
    match (transfer(request).await, args.expect) {
        (Ok(outcome), Expect::Copied) => copied(&args, outcome).await,
        (Err(failure), Expect::Refused) => {
            println!("refused as expected: {failure}");
            if let Some(metadata) = failure.metadata_failure() {
                for family in metadata.failures() {
                    println!("  {family}");
                }
            }
            Ok(())
        }
        (Ok(_), Expect::Refused) => Err("the copy succeeded but a refusal was expected".into()),
        (Err(failure), Expect::Copied) => Err(format!("the copy failed: {failure:?}").into()),
    }
}
