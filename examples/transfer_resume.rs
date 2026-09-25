//! Copies one file through the role-based transfer engine between any two endpoints and prints one
//! JSON line, so a script can interrupt a transfer and check what a fresh process resumes. See
//! `.claude/skills/_shared/resume_matrix.sh` and `.claude/skills/e2e-s3/scripts/staged_matrix.sh`.
//!
//! ```text
//! cargo run --example transfer_resume -- \
//!     --source /tmp/dm-src --source-path f200m --seed-bytes 209715200 \
//!     --destination s3:run1 --destination-path f200m --bandwidth 20971520 --cancel-after-ms 3000
//! ```
//!
//! Endpoints: see `endpoint_support` (`nfs://…`, `smb:<sub-path>`, `s3:<prefix>`, `hdfs://…`, or a
//! local directory). A second run with the same endpoints and paths derives the same transfer
//! identity and resumes what the first left behind — today only where the engine's recovery records
//! (`DATA_MOVER_RECOVERY_DIR`) survived; ADR-0006 moves them to the destination. `--identity`
//! replaces the derived identity with a label, which a resume must then repeat.
//!
//! `--source-version <versionId>` copies one stored version of an S3 source; the selector is part of
//! the derived identity, so a resume must repeat it. `--client-shaped` streams an S3→S3 copy instead
//! of copying it natively; a native copy goes to the final key and a checkpointed one over 64 MiB
//! resumes like a streamed one (ADR-0006 C18). `native_bytes` / `native_requests` count what the
//! server copied (a resumed copy counts only what it added).
//!
//! With `RUST_LOG` set, the library's tracing (warnings included) goes to stderr.
//!
//! The identity goes to stderr as soon as the request is built, so a run killed before it reports
//! still leaves it behind, and into the stdout line as `identity`.
//!
//! `source_streamed_bytes` is what the engine actually read from the source; it is counted only
//! under a source budget, so a run that must report it passes `--bandwidth` (a high value if the
//! run should not be slowed).

mod endpoint_support;

use std::env;
use std::error::Error;
use std::fs;
use std::io::{Write as _, stderr};
use std::path::Path;
use std::process::exit;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use data_mover::integrity::{IntegrityMode, IntegrityOptions, IntegrityRequest, compare};
use data_mover::model::StoragePath;
use data_mover::storage::{Storage, create_directory_all};
use data_mover::transfer::{
    InflightLimits, PayloadShapingPolicy, ReadBackVerification, SourceQosGroup, SourceQosPolicy,
    SourceVersion, TransferFailure, TransferIdentity, TransferOutcome, TransferPolicy,
    TransferRequest, transfer,
};
use endpoint_support::{Result, artifacts, connect, remove_run};
use serde_json::{Value, json};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
#[command(about = "Copy one file between two endpoints and report the outcome as JSON")]
struct Args {
    #[arg(long)]
    destination: String,
    #[arg(long, required_unless_present_any = ["list_artifacts", "remove_run"])]
    source: Option<String>,
    #[arg(long, required_unless_present_any = ["list_artifacts", "remove_run"])]
    source_path: Option<String>,
    #[arg(long, required_unless_present_any = ["list_artifacts", "remove_run"])]
    destination_path: Option<String>,
    /// Create the local source file with this many deterministic bytes if it does not exist yet;
    /// an existing file of another size is refused, so a resume keeps the source identity.
    #[arg(long)]
    seed_bytes: Option<u64>,
    #[arg(long, value_enum, default_value_t = Policy::Checkpointed)]
    policy: Policy,
    #[arg(long, value_enum, default_value_t = OnOff::On)]
    read_back: OnOff,
    /// Override the derived transfer identity with this label; a resume must repeat it.
    #[arg(long)]
    identity: Option<String>,
    /// Copy this stored version of an S3 source (its versionId) instead of the current object.
    /// Any other source fails at preflight, before the destination is touched.
    #[arg(long)]
    source_version: Option<String>,
    /// Stream the payload through this process even where a native (server-side) copy is possible.
    #[arg(long)]
    client_shaped: bool,
    /// Hard limit on source read bandwidth, so an interruption lands mid-transfer.
    #[arg(long)]
    bandwidth: Option<u64>,
    /// Cancel the transfer after this many milliseconds.
    #[arg(long)]
    cancel_after_ms: Option<u64>,
    /// Instead of copying, print the `.data-mover-*` artifacts in this directory of the destination.
    #[arg(long)]
    list_artifacts: Option<String>,
    /// Instead of copying, remove this directory of the destination with its artifacts and files.
    #[arg(long)]
    remove_run: Option<String>,
    /// Instead of copying, compare the source file with the destination file byte for byte and
    /// print both BLAKE3 digests.
    #[arg(long)]
    compare: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Policy {
    Checkpointed,
    Atomic,
    Direct,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum OnOff {
    On,
    Off,
}

/// Deterministic bytes, written once: a resume must see the same file (and inode) it started on.
fn seed(root: &str, path: &str, bytes: u64) -> Result {
    let file = Path::new(root).join(path);
    if let Ok(existing) = fs::metadata(&file) {
        if existing.len() == bytes {
            return Ok(());
        }
        return Err(format!(
            "{} exists with {} bytes, not {bytes}",
            file.display(),
            existing.len()
        )
        .into());
    }
    if let Some(parent) = file.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut out = fs::File::create(&file)?;
    let mut reader = blake3::Hasher::new().update(path.as_bytes()).finalize_xof();
    let mut block = vec![0_u8; 1 << 20];
    let mut left = bytes;
    while left > 0 {
        let take = usize::try_from(left.min(block.len() as u64))?;
        reader.fill(&mut block[..take]);
        out.write_all(&block[..take])?;
        left -= take as u64;
    }
    out.sync_all()?;
    Ok(())
}

/// The failure and every cause below it: the top-level Display names only the phase and side.
fn chain(error: &dyn Error) -> String {
    let mut text = error.to_string();
    let mut cause = error.source();
    let mut last = text.clone();
    while let Some(next) = cause {
        let part = next.to_string();
        // A wrapper that displays its cause would otherwise print it twice.
        if !last.ends_with(&part) {
            text.push_str(": ");
            text.push_str(&part);
        }
        last = part;
        cause = next.source();
    }
    text
}

fn request(
    args: &Args,
    source: Storage,
    (destination, destination_path): (Storage, &str),
    cancel: CancellationToken,
) -> Result<TransferRequest> {
    let source_path = args.source_path.as_deref().ok_or("--source-path")?;
    let mut request = TransferRequest::new(
        source,
        StoragePath::new(source_path)?,
        destination,
        StoragePath::new(destination_path)?,
        InflightLimits::new(8, 64 << 20, 8)?,
        cancel,
    )
    .with_transfer_policy(match args.policy {
        Policy::Checkpointed => TransferPolicy::Checkpointed,
        Policy::Atomic => TransferPolicy::AtomicReplace,
        Policy::Direct => TransferPolicy::Direct,
    })
    .with_read_back_verification(match args.read_back {
        OnOff::On => ReadBackVerification::Enabled,
        OnOff::Off => ReadBackVerification::Disabled,
    });
    if let Some(version) = &args.source_version {
        request = request.with_source_version(SourceVersion::Id(version.clone()));
    }
    if args.client_shaped {
        request = request.with_payload_shaping(PayloadShapingPolicy::RequireClientShaped);
    }
    if let Some(label) = &args.identity {
        request = request.with_identity_override(TransferIdentity::from_label(label.as_str())?);
    }
    if let Some(rate) = args.bandwidth {
        let policy = SourceQosPolicy::new(Some((rate, rate, Duration::ZERO)), 8 << 20, None)?;
        request = request.with_source_qos(SourceQosGroup::new(policy));
    }
    Ok(request)
}

fn report(result: &Result<TransferOutcome, TransferFailure>, elapsed_ms: u128) -> Value {
    match result {
        Ok(outcome) => json!({
            "result": "ok",
            "elapsed_ms": elapsed_ms,
            "bytes": outcome.transferred_bytes,
            "route": format!("{:?}", outcome.route),
            "recovery": format!("{:?}", outcome.recovery),
            "prepare": format!("{:?}", outcome.prepare),
            "reused_bytes": outcome.reused_bytes,
            "destination_version": outcome.destination_version,
            "read_back": format!("{:?}", outcome.read_back),
            "source_streamed_bytes": outcome.source_qos.client_streamed_shaped_bytes,
            "source_read_operations": outcome.source_qos.source_read_operations,
            "native_bytes": outcome.source_qos.native_bytes,
            "native_requests": outcome.source_qos.native_requests,
        }),
        Err(failure) => json!({
            "result": "failed",
            "elapsed_ms": elapsed_ms,
            "phase": format!("{:?}", failure.phase()),
            "side": format!("{:?}", failure.side()),
            "failure": chain(failure),
            "recoverable_stage": failure.has_recoverable_stage(),
            "source_streamed_bytes": failure.source_qos().client_streamed_shaped_bytes,
            "source_read_operations": failure.source_qos().source_read_operations,
            "native_bytes": failure.source_qos().native_bytes,
            "native_requests": failure.source_qos().native_requests,
        }),
    }
}

/// `--list-artifacts` / `--remove-run`: inspect or clean the destination instead of copying.
async fn inspect(args: &Args) -> Result<Option<Value>> {
    if let Some(dir) = &args.remove_run {
        let removed = remove_run(&args.destination, dir).await?;
        return Ok(Some(json!({ "removed_artifacts": removed })));
    }
    if let Some(dir) = &args.list_artifacts {
        let found = artifacts(&args.destination, dir).await?;
        return Ok(Some(json!({ "artifacts": found })));
    }
    if args.compare {
        return Ok(Some(compare_content(args).await?));
    }
    Ok(None)
}

/// `--compare`: whether the destination file holds exactly the source file's bytes.
async fn compare_content(args: &Args) -> Result<Value> {
    let report = compare(IntegrityRequest {
        source: connect(args.source.as_deref().ok_or("--source")?).await?,
        source_path: StoragePath::new(args.source_path.as_deref().ok_or("--source-path")?)?,
        destination: connect(&args.destination).await?,
        destination_path: StoragePath::new(
            args.destination_path
                .as_deref()
                .ok_or("--destination-path")?,
        )?,
        options: IntegrityOptions {
            mode: IntegrityMode::Content,
            ..IntegrityOptions::default()
        },
        cancel: CancellationToken::new(),
    })
    .await?;
    let hex = |digest: [u8; 32]| blake3::Hash::from_bytes(digest).to_hex().to_string();
    let (source, destination) = report
        .digests()
        .ok_or("no digests in a content comparison")?;
    Ok(json!({
        "content_equal": source == destination,
        "source_blake3": hex(source),
        "destination_blake3": hex(destination),
        "compared_bytes": report.compared_bytes(),
    }))
}

/// `RUST_LOG=warn` shows the library's warnings (an S3 pointer version Object Lock kept, ADR-0006
/// C17) on stderr; stdout keeps its one JSON line.
fn init_tracing() {
    if env::var_os("RUST_LOG").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_ansi(false)
            .with_writer(stderr)
            .try_init();
    }
}

#[tokio::main]
async fn main() -> Result {
    let args = Args::parse();
    init_tracing();
    if let Some(line) = inspect(&args).await? {
        println!("{line}");
        return Ok(());
    }
    let (source_root, destination_path) = (
        args.source.as_deref().ok_or("--source")?,
        args.destination_path
            .as_deref()
            .ok_or("--destination-path")?,
    );
    if let Some(bytes) = args.seed_bytes {
        seed(
            source_root,
            args.source_path.as_deref().unwrap_or_default(),
            bytes,
        )?;
    }
    let source = connect(source_root).await?;
    let destination = connect(&args.destination).await?;
    // The endpoints data-mover derived (ADR-0006): a resume from a fresh process must print the same.
    let endpoints = json!({
        "source_endpoint": source.identity().stable_id(),
        "destination_endpoint": destination.identity().stable_id(),
    });
    // The caller creates the parent, as terrasync does: CIFS `prepare` does not.
    if let Some((parent, _)) = destination_path.rsplit_once('/')
        && !args.destination.starts_with("s3:")
    {
        create_directory_all(&destination, &StoragePath::new(parent)?).await?;
    }
    let cancel = CancellationToken::new();
    if let Some(ms) = args.cancel_after_ms {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(ms)).await;
            cancel.cancel();
        });
    }
    let started = Instant::now();
    let request = request(&args, source, (destination, destination_path), cancel)?;
    let identity = request.identity().to_string();
    let source_version = args.source_version.clone();
    eprintln!(
        "{}",
        json!({ "event": "start", "identity": identity, "source_version": source_version })
    );
    let result = transfer(request).await;
    let mut line = report(&result, started.elapsed().as_millis());
    if let (Some(line), Some(endpoints)) = (line.as_object_mut(), endpoints.as_object()) {
        line.extend(endpoints.clone());
        line.insert("identity".to_string(), Value::String(identity));
        line.insert("source_version".to_string(), json!(source_version));
    }
    println!("{line}");
    if result.is_err() {
        exit(1);
    }
    Ok(())
}
