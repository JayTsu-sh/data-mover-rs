//! Replays the stored history of S3 keys onto a destination S3, oldest record first (P7 H1,
//! §14.1 D5 / D10 / D11 / D12), and prints what happened to each record — the reference for a
//! caller that rebuilds histories, and the driver of `.claude/skills/e2e-s3/scripts/
//! directory_objects.sh`.
//!
//! ```text
//! cargo run --example s3_history_replay -- --source s3:<prefix> \
//!     --destination s3@<bucket>:<prefix> --path m/ [--path z/] [--client-shaped]
//! ```
//!
//! Endpoints as in `transfer_resume` (`s3:<prefix>` is `S3_BUCKET`, `s3@<bucket>:<prefix>` another
//! bucket of the same server). The source's records come from the role-based traversal of every
//! version (the key's parent, one level deep: a directory object `m/` is a file of `m`).
//!
//! - A destination that keeps histories (`transfer::destination_keeps_history`: versioning
//!   `Enabled` now) gets every record, strictly one after another: each object version is a
//!   transfer with `--source-version` semantics to the same path, each delete marker a
//!   `transfer::write_delete_marker`. Nothing is deduplicated: rerunning adds every record again
//!   (D11). A record that fails ends that key's replay (the rest would be written out of order).
//! - Any other destination gets the latest state only (D12): the newest record when it is a
//!   version, nothing when it is a delete marker (an older object already at that key stays: a
//!   deletion on such a destination is the caller's call), and the line `history not rebuilt: …`.
//!   A marker refused mid-replay (the bucket stopped keeping versions) ends that key as failed.
//!
//! `--delete-marker` writes one marker on each path of the destination instead and prints
//! `marker <path> -> <outcome>` (a probe of how the store answers, e.g. a second marker in a row).
//!
//! One line per record:
//! `record <path> <n> <source version> version|marker size=<bytes|->[ order=inferred] -> <outcome>`
//! where the outcome is `copied version=<destination id|->`, `written marker=<id>`, `merged
//! marker=<id>` (the store added no marker: its newest record already was one),
//! `null-version-removed version=<id> marker=<id|->`, `skipped:<reason>` or `failed:<phase> <side>
//! <diagnostic>`; ` order=inferred` marks a history whose order the source's listing did not give
//! (`RustFS`; same-millisecond records may be out of order). Then `replayed records=… copied=…
//! written=… merged=… null_removed=… skipped=… failed=…`. Exits 1 when a record failed.

#[expect(
    dead_code,
    reason = "the replay uses only `connect` of the shared endpoint support"
)]
mod endpoint_support;

use std::error::Error;
use std::num::NonZeroUsize;
use std::process::exit;

use clap::Parser;
use data_mover::model::{
    EntryVersion, FailureClass, HistoryOrder, ObservedEntry, SourceVersion, StoragePath,
};
use data_mover::storage::{Storage, StorageRoleFailure};
use data_mover::transfer::{
    DeleteMarkerOutcome, InflightLimits, PayloadShapingPolicy, TransferFailure, TransferRequest,
    destination_keeps_history, transfer, write_delete_marker,
};
use data_mover::traversal::{
    StorageTraversalSource, TraversalItem, TraversalRequest, TraversalSource as _,
    TraversalVersions,
};
use endpoint_support::{Result, connect};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
#[command(about = "Replay S3 key histories onto a destination, oldest record first")]
struct Args {
    #[arg(long)]
    source: String,
    #[arg(long)]
    destination: String,
    /// A key to replay (relative to the source root; the same path at the destination).
    #[arg(long = "path", required = true)]
    paths: Vec<String>,
    /// Stream every version through this process instead of copying it natively.
    #[arg(long)]
    client_shaped: bool,
    /// Instead of replaying, write one delete marker on each `--path` of the destination.
    #[arg(long)]
    delete_marker: bool,
}

#[derive(Default)]
struct Tally {
    records: u32,
    copied: u32,
    written: u32,
    merged: u32,
    null_removed: u32,
    skipped: u32,
    failed: u32,
}

/// The two ends and how versions are copied.
struct Replay {
    source: Storage,
    destination: Storage,
    client_shaped: bool,
}

/// Where `key` is listed: its parent directory, and how deep below it (a directory object `m/` is
/// a file of the directory `m`, one level further down).
fn listed_at(key: &str) -> (&str, usize) {
    let (spelled, depth) = key
        .strip_suffix('/')
        .map_or((key, 1), |directory| (directory, 2));
    let parent = spelled.rsplit_once('/').map_or("", |(parent, _)| parent);
    (parent, depth)
}

/// Every record of `key` on `source`, oldest first, as the traversal of every version lists them.
async fn records(source: &Storage, key: &str) -> Result<Vec<ObservedEntry>> {
    let (parent, depth) = listed_at(key);
    let request = TraversalRequest::new(StoragePath::new(parent)?)
        .versions(TraversalVersions::All)
        .max_depth(NonZeroUsize::new(depth))
        .cancel(CancellationToken::new());
    let mut session = StorageTraversalSource::new(source)?.traverse(request);
    let mut found = Vec::new();
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) if entry.path().as_str() == key => found.push(*entry),
            // A failure at the key or at a directory above it: the history may be incomplete.
            TraversalItem::Failure(failure) if key.starts_with(failure.path.as_str()) => {
                return Err(format!("{key}: {:?}", failure.failure).into());
            }
            _ => {}
        }
    }
    session.finish().await?;
    Ok(found)
}

fn describe(failure: &TransferFailure) -> String {
    let mut text = failure.to_string();
    let mut cause = failure.source();
    while let Some(next) = cause {
        text.push_str(": ");
        text.push_str(&next.to_string());
        cause = next.source();
    }
    format!("{:?} {:?} {text}", failure.phase(), failure.side())
}

/// Whether the destination refused a marker because it cannot hold one (not versioned).
fn unholdable(failure: &TransferFailure) -> bool {
    matches!(
        failure.source().and_then(|cause| cause.downcast_ref::<StorageRoleFailure>()),
        Some(StorageRoleFailure::Entry(entry)) if entry.class() == FailureClass::Unsupported
    )
}

fn is_marker(record: &ObservedEntry) -> bool {
    record.version().is_some_and(EntryVersion::is_delete_marker)
}

/// Writes one marker. `refusal_fails`: in a history replay a refusal (the destination stopped
/// keeping versions mid-way) is a failure that ends the key; in the `--delete-marker` probe it is
/// the answer being probed.
async fn replay_marker(
    destination: &Storage,
    key: &str,
    refusal_fails: bool,
    tally: &mut Tally,
) -> Result<String> {
    let outcome = write_delete_marker(
        destination.clone(),
        StoragePath::new(key)?,
        CancellationToken::new(),
    )
    .await;
    Ok(match outcome {
        Ok(DeleteMarkerOutcome::Written { version_id }) => {
            tally.written += 1;
            format!("written marker={version_id}")
        }
        Ok(DeleteMarkerOutcome::Merged { version_id }) => {
            tally.merged += 1;
            format!("merged marker={version_id}")
        }
        Ok(DeleteMarkerOutcome::NullVersionRemoved { version_id, marker }) => {
            tally.null_removed += 1;
            format!(
                "null-version-removed version={version_id} marker={}",
                marker.as_deref().unwrap_or("-")
            )
        }
        Ok(other) => format!("other {other:?}"),
        Err(failure) if unholdable(&failure) && !refusal_fails => {
            tally.skipped += 1;
            format!("skipped:{}", describe(&failure))
        }
        Err(failure) => {
            tally.failed += 1;
            format!("failed:{}", describe(&failure))
        }
    })
}

impl Replay {
    async fn copy(&self, record: &ObservedEntry, tally: &mut Tally) -> Result<String> {
        let version = record.source_version().unwrap_or(SourceVersion::Current);
        let mut request = TransferRequest::new(
            self.source.clone(),
            record.path().clone(),
            self.destination.clone(),
            record.path().clone(),
            InflightLimits::new(4, 64 * 1024 * 1024, 4)?,
            CancellationToken::new(),
        )
        .with_source_version(version);
        if self.client_shaped {
            request = request.with_payload_shaping(PayloadShapingPolicy::RequireClientShaped);
        }
        Ok(match transfer(request).await {
            Ok(outcome) => {
                tally.copied += 1;
                format!(
                    "copied version={}",
                    outcome.destination_version.as_deref().unwrap_or("-")
                )
            }
            Err(failure) => {
                tally.failed += 1;
                format!("failed:{}", describe(&failure))
            }
        })
    }

    /// One key: every record oldest first onto a destination keeping histories, otherwise only
    /// the newest (D12). Stops at the first failed record.
    async fn key(&self, key: &str, keeps_history: bool, tally: &mut Tally) -> Result {
        let mut listed = records(&self.source, key).await?;
        if !keeps_history {
            println!(
                "key {key}: history not rebuilt: the destination bucket keeps no versions (D12)"
            );
            listed = listed.pop().into_iter().collect();
        }
        for (index, record) in listed.iter().enumerate() {
            let failed_before = tally.failed;
            let outcome = match (is_marker(record), keeps_history) {
                (true, true) => replay_marker(&self.destination, key, true, tally).await?,
                (true, false) => {
                    tally.skipped += 1;
                    "skipped:the newest record is a delete marker and the destination keeps no \
                     versions: nothing written"
                        .to_owned()
                }
                (false, _) => self.copy(record, tally).await?,
            };
            tally.records += 1;
            print_record(key, index + 1, record, &outcome);
            if tally.failed > failed_before {
                break;
            }
        }
        Ok(())
    }
}

fn print_record(key: &str, number: usize, record: &ObservedEntry, outcome: &str) {
    let version = record.version();
    println!(
        "record {key} {number} {} {} size={}{} -> {outcome}",
        version.and_then(EntryVersion::id).unwrap_or("null"),
        if is_marker(record) {
            "marker"
        } else {
            "version"
        },
        record
            .size()
            .map_or_else(|| "-".to_owned(), |size| size.to_string()),
        if version.is_some_and(|version| version.order() == HistoryOrder::Inferred) {
            " order=inferred"
        } else {
            ""
        },
    );
}

#[tokio::main]
async fn main() -> Result {
    let args = Args::parse();
    let replay = Replay {
        source: connect(&args.source).await?,
        destination: connect(&args.destination).await?,
        client_shaped: args.client_shaped,
    };
    let mut tally = Tally::default();
    if args.delete_marker {
        for key in &args.paths {
            let outcome = replay_marker(&replay.destination, key, false, &mut tally).await?;
            println!("marker {key} -> {outcome}");
        }
        exit(i32::from(tally.failed > 0));
    }
    let keeps_history = destination_keeps_history(&replay.destination).await?;
    for key in &args.paths {
        replay.key(key, keeps_history, &mut tally).await?;
    }
    let Tally {
        records,
        copied,
        written,
        merged,
        null_removed,
        skipped,
        failed,
    } = tally;
    println!(
        "replayed records={records} copied={copied} written={written} merged={merged} \
         null_removed={null_removed} skipped={skipped} failed={failed}"
    );
    exit(i32::from(failed > 0));
}
