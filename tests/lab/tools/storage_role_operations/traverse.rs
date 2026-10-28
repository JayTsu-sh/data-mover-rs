//! `traverse`: a filtered, depth-limited role-based traversal, printed one line per item (or
//! counted, with `--quiet`); with `--checkpoint-every` it checkpoints, `--resume-from` resumes,
//! and `--cancel-after-*` cancels part way, so a script can check that a cancelled traversal and
//! its resumption together deliver what one uninterrupted traversal does (P7 §9).
//!
//! A key with its history (a versioned S3 bucket, P7 §10) is its entry line with
//! ` history=<segment>/<complete 0|1> records=<n>[ order=inferred]`, then one line per record:
//! `Record <path> <segment>.<n> version=<id|null> latest=<0|1> marker=<0|1> size=<bytes|->`
//! (`n` from 1 within the segment). `--current` prints only the current objects instead.

use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{self, Write as _};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use data_mover::DslTraversalFilter;
use data_mover::model::{
    HistoryOrder, MetadataObservation, ObjectHistory, ObservationMode, ObservationPlan,
    ObservedEntry, StoragePath,
};
use data_mover::storage::Storage;
use data_mover::traversal::{
    BlockCounts, Checkpoint, DirectoryListed, Resumable, ScanTotals, StorageTraversalSource,
    TraversalItem, TraversalOrder, TraversalOutcome, TraversalRequest, TraversalSession,
    TraversalSource as _,
};

use super::{Args, Command, Error};

/// What a traversal delivered, for the summary line.
#[derive(Default)]
struct Tally {
    entries: u64,
    failures: u64,
    checkpoints: u64,
    snapshots: u64,
}

pub(super) async fn traverse(
    storage: &Storage,
    args: &Args,
    command: &Command,
) -> Result<(), Error> {
    let Command::Traverse {
        snapshot_bytes,
        cancel_after_secs,
        ..
    } = command
    else {
        unreachable!("dispatched by the caller")
    };
    let request = traversal_request(args, command)?;
    let cancel = request.cancel.clone();
    if let Some(seconds) = cancel_after_secs {
        let (cancel, after) = (cancel.clone(), Duration::try_from_secs_f64(*seconds)?);
        tokio::spawn(async move {
            sleep(after).await;
            cancel.cancel();
        });
    }
    let mut session = StorageTraversalSource::new(storage)?.traverse(request);
    let tally = consume(&mut session, command, &cancel).await?;
    let outcome = session.finish().await?;
    summarize(storage, &tally, &outcome, *snapshot_bytes);
    Ok(())
}

/// Takes every item of `session`, printing and counting it; cancels once
/// `--cancel-after-entries` entries arrived, and keeps draining.
async fn consume(
    session: &mut TraversalSession,
    command: &Command,
    cancel: &CancellationToken,
) -> Result<Tally, Error> {
    let Command::Traverse {
        quiet,
        snapshot_bytes,
        checkpoint_out,
        cancel_after_entries,
        ..
    } = command
    else {
        unreachable!("dispatched by the caller")
    };
    let mut tally = Tally::default();
    while let Some(item) = session.next_item().await {
        match &item {
            TraversalItem::Entry(entry) => {
                tally.entries += 1;
                if *snapshot_bytes {
                    tally.snapshots += entry.encode_snapshot().as_bytes().len() as u64;
                }
                if !*quiet {
                    print_entry(entry, command)?;
                }
                if *cancel_after_entries == Some(tally.entries) {
                    cancel.cancel();
                }
            }
            TraversalItem::Failure(failure) => {
                tally.failures += 1;
                eprintln!(
                    "failure scope={:?} {} {:?}",
                    failure.scope,
                    failure.path,
                    failure.failure.class()
                );
            }
            TraversalItem::ObservationFailure(failure) => {
                tally.failures += 1;
                eprintln!(
                    "metadata failure {} {:?} {:?} {:?} {:?}",
                    failure.path,
                    failure.version,
                    failure.family,
                    failure.class,
                    failure.transience
                );
            }
            TraversalItem::DirectoryListed(listed) if !*quiet => print_listed(listed),
            TraversalItem::Checkpoint(checkpoint) => {
                tally.checkpoints += 1;
                if let Some(path) = checkpoint_out {
                    save(path, checkpoint)?;
                }
            }
            // `TraversalItem` is `#[non_exhaustive]`: a later item kind must not break this.
            _ => {}
        }
    }
    Ok(tally)
}

/// One line per entry, with what the flags ask for, then one per record of its history — or,
/// with `--current`, the current object alone (nothing for a key deleted now, or a segment that
/// is not the key's last).
fn print_entry(entry: &ObservedEntry, command: &Command) -> Result<(), Error> {
    let Command::Traverse {
        sizes,
        identity,
        ownership,
        acl,
        current,
        ..
    } = command
    else {
        unreachable!("dispatched by the caller")
    };
    let history = entry.history();
    if *current && history.is_some_and(|history| history.current().is_none()) {
        return Ok(());
    }
    let history = history.filter(|_| !*current);
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
    let size = if *sizes {
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
        history.map_or_else(String::new, history_of)
    );
    if let Some(history) = history {
        print_records(entry, history)?;
    }
    Ok(())
}

fn print_listed(listed: &DirectoryListed) {
    println!(
        "listed {} {:?} {}",
        listed.path,
        listed.outcome,
        block_counts(&listed.counts)
    );
}

/// Replaces `path` with `checkpoint`. First the line `checkpoint bytes=<n>` goes out, after every
/// entry printed before it: a script that dies at any point holds a file no newer than the last
/// such line, so the entries printed before that line and a traversal resumed from the file
/// together cover the tree (P7 §9.2) — the check `_shared/traversal_resume.sh` makes.
fn save(path: &Path, checkpoint: &Checkpoint) -> Result<(), Error> {
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "checkpoint bytes={}", checkpoint.as_bytes().len())?;
    stdout.flush()?;
    drop(stdout);
    let staged = path.with_extension("tmp");
    let mut file = File::create(&staged)?;
    file.write_all(checkpoint.as_bytes())?;
    file.sync_all()?;
    fs::rename(&staged, path)?;
    Ok(())
}

fn summarize(storage: &Storage, tally: &Tally, outcome: &TraversalOutcome, snapshot_bytes: bool) {
    println!(
        "entries={} failures={} checkpoints={} outcome={outcome:?}",
        tally.entries, tally.failures, tally.checkpoints
    );
    if let TraversalOutcome::Completed(completion) = outcome {
        println!("{}", totals(&completion.totals));
    }
    if snapshot_bytes {
        println!(
            "snapshot_bytes={} endpoint_bytes={}",
            tally.snapshots,
            storage.identity().stable_id().len()
        );
    }
}

fn traversal_request(args: &Args, command: &Command) -> Result<TraversalRequest, Error> {
    let Command::Traverse {
        match_expression,
        exclude_expression,
        max_depth,
        order,
        path,
        ownership,
        acl,
        checkpoint_every,
        resume_from,
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
        .observation_plan(plan)
        .max_depth(max_depth.and_then(NonZeroUsize::new));
    let request = match checkpoint_every {
        Some(seconds) => request.resumable(Resumable::On {
            every: Duration::try_from_secs_f64(*seconds)?,
        }),
        None => request,
    };
    let request = match resume_from {
        Some(path) => request.resume_from(Checkpoint::from_bytes(Bytes::from(fs::read(path)?))),
        None => request,
    };
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
         truncated={} bytes={} files={} dirs={} symlinks={} other={} versions={} delete_markers={} \
         all_versions_bytes={}",
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
        counts.other,
        counts.versions,
        counts.delete_markers,
        counts.all_versions_bytes
    )
}

/// The whole traversal's counts as `totals key=value …`, for scripts to parse.
fn totals(totals: &ScanTotals) -> String {
    format!(
        "totals balanced={} listed={} emitted={} filtered={} failed={} vanished={} hidden={} \
         listing_failures={} interrupted={} vanished_listings={} pruned={} truncated={} bytes={} \
         files={} dirs={} symlinks={} other={} versions={} delete_markers={} all_versions_bytes={}",
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
        totals.other,
        totals.versions,
        totals.delete_markers,
        totals.all_versions_bytes
    )
}

/// ` history=<segment>/<complete 0|1> records=<n>`, then ` order=inferred` when the key's history
/// was ordered by `LastModified` rather than by the server's listing order.
fn history_of(history: &ObjectHistory) -> String {
    format!(
        " history={}/{} records={}{}",
        history.segment(),
        u8::from(history.is_complete()),
        history.records().len(),
        if history.order() == HistoryOrder::Inferred {
            " order=inferred"
        } else {
            ""
        }
    )
}

/// `Record <path> <segment>.<n> version=<id|null> latest=<0|1> marker=<0|1> size=<n|->`, one
/// line per record of the segment, oldest first.
fn print_records(entry: &ObservedEntry, history: &ObjectHistory) -> Result<(), Error> {
    let path = entry
        .history_source_path()?
        .ok_or("history has no source object key")?;
    for (index, record) in history.records().iter().enumerate() {
        println!(
            "Record {} {}.{} version={} latest={} marker={} size={}",
            path,
            history.segment(),
            index + 1,
            record.version_id().unwrap_or("null"),
            u8::from(record.is_latest()),
            u8::from(record.is_delete_marker()),
            record
                .size()
                .map_or_else(|| "-".to_string(), |size| size.to_string()),
        );
    }
    Ok(())
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
