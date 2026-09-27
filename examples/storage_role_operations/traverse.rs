//! `traverse`: a filtered, depth-limited role-based traversal, printed one line per item (or
//! counted, with `--quiet`).

use std::fmt::Write as _;
use std::num::NonZeroUsize;
use std::sync::Arc;

use data_mover::DslTraversalFilter;
use data_mover::model::{
    HistoryOrder, MetadataObservation, ObservationMode, ObservationPlan, ObservedEntry, StoragePath,
};
use data_mover::storage::Storage;
use data_mover::traversal::{
    BlockCounts, ScanTotals, StorageTraversalSource, TraversalItem, TraversalOrder,
    TraversalOutcome, TraversalRequest, TraversalSource as _, TraversalVersions,
};

use super::{Args, Command, Error};

pub(super) async fn traverse(
    storage: &Storage,
    args: &Args,
    command: &Command,
) -> Result<(), Error> {
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
