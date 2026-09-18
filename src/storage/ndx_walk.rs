//! NDX-paged depth-first traversal driven by the [`Namespace`] role.
//!
//! Replaces the legacy per-backend `walkdir_2`: directories are listed through
//! `NamespaceRequest::List` and handed to the shared DFS driver in [`crate::dir_tree`], which
//! owns the prefetch window, the depth-first order and the NDX / gap numbering. Only the
//! listing and the legacy-model conversion live here, so the adapter carries no
//! protocol-specific code.
//!
//! Semantics worth knowing before relying on it:
//!
//! - **Every emitted entry needs a modification time**, and the only source that costs no extra
//!   round trip is [`SourceDescriptor::inline_timestamps`]. CIFS, NFS and HDFS all attach it:
//!   `QUERY_DIRECTORY`, `readdirplus` and the HDFS listing each already carry the attributes.
//!   A listing that does not is reported as an error rather than emitted with an epoch
//!   timestamp — an epoch would silently tell an incremental sync that every entry changed, and
//!   would disable every `modified` filter condition.
//! - **Local and S3 lend no namespace role**, so they fail preflight here rather than
//!   producing an empty walk.
//! - **Pages carry [`ObservedEntry`], not `EntryEnum`.** That is the same immutable observation
//!   [`crate::traversal::StorageTraversalSource`] emits, so both role-layer walkers describe an
//!   entry the same way and `EntryEnum` — which is on its way out — gains no new caller here.
//!   As in that traversal, the emitted entry carries its **backend-relative** path; the
//!   scan-root-relative spelling is what the filter matches against and what the driver uses to
//!   derive depth.
//! - **What each backend can fill differs.** `created` holds the POSIX change time on NFS
//!   (`NFSv3` has no birth time), the real creation time on CIFS, and nothing on HDFS. Permission
//!   bits ride along as `ownership_mode` only where the backend observes them: real POSIX bits
//!   on NFS and HDFS, and on CIFS an approximation derived from `FILE_ATTRIBUTE_READONLY` that
//!   is reported for display and never applied to a destination.
//! - **Stopping needs the token.** `NdxWalkRequest::cancel` stops the reader pool at its next
//!   listing. Dropping the returned iterator is *not* reliable on its own: the driver only
//!   gives up when sending a non-empty page fails, and ignores send failures for error and
//!   completion events, so a subtree that is entirely empty or entirely failing keeps
//!   listing after the consumer is gone.

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::{
    CapabilityUnavailable, Namespace, NamespaceRequest, NamespaceResult, PreflightPolicy,
    SourceDescriptor, Storage, StorageRoleFailure,
};
use crate::TransferConcurrency;
use crate::dir_tree::{
    DirHandle, NdxEvent, ReadContext, ReadResult, SubdirEntry, run_dfs_driver_with,
};
use crate::error::StorageError;
use crate::filter::{FilterExpression, FilterInput, should_skip};
use crate::model::{
    BackendKind, EntryKind, FailureClass, MetadataObservation, MetadataObservations,
    MetadataProvenance, ObservedEntry, StoragePath,
};
use crate::traversal::relative_to;

/// The event stream [`ndx_walk`] produces.
pub type NdxWalkIterator = crate::AsyncReceiver<NdxEvent<ObservedEntry>>;

/// One NDX-paged traversal request.
#[derive(Clone, Debug)]
pub struct NdxWalkRequest {
    /// Subtree to enumerate, relative to the connected backend root. Emitted entries carry
    /// paths relative to **this** root, matching what the legacy walkers produced.
    pub root: StoragePath,
    /// Maximum depth to enumerate; direct children of `root` are depth 1. `None` is unlimited.
    pub max_depth: Option<NonZeroUsize>,
    /// Admission expression; `None` admits everything.
    pub match_expressions: Option<FilterExpression>,
    /// Exclusion expression; `None` excludes nothing.
    pub exclude_expressions: Option<FilterExpression>,
    /// Reader pool size, clamped to `1..=TransferConcurrency::MAX`.
    pub concurrency: NonZeroUsize,
    /// Stops the reader pool at its next listing. The DFS driver has no cancellation channel,
    /// so it keeps asking for directories; the readers answer without touching the backend,
    /// which is what actually bounds the cost.
    pub cancel: CancellationToken,
}

/// Starts an NDX-paged traversal of `request.root` through `storage`'s namespace role.
///
/// Stop the walk by dropping the returned iterator.
///
/// # Errors
/// Returns the capability failure when the storage lends no namespace role under production
/// preflight policy, before any backend I/O.
pub fn ndx_walk(
    storage: &Storage,
    request: NdxWalkRequest,
) -> Result<NdxWalkIterator, CapabilityUnavailable> {
    let namespace = storage.namespace(&PreflightPolicy::production())?;
    let concurrency = request.concurrency.get().min(TransferConcurrency::MAX);
    let (request_sender, request_receiver) = async_channel::bounded::<
        crate::dir_tree::ReadRequest<ObservedEntry>,
    >(concurrency.saturating_mul(2));
    let (output_sender, output_receiver) = async_channel::bounded(64);

    for _ in 0..concurrency {
        let namespace = Arc::clone(&namespace);
        let root = request.root.clone();
        let receiver = request_receiver.clone();
        let cancel = request.cancel.clone();
        tokio::spawn(async move {
            while let Ok(read) = receiver.recv().await {
                // Once the walk is over, answer without a backend call. The driver has no
                // cancellation channel and will keep requesting the remaining directories, so
                // this is what keeps a dead session or a cancelled walk from costing one
                // doomed round trip per directory.
                let result = if cancel.is_cancelled() {
                    Ok(errors_only(
                        &read.dir_path,
                        format!("cancelled before listing '{}'", read.dir_path),
                    ))
                } else {
                    read_dir(
                        namespace.as_ref(),
                        &root,
                        &read.dir_path,
                        &read.ctx,
                        &cancel,
                    )
                    .await
                };
                let _ = read.reply.send(result);
            }
        });
    }

    let context = ReadContext {
        match_expr: Arc::new(request.match_expressions),
        exclude_expr: Arc::new(request.exclude_expressions),
        current_depth: 0,
        max_depth: request.max_depth.map_or(0, NonZeroUsize::get),
        apply_filter: true,
        include_tags: false,
        is_versioned: false,
    };
    let root = request.root.clone();
    let kind = storage.kind();
    tokio::spawn(run_dfs_driver_with(
        request_sender,
        output_sender,
        root_handle(kind),
        context,
        move |entry| {
            (
                relative_to(&root, entry.path()).to_owned(),
                root_handle(kind),
            )
        },
    ));
    Ok(crate::AsyncReceiver::new(output_receiver))
}

/// Picks the handle variant for the connected backend.
///
/// `DirHandle` exists so a legacy reader can carry protocol-specific state (an NFS file
/// handle, a Local absolute path). This adapter needs none of it: every directory it reads is
/// identified by `ReadRequest::dir_path`, which the driver fills with the traversal root's
/// empty string for the root frame and with the child's scan-root-relative path below it. The
/// variant therefore only decides which `BackendKind` the driver derives, and that in turn
/// only selects which handle `extract_dir_handle` builds for a child — a value this adapter
/// ignores. Matching the connected backend keeps the value honest for anyone reading a trace.
fn root_handle(kind: BackendKind) -> DirHandle {
    match kind {
        BackendKind::Cifs => DirHandle::Cifs(String::new()),
        BackendKind::Hdfs => DirHandle::Hdfs(PathBuf::new()),
        BackendKind::Nfs => DirHandle::Nfs {
            fh: bytes::Bytes::new(),
            path: String::new(),
        },
        // Local and S3 lend no namespace role, so preflight already refused them.
        BackendKind::Local | BackendKind::S3 => DirHandle::Local(PathBuf::new()),
    }
}

/// Re-expresses a scan-root-relative path as the backend-relative path `List` expects.
fn join_root(root: &StoragePath, relative: &str) -> Result<StoragePath, StorageError> {
    let relative = relative.trim_start_matches('/');
    let joined = match (root.as_str(), relative) {
        ("", rest) => rest.to_owned(),
        (base, "") => base.to_owned(),
        (base, rest) => format!("{base}/{rest}"),
    };
    StoragePath::new(joined).map_err(|error| StorageError::InvalidPath(error.to_string()))
}

async fn read_dir(
    namespace: &dyn Namespace,
    root: &StoragePath,
    dir_path: &str,
    ctx: &ReadContext,
    cancel: &CancellationToken,
) -> crate::Result<ReadResult<ObservedEntry>> {
    let target = join_root(root, dir_path)?;
    let descriptors = match namespace.execute(NamespaceRequest::List(target)).await {
        Ok(NamespaceResult::Entries(entries)) => entries,
        Ok(_) => return Err(StorageError::MismatchedType),
        Err(failure) => return listing_failure(dir_path, &failure, cancel),
    };
    Ok(build_read_result(root, dir_path, &descriptors, ctx))
}

/// Splits a listing failure into the driver's two channels, and ends the walk when the
/// failure means no later listing can succeed either.
///
/// An entry-scoped failure (one unreadable directory) stays soft so the rest of the tree keeps
/// paging, matching what the legacy `read_dir_sorted` did for a failed open. A session-scoped
/// failure and a cancellation both trip the token: the driver cannot be told to stop, so
/// without this every remaining directory would cost one more doomed round trip. `Cancelled`
/// is reported with a distinguishable prefix rather than as an I/O error, per R10.
fn listing_failure<E>(
    dir_path: &str,
    failure: &StorageRoleFailure,
    cancel: &CancellationToken,
) -> crate::Result<ReadResult<E>> {
    let class = match failure {
        StorageRoleFailure::Entry(error) => error.class(),
        StorageRoleFailure::Session(error) => error.class(),
    };
    if class == FailureClass::Cancelled {
        cancel.cancel();
        return Ok(errors_only(dir_path, format!("cancelled: {dir_path}")));
    }
    match failure {
        StorageRoleFailure::Entry(error) => Ok(errors_only(
            dir_path,
            format!("failed to list '{dir_path}': {error}"),
        )),
        StorageRoleFailure::Session(error) => {
            cancel.cancel();
            Err(StorageError::OperationError(format!(
                "namespace session failed while listing '{dir_path}': {error}"
            )))
        }
    }
}

fn errors_only<E>(dir_path: &str, reason: String) -> ReadResult<E> {
    ReadResult {
        dir_path: dir_path.to_owned(),
        files: Vec::new(),
        subdirs: Vec::new(),
        errors: vec![reason],
    }
}

fn build_read_result(
    root: &StoragePath,
    dir_path: &str,
    descriptors: &[SourceDescriptor],
    ctx: &ReadContext,
) -> ReadResult<ObservedEntry> {
    let mut entries = Vec::with_capacity(descriptors.len());
    let mut undated = 0_usize;
    for descriptor in descriptors {
        match observed(descriptor) {
            Some(entry) => entries.push(entry),
            None => undated += 1,
        }
    }
    if undated > 0 {
        // Aggregated rather than one error per entry: a backend attaches inline timestamps to
        // every listed entry or to none, so per-entry reporting would emit a line per file for
        // the whole tree. The count is carried anyway, because "or to none" is an observation
        // about today's backends, not a contract `SourceDescriptor` enforces.
        return errors_only(
            dir_path,
            format!(
                "{undated} of {} entries in '{dir_path}' carry no inline modification time; \
                 emitting them would report the epoch and tell an incremental sync that every \
                 entry changed, so the directory and its subtree are skipped",
                descriptors.len()
            ),
        );
    }
    let (files, subdirs) = partition_entries(root, entries, ctx);
    ReadResult {
        dir_path: dir_path.to_owned(),
        files,
        subdirs,
        errors: Vec::new(),
    }
}

/// Applies the legacy `should_skip` triple and sorts each bucket by name.
///
/// A directory at the depth limit is emitted as an entry rather than descended into, which is
/// what the legacy readers did; `visible: false` keeps a filtered-out directory out of the page
/// while still descending into it.
fn partition_entries(
    root: &StoragePath,
    entries: Vec<ObservedEntry>,
    ctx: &ReadContext,
) -> (Vec<ObservedEntry>, Vec<SubdirEntry<ObservedEntry>>) {
    let mut files = Vec::new();
    let mut subdirs = Vec::new();
    for entry in entries {
        let (skip, continue_scan, need_filter) = if ctx.apply_filter {
            filter_decision(root, &entry, ctx)
        } else {
            (false, true, false)
        };
        let can_descend = entry.kind() == EntryKind::Directory
            && (ctx.max_depth == 0 || ctx.current_depth.saturating_add(1) < ctx.max_depth);
        if skip {
            if can_descend && continue_scan {
                subdirs.push(SubdirEntry {
                    entry,
                    visible: false,
                    need_filter,
                });
            }
        } else if can_descend {
            subdirs.push(SubdirEntry {
                entry,
                visible: true,
                need_filter,
            });
        } else {
            files.push(entry);
        }
    }
    files.sort_by(|left, right| entry_name(left).cmp(entry_name(right)));
    subdirs.sort_by(|left, right| entry_name(&left.entry).cmp(entry_name(&right.entry)));
    (files, subdirs)
}

/// Final path component of an observation.
fn entry_name(entry: &ObservedEntry) -> &str {
    let value = entry.path().as_str();
    value.rsplit_once('/').map_or(value, |(_, name)| name)
}

fn filter_decision(
    root: &StoragePath,
    entry: &ObservedEntry,
    ctx: &ReadContext,
) -> (bool, bool, bool) {
    let name = entry_name(entry);
    // The filter frame of reference is the traversal root, matching every legacy walker and
    // `DslTraversalFilter`, even though the observation itself carries the backend-relative path.
    let path = relative_to(root, entry.path());
    let file_type = if entry.kind() == EntryKind::Directory {
        "dir"
    } else {
        "file"
    };
    let extension = Path::new(name).extension().and_then(|value| value.to_str());
    should_skip(
        ctx.match_expr.as_ref().as_ref(),
        ctx.exclude_expr.as_ref().as_ref(),
        FilterInput {
            file_name: Some(name),
            file_path: Some(path),
            file_type: Some(file_type),
            modified_epoch: entry
                .modified()
                .and_then(|value| i64::try_from(value.unix_nanos() / 1_000_000_000).ok()),
            size: entry.size(),
            extension: extension.or(Some("")),
        },
    )
}

/// Turns one listed descriptor into the immutable observation the pages carry.
///
/// Returns `None` unless the listing carried a modification time. The whole record being absent
/// and the record being present with `modified: None` are the same thing here: either way the
/// entry would report the epoch, which is the outcome the module docs rule out. `accessed` and
/// `created` are allowed to be missing — nothing decides transfers on them.
///
/// No `Metadata` role call is made: everything here comes from facts the listing already
/// returned, which is the whole point of the adapter.
fn observed(descriptor: &SourceDescriptor) -> Option<ObservedEntry> {
    let timestamps = descriptor.inline_timestamps()?;
    let modified = timestamps.modified?;
    let observations = MetadataObservations::new(
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        // `OwnershipMode` requires numeric owner and group; the model deliberately has no
        // mode-only shape here (that is what `CopiedMetadataObservation::mode_without_ownership`
        // is for), and inventing ids would let a destination be given the wrong principal.
        MetadataObservation::NotRequested,
        MetadataObservation::Value {
            value: timestamps,
            provenance: MetadataProvenance::Inline,
        },
    )
    .ok()?;
    let entry = ObservedEntry::new(
        descriptor.path.clone(),
        descriptor.kind,
        descriptor.size,
        Some(modified),
        descriptor.source_identity.clone(),
    )
    .ok()?
    .with_metadata(observations);
    match descriptor.backend_fact.clone() {
        Some(fact) => entry.with_backend_fact_bytes(fact.to_vec()).ok(),
        None => Some(entry),
    }
}

#[cfg(test)]
mod tests;
