//! Applies metadata through the verified directory handle.
use std::{io, sync::Arc};

use tokio_util::sync::CancellationToken;

use super::super::{namespace::open_metadata_directory, staged::apply_local_mutations};
use super::{
    LocalObservationAdapter, classify_io, local_session_failure, metadata_failure,
    metadata_failure_with_transience, source_identity,
};
use crate::model::{EntryIdentityKey, FailureClass, Operation, StoragePath, Transience};
use crate::storage::{MetadataBatchFailure, MetadataMutation, StorageRoleFailure};

pub(super) async fn apply(
    adapter: &LocalObservationAdapter,
    path: &StoragePath,
    expected: EntryIdentityKey,
    mutations: Vec<MetadataMutation>,
    cancel: CancellationToken,
) -> Result<(), MetadataBatchFailure> {
    let count = mutations.len();
    let root = Arc::clone(&adapter.root);
    let identity = adapter.identity.clone();
    let destination = path.clone();
    let root_anchor = adapter.root_anchor.clone();
    // 调用方丢弃等待时，阻塞线程仍可能在队列中；阻止它随后开始新的写入。
    let cancel = cancel.child_token();
    let _cancel_on_drop = cancel.clone().drop_guard();
    tokio::task::spawn_blocking(move || {
        let whole = |error| MetadataBatchFailure::whole_batch(count, 0, error);
        if cancel.is_cancelled() {
            return Err(whole(None));
        }
        let directory =
            open_metadata_directory(&root, &destination, root_anchor.as_deref(), &cancel)
                .map_err(|error| whole(Some(error)))?;
        let observed = directory
            .dir_metadata()
            .map_err(|error| whole(Some(metadata_io_failure(&destination, &error, false))))?;
        let identity = source_identity(&identity, &destination, &observed)
            .map_err(|error| whole(Some(error)))?;
        if identity.identity_key() != expected {
            return Err(whole(Some(metadata_failure(
                &destination,
                FailureClass::Conflict,
            ))));
        }
        apply_local_mutations(&directory.into_std_file(), mutations, &cancel).map_err(
            |(index, error)| MetadataBatchFailure {
                failed_index: index,
                completed: index,
                error: error.map(|failure| {
                    if failure.cancelled {
                        let transience = if failure.changed {
                            Transience::Unknown
                        } else {
                            Transience::Transient
                        };
                        metadata_failure_with_transience(
                            &destination,
                            FailureClass::Cancelled,
                            transience,
                        )
                    } else {
                        metadata_io_failure(&destination, &failure.error, failure.changed)
                    }
                }),
            },
        )
    })
    .await
    .map_err(|_| {
        MetadataBatchFailure::whole_batch(
            count,
            0,
            Some(metadata_failure(path, FailureClass::Internal)),
        )
    })?
}

fn metadata_io_failure(path: &StoragePath, error: &io::Error, changed: bool) -> StorageRoleFailure {
    let (class, transience) = classify_io(error.kind());
    // 单项属性已经写入一部分时，其余属性不能按确定拒绝继续提交。
    let transience = if changed {
        Transience::Unknown
    } else {
        transience
    };
    match error.kind() {
        io::ErrorKind::NotConnected | io::ErrorKind::BrokenPipe => {
            local_session_failure(Operation::Metadata, class, transience)
        }
        _ => metadata_failure_with_transience(path, class, transience),
    }
}
