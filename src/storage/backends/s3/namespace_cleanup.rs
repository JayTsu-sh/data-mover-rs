//! Removes current transfer artifacts without deleting ordinary children or object history.
use super::listing::directory_prefix;
use super::paging::{ListingLimits, Paging};
use super::source::role_failure;
use super::{S3ObjectPage, S3Protocol, S3ProtocolFailure};
use crate::model::{FailureClass, Operation, StoragePath, Transience};
use crate::storage::StorageRoleFailure;
use crate::storage::artifacts::is_artifact_path;
use std::collections::{HashSet, VecDeque};

pub(super) async fn cleanup<P: S3Protocol + ?Sized>(
    protocol: &P,
    path: &StoragePath,
    limits: ListingLimits,
    require_empty: bool,
) -> Result<(), StorageRoleFailure> {
    let prefix = directory_prefix(path)?;
    if require_empty {
        // Prove emptiness before deleting even an artifact. The directory's own marker is
        // removable, while any other ordinary key (or prefix) preserves the entire directory.
        let mut paging = Paging::new(limits);
        let mut token = None;
        loop {
            let page = protocol
                .list_objects_page(&prefix, token.as_deref())
                .await
                .map_err(|e| role_failure(path, Operation::Namespace, e))?;
            let count = page.objects.len() + page.prefixes.len() + page.rejected.len();
            validate_page(path, &prefix, &page)?;
            if page.objects.iter().any(|object| {
                !is_artifact_path(&object.key) && (object.key != prefix || object.size != 0)
            }) || page.prefixes.iter().any(|child| !is_artifact_path(child))
            {
                return Err(role_failure(
                    path,
                    Operation::Namespace,
                    S3ProtocolFailure::entry(
                        FailureClass::Conflict,
                        Transience::Permanent,
                        "S3 prefix is not empty",
                    ),
                ));
            }
            token = paging.advance_holding(path, count, 0, page.next)?;
            if token.is_none() {
                break;
            }
        }
    }
    let mut queue = VecDeque::from([prefix.clone()]);
    let mut reached = HashSet::from([prefix.clone()]);
    while let Some(current) = queue.pop_front() {
        let mut paging = Paging::new(limits);
        let mut token = None;
        loop {
            let page = protocol
                .list_objects_page(&current, token.as_deref())
                .await
                .map_err(|e| role_failure(path, Operation::Namespace, e))?;
            let count = page.objects.len() + page.prefixes.len() + page.rejected.len();
            validate_page(path, &current, &page)?;
            let keys: Vec<_> = page
                .objects
                .into_iter()
                .filter_map(|object| {
                    (is_artifact_path(&object.key) || (require_empty && object.key == prefix))
                        .then_some(object.key)
                })
                .collect();
            for results in protocol.delete_objects(&keys).await {
                results.map_err(|e| role_failure(path, Operation::Namespace, e))?;
            }
            for child in page.prefixes {
                if is_artifact_path(&child) && reached.insert(child.clone()) {
                    queue.push_back(child);
                }
            }
            token = paging.advance_holding(path, count, reached.len(), page.next)?;
            if token.is_none() {
                break;
            }
        }
    }
    Ok(())
}

fn protocol_failure(path: &StoragePath) -> StorageRoleFailure {
    role_failure(
        path,
        Operation::Namespace,
        S3ProtocolFailure::protocol("S3 artifact cleanup listing contained an invalid child"),
    )
}

/// Validate the entire page before mutating any key. A malformed server reply must never
/// authorize cleanup outside the requested subtree, even if the reported key is an artifact.
fn validate_page(
    path: &StoragePath,
    prefix: &str,
    page: &S3ObjectPage,
) -> Result<(), StorageRoleFailure> {
    let valid_key = |key: &str| {
        key.strip_prefix(prefix).is_some_and(|rest| {
            rest.is_empty()
                || rest
                    .strip_suffix('/')
                    .unwrap_or(rest)
                    .split('/')
                    .all(|part| !matches!(part, "" | "." | ".."))
        })
    };
    if !page.rejected.is_empty()
        || page.objects.iter().any(|object| !valid_key(&object.key))
        || page
            .prefixes
            .iter()
            .any(|child| child == prefix || !child.ends_with('/') || !valid_key(child))
    {
        return Err(protocol_failure(path));
    }
    Ok(())
}
