//! Taking an interrupted upload back: checking the recovery identity belongs to this binding, and
//! rebuilding the durable prefix from the parts the service reports.

use bytes::Bytes;

use super::{PrepareRequest, S3StagedDestination, planned_part_size};
use crate::model::{FailureClass, Operation, Transience};
use crate::storage::backends::s3::source::{classified_entry, role_failure};
use crate::storage::backends::s3::{S3PartFacts, S3Protocol, S3ProtocolFailure};
use crate::storage::{RecoverRequest, StorageRoleFailure};

use super::MIN_MULTIPART_PART_SIZE;

pub(super) fn resumable_parts(
    path: &crate::model::StoragePath,
    mut observed: Vec<S3PartFacts>,
    expected_size: Option<u64>,
) -> Result<(u64, Vec<(i32, String)>), StorageRoleFailure> {
    observed.sort_by_key(|part| part.number);
    if observed.len() > 10_000 {
        return Err(invalid_manifest(
            path,
            "S3 multipart manifest is not a contiguous reusable prefix",
        ));
    }
    let mut persisted = 0_u64;
    for (index, part) in observed.iter().enumerate() {
        persisted = persisted
            .checked_add(part.size)
            .ok_or_else(|| invalid_manifest(path, "S3 multipart prefix size overflow"))?;
        let contiguous = part.number == i32::try_from(index + 1).unwrap_or(i32::MAX);
        let bounded = part.size <= 5 * 1024 * 1024 * 1024;
        let short_final = part.size < MIN_MULTIPART_PART_SIZE
            && index + 1 == observed.len()
            && expected_size == Some(persisted);
        if !contiguous || !bounded || (part.size < MIN_MULTIPART_PART_SIZE && !short_final) {
            return Err(invalid_manifest(
                path,
                "S3 multipart manifest is not a contiguous reusable prefix",
            ));
        }
    }
    Ok((
        persisted,
        observed
            .into_iter()
            .map(|part| (part.number, part.etag))
            .collect(),
    ))
}

fn invalid_manifest(path: &crate::model::StoragePath, diagnostic: &str) -> StorageRoleFailure {
    classified_entry(
        path,
        Operation::Prepare,
        FailureClass::Corruption,
        Transience::Permanent,
        diagnostic,
    )
}

impl<P: S3Protocol> S3StagedDestination<P> {
    pub(super) fn validated_recovery(
        request: &RecoverRequest,
    ) -> Result<(Bytes, String, String), StorageRoleFailure> {
        let token = Bytes::copy_from_slice(request.identity.as_bytes());
        let (key, upload_id) = Self::decode_token(&token).map_err(|()| {
            classified_entry(
                request.final_destination.path(),
                Operation::Prepare,
                FailureClass::Corruption,
                Transience::Permanent,
                "invalid S3 recovery identity",
            )
        })?;
        let expected = Self::temp_key(&PrepareRequest {
            final_destination: request.final_destination.clone(),
            source: request.source.clone(),
            recovery_binding: request.recovery_binding,
        });
        if key != expected {
            return Err(classified_entry(
                request.final_destination.path(),
                Operation::Prepare,
                FailureClass::Corruption,
                Transience::Permanent,
                "S3 recovery identity does not match destination binding",
            ));
        }
        Ok((token, key, upload_id))
    }

    pub(super) async fn recovered_state(
        &self,
        request: &RecoverRequest,
        key: &str,
        upload_id: &str,
    ) -> Result<(u64, Vec<(i32, String)>, bool), StorageRoleFailure> {
        match self.protocol.list_parts(key, upload_id).await {
            Ok(parts) => {
                let planned =
                    planned_part_size(request.source.size, request.final_destination.path())?
                        as u64;
                let (size, parts) =
                    resumable_parts(request.final_destination.path(), parts, request.source.size)?;
                let available = 10_000 - parts.len() as u64;
                if request.source.size.is_some_and(|expected| {
                    expected
                        .checked_sub(size)
                        .is_none_or(|remaining| remaining.div_ceil(planned) > available)
                }) {
                    return Err(invalid_manifest(
                        request.final_destination.path(),
                        "S3 recovered parts cannot satisfy the planned part limit",
                    ));
                }
                Ok((size, parts, false))
            }
            Err(S3ProtocolFailure::Entry {
                class: crate::model::FailureClass::NotFound,
                ..
            }) => {
                let facts = self.protocol.head(key).await.map_err(|failure| {
                    role_failure(
                        request.final_destination.path(),
                        Operation::Prepare,
                        failure,
                    )
                })?;
                Ok((facts.size, Vec::new(), true))
            }
            Err(failure) => Err(role_failure(
                request.final_destination.path(),
                Operation::Prepare,
                failure,
            )),
        }
    }

    pub(super) async fn remove_invalid_upload(
        &self,
        request: &RecoverRequest,
        key: &str,
        upload_id: &str,
    ) -> Result<(), StorageRoleFailure> {
        self.protocol
            .abort_multipart(key, upload_id)
            .await
            .map_err(|failure| {
                role_failure(
                    request.final_destination.path(),
                    Operation::Prepare,
                    failure,
                )
            })
    }
}
