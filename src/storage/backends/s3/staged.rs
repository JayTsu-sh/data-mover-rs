use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use tokio::sync::Mutex;

use crate::model::{BackendIdentity, Operation};
use crate::storage::{
    ByteStream, CheckpointObservation, PrepareRequest, PreparedStage, PublicationEvidence,
    PublicationFailure, PublishRequest, RecoverRequest, RecoveryIdentity, StagedDestination,
    StorageRoleFailure, VerificationEvidence, VerifyRequest, WriteEvidence,
};

use super::source::{cancelled, entry, role_failure};

mod publication;
use super::{S3ClaimOutcome, S3Protocol, S3ProtocolFailure};

const PART_SIZE: usize = 8 * 1024 * 1024;
const MAX_INFLIGHT_PARTS: usize = 4;

async fn upload<P: S3Protocol>(
    protocol: Arc<P>,
    key: String,
    upload_id: String,
    number: i32,
    bytes: Bytes,
) -> Result<(i32, String), S3ProtocolFailure> {
    let etag = protocol
        .upload_part(&key, &upload_id, number, bytes)
        .await?;
    Ok((number, etag))
}

#[derive(Clone, Default)]
struct StageState {
    persisted: u64,
    upload_id: String,
    parts: Vec<(i32, String)>,
    completed: bool,
    claim_key: Option<String>,
}

pub(crate) struct S3StagedDestination<P> {
    protocol: Arc<P>,
    identity: BackendIdentity,
    states: Mutex<HashMap<Vec<u8>, StageState>>,
}

impl<P> S3StagedDestination<P> {
    pub(crate) fn new(protocol: Arc<P>, identity: BackendIdentity) -> Self {
        Self {
            protocol,
            identity,
            states: Mutex::new(HashMap::new()),
        }
    }

    fn temp_key(request: &PrepareRequest) -> String {
        format!(
            ".data-mover-stage/{}/{}",
            blake3::Hash::from_bytes(request.recovery_binding).to_hex(),
            blake3::hash(request.final_destination.path().as_str().as_bytes()).to_hex()
        )
    }

    fn key(stage: &PreparedStage) -> Result<String, StorageRoleFailure> {
        let (key, _) = Self::decode_token(&stage.token).map_err(|()| {
            entry(
                stage.final_destination.path(),
                Operation::Prepare,
                "invalid S3 stage identity",
            )
        })?;
        Ok(key)
    }

    fn encode_token(key: &str, upload_id: &str) -> Bytes {
        let mut token = Vec::with_capacity(key.len() + upload_id.len() + 1);
        token.extend_from_slice(key.as_bytes());
        token.push(0);
        token.extend_from_slice(upload_id.as_bytes());
        Bytes::from(token)
    }

    fn decode_token(token: &Bytes) -> Result<(String, String), ()> {
        let split = token.iter().position(|byte| *byte == 0).ok_or(())?;
        let key = String::from_utf8(token[..split].to_vec()).map_err(|_| ())?;
        let upload_id = String::from_utf8(token[split + 1..].to_vec()).map_err(|_| ())?;
        if key.is_empty() || upload_id.is_empty() {
            return Err(());
        }
        Ok((key, upload_id))
    }

    fn validate(&self, stage: &PreparedStage) -> Result<String, StorageRoleFailure> {
        stage.validate_owner(&self.identity).map_err(|_| {
            entry(
                stage.final_destination.path(),
                Operation::Prepare,
                "S3 stage belongs to another backend",
            )
        })?;
        Self::key(stage)
    }
}

fn resumable_parts(
    path: &crate::model::StoragePath,
    mut observed: Vec<super::S3PartFacts>,
) -> Result<(u64, Vec<(i32, String)>), StorageRoleFailure> {
    observed.sort_by_key(|part| part.number);
    let valid = observed.iter().enumerate().all(|(index, part)| {
        part.number == i32::try_from(index + 1).unwrap_or(i32::MAX) && part.size == PART_SIZE as u64
    });
    if !valid {
        return Err(entry(
            path,
            Operation::Prepare,
            "S3 multipart manifest is not a contiguous durable prefix",
        ));
    }
    let persisted = observed.iter().try_fold(0_u64, |total, part| {
        total.checked_add(part.size).ok_or_else(|| {
            entry(
                path,
                Operation::Prepare,
                "S3 multipart prefix size overflow",
            )
        })
    })?;
    Ok((
        persisted,
        observed
            .into_iter()
            .map(|part| (part.number, part.etag))
            .collect(),
    ))
}

impl<P: S3Protocol> S3StagedDestination<P> {
    fn validated_recovery(
        request: &RecoverRequest,
    ) -> Result<(Bytes, String, String), StorageRoleFailure> {
        let token = Bytes::copy_from_slice(request.identity.as_bytes());
        let (key, upload_id) = Self::decode_token(&token).map_err(|()| {
            entry(
                request.final_destination.path(),
                Operation::Prepare,
                "invalid S3 recovery identity",
            )
        })?;
        let expected = Self::temp_key(&PrepareRequest {
            final_destination: request.final_destination.clone(),
            source: request.source.clone(),
            recovery_binding: request.recovery_binding,
        });
        if key != expected {
            return Err(entry(
                request.final_destination.path(),
                Operation::Prepare,
                "S3 recovery identity does not match destination binding",
            ));
        }
        Ok((token, key, upload_id))
    }

    async fn claim_recovery(
        &self,
        request: &RecoverRequest,
        key: &str,
    ) -> Result<String, StorageRoleFailure> {
        let claim_key = format!("{key}.claim");
        let claim = self
            .protocol
            .claim(&claim_key, request.claim_token)
            .await
            .map_err(|failure| {
                role_failure(
                    request.final_destination.path(),
                    Operation::Prepare,
                    failure,
                )
            })?;
        if claim == S3ClaimOutcome::Conflict {
            return Err(entry(
                request.final_destination.path(),
                Operation::Prepare,
                "S3 recovery state is claimed by another attempt",
            ));
        }
        Ok(claim_key)
    }

    async fn recovered_state(
        &self,
        request: &RecoverRequest,
        key: &str,
        upload_id: &str,
    ) -> Result<(u64, Vec<(i32, String)>, bool), StorageRoleFailure> {
        match self.protocol.list_parts(key, upload_id).await {
            Ok(parts) => {
                let (size, parts) = resumable_parts(request.final_destination.path(), parts)?;
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

    async fn content_matches(
        &self,
        path: &crate::model::StoragePath,
        expected_size: u64,
        expected_blake3: &[u8; 32],
        cancel: &tokio_util::sync::CancellationToken,
        operation: Operation,
    ) -> Result<bool, StorageRoleFailure> {
        let facts = match self.protocol.head(path.as_str()).await {
            Ok(facts) => facts,
            Err(S3ProtocolFailure::Entry {
                class: crate::model::FailureClass::NotFound,
                ..
            }) => return Ok(false),
            Err(failure) => return Err(role_failure(path, operation, failure)),
        };
        if facts.size != expected_size {
            return Ok(false);
        }
        let mut hasher = blake3::Hasher::new();
        let mut offset = 0;
        while offset < facts.size {
            if cancel.is_cancelled() {
                return Err(cancelled(path, operation));
            }
            let end = (offset + PART_SIZE as u64).min(facts.size);
            let bytes = self
                .protocol
                .get_range(path.as_str(), offset..end)
                .await
                .map_err(|failure| role_failure(path, operation, failure))?;
            if bytes.len() as u64 != end - offset {
                return Ok(false);
            }
            hasher.update(&bytes);
            offset = end;
        }
        Ok(hasher.finalize().as_bytes() == expected_blake3)
    }
}

#[async_trait]
impl<P: S3Protocol + 'static> StagedDestination for S3StagedDestination<P> {
    async fn prepare(&self, request: PrepareRequest) -> Result<PreparedStage, StorageRoleFailure> {
        let key = Self::temp_key(&request);
        let upload_id =
            self.protocol.begin_multipart(&key).await.map_err(|e| {
                role_failure(request.final_destination.path(), Operation::Prepare, e)
            })?;
        let token = Self::encode_token(&key, &upload_id);
        self.states.lock().await.insert(
            token.to_vec(),
            StageState {
                persisted: 0,
                upload_id,
                parts: Vec::new(),
                completed: false,
                claim_key: None,
            },
        );
        Ok(PreparedStage::new(
            self.identity.clone(),
            request.final_destination,
            token,
            request.recovery_binding,
            0,
            None,
        ))
    }

    async fn recovery_identity(
        &self,
        stage: &PreparedStage,
    ) -> Result<RecoveryIdentity, StorageRoleFailure> {
        self.validate(stage)?;
        RecoveryIdentity::from_bytes(stage.token.clone()).map_err(|e| {
            entry(
                stage.final_destination.path(),
                Operation::Prepare,
                e.to_string(),
            )
        })
    }

    async fn recover(&self, request: RecoverRequest) -> Result<PreparedStage, StorageRoleFailure> {
        let (token, key, upload_id) = Self::validated_recovery(&request)?;
        let claim_key = self.claim_recovery(&request, &key).await?;
        let (persisted, parts, completed) =
            self.recovered_state(&request, &key, &upload_id).await?;
        self.states.lock().await.insert(
            token.to_vec(),
            StageState {
                persisted,
                upload_id,
                parts,
                completed,
                claim_key: Some(claim_key),
            },
        );
        Ok(PreparedStage::new(
            self.identity.clone(),
            request.final_destination,
            token,
            request.recovery_binding,
            persisted,
            None,
        ))
    }

    async fn write(
        &self,
        stage: &PreparedStage,
        mut input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        let key = self.validate(stage)?;
        let initial = self
            .states
            .lock()
            .await
            .get(stage.token.as_ref())
            .cloned()
            .ok_or_else(|| {
                entry(
                    stage.final_destination.path(),
                    Operation::Write,
                    "S3 stage is not claimed",
                )
            })?;
        if initial.completed {
            return Ok(WriteEvidence {
                persisted_bytes: initial.persisted,
            });
        }
        let upload_id = initial.upload_id.clone();
        let mut buffered = BytesMut::with_capacity(PART_SIZE);
        let mut parts = initial.parts;
        let mut number = parts.iter().map(|part| part.0).max().unwrap_or(0) + 1;
        let result: Result<u64, StorageRoleFailure> = async {
            let mut inflight = futures::stream::FuturesUnordered::new();
            while let Some(chunk) = input.next().await {
                buffered.extend_from_slice(&chunk?);
                while buffered.len() >= PART_SIZE {
                    let part = buffered.split_to(PART_SIZE).freeze();
                    inflight.push(upload(
                        self.protocol.clone(),
                        key.clone(),
                        upload_id.clone(),
                        number,
                        part,
                    ));
                    number += 1;
                    if inflight.len() >= MAX_INFLIGHT_PARTS {
                        let Some(completed) = inflight.next().await else {
                            return Err(entry(
                                stage.final_destination.path(),
                                Operation::Write,
                                "S3 inflight upload disappeared",
                            ));
                        };
                        parts.push(completed.map_err(|e| {
                            role_failure(stage.final_destination.path(), Operation::Write, e)
                        })?);
                    }
                }
            }
            if !buffered.is_empty() || parts.is_empty() {
                inflight.push(upload(
                    self.protocol.clone(),
                    key.clone(),
                    upload_id.clone(),
                    number,
                    buffered.freeze(),
                ));
            }
            while let Some(part) = inflight.next().await {
                parts.push(part.map_err(|e| {
                    role_failure(stage.final_destination.path(), Operation::Write, e)
                })?);
            }
            parts.sort_by_key(|part| part.0);
            self.protocol
                .complete_multipart(&key, &upload_id, &parts)
                .await
                .map_err(|e| role_failure(stage.final_destination.path(), Operation::Write, e))?;
            Ok(parts.iter().map(|_| 0u64).sum())
        }
        .await;
        result?;
        let persisted = self
            .protocol
            .head(&key)
            .await
            .map_err(|e| role_failure(stage.final_destination.path(), Operation::Write, e))?
            .size;
        self.states.lock().await.insert(
            stage.token.to_vec(),
            StageState {
                persisted,
                upload_id,
                parts,
                completed: true,
                claim_key: initial.claim_key,
            },
        );
        Ok(WriteEvidence {
            persisted_bytes: persisted,
        })
    }

    async fn observe_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<CheckpointObservation, StorageRoleFailure> {
        let key = self.validate(stage)?;
        let stage_state = self
            .states
            .lock()
            .await
            .get(stage.token.as_ref())
            .cloned()
            .ok_or_else(|| {
                entry(
                    stage.final_destination.path(),
                    Operation::Prepare,
                    "S3 stage is not claimed",
                )
            })?;
        let durable_prefix = if stage_state.completed {
            stage_state.persisted
        } else {
            let parts = self
                .protocol
                .list_parts(&key, &stage_state.upload_id)
                .await
                .map_err(|e| role_failure(stage.final_destination.path(), Operation::Prepare, e))?;
            let (persisted, parts) = resumable_parts(stage.final_destination.path(), parts)?;
            self.states.lock().await.insert(
                stage.token.to_vec(),
                StageState {
                    persisted,
                    upload_id: stage_state.upload_id,
                    parts,
                    completed: false,
                    claim_key: stage_state.claim_key,
                },
            );
            persisted
        };
        Ok(CheckpointObservation { durable_prefix })
    }

    async fn verify(
        &self,
        stage: &PreparedStage,
        request: VerifyRequest,
    ) -> Result<VerificationEvidence, StorageRoleFailure> {
        let key = self.validate(stage)?;
        let facts = self
            .protocol
            .head(&key)
            .await
            .map_err(|e| role_failure(stage.final_destination.path(), Operation::Verify, e))?;
        if facts.size != request.expected_size {
            return Err(entry(
                stage.final_destination.path(),
                Operation::Verify,
                "S3 staged size mismatch",
            ));
        }
        let mut hasher = blake3::Hasher::new();
        let mut offset = 0;
        while offset < facts.size {
            if request.cancel.is_cancelled() {
                return Err(cancelled(stage.final_destination.path(), Operation::Verify));
            }
            let end = (offset + PART_SIZE as u64).min(facts.size);
            let bytes = self
                .protocol
                .get_range(&key, offset..end)
                .await
                .map_err(|e| role_failure(stage.final_destination.path(), Operation::Verify, e))?;
            hasher.update(&bytes);
            offset = end;
        }
        let actual = *hasher.finalize().as_bytes();
        if actual != request.expected_blake3 {
            return Err(entry(
                stage.final_destination.path(),
                Operation::Verify,
                "S3 staged checksum mismatch",
            ));
        }
        Ok(VerificationEvidence {
            verified_bytes: facts.size,
            blake3: actual,
        })
    }

    async fn publish(
        &self,
        stage: &PreparedStage,
        request: PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        publication::publish(self, stage, request).await
    }
    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        let key = self.validate(&stage)?;
        let stage_state = self
            .states
            .lock()
            .await
            .remove(stage.token.as_ref())
            .ok_or_else(|| {
                entry(
                    stage.final_destination.path(),
                    Operation::Namespace,
                    "S3 stage is not claimed",
                )
            })?;
        if stage_state.completed {
            self.protocol.delete_object(&key).await.map_err(|e| {
                role_failure(stage.final_destination.path(), Operation::Namespace, e)
            })?;
        } else {
            self.protocol
                .abort_multipart(&key, &stage_state.upload_id)
                .await
                .map_err(|e| {
                    role_failure(stage.final_destination.path(), Operation::Namespace, e)
                })?;
        }
        if let Some(claim_key) = stage_state.claim_key {
            self.protocol
                .release_claim(&claim_key)
                .await
                .map_err(|failure| {
                    role_failure(
                        stage.final_destination.path(),
                        Operation::Namespace,
                        failure,
                    )
                })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod manifest_tests {
    use super::*;

    fn part(number: i32, size: u64) -> super::super::S3PartFacts {
        super::super::S3PartFacts {
            number,
            size,
            etag: format!("etag-{number}"),
        }
    }

    #[test]
    fn resumable_manifest_requires_contiguous_full_parts() -> Result<(), Box<dyn std::error::Error>>
    {
        let path = crate::model::StoragePath::new("stage")?;
        let valid = resumable_parts(
            &path,
            vec![part(2, PART_SIZE as u64), part(1, PART_SIZE as u64)],
        );
        assert_eq!(valid?.0, (PART_SIZE * 2) as u64);
        assert!(resumable_parts(&path, vec![part(2, PART_SIZE as u64)]).is_err());
        assert!(resumable_parts(&path, vec![part(1, 17)]).is_err());
        Ok(())
    }
}
