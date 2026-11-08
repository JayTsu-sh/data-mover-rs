//! Direct 坏输出清理只信任本次写请求的确认回执；内容相同的旧对象不构成所有权。
use std::sync::{Mutex, PoisonError};

use super::super::source::{answers_version, classified_entry, role_failure};
use super::super::{S3Protocol, S3ProtocolFailure, S3WriteFacts, is_real_version_id};
use super::single::{definite_refusal, same_etag};
use super::{S3StagedDestination, direct, foreign};
use crate::model::{FailureClass, Operation, Transience};
use crate::storage::{PreparedStage, StorageRoleFailure};

#[derive(Clone, Default)]
enum WriteClaim {
    #[default]
    Untouched,
    Uncertain,
    Confirmed {
        size: u64,
        facts: S3WriteFacts,
    },
}

#[derive(Default)]
pub(super) struct DirectCleanup {
    claim: Mutex<WriteClaim>,
}

impl DirectCleanup {
    pub(super) fn attempted(&self) {
        *self.claim.lock().unwrap_or_else(PoisonError::into_inner) = WriteClaim::Uncertain;
    }

    pub(super) fn refused(&self) {
        *self.claim.lock().unwrap_or_else(PoisonError::into_inner) = WriteClaim::Untouched;
    }

    pub(super) fn confirmed(&self, size: u64, facts: &S3WriteFacts, versioning: bool) {
        let exact = facts
            .reported_version
            .as_deref()
            .is_some_and(is_real_version_id);
        // 版本化写入未回显版本时，后续推断可能命中旧内容；显式 null 回执仍可按可变槽复核。
        let reported = facts
            .reported_version
            .as_deref()
            .is_some_and(|id| !id.is_empty());
        if (versioning && !reported) || (!exact && facts.etag.is_empty()) {
            self.attempted();
            return;
        }
        *self.claim.lock().unwrap_or_else(PoisonError::into_inner) = WriteClaim::Confirmed {
            size,
            facts: facts.clone(),
        };
    }
}

fn refused(
    stage: &PreparedStage,
    class: FailureClass,
    transience: Transience,
    reason: &'static str,
) -> StorageRoleFailure {
    classified_entry(
        stage.final_destination().path(),
        Operation::Namespace,
        class,
        transience,
        reason,
    )
}

impl<P: S3Protocol + 'static> S3StagedDestination<P> {
    pub(super) async fn remove_direct_data(
        &self,
        stage: PreparedStage,
    ) -> Result<(), StorageRoleFailure> {
        self.validate(&stage)?;
        if !stage.direct() {
            return Err(foreign(&stage, Operation::Namespace));
        }
        let authority = direct::cleanup_authority(&stage)
            .ok_or_else(|| foreign(&stage, Operation::Namespace))?;
        let claim = authority
            .claim
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        // 先终止本次仍有的 upload／流；普通 discard 不授予删除最终对象的权限。
        self.discard_direct(&stage).await?;
        match claim {
            WriteClaim::Untouched => Ok(()),
            WriteClaim::Uncertain => Err(refused(
                &stage,
                FailureClass::Conflict,
                Transience::Unknown,
                "S3 Direct write outcome is unconfirmed; matching preexisting content cannot authorize deletion",
            )),
            WriteClaim::Confirmed { size, facts } => {
                self.remove_confirmed_direct(&stage, size, facts).await
            }
        }
    }

    async fn remove_confirmed_direct(
        &self,
        stage: &PreparedStage,
        size: u64,
        written: S3WriteFacts,
    ) -> Result<(), StorageRoleFailure> {
        if !self
            .protocol
            .bucket_capabilities()
            .durable_object_publication
        {
            return Err(refused(
                stage,
                FailureClass::Unsupported,
                Transience::Permanent,
                "S3 Direct data cleanup requires a certified object bucket; NAS output is retained",
            ));
        }
        let path = stage.final_destination().path();
        let version = written
            .reported_version
            .as_deref()
            .filter(|id| is_real_version_id(id));
        if !self
            .confirm_direct_output(stage, size, &written, version)
            .await?
        {
            return Ok(());
        }
        let removal = if self.protocol.bucket_capabilities().versions {
            self.protocol
                .delete_version(path.as_str(), version.unwrap_or("null"))
                .await
        } else {
            self.protocol.delete_object(path.as_str()).await
        };
        removal.map_err(|mut error| {
            // 已发出的修改没有可信回复时，不能把网络错误表达成“没有执行”。
            if !definite_refusal(&error) {
                let (S3ProtocolFailure::Entry { transience, .. }
                | S3ProtocolFailure::Session { transience, .. }) = &mut error;
                *transience = Transience::Unknown;
            }
            role_failure(path, Operation::Namespace, error)
        })
    }

    async fn confirm_direct_output(
        &self,
        stage: &PreparedStage,
        size: u64,
        written: &S3WriteFacts,
        version: Option<&str>,
    ) -> Result<bool, StorageRoleFailure> {
        let path = stage.final_destination().path();
        let head = match version {
            Some(id) => self.protocol.head_version(path.as_str(), id).await,
            None => self.protocol.head(path.as_str()).await,
        };
        let current = match head {
            Ok(current) => current,
            Err(S3ProtocolFailure::Entry {
                class: FailureClass::NotFound,
                ..
            }) => return Ok(false),
            Err(error) => return Err(role_failure(path, Operation::Namespace, error)),
        };
        if let Some(id) = version {
            if !answers_version(&current, id) {
                return Err(refused(
                    stage,
                    FailureClass::Unsupported,
                    Transience::Permanent,
                    "S3 Direct cleanup did not receive the requested owned version; retained",
                ));
            }
        } else if current
            .version_id
            .as_deref()
            .is_some_and(is_real_version_id)
            || current.size != size
            || !same_etag(&current.etag, &written.etag)
        {
            return Err(refused(
                stage,
                FailureClass::Conflict,
                Transience::Permanent,
                "S3 Direct output was replaced or changed after its write receipt; retained",
            ));
        }
        Ok(true)
    }
}
