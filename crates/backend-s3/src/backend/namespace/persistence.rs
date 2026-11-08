//! 对象发布的持久化由对象桶契约保证；只读 HEAD 复核已校验对象，绝不改写或重读数据。

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::super::listing::file_descriptor;
use super::super::source::{answers_version, role_failure};
use super::{S3Namespace, S3Protocol, S3ProtocolFailure, refuse_artifact};
use crate::model::{EntryKind, FailureClass, IdentityStrength, Operation, StoragePath, Transience};
use crate::storage::{SourceDescriptor, StorageRoleFailure};

impl<P: S3Protocol + 'static> S3Namespace<P> {
    pub(super) async fn persist_entry(
        &self,
        expected: &SourceDescriptor,
        cancel: &CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        tokio::select! {
            biased;
            () = cancel.cancelled() => Err(role_failure(
                &expected.path, Operation::Publish,
                S3ProtocolFailure::entry(FailureClass::Cancelled, Transience::Transient,
                    "S3 persistence confirmation cancelled; no object mutation was sent"),
            )),
            result = self.confirm_published_object(expected) => result,
        }
    }

    fn validate_persistence(&self, expected: &SourceDescriptor) -> Result<(), StorageRoleFailure> {
        refuse_artifact(&expected.path)?;
        let bucket = self.protocol.bucket_capabilities();
        if !bucket.durable_object_publication
            || expected.path.as_str().is_empty()
            || expected.kind != EntryKind::File
            || expected.listing.listing_only
            || expected.source_identity.backend() != &self.identity
            || !matches!(
                expected.source_identity.strength(),
                IdentityStrength::PathScoped | IdentityStrength::VersionScoped
            )
            || expected.size.is_none()
            || expected
                .content_version
                .as_ref()
                .is_none_or(Bytes::is_empty)
            || expected
                .version
                .version_id()
                .is_some_and(|id| id.is_empty() || !bucket.versions)
        {
            return Err(rejected(
                &expected.path,
                FailureClass::Unsupported,
                "S3 persistence requires a certified object bucket and a bound data object with size and ETag; virtual prefixes, delete markers and NAS buckets are unsupported",
            ));
        }
        Ok(())
    }

    async fn confirm_published_object(
        &self,
        expected: &SourceDescriptor,
    ) -> Result<(), StorageRoleFailure> {
        self.validate_persistence(expected)?;
        let facts = match expected.version.version_id() {
            Some(id) => self.protocol.head_version(expected.path.as_str(), id).await,
            None => self.protocol.head(expected.path.as_str()).await,
        }
        .map_err(|error| role_failure(&expected.path, Operation::Publish, error))?;
        if let Some(id) = expected.version.version_id()
            && !answers_version(&facts, id)
        {
            return Err(rejected(
                &expected.path,
                FailureClass::Unsupported,
                "the S3 store did not answer with the requested persistence version",
            ));
        }
        let current = file_descriptor(&self.identity, expected.path.clone(), &facts)
            .map_err(StorageRoleFailure::Entry)?;
        if current.source_identity != expected.source_identity
            || current.size != expected.size
            || current.content_version != expected.content_version
            || expected
                .inline_timestamps
                .and_then(|times| times.modified)
                .is_some_and(|modified| facts.last_modified != Some(modified))
        {
            return Err(rejected(
                &expected.path,
                FailureClass::Conflict,
                "S3 target object changed after verification; persistence was not confirmed",
            ));
        }
        Ok(())
    }
}

fn rejected(path: &StoragePath, class: FailureClass, diagnostic: &str) -> StorageRoleFailure {
    role_failure(
        path,
        Operation::Publish,
        S3ProtocolFailure::entry(class, Transience::Permanent, diagnostic),
    )
}
