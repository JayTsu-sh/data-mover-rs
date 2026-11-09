//! Settling a write to the final key in one request — a `PutObject`, buffered or streamed — whose
//! reply was lost or late, so it may or may not have committed.
//!
//! MD5／大小匹配可满足当前数据复制，但可能命中相同旧内容，不能当成创建回执。
//! 不透明 `ETag` 的桶只依据 prepare 的对象作未改变核对；两次 HEAD 间的 grace 不保证
//! 服务器此后不会迟提交，现有单写者及设备提交时限前提仍适用。

/// How long settling waits between the two `HEAD`s that find a final key unchanged.
pub(super) const SETTLE_GRACE: Duration = Duration::from_secs(2);

use std::time::Duration;

use tokio::time::sleep;

use super::super::source::role_failure;
use super::super::{S3ObjectFacts, S3Protocol, S3ProtocolFailure, S3WriteFacts};
use super::S3StagedDestination;
use super::completion::observed_facts;
use super::single::{changed, same_etag, unchanged};
use crate::model::{FailureClass, Operation, StoragePath};
use crate::storage::{PublicationFailure, StorageRoleFailure};

/// What the final key held when the stage was prepared: `None` when nothing was there.
pub(super) type Previous = Option<S3ObjectFacts>;

/// What a `HEAD` found: the object, or `None` when there is none.
fn found(head: Result<S3ObjectFacts, S3ProtocolFailure>) -> Result<Previous, S3ProtocolFailure> {
    match head {
        Ok(facts) => Ok(Some(facts)),
        Err(S3ProtocolFailure::Entry {
            class: FailureClass::NotFound,
            ..
        }) => Ok(None),
        Err(failure) => Err(failure),
    }
}

fn same_object(now: &Previous, before: &Previous) -> bool {
    match (now, before) {
        (None, None) => true,
        (Some(now), Some(before)) => {
            now.size == before.size
                && same_etag(&now.etag, &before.etag)
                && now.version_id == before.version_id
        }
        _ => false,
    }
}

impl<P: S3Protocol> S3StagedDestination<P> {
    /// What the final key holds now, on a bucket whose `ETag`s cannot prove an object ours (see
    /// the module documentation); `None` (nothing asked) on any other.
    pub(super) async fn previous_object(
        &self,
        path: &StoragePath,
    ) -> Result<Option<Previous>, StorageRoleFailure> {
        if self.protocol.bucket_capabilities().md5_etags {
            return Ok(None);
        }
        found(self.protocol.head(path.as_str()).await)
            .map(Some)
            .map_err(|failure| role_failure(path, Operation::Prepare, failure))
    }
}

/// How to settle one write: its size, what prepare found at the key, and how long to wait before
/// trusting that the key is unchanged.
pub(super) struct Settling<'a> {
    pub(super) size: u64,
    pub(super) previous: Option<&'a Previous>,
    pub(super) grace: Duration,
}

/// Settles a write whose outcome is unknown, `failure` being how it failed: content matched when the key
/// holds the expected MD5 (`md5`, given only on a bucket whose `ETag`s are MD5s); unchanged when it still
/// holds what prepare found — checked twice, `grace` apart, since the store has the whole body and
/// may commit just after a lost reply; otherwise changed.
pub(super) async fn settle_unknown_write<P: S3Protocol + ?Sized>(
    protocol: &P,
    path: &StoragePath,
    settling: &Settling<'_>,
    md5: Option<&str>,
    failure: S3ProtocolFailure,
) -> Result<S3WriteFacts, PublicationFailure> {
    let now = found(protocol.head(path.as_str()).await);
    if let (Ok(Some(facts)), Some(md5)) = (&now, md5)
        && facts.size == settling.size
        && same_etag(&facts.etag, md5)
    {
        tracing::warn!(path = %path.as_str(), ?failure, "S3 write response unconfirmed; matching data reconciled without a created-version receipt");
        return Ok(observed_facts(facts.etag.clone(), facts.version_id.clone()));
    }
    let Some(before) = settling.previous else {
        return Err(changed(role_failure(path, Operation::Publish, failure)));
    };
    let still = |now: &Result<Previous, S3ProtocolFailure>| {
        now.as_ref().is_ok_and(|now| same_object(now, before))
    };
    if still(&now) {
        sleep(settling.grace).await;
        if still(&found(protocol.head(path.as_str()).await)) {
            return Err(unchanged(role_failure(path, Operation::Publish, failure)));
        }
    }
    Err(changed(role_failure(path, Operation::Publish, failure)))
}
