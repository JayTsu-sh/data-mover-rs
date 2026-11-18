//! 已核验目录句柄上的元数据修改。所有 RPC 均以同一个 FH 为目标。
use std::future::Future;

use bytes::Bytes;
use nfs_rs::Time;
use tokio_util::sync::CancellationToken;

use super::{NFSStorage, i64_to_time, role_metadata::invalid_acl};
use crate::model::acl::equivalent_native;
use crate::model::{
    AclEncoding, AclMetadata, ExtendedAttribute, FailureClass, StorageTimestamp, Transience,
};
use crate::storage::MetadataMutation;
use crate::storage::backends::nfs::acl;
use crate::storage::backends::nfs::protocol::{classify_error, classify_mutation_error};
use crate::storage::backends::nfs::source::NfsProtocolFailure;

impl NFSStorage {
    pub(super) async fn apply_bound_directory_mutation(
        &self,
        handle: &Bytes,
        mutation: MetadataMutation,
        cancel: &CancellationToken,
    ) -> Result<(), NfsProtocolFailure> {
        check_cancel(cancel, Transience::Transient)?;
        // 已核验的句柄失效时直接返回错误，避免 SETATTR 的普通按路径重新查找。
        match mutation {
            MetadataMutation::NumericOwnership(value) => self
                .mount
                .setattr(
                    handle.clone(),
                    None,
                    Some(value.mode),
                    Some(value.uid),
                    Some(value.gid),
                    None,
                    None,
                    None,
                )
                .await
                .map_err(classify_mutation_error),
            MetadataMutation::Mode(mode) => self
                .mount
                .setattr(
                    handle.clone(),
                    None,
                    Some(mode),
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .map_err(classify_mutation_error),
            MetadataMutation::Timestamps(times) if times.created.is_none() => self
                .mount
                .setattr(
                    handle.clone(),
                    None,
                    None,
                    None,
                    None,
                    None,
                    wire_time(times.accessed)?,
                    wire_time(times.modified)?,
                )
                .await
                .map_err(classify_mutation_error),
            MetadataMutation::Acl(value) => self.apply_bound_acl(handle, &value, cancel).await,
            MetadataMutation::Xattrs(values) if self.supports_xattr() => {
                apply_bound_xattrs(&values, cancel, |name, value| {
                    self.mount.setxattr(handle.clone(), name, value)
                })
                .await
            }
            _ => Err(NfsProtocolFailure::new(
                FailureClass::Unsupported,
                Transience::Permanent,
            )),
        }
    }

    async fn apply_bound_acl(
        &self,
        handle: &Bytes,
        value: &AclMetadata,
        cancel: &CancellationToken,
    ) -> Result<(), NfsProtocolFailure> {
        let Some(encoding) = self.acl_encoding() else {
            return Err(NfsProtocolFailure::new(
                FailureClass::Unsupported,
                Transience::Permanent,
            ));
        };
        if value.encoding() != encoding {
            return Err(invalid_acl());
        }
        equivalent_native(value, value).map_err(|_| invalid_acl())?;
        match encoding {
            AclEncoding::Posix => self.apply_bound_posix_acl(handle, value, cancel).await,
            AclEncoding::NfsV4 => {
                let native = acl::decode(value).map_err(|_| invalid_acl())?;
                self.mount
                    .setacl(handle.clone(), &native)
                    .await
                    .map_err(classify_mutation_error)?;
                verify_written_acl(async {
                    let actual = self
                        .mount
                        .getacl(handle.clone())
                        .await
                        .map_err(classify_error)?;
                    let actual =
                        acl::encode(&actual).map_err(|_| NfsProtocolFailure::protocol())?;
                    Ok((value.clone(), actual))
                })
                .await
            }
            AclEncoding::WindowsSecurityDescriptor => Err(invalid_acl()),
        }
    }

    async fn apply_bound_posix_acl(
        &self,
        handle: &Bytes,
        value: &AclMetadata,
        cancel: &CancellationToken,
    ) -> Result<(), NfsProtocolFailure> {
        let mut planned = acl::decode_posix(value).map_err(|_| invalid_acl())?;
        if planned.access.is_empty() {
            let attrs = self
                .mount
                .getattr(handle.clone())
                .await
                .map_err(classify_error)?;
            planned.access = acl::posix_from_mode(attrs.file_mode);
        }
        check_cancel(cancel, Transience::Transient)?;
        self.mount
            .set_posix_acl(handle.clone(), &planned)
            .await
            .map_err(classify_mutation_error)?;
        verify_written_acl(async {
            let mut actual = self
                .mount
                .get_posix_acl(handle.clone())
                .await
                .map_err(classify_error)?;
            if actual.access.is_empty() {
                let attrs = self
                    .mount
                    .getattr(handle.clone())
                    .await
                    .map_err(classify_error)?;
                actual.access = acl::posix_from_mode(attrs.file_mode);
            }
            let planned =
                acl::encode_posix(&planned).map_err(|_| NfsProtocolFailure::protocol())?;
            let actual = acl::encode_posix(&actual).map_err(|_| NfsProtocolFailure::protocol())?;
            Ok((planned, actual))
        })
        .await
    }
}

async fn apply_bound_xattrs<'a, Set, Write>(
    values: &'a [ExtendedAttribute],
    cancel: &CancellationToken,
    mut set: Set,
) -> Result<(), NfsProtocolFailure>
where
    Set: FnMut(&'a str, Bytes) -> Write,
    Write: Future<Output = nfs_rs::Result<()>>,
{
    let names = values
        .iter()
        .map(|value| std::str::from_utf8(value.name()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| NfsProtocolFailure::new(FailureClass::InvalidInput, Transience::Permanent))?;
    for (index, (name, value)) in names.into_iter().zip(values).enumerate() {
        let prior_writes = index != 0;
        let transience = if prior_writes {
            Transience::Unknown
        } else {
            Transience::Transient
        };
        check_cancel(cancel, transience)?;
        set(name, Bytes::copy_from_slice(value.value()))
            .await
            .map_err(classify_mutation_error)
            .map_err(|error| {
                if prior_writes {
                    unconfirmed_family(error)
                } else {
                    error
                }
            })?;
    }
    Ok(())
}

/// SET 已成功；后续失败表示当前属性族未获完整确认，不能作为明确拒绝继续。
async fn verify_written_acl(
    readback: impl Future<Output = Result<(AclMetadata, AclMetadata), NfsProtocolFailure>>,
) -> Result<(), NfsProtocolFailure> {
    let (planned, actual) = readback.await.map_err(unconfirmed_family)?;
    let equivalent = equivalent_native(&planned, &actual)
        .map_err(|_| unconfirmed_family(NfsProtocolFailure::protocol()))?;
    if !equivalent {
        return Err(unconfirmed_family(
            NfsProtocolFailure::protocol().with_status("ACL_READBACK_MISMATCH"),
        ));
    }
    Ok(())
}

fn unconfirmed_family(mut error: NfsProtocolFailure) -> NfsProtocolFailure {
    error.transience = Transience::Unknown;
    error
}

fn check_cancel(
    cancel: &CancellationToken,
    transience: Transience,
) -> Result<(), NfsProtocolFailure> {
    if cancel.is_cancelled() {
        return Err(NfsProtocolFailure::new(FailureClass::Cancelled, transience));
    }
    Ok(())
}

fn wire_time(time: Option<StorageTimestamp>) -> Result<Option<Time>, NfsProtocolFailure> {
    time.map(|time| {
        let nanos = i64::try_from(time.unix_nanos()).map_err(|_| {
            NfsProtocolFailure::new(FailureClass::InvalidInput, Transience::Permanent)
        })?;
        i64_to_time(nanos)
            .map_err(|_| NfsProtocolFailure::new(FailureClass::InvalidInput, Transience::Permanent))
    })
    .transpose()
}

#[cfg(test)]
mod tests {
    use std::{future::ready, sync::Arc};

    use nfs_rs::{Nfs4ErrorCode, NfsError, PosixAcl};

    use super::*;

    #[tokio::test]
    async fn acl_readback_refusal_keeps_sdk_cause_but_is_not_a_clean_write_refusal()
    -> Result<(), &'static str> {
        let readback = classify_error(NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_ACCESS));
        let expected_cause = readback.cause.clone().ok_or("NFS read cause")?;
        let expected_status = readback.status.clone();
        let failure = verify_written_acl(ready(Err(readback)))
            .await
            .err()
            .ok_or("failed ACL readback must stop application")?;
        assert_eq!(failure.class, FailureClass::PermissionDenied);
        assert_eq!(failure.transience, Transience::Unknown);
        assert_eq!(failure.status, expected_status);
        let cause = failure.cause.as_ref().ok_or("preserved NFS read cause")?;
        assert!(Arc::ptr_eq(cause, &expected_cause));
        Ok(())
    }

    #[tokio::test]
    async fn acl_readback_mismatch_is_an_unconfirmed_family() -> Result<(), &'static str> {
        let acl = |mode| {
            acl::encode_posix(&PosixAcl {
                access: acl::posix_from_mode(mode),
                default: Vec::new(),
            })
            .map_err(|_| "valid POSIX permissions")
        };
        let failure = verify_written_acl(ready(Ok((acl(0o750)?, acl(0o700)?))))
            .await
            .err()
            .ok_or("ACL mismatch")?;
        assert_eq!(failure.class, FailureClass::Protocol);
        assert_eq!(failure.transience, Transience::Unknown);
        assert!(
            failure
                .status
                .is_some_and(|status| status.contains("ACL_READBACK_MISMATCH"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_later_xattr_refusal_marks_the_family_partial_and_stops_further_writes()
    -> Result<(), &'static str> {
        let values = [b"user.first", b"user.other", b"user.third"]
            .map(|name| ExtendedAttribute::new(name.to_vec(), b"value".to_vec()));
        let [first, second, third] = values;
        let values = [
            first.map_err(|_| "first xattr")?,
            second.map_err(|_| "second xattr")?,
            third.map_err(|_| "third xattr")?,
        ];
        for (at, transience) in [(1, Transience::Permanent), (2, Transience::Unknown)] {
            let mut writes = Vec::new();
            let failure = apply_bound_xattrs(&values, &CancellationToken::new(), |name, _| {
                writes.push(name);
                ready(if writes.len() == at {
                    Err(NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_PERM))
                } else {
                    Ok(())
                })
            })
            .await
            .err()
            .ok_or("expected xattr refusal")?;
            assert_eq!(failure.class, FailureClass::PermissionDenied);
            assert_eq!(failure.transience, transience);
            assert!(
                failure
                    .status
                    .is_some_and(|status| status.contains("NFS4ERR_PERM"))
            );
            assert!(failure.cause.is_some());
            assert_eq!(writes, ["user.first", "user.other"][..at]);
        }
        Ok(())
    }
}
