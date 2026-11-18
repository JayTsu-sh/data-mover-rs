//! 声明长度与实际读取量不符时，不能把相同的截断摘要当作完整校验成功。
use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::{Result, described_storage};
use crate::integrity::{
    IntegrityMode, IntegrityOptions, IntegrityPlan, IntegrityRequest, IntegritySide, compare,
};
use crate::model::{FailureClass, Operation, StoragePath};
use crate::storage::{
    ByteStream, ReadRequest, ReadSource, SourceDescriptor, Storage, StorageBuilder,
    StorageRoleFailure,
};

struct DeclaredLength {
    inner: Arc<dyn ReadSource>,
    size: Option<u64>,
}

#[async_trait]
impl ReadSource for DeclaredLength {
    async fn describe(
        &self,
        path: &StoragePath,
    ) -> std::result::Result<SourceDescriptor, StorageRoleFailure> {
        let mut described = self.inner.describe(path).await?;
        described.size = self.size;
        Ok(described)
    }

    async fn read(
        &self,
        request: ReadRequest,
    ) -> std::result::Result<ByteStream, StorageRoleFailure> {
        self.inner.read(request).await
    }
}

fn backend(name: &str, payload: &'static [u8], size: Option<u64>) -> Result<Storage> {
    let original = described_storage(name, payload, None)?;
    Ok(
        StorageBuilder::new(original.identity().clone(), original.capabilities().clone())
            .read_source(Arc::new(DeclaredLength {
                inner: original.read_source()?,
                size,
            }))
            .metadata(original.metadata()?)
            .build()?,
    )
}

fn request(
    payload: &'static [u8],
    source_size: Option<u64>,
    target_size: Option<u64>,
    mode: IntegrityMode,
) -> Result<IntegrityRequest> {
    Ok(IntegrityRequest {
        source: backend("length-source", payload, source_size)?,
        source_path: StoragePath::new("object")?,
        destination: backend("length-target", payload, target_size)?,
        destination_path: StoragePath::new("object")?,
        plan: IntegrityPlan::default(),
        options: IntegrityOptions {
            mode,
            ..IntegrityOptions::default()
        },
        cancel: CancellationToken::new(),
    })
}

#[tokio::test]
async fn equal_truncated_or_oversized_content_is_not_a_successful_full_comparison() -> Result {
    for (payload, declared) in [
        (b"same".as_slice(), 5),
        (b"same".as_slice(), 3),
        (b"".as_slice(), 1),
    ] {
        for side in [IntegritySide::Source, IntegritySide::Destination] {
            let actual = payload.len() as u64;
            let (source_size, target_size) = if side == IntegritySide::Source {
                (declared, actual)
            } else {
                (actual, declared)
            };
            let Err(failure) = compare(request(
                payload,
                Some(source_size),
                Some(target_size),
                IntegrityMode::Content,
            )?)
            .await
            else {
                panic!("invalid read length must fail for {side:?}");
            };
            assert_eq!(failure.side, side);
            assert!(
                matches!(failure.failure, StorageRoleFailure::Entry(ref error)
                if error.class() == FailureClass::Protocol && error.operation() == Operation::Read
                && error.diagnostic().contains(&format!("expected={declared}, actual={actual}")))
            );
        }
        assert!(
            compare(request(
                payload,
                Some(declared),
                Some(declared),
                IntegrityMode::Content
            )?)
            .await
            .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn two_equal_short_streams_are_not_complete_content_evidence() -> Result {
    let result = compare(request(b"same", Some(8), Some(8), IntegrityMode::Content)?).await;
    assert!(
        result.is_err(),
        "two equal short streams must fail: {result:?}"
    );
    Ok(())
}

#[tokio::test]
async fn matching_complete_empty_and_unknown_size_streams_keep_their_contract() -> Result {
    for payload in [b"".as_slice(), b"payload".as_slice()] {
        for size in [Some(payload.len() as u64), None] {
            let report = compare(request(payload, size, size, IntegrityMode::Content)?).await?;
            assert!(report.matches());
            assert_eq!(report.compared_bytes(), payload.len() as u64);
        }
    }
    // 快速校验不读取内容，不通过读取量推断声明错误。
    assert!(
        compare(request(b"same", Some(9), Some(9), IntegrityMode::Metadata)?)
            .await?
            .matches()
    );
    Ok(())
}
