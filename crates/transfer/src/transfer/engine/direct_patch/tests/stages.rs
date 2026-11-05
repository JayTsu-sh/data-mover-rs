//! 在真实 Local 角色边界暂停，逐阶段检查调用方取消、已发生副作用和后续阶段禁止执行。
use super::*;
use crate::metadata::MetadataPolicy;
use crate::model::{MetadataObservation, MetadataProvenance, OwnershipMode};
use crate::storage::{
    ByteStream, CheckpointObservation, DestinationPrepareRequest, MetadataMutation,
    PublicationEvidence, PublicationFailure, ReadSource, VerificationEvidence, WriteEvidence,
};
use async_trait::async_trait;
use std::os::unix::fs::PermissionsExt;
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Boundary {
    Prepare,
    Patch,
    Verify,
    Metadata,
    Publish,
}

struct GatedDestination {
    inner: Arc<dyn StagedDestination>,
    boundary: Boundary,
    reached: Notify,
    calls: Mutex<Vec<Boundary>>,
    cancel_between_pieces: Option<CancellationToken>,
}

impl GatedDestination {
    async fn enter(&self, boundary: Boundary, cancel: &CancellationToken) {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(boundary);
        if boundary == self.boundary {
            self.reached.notify_one();
            // 回归失败时也不永久挂起测试进程。
            let _ = timeout(Duration::from_secs(2), cancel.cancelled()).await;
        }
    }
}

#[async_trait]
impl StagedDestination for GatedDestination {
    fn supports_direct(&self) -> bool {
        self.inner.supports_direct()
    }
    fn supports_sparse_patch(&self) -> bool {
        self.inner.supports_sparse_patch()
    }

    async fn prepare_direct(
        &self,
        request: PrepareRequest,
        cancel: CancellationToken,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        self.enter(Boundary::Prepare, &cancel).await;
        self.inner.prepare_direct(request, cancel).await
    }

    async fn patch_direct(
        &self,
        stage: &PreparedStage,
        input: PositionedByteStream,
        size: u64,
        cancel: CancellationToken,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        self.enter(Boundary::Patch, &cancel).await;
        let input: PositionedByteStream = if let Some(parent) = self.cancel_between_pieces.clone() {
            Box::pin(input.enumerate().map(move |(index, piece)| {
                if index == 1 {
                    parent.cancel();
                }
                piece
            }))
        } else {
            input
        };
        self.inner.patch_direct(stage, input, size, cancel).await
    }

    async fn verify(
        &self,
        stage: &PreparedStage,
        request: VerifyRequest,
    ) -> Result<VerificationEvidence, StorageRoleFailure> {
        self.enter(Boundary::Verify, &request.cancel).await;
        self.inner.verify(stage, request).await
    }

    async fn apply_metadata(
        &self,
        stage: &PreparedStage,
        mutation: MetadataMutation,
        cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        self.enter(Boundary::Metadata, &cancel).await;
        self.inner.apply_metadata(stage, mutation, cancel).await
    }

    async fn publish(
        &self,
        stage: &PreparedStage,
        request: PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        self.enter(Boundary::Publish, &request.cancel).await;
        self.inner.publish(stage, request).await
    }

    async fn prepare_at_destination(
        &self,
        request: DestinationPrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        self.inner.prepare_at_destination(request).await
    }
    async fn write(
        &self,
        stage: &PreparedStage,
        input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        self.inner.write(stage, input).await
    }
    async fn observe_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<CheckpointObservation, StorageRoleFailure> {
        self.inner.observe_checkpoint(stage).await
    }
    async fn discard(&self, stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        self.inner.discard(stage).await
    }
}

fn gated_storage(
    root: &TempDir,
    boundary: Boundary,
    cancel_between_pieces: Option<CancellationToken>,
) -> TestResult<(Storage, Arc<GatedDestination>)> {
    let local = storage(root)?;
    let destination = Arc::new(GatedDestination {
        inner: local.staged_destination()?,
        boundary,
        reached: Notify::new(),
        calls: Mutex::new(Vec::new()),
        cancel_between_pieces,
    });
    let storage = StorageBuilder::new(local.identity().clone(), local.capabilities().clone())
        .read_source(local.read_source()?)
        .staged_destination(destination.clone())
        .namespace(local.namespace()?)
        .metadata(local.metadata()?)
        .build()?;
    Ok((storage, destination))
}

fn mode_plan(root: &TempDir) -> TestResult<MetadataPlan> {
    let current = metadata(root.path().join("file"))?;
    let mut observations = MetadataObservations::default();
    observations.set_ownership_mode(MetadataObservation::Value {
        value: OwnershipMode {
            uid: current.uid(),
            gid: current.gid(),
            mode: 0o640,
        },
        provenance: MetadataProvenance::Inline,
    });
    Ok(compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: MetadataTarget {
            acl: AclTarget::NotApplicable,
            xattrs: ValueTarget::NotApplicable,
            tags: ValueTarget::NotApplicable,
            ownership_mode: OwnershipTarget::ModeOnly,
            timestamps: TimestampTargetCapability::NotApplicable,
        },
        policies: MetadataPolicies::default().with_ownership_mode(MetadataPolicy::AllowKnownLoss),
        principal_mapper: None,
    })?)
}

#[tokio::test]
async fn cancellation_at_each_commit_boundary_stops_later_phases_and_preserves_actual_effects()
-> TestResult {
    let order = [
        Boundary::Prepare,
        Boundary::Patch,
        Boundary::Verify,
        Boundary::Metadata,
        Boundary::Publish,
    ];
    for boundary in [
        Boundary::Prepare,
        Boundary::Patch,
        Boundary::Metadata,
        Boundary::Publish,
    ] {
        let root = tempfile::tempdir()?;
        let path = root.path().join("file");
        write(&path, b"AAAABBBB")?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        let (storage, destination) = gated_storage(&root, boundary, None)?;
        let cancel = CancellationToken::new();
        let mut session = session_with_cancel(storage, 8, cancel.clone()).await?;
        session.plan = mode_plan(&root)?;
        let mut input: DirectPatchInput = Box::pin(stream::iter([DirectPatchChunk {
            offset: 0,
            data: Bytes::from_static(b"AAAAXXXX"),
            unchanged_basis: false,
        }]));
        session.receive(&mut input).await?;
        let completion = tokio::spawn(session.complete());
        timeout(Duration::from_secs(1), destination.reached.notified()).await?;
        cancel.cancel();
        let result = timeout(Duration::from_secs(1), completion).await??;
        let error = result.err().ok_or("取消不得报告成功")?;
        assert!(error.is_cancelled(), "{boundary:?} 必须保留准确的取消分类");
        let expected = order
            .iter()
            .position(|value| *value == boundary)
            .ok_or("missing boundary")?
            + 1;
        assert_eq!(
            *destination
                .calls
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
            order[..expected]
        );
        let patched = matches!(boundary, Boundary::Metadata | Boundary::Publish);
        assert_eq!(
            read(&path)?,
            if patched { b"AAAAXXXX" } else { b"AAAABBBB" }
        );
        let mode = if boundary == Boundary::Publish {
            0o640
        } else {
            0o600
        };
        assert_eq!(metadata(&path)?.mode() & 0o777, mode);
        assert_eq!(
            std::fs::read_dir(root.path())?.count(),
            1,
            "不得遗留私有暂存文件"
        );
    }
    Ok(())
}

#[tokio::test]
async fn cancellation_after_first_native_patch_preserves_only_the_applied_prefix() -> TestResult {
    const PIECE: usize = 1024 * 1024;
    let root = tempfile::tempdir()?;
    let path = root.path().join("file");
    write(&path, vec![b'A'; 2 * PIECE])?;
    let inode = metadata(&path)?.ino();
    let cancel = CancellationToken::new();
    let (storage, destination) = gated_storage(&root, Boundary::Verify, Some(cancel.clone()))?;
    let mut session = session_with_cancel(storage, 2 * PIECE, cancel).await?;
    let mut input: DirectPatchInput = Box::pin(stream::iter([DirectPatchChunk {
        offset: 0,
        data: Bytes::from(vec![b'X'; 2 * PIECE]),
        unchanged_basis: false,
    }]));
    session.receive(&mut input).await?;
    let error = session
        .complete()
        .await
        .err()
        .ok_or("中途取消不能成功提交")?;
    assert!(error.is_cancelled());
    let after = read(&path)?;
    assert_eq!(after[..PIECE], vec![b'X'; PIECE]);
    assert_eq!(after[PIECE..], vec![b'A'; PIECE]);
    assert_eq!(metadata(&path)?.ino(), inode);
    assert_eq!(
        *destination
            .calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner),
        [Boundary::Prepare, Boundary::Patch]
    );
    Ok(())
}

struct GatedSource {
    inner: Arc<dyn ReadSource>,
    reached: Notify,
    read_cancel: Mutex<Option<CancellationToken>>,
}

#[async_trait]
impl ReadSource for GatedSource {
    async fn describe(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.inner.describe(path).await
    }

    async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        *self
            .read_cancel
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(request.cancel.clone());
        self.reached.notify_one();
        let _ = timeout(Duration::from_secs(2), request.cancel.cancelled()).await;
        self.inner.read(request).await
    }
}

#[tokio::test]
async fn basis_read_receives_caller_cancellation_and_cannot_publish() -> TestResult {
    let root = tempfile::tempdir()?;
    write(root.path().join("file"), b"AAAABBBB")?;
    let local = storage(&root)?;
    let source = Arc::new(GatedSource {
        inner: local.read_source()?,
        reached: Notify::new(),
        read_cancel: Mutex::new(None),
    });
    let storage = StorageBuilder::new(local.identity().clone(), local.capabilities().clone())
        .read_source(source.clone())
        .staged_destination(local.staged_destination()?)
        .namespace(local.namespace()?)
        .metadata(local.metadata()?)
        .build()?;
    let cancel = CancellationToken::new();
    let mut session = session_with_cancel(storage, 8, cancel.clone()).await?;
    let mut input: DirectPatchInput = Box::pin(stream::iter([DirectPatchChunk {
        offset: 0,
        data: Bytes::from_static(b"AAAAXXXX"),
        unchanged_basis: false,
    }]));
    let (result, ()) = tokio::join!(
        timeout(Duration::from_secs(1), session.receive(&mut input)),
        async {
            source.reached.notified().await;
            cancel.cancel();
        }
    );
    assert!(matches!(result?, Err(DirectPatchError::Cancelled)));
    assert!(
        source
            .read_cancel
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
    );
    assert!(session.complete().await.is_err());
    assert_eq!(read(root.path().join("file"))?, b"AAAABBBB");
    Ok(())
}
