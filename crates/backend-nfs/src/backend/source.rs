use std::error::Error;
use std::fmt::{self, Write};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt as _;
use futures::future::Either;
use futures::stream::{FuturesOrdered, FuturesUnordered};

use crate::model::{
    BackendIdentity, BackendSessionFailure, EntryKind, EntryOperationFailure, FailureClass,
    IdentityStrength, Operation, SourceIdentity, SourceVersion, StoragePath, Transience,
};
use crate::storage::ListingFacts;
use crate::storage::{
    ByteStream, PositionedByteStream, PositionedChunk, ReadRequest, ReadSource, SourceDescriptor,
    StorageRoleFailure,
};
use crate::url_redact::redact_storage_url;

const MAX_ROLE_READ: usize = 1024 * 1024;

#[derive(Clone)]
pub struct NfsSourceObservation {
    pub kind: EntryKind,
    pub size: Option<u64>,
    pub file_handle: Bytes,
    pub content_version: Option<Bytes>,
    pub timestamps: Option<crate::model::TimestampMetadata>,
    pub ownership: Option<crate::storage::InlineOwnership>,
    pub mode: Option<u32>,
    /// Independently mapped principals; one can be known while the other is not.
    pub uid: Option<u32>,
    pub gid: Option<u32>,
}

#[derive(Clone)]
pub struct NfsProtocolFailure {
    pub class: FailureClass,
    pub transience: Transience,
    /// 原始 SDK 原因仅由 NFS adapter 持有；跨 role 边界时转换成有界脱敏诊断。
    pub(super) cause: Option<Arc<nfs_rs::NfsError>>,
    /// The status the server answered with, when the failure came from one — `NFS4ERR_PERM
    /// (permission denied)`: the diagnostic names it, so a refused owner can be told from a
    /// refused permission without a packet capture.
    pub status: Option<Arc<str>>,
    /// The directory level that failed, when it is not the entry the operation names: a missing
    /// parent the write, rename or directory creation had to create first
    /// (`NFSStorage::role_create_all`). The failure is reported at that level, and the
    /// diagnostic names the operation's own entry.
    pub level: Option<Box<StoragePath>>,
}

impl NfsProtocolFailure {
    /// A failure with no server status: the client or transport decided it.
    #[must_use]
    pub const fn new(class: FailureClass, transience: Transience) -> Self {
        Self {
            class,
            transience,
            cause: None,
            status: None,
            level: None,
        }
    }

    /// Reports the failure at the missing parent `level` that failed, unless a deeper call
    /// already named the level that failed.
    #[must_use]
    pub fn at_level(mut self, level: StoragePath) -> Self {
        self.level.get_or_insert_with(|| Box::new(level));
        self
    }

    #[must_use]
    pub const fn protocol() -> Self {
        Self::new(FailureClass::Protocol, Transience::Permanent)
    }

    /// Names the status the server answered with.
    #[must_use]
    pub fn with_status(mut self, status: impl fmt::Debug + fmt::Display) -> Self {
        self.status = Some(Arc::from(format!("{status:?} ({status})")));
        self
    }
}

#[async_trait]
pub trait NfsReadCursor: Send + Sync {
    async fn read_at(&self, offset: u64, count: usize) -> Result<Bytes, NfsProtocolFailure>;
}

#[async_trait]
pub trait NfsSourceProtocol: Send + Sync {
    fn read_concurrency(&self) -> usize {
        4
    }
    fn maximum_read_chunk_bytes(&self) -> usize {
        MAX_ROLE_READ
    }
    async fn describe(
        &self,
        path: &StoragePath,
    ) -> Result<NfsSourceObservation, NfsProtocolFailure>;
    async fn open(
        &self,
        path: &StoragePath,
    ) -> Result<(Box<dyn NfsReadCursor>, Bytes), NfsProtocolFailure>;
}

pub struct NfsReadSourceAdapter {
    protocol: Arc<dyn NfsSourceProtocol>,
    identity: BackendIdentity,
}

impl NfsReadSourceAdapter {
    pub fn new(protocol: Arc<dyn NfsSourceProtocol>, identity: BackendIdentity) -> Self {
        Self { protocol, identity }
    }

    #[cfg(any(test, feature = "test-support"))]
    fn with_protocol(protocol: Arc<dyn NfsSourceProtocol>, identity: BackendIdentity) -> Self {
        Self { protocol, identity }
    }

    async fn descriptor(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        let observed = self
            .protocol
            .describe(path)
            .await
            .map_err(|error| role_failure(path, Operation::Observe, error))?;
        let source_identity = SourceIdentity::new(
            self.identity.clone(),
            IdentityStrength::StableWithinBackend,
            &observed.file_handle,
        )
        .map_err(|_| role_failure(path, Operation::Observe, NfsProtocolFailure::protocol()))?;
        Ok(SourceDescriptor {
            rename_evidence: None,
            path: path.clone(),
            kind: observed.kind,
            size: observed.size,
            source_identity,
            backend_fact: None,
            content_version: observed.content_version,
            inline_timestamps: observed.timestamps,
            inline_mode: observed.mode,
            permissions: super::namespace::permission_facts(
                observed.ownership,
                observed.mode,
                observed.uid,
                observed.gid,
            ),
            inline_ownership: observed.ownership,
            version: SourceVersion::Current,
            listing: ListingFacts::default(),
        })
    }
}

#[async_trait]
impl ReadSource for NfsReadSourceAdapter {
    fn read_concurrency(&self) -> usize {
        self.protocol.read_concurrency().max(1)
    }
    fn supports_read_budget(&self) -> bool {
        true
    }
    fn maximum_read_chunk_bytes(&self) -> usize {
        self.protocol.maximum_read_chunk_bytes().max(1)
    }

    async fn describe(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        self.descriptor(path).await
    }

    fn supports_positioned_read(&self) -> bool {
        true
    }

    async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        request.require_current()?;
        let state = self.open_state(request, false).await?;
        Ok(Box::pin(
            futures::stream::try_unfold(state, read_next)
                .map(|result| result.map(|chunk| chunk.data)),
        ))
    }

    async fn read_positioned(
        &self,
        request: ReadRequest,
    ) -> Result<PositionedByteStream, StorageRoleFailure> {
        request.require_current()?;
        let state = self.open_state(request, true).await?;
        Ok(Box::pin(futures::stream::try_unfold(state, read_next)))
    }
}

impl NfsReadSourceAdapter {
    async fn open_state(
        &self,
        request: ReadRequest,
        unordered: bool,
    ) -> Result<ReadState, StorageRoleFailure> {
        if request.cancel.is_cancelled() {
            return Err(cancelled(&request.path));
        }
        if request.maximum_chunk_bytes == 0 || request.read_inflight == 0 {
            return Err(entry_failure(
                &request.path,
                Operation::Read,
                FailureClass::InvalidInput,
                Transience::Permanent,
            ));
        }
        let range = if let Some(range) = request.range {
            range
        } else {
            let observed = self.descriptor(&request.path).await?;
            0..observed.size.ok_or_else(|| {
                entry_failure(
                    &request.path,
                    Operation::Read,
                    FailureClass::Unsupported,
                    Transience::Permanent,
                )
            })?
        };
        if range.end < range.start {
            return Err(entry_failure(
                &request.path,
                Operation::Read,
                FailureClass::InvalidInput,
                Transience::Permanent,
            ));
        }
        let (cursor, opened_handle) = self
            .protocol
            .open(&request.path)
            .await
            .map_err(|error| role_failure(&request.path, Operation::Read, error))?;
        let opened_identity = SourceIdentity::new(
            self.identity.clone(),
            IdentityStrength::StableWithinBackend,
            opened_handle,
        )
        .map_err(|_| {
            entry_failure(
                &request.path,
                Operation::Read,
                FailureClass::Protocol,
                Transience::Permanent,
            )
        })?;
        if request
            .expected_source
            .as_ref()
            .is_some_and(|expected| expected != &opened_identity)
        {
            return Err(entry_failure(
                &request.path,
                Operation::Read,
                FailureClass::Conflict,
                Transience::Permanent,
            ));
        }
        let state = ReadState {
            cursor: Arc::from(cursor),
            path: request.path,
            next_issue: range.start,
            next_emit: range.start,
            end: range.end,
            maximum_chunk_bytes: request
                .maximum_chunk_bytes
                .min(self.protocol.maximum_read_chunk_bytes().max(1)),
            read_concurrency: request.read_inflight.min(self.read_concurrency()),
            inflight: if unordered {
                Either::Right(FuturesUnordered::new())
            } else {
                Either::Left(FuturesOrdered::new())
            },
            cancel: request.cancel,
            qos: request.source_qos,
            budget: request.read_budget,
        };
        Ok(state)
    }
}

struct ReadState {
    cursor: Arc<dyn NfsReadCursor>,
    path: StoragePath,
    next_issue: u64,
    next_emit: u64,
    end: u64,
    maximum_chunk_bytes: usize,
    read_concurrency: usize,
    inflight: Either<FuturesOrdered<NfsReadFuture>, FuturesUnordered<NfsReadFuture>>,
    cancel: tokio_util::sync::CancellationToken,
    qos: Option<crate::storage::SourceQosBudget>,
    budget: Option<crate::storage::ReadBudget>,
}

impl ReadState {
    fn inflight_len(&self) -> usize {
        match &self.inflight {
            Either::Left(queue) => queue.len(),
            Either::Right(queue) => queue.len(),
        }
    }
}

type NfsReadFuture = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = (
                    u64,
                    usize,
                    Result<Bytes, NfsProtocolFailure>,
                    Option<crate::storage::ReadAdmission>,
                ),
            > + Send,
    >,
>;

async fn fill_read_pipeline(state: &mut ReadState) -> Result<(), StorageRoleFailure> {
    while state.inflight_len() < state.read_concurrency && state.next_issue < state.end {
        if state.cancel.is_cancelled() {
            return Err(cancelled(&state.path));
        }
        let requested = (state.end - state.next_issue).min(state.maximum_chunk_bytes as u64);
        let admission = if let Some(budget) = &state.budget {
            let Some(admission) = budget
                .reserve(
                    usize::try_from(requested).map_err(|_| {
                        entry_failure(
                            &state.path,
                            Operation::Read,
                            FailureClass::InvalidInput,
                            Transience::Permanent,
                        )
                    })?,
                    state.inflight_len() == 0,
                )
                .await
                .map_err(|_| cancelled(&state.path))?
            else {
                break;
            };
            Some(admission)
        } else {
            None
        };
        let granted = state
            .qos
            .as_ref()
            .map_or(requested, |qos| qos.read_size(requested));
        let count = usize::try_from(granted).map_err(|_| {
            entry_failure(
                &state.path,
                Operation::Read,
                FailureClass::InvalidInput,
                Transience::Permanent,
            )
        })?;
        let offset = state.next_issue;
        let cursor = Arc::clone(&state.cursor);
        let cancel = state.cancel.clone();
        let qos = state.qos.clone();
        let future: NfsReadFuture = Box::pin(async move {
            let read = async {
                if let Some(qos) = &qos {
                    qos.admit_read(granted, &cancel).await.map_err(|_| {
                        NfsProtocolFailure::new(FailureClass::Cancelled, Transience::Transient)
                    })?;
                }
                cursor.read_at(offset, count).await
            };
            let result = tokio::select! {
                biased;
                () = cancel.cancelled() => Err(NfsProtocolFailure::new(
                    FailureClass::Cancelled,
                    Transience::Transient,
                )),
                result = read => result,
            };
            (offset, count, result, admission)
        });
        match &mut state.inflight {
            Either::Left(queue) => queue.push_back(future),
            Either::Right(queue) => queue.push(future),
        }
        state.next_issue = state.next_issue.checked_add(granted).ok_or_else(|| {
            entry_failure(
                &state.path,
                Operation::Read,
                FailureClass::InvalidInput,
                Transience::Permanent,
            )
        })?;
    }
    Ok(())
}

async fn read_next(
    mut state: ReadState,
) -> Result<Option<(PositionedChunk, ReadState)>, StorageRoleFailure> {
    fill_read_pipeline(&mut state).await?;
    if state.inflight_len() == 0 {
        return Ok(None);
    }
    let next = tokio::select! {
        biased;
        () = state.cancel.cancelled() => return Err(cancelled(&state.path)),
        result = state.inflight.next() => result,
    };
    let (offset, count, result, admission) = next.ok_or_else(|| {
        entry_failure(
            &state.path,
            Operation::Read,
            FailureClass::Internal,
            Transience::Unknown,
        )
    })?;
    if matches!(state.inflight, Either::Left(_)) && offset != state.next_emit {
        return Err(entry_failure(
            &state.path,
            Operation::Read,
            FailureClass::Internal,
            Transience::Unknown,
        ));
    }
    let bytes = result.map_err(|error| role_failure(&state.path, Operation::Read, error))?;
    if bytes.len() != count {
        return Err(entry_failure(
            &state.path,
            Operation::Read,
            FailureClass::Corruption,
            Transience::Unknown,
        ));
    }
    if let Some(qos) = &state.qos {
        qos.record_read_bytes(bytes.len() as u64);
    }
    state.next_emit = state
        .next_emit
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| {
            entry_failure(
                &state.path,
                Operation::Read,
                FailureClass::InvalidInput,
                Transience::Permanent,
            )
        })?;
    if let (Some(budget), Some(admission)) = (&state.budget, admission) {
        budget.ready(offset, admission);
    }
    Ok(Some((
        PositionedChunk {
            offset,
            data: bytes,
        },
        state,
    )))
}

pub(super) fn cancelled(path: &StoragePath) -> StorageRoleFailure {
    entry_failure(
        path,
        Operation::Read,
        FailureClass::Cancelled,
        Transience::Transient,
    )
}

pub(super) fn role_failure(
    path: &StoragePath,
    operation: Operation,
    error: NfsProtocolFailure,
) -> StorageRoleFailure {
    if matches!(
        error.class,
        FailureClass::Connectivity | FailureClass::Timeout
    ) {
        let diagnostic = diagnostic(format_args!("NFS session failed"), &error);
        return StorageRoleFailure::Session(
            BackendSessionFailure::new(operation, error.class, error.transience, diagnostic)
                .unwrap_or_else(|_| unreachable!("{DIAGNOSTIC_IS_VALID}")),
        );
    }
    entry_scoped(path, operation, error)
}

/// The failure as entry-scoped even when it is a connectivity class — for callers that must not
/// promote it to the session — keeping the server's status in the diagnostic.
pub(super) fn entry_scoped(
    path: &StoragePath,
    operation: Operation,
    error: NfsProtocolFailure,
) -> StorageRoleFailure {
    let plain = diagnostic(format_args!("NFS role failed"), &error);
    let diagnostic = if error.level.as_deref().is_some_and(|level| level != path) {
        parent_context(path, operation, &error)
    } else {
        plain.clone()
    };
    let at = error.level.map_or_else(|| path.clone(), |level| *level);
    let failure = EntryOperationFailure::new(
        at.clone(),
        operation,
        error.class,
        error.transience,
        diagnostic,
    )
    .or_else(|_| EntryOperationFailure::new(at, operation, error.class, error.transience, plain));
    StorageRoleFailure::Entry(failure.unwrap_or_else(|_| unreachable!("{DIAGNOSTIC_IS_VALID}")))
}

/// 父级失败仍报告真实失败层级，诊断保留需要它的原操作及 SDK 原因。
fn parent_context(path: &StoragePath, operation: Operation, error: &NfsProtocolFailure) -> String {
    if path.as_str().len() <= 1024 {
        return diagnostic(
            format_args!(
                "NFS role failed creating a missing parent directory of `{}` ({operation:?})",
                path.as_str()
            ),
            error,
        );
    }
    // 超长上下文不能占满预算而挤掉实际失败原因。
    let mut output = Diagnostic::default();
    let _ = write!(
        output,
        "NFS role failed creating a missing parent directory ({operation:?})"
    );
    append_cause(&mut output, error);
    let _ = write!(output, "; requested entry `{}`", path.as_str());
    output.finish()
}

const MAX_DIAGNOSTIC_BYTES: usize = 4096;
const DIAGNOSTIC_IS_VALID: &str =
    "diagnostics have a fixed nonblank prefix, no NUL and at most 4096 bytes";

pub(super) fn diagnostic(context: fmt::Arguments<'_>, error: &NfsProtocolFailure) -> String {
    let mut output = Diagnostic::default();
    let _ = output.write_fmt(context);
    append_cause(&mut output, error);
    output.finish()
}

fn append_cause(output: &mut Diagnostic, error: &NfsProtocolFailure) {
    // 服务端状态保持原有文本；客户端解码、传输等失败不伪装成 server status。
    if let Some(status) = &error.status {
        let _ = write!(output, ": {status}");
        return;
    }
    let mut cause = error.cause.as_deref().map(|error| error as &dyn Error);
    for _ in 0..8 {
        let Some(error) = cause else { break };
        if write!(output, ": {error}").is_err() {
            break;
        }
        cause = error.source();
    }
}

#[derive(Default)]
struct Diagnostic {
    text: String,
    truncated: bool,
}

impl Write for Diagnostic {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let available = (MAX_DIAGNOSTIC_BYTES - 16).saturating_sub(self.text.len());
        let mut take = available.min(value.len());
        while !value.is_char_boundary(take) {
            take -= 1;
        }
        self.text.push_str(&value[..take]);
        if take < value.len() {
            self.truncated = true;
            return Err(fmt::Error);
        }
        Ok(())
    }
}

impl Diagnostic {
    fn finish(mut self) -> String {
        if self.truncated {
            // 凭据的 @ 可能尚未进入截断片段，不能把不完整 URL 当作无凭据文本。
            if let Some(scheme) = self.text.find("://") {
                self.text.truncate(scheme + 3);
                self.text.push_str("<truncated>");
            }
            self.text.push_str(" …");
        }
        let mut safe = redact_storage_url(&self.text).replace('\0', "?");
        if safe.len() > MAX_DIAGNOSTIC_BYTES {
            let mut end = MAX_DIAGNOSTIC_BYTES;
            while !safe.is_char_boundary(end) {
                end -= 1;
            }
            safe.truncate(end);
        }
        safe
    }
}

pub(super) fn entry_failure(
    path: &StoragePath,
    operation: Operation,
    class: FailureClass,
    transience: Transience,
) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            operation,
            class,
            transience,
            "NFS role failed",
        )
        .unwrap_or_else(|_| unreachable!("static diagnostic is valid")),
    )
}

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;
