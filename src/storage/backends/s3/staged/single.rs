//! Objects up to the single-PUT threshold T (ADR-0006 C14b): one `PutObject` with `Content-MD5`
//! to the final key, no upload, no temp key, no resume.
//!
//! The stage lives in `PreparedStage::backend_state`, not in the adapter's stage map, so a stage
//! the caller abandons takes its buffered bytes with it. `write` buffers the content, `publish`
//! sends it, and read-back verification happens after publication, pinned to what the PUT
//! created: its version in a versioned bucket, otherwise `If-Match` on its `ETag`.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use md5::{Digest as _, Md5};

use super::super::source::{cancelled, classified_entry, entry, role_failure};
use super::super::{
    S3ObjectFacts, S3Protocol, S3ProtocolFailure, S3WriteFacts, is_real_version_id,
};
use super::{PART_SIZE, S3StagedDestination};
use crate::model::{FailureClass, ObjectTag, Operation, StoragePath, Transience};
use crate::storage::{
    ByteStream, MetadataMutation, PrepareRequest, PreparedStage, PublicationDisposition,
    PublicationEvidence, PublicationFailure, PublishRequest, StorageRoleFailure,
    VerificationEvidence, VerifyRequest, WriteEvidence,
};

/// The default single-PUT threshold T: 8 MiB.
pub(crate) const DEFAULT_SINGLE_PUT_THRESHOLD: u64 = 8 * 1024 * 1024;
/// The smallest T a configuration may choose: the multipart minimum part size, 5 MiB.
pub(crate) const MIN_SINGLE_PUT_THRESHOLD: u64 = 5 * 1024 * 1024;
/// The largest T a configuration may choose: the `PutObject` limit, 5 GiB.
pub(crate) const MAX_SINGLE_PUT_THRESHOLD: u64 = 5 * 1024 * 1024 * 1024;

/// The upload-id half of a single stage's token. A real upload id is printable, so it never
/// starts with a control character.
pub(super) const SINGLE_MARKER: &str = "\u{1}single-put";

/// A configured single-PUT threshold outside `[5 MiB, 5 GiB]`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InvalidSinglePutThreshold(u64);

impl fmt::Display for InvalidSinglePutThreshold {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid configuration: S3 single_put_threshold {} is outside \
             [{MIN_SINGLE_PUT_THRESHOLD}, {MAX_SINGLE_PUT_THRESHOLD}] bytes",
            self.0
        )
    }
}

impl std::error::Error for InvalidSinglePutThreshold {}

/// The configured single-PUT threshold, or the default when none is configured.
///
/// # Errors
/// A value outside `[5 MiB, 5 GiB]`.
pub(crate) fn single_put_threshold(
    configured: Option<u64>,
) -> Result<u64, InvalidSinglePutThreshold> {
    let threshold = configured.unwrap_or(DEFAULT_SINGLE_PUT_THRESHOLD);
    if (MIN_SINGLE_PUT_THRESHOLD..=MAX_SINGLE_PUT_THRESHOLD).contains(&threshold) {
        Ok(threshold)
    } else {
        Err(InvalidSinglePutThreshold(threshold))
    }
}

/// One object on its way to a single `PutObject`.
pub(super) struct SingleStage {
    expected_size: u64,
    state: Mutex<SingleState>,
}

#[derive(Default)]
struct SingleState {
    written: Option<Bytes>,
    tags: Option<Vec<ObjectTag>>,
    published: Option<S3WriteFacts>,
    /// A native copy filled this stage through the temp-key path instead (until C18).
    native: bool,
}

impl SingleStage {
    fn lock(&self) -> MutexGuard<'_, SingleState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Hands the stage to the temp-key path: a native copy fills it there.
    pub(super) fn mark_native(&self) {
        self.lock().native = true;
    }

    /// The buffered content, which must be `expected_size` bytes, and the tags waiting for it.
    fn content(
        &self,
        path: &StoragePath,
        expected_size: u64,
    ) -> Result<(Bytes, Option<Vec<ObjectTag>>), PublicationFailure> {
        let guard = self.lock();
        let body = guard
            .written
            .clone()
            .filter(|body| body.len() as u64 == expected_size)
            .ok_or_else(|| {
                unchanged(entry(
                    path,
                    Operation::Publish,
                    "S3 object content differs from the published size",
                ))
            })?;
        Ok((body, guard.tags.clone()))
    }

    /// The source size the stage was prepared for.
    pub(super) fn expected_size(&self) -> u64 {
        self.expected_size
    }

    /// What the `PutObject` reported, once it was sent.
    pub(super) fn published(&self) -> Option<S3WriteFacts> {
        self.lock().published.clone()
    }

    /// Bytes the object holds once sent, otherwise bytes buffered so far.
    pub(super) fn durable_len(&self) -> u64 {
        if self.lock().published.is_some() {
            self.expected_size
        } else {
            self.written_len()
        }
    }

    pub(super) fn written_len(&self) -> u64 {
        self.lock()
            .written
            .as_ref()
            .map_or(0, |bytes| bytes.len() as u64)
    }
}

/// The single stage `stage` is, unless a native copy moved it to the temp-key path.
pub(super) fn of(stage: &PreparedStage) -> Option<&SingleStage> {
    stage
        .backend_state
        .as_deref()?
        .downcast_ref::<SingleStage>()
        .filter(|single| !single.lock().native)
}

impl<P: S3Protocol> S3StagedDestination<P> {
    /// Whether a source of `size` bytes goes as one `PutObject`.
    pub(super) fn is_single_put(&self, size: Option<u64>) -> bool {
        matches!((size, self.single_put_threshold), (Some(size), Some(limit)) if size <= limit)
    }

    /// A fresh single stage: nothing is sent until publication, and nothing can be resumed.
    pub(super) fn prepare_single(&self, request: PrepareRequest, size: u64) -> PreparedStage {
        let token = Self::encode_token(&Self::temp_key(&request), SINGLE_MARKER);
        let mut stage = PreparedStage::new(
            self.identity.clone(),
            request.final_destination,
            token,
            request.recovery_binding,
            0,
            None,
        );
        stage.backend_state = Some(Arc::new(SingleStage {
            expected_size: size,
            state: Mutex::new(SingleState::default()),
        }));
        stage.disable_recovery()
    }
}

/// Buffers the whole content; more bytes than the source size is `InvalidInput`.
pub(super) async fn write(
    stage: &PreparedStage,
    single: &SingleStage,
    mut input: ByteStream,
) -> Result<WriteEvidence, StorageRoleFailure> {
    let path = stage.final_destination.path();
    let capacity = usize::try_from(single.expected_size)
        .map_err(|_| entry(path, Operation::Write, "S3 object cannot fit address space"))?;
    let mut buffer = BytesMut::with_capacity(capacity);
    while let Some(chunk) = input.next().await {
        let chunk = chunk?;
        if (buffer.len() + chunk.len()) as u64 > single.expected_size {
            return Err(oversized(path));
        }
        buffer.extend_from_slice(&chunk);
    }
    store(stage, single, buffer.freeze())
}

/// Buffers one complete chunk without copying it.
pub(super) fn write_single(
    stage: &PreparedStage,
    single: &SingleStage,
    data: Bytes,
) -> Result<WriteEvidence, StorageRoleFailure> {
    if data.len() as u64 > single.expected_size {
        return Err(oversized(stage.final_destination.path()));
    }
    store(stage, single, data)
}

fn store(
    stage: &PreparedStage,
    single: &SingleStage,
    content: Bytes,
) -> Result<WriteEvidence, StorageRoleFailure> {
    let persisted_bytes = content.len() as u64;
    let mut guard = single.lock();
    if guard.published.is_some() {
        return Err(entry(
            stage.final_destination.path(),
            Operation::Write,
            "S3 object is already published",
        ));
    }
    guard.written = Some(content);
    Ok(WriteEvidence { persisted_bytes })
}

fn oversized(path: &StoragePath) -> StorageRoleFailure {
    classified_entry(
        path,
        Operation::Write,
        FailureClass::InvalidInput,
        Transience::Permanent,
        "more bytes than the source size were written to a single-PUT S3 object",
    )
}

/// Keeps object tags for the PUT: the object does not exist before publication.
pub(super) fn apply_metadata(
    stage: &PreparedStage,
    single: &SingleStage,
    tags_supported: bool,
    mutation: MetadataMutation,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<(), StorageRoleFailure> {
    let tags = pending_tags(
        stage.final_destination.path(),
        tags_supported,
        mutation,
        cancel,
    )?;
    single.lock().tags = Some(tags);
    Ok(())
}

/// The tags a mutation sets on an object that does not exist yet, to be set right after it is
/// written; any other mutation is refused, as the S3 metadata role refuses it.
pub(super) fn pending_tags(
    path: &StoragePath,
    tags_supported: bool,
    mutation: MetadataMutation,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<Vec<ObjectTag>, StorageRoleFailure> {
    if cancel.is_cancelled() {
        return Err(cancelled(path, Operation::Metadata));
    }
    match mutation {
        MetadataMutation::Tags(tags) if tags_supported => Ok(tags),
        MetadataMutation::Tags(_) => Err(classified_entry(
            path,
            Operation::Metadata,
            FailureClass::Unsupported,
            Transience::Permanent,
            "object tags are unsupported by this S3 compatibility profile",
        )),
        _ => Err(entry(
            path,
            Operation::Metadata,
            "metadata kind is unsupported by S3",
        )),
    }
}

/// One `PutObject` of the buffered content to the final key.
pub(super) async fn publish<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    stage: &PreparedStage,
    single: &SingleStage,
    request: &PublishRequest,
) -> Result<PublicationEvidence, PublicationFailure> {
    let path = stage.final_destination.path();
    adapter.validate(stage).map_err(unchanged)?;
    if request.cancel.is_cancelled() {
        return Err(unchanged(cancelled(path, Operation::Publish)));
    }
    let facts = send(adapter, path, single, request.expected_size).await?;
    Ok(PublicationEvidence {
        final_destination: path.clone(),
        disposition: PublicationDisposition::Published,
        version: facts.version_id,
    })
}

/// Sends the buffered content, which must be `expected_size` bytes, as one `PutObject` with
/// `Content-MD5` and sets the pending tags; the object is then visible at `path`.
pub(super) async fn send<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    path: &StoragePath,
    single: &SingleStage,
    expected_size: u64,
) -> Result<S3WriteFacts, PublicationFailure> {
    let (body, tags) = single.content(path, expected_size)?;
    let digest = Md5::digest(&body);
    let quoted_md5 = format!("\"{digest:x}\"");
    let facts = match adapter
        .protocol
        .put_object(path.as_str(), body, &BASE64_STANDARD.encode(digest))
        .await
    {
        Ok(facts) => facts,
        Err(failure) if definite_refusal(&failure) => {
            return Err(unchanged(role_failure(path, Operation::Publish, failure)));
        }
        Err(failure) => reconcile(adapter, path, expected_size, &quoted_md5, failure).await?,
    };
    if let Some(tags) = tags {
        adapter
            .protocol
            .put_tags(path.as_str(), &tags)
            .await
            .map_err(|failure| changed(role_failure(path, Operation::Publish, failure)))?;
    }
    let mut guard = single.lock();
    guard.published = Some(facts.clone());
    // The content is on the server now; verification reads it back from there.
    guard.written = None;
    Ok(facts)
}

/// A refusal the service answered before writing anything; every other failure (a lost or
/// malformed reply, a server error) may have stored the object.
pub(super) fn definite_refusal(failure: &S3ProtocolFailure) -> bool {
    let (S3ProtocolFailure::Entry { class, .. } | S3ProtocolFailure::Session { class, .. }) =
        failure;
    matches!(
        class,
        FailureClass::Authentication
            | FailureClass::PermissionDenied
            | FailureClass::NotFound
            | FailureClass::Corruption
            | FailureClass::InvalidInput
            | FailureClass::Conflict
            | FailureClass::Unsupported
    )
}

/// Settles a PUT whose reply was lost: the final object is ours when it has our size and our
/// MD5 as its `ETag`.
async fn reconcile<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    path: &StoragePath,
    expected_size: u64,
    quoted_md5: &str,
    put_failure: S3ProtocolFailure,
) -> Result<S3WriteFacts, PublicationFailure> {
    match adapter.protocol.head(path.as_str()).await {
        Ok(facts) if facts.size == expected_size && same_etag(&facts.etag, quoted_md5) => {
            Ok(S3WriteFacts::new(facts.etag, facts.version_id))
        }
        _ => Err(changed(role_failure(path, Operation::Publish, put_failure))),
    }
}

/// Reads the published object back, pinned to what the PUT created (E1).
pub(super) async fn verify<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    stage: &PreparedStage,
    single: &SingleStage,
    request: &VerifyRequest,
) -> Result<VerificationEvidence, StorageRoleFailure> {
    let path = stage.final_destination.path();
    adapter.validate(stage)?;
    let published = single.published().ok_or_else(|| {
        entry(
            path,
            Operation::Verify,
            "S3 object is verified only after publication",
        )
    })?;
    verify_written(adapter, path, published, request).await
}

/// Reads an object written at its final key back, pinned to what the write created: by its
/// version when it has one, otherwise with `If-Match` on its `ETag`. A current object that is no
/// longer ours is `Conflict`.
pub(super) async fn verify_written<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    path: &StoragePath,
    written: S3WriteFacts,
    request: &VerifyRequest,
) -> Result<VerificationEvidence, StorageRoleFailure> {
    let version = request
        .published
        .as_ref()
        .and_then(|evidence| evidence.version.clone())
        .or(written.version_id);
    let pinned = S3ObjectFacts {
        size: request.expected_size,
        etag: written.etag,
        version_id: version,
        last_modified: None,
    };
    ensure_current(adapter, path, &pinned).await?;
    let actual = read_pinned(adapter, path, &pinned, &request.cancel).await?;
    if actual != request.expected_blake3 {
        return Err(entry(
            path,
            Operation::Verify,
            "S3 published checksum mismatch",
        ));
    }
    Ok(VerificationEvidence {
        verified_bytes: pinned.size,
        blake3: actual,
    })
}

async fn ensure_current<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    path: &StoragePath,
    pinned: &S3ObjectFacts,
) -> Result<(), StorageRoleFailure> {
    let current = adapter
        .protocol
        .head(path.as_str())
        .await
        .map_err(|failure| role_failure(path, Operation::Verify, failure))?;
    let current_version = current
        .version_id
        .filter(|version| is_real_version_id(version));
    let replaced = !same_etag(&current.etag, &pinned.etag)
        || (pinned.version_id.is_some() && current_version != pinned.version_id);
    if replaced {
        return Err(classified_entry(
            path,
            Operation::Verify,
            FailureClass::Conflict,
            Transience::Permanent,
            "the final S3 object was replaced after publication",
        ));
    }
    if current.size != pinned.size {
        return Err(entry(path, Operation::Verify, "S3 published size mismatch"));
    }
    Ok(())
}

async fn read_pinned<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    path: &StoragePath,
    pinned: &S3ObjectFacts,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<[u8; 32], StorageRoleFailure> {
    let mut hasher = blake3::Hasher::new();
    let mut offset = 0;
    while offset < pinned.size {
        if cancel.is_cancelled() {
            return Err(cancelled(path, Operation::Verify));
        }
        let end = (offset + PART_SIZE as u64).min(pinned.size);
        let bytes = adapter
            .protocol
            .get_range(path.as_str(), offset..end, pinned)
            .await
            .map_err(|failure| role_failure(path, Operation::Verify, failure))?;
        if bytes.len() as u64 != end - offset {
            return Err(entry(path, Operation::Verify, "S3 published size mismatch"));
        }
        hasher.update(&bytes);
        offset = end;
    }
    Ok(*hasher.finalize().as_bytes())
}

/// Whether two `ETag`s are the same, ignoring quotes and the case of hex digits.
pub(super) fn same_etag(left: &str, right: &str) -> bool {
    left.trim_matches('"')
        .eq_ignore_ascii_case(right.trim_matches('"'))
}

fn unchanged(error: StorageRoleFailure) -> PublicationFailure {
    PublicationFailure {
        error,
        final_destination_changed: false,
    }
}

fn changed(error: StorageRoleFailure) -> PublicationFailure {
    PublicationFailure {
        error,
        final_destination_changed: true,
    }
}
