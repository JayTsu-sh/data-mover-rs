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
use bytes::Bytes;
use futures::StreamExt;

use super::super::source::{cancelled, classified_entry, entry, role_failure};
use super::super::{
    S3NativeCopySource, S3ObjectFacts, S3Protocol, S3ProtocolFailure, S3WriteFacts,
    is_real_version_id,
};
use super::settle::{Previous, Settling, settle_unknown_write};
use super::{PART_SIZE, S3StagedDestination, directory_object, native_final};
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
const SINGLE_MARKER: &str = "\u{1}single-put";

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
    /// What prepare found at the final key, on a bucket whose `ETag`s cannot prove an object
    /// ours ([`settle`](super::settle)).
    previous: Option<Previous>,
    state: Mutex<SingleState>,
}

#[derive(Default)]
struct SingleState {
    written: Option<super::super::S3Body>,
    tags: Option<Vec<ObjectTag>>,
    published: Option<S3WriteFacts>,
    /// The source a native copy publishes with one `CopyObject` instead of a `PutObject`
    /// (ADR-0006 C18).
    native_source: Option<S3NativeCopySource>,
}

impl SingleStage {
    fn lock(&self) -> MutexGuard<'_, SingleState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The buffered content, which must be `expected_size` bytes, and the tags waiting for it.
    fn content(
        &self,
        path: &StoragePath,
        expected_size: u64,
    ) -> Result<(super::super::S3Body, Option<Vec<ObjectTag>>), PublicationFailure> {
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

    /// Publishes this stage with one `CopyObject` of `source` (ADR-0006 C18): nothing is sent
    /// before publication.
    pub(super) fn set_native_source(&self, source: S3NativeCopySource) {
        self.lock().native_source = Some(source);
    }

    /// What prepare found at the final key, where the bucket's `ETag`s cannot settle a lost reply.
    pub(super) fn previous(&self) -> Option<&Previous> {
        self.previous.as_ref()
    }

    /// Whether this stage publishes a server-side copy rather than bytes it holds.
    pub(super) fn is_native(&self) -> bool {
        self.lock().native_source.is_some()
    }

    /// The tags waiting for the object.
    pub(super) fn pending(&self) -> Option<Vec<ObjectTag>> {
        self.lock().tags.clone()
    }

    /// Records what the write that published the object reported.
    pub(super) fn record_published(&self, facts: S3WriteFacts) {
        let mut guard = self.lock();
        guard.published = Some(facts);
        // The content is on the server now; verification reads it back from there.
        guard.written = None;
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
        let guard = self.lock();
        match (&guard.native_source, &guard.written) {
            (Some(source), _) => source.size,
            (None, written) => written.as_ref().map_or(0, |bytes| bytes.len() as u64),
        }
    }
}

/// The single stage `stage` is, if it is one.
pub(super) fn of(stage: &PreparedStage) -> Option<&SingleStage> {
    stage
        .backend_state
        .as_deref()?
        .downcast_ref::<SingleStage>()
}

impl<P: S3Protocol> S3StagedDestination<P> {
    /// The size of a source that goes as one buffered `PutObject` (`None`: streamed or multipart):
    /// a directory object up to the buffering limit (P7 D13 — its `Unsupported` when one request
    /// cannot carry it — and F5h), any other key up to the single-PUT threshold. No I/O.
    pub(super) fn single_put_size(
        &self,
        path: &StoragePath,
        size: Option<u64>,
    ) -> Result<Option<u64>, StorageRoleFailure> {
        if let Some(size) = directory_object::single_request_size(path, size)? {
            // One request either way (P7 D13); above the threshold it is streamed (F5h).
            return Ok((size <= self.buffered_directory_object_limit()).then_some(size));
        }
        Ok(match (size, self.single_put_threshold) {
            (Some(size), Some(limit)) if size <= limit => Some(size),
            _ => None,
        })
    }

    /// The most bytes a directory object is buffered for: the single-PUT threshold, or its default
    /// where every other object goes through a multipart upload. Above it the one request streams.
    fn buffered_directory_object_limit(&self) -> u64 {
        self.single_put_threshold
            .unwrap_or(DEFAULT_SINGLE_PUT_THRESHOLD)
    }

    /// The size of a source written as one streamed `PutObject`: a directory object over the
    /// buffering limit on any bucket (P7 F5h: one request, never buffered whole), and any object
    /// over the single-PUT threshold on a bucket without multipart uploads (P7 §14.2; its size is
    /// known: an unknown one was refused by the bucket's limits). `None` for any other.
    pub(super) fn streamed_size(&self, path: &StoragePath, size: Option<u64>) -> Option<u64> {
        if path.names_directory_object() {
            // Checked by `single_put_size` first: known, and at most 5 GiB.
            debug_assert!(
                size.is_some_and(|size| size <= directory_object::MAX_DIRECTORY_OBJECT_BYTES)
            );
            return size.filter(|size| *size > self.buffered_directory_object_limit());
        }
        if self.protocol.bucket_capabilities().multipart {
            return None;
        }
        match (size, self.single_put_threshold) {
            (Some(size), Some(limit)) if size > limit => Some(size),
            (Some(size), None) => Some(size),
            _ => None,
        }
    }

    /// A fresh single stage: nothing is sent until publication, and nothing can be resumed.
    /// `previous` is what the final key held, where the bucket's `ETag`s cannot settle a lost
    /// reply ([`Self::previous_object`]).
    pub(super) fn prepare_single(
        &self,
        request: PrepareRequest,
        size: u64,
        previous: Option<Previous>,
    ) -> PreparedStage {
        let token = Self::encode_token(request.final_destination.path().as_str(), SINGLE_MARKER);
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
            previous,
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
    let _capacity = usize::try_from(single.expected_size)
        .map_err(|_| entry(path, Operation::Write, "S3 object cannot fit address space"))?;
    let mut fragments = Vec::new();
    let mut total = 0_u64;
    while let Some(chunk) = input.next().await {
        let chunk = chunk?;
        total = total
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| oversized(path))?;
        if total > single.expected_size {
            return Err(oversized(path));
        }
        if !chunk.is_empty() {
            fragments.push(chunk);
        }
    }
    store(stage, single, super::super::S3Body::new(fragments))
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
    store(stage, single, data.into())
}

fn store(
    stage: &PreparedStage,
    single: &SingleStage,
    content: super::super::S3Body,
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
    stage.confirmed_write(0, request.expected_size);
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
    let native = single.lock().native_source.clone();
    if let Some(source) = native {
        return native_final::send_copy(adapter, path, single, &source, expected_size).await;
    }
    let (body, tags) = single.content(path, expected_size)?;
    let facts = put(adapter, path, single, body, expected_size).await?;
    if let Some(tags) = tags {
        adapter
            .protocol
            .put_tags(path.as_str(), &tags)
            .await
            .map_err(|failure| changed(role_failure(path, Operation::Publish, failure)))?;
    }
    single.record_published(facts.clone());
    Ok(facts)
}

/// One `PutObject` of `body` with `Content-MD5`; a failure that may have stored it is settled.
async fn put<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    path: &StoragePath,
    single: &SingleStage,
    body: super::super::S3Body,
    expected_size: u64,
) -> Result<S3WriteFacts, PublicationFailure> {
    let digest = body.md5();
    let quoted_md5 = format!("\"{digest:x}\"");
    let failure = match adapter
        .protocol
        .put_object(path.as_str(), body, &BASE64_STANDARD.encode(digest))
        .await
    {
        Ok(facts) => return Ok(facts),
        Err(failure) if definite_refusal(&failure) => {
            return Err(unchanged(role_failure(path, Operation::Publish, failure)));
        }
        Err(failure) => failure,
    };
    let md5 = adapter
        .protocol
        .bucket_capabilities()
        .md5_etags
        .then_some(quoted_md5.as_str());
    let settling = Settling {
        size: expected_size,
        previous: single.previous.as_ref(),
        grace: adapter.settle_grace,
    };
    settle_unknown_write(&*adapter.protocol, path, &settling, md5, failure).await
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

/// Reads the published object back, pinned to what the write created — the `PutObject`, or for a
/// native stage the `CopyObject` (E1) — or bracketed where reads cannot be pinned.
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
    if single.is_native() && !adapter.protocol.bucket_capabilities().conditional_reads {
        return verify_bracketed(adapter, path, request, Some(&published.etag)).await;
    }
    verify_written(adapter, path, published, request).await
}

/// Reads an object written at its final key back, pinned to what the write created: by its
/// version when it has one, otherwise with `If-Match` on its `ETag`. A current object that is no
/// longer ours is `Conflict`. On a bucket without conditional reads the read is bracketed
/// instead ([`verify_bracketed`]).
pub(super) async fn verify_written<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    path: &StoragePath,
    written: S3WriteFacts,
    request: &VerifyRequest,
) -> Result<VerificationEvidence, StorageRoleFailure> {
    if !adapter.protocol.bucket_capabilities().conditional_reads {
        return verify_bracketed(adapter, path, request, None).await;
    }
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

/// Read-back where no read can be pinned (P7 §14.2 N4, decision 6; an ONTAP NAS bucket): a `HEAD`
/// anchors the object's `ETag`, every range response must carry it (a replacement during the
/// read is a permanent `Conflict`, from [`S3Protocol::get_range`]), and the content is compared
/// with the source's. Nothing links the anchor to the write — the bucket's `ETag` is not the MD5
/// the `PutObject` reply carried — so content that differs (or a size that does) is a transient
/// `Corruption` that says a replacement between the write and the read cannot be ruled out.
///
/// A server-side copy's reply names the `ETag` the copy has (N0: the same opaque value a `HEAD`
/// gives), so for one (`copied`) the anchor must still be it, else the object was replaced after
/// publication (`Conflict`, permanent) — a link the streamed writes cannot have, their reply
/// carrying the MD5.
async fn verify_bracketed<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    path: &StoragePath,
    request: &VerifyRequest,
    copied: Option<&str>,
) -> Result<VerificationEvidence, StorageRoleFailure> {
    let anchor = anchor(adapter, path).await?;
    let differs = || {
        classified_entry(
            path,
            Operation::Verify,
            FailureClass::Corruption,
            Transience::Transient,
            "the S3 object read back differs from the source; without conditional reads a \
             replacement between the write and this read cannot be ruled out",
        )
    };
    if copied.is_some_and(|copied| !same_etag(&anchor.etag, copied)) {
        return Err(classified_entry(
            path,
            Operation::Verify,
            FailureClass::Conflict,
            Transience::Permanent,
            "the final S3 object was replaced after the copy",
        ));
    }
    if anchor.size != request.expected_size {
        return Err(differs());
    }
    let anchor = S3ObjectFacts {
        version_id: None,
        ..anchor
    };
    let actual = read_pinned(adapter, path, &anchor, &request.cancel).await?;
    if actual != request.expected_blake3 {
        return Err(differs());
    }
    Ok(VerificationEvidence {
        verified_bytes: anchor.size,
        blake3: actual,
    })
}

/// The `HEAD` a bracketed read anchors on. Gone since publication is a replacement, as a pinned
/// read would report it (`Conflict`, permanent).
async fn anchor<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    path: &StoragePath,
) -> Result<S3ObjectFacts, StorageRoleFailure> {
    match adapter.protocol.head(path.as_str()).await {
        Ok(anchor) => Ok(anchor),
        Err(S3ProtocolFailure::Entry {
            class: FailureClass::NotFound,
            ..
        }) => Err(classified_entry(
            path,
            Operation::Verify,
            FailureClass::Conflict,
            Transience::Permanent,
            "the final S3 object is gone since publication",
        )),
        Err(failure) => Err(role_failure(path, Operation::Verify, failure)),
    }
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

pub(super) fn unchanged(error: StorageRoleFailure) -> PublicationFailure {
    PublicationFailure {
        error,
        final_destination_changed: false,
    }
}

pub(super) fn changed(error: StorageRoleFailure) -> PublicationFailure {
    PublicationFailure {
        error,
        final_destination_changed: true,
    }
}
