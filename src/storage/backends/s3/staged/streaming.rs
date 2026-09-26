//! One streamed `PutObject` of an object over the single-PUT threshold that is not written as a
//! multipart upload (P7 §14.2 N3: an S3 bucket without multipart uploads — an ONTAP NAS bucket).
//!
//! - The request starts with the first `write`, in a task of its own, with `Content-Length` = the
//!   source's size ([`S3Protocol::put_object_streaming`]): no more than one chunk waits in its
//!   channel, so nothing near the object's size is buffered.
//! - **The last byte is held until publication.** `write` forwards every byte but the last and
//!   leaves that one with a holder task; `publish` releases it and awaits the reply. No store
//!   commits a `PutObject` whose body is short (N0: an ONTAP NAS bucket keeps the old object
//!   whatever the cut), so any failure or cancellation before publication — the source failing, a
//!   stall, a dropped stage — leaves the final key as it was: the request is aborted, which closes
//!   the connection with the body incomplete. A byte held longer than [`HOLD_LIMIT`] is given up
//!   the same way; publication then fails with the final key unchanged.
//! - `Direct` commits inside `write`: the last byte goes once the input has ended cleanly.
//! - A store that drops the connection while the byte is held (an idle-body timeout shorter than
//!   the hold) ends the request with the body incomplete: publication reports the final key
//!   unchanged.
//! - A watchdog replaces the SDK's timeouts, which are off for this request: a chunk the request
//!   does not take, or a reply that does not come, within the stall timeout (the connection's read
//!   timeout) fails the transfer as a transient `Connectivity` — unless the request already ended
//!   with a failure of its own, which is reported instead.
//! - The MD5 of the bytes is computed as they pass (off the async threads) and compared with the
//!   reply's `ETag` where that can be an MD5 (a quoted 32-hex value, no KMS / customer-key
//!   encryption): a mismatch is a transient `Corruption` with the final key changed.
//! - A publication whose outcome is unknown (the reply lost or late) is settled as
//!   [`settle`](super::settle) says.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use md5::{Digest as _, Md5};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, spawn_blocking};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::super::source::{cancelled, classified_entry, entry, role_failure};
use super::super::{S3Protocol, S3ProtocolFailure, S3StreamedPut, S3WriteFacts};
use super::settle::{Previous, Settling, settle_unknown_write};
use super::single::{changed, definite_refusal, same_etag, unchanged};
use super::{S3StagedDestination, single};
use crate::model::{FailureClass, ObjectTag, Operation, StoragePath, Transience};
use crate::storage::{
    ByteStream, MetadataMutation, PrepareRequest, PreparedStage, PublicationDisposition,
    PublicationEvidence, PublicationFailure, PublishRequest, StorageRoleFailure, UploadDigest,
    UploadDigestGap, VerificationEvidence, VerifyRequest, WriteEvidence,
};

/// How long the last byte may wait for publication before the upload is given up: well within
/// the 900 s an ONTAP NAS bucket was seen to wait (N0), and long enough for metadata and the
/// expert source's evidence between `write` and `publish`.
pub(super) const HOLD_LIMIT: Duration = Duration::from_secs(300);

/// The upload-id half of a streaming stage's token; a real upload id is printable.
const STREAMING_MARKER: &str = "\u{1}streamed-put";

/// One object on its way to a single streamed `PutObject`.
pub(super) struct StreamingStage {
    expected_size: u64,
    /// `Direct`: the request commits inside `write`; otherwise the last byte waits for `publish`.
    commit_in_write: bool,
    /// What prepare found at the final key, on a bucket whose `ETag`s cannot prove an object ours.
    previous: Option<Previous>,
    stall: Duration,
    hold_limit: Duration,
    settle_grace: Duration,
    state: Mutex<StreamingState>,
}

#[derive(Default)]
struct StreamingState {
    /// The request of a written stage whose last byte is held.
    inflight: Option<InFlight>,
    /// Bytes `write` accepted: the source's size once written.
    written: u64,
    /// The quoted hex MD5 of every byte written.
    md5: Option<String>,
    tags: Option<Vec<ObjectTag>>,
    published: Option<S3WriteFacts>,
    /// Whether the reply's `ETag` (or, settling a lost reply, the object's) matched our MD5.
    digest_matched: bool,
}

/// A request in flight. Dropping it aborts the request and the holder of the last byte: the body
/// ends before its declared length, the connection closes, and nothing commits.
struct InFlight {
    request: JoinHandle<Result<S3StreamedPut, S3ProtocolFailure>>,
    /// Releases the held last byte.
    release: Option<oneshot::Sender<()>>,
    holder: Option<JoinHandle<()>>,
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.request.abort();
        if let Some(holder) = &self.holder {
            holder.abort();
        }
    }
}

impl InFlight {
    /// Starts the request: `size` bytes to `path`, read from `chunks`.
    fn start<P: S3Protocol + 'static>(
        adapter: &S3StagedDestination<P>,
        path: &StoragePath,
        size: u64,
        chunks: mpsc::Receiver<Bytes>,
    ) -> Self {
        let protocol = Arc::clone(&adapter.protocol);
        let key = path.as_str().to_string();
        Self {
            request: tokio::spawn(async move {
                protocol.put_object_streaming(&key, size, chunks).await
            }),
            release: None,
            holder: None,
        }
    }

    /// Leaves `byte` with a holder task until `publish` releases it — or `limit` passes, or the
    /// stage is dropped: then `sender` goes, the body ends short, and nothing commits.
    fn hold(&mut self, sender: mpsc::Sender<Bytes>, byte: Bytes, limit: Duration) {
        let (release, released) = oneshot::channel::<()>();
        self.holder = Some(tokio::spawn(async move {
            if let Ok(Ok(())) = timeout(limit, released).await {
                let _ = sender.send(byte).await;
            }
        }));
        self.release = Some(release);
    }

    /// The request's own failure, when it has already ended (the server refused the upload).
    async fn ended_failure(&mut self) -> Option<S3ProtocolFailure> {
        if !self.request.is_finished() {
            return None;
        }
        match (&mut self.request).await {
            Ok(Err(failure)) => Some(failure),
            Ok(Ok(_)) => Some(S3ProtocolFailure::protocol(
                "the S3 upload was answered before its body was complete",
            )),
            Err(_) => Some(S3ProtocolFailure::protocol("the S3 upload task stopped")),
        }
    }
}

impl StreamingStage {
    fn lock(&self) -> MutexGuard<'_, StreamingState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Bytes accepted so far: the durable prefix a checkpoint observes (the object itself commits
    /// only in publication).
    pub(super) fn written_len(&self) -> u64 {
        self.lock().written
    }

    /// What the request that made the object reported, once it is written.
    pub(super) fn published(&self) -> Option<S3WriteFacts> {
        self.lock().published.clone()
    }

    /// What the store confirmed of the bytes it received (P7 §14.2 N4).
    pub(super) fn upload_digest(&self) -> UploadDigest {
        if self.lock().digest_matched {
            UploadDigest::Matched
        } else {
            UploadDigest::Unchecked(UploadDigestGap::EtagNotMd5)
        }
    }

    /// Aborts a request still in flight: the final key keeps what it had.
    pub(super) fn abandon(&self) {
        drop(self.lock().inflight.take());
    }

    fn settling(&self) -> Settling<'_> {
        Settling {
            size: self.expected_size,
            previous: self.previous.as_ref(),
            grace: self.settle_grace,
        }
    }
}

/// The streaming stage `stage` is, if it is one.
pub(super) fn of(stage: &PreparedStage) -> Option<&StreamingStage> {
    stage
        .backend_state
        .as_deref()?
        .downcast_ref::<StreamingStage>()
}

fn connectivity(
    path: &StoragePath,
    operation: Operation,
    diagnostic: String,
) -> StorageRoleFailure {
    role_failure(
        path,
        operation,
        S3ProtocolFailure::session(
            FailureClass::Connectivity,
            Transience::Transient,
            diagnostic,
        ),
    )
}

fn invalid(path: &StoragePath, diagnostic: &str) -> StorageRoleFailure {
    classified_entry(
        path,
        Operation::Write,
        FailureClass::InvalidInput,
        Transience::Permanent,
        diagnostic,
    )
}

/// Adds `chunk` to `hasher` off the async threads.
async fn md5_update(mut hasher: Md5, chunk: Bytes) -> Result<Md5, ()> {
    spawn_blocking(move || {
        hasher.update(&chunk);
        hasher
    })
    .await
    .map_err(drop)
}

impl<P: S3Protocol + 'static> S3StagedDestination<P> {
    /// A streaming stage for `size` bytes: `commit_in_write` for `Direct`. On a bucket whose
    /// `ETag`s are not MD5s the final key is looked at once (`HEAD`), to settle a publication whose
    /// reply is lost (`Direct` has nothing to settle: its failures report the key changed anyway).
    /// Never resumable.
    pub(super) async fn prepare_streaming(
        &self,
        request: PrepareRequest,
        size: u64,
        commit_in_write: bool,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        let path = request.final_destination.path().clone();
        let previous = if commit_in_write {
            None
        } else {
            self.previous_object(&path).await?
        };
        let token = Self::encode_token(path.as_str(), STREAMING_MARKER);
        let mut stage = PreparedStage::new(
            self.identity.clone(),
            request.final_destination,
            token,
            request.recovery_binding,
            0,
            None,
        );
        stage.backend_state = Some(Arc::new(StreamingStage {
            expected_size: size,
            commit_in_write,
            previous,
            stall: self.protocol.stall_timeout(),
            hold_limit: self.hold_limit,
            settle_grace: self.settle_grace,
            state: Mutex::new(StreamingState::default()),
        }));
        Ok(stage.disable_recovery())
    }
}

/// Streams the input to the final key: all but the last byte, which `publish` releases — or,
/// for `Direct`, every byte once the input ended, committing now.
pub(super) async fn write<P: S3Protocol + 'static>(
    adapter: &S3StagedDestination<P>,
    stage: &PreparedStage,
    streaming: &StreamingStage,
    input: ByteStream,
) -> Result<WriteEvidence, StorageRoleFailure> {
    let path = stage.final_destination.path();
    {
        let current = streaming.lock();
        if current.written > 0 || current.inflight.is_some() || current.published.is_some() {
            return Err(entry(
                path,
                Operation::Write,
                "S3 object is already written",
            ));
        }
    }
    let size = streaming.expected_size;
    let (sender, chunks) = mpsc::channel::<Bytes>(1);
    let mut inflight = InFlight::start(adapter, path, size, chunks);
    let (last, md5) = send_all_but_last(input, &sender, streaming, &mut inflight, path).await?;
    if streaming.commit_in_write {
        forward(&sender, last, streaming.stall, &mut inflight, path).await?;
        drop(sender);
        let facts = finish(adapter, path, streaming, inflight, &md5)
            .await
            .map_err(|failure| failure.error)?;
        record(streaming, None, md5, Some(facts));
    } else {
        inflight.hold(sender, last, streaming.hold_limit);
        record(streaming, Some(inflight), md5, None);
    }
    Ok(WriteEvidence {
        persisted_bytes: size,
    })
}

/// A written stage's state: the request holding its last byte, or what the committed request
/// reported.
fn record(
    streaming: &StreamingStage,
    inflight: Option<InFlight>,
    md5: String,
    published: Option<S3WriteFacts>,
) {
    let mut current = streaming.lock();
    current.inflight = inflight;
    current.written = streaming.expected_size;
    current.md5 = Some(md5);
    current.published = published;
}

/// Sends every input byte but the last, hashing all of them. Returns the last byte and the quoted
/// hex MD5. More or fewer bytes than the source's size is `InvalidInput`; a failing input fails.
async fn send_all_but_last(
    mut input: ByteStream,
    sender: &mpsc::Sender<Bytes>,
    streaming: &StreamingStage,
    inflight: &mut InFlight,
    path: &StoragePath,
) -> Result<(Bytes, String), StorageRoleFailure> {
    let size = streaming.expected_size;
    let mut hasher = Md5::new();
    let mut sent = 0_u64;
    let mut last = None;
    while let Some(chunk) = input.next().await {
        // A failing source drops the request with the stage: nothing commits.
        let mut chunk = chunk?;
        sent = sent
            .checked_add(chunk.len() as u64)
            .filter(|sent| *sent <= size)
            .ok_or_else(|| invalid(path, "more bytes than the source size were written"))?;
        hasher = md5_update(hasher, chunk.clone())
            .await
            .map_err(|()| entry(path, Operation::Write, "S3 upload digest failed"))?;
        if sent == size && !chunk.is_empty() {
            last = Some(chunk.split_off(chunk.len() - 1));
        }
        if !chunk.is_empty() {
            forward(sender, chunk, streaming.stall, inflight, path).await?;
        }
    }
    match last {
        Some(last) if sent == size => Ok((last, format!("\"{:x}\"", hasher.finalize()))),
        _ => Err(invalid(
            path,
            "fewer bytes than the source size were written",
        )),
    }
}

/// Hands one chunk to the request within the stall timeout. A request that ended before taking
/// it — the server refused the upload early — reports its own failure.
async fn forward(
    sender: &mpsc::Sender<Bytes>,
    chunk: Bytes,
    stall: Duration,
    inflight: &mut InFlight,
    path: &StoragePath,
) -> Result<(), StorageRoleFailure> {
    let sent = timeout(stall, sender.send(chunk)).await;
    if matches!(sent, Ok(Ok(()))) {
        return Ok(());
    }
    if let Some(failure) = inflight.ended_failure().await {
        return Err(role_failure(path, Operation::Write, failure));
    }
    match sent {
        Err(_) => Err(connectivity(
            path,
            Operation::Write,
            format!(
                "the S3 upload took no bytes for {}s: abandoned",
                stall.as_secs()
            ),
        )),
        _ => Err(entry(
            path,
            Operation::Write,
            "the S3 upload ended before taking its body",
        )),
    }
}

/// The reply of a request whose body is complete, within the stall timeout.
async fn reply(
    streaming: &StreamingStage,
    inflight: &mut InFlight,
) -> Result<S3StreamedPut, S3ProtocolFailure> {
    match timeout(streaming.stall, &mut inflight.request).await {
        Ok(Ok(reply)) => reply,
        Ok(Err(_)) => Err(S3ProtocolFailure::protocol("the S3 upload task stopped")),
        Err(_) => Err(S3ProtocolFailure::session(
            FailureClass::Connectivity,
            Transience::Transient,
            format!(
                "the S3 upload's reply did not come within {}s",
                streaming.stall.as_secs()
            ),
        )),
    }
}

/// Awaits the reply of a request whose body is complete and checks its `ETag` against our MD5. A
/// failure is settled ([`settle_unknown_write`]) unless the store refused before committing.
async fn finish<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    path: &StoragePath,
    streaming: &StreamingStage,
    mut inflight: InFlight,
    md5: &str,
) -> Result<S3WriteFacts, PublicationFailure> {
    let reply = reply(streaming, &mut inflight).await;
    drop(inflight);
    let put = match reply {
        Ok(put) => put,
        Err(failure) if definite_refusal(&failure) => {
            return Err(unchanged(role_failure(path, Operation::Publish, failure)));
        }
        Err(failure) => {
            let md5_etags = adapter.protocol.bucket_capabilities().md5_etags;
            let settled = settle_unknown_write(
                &*adapter.protocol,
                path,
                &streaming.settling(),
                md5_etags.then_some(md5),
                failure,
            )
            .await?;
            // Settled as ours only by our MD5 on the object.
            streaming.lock().digest_matched = true;
            return Ok(settled);
        }
    };
    if put.etag_is_md5 && is_md5_etag(&put.facts.etag) && !same_etag(&put.facts.etag, md5) {
        return Err(changed(classified_entry(
            path,
            Operation::Publish,
            FailureClass::Corruption,
            Transience::Transient,
            "the S3 object's ETag is not the MD5 of the bytes sent: corrupted in flight",
        )));
    }
    streaming.lock().digest_matched = put.etag_is_md5 && is_md5_etag(&put.facts.etag);
    Ok(put.facts)
}

/// Whether `etag` has the shape of an MD5 (`"<32 hex>"`).
fn is_md5_etag(etag: &str) -> bool {
    let hex = etag.trim_matches('"');
    hex.len() == 32 && hex.bytes().all(|digit| digit.is_ascii_hexdigit())
}

/// Keeps object tags for the object, which exists only once published.
pub(super) fn apply_metadata(
    stage: &PreparedStage,
    streaming: &StreamingStage,
    tags_supported: bool,
    mutation: MetadataMutation,
    cancel: &CancellationToken,
) -> Result<(), StorageRoleFailure> {
    let tags = single::pending_tags(
        stage.final_destination.path(),
        tags_supported,
        mutation,
        cancel,
    )?;
    streaming.lock().tags = Some(tags);
    Ok(())
}

/// Takes the written stage's request, MD5 and pending tags for publication.
fn take_written(
    path: &StoragePath,
    streaming: &StreamingStage,
    expected_size: u64,
) -> Result<(InFlight, String, Option<Vec<ObjectTag>>), PublicationFailure> {
    let mut current = streaming.lock();
    if current.written != expected_size || current.published.is_some() {
        return Err(unchanged(entry(
            path,
            Operation::Publish,
            "S3 object content differs from the published size",
        )));
    }
    match (current.inflight.take(), current.md5.clone()) {
        (Some(inflight), Some(md5)) => Ok((inflight, md5, current.tags.clone())),
        _ => Err(unchanged(entry(
            path,
            Operation::Publish,
            "the S3 upload is not in flight",
        ))),
    }
}

/// Releases the last byte and awaits the reply: the object commits now. Then the pending tags.
pub(super) async fn publish<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    stage: &PreparedStage,
    streaming: &StreamingStage,
    request: &PublishRequest,
) -> Result<PublicationEvidence, PublicationFailure> {
    let path = stage.final_destination.path();
    adapter.validate(stage).map_err(unchanged)?;
    if request.cancel.is_cancelled() {
        return Err(unchanged(cancelled(path, Operation::Publish)));
    }
    let (mut inflight, md5, tags) = take_written(path, streaming, request.expected_size)?;
    // A request that already ended never had its whole body (a store that dropped the idle
    // connection, a refusal): nothing committed.
    if let Some(failure) = inflight.ended_failure().await {
        return Err(unchanged(role_failure(path, Operation::Publish, failure)));
    }
    let released = inflight
        .release
        .take()
        .is_some_and(|release| release.send(()).is_ok());
    if !released {
        return Err(unchanged(connectivity(
            path,
            Operation::Publish,
            format!(
                "the S3 upload held its last byte longer than {}s and was abandoned",
                streaming.hold_limit.as_secs()
            ),
        )));
    }
    let facts = finish(adapter, path, streaming, inflight, &md5).await?;
    if let Some(tags) = tags {
        adapter
            .protocol
            .put_tags(path.as_str(), &tags)
            .await
            .map_err(|failure| changed(role_failure(path, Operation::Publish, failure)))?;
    }
    streaming.lock().published = Some(facts.clone());
    Ok(PublicationEvidence {
        final_destination: path.clone(),
        disposition: PublicationDisposition::Published,
        version: facts.version_id,
    })
}

/// Reads the published object back (see [`single::verify_written`]).
pub(super) async fn verify<P: S3Protocol>(
    adapter: &S3StagedDestination<P>,
    stage: &PreparedStage,
    streaming: &StreamingStage,
    request: &VerifyRequest,
) -> Result<VerificationEvidence, StorageRoleFailure> {
    let path = stage.final_destination.path();
    adapter.validate(stage)?;
    let published = streaming.published().ok_or_else(|| {
        entry(
            path,
            Operation::Verify,
            "S3 object is verified only after publication",
        )
    })?;
    single::verify_written(adapter, path, published, request).await
}

#[cfg(test)]
#[path = "streaming_tests.rs"]
mod tests;
