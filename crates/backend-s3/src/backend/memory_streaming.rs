//! Streamed `PutObject`s of [`MemoryS3`] (P7 §14.2 N3): the body is read from the channel as a
//! server reads it — nothing is stored until all declared bytes arrived, and a body that ends
//! short (the sender gone) stores nothing — then stored as one write, the reply carrying the
//! body's MD5 as its `ETag`, as a single `PutObject`'s does (an ONTAP NAS bucket included).
//!
//! Knobs ([`StreamingKnobs`]): a server that stops reading (`stall`), a reply lost after the
//! commit (`commits_then_fails`) or without one (`fails_after_body`), or never sent (`reply_hangs`), another `ETag` in the reply
//! (`etag`), a refusal before any byte is read (`refuse`); `received` counts the bytes the last
//! request took, `puts` the requests (each also counts in [`MemoryS3::puts`]).

use std::future::pending;
use std::mem;
use std::sync::{MutexGuard, PoisonError};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use md5::{Digest as _, Md5};
use tokio::sync::mpsc;

use super::{MemoryS3, etag_of};
use crate::model::{FailureClass, Transience};
use crate::storage::backends::s3::{S3ProtocolFailure, S3Result, S3StreamedPut, S3WriteFacts};

/// How the fake answers streamed `PutObject`s.
#[allow(clippy::struct_excessive_bools)]
#[derive(Default)]
pub struct StreamingKnobs {
    /// The server stops reading bodies: every chunk after the channel's one slot waits.
    pub stall: bool,
    /// The next request stores its object, then its reply never comes.
    pub reply_hangs: bool,
    /// The next request stores its object, then loses its reply.
    pub commits_then_fails: bool,
    /// The next request reads its whole body, stores nothing, and loses its reply.
    pub fails_after_body: bool,
    /// The next request drops its connection once all but the last byte arrived (a store whose
    /// idle-body timeout is shorter than the hold): nothing is stored.
    pub drops_while_held: bool,
    /// The next request's reply carries this `ETag` instead of the body's MD5.
    pub etag: Option<String>,
    /// The next request is refused with this failure before any body byte is read.
    pub refuse: Option<S3ProtocolFailure>,
    /// Replies say the object is encrypted with a KMS key: their `ETag` is no MD5.
    pub encrypted: bool,
    /// The stall timeout the fake's connection reports (default 5 s).
    pub stall_timeout: Option<Duration>,
    /// Bytes the last request took from its body, and requests received.
    pub received: u64,
    pub puts: u32,
}

fn lost_reply() -> S3ProtocolFailure {
    S3ProtocolFailure::session(
        FailureClass::Connectivity,
        Transience::Transient,
        "streamed put response lost",
    )
}

impl MemoryS3 {
    pub fn streaming(&self) -> MutexGuard<'_, StreamingKnobs> {
        self.streaming
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn memory_stall_timeout(&self) -> Duration {
        self.streaming()
            .stall_timeout
            .unwrap_or(Duration::from_secs(5))
    }

    pub(super) async fn memory_put_streaming(
        &self,
        key: &str,
        len: u64,
        mut chunks: mpsc::Receiver<Bytes>,
    ) -> S3Result<S3StreamedPut> {
        let (refused, stall) = {
            let mut knobs = self.streaming();
            knobs.puts += 1;
            knobs.received = 0;
            (knobs.refuse.take(), knobs.stall)
        };
        *self.puts.lock().await += 1;
        if let Some(failure) = refused {
            return Err(failure);
        }
        if stall {
            // Holds the channel open and never reads it.
            let _open = chunks;
            return pending().await;
        }
        let mut body = BytesMut::new();
        while (body.len() as u64) < len {
            let Some(chunk) = chunks.recv().await else {
                // The body ended short: the request fails, nothing is stored.
                return Err(lost_reply());
            };
            body.extend_from_slice(&chunk);
            let mut knobs = self.streaming();
            knobs.received = body.len() as u64;
            if body.len() as u64 + 1 == len && mem::take(&mut knobs.drops_while_held) {
                return Err(lost_reply());
            }
        }
        if body.len() as u64 != len {
            return Err(S3ProtocolFailure::entry(
                FailureClass::InvalidInput,
                Transience::Permanent,
                "IncompleteBody: more bytes than Content-Length",
            ));
        }
        let body = body.freeze();
        if mem::take(&mut self.streaming().fails_after_body) {
            return Err(lost_reply());
        }
        self.multipart_etags
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&<[u8; 16]>::from(Md5::digest(&body)));
        let version = self.store_written(key, body.clone()).await;
        let (hangs, lost, etag, encrypted) = {
            let mut knobs = self.streaming();
            (
                mem::take(&mut knobs.reply_hangs),
                mem::take(&mut knobs.commits_then_fails),
                knobs.etag.take(),
                knobs.encrypted,
            )
        };
        if hangs {
            return pending().await;
        }
        if lost {
            return Err(lost_reply());
        }
        Ok(S3StreamedPut {
            facts: S3WriteFacts::new(etag.unwrap_or_else(|| etag_of(&body)), version),
            etag_is_md5: !encrypted,
        })
    }
}
