//! Replayable shared payload fragments. Aggregating a write never merges its allocations.
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use aws_smithy_types::{body::SdkBody, byte_stream::ByteStream};
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use md5::{Digest as _, Md5};

#[derive(Clone, Debug, Default)]
pub(crate) struct S3Body {
    fragments: Arc<[Bytes]>,
    len: usize,
}

impl S3Body {
    pub(crate) fn new(fragments: Vec<Bytes>) -> Self {
        let len = fragments.iter().map(Bytes::len).sum();
        Self {
            fragments: fragments.into(),
            len,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub(crate) fn fragments(&self) -> &[Bytes] {
        &self.fragments
    }

    pub(crate) fn md5(&self) -> md5::digest::Output<Md5> {
        let mut hash = Md5::new();
        for fragment in self.fragments.iter() {
            hash.update(fragment);
        }
        hash.finalize()
    }

    pub(crate) fn into_stream(self) -> ByteStream {
        ByteStream::new(SdkBody::retryable(move || {
            SdkBody::from_body_1_x(FragmentBody {
                fragments: self.fragments.iter().cloned().collect(),
                remaining: self.len as u64,
            })
        }))
    }

    /// In-memory protocol fakes store contiguous objects. Production uploads use the
    /// replayable body directly; this collector is intentionally restricted to tests.
    #[cfg(test)]
    pub(crate) fn into_bytes(self) -> Bytes {
        if self.fragments.len() == 1 {
            return self.fragments[0].clone();
        }
        let mut bytes = bytes::BytesMut::with_capacity(self.len);
        for fragment in self.fragments.iter() {
            bytes.extend_from_slice(fragment);
        }
        bytes.freeze()
    }
}

impl From<Bytes> for S3Body {
    fn from(bytes: Bytes) -> Self {
        Self::new(vec![bytes])
    }
}

struct FragmentBody {
    fragments: VecDeque<Bytes>,
    remaining: u64,
}

impl Body for FragmentBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        Poll::Ready(self.fragments.pop_front().map(|fragment| {
            self.remaining -= fragment.len() as u64;
            Ok(Frame::data(fragment))
        }))
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining)
    }
    fn is_end_stream(&self) -> bool {
        self.fragments.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AssertTestValue as _;

    #[tokio::test]
    async fn fragments_and_retries_share_the_original_allocations() {
        let one = Bytes::from(vec![1_u8; 13]);
        let two = Bytes::from(vec![2_u8; 17]);
        let body = S3Body::new(vec![one.clone(), two.clone()]);
        let mut first = body.into_stream().into_inner();
        let mut retry = first.try_clone().assert_value("replayable fragments");
        for stream in [&mut first, &mut retry] {
            for expected in [&one, &two] {
                let frame = futures::future::poll_fn(|cx| Pin::new(&mut *stream).poll_frame(cx))
                    .await
                    .assert_value("frame")
                    .assert_value("payload")
                    .into_data()
                    .assert_value("data frame");
                assert_eq!(frame.as_ptr(), expected.as_ptr());
                assert_eq!(frame, *expected);
            }
            assert_eq!(stream.size_hint().exact(), Some(0));
        }
    }
}
