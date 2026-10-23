use bytes::Bytes;
use futures::{StreamExt as _, stream};

use super::checkpoint_stream::ordered;
use super::{FinalDestination, PositionedChunk, PreparedStage};
use crate::model::{BackendIdentity, BackendKind, StoragePath};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn stage(
    start: u64,
    size: u64,
    window: usize,
) -> Result<PreparedStage, Box<dyn std::error::Error>> {
    let stage = PreparedStage::new(
        BackendIdentity::new(BackendKind::Local, "checkpoint-order-test")?,
        FinalDestination::new(StoragePath::new("file")?),
        Bytes::new(),
        [0; 32],
        start,
        None,
    );
    stage.enable_checkpoint(size, Bytes::from_static(b"hash"));
    stage.set_checkpoint_window(window);
    Ok(stage)
}

fn chunk(offset: u64, data: &'static [u8]) -> PositionedChunk {
    PositionedChunk {
        offset,
        data: Bytes::from_static(data),
    }
}

#[tokio::test]
async fn ordered_append_reassembles_unaligned_source_completions() -> TestResult {
    let stage = stage(5, 16, 11)?;
    let input =
        Box::pin(stream::iter([chunk(12, b"mnop"), chunk(5, b"fgh"), chunk(8, b"ijkl")]).map(Ok));
    let chunks = ordered(&stage, input).collect::<Vec<_>>().await;
    let mut bytes = Vec::new();
    for chunk in chunks {
        bytes.extend_from_slice(&chunk?);
    }
    assert_eq!(bytes, b"fghijklmnop");
    Ok(())
}

#[tokio::test]
async fn invalid_ordered_streams_fail_instead_of_dropping_bytes() -> TestResult {
    for input in [
        vec![chunk(0, b"ab"), chunk(1, b"bc"), chunk(2, b"cd")],
        vec![chunk(2, b"cd")],
        vec![chunk(0, b"abcde")],
        vec![chunk(0, b"")],
        vec![chunk(2, b"cd"), chunk(2, b"cd")],
    ] {
        let stage = stage(0, 4, 4)?;
        let result = ordered(&stage, Box::pin(stream::iter(input).map(Ok)))
            .collect::<Vec<_>>()
            .await;
        assert!(result.iter().any(Result::is_err));
    }
    Ok(())
}

#[tokio::test]
async fn completed_empty_suffix_and_reorder_limit_are_explicit() -> TestResult {
    let complete = stage(4, 4, 1)?;
    assert!(
        ordered(&complete, Box::pin(stream::empty()))
            .next()
            .await
            .is_none()
    );
    let limited = stage(0, 8, 3)?;
    let input = Box::pin(stream::iter([chunk(4, b"efgh"), chunk(0, b"abcd")]).map(Ok));
    assert!(
        ordered(&limited, input)
            .next()
            .await
            .is_some_and(|value| value.is_err())
    );
    Ok(())
}
