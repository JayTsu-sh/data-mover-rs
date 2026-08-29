use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

const MAX_BUDGET: usize = u32::MAX as usize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InflightConfig {
    chunks: usize,
    bytes: usize,
    operations: usize,
}

impl InflightConfig {
    pub(crate) fn new(
        chunks: usize,
        bytes: usize,
        operations: usize,
    ) -> Result<Self, InflightFailure> {
        if chunks == 0 || bytes == 0 || operations == 0 {
            return Err(InflightFailure::InvalidConfig(
                "inflight limits must be greater than zero",
            ));
        }
        if chunks > MAX_BUDGET || bytes > MAX_BUDGET || operations > MAX_BUDGET {
            return Err(InflightFailure::InvalidConfig(
                "inflight limits exceed semaphore capacity",
            ));
        }
        Ok(Self {
            chunks,
            bytes,
            operations,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReadRange {
    pub offset: u64,
    pub length: usize,
}

pub(crate) struct SequentialRanges {
    next: u64,
    end: u64,
    chunk_bytes: usize,
}

impl SequentialRanges {
    pub(crate) fn new(
        start: u64,
        length: u64,
        chunk_bytes: usize,
    ) -> Result<Self, InflightFailure> {
        if chunk_bytes == 0 {
            return Err(InflightFailure::InvalidConfig(
                "sequential read chunk size must be greater than zero",
            ));
        }
        let end = start
            .checked_add(length)
            .ok_or(InflightFailure::OffsetOverflow)?;
        Ok(Self {
            next: start,
            end,
            chunk_bytes,
        })
    }
}

impl Iterator for SequentialRanges {
    type Item = ReadRange;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.end {
            return None;
        }
        let remaining = self.end - self.next;
        let length =
            usize::try_from(remaining.min(self.chunk_bytes as u64)).unwrap_or(self.chunk_bytes);
        let range = ReadRange {
            offset: self.next,
            length,
        };
        self.next += length as u64;
        Some(range)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum InflightFailure {
    InvalidConfig(&'static str),
    ChunkExceedsBudget { bytes: usize, budget: usize },
    EmptyChunk,
    ProducedLengthMismatch { reserved: usize, produced: usize },
    OffsetOverflow,
    DuplicateOrOverlapping { offset: u64 },
    OutOfOrderAdmission { expected: u64, actual: u64 },
    RangeOutsideExpected { end: u64, expected_end: u64 },
    Gap { expected: u64, next: u64 },
    Cancelled,
    Closed,
    Upstream,
}

impl fmt::Display for InflightFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => formatter.write_str(message),
            Self::ChunkExceedsBudget { bytes, budget } => {
                write!(formatter, "chunk size {bytes} exceeds byte budget {budget}")
            }
            Self::EmptyChunk => formatter.write_str("inflight chunks must not be empty"),
            Self::ProducedLengthMismatch { reserved, produced } => write!(
                formatter,
                "reserved {reserved} payload bytes but produced {produced}"
            ),
            Self::OffsetOverflow => formatter.write_str("chunk end offset overflowed"),
            Self::DuplicateOrOverlapping { offset } => {
                write!(
                    formatter,
                    "chunk at offset {offset} duplicates or overlaps delivered data"
                )
            }
            Self::OutOfOrderAdmission { expected, actual } => write!(
                formatter,
                "source range admission expected offset {expected}, got {actual}"
            ),
            Self::RangeOutsideExpected { end, expected_end } => write!(
                formatter,
                "source range ends at {end}, beyond expected end {expected_end}"
            ),
            Self::Gap { expected, next } => {
                write!(
                    formatter,
                    "inflight stream closed with gap at {expected}; next offset is {next}"
                )
            }
            Self::Cancelled => formatter.write_str("inflight operation cancelled"),
            Self::Closed => formatter.write_str("inflight stream closed"),
            Self::Upstream => formatter.write_str("upstream operation failed"),
        }
    }
}

impl std::error::Error for InflightFailure {}

struct BudgetedChunk {
    offset: u64,
    data: Bytes,
    _chunk_permit: OwnedSemaphorePermit,
    _byte_permit: OwnedSemaphorePermit,
}

enum Message {
    Chunk(BudgetedChunk),
    Failure(InflightFailure),
}

#[derive(Clone)]
pub(crate) struct InflightRuntime {
    sender: mpsc::Sender<Message>,
    chunks: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    operations: Arc<Semaphore>,
    byte_budget: usize,
    expected_end: u64,
    next_admission: Arc<Mutex<u64>>,
    cancel: CancellationToken,
}

pub(crate) struct InflightAdmission {
    offset: u64,
    length: usize,
    sender: mpsc::Sender<Message>,
    cancel: CancellationToken,
    chunk_permit: OwnedSemaphorePermit,
    byte_permit: OwnedSemaphorePermit,
    _operation_permit: OwnedSemaphorePermit,
}

impl InflightAdmission {
    pub(crate) async fn complete(self, data: Bytes) -> Result<(), InflightFailure> {
        if data.len() != self.length {
            return Err(InflightFailure::ProducedLengthMismatch {
                reserved: self.length,
                produced: data.len(),
            });
        }
        let message = Message::Chunk(BudgetedChunk {
            offset: self.offset,
            data,
            _chunk_permit: self.chunk_permit,
            _byte_permit: self.byte_permit,
        });
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => Err(InflightFailure::Cancelled),
            result = self.sender.send(message) => result.map_err(|_| InflightFailure::Closed),
        }
    }
}

impl InflightRuntime {
    pub(crate) fn channel(
        config: InflightConfig,
        start_offset: u64,
        expected_end: u64,
        cancel: CancellationToken,
    ) -> Result<(Self, OrderedChunks), InflightFailure> {
        if expected_end < start_offset {
            return Err(InflightFailure::InvalidConfig(
                "expected end precedes the start offset",
            ));
        }
        let (sender, receiver) = mpsc::channel(config.chunks);
        let runtime = Self {
            sender,
            chunks: Arc::new(Semaphore::new(config.chunks)),
            bytes: Arc::new(Semaphore::new(config.bytes)),
            operations: Arc::new(Semaphore::new(config.operations)),
            byte_budget: config.bytes,
            expected_end,
            next_admission: Arc::new(Mutex::new(start_offset)),
            cancel: cancel.clone(),
        };
        let ordered = OrderedChunks {
            receiver,
            pending: BTreeMap::new(),
            next_offset: start_offset,
            expected_end,
            cancel,
            terminal: false,
        };
        Ok((runtime, ordered))
    }

    /// Reserves chunk, byte, and operation capacity before a caller starts a read.
    /// Callers admit ranges in source order; completions may arrive in any order.
    pub(crate) async fn admit(
        &self,
        offset: u64,
        length: usize,
    ) -> Result<InflightAdmission, InflightFailure> {
        if length == 0 {
            return Err(InflightFailure::EmptyChunk);
        }
        if length > self.byte_budget {
            return Err(InflightFailure::ChunkExceedsBudget {
                bytes: length,
                budget: self.byte_budget,
            });
        }
        let end = offset
            .checked_add(length as u64)
            .ok_or(InflightFailure::OffsetOverflow)?;
        if end > self.expected_end {
            return Err(InflightFailure::RangeOutsideExpected {
                end,
                expected_end: self.expected_end,
            });
        }
        let mut next_admission = tokio::select! {
            biased;
            () = self.cancel.cancelled() => return Err(InflightFailure::Cancelled),
            guard = self.next_admission.lock() => guard,
        };
        if offset != *next_admission {
            return Err(InflightFailure::OutOfOrderAdmission {
                expected: *next_admission,
                actual: offset,
            });
        }
        let chunk_permit = self.acquire(self.chunks.clone(), 1).await?;
        let byte_permit = self.acquire(self.bytes.clone(), length).await?;
        let operation_permit = self.acquire(self.operations.clone(), 1).await?;
        *next_admission = end;
        Ok(InflightAdmission {
            offset,
            length,
            sender: self.sender.clone(),
            cancel: self.cancel.clone(),
            chunk_permit,
            byte_permit,
            _operation_permit: operation_permit,
        })
    }

    pub(crate) async fn fail(&self, failure: InflightFailure) -> Result<(), InflightFailure> {
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => Err(InflightFailure::Cancelled),
            result = self.sender.send(Message::Failure(failure)) => {
                result.map_err(|_| InflightFailure::Closed)
            }
        }
    }

    async fn acquire(
        &self,
        semaphore: Arc<Semaphore>,
        permits: usize,
    ) -> Result<OwnedSemaphorePermit, InflightFailure> {
        let permits = u32::try_from(permits).map_err(|_| {
            InflightFailure::InvalidConfig("requested permit count exceeds semaphore capacity")
        })?;
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => Err(InflightFailure::Cancelled),
            permit = semaphore.acquire_many_owned(permits) => {
                permit.map_err(|_| InflightFailure::Closed)
            }
        }
    }
}

pub(crate) struct OrderedChunks {
    receiver: mpsc::Receiver<Message>,
    pending: BTreeMap<u64, BudgetedChunk>,
    next_offset: u64,
    expected_end: u64,
    cancel: CancellationToken,
    terminal: bool,
}

impl OrderedChunks {
    pub(crate) const fn recovery_offset(&self) -> u64 {
        self.next_offset
    }

    pub(crate) async fn next(&mut self) -> Option<Result<Bytes, InflightFailure>> {
        if self.terminal {
            return None;
        }
        loop {
            if self.cancel.is_cancelled() {
                return Some(self.terminate(InflightFailure::Cancelled));
            }
            if let Some(chunk) = self.pending.remove(&self.next_offset) {
                let Some(next) = self.next_offset.checked_add(chunk.data.len() as u64) else {
                    return Some(self.terminate(InflightFailure::OffsetOverflow));
                };
                self.next_offset = next;
                return Some(Ok(chunk.data));
            }
            let message = tokio::select! {
                biased;
                () = self.cancel.cancelled() => {
                    return Some(self.terminate(InflightFailure::Cancelled));
                }
                message = self.receiver.recv() => message,
            };
            match message {
                Some(Message::Chunk(chunk)) => {
                    let end = chunk.offset + chunk.data.len() as u64;
                    let overlaps_previous =
                        self.pending.range(..=chunk.offset).next_back().is_some_and(
                            |(_, previous)| {
                                previous.offset + previous.data.len() as u64 > chunk.offset
                            },
                        );
                    let overlaps_next = self
                        .pending
                        .range(chunk.offset..)
                        .next()
                        .is_some_and(|(next, _)| *next < end);
                    if chunk.offset < self.next_offset || overlaps_previous || overlaps_next {
                        return Some(self.terminate(InflightFailure::DuplicateOrOverlapping {
                            offset: chunk.offset,
                        }));
                    }
                    self.pending.insert(chunk.offset, chunk);
                }
                Some(Message::Failure(failure)) => {
                    return Some(self.terminate(failure));
                }
                None => {
                    if self.next_offset == self.expected_end {
                        self.terminal = true;
                        return None;
                    }
                    let next = self
                        .pending
                        .first_key_value()
                        .map_or(self.expected_end, |(next, _)| *next);
                    return Some(self.terminate(InflightFailure::Gap {
                        expected: self.next_offset,
                        next,
                    }));
                }
            }
        }
    }

    fn terminate(&mut self, failure: InflightFailure) -> Result<Bytes, InflightFailure> {
        self.terminal = true;
        self.pending.clear();
        self.receiver.close();
        while self.receiver.try_recv().is_ok() {}
        Err(failure)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    fn ok<T, E: fmt::Display>(result: Result<T, E>) -> T {
        result.unwrap_or_else(|error| panic!("unexpected failure: {error}"))
    }

    fn some<T>(value: Option<T>) -> T {
        value.unwrap_or_else(|| panic!("expected an item"))
    }

    fn config(chunks: usize, bytes: usize, operations: usize) -> InflightConfig {
        InflightConfig::new(chunks, bytes, operations).unwrap_or_else(|error| panic!("{error}"))
    }

    fn channel(
        config: InflightConfig,
        start: u64,
        end: u64,
        cancel: CancellationToken,
    ) -> (InflightRuntime, OrderedChunks) {
        ok(InflightRuntime::channel(config, start, end, cancel))
    }

    async fn complete(runtime: &InflightRuntime, offset: u64, data: Bytes) {
        let admission = ok(runtime.admit(offset, data.len()).await);
        ok(admission.complete(data).await);
    }

    #[tokio::test]
    async fn out_of_order_arrival_is_delivered_sequentially_with_exact_recovery_boundary() {
        let cancel = CancellationToken::new();
        let (runtime, mut ordered) = channel(config(3, 9, 3), 0, 9, cancel);
        // Admission is always source ordered, so the missing earliest completion can never
        // be starved by later chunks that fill the budget.
        let first = ok(runtime.admit(0, 3).await);
        let second = ok(runtime.admit(3, 3).await);
        let third = ok(runtime.admit(6, 3).await);
        ok(second.complete(Bytes::from_static(b"def")).await);
        ok(third.complete(Bytes::from_static(b"ghi")).await);
        ok(first.complete(Bytes::from_static(b"abc")).await);
        drop(runtime);

        assert_eq!(ok(some(ordered.next().await)), Bytes::from_static(b"abc"));
        assert_eq!(ordered.recovery_offset(), 3);
        assert_eq!(ok(some(ordered.next().await)), Bytes::from_static(b"def"));
        assert_eq!(ok(some(ordered.next().await)), Bytes::from_static(b"ghi"));
        assert_eq!(ordered.recovery_offset(), 9);
        assert!(ordered.next().await.is_none());
    }

    #[test]
    fn read_ranges_are_issued_in_strict_source_order() {
        let ranges = SequentialRanges::new(10, 7, 3)
            .unwrap_or_else(|error| panic!("{error}"))
            .collect::<Vec<_>>();
        assert_eq!(
            ranges,
            vec![
                ReadRange {
                    offset: 10,
                    length: 3
                },
                ReadRange {
                    offset: 13,
                    length: 3
                },
                ReadRange {
                    offset: 16,
                    length: 1
                },
            ]
        );
    }

    #[tokio::test]
    async fn byte_budget_backpressures_until_a_chunk_is_consumed() {
        let cancel = CancellationToken::new();
        let (runtime, mut ordered) = channel(config(2, 3, 2), 0, 6, cancel);
        let first = ok(runtime.admit(0, 3).await);
        ok(first.complete(Bytes::from_static(b"abc")).await);
        let blocked = tokio::time::timeout(Duration::from_millis(20), runtime.admit(3, 3)).await;
        assert!(blocked.is_err());
        assert!(some(ordered.next().await).is_ok());
        let second = ok(runtime.admit(3, 3).await);
        ok(second.complete(Bytes::from_static(b"def")).await);
    }

    #[tokio::test]
    async fn cancellation_releases_blocked_submit_and_operation() {
        let cancel = CancellationToken::new();
        let (runtime, _ordered) = channel(config(1, 1, 1), 0, 2, cancel.clone());
        let _first = ok(runtime.admit(0, 1).await);
        let blocked_runtime = runtime.clone();
        let blocked = tokio::spawn(async move { blocked_runtime.admit(1, 1).await });
        tokio::task::yield_now().await;
        cancel.cancel();
        assert!(matches!(ok(blocked.await), Err(InflightFailure::Cancelled)));
    }

    #[tokio::test]
    async fn operation_concurrency_is_bounded() {
        let cancel = CancellationToken::new();
        let (runtime, _ordered) = channel(config(6, 6, 2), 0, 6, cancel);
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for offset in 0..6 {
            let admission = ok(runtime.admit(offset, 1).await);
            let active = active.clone();
            let peak = peak.clone();
            tasks.push(tokio::spawn(async move {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                active.fetch_sub(1, Ordering::SeqCst);
                admission.complete(Bytes::from_static(b"x")).await
            }));
        }
        for task in tasks {
            ok(ok(task.await));
        }
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn failure_and_gap_terminate_without_advancing_recovery_boundary() {
        let cancel = CancellationToken::new();
        let (runtime, mut ordered) = channel(config(2, 8, 1), 10, 18, cancel);
        let missing = ok(runtime.admit(10, 4).await);
        drop(missing);
        complete(&runtime, 14, Bytes::from_static(b"tail")).await;
        drop(runtime);
        assert_eq!(
            ordered.next().await,
            Some(Err(InflightFailure::Gap {
                expected: 10,
                next: 14
            }))
        );
        assert_eq!(ordered.recovery_offset(), 10);

        let cancel = CancellationToken::new();
        let (runtime, mut ordered) = channel(config(1, 8, 1), 0, 4, cancel);
        ok(runtime.fail(InflightFailure::Upstream).await);
        assert_eq!(ordered.next().await, Some(Err(InflightFailure::Upstream)));
        assert!(ordered.next().await.is_none());
    }

    #[tokio::test]
    async fn contiguous_short_close_is_failure_and_keeps_exact_recovery_offset() {
        for produced in [0, 3] {
            let cancel = CancellationToken::new();
            let (runtime, mut ordered) = channel(config(1, 8, 1), 0, 6, cancel);
            if produced > 0 {
                complete(&runtime, 0, Bytes::from_static(b"abc")).await;
                assert!(some(ordered.next().await).is_ok());
            }
            drop(runtime);
            assert_eq!(
                ordered.next().await,
                Some(Err(InflightFailure::Gap {
                    expected: produced,
                    next: 6
                }))
            );
            assert_eq!(ordered.recovery_offset(), produced);
        }
    }

    #[tokio::test]
    async fn receiver_cancellation_drops_pending_payload_and_releases_budget() {
        let cancel = CancellationToken::new();
        let (runtime, mut ordered) = channel(config(1, 4, 1), 0, 8, cancel.clone());
        let missing = ok(runtime.admit(0, 4).await);
        drop(missing);
        complete(&runtime, 4, Bytes::from_static(b"tail")).await;
        cancel.cancel();
        assert_eq!(ordered.next().await, Some(Err(InflightFailure::Cancelled)));
        assert_eq!(ordered.recovery_offset(), 0);
        assert_eq!(runtime.bytes.available_permits(), 4);
        assert_eq!(runtime.chunks.available_permits(), 1);
    }

    #[tokio::test]
    async fn cancellation_preempts_a_contiguous_buffered_chunk() {
        let cancel = CancellationToken::new();
        let (runtime, mut ordered) = channel(config(1, 4, 1), 0, 4, cancel.clone());
        complete(&runtime, 0, Bytes::from_static(b"data")).await;
        cancel.cancel();

        assert_eq!(ordered.next().await, Some(Err(InflightFailure::Cancelled)));
        assert_eq!(ordered.recovery_offset(), 0);
        assert_eq!(runtime.bytes.available_permits(), 4);
    }

    #[test]
    fn upstream_failure_diagnostics_cannot_echo_protocol_secrets() {
        assert_eq!(
            format!("{}", InflightFailure::Upstream),
            "upstream operation failed"
        );
        assert_eq!(format!("{:?}", InflightFailure::Upstream), "Upstream");
    }

    #[test]
    fn invalid_and_overflowing_ranges_are_rejected() {
        assert!(InflightConfig::new(0, 1, 1).is_err());
        assert!(SequentialRanges::new(u64::MAX, 1, 1).is_err());
        let cancel = CancellationToken::new();
        assert!(InflightRuntime::channel(config(1, 1, 1), 2, 1, cancel).is_err());
    }
}
