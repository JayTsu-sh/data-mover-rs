use super::*;

#[derive(Clone, Copy, Default)]
pub(super) enum Fault {
    #[default]
    None,
    Short,
    Duplicate,
    Empty,
    Outside,
    Oversized,
    PendingOpen,
    PendingAfterFirst,
    Error,
}

pub(super) struct Source {
    pub(super) descriptor: SourceDescriptor,
    pub(super) data: Bytes,
    pub(super) maximum: usize,
    pub(super) concurrency: usize,
    pub(super) positioned: bool,
    pub(super) fault: Fault,
    pub(super) reads: Mutex<Vec<(bool, ReadRequest)>>,
}

impl Source {
    pub(super) fn new(descriptor: SourceDescriptor, data: Bytes) -> Self {
        Self {
            descriptor,
            data,
            maximum: 4,
            concurrency: 2,
            positioned: true,
            fault: Fault::None,
            reads: Mutex::default(),
        }
    }

    async fn open(
        &self,
        positioned: bool,
        request: ReadRequest,
    ) -> Result<PositionedByteStream, StorageRoleFailure> {
        self.reads
            .lock()
            .unwrap()
            .push((positioned, request.clone()));
        if matches!(self.fault, Fault::PendingOpen) {
            return std::future::pending().await;
        }
        let range = request.range.unwrap();
        let mut items: Vec<_> = (range.start as usize..range.end as usize)
            .step_by(request.maximum_chunk_bytes)
            .map(|start| PositionedChunk {
                offset: start as u64,
                data: self
                    .data
                    .slice(start..(start + request.maximum_chunk_bytes).min(range.end as usize)),
            })
            .collect();
        if positioned {
            items.reverse();
        }
        match self.fault {
            Fault::Short => {
                items.pop();
            }
            Fault::Duplicate => items.push(items[0].clone()),
            Fault::Empty => items[0].data = Bytes::new(),
            Fault::Outside => items[0].offset = range.end,
            Fault::Oversized => items[0].data = Bytes::from(vec![0; self.maximum + 1]),
            Fault::Error => {
                return Err(super::super::super::failure::source_failure(
                    &self.descriptor.path,
                    FailureClass::Corruption,
                ));
            }
            Fault::PendingAfterFirst => {
                return Ok(futures::stream::iter(items.into_iter().take(1).map(Ok))
                    .chain(futures::stream::pending())
                    .boxed());
            }
            Fault::None | Fault::PendingOpen => {}
        }
        Ok(futures::stream::iter(items.into_iter().map(Ok)).boxed())
    }
}

#[async_trait]
impl ReadSource for Source {
    fn maximum_read_chunk_bytes(&self) -> usize {
        self.maximum
    }
    fn read_concurrency(&self) -> usize {
        self.concurrency
    }
    fn supports_positioned_read(&self) -> bool {
        self.positioned
    }
    async fn describe(&self, _: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        Ok(self.descriptor.clone())
    }
    async fn read_positioned(
        &self,
        request: ReadRequest,
    ) -> Result<PositionedByteStream, StorageRoleFailure> {
        self.open(true, request).await
    }
    async fn read(&self, request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        Ok(self
            .open(false, request)
            .await?
            .map(|chunk| chunk.map(|chunk| chunk.data))
            .boxed())
    }
}

#[derive(Default)]
pub(super) enum AnchorBehavior {
    #[default]
    Succeed,
    Refuse,
    Pending,
}

#[derive(Default)]
pub(super) struct Destination {
    pub(super) intent: bool,
    pub(super) anchor: AnchorBehavior,
    pub(super) refuse_write: bool,
    pub(super) require_persist_to_observe: bool,
    pub(super) observations: std::sync::atomic::AtomicUsize,
    pub(super) snapshots: Mutex<Vec<CheckpointData>>,
    pub(super) chunks: Mutex<Vec<PositionedChunk>>,
    pub(super) drain: Option<Arc<(tokio::sync::Notify, tokio::sync::Notify)>>,
    pub(super) drained: std::sync::atomic::AtomicBool,
    pub(super) persist_gate: Option<Arc<(tokio::sync::Notify, tokio::sync::Notify)>>,
    pub(super) persist_settled: std::sync::atomic::AtomicBool,
}

impl Destination {
    fn refusal(stage: &PreparedStage) -> StorageRoleFailure {
        crate::storage::checkpoint_stream::failure(
            stage.final_destination.path(),
            FailureClass::Unsupported,
            "test operation refused",
        )
    }

    fn assert_source_evidence(&self, stage: &PreparedStage, chunk: &PositionedChunk) {
        let data = CheckpointData::decode(&stage.checkpoint_snapshot().unwrap()).unwrap();
        let hash = RangeHash::decode(&data.hash, data.size).unwrap();
        let range = chunk.offset..chunk.offset + chunk.data.len() as u64;
        assert!(hash.covers(range.clone()));
        if self.intent {
            let anchored = self.snapshots.lock().unwrap().last().unwrap().clone();
            assert!(
                RangeHash::decode(&anchored.hash, anchored.size)
                    .unwrap()
                    .covers(range)
            );
        }
    }
}

#[async_trait]
impl StagedDestination for Destination {
    fn supports_hash_checkpoint(&self) -> bool {
        true
    }
    fn requires_hash_intent(&self) -> bool {
        self.intent
    }
    async fn persist_hash_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<(), StorageRoleFailure> {
        if let Some(gate) = &self.persist_gate {
            gate.0.notify_one();
            gate.1.notified().await;
            self.persist_settled
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        match self.anchor {
            AnchorBehavior::Pending => return std::future::pending().await,
            AnchorBehavior::Refuse => return Err(Self::refusal(stage)),
            AnchorBehavior::Succeed => {}
        }
        let data = CheckpointData::decode(&stage.checkpoint_snapshot().unwrap()).unwrap();
        self.snapshots.lock().unwrap().push(data);
        if self.require_persist_to_observe {
            stage
                .recovery_enabled
                .store(true, std::sync::atomic::Ordering::Release);
        }
        Ok(())
    }
    async fn write_checkpointed(
        &self,
        stage: &PreparedStage,
        mut input: PositionedByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        if self.refuse_write {
            return Err(Self::refusal(stage));
        }
        assert!(stage.checkpoint_enabled());
        let mut received = stage.checkpoint_ranges();
        while let Some(chunk) = input.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    if let Some(gate) = &self.drain {
                        gate.0.notify_one();
                        gate.1.notified().await;
                    }
                    self.drained
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    return Err(error);
                }
            };
            self.assert_source_evidence(stage, &chunk);
            stage.confirmed_write(chunk.offset, chunk.data.len() as u64);
            crate::storage::checkpoint::insert_range(
                &mut received,
                chunk.offset..chunk.offset + chunk.data.len() as u64,
            );
            self.chunks.lock().unwrap().push(chunk);
        }
        if !crate::storage::checkpoint::missing_ranges(&received, 0..stage.checkpoint_size())
            .is_empty()
        {
            return Err(Self::refusal(stage));
        }
        stage.checkpoint_barrier(stage.checkpoint_size());
        Ok(WriteEvidence {
            persisted_bytes: stage.checkpoint_size(),
        })
    }
    async fn prepare_at_destination(
        &self,
        _: DestinationPrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        unreachable!()
    }
    async fn write(
        &self,
        stage: &PreparedStage,
        _: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        Err(Self::refusal(stage))
    }
    async fn observe_checkpoint(
        &self,
        stage: &PreparedStage,
    ) -> Result<CheckpointObservation, StorageRoleFailure> {
        self.observations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.require_persist_to_observe && !stage.recovery_enabled() {
            return Err(Self::refusal(stage));
        }
        Ok(CheckpointObservation {
            durable_prefix: stage.checkpoint_prefix(),
        })
    }
    async fn verify(
        &self,
        stage: &PreparedStage,
        _: VerifyRequest,
    ) -> Result<VerificationEvidence, StorageRoleFailure> {
        Err(Self::refusal(stage))
    }
    async fn publish(
        &self,
        stage: &PreparedStage,
        _: PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        Err(PublicationFailure {
            error: Self::refusal(stage),
            final_destination_changed: false,
        })
    }
    async fn discard(&self, _: PreparedStage) -> Result<(), StorageRoleFailure> {
        Ok(())
    }
}
