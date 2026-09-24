#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::model::{BackendKind, EntryKind, IdentityStrength, SourceIdentity, SourceVersion};
    use crate::storage::artifacts::{ArtifactKind, artifact_name};
    use crate::storage::{
        DestinationPrepareRequest, FinalDestination, PrepareFact, ResumeMode, SourceDescriptor,
    };
    use futures::stream;

    #[derive(Default)]
    struct CheckpointGate {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
        fail: std::sync::atomic::AtomicBool,
    }

    #[derive(Default)]
    pub(in crate::storage::backends::nfs) struct FakeProtocol {
        pub(in crate::storage::backends::nfs) files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        opens: std::sync::atomic::AtomicU64,
        closes: Arc<std::sync::atomic::AtomicU64>,
        pub(in crate::storage::backends::nfs) rename_mode: std::sync::atomic::AtomicU8,
        active_writes: Arc<std::sync::atomic::AtomicUsize>,
        maximum_active_writes: Arc<std::sync::atomic::AtomicUsize>,
        active_reads: Arc<std::sync::atomic::AtomicUsize>,
        maximum_active_reads: Arc<std::sync::atomic::AtomicUsize>,
        maximum_read_chunk: std::sync::atomic::AtomicUsize,
        maximum_write_chunk: std::sync::atomic::AtomicUsize,
        write_sizes: Arc<Mutex<Vec<usize>>>,
        deferred_writes: Arc<std::sync::atomic::AtomicU64>,
        checkpoint_pending_writes: Arc<std::sync::atomic::AtomicU64>,
        uncommitted_writes: Arc<std::sync::atomic::AtomicU64>,
        checkpoints: Arc<std::sync::atomic::AtomicU64>,
        uncommitted_closes: Arc<std::sync::atomic::AtomicU64>,
        failed_write_offset: Arc<Mutex<Option<u64>>>,
        checkpoint_gate: Arc<Mutex<Option<Arc<CheckpointGate>>>>,
        unstable_pressure: Arc<std::sync::atomic::AtomicBool>,
        size_calls: std::sync::atomic::AtomicU64,
        /// Writes of a pointer (through its temporary): one per recorded checkpoint.
        pointer_writes: Arc<std::sync::atomic::AtomicU64>,
    }

    struct FakeFile {
        path: String,
        files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        closes: Arc<std::sync::atomic::AtomicU64>,
        active_writes: Arc<std::sync::atomic::AtomicUsize>,
        maximum_active_writes: Arc<std::sync::atomic::AtomicUsize>,
        active_reads: Arc<std::sync::atomic::AtomicUsize>,
        maximum_active_reads: Arc<std::sync::atomic::AtomicUsize>,
        write_sizes: Arc<Mutex<Vec<usize>>>,
        deferred_writes: Arc<std::sync::atomic::AtomicU64>,
        checkpoint_pending_writes: Arc<std::sync::atomic::AtomicU64>,
        uncommitted_writes: Arc<std::sync::atomic::AtomicU64>,
        checkpoints: Arc<std::sync::atomic::AtomicU64>,
        uncommitted_closes: Arc<std::sync::atomic::AtomicU64>,
        failed_write_offset: Arc<Mutex<Option<u64>>>,
        checkpoint_gate: Arc<Mutex<Option<Arc<CheckpointGate>>>>,
        unstable_pressure: Arc<std::sync::atomic::AtomicBool>,
        pointer_writes: Arc<std::sync::atomic::AtomicU64>,
    }

    /// A destination-kept stage records its checkpoints in its pointer and never registers one
    /// where data-mover runs.
    struct NeverRegistered;

    #[async_trait]
    impl crate::storage::CheckpointRegistration for NeverRegistered {
        async fn register(
            &self,
            _stage: &PreparedStage,
            _identity: RecoveryIdentity,
        ) -> Result<(), StorageRoleFailure> {
            panic!("an NFS stage keeps its recovery state at the destination");
        }
    }

    #[async_trait]
    impl NfsStageFile for FakeFile {
        async fn read_at(&self, offset: u64, count: usize) -> Result<Bytes, NfsProtocolFailure> {
            let active = self
                .active_reads
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            self.maximum_active_reads
                .fetch_max(active, std::sync::atomic::Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let files = self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let value = files
                .get(&self.path)
                .ok_or_else(NfsProtocolFailure::protocol)?;
            let start = usize::try_from(offset).map_err(|_| NfsProtocolFailure::protocol())?;
            let end = start
                .checked_add(count)
                .ok_or_else(NfsProtocolFailure::protocol)?;
            let result = if start >= value.len() {
                Bytes::new()
            } else {
                Bytes::copy_from_slice(&value[start..end.min(value.len())])
            };
            self.active_reads
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            Ok(result)
        }

        async fn write_at(&self, offset: u64, data: Bytes) -> Result<u64, NfsProtocolFailure> {
            if self.path.contains(".pointer") {
                self.pointer_writes
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let gate = self
                    .checkpoint_gate
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                if let Some(gate) = gate {
                    gate.entered.notify_one();
                    gate.release.notified().await;
                    if gate.fail.load(std::sync::atomic::Ordering::SeqCst) {
                        return Err(NfsProtocolFailure::protocol());
                    }
                }
                let mut files = self
                    .files
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let value = files
                    .get_mut(&self.path)
                    .ok_or_else(NfsProtocolFailure::protocol)?;
                let start = usize::try_from(offset).map_err(|_| NfsProtocolFailure::protocol())?;
                let end = start
                    .checked_add(data.len())
                    .ok_or_else(NfsProtocolFailure::protocol)?;
                value.resize(value.len().max(end), 0);
                value[start..end].copy_from_slice(&data);
                return Ok(data.len() as u64);
            }
            self.write_sizes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(data.len());
            let active = self
                .active_writes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            self.maximum_active_writes
                .fetch_max(active, std::sync::atomic::Ordering::SeqCst);
            let should_fail = self
                .failed_write_offset
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some_and(|failed| failed == offset);
            tokio::time::sleep(std::time::Duration::from_millis(if should_fail {
                30
            } else {
                5
            }))
            .await;
            if should_fail {
                self.active_writes
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                return Err(NfsProtocolFailure::protocol());
            }
            let mut files = self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let value = files
                .get_mut(&self.path)
                .ok_or_else(NfsProtocolFailure::protocol)?;
            let start = usize::try_from(offset).map_err(|_| NfsProtocolFailure::protocol())?;
            let end = start
                .checked_add(data.len())
                .ok_or_else(NfsProtocolFailure::protocol)?;
            value.resize(value.len().max(end), 0);
            value[start..end].copy_from_slice(&data);
            self.active_writes
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            Ok(data.len() as u64)
        }

        async fn write_deferred_at(
            &self,
            offset: u64,
            data: Bytes,
        ) -> Result<u64, NfsProtocolFailure> {
            let count = self
                .deferred_writes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            let written = self.write_at(offset, data).await?;
            // Model the legacy non-periodic batching path: every four unstable receipts.
            if self
                .unstable_pressure
                .load(std::sync::atomic::Ordering::SeqCst)
                && count.is_multiple_of(4)
            {
                self.checkpoint().await?;
            }
            Ok(written)
        }

        async fn write_uncommitted_at(
            &self,
            offset: u64,
            data: Bytes,
        ) -> Result<u64, NfsProtocolFailure> {
            self.uncommitted_writes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.write_at(offset, data).await
        }

        async fn write_until_checkpoint_at(
            &self,
            offset: u64,
            data: Bytes,
        ) -> Result<u64, NfsProtocolFailure> {
            self.checkpoint_pending_writes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.write_at(offset, data).await
        }

        async fn checkpoint(&self) -> Result<(), NfsProtocolFailure> {
            self.checkpoints
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }

        async fn set_len(&self, size: u64) -> Result<(), NfsProtocolFailure> {
            let size = usize::try_from(size).map_err(|_| NfsProtocolFailure::protocol())?;
            let mut files = self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            files
                .get_mut(&self.path)
                .ok_or_else(NfsProtocolFailure::protocol)?
                .resize(size, 0);
            Ok(())
        }

        async fn close(&self) -> Result<(), NfsProtocolFailure> {
            if !self.path.contains(".pointer") {
                self.closes
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Ok(())
        }

        async fn close_uncommitted(&self) -> Result<(), NfsProtocolFailure> {
            self.uncommitted_closes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.close().await
        }
    }

    #[async_trait]
    impl NfsStagedProtocol for FakeProtocol {
        fn read_inflight(&self) -> usize {
            4
        }

        fn write_inflight(&self) -> usize {
            4
        }

        fn maximum_read_chunk_bytes(&self) -> usize {
            self.maximum_read_chunk
                .load(std::sync::atomic::Ordering::SeqCst)
                .max(1)
        }

        fn maximum_write_chunk_bytes(&self) -> usize {
            self.maximum_write_chunk
                .load(std::sync::atomic::Ordering::SeqCst)
                .max(1)
        }

        async fn create_empty(&self, path: &StoragePath) -> Result<(), NfsProtocolFailure> {
            let mut files = self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if files.insert(path.as_str().to_owned(), Vec::new()).is_some() {
                return Err(NfsProtocolFailure::new(
                    FailureClass::Conflict,
                    Transience::Permanent,
                ));
            }
            Ok(())
        }

        async fn open_read(
            &self,
            path: &StoragePath,
        ) -> Result<Box<dyn NfsStageFile>, NfsProtocolFailure> {
            self.open_write(path).await
        }

        async fn open_write(
            &self,
            path: &StoragePath,
        ) -> Result<Box<dyn NfsStageFile>, NfsProtocolFailure> {
            if !self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(path.as_str())
            {
                return Err(NfsProtocolFailure::new(
                    FailureClass::NotFound,
                    Transience::Permanent,
                ));
            }
            if !path.as_str().contains(".pointer") {
                self.opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Ok(Box::new(FakeFile {
                path: path.as_str().to_owned(),
                files: Arc::clone(&self.files),
                closes: Arc::clone(&self.closes),
                active_writes: Arc::clone(&self.active_writes),
                maximum_active_writes: Arc::clone(&self.maximum_active_writes),
                active_reads: Arc::clone(&self.active_reads),
                maximum_active_reads: Arc::clone(&self.maximum_active_reads),
                write_sizes: Arc::clone(&self.write_sizes),
                deferred_writes: Arc::clone(&self.deferred_writes),
                checkpoint_pending_writes: Arc::clone(&self.checkpoint_pending_writes),
                uncommitted_writes: Arc::clone(&self.uncommitted_writes),
                checkpoints: Arc::clone(&self.checkpoints),
                uncommitted_closes: Arc::clone(&self.uncommitted_closes),
                failed_write_offset: Arc::clone(&self.failed_write_offset),
                checkpoint_gate: Arc::clone(&self.checkpoint_gate),
                unstable_pressure: Arc::clone(&self.unstable_pressure),
                pointer_writes: Arc::clone(&self.pointer_writes),
            }))
        }

        async fn size(&self, path: &StoragePath) -> Result<u64, NfsProtocolFailure> {
            self.size_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(path.as_str())
                .map(|value| value.len() as u64)
                .ok_or(NfsProtocolFailure::new(
                    FailureClass::NotFound,
                    Transience::Permanent,
                ))
        }

        async fn rename(
            &self,
            from: &StoragePath,
            to: &StoragePath,
        ) -> Result<(), NfsProtocolFailure> {
            let mut files = self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let value = files.remove(from.as_str()).ok_or(NfsProtocolFailure::new(
                FailureClass::NotFound,
                Transience::Permanent,
            ))?;
            match self.rename_mode.load(std::sync::atomic::Ordering::SeqCst) {
                2 => Err(NfsProtocolFailure::protocol()),
                3 => {
                    files.insert(to.as_str().to_owned(), b"wrong".to_vec());
                    Err(NfsProtocolFailure::protocol())
                }
                4 => {
                    files.insert(to.as_str().to_owned(), value);
                    Err(NfsProtocolFailure::protocol())
                }
                _ => {
                    files.insert(to.as_str().to_owned(), value);
                    Ok(())
                }
            }
        }

        async fn delete(&self, path: &StoragePath) -> Result<(), NfsProtocolFailure> {
            self.files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(path.as_str())
                .map(|_| ())
                .ok_or(NfsProtocolFailure::new(
                    FailureClass::NotFound,
                    Transience::Permanent,
                ))
        }
    }

    #[tokio::test]
    async fn ephemeral_prepare_reuses_created_handle_for_first_write() {
        let (adapter, protocol, identity) = adapter();
        let stage = prepare_ephemeral_stage(&adapter, prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(protocol.opens.load(std::sync::atomic::Ordering::SeqCst), 1);

        adapter
            .write(
                &stage,
                Box::pin(stream::iter([Ok(Bytes::from_static(b"payload"))])),
            )
            .await
            .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(
            protocol.opens.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the write must reuse the handle opened by ephemeral prepare"
        );
        assert_eq!(protocol.closes.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            protocol
                .deferred_writes
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(
            protocol
                .checkpoint_pending_writes
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            protocol
                .checkpoints
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    #[test]
    fn automatic_recovery_uses_the_same_64_mib_threshold_as_local() {
        let (adapter, _, _) = adapter();
        assert_eq!(
            adapter.automatic_checkpoint_interval_bytes(),
            Some(64 * 1024 * 1024)
        );
    }

    #[tokio::test]
    async fn atomic_replace_uses_unstable_writes_without_a_commit_checkpoint() {
        let (adapter, protocol, identity) = adapter();
        let mut stage = prepare_ephemeral_stage(&adapter, prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        stage.durable_publication = false;

        adapter
            .write(
                &stage,
                Box::pin(stream::iter([Ok(Bytes::from_static(b"payload"))])),
            )
            .await
            .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(
            protocol
                .uncommitted_writes
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            protocol
                .deferred_writes
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(
            protocol
                .checkpoints
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(
            protocol
                .uncommitted_closes
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert!(!stage.recovery_enabled());
        adapter
            .publish(
                &stage,
                PublishRequest {
                    expected_size: 7,
                    expected_blake3: Some(*blake3::hash(b"payload").as_bytes()),
                    cancel: tokio_util::sync::CancellationToken::new(),
                },
            )
            .await
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            protocol
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get("final.bin"),
            Some(&b"payload".to_vec())
        );
    }

    #[tokio::test]
    async fn checkpointed_below_threshold_commits_final_data_without_a_pointer() {
        let (adapter, protocol, identity) = adapter();
        let stage = prepare_ephemeral_stage(&adapter, prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));

        adapter
            .write(
                &stage,
                Box::pin(stream::iter([Ok(Bytes::from_static(b"payload"))])),
            )
            .await
            .unwrap_or_else(|error| panic!("{error}"));

        assert!(stage.deferred_checkpoint.is_none());
        assert_eq!(
            protocol
                .deferred_writes
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(
            protocol
                .checkpoint_pending_writes
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            protocol
                .checkpoints
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            protocol
                .uncommitted_closes
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert!(!stage.recovery_enabled());
    }

    #[tokio::test]
    async fn checkpointed_writes_its_pointer_after_the_first_durable_interval() {
        let (adapter, protocol, identity) = adapter();
        let mut stage = prepare_ephemeral_stage(&adapter, prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        enable_deferred_checkpointed(&mut stage, 4, 8);

        adapter
            .write(
                &stage,
                Box::pin(stream::iter([
                    Ok(Bytes::from_static(b"abcd")),
                    Ok(Bytes::from_static(b"efgh")),
                ])),
            )
            .await
            .unwrap_or_else(|error| panic!("{error}"));

        // The first interval's pointer, then the final prefix's.
        assert_eq!(
            protocol
                .pointer_writes
                .load(std::sync::atomic::Ordering::SeqCst),
            2
        );
        assert!(stage.recovery_enabled());
        assert_eq!(
            protocol
                .checkpoints
                .load(std::sync::atomic::Ordering::SeqCst),
            2
        );
        // The pointer write overlaps subsequent writes: either tracked mode is safe.
        let deferred = protocol
            .deferred_writes
            .load(std::sync::atomic::Ordering::SeqCst);
        let pending = protocol
            .checkpoint_pending_writes
            .load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(deferred + pending, 2);
        assert!(pending >= 1);
    }

    #[tokio::test]
    async fn checkpointed_persists_every_completed_checkpoint_interval() {
        let (adapter, protocol, identity) = adapter();
        let mut stage = prepare_ephemeral_stage(&adapter, prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        enable_deferred_checkpointed(&mut stage, 4, 13);
        let input_error = failure(
            stage.final_destination.path(),
            FailureClass::Protocol,
            Transience::Permanent,
        );

        let result = adapter
            .write(
                &stage,
                Box::pin(stream::iter([
                    Ok(Bytes::from_static(b"abcd")),
                    Ok(Bytes::from_static(b"efgh")),
                    Ok(Bytes::from_static(b"ijkl")),
                    Err(input_error),
                ])),
            )
            .await;

        assert!(result.is_err());
        assert_eq!(
            protocol
                .pointer_writes
                .load(std::sync::atomic::Ordering::SeqCst),
            3,
            "one pointer per completed interval"
        );
        assert_eq!(
            adapter
                .observe_checkpoint(&stage)
                .await
                .unwrap_or_else(|error| panic!("{error}"))
                .durable_prefix,
            12
        );
        assert_eq!(
            protocol
                .checkpoints
                .load(std::sync::atomic::Ordering::SeqCst),
            4,
            "three interval barriers plus failure finalization"
        );
    }

    #[tokio::test]
    async fn checkpointed_prepare_keeps_the_created_handle_for_first_write() {
        let (adapter, protocol, identity) = adapter();
        let stage = prepare_stage(&adapter, prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(
            protocol.opens.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the first pointer must not force the create handle to close"
        );

        adapter
            .write(
                &stage,
                Box::pin(stream::iter([Ok(Bytes::from_static(b"payload"))])),
            )
            .await
            .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(
            protocol.opens.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the first checkpointed write must reuse the create handle"
        );
        assert_eq!(protocol.closes.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(
            protocol
                .deferred_writes
                .load(std::sync::atomic::Ordering::SeqCst)
                > 0
        );
        assert_eq!(
            protocol
                .checkpoints
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    #[tokio::test]
    async fn discarding_one_files_stage_leaves_another_files_stage_and_pointer() {
        let (adapter, protocol, identity) = adapter();
        let first = prepare_stage(&adapter, prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        // One file has one stage at a time: the second stage is another file's.
        let mut other = prepare_request(&identity);
        other.final_destination = FinalDestination::new(
            StoragePath::new("other.bin").unwrap_or_else(|error| panic!("{error}")),
        );
        let second = prepare_stage(&adapter, other)
            .await
            .unwrap_or_else(|error| panic!("{error}"));

        adapter
            .discard(second)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(
            adapter
                .observe_checkpoint(&first)
                .await
                .unwrap_or_else(|error| panic!("{error}"))
                .durable_prefix,
            0
        );
        assert_eq!(
            protocol
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            2,
            "the first stage and its pointer must remain"
        );
        adapter
            .discard(first)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(
            protocol
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
        );
    }

    #[tokio::test]
    async fn ephemeral_discard_closes_created_handle_and_removes_stage() {
        let (adapter, protocol, identity) = adapter();
        let stage = prepare_ephemeral_stage(&adapter, prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));

        adapter
            .discard(stage)
            .await
            .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(protocol.closes.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(
            protocol
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
        );
    }

    #[tokio::test]
    async fn staged_write_keeps_multiple_nfs_writes_inflight() {
        let (adapter, protocol, identity) = adapter();
        let stage = prepare_stage(&adapter, prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let input: ByteStream = Box::pin(stream::iter([
            Ok(Bytes::from_static(b"aa")),
            Ok(Bytes::from_static(b"bb")),
            Ok(Bytes::from_static(b"cc")),
            Ok(Bytes::from_static(b"dd")),
        ]));
        adapter
            .write(&stage, input)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(
            protocol
                .maximum_active_writes
                .load(std::sync::atomic::Ordering::SeqCst)
                > 1
        );
    }

    #[tokio::test]
    async fn failed_lower_offset_truncates_completed_higher_writes() {
        let (adapter, protocol, identity) = adapter();
        protocol
            .maximum_write_chunk
            .store(2, std::sync::atomic::Ordering::SeqCst);
        *protocol
            .failed_write_offset
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(0);
        let stage = prepare_stage(&adapter, prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));

        let result = adapter
            .write(
                &stage,
                Box::pin(stream::iter([Ok(Bytes::from_static(b"abcdefgh"))])),
            )
            .await;

        assert!(result.is_err());
        assert_eq!(
            adapter
                .observe_checkpoint(&stage)
                .await
                .unwrap_or_else(|error| panic!("{error}"))
                .durable_prefix,
            0
        );
    }

    #[tokio::test]
    async fn staged_write_splits_at_the_negotiated_protocol_limit() {
        let (adapter, protocol, identity) = adapter();
        protocol
            .maximum_write_chunk
            .store(3, std::sync::atomic::Ordering::SeqCst);
        let stage = prepare_stage(&adapter, prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let input: ByteStream = Box::pin(stream::iter([Ok(Bytes::from_static(b"abcdefgh"))]));
        adapter
            .write(&stage, input)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let mut sizes = protocol
            .write_sizes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        sizes.sort_unstable();
        assert_eq!(sizes, vec![2, 3, 3]);
    }

    #[tokio::test]
    async fn staged_verification_keeps_multiple_ordered_nfs_reads_inflight() {
        let (adapter, protocol, identity) = adapter();
        let stage = prepare_stage(&adapter, prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let piece = Bytes::from(vec![7; 1024 * 1024]);
        let input: ByteStream = Box::pin(stream::iter([
            Ok(piece.clone()),
            Ok(piece.clone()),
            Ok(piece.clone()),
            Ok(piece),
        ]));
        adapter
            .write(&stage, input)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let expected = vec![7; 4 * 1024 * 1024];
        adapter
            .verify(
                &stage,
                VerifyRequest {
                    expected_size: expected.len() as u64,
                    expected_blake3: *blake3::hash(&expected).as_bytes(),
                    cancel: tokio_util::sync::CancellationToken::new(),
                    published: None,
                },
            )
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(
            protocol
                .maximum_active_reads
                .load(std::sync::atomic::Ordering::SeqCst)
                > 1
        );
    }

    #[tokio::test]
    async fn staged_verification_does_not_issue_a_separate_size_rpc() {
        let (adapter, protocol, identity) = adapter();
        let stage = prepare_stage(&adapter, prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        adapter
            .write(
                &stage,
                Box::pin(stream::iter([Ok(Bytes::from_static(b"abcdef"))])),
            )
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let size_calls_before = protocol
            .size_calls
            .load(std::sync::atomic::Ordering::SeqCst);

        adapter
            .verify(
                &stage,
                VerifyRequest {
                    expected_size: 6,
                    expected_blake3: *blake3::hash(b"abcdef").as_bytes(),
                    cancel: tokio_util::sync::CancellationToken::new(),
                    published: None,
                },
            )
            .await
            .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(
            protocol
                .size_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            size_calls_before,
            "verification should prove exact length through its open read handle"
        );
    }

    #[tokio::test]
    async fn staged_verification_rejects_a_trailing_byte() {
        let (adapter, _protocol, identity) = adapter();
        let stage = prepare_stage(&adapter, prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        adapter
            .write(
                &stage,
                Box::pin(stream::iter([Ok(Bytes::from_static(b"abcdef!"))])),
            )
            .await
            .unwrap_or_else(|error| panic!("{error}"));

        assert!(
            adapter
                .verify(
                    &stage,
                    VerifyRequest {
                        expected_size: 6,
                        expected_blake3: *blake3::hash(b"abcdef").as_bytes(),
                        cancel: tokio_util::sync::CancellationToken::new(),
                        published: None,
                    },
                )
                .await
                .is_err()
        );
    }

    /// NFS stages are prepared only at the destination: the store-era entry points refuse, so a
    /// caller that bypassed the engine's `recovery_at_destination` check fails loudly.
    #[tokio::test]
    async fn store_era_entry_points_are_unsupported() {
        let (adapter, _, identity) = adapter();
        let refused = adapter.prepare(prepare_request(&identity)).await;
        assert!(matches!(
            refused,
            Err(StorageRoleFailure::Entry(ref error)) if error.class() == FailureClass::Unsupported
        ));
        assert!(
            adapter
                .prepare_ephemeral(prepare_request(&identity))
                .await
                .is_err()
        );
    }

    pub(in crate::storage::backends::nfs) fn adapter() -> (
        NfsStagedDestinationAdapter,
        Arc<FakeProtocol>,
        BackendIdentity,
    ) {
        let protocol = Arc::new(FakeProtocol::default());
        protocol
            .maximum_read_chunk
            .store(1024 * 1024, std::sync::atomic::Ordering::SeqCst);
        protocol
            .maximum_write_chunk
            .store(1024 * 1024, std::sync::atomic::Ordering::SeqCst);
        let identity = BackendIdentity::new(BackendKind::Nfs, "test-nfs")
            .unwrap_or_else(|error| panic!("{error:?}"));
        (
            NfsStagedDestinationAdapter::new(protocol.clone(), identity.clone()),
            protocol,
            identity,
        )
    }

    fn prepare_request(identity: &BackendIdentity) -> PrepareRequest {
        PrepareRequest {
            final_destination: FinalDestination::new(
                StoragePath::new("final.bin").unwrap_or_else(|error| panic!("{error}")),
            ),
            source: SourceDescriptor {
                path: StoragePath::new("source.bin").unwrap_or_else(|error| panic!("{error}")),
                kind: EntryKind::File,
                size: Some(6),
                source_identity: SourceIdentity::new(
                    identity.clone(),
                    IdentityStrength::StableWithinBackend,
                    b"source",
                )
                .unwrap_or_else(|error| panic!("{error}")),
                backend_fact: None,
                content_version: None,
                inline_timestamps: None,
                inline_mode: None,
                version: SourceVersion::Current,
            },
            recovery_binding: [7; 32],
        }
    }

    fn enable_deferred_checkpointed(
        stage: &mut PreparedStage,
        interval_bytes: u64,
        source_size: u64,
    ) {
        stage.deferred_checkpoint = Some(crate::storage::DeferredCheckpoint {
            interval_bytes,
            source_size,
            registration: Arc::new(NeverRegistered),
        });
    }

    /// A fresh stage whose pointer is written at prepare: whatever an earlier stage of the file
    /// left is cleaned up first.
    async fn prepare_stage(
        adapter: &NfsStagedDestinationAdapter,
        request: PrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        adapter
            .prepare_at_destination(at_destination(request, ResumeMode::Restart, true))
            .await
    }

    /// A fresh stage whose first pointer waits for a checkpoint.
    async fn prepare_ephemeral_stage(
        adapter: &NfsStagedDestinationAdapter,
        request: PrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        adapter
            .prepare_at_destination(at_destination(request, ResumeMode::Restart, false))
            .await
    }

    /// Resumes what an earlier stage of the file left, as a new process would.
    async fn resume_stage(
        adapter: &NfsStagedDestinationAdapter,
        request: PrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        adapter
            .prepare_at_destination(at_destination(request, ResumeMode::Discover, true))
            .await
    }

    fn at_destination(
        request: PrepareRequest,
        resume: ResumeMode,
        recoverable: bool,
    ) -> DestinationPrepareRequest {
        DestinationPrepareRequest::new(request, [3; 32])
            .with_resume(resume)
            .with_recoverable(recoverable)
    }

    #[test]
    fn final_and_stage_paths_are_confined() {
        assert!(
            checked_final(&StoragePath::new("file").unwrap_or_else(|error| panic!("{error}")))
                .is_ok()
        );
        assert!(checked_final(&StoragePath::root()).is_err());
        assert!(
            checked_final(&StoragePath::new("../escape").unwrap_or_else(|error| panic!("{error}")))
                .is_err()
        );
        assert!(
            checked_final(
                &StoragePath::new("nested/.data-mover-forged")
                    .unwrap_or_else(|error| panic!("{error}"))
            )
            .is_err()
        );
    }

    #[test]
    fn only_the_deterministic_stage_name_is_accepted() {
        let (_, _, identity) = adapter();
        let deterministic = artifact_name("final.bin", ArtifactKind::Stage);
        for (token, at_destination, accepted) in [
            // The random names stages had before ADR-0006 C10, even for a destination-kept stage.
            (
                format!(".data-mover-{}-fresh.stage", "a".repeat(32)),
                true,
                false,
            ),
            (
                format!(
                    ".data-mover-{}-claim-{}.stage",
                    "a".repeat(32),
                    "b".repeat(32)
                ),
                true,
                false,
            ),
            (".data-mover-staging/old.part".to_owned(), true, false),
            // The deterministic name, only for a stage prepared at the destination.
            (deterministic.clone(), false, false),
            (deterministic, true, true),
        ] {
            let mut stage = PreparedStage::new(
                identity.clone(),
                FinalDestination::new(
                    StoragePath::new("final.bin").unwrap_or_else(|error| panic!("{error}")),
                ),
                Bytes::from(token.clone()),
                [7; 32],
                0,
                None,
            );
            if at_destination {
                stage.mark_at_destination(PrepareFact::Fresh);
            }
            assert_eq!(
                super::super::at_destination::stage_path(&stage).is_ok(),
                accepted,
                "{token} at_destination={at_destination}"
            );
        }
    }

    #[tokio::test]
    async fn staged_lifecycle_writes_verifies_publishes_and_closes_handles() {
        let (adapter, protocol, identity) = adapter();
        let stage = prepare_stage(&adapter, prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let input: ByteStream = Box::pin(stream::iter([
            Ok(Bytes::from_static(b"abc")),
            Ok(Bytes::from_static(b"def")),
        ]));
        assert_eq!(
            adapter
                .write(&stage, input)
                .await
                .unwrap_or_else(|error| panic!("{error}"))
                .persisted_bytes,
            6
        );
        assert_eq!(
            adapter
                .observe_checkpoint(&stage)
                .await
                .unwrap_or_else(|error| panic!("{error}"))
                .durable_prefix,
            6
        );
        let hash = *blake3::hash(b"abcdef").as_bytes();
        assert_eq!(
            adapter
                .verify(
                    &stage,
                    VerifyRequest {
                        expected_size: 6,
                        expected_blake3: hash,
                        cancel: tokio_util::sync::CancellationToken::new(),
                        published: None,
                    }
                )
                .await
                .unwrap_or_else(|error| panic!("{error}"))
                .blake3,
            hash
        );
        let published = adapter
            .publish(
                &stage,
                PublishRequest {
                    expected_size: 6,
                    expected_blake3: Some(hash),
                    cancel: tokio_util::sync::CancellationToken::new(),
                },
            )
            .await
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(published.disposition, PublicationDisposition::Published);
        assert_eq!(
            protocol
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get("final.bin"),
            Some(&b"abcdef".to_vec())
        );
        assert_eq!(protocol.closes.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    /// A process that resumed a stage and died before using it leaves a stage the next process
    /// resumes again, from the same prefix.
    #[tokio::test]
    async fn a_resume_whose_result_is_lost_is_resumed_again() {
        let (adapter, protocol, identity) = adapter();
        let request = prepare_request(&identity);
        let stage = prepare_stage(&adapter, request.clone())
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        adapter
            .write(
                &stage,
                Box::pin(stream::iter([Ok(Bytes::from_static(b"abc"))])),
            )
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        drop(stage);
        drop(adapter);

        let first = NfsStagedDestinationAdapter::new(protocol.clone(), identity.clone());
        let lost = resume_stage(&first, request.clone())
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(lost.prepare_fact, PrepareFact::Resumed { bytes: 3 });
        drop(lost);
        drop(first);

        let restarted = NfsStagedDestinationAdapter::new(protocol, identity);
        let resumed = resume_stage(&restarted, request)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(resumed.prepare_fact, PrepareFact::Resumed { bytes: 3 });
        assert_eq!(resumed.write_offset, 3);
        restarted
            .discard(resumed)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
    }

    #[tokio::test]
    async fn precancelled_verification_does_not_open_remote_state() {
        let (adapter, protocol, identity) = adapter();
        let stage = prepare_stage(&adapter, prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let cancel = tokio_util::sync::CancellationToken::new();
        cancel.cancel();
        assert!(
            adapter
                .verify(
                    &stage,
                    VerifyRequest {
                        expected_size: 0,
                        expected_blake3: *blake3::hash(b"").as_bytes(),
                        cancel,
                        published: None,
                    }
                )
                .await
                .is_err()
        );
        assert_eq!(protocol.closes.load(std::sync::atomic::Ordering::SeqCst), 0);
        adapter
            .discard(stage)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
    }

    #[tokio::test]
    async fn missing_stage_makes_rename_failure_ambiguous_when_final_is_missing_or_mismatched() {
        for mode in [2, 3] {
            let (adapter, protocol, identity) = adapter();
            let stage = prepare_stage(&adapter, prepare_request(&identity))
                .await
                .unwrap_or_else(|error| panic!("{error}"));
            protocol
                .rename_mode
                .store(mode, std::sync::atomic::Ordering::SeqCst);
            let result = adapter
                .publish(
                    &stage,
                    PublishRequest {
                        expected_size: 0,
                        expected_blake3: Some(*blake3::hash(b"").as_bytes()),
                        cancel: tokio_util::sync::CancellationToken::new(),
                    },
                )
                .await;
            let failure = match result {
                Ok(evidence) => panic!("unexpected publication: {evidence:?}"),
                Err(failure) => failure,
            };
            assert!(failure.final_destination_changed);
            adapter
                .discard(stage)
                .await
                .unwrap_or_else(|error| panic!("cleanup authority failed: {error}"));
        }
    }

    // A slow producer must not prevent an available chunk from starting its write.
    #[tokio::test]
    async fn eight_slot_write_starts_before_input_refill() {
        use std::sync::atomic::Ordering;
        let (adapter, protocol, identity) = adapter();
        let stage = prepare_ephemeral_stage(&adapter, prepare_request(&identity))
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let native = adapter
            .validate(&stage)
            .unwrap_or_else(|error| panic!("{error}"));
        let handle = adapter
            .open_stage_for_write(&stage, &native)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let (sender, receiver) = futures::channel::mpsc::unbounded();
        sender
            .unbounded_send(Ok(Bytes::from_static(b"aa")))
            .unwrap_or_else(|error| panic!("{error}"));
        let mut input: ByteStream = Box::pin(receiver);
        let mut consume = Box::pin(adapter.consume_input(&stage, &mut input, &handle, 8, 2));
        assert!(futures::poll!(&mut consume).is_pending());
        let started_before_refill = protocol.maximum_active_writes.load(Ordering::SeqCst);
        for _ in 0..7 {
            sender
                .unbounded_send(Ok(Bytes::from_static(b"aa")))
                .unwrap_or_else(|error| panic!("{error}"));
        }
        assert!(futures::poll!(&mut consume).is_pending());
        let started_after_refill = protocol.maximum_active_writes.load(Ordering::SeqCst);
        drop(sender);
        let progress = consume.await;
        assert!(progress.failure.is_none());
        assert_eq!(progress.persisted, 16);
        assert_eq!(started_after_refill, 8);
        assert!(
            started_before_refill > 0,
            "existing input waits for more input before any write starts"
        );
    }

    #[tokio::test]
    async fn checkpoint_record_overlaps_next_writes_but_settles_before_finish_or_next_barrier() {
        use std::sync::atomic::{AtomicBool, Ordering};
        for size in [6, 10] {
            for fail_record in [false, true] {
                let (adapter, protocol, identity) = adapter();
                protocol.maximum_write_chunk.store(2, Ordering::SeqCst);
                let mut stage = prepare_ephemeral_stage(&adapter, prepare_request(&identity))
                    .await
                    .unwrap_or_else(|error| panic!("{error}"));
                enable_deferred_checkpointed(&mut stage, 4, size);
                let gate = Arc::new(CheckpointGate::default());
                gate.fail.store(fail_record, Ordering::SeqCst);
                *protocol
                    .checkpoint_gate
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&gate));
                let finished = AtomicBool::new(false);
                let write = async {
                    let result = adapter
                        .write(
                            &stage,
                            Box::pin(stream::iter([Ok(Bytes::from(vec![
                                7;
                                usize::try_from(size)
                                    .unwrap_or_else(
                                        |error| panic!("{error}")
                                    )
                            ]))])),
                        )
                        .await;
                    finished.store(true, Ordering::SeqCst);
                    result
                };
                let observe = async {
                    gate.entered.notified().await;
                    // Data barrier precedes checkpoint publication.
                    assert_eq!(protocol.checkpoints.load(Ordering::SeqCst), 1);
                    let overlapping =
                        tokio::time::timeout(std::time::Duration::from_secs(1), async {
                            loop {
                                if protocol
                                    .write_sizes
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .len()
                                    >= 3
                                {
                                    break;
                                }
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .is_ok();
                    // Give all writes after the prefix time to complete while the record is blocked.
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    assert!(!finished.load(Ordering::SeqCst));
                    assert_eq!(protocol.checkpoints.load(Ordering::SeqCst), 1);
                    gate.release.notify_one();
                    assert!(
                        overlapping,
                        "next window must write while checkpoint record is blocked"
                    );
                };
                let (result, ()) = tokio::join!(write, observe);
                assert_eq!(result.is_err(), fail_record);
                assert_eq!(protocol.active_writes.load(Ordering::SeqCst), 0);
                if !fail_record {
                    assert_eq!(
                        result
                            .unwrap_or_else(|error| panic!("{error}"))
                            .persisted_bytes,
                        size
                    );
                    assert!(stage.recovery_enabled());
                }
            }
        }
    }

    #[tokio::test]
    async fn periodic_checkpoints_own_commit_cadence_before_and_after_the_first_pointer() {
        use std::sync::atomic::Ordering;
        for already_registered in [true, false] {
            for unstable in [true, false] {
                let (adapter, protocol, identity) = adapter();
                protocol.maximum_write_chunk.store(1, Ordering::SeqCst);
                protocol.unstable_pressure.store(unstable, Ordering::SeqCst);
                let mut stage = if already_registered {
                    prepare_stage(&adapter, prepare_request(&identity)).await
                } else {
                    prepare_ephemeral_stage(&adapter, prepare_request(&identity)).await
                }
                .unwrap_or_else(|error| panic!("{error}"));
                enable_deferred_checkpointed(&mut stage, 4, 13);
                let evidence = adapter
                    .write(
                        &stage,
                        Box::pin(stream::iter([Ok(Bytes::from_static(b"abcdefghijklm"))])),
                    )
                    .await
                    .unwrap_or_else(|error| panic!("{error}"));
                assert_eq!(evidence.persisted_bytes, 13);
                assert_eq!(
                    protocol.checkpoints.load(Ordering::SeqCst),
                    4,
                    "only boundaries 4/8/12 and final tail may request a data barrier"
                );
                assert_eq!(protocol.active_writes.load(Ordering::SeqCst), 0);
                assert_eq!(
                    adapter
                        .observe_checkpoint(&stage)
                        .await
                        .unwrap_or_else(|error| panic!("{error}"))
                        .durable_prefix,
                    13
                );
            }
        }
    }

    #[tokio::test]
    async fn nonperiodic_recovery_keeps_unstable_batching_and_stable_writes_skip_it() {
        use std::sync::atomic::Ordering;
        for unstable in [true, false] {
            let (adapter, protocol, identity) = adapter();
            protocol.maximum_write_chunk.store(1, Ordering::SeqCst);
            protocol.unstable_pressure.store(unstable, Ordering::SeqCst);
            let stage = prepare_stage(&adapter, prepare_request(&identity))
                .await
                .unwrap_or_else(|error| panic!("{error}"));
            assert!(stage.deferred_checkpoint.is_none());
            adapter
                .write(
                    &stage,
                    Box::pin(stream::iter([Ok(Bytes::from_static(b"abcdefghi"))])),
                )
                .await
                .unwrap_or_else(|error| panic!("{error}"));
            assert_eq!(
                protocol.checkpoints.load(Ordering::SeqCst),
                if unstable { 3 } else { 1 }
            );
        }
    }
    include!("positioned_tests.rs");
}
