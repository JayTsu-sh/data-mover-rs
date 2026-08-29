#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::model::{BackendKind, EntryKind, IdentityStrength, SourceIdentity};
    use crate::storage::{FinalDestination, SourceDescriptor};
    use futures::stream;

    #[derive(Default)]
    struct FakeProtocol {
        files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        closes: Arc<std::sync::atomic::AtomicU64>,
        rename_mode: std::sync::atomic::AtomicU8,
    }

    struct FakeFile {
        path: String,
        files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        closes: Arc<std::sync::atomic::AtomicU64>,
    }

    #[async_trait]
    impl NfsStageFile for FakeFile {
        async fn read_at(
            &mut self,
            offset: u64,
            count: usize,
        ) -> Result<Bytes, NfsProtocolFailure> {
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
            Ok(Bytes::copy_from_slice(
                value
                    .get(start..end)
                    .ok_or_else(NfsProtocolFailure::protocol)?,
            ))
        }

        async fn write_at(&mut self, offset: u64, data: Bytes) -> Result<u64, NfsProtocolFailure> {
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
            value.resize(end, 0);
            value[start..end].copy_from_slice(&data);
            Ok(data.len() as u64)
        }

        async fn close(self: Box<Self>) -> Result<(), NfsProtocolFailure> {
            self.closes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl NfsStagedProtocol for FakeProtocol {
        async fn create_empty(&self, path: &StoragePath) -> Result<(), NfsProtocolFailure> {
            let mut files = self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if files.insert(path.as_str().to_owned(), Vec::new()).is_some() {
                return Err(NfsProtocolFailure {
                    class: FailureClass::Conflict,
                    transience: Transience::Permanent,
                });
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
                return Err(NfsProtocolFailure::protocol());
            }
            Ok(Box::new(FakeFile {
                path: path.as_str().to_owned(),
                files: Arc::clone(&self.files),
                closes: Arc::clone(&self.closes),
            }))
        }

        async fn size(&self, path: &StoragePath) -> Result<u64, NfsProtocolFailure> {
            self.files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(path.as_str())
                .map(|value| value.len() as u64)
                .ok_or(NfsProtocolFailure {
                    class: FailureClass::NotFound,
                    transience: Transience::Permanent,
                })
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
            let value = files
                .remove(from.as_str())
                .ok_or(NfsProtocolFailure {
                    class: FailureClass::NotFound,
                    transience: Transience::Permanent,
                })?;
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
                .ok_or(NfsProtocolFailure {
                    class: FailureClass::NotFound,
                    transience: Transience::Permanent,
                })
        }
    }

    fn adapter() -> (
        NfsStagedDestinationAdapter,
        Arc<FakeProtocol>,
        BackendIdentity,
    ) {
        let protocol = Arc::new(FakeProtocol::default());
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
            },
            recovery_binding: [7; 32],
        }
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
                &StoragePath::new(".data-mover-staging/forged")
                    .unwrap_or_else(|error| panic!("{error}"))
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn staged_lifecycle_writes_verifies_publishes_and_closes_handles() {
        let (adapter, protocol, identity) = adapter();
        let stage = adapter
            .prepare(prepare_request(&identity))
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
                        cancel: tokio_util::sync::CancellationToken::new()
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
                    policy: ExistingDestinationPolicy::Overwrite,
                    expected_size: 6,
                    expected_blake3: hash,
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

    #[tokio::test]
    async fn recovery_reobserves_durable_prefix_and_transfers_authority() {
        let (adapter, protocol, identity) = adapter();
        let request = prepare_request(&identity);
        let stage = adapter
            .prepare(request.clone())
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let input: ByteStream = Box::pin(stream::iter([Ok(Bytes::from_static(b"abc"))]));
        adapter
            .write(&stage, input)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let recovery_identity = adapter
            .recovery_identity(&stage)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(adapter.observe_checkpoint(&stage).await.is_err());

        let recovered_adapter = NfsStagedDestinationAdapter::new(protocol.clone(), identity);
        let recovered = recovered_adapter
            .recover(RecoverRequest {
                identity: recovery_identity,
                final_destination: request.final_destination,
                source: request.source,
                recovery_binding: request.recovery_binding,
                claim_token: [1; 32],
            })
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(
            recovered_adapter
                .observe_checkpoint(&recovered)
                .await
                .unwrap_or_else(|error| panic!("{error}"))
                .durable_prefix,
            3
        );
        recovered_adapter
            .discard(recovered)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
    }

    #[tokio::test]
    async fn recovery_identity_is_atomically_consumed_across_adapters() {
        let (adapter, protocol, identity) = adapter();
        let request = prepare_request(&identity);
        let stage = adapter
            .prepare(request.clone())
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let recovery_identity = adapter
            .recovery_identity(&stage)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let first = NfsStagedDestinationAdapter::new(protocol.clone(), identity.clone());
        let second = NfsStagedDestinationAdapter::new(protocol, identity);
        let first_request = RecoverRequest {
            identity: recovery_identity.clone(),
            final_destination: request.final_destination.clone(),
            source: request.source.clone(),
            recovery_binding: request.recovery_binding,
            claim_token: [1; 32],
        };
        let second_request = RecoverRequest {
            identity: recovery_identity,
            final_destination: request.final_destination,
            source: request.source,
            recovery_binding: request.recovery_binding,
            claim_token: [2; 32],
        };
        let (first_result, second_result) = tokio::join!(
            first.recover(first_request),
            second.recover(second_request)
        );
        let (winner, loser) = match (first_result, second_result) {
            (Ok(stage), Err(error)) => ((&first, stage), error),
            (Err(error), Ok(stage)) => ((&second, stage), error),
            (left, right) => panic!("exactly one recovery must win: {left:?}, {right:?}"),
        };
        assert!(matches!(
            loser,
            StorageRoleFailure::Entry(error)
                if matches!(error.class(), FailureClass::Conflict | FailureClass::NotFound)
        ));
        winner
            .0
            .discard(winner.1)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
    }

    #[tokio::test]
    async fn recovery_reconciles_an_ambiguous_committed_claim() {
        let (adapter, protocol, identity) = adapter();
        let request = prepare_request(&identity);
        let stage = adapter
            .prepare(request.clone())
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let recovery_identity = adapter
            .recovery_identity(&stage)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        protocol
            .rename_mode
            .store(4, std::sync::atomic::Ordering::SeqCst);
        let recovered = adapter
            .recover(RecoverRequest {
                identity: recovery_identity,
                final_destination: request.final_destination,
                source: request.source,
                recovery_binding: request.recovery_binding,
                claim_token: [3; 32],
            })
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        protocol
            .rename_mode
            .store(0, std::sync::atomic::Ordering::SeqCst);
        adapter
            .discard(recovered)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
    }

    #[tokio::test]
    async fn persisted_claim_token_reenters_after_process_loses_recover_result() {
        let (adapter, protocol, identity) = adapter();
        let request = prepare_request(&identity);
        let stage = adapter
            .prepare(request.clone())
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let recovery_identity = adapter
            .recovery_identity(&stage)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let recover = || RecoverRequest {
            identity: recovery_identity.clone(),
            final_destination: request.final_destination.clone(),
            source: request.source.clone(),
            recovery_binding: request.recovery_binding,
            claim_token: [9; 32],
        };
        let first = NfsStagedDestinationAdapter::new(protocol.clone(), identity.clone());
        let lost = first
            .recover(recover())
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        drop(lost);
        drop(first);

        let restarted = NfsStagedDestinationAdapter::new(protocol, identity);
        let recovered = restarted
            .recover(recover())
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(recovered.write_offset, 0);
        restarted
            .discard(recovered)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
    }

    #[tokio::test]
    async fn recovery_rejects_tampering_binding_changes_and_missing_remote_stage() {
        let (adapter, protocol, identity) = adapter();
        let request = prepare_request(&identity);
        let stage = adapter
            .prepare(request.clone())
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let recovery_identity = adapter
            .recovery_identity(&stage)
            .await
            .unwrap_or_else(|error| panic!("{error}"));

        let mut tampered = recovery_identity.as_bytes().to_vec();
        let last = tampered.len() - 1;
        tampered[last] ^= 1;
        let corrupt =
            RecoveryIdentity::from_bytes(tampered).unwrap_or_else(|error| panic!("{error}"));
        assert!(matches!(
            adapter
                .recover(RecoverRequest {
                    identity: corrupt,
                    final_destination: request.final_destination.clone(),
                    source: request.source.clone(),
                    recovery_binding: request.recovery_binding,
                    claim_token: [4; 32],
                })
                .await,
            Err(StorageRoleFailure::Entry(error))
                if error.class() == FailureClass::Conflict
        ));

        assert!(matches!(
            adapter
                .recover(RecoverRequest {
                    identity: recovery_identity.clone(),
                    final_destination: request.final_destination.clone(),
                    source: request.source.clone(),
                    recovery_binding: [8; 32],
                    claim_token: [5; 32],
                })
                .await,
            Err(StorageRoleFailure::Entry(error))
                if error.class() == FailureClass::Conflict
        ));

        protocol
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        assert!(matches!(
            adapter
                .recover(RecoverRequest {
                    identity: recovery_identity,
                    final_destination: request.final_destination,
                    source: request.source,
                    recovery_binding: request.recovery_binding,
                    claim_token: [6; 32],
                })
                .await,
            Err(StorageRoleFailure::Entry(error))
                if error.class() == FailureClass::NotFound
        ));
    }

    #[tokio::test]
    async fn precancelled_verification_does_not_open_remote_state() {
        let (adapter, protocol, identity) = adapter();
        let stage = adapter
            .prepare(prepare_request(&identity))
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
                        cancel
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
            let stage = adapter
                .prepare(prepare_request(&identity))
                .await
                .unwrap_or_else(|error| panic!("{error}"));
            protocol
                .rename_mode
                .store(mode, std::sync::atomic::Ordering::SeqCst);
            let result = adapter
                .publish(
                    &stage,
                    PublishRequest {
                        policy: ExistingDestinationPolicy::Overwrite,
                        expected_size: 0,
                        expected_blake3: *blake3::hash(b"").as_bytes(),
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
}
