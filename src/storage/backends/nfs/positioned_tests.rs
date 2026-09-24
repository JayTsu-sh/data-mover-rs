fn positioned_input(parts: &[(u64, &'static [u8])]) -> crate::storage::PositionedByteStream {
    let items: Vec<_> = parts
        .iter()
        .map(|(offset, data)| {
            Ok(crate::storage::PositionedChunk {
                offset: *offset,
                data: Bytes::from_static(data),
            })
        })
        .collect();
    Box::pin(stream::iter(items))
}

#[tokio::test]
async fn positioned_nfs_writes_completed_later_read_before_prefix_arrives()
-> Result<(), Box<dyn std::error::Error>> {
    let (adapter, protocol, identity) = adapter();
    let stage = prepare_ephemeral_stage(&adapter, prepare_request(&identity)).await?;
    let path = adapter.validate(&stage)?;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(Ok(crate::storage::PositionedChunk {
        offset: 4,
        data: Bytes::from_static(b"ef"),
    }))
    .await?;
    let input = Box::pin(stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|item| (item, rx))
    }));
    let feeder = async {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if protocol
                    .files
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(path.as_str())
                    .is_some_and(|bytes| bytes.len() == 6)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        tx.send(Ok(crate::storage::PositionedChunk {
            offset: 0,
            data: Bytes::from_static(b"abcd"),
        }))
        .await?;
        drop(tx);
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let (written, sent) = tokio::join!(adapter.write_positioned(&stage, input), feeder);
    sent?;
    assert_eq!(written?.persisted_bytes, 6);
    assert_eq!(
        protocol
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(path.as_str()),
        Some(&b"abcdef".to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn positioned_nfs_checkpoint_tracks_prefix_with_sparse_accepted_suffix()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering;
    for unstable in [false, true] {
        let (adapter, protocol, identity) = adapter();
        protocol.unstable_pressure.store(unstable, Ordering::SeqCst);
        let mut request = prepare_request(&identity);
        request.source.size = Some(12);
        let mut stage = prepare_ephemeral_stage(&adapter, request).await?;
        enable_deferred_checkpointed(&mut stage, 4, 12);
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        for (offset, data) in [(8, &b"ijkl"[..]), (0, &b"abcd"[..])] {
            tx.send(Ok(crate::storage::PositionedChunk {
                offset,
                data: Bytes::from_static(data),
            }))
            .await?;
        }
        let input = Box::pin(stream::unfold(rx, |mut rx| async {
            rx.recv().await.map(|item| (item, rx))
        }));
        let feeder = async {
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                // Recovery turns on once the first pointer is written.
                while !stage.recovery_enabled() {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            assert_eq!(adapter.observe_checkpoint(&stage).await?.durable_prefix, 4);
            assert_eq!(protocol.active_writes.load(Ordering::SeqCst), 0);
            tx.send(Err(entry_failure(
                &StoragePath::new("source.bin")?,
                Operation::Read,
                FailureClass::Cancelled,
                Transience::Transient,
            )))
            .await?;
            drop(tx);
            Ok::<_, Box<dyn std::error::Error>>(())
        };
        let (written, sent) = tokio::join!(adapter.write_positioned(&stage, input), feeder);
        sent?;
        assert!(
            matches!(written, Err(StorageRoleFailure::Entry(ref error)) if error.class() == FailureClass::Cancelled)
        );
        assert_eq!(adapter.observe_checkpoint(&stage).await?.durable_prefix, 4);
        assert_eq!(protocol.active_writes.load(Ordering::SeqCst), 0);
        assert_eq!(protocol.checkpoint_pending_writes.load(Ordering::SeqCst), 2);
        assert_eq!(protocol.deferred_writes.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
async fn positioned_nfs_rejects_holes_and_overlap_and_drains_writes()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering;
    for parts in [
        vec![(4, &b"ef"[..])],
        vec![(4, &b"ef"[..]), (4, &b"ef"[..])],
    ] {
        let (adapter, protocol, identity) = adapter();
        let stage = prepare_ephemeral_stage(&adapter, prepare_request(&identity)).await?;
        assert!(
            adapter
                .write_positioned(&stage, positioned_input(&parts))
                .await
                .is_err()
        );
        assert_eq!(protocol.active_writes.load(Ordering::SeqCst), 0);
        assert_eq!(protocol.closes.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn positioned_nfs_threshold_crossing_at_eof_still_writes_its_pointer()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering;
    for unstable in [false, true] {
        let (adapter, protocol, identity) = adapter();
        protocol.unstable_pressure.store(unstable, Ordering::SeqCst);
        let mut stage = prepare_ephemeral_stage(&adapter, prepare_request(&identity)).await?;
        enable_deferred_checkpointed(&mut stage, 4, 6);
        adapter
            .write_positioned(&stage, positioned_input(&[(4, b"ef"), (0, b"abcd")]))
            .await?;
        assert!(stage.recovery_enabled());
        assert!(protocol.pointer_writes.load(Ordering::SeqCst) > 0);
        assert_eq!(adapter.observe_checkpoint(&stage).await?.durable_prefix, 6);
        assert_eq!(protocol.deferred_writes.load(Ordering::SeqCst), 0);
        assert_eq!(protocol.checkpoint_pending_writes.load(Ordering::SeqCst), 2);
    }
    Ok(())
}

#[tokio::test]
async fn ordered_nfs_writer_preserves_empty_chunk_and_empty_file_support()
-> Result<(), Box<dyn std::error::Error>> {
    let (adapter, _, identity) = adapter();
    let stage = prepare_ephemeral_stage(&adapter, prepare_request(&identity)).await?;
    assert_eq!(
        adapter
            .write_single(&stage, Bytes::new())
            .await?
            .persisted_bytes,
        0
    );
    let input = Box::pin(stream::iter([
        Ok(Bytes::new()),
        Ok(Bytes::from_static(b"ab")),
        Ok(Bytes::new()),
        Ok(Bytes::from_static(b"cdef")),
        Ok(Bytes::new()),
    ]));
    assert_eq!(adapter.write(&stage, input).await?.persisted_bytes, 6);
    Ok(())
}

async fn wait_positioned_nfs_writes(protocol: &FakeProtocol, count: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let started = protocol
                .write_sizes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len();
            if started >= count
                && protocol.active_writes.load(std::sync::atomic::Ordering::SeqCst) == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|error| panic!("writes did not finish: {error}"));
}

async fn wait_positioned_nfs_checkpoint(
    adapter: &NfsStagedDestinationAdapter,
    stage: &PreparedStage,
    prefix: u64,
) {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if matches!(adapter.observe_checkpoint(stage).await, Ok(value) if value.durable_prefix == prefix) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|error| panic!("checkpoint did not reach {prefix}: {error}"));
}

#[tokio::test]
async fn positioned_nfs_checkpoint_coalesces_thresholds_and_rebases_next_interval()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering;
    for unstable in [false, true] {
        let (adapter, protocol, identity) = adapter();
        protocol.unstable_pressure.store(unstable, Ordering::SeqCst);
        protocol.maximum_write_chunk.store(32, Ordering::SeqCst);
        let mut request = prepare_request(&identity);
        request.source.size = Some(24);
        let mut stage = prepare_ephemeral_stage(&adapter, request).await?;
        enable_deferred_checkpointed(&mut stage, 4, 24);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let input = Box::pin(stream::unfold(rx, |mut rx| async {
            rx.recv().await.map(|item| (item, rx))
        }));
        let feeder = async {
            for (index, (offset, data, prefix)) in [
                (4, &b"efghijkl"[..], 0),
                (0, &b"abcd"[..], 12),
                (12, &b"mn"[..], 12),
                (14, &b"opqr"[..], 18),
            ].into_iter().enumerate() {
                tx.send(Ok(crate::storage::PositionedChunk {
                    offset, data: Bytes::from_static(data),
                })).await?;
                wait_positioned_nfs_writes(&protocol, index + 1).await;
                if prefix == 0 {
                    assert!(!stage.recovery_enabled());
                    assert_eq!(protocol.pointer_writes.load(Ordering::SeqCst), 0);
                    assert_eq!(protocol.checkpoints.load(Ordering::SeqCst), 0);
                } else {
                    wait_positioned_nfs_checkpoint(&adapter, &stage, prefix).await;
                    assert_eq!(protocol.checkpoints.load(Ordering::SeqCst), if prefix == 12 { 1 } else { 2 });
                }
            }
            tx.send(Ok(crate::storage::PositionedChunk {
                offset: 18, data: Bytes::from_static(b"stuvwx"),
            })).await?;
            drop(tx);
            Ok::<_, Box<dyn std::error::Error>>(())
        };
        let (written, sent) = tokio::join!(adapter.write_positioned(&stage, input), feeder);
        sent?;
        assert_eq!(written?.persisted_bytes, 24);
        assert_eq!(adapter.observe_checkpoint(&stage).await?.durable_prefix, 24);
    }
    Ok(())
}
