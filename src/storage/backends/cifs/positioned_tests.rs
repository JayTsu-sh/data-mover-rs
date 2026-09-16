use super::*;
use crate::storage::{PositionedByteStream, PositionedChunk};

fn positioned(parts: &[(u64, &'static [u8])]) -> PositionedByteStream {
    let parts: Vec<_> = parts
        .iter()
        .map(|(offset, bytes)| {
            Ok(PositionedChunk {
                offset: *offset,
                data: Bytes::from_static(bytes),
            })
        })
        .collect();
    Box::pin(stream::iter(parts))
}

#[tokio::test]
async fn later_read_writes_before_missing_prefix_arrives() -> Result<(), Box<dyn std::error::Error>>
{
    let protocol = Arc::new(MemoryProtocol::default());
    let destination = CifsStagedDestination::new(protocol.clone(), identity()?);
    let stage = destination
        .prepare_ephemeral(prepare_request(&identity()?)?)
        .await?;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(Ok(PositionedChunk {
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
                if protocol.writes.lock().unwrap().contains(&(4, 2)) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        tx.send(Ok(PositionedChunk {
            offset: 0,
            data: Bytes::from_static(b"abcd"),
        }))
        .await?;
        drop(tx);
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let (written, sent) = tokio::join!(destination.write_positioned(&stage, input), feeder);
    sent?;
    assert_eq!(written?.persisted_bytes, 6);
    let path = super::super::staged::token_path(&stage.token, stage.final_destination.path())?;
    assert_eq!(
        protocol.files.lock().unwrap().get(path.as_str()),
        Some(&b"abcdef".to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn gap_or_overlap_never_creates_a_checkpoint() -> Result<(), Box<dyn std::error::Error>> {
    for parts in [
        vec![(4, &b"ef"[..])],
        vec![(4, &b"ef"[..]), (4, &b"ef"[..])],
    ] {
        let protocol = Arc::new(MemoryProtocol::default());
        let destination = CifsStagedDestination::new(protocol.clone(), identity()?);
        let mut stage = destination
            .prepare_ephemeral(prepare_request(&identity()?)?)
            .await?;
        let registration = Arc::new(Registration(AtomicUsize::new(0)));
        stage.deferred_checkpoint = Some(crate::storage::roles::DeferredCheckpoint {
            interval_bytes: 4,
            source_size: 6,
            registration: registration.clone(),
        });
        assert!(
            destination
                .write_positioned(&stage, positioned(&parts))
                .await
                .is_err()
        );
        assert_eq!(registration.0.load(Ordering::SeqCst), 0);
        assert_eq!(protocol.flushes.load(Ordering::SeqCst), 0);
        assert_eq!(protocol.closes.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn cancellation_drains_positioned_writes_before_close()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    protocol.activity.delayed.store(true, Ordering::SeqCst);
    let destination = CifsStagedDestination::new(protocol.clone(), identity()?);
    let stage = destination
        .prepare_ephemeral(prepare_request(&identity()?)?)
        .await?;
    let cancelled = StorageRoleFailure::Entry(EntryOperationFailure::new(
        StoragePath::new("source.bin")?,
        Operation::Read,
        FailureClass::Cancelled,
        Transience::Transient,
        "cancelled",
    )?);
    let input = Box::pin(stream::iter(vec![
        Ok(PositionedChunk {
            offset: 4,
            data: Bytes::from_static(b"ef"),
        }),
        Err(cancelled.clone()),
    ]));
    assert_eq!(
        destination.write_positioned(&stage, input).await.err(),
        Some(cancelled)
    );
    assert_eq!(protocol.activity.active.load(Ordering::SeqCst), 0);
    assert_eq!(protocol.closes.load(Ordering::SeqCst), 1);
    assert_eq!(protocol.flushes.load(Ordering::SeqCst), 0);
    assert_eq!(*protocol.writes.lock().unwrap(), vec![(4, 2)]);
    Ok(())
}

#[tokio::test]
async fn checkpoint_records_contiguous_prefix_despite_completed_later_range()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    protocol.activity.delayed.store(true, Ordering::SeqCst);
    let destination = CifsStagedDestination::new(protocol.clone(), identity()?);
    let mut request = prepare_request(&identity()?)?;
    request.source.size = Some(12);
    let mut stage = destination.prepare_ephemeral(request).await?;
    let registration = Arc::new(Registration(AtomicUsize::new(0)));
    stage.deferred_checkpoint = Some(crate::storage::roles::DeferredCheckpoint {
        interval_bytes: 4,
        source_size: 12,
        registration: registration.clone(),
    });
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    for (offset, data) in [(8, &b"ijkl"[..]), (0, &b"abcd"[..])] {
        tx.send(Ok(PositionedChunk {
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
            while registration.0.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let cancelled = StorageRoleFailure::Entry(EntryOperationFailure::new(
            StoragePath::new("source.bin")?,
            Operation::Read,
            FailureClass::Cancelled,
            Transience::Transient,
            "cancelled",
        )?);
        tx.send(Err(cancelled)).await?;
        drop(tx);
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let (written, sent) = tokio::join!(destination.write_positioned(&stage, input), feeder);
    sent?;
    assert!(written.is_err());
    assert_eq!(
        destination.observe_checkpoint(&stage).await?.durable_prefix,
        4
    );
    let path = super::super::staged::token_path(&stage.token, stage.final_destination.path())?;
    assert_eq!(
        protocol
            .files
            .lock()
            .unwrap()
            .get(path.as_str())
            .map(Vec::len),
        Some(12)
    );
    Ok(())
}

#[tokio::test]
async fn crossing_checkpoint_threshold_registers_even_when_drain_reaches_eof()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    let destination = CifsStagedDestination::new(protocol, identity()?);
    let mut request = prepare_request(&identity()?)?;
    request.source.size = Some(8);
    let mut stage = destination.prepare_ephemeral(request).await?;
    let registration = Arc::new(Registration(AtomicUsize::new(0)));
    stage.deferred_checkpoint = Some(crate::storage::roles::DeferredCheckpoint {
        interval_bytes: 4,
        source_size: 8,
        registration: registration.clone(),
    });
    destination
        .write_positioned(&stage, positioned(&[(4, b"efgh"), (0, b"abcd")]))
        .await?;
    assert_eq!(registration.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        destination.observe_checkpoint(&stage).await?.durable_prefix,
        8
    );
    Ok(())
}
