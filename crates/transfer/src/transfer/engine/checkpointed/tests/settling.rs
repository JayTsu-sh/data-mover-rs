use super::*;

fn roles(
    source: mocks::Source,
    destination: mocks::Destination,
) -> (
    Arc<dyn ReadSource>,
    Arc<mocks::Destination>,
    Arc<dyn StagedDestination>,
) {
    let destination = Arc::new(destination);
    (Arc::new(source), destination.clone(), destination)
}

#[tokio::test]
async fn producer_error_waits_for_the_writer_to_drain_before_returning() {
    let (mut source, mut destination, stage) = fixture(24);
    source.fault = mocks::Fault::Error;
    let gate = Arc::new((tokio::sync::Notify::new(), tokio::sync::Notify::new()));
    destination.drain = Some(gate.clone());
    let descriptor = source.descriptor.clone();
    let (source, destination, destination_role) = roles(source, destination);
    let request = request();
    let transfer = transfer(
        &request,
        &source,
        &destination_role,
        &descriptor,
        &stage,
        plan(24),
        None,
    );
    tokio::pin!(transfer);
    tokio::select! {
        biased;
        result = &mut transfer => panic!("returned before draining: {}", result.is_ok()),
        () = gate.0.notified() => {},
        () = tokio::time::sleep(Duration::from_secs(2)) => panic!("writer never received terminal source error"),
    }
    assert!(
        !destination
            .drained
            .load(std::sync::atomic::Ordering::SeqCst)
    );
    gate.1.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), transfer)
            .await
            .unwrap()
            .is_err()
    );
    assert!(
        destination
            .drained
            .load(std::sync::atomic::Ordering::SeqCst)
    );
}

#[tokio::test]
async fn cancellation_settles_an_already_started_pointer_transaction() {
    let (source, mut destination, stage) = fixture(24);
    destination.intent = true;
    let gate = Arc::new((tokio::sync::Notify::new(), tokio::sync::Notify::new()));
    destination.persist_gate = Some(gate.clone());
    let descriptor = source.descriptor.clone();
    let (source, destination, destination_role) = roles(source, destination);
    let request = request();
    let transfer = transfer(
        &request,
        &source,
        &destination_role,
        &descriptor,
        &stage,
        plan(24),
        None,
    );
    tokio::pin!(transfer);
    tokio::select! {
        result = &mut transfer => panic!("returned before persist started: {}", result.is_ok()),
        () = gate.0.notified() => {},
        () = tokio::time::sleep(Duration::from_secs(2)) => panic!("persist did not start"),
    }
    request.cancel.cancel();
    tokio::select! {
        biased;
        result = &mut transfer => panic!("abandoned pointer transaction: {}", result.is_ok()),
        () = tokio::task::yield_now() => {},
    }
    assert!(
        !destination
            .persist_settled
            .load(std::sync::atomic::Ordering::SeqCst)
    );
    gate.1.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), transfer)
            .await
            .unwrap()
            .is_err()
    );
    assert!(
        destination
            .persist_settled
            .load(std::sync::atomic::Ordering::SeqCst)
    );
}

#[tokio::test]
async fn writer_refusal_closes_input_and_stops_a_blocked_source() {
    let (mut source, mut destination, stage) = fixture(24);
    source.fault = mocks::Fault::PendingOpen;
    destination.refuse_write = true;
    let descriptor = source.descriptor.clone();
    let (source, _, destination) = roles(source, destination);
    let request = request();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        transfer(
            &request,
            &source,
            &destination,
            &descriptor,
            &stage,
            plan(24),
            None,
        ),
    )
    .await
    .unwrap();
    assert_eq!(result.err().unwrap().side(), TransferSide::Destination);
}
