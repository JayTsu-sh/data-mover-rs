use super::*;

#[test]
fn restored_hash_remains_eligible_after_read_size_or_threshold_changes() {
    let (source, destination, mut stage) = fixture(24);
    let mut plan = plan(24);
    plan.recovery_enabled = false;
    plan.automatic_interval = None;
    plan.chunk_bytes = 1024;
    let request = request();
    assert!(!eligible(&request, &destination, &stage, plan));
    restore(&stage, &source, &[0..17], vec![0..17]);
    assert!(eligible(&request, &destination, &stage, plan));
    stage.set_direct(true);
    assert!(!eligible(&request, &destination, &stage, plan));
}

#[tokio::test]
async fn legacy_native_hash_fails_closed_without_arming_or_reading() {
    let (source, destination, mut stage) = fixture(24);
    stage.set_write_offset(17);
    stage.set_prepare_fact(PrepareFact::Resumed { bytes: 17 });
    let descriptor = source.descriptor.clone();
    let source = Arc::new(source);
    let destination = Arc::new(destination);
    let request = request();
    assert!(!eligible(&request, destination.as_ref(), &stage, plan(24)));
    let source_role: Arc<dyn ReadSource> = source.clone();
    let destination_role: Arc<dyn StagedDestination> = destination.clone();
    let failure = hash_only(
        &request,
        &source_role,
        &destination_role,
        &descriptor,
        &stage,
        plan(24),
        None,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(failure.role.as_deref(), Some(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Unsupported)
    );
    assert!(!stage.checkpoint_enabled());
    assert!(stage.checkpoint_snapshot().is_none());
    assert!(source.reads.lock().unwrap().is_empty());
    assert!(destination.snapshots.lock().unwrap().is_empty());
}

#[tokio::test]
async fn recovered_publication_uses_saved_full_hash_without_io() -> TestResult {
    let (source, destination, mut stage) = fixture(24);
    restore(&stage, &source, &[0..24], vec![0..24]);
    stage.set_recovered_publication(Some(PublicationEvidence::new(
        stage.final_destination().path().clone(),
        crate::storage::PublicationDisposition::Published,
        None,
    )));
    let descriptor = source.descriptor.clone();
    let expected = *blake3::hash(&source.data).as_bytes();
    let source = Arc::new(source);
    let destination = Arc::new(destination);
    let source_role: Arc<dyn ReadSource> = source.clone();
    let destination_role: Arc<dyn StagedDestination> = destination.clone();
    let request = request();
    let evidence = transfer(
        &request,
        &source_role,
        &destination_role,
        &descriptor,
        &stage,
        plan(24),
        None,
    )
    .await?;
    assert_eq!(evidence.source_blake3, Some(expected));
    assert_eq!(
        hash_only(
            &request,
            &source_role,
            &destination_role,
            &descriptor,
            &stage,
            plan(24),
            None
        )
        .await?,
        expected
    );
    assert!(source.reads.lock().unwrap().is_empty());
    assert!(destination.snapshots.lock().unwrap().is_empty());
    assert!(destination.chunks.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn cancellation_interrupts_pending_positioned_and_ordered_reads() {
    for positioned in [false, true] {
        let (mut source, destination, stage) = fixture(24);
        source.positioned = positioned;
        source.fault = mocks::Fault::PendingOpen;
        let cancel = CancellationToken::new();
        let context = context(&source, &destination, &stage, &cancel);
        let recovered = state::initialize(&context).unwrap();
        let produce = super::super::producer::produce(&context, recovered, None);
        let cancel_read = async {
            tokio::task::yield_now().await;
            cancel.cancel();
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(produce, cancel_read)
        })
        .await
        .unwrap();
        assert!(
            matches!(result.unwrap_err().role.as_deref(), Some(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Cancelled)
        );
    }
}

#[tokio::test]
async fn source_qos_and_pinned_version_reach_every_missing_range_request() -> TestResult {
    let (mut source, destination, stage) = fixture(24);
    source.descriptor.version = crate::model::SourceVersion::Id("selected-version".into());
    let group =
        crate::storage::SourceQosGroup::new(crate::storage::SourceQosPolicy::new(None, 4, None)?);
    let cancel = CancellationToken::new();
    let context = Context::new(
        &cancel,
        &source,
        &destination,
        &source.descriptor,
        &stage,
        24,
        Some(group.transfer_budget()),
    )?;
    let recovered = state::initialize(&context)?;
    super::super::producer::produce(&context, recovered, None).await?;
    for (_, request) in source.reads.lock().unwrap().iter() {
        assert!(request.source_qos.is_some());
        assert_eq!(request.version, source.descriptor.version);
        assert_eq!(request.maximum_chunk_bytes, 4);
        assert_eq!(request.read_inflight, 2);
    }
    Ok(())
}
