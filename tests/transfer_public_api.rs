use data_mover::transfer::{
    PayloadShapingPolicy, RecoveryIdentity, RecoveryPolicy, SourceQosGroup, SourceQosPolicy,
    SourceQosStats, SourceQosValueError, TransferFailure,
};

#[allow(dead_code)]
fn public_failure_state(error: &TransferFailure) -> (bool, bool, bool) {
    (
        error.final_destination_changed(),
        error.has_recoverable_stage(),
        error.has_pending_cleanup(),
    )
}

#[allow(dead_code)]
fn public_qos_state(error: &TransferFailure) -> SourceQosStats {
    error.source_qos()
}

#[allow(dead_code)]
async fn public_recoverable_cleanup(error: TransferFailure) {
    let _result = error.discard_stage().await;
}

#[allow(dead_code)]
async fn public_committed_cleanup(error: TransferFailure) {
    let _result = error.cleanup_published_stage().await;
}

#[allow(dead_code)]
async fn public_recovery_export(error: TransferFailure) -> Option<RecoveryIdentity> {
    error.into_recovery_identity().await.ok()
}

#[test]
fn transfer_failure_cleanup_contract_is_public() {
    let state: fn(&TransferFailure) -> (bool, bool, bool) = public_failure_state;
    let _ = state;
    assert_eq!(RecoveryPolicy::default(), RecoveryPolicy::ResumeOrRestart);
    assert_eq!(
        PayloadShapingPolicy::default(),
        PayloadShapingPolicy::AllowUnshapedNative
    );
    let policy: Result<SourceQosPolicy, SourceQosValueError> =
        SourceQosPolicy::new(None, 64 * 1024, None);
    assert!(policy.map(SourceQosGroup::new).is_ok());
}
