use data_mover::transfer::TransferFailure;

#[allow(dead_code)]
fn public_failure_state(error: &TransferFailure) -> (bool, bool, bool) {
    (
        error.final_destination_changed(),
        error.has_recoverable_stage(),
        error.has_pending_cleanup(),
    )
}

#[allow(dead_code)]
async fn public_recoverable_cleanup(error: TransferFailure) {
    let _result = error.discard_stage().await;
}

#[allow(dead_code)]
async fn public_committed_cleanup(error: TransferFailure) {
    let _result = error.cleanup_published_stage().await;
}

#[test]
fn transfer_failure_cleanup_contract_is_public() {
    let state: fn(&TransferFailure) -> (bool, bool, bool) = public_failure_state;
    let _ = state;
}
