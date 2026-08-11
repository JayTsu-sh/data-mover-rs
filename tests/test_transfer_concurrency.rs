use data_mover::{LocalStorage, StorageEnum, TransferConcurrency};
mod common;
use common::AssertTestValue;

#[test]
fn transfer_concurrency_keeps_read_and_write_independent() {
    let concurrency = TransferConcurrency::new(2, 8).assert_value("valid concurrency");
    assert_eq!(concurrency.read(), 2);
    assert_eq!(concurrency.write(), 8);
}

#[test]
fn explicit_storage_configuration_is_observable_through_the_shared_interface() {
    let concurrency = TransferConcurrency::new(3, 11).assert_value("valid concurrency");
    let storage = StorageEnum::Local(
        LocalStorage::new(std::env::temp_dir(), None).with_transfer_concurrency(concurrency),
    );
    assert_eq!(storage.transfer_concurrency(), concurrency);
}

#[test]
fn transfer_concurrency_rejects_zero_and_values_above_the_safe_limit() {
    assert!(TransferConcurrency::new(16, 16).is_ok());
    assert!(TransferConcurrency::new(0, 1).is_err());
    assert!(TransferConcurrency::new(1, 0).is_err());
    assert!(TransferConcurrency::new(17, 1).is_err());
    assert!(TransferConcurrency::new(1, 17).is_err());
}
