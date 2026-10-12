use data_mover::storage_enum::{
    BackendConfig as ModuleBackendConfig, CopyOptions as ModuleCopyOptions,
    CreateStorageOptions as ModuleCreateStorageOptions, TarPackOptions as ModuleTarPackOptions,
};
use data_mover::storage_options::CopyOptions as OptionsModuleCopyOptions;
use data_mover::{BackendConfig, CopyOptions, CreateStorageOptions, TarPackOptions};

#[test]
fn storage_options_keep_root_and_storage_enum_paths_compatible() {
    let root_copy: CopyOptions = ModuleCopyOptions::default();
    assert!(root_copy.qos.is_none());
    assert!(!root_copy.enable_integrity_check);
    assert!(!root_copy.is_source_reserved);
    assert!(root_copy.bytes_counter.is_none());
    assert!(root_copy.cancel.is_none());
    let _: CopyOptions = OptionsModuleCopyOptions::default();

    let root_tar: TarPackOptions = ModuleTarPackOptions::default();
    assert!(root_tar.qos.is_none());
    assert!(root_tar.bytes_counter.is_none());

    let root_backend: BackendConfig = ModuleBackendConfig::default();
    assert_eq!(root_backend, BackendConfig::Default);

    let root_create = CreateStorageOptions::new(Some(4096), true);
    let module_create = ModuleCreateStorageOptions::new(Some(4096), true);
    assert_eq!(root_create, module_create);
    assert_eq!(root_create.block_size, Some(4096));
    assert!(root_create.ensure_dir);
    assert_eq!(root_create.backend, BackendConfig::Default);
}
