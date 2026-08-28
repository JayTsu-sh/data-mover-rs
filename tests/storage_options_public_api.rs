use data_mover::storage_enum::{
    BackendConfig as ModuleBackendConfig, CopyOptions as ModuleCopyOptions,
    CreateStorageOptions as ModuleCreateStorageOptions, TarPackOptions as ModuleTarPackOptions,
    WalkOptions as ModuleWalkOptions,
};
use data_mover::storage_options::CopyOptions as OptionsModuleCopyOptions;
use data_mover::{BackendConfig, CopyOptions, CreateStorageOptions, TarPackOptions, WalkOptions};

#[test]
fn storage_options_keep_root_and_storage_enum_paths_compatible() {
    let root_walk = WalkOptions::default();
    let module_walk = ModuleWalkOptions::default();
    assert_eq!(root_walk.depth, None);
    assert!(root_walk.match_expressions.is_none());
    assert!(root_walk.exclude_expressions.is_none());
    assert_eq!(root_walk.concurrency, 1);
    assert!(!root_walk.include_tags);
    assert!(!root_walk.packaged);
    assert_eq!(root_walk.package_depth, 0);
    assert_eq!(module_walk.concurrency, 1);

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
