//! Disabled connectors retain configuration types and fail before I/O.
use data_mover::model::BackendKind;
use data_mover::storage::{
    BackendConfig, CifsBackendConfig, HdfsBackendConfig, HdfsConfig, LocalBackendConfig,
    NfsBackendConfig, S3BackendConfig, connect_backend,
};
use std::num::NonZeroUsize;

#[tokio::test]
async fn omitted_connectors_report_a_typed_build_error() -> Result<(), Box<dyn std::error::Error>> {
    let cases = [
        (
            cfg!(feature = "backend-local"),
            BackendKind::Local,
            BackendConfig::Local(LocalBackendConfig {
                root: "this-path-must-not-be-touched".into(),
                rename_context: None,
                read_concurrency: NonZeroUsize::MIN,
                write_concurrency: NonZeroUsize::MIN,
                read_chunk_bytes: None,
                write_chunk_bytes: None,
            }),
        ),
        (
            cfg!(feature = "backend-nfs"),
            BackendKind::Nfs,
            BackendConfig::Nfs(NfsBackendConfig {
                url: "invalid-url-secret".into(),
                block_size: None,
                ensure_dir: false,
                transfer_concurrency: None,
            }),
        ),
        (
            cfg!(feature = "backend-cifs"),
            BackendKind::Cifs,
            BackendConfig::Cifs(CifsBackendConfig::from_url(
                "smb://user:secret@invalid/share",
            )?),
        ),
        (
            cfg!(feature = "backend-s3"),
            BackendKind::S3,
            BackendConfig::S3(S3BackendConfig {
                url: "invalid-url-secret".into(),
                block_size: None,
                single_put_threshold: None,
            }),
        ),
        (
            cfg!(feature = "backend-hdfs"),
            BackendKind::Hdfs,
            BackendConfig::Hdfs(HdfsBackendConfig {
                location: "invalid-url-secret".into(),
                client: HdfsConfig::default(),
                block_size: None,
                ensure_dir: false,
            }),
        ),
    ];
    for (enabled, kind, config) in cases {
        if enabled {
            continue;
        }
        let Err(error) = connect_backend(config).await else {
            return Err("disabled connector succeeded".into());
        };
        assert_eq!(error.kind(), kind);
        assert!(error.is_not_compiled());
        assert!(error.session_failure().is_none());
        assert!(!error.to_string().contains("secret"));
    }
    Ok(())
}
