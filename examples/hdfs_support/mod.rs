pub fn config() -> data_mover::HdfsConfig {
    let config_dir = std::env::var_os("LAB_HDFS_CONFIG_DIR").map(Into::into);
    let kerberos_credentials = std::env::var_os("LAB_HDFS_KEYTAB").map(|keytab| {
        data_mover::HdfsKerberosCredentials::Keytab {
            keytab: keytab.into(),
        }
    });
    data_mover::HdfsConfig {
        config_dir,
        kerberos_credentials,
        ..Default::default()
    }
}
