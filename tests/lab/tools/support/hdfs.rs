pub fn config() -> data_mover::storage::HdfsConfig {
    let config_dir = std::env::var_os("LAB_HDFS_CONFIG_DIR").map(Into::into);
    let kerberos_credentials = std::env::var_os("LAB_HDFS_KEYTAB").map(|keytab| {
        data_mover::storage::HdfsKerberosCredentials {
            keytab: Some(keytab.into()),
            ..Default::default()
        }
    });
    data_mover::storage::HdfsConfig {
        config_dir,
        kerberos_credentials,
        ..Default::default()
    }
}

/// Joins a probe's relative path beneath its explicitly configured native HDFS root.
pub fn resolve(
    location: &data_mover::storage::HdfsLocation,
    relative: &std::path::Path,
) -> Result<String, Box<dyn std::error::Error>> {
    let value = relative.to_str().ok_or("HDFS path must be UTF-8")?;
    if value.is_empty()
        || value.starts_with('/')
        || value.split('/').any(|part| matches!(part, "." | ".."))
    {
        return Err("HDFS probe path must remain below its configured root".into());
    }
    Ok(format!(
        "{}/{}",
        location.root().trim_end_matches('/'),
        value
    ))
}
