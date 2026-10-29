use crate::error::StorageError;
use percent_encoding::percent_decode_str;
use serde::Deserialize;
use std::{collections::HashMap, fmt, fs, path::PathBuf};
use url::{Host, Url};
/// Explicit HDFS location for a direct `NameNode` or logical `NameService`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HdfsLocation {
    endpoint: String,
    kind: HdfsEndpointKind,
    user: String,
    root: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HdfsEndpointKind {
    /// A single explicit `NameNode` RPC endpoint.
    Direct,
    /// A logical Hadoop HA `NameService`.
    NameService,
}

/// Explicit Hadoop configuration used for HDFS location resolution.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct HdfsConfig {
    pub config_dir: Option<PathBuf>,
    pub overrides: HashMap<String, String>,
    pub kerberos_credentials: Option<HdfsKerberosCredentials>,
}

/// Kerberos credential material scoped to one HDFS backend instance.
///
/// The principal is parsed from the HDFS location URL so source and destination
/// backends in the same process can use independent identities.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct HdfsKerberosCredentials {
    /// Kerberos principal to acquire. When omitted, the user in the HDFS URL is used.
    pub principal: Option<String>,
    /// Keytab from which initiator credentials are acquired.
    pub keytab: Option<PathBuf>,
    /// Kerberos credential cache to read from or populate.
    pub cache: Option<String>,
}

impl fmt::Debug for HdfsKerberosCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HdfsKerberosCredentials")
            .field("principal", &self.principal)
            .field("keytab", &self.keytab.as_ref().map(|_| "<redacted>"))
            .field("cache", &self.cache.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl fmt::Debug for HdfsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let overrides = self
            .overrides
            .iter()
            .map(|(key, value)| {
                let safe_value = if is_sensitive_key(key) {
                    "<redacted>"
                } else {
                    value
                };
                (key, safe_value)
            })
            .collect::<HashMap<_, _>>();
        formatter
            .debug_struct("HdfsConfig")
            .field("config_dir", &self.config_dir)
            .field("overrides", &overrides)
            .field("kerberos_credentials", &self.kerberos_credentials)
            .finish()
    }
}

impl HdfsLocation {
    /// Parse `hdfs://user@host:port/root` without consulting ambient Hadoop
    /// configuration or authentication state.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the URL is not an explicit,
    /// password-free Simple-auth direct `NameNode` location.
    pub fn parse(location: &str) -> Result<Self, StorageError> {
        let parsed = Self::parse_configured(location, &HdfsConfig::default())?;
        if parsed.kind != HdfsEndpointKind::Direct {
            return Err(config_error("direct HDFS location requires an RPC port"));
        }
        Ok(parsed)
    }

    /// Parse either a direct `NameNode` or explicitly configured logical
    /// `NameService` location.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for incomplete topology, non-Simple
    /// authentication, or an invalid location.
    pub fn parse_configured(location: &str, config: &HdfsConfig) -> Result<Self, StorageError> {
        validate_percent_encoding(location)?;
        reject_raw_traversal(location)?;
        let url =
            Url::parse(location).map_err(|error| StorageError::UrlParseError(error.to_string()))?;

        if url.scheme() != "hdfs" {
            return Err(config_error("HDFS location must use the hdfs scheme"));
        }
        if url.password().is_some() {
            return Err(config_error("HDFS location must not contain a password"));
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(config_error(
                "HDFS location must not contain a query or fragment",
            ));
        }

        let user = decode_component(url.username(), "username")?;
        if user.is_empty() {
            return Err(config_error("HDFS location requires an explicit user"));
        }
        let host = url
            .host()
            .ok_or_else(|| config_error("HDFS location requires a NameNode host"))?;
        let settings = config.settings()?;
        validate_authentication(&settings, config.kerberos_credentials.as_ref())?;
        let (endpoint, kind) = match url.port() {
            Some(port) => (format_endpoint(&host, port), HdfsEndpointKind::Direct),
            None => (
                format_nameservice_endpoint(&host, &settings)?,
                HdfsEndpointKind::NameService,
            ),
        };
        let root = normalize_root(url.path())?;

        Ok(Self {
            endpoint,
            kind,
            user,
            root,
        })
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[must_use]
    pub const fn kind(&self) -> HdfsEndpointKind {
        self.kind
    }

    #[must_use]
    pub fn user(&self) -> &str {
        &self.user
    }

    #[must_use]
    pub fn root(&self) -> &str {
        &self.root
    }

    /// The canonical endpoint identity of this location (ADR-0006): `hdfs://nameservice[/root]` or
    /// `hdfs://namenode:port[/root]`, without the user or principal.
    ///
    /// # Errors
    /// Returns an error if the configured endpoint or root cannot form a bounded identity.
    pub fn endpoint_identity(&self) -> Result<crate::model::BackendIdentity, StorageError> {
        crate::storage::endpoint::hdfs(&self.endpoint, &self.root)
            .map_err(|error| StorageError::ConfigError(error.to_string()))
    }
}

/// The canonical endpoint identity of an HDFS location without connecting (ADR-0006), parsed
/// exactly as the HDFS client parses it.
///
/// # Errors
/// Returns an error if the configured endpoint or root cannot form a bounded identity.
pub fn endpoint_identity(
    location: &str,
    config: &HdfsConfig,
) -> Result<crate::model::BackendIdentity, StorageError> {
    HdfsLocation::parse_configured(location, config)?.endpoint_identity()
}

impl HdfsConfig {
    ///
    /// # Errors
    /// Returns an error when an XML configuration file cannot be read or decoded.
    pub fn settings(&self) -> Result<HashMap<String, String>, StorageError> {
        let mut settings = HashMap::new();
        if let Some(directory) = &self.config_dir {
            for filename in ["core-site.xml", "hdfs-site.xml"] {
                let path = directory.join(filename);
                if path.exists() {
                    let contents = fs::read_to_string(&path).map_err(StorageError::IoError)?;
                    let document: HadoopConfiguration = quick_xml::de::from_str(&contents)
                        .map_err(|error| StorageError::ConfigError(error.to_string()))?;
                    for property in document.properties {
                        settings.insert(
                            property.name.trim().to_string(),
                            property.value.trim().to_string(),
                        );
                    }
                }
            }
        }
        settings.extend(self.overrides.clone());
        Ok(settings)
    }
}

#[derive(Deserialize)]
struct HadoopConfiguration {
    #[serde(rename = "property", default)]
    properties: Vec<HadoopProperty>,
}

#[derive(Deserialize)]
struct HadoopProperty {
    name: String,
    value: String,
}

///
/// # Errors
/// Returns an error for unsupported authentication modes or incomplete Kerberos configuration.
pub fn validate_authentication<S: std::hash::BuildHasher>(
    settings: &HashMap<String, String, S>,
    credentials: Option<&HdfsKerberosCredentials>,
) -> Result<(), StorageError> {
    let mode = settings.get("hadoop.security.authentication").map_or_else(
        || {
            if credentials.is_some() {
                "kerberos"
            } else {
                "simple"
            }
        },
        |mode| mode.trim(),
    );
    if mode.eq_ignore_ascii_case("simple") {
        if credentials.is_some() {
            return Err(config_error(
                "HDFS Kerberos credentials require kerberos authentication",
            ));
        }
    } else if mode.eq_ignore_ascii_case("kerberos") {
        if credentials.is_none() {
            return Err(config_error(
                "Kerberos HDFS authentication requires client-scoped credentials",
            ));
        }
    } else {
        return Err(config_error("unsupported HDFS authentication mode"));
    }
    Ok(())
}

///
/// # Errors
/// Returns an error for missing, invalid, or duplicate nameservice addresses.
pub fn format_nameservice_endpoint<S: std::hash::BuildHasher>(
    host: &Host<&str>,
    settings: &HashMap<String, String, S>,
) -> Result<String, StorageError> {
    let Host::Domain(service) = host else {
        return Err(config_error("direct HDFS IP locations require an RPC port"));
    };
    let membership_key = format!("dfs.ha.namenodes.{service}");
    let members = settings
        .get(&membership_key)
        .ok_or_else(|| config_error("HDFS NameService requires explicit HA membership"))?;
    let members = members
        .split(',')
        .map(str::trim)
        .filter(|member| !member.is_empty())
        .collect::<Vec<_>>();
    if members.is_empty() {
        return Err(config_error(
            "HDFS NameService membership must not be empty",
        ));
    }
    for member in members {
        let address_key = format!("dfs.namenode.rpc-address.{service}.{member}");
        if settings
            .get(&address_key)
            .is_none_or(|address| address.trim().is_empty())
        {
            return Err(config_error(
                "every HDFS NameService member requires an RPC address",
            ));
        }
    }
    Ok(format!("hdfs://{service}"))
}

#[must_use]
pub fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    ["password", "secret", "token", "credential", "keytab"]
        .iter()
        .any(|marker| key.contains(marker))
}

///
/// # Errors
/// Returns an error if the raw URL contains a parent-directory component.
pub fn reject_raw_traversal(location: &str) -> Result<(), StorageError> {
    let Some(authority_start) = location.find("://").map(|index| index + 3) else {
        return Ok(());
    };
    let Some(path_offset) = location[authority_start..].find('/') else {
        return Ok(());
    };
    let raw_path = &location[authority_start + path_offset..];
    let raw_path = raw_path
        .split(['?', '#'])
        .next()
        .ok_or_else(|| config_error("HDFS location contains an invalid path"))?;
    let decoded = decode_component(raw_path, "root path")?;
    if decoded.split('/').any(|component| component == "..") {
        return Err(config_error("HDFS root must not contain traversal"));
    }
    Ok(())
}

#[must_use]
pub fn config_error(message: &str) -> StorageError {
    StorageError::ConfigError(message.to_string())
}

///
/// # Errors
/// Returns an error if a percent escape is incomplete or non-hexadecimal.
pub fn validate_percent_encoding(value: &str) -> Result<(), StorageError> {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && (index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit())
        {
            return Err(config_error(
                "HDFS location contains malformed percent encoding",
            ));
        }
        index += if bytes[index] == b'%' { 3 } else { 1 };
    }
    Ok(())
}

///
/// # Errors
/// Returns an error for invalid percent encoding or non-UTF-8 decoded text.
pub fn decode_component(value: &str, label: &str) -> Result<String, StorageError> {
    percent_decode_str(value)
        .decode_utf8()
        .map(String::from)
        .map_err(|_| config_error(&format!("HDFS {label} is not valid UTF-8")))
}

#[must_use]
pub fn format_endpoint(host: &Host<&str>, port: u16) -> String {
    match host {
        Host::Domain(domain) => format!("hdfs://{domain}:{port}"),
        Host::Ipv4(address) => format!("hdfs://{address}:{port}"),
        Host::Ipv6(address) => format!("hdfs://[{address}]:{port}"),
    }
}

///
/// # Errors
/// Returns an error for invalid escapes, traversal components, or an invalid root path.
pub fn normalize_root(encoded_path: &str) -> Result<String, StorageError> {
    let decoded = decode_component(encoded_path, "root path")?;
    if !decoded.starts_with('/') {
        return Err(config_error("HDFS root must be absolute"));
    }

    let mut components = Vec::new();
    for component in decoded.split('/') {
        match component {
            "" | "." => {}
            ".." => return Err(config_error("HDFS root must not contain traversal")),
            value => components.push(value),
        }
    }

    if components.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", components.join("/")))
    }
}
